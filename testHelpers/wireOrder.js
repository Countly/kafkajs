const PRODUCE = 'Produce'

/**
 * What every check of `WireOrder` throws, so a caller can tell a broken ordering from any other
 * failure (a missing answer, a bad log) that would otherwise look the same.
 */
class WireOrderViolation extends Error {
  /** @param {string} message what was found out of order, lost or rewritten */
  constructor(message) {
    super(message)
    this.name = 'WireOrderViolation'
  }
}
const TRANSACTION_APIS = ['AddPartitionsToTxn', 'AddOffsetsToTxn', 'TxnOffsetCommit', 'EndTxn']

/**
 * The wire-order oracle. It is built from what the client wrote, as the scripted cluster's request
 * readers saw it, and never from what a test meant to send: a test that expects one thing and a
 * client that transmitted another must fail here.
 *
 * Each Produce request becomes one row per partition batch, each transaction request one row:
 * `{ order, api, apiKey, node, connectionGeneration, correlationId, producerId, producerEpoch,
 * sequence, topic, partition, transactionalId, ... }`. `order` is the request's place among
 * everything the cluster saw, `connectionGeneration` counts the sockets opened to its node, and
 * `topic` is the topic's name when the request carries one and its id otherwise (Produce v13).
 */
class WireOrder {
  /**
   * @param {object} log
   * @param {object[]} log.requests the cluster's request records, `ScriptedCluster#requests()`
   * @param {object[]} [log.timeline] requests, answers and marks, `ScriptedCluster#timeline()`;
   *   only the checks that look at answers or marks need it
   */
  constructor({ requests, timeline = [] }) {
    this.timeline = timeline
    this.rows = requests.flatMap(rowsOf)
  }

  /**
   * @param {{ requests(): object[], timeline(): object[] }} cluster a scripted cluster
   * @returns {WireOrder}
   */
  static of(cluster) {
    return new WireOrder({ requests: cluster.requests(), timeline: cluster.timeline() })
  }

  /**
   * @returns {Array<object>} the Produce rows in transmission order
   */
  produced() {
    return this.rows.filter((row) => row.api === PRODUCE)
  }

  /**
   * A partition's batches for one producer id and epoch never go backwards on one connection,
   * and the sequences it was sent form an unbroken run: each new sequence is the one after the previous batch's last
   * record. A resend of a sequence already sent adds nothing to the run.
   * @throws {WireOrderViolation} naming the partition and the sequences transmitted, when either fails
   */
  assertSequencesInOrder() {
    for (const rows of groupBy(this.produced(), (row) =>
      JSON.stringify([row.topic, row.partition, row.producerId, row.producerEpoch])
    ).values()) {
      const key = partitionKey(rows[0])
      const runs = groupBy(rows, (row) => `${row.node}#${row.connectionGeneration}`)
      for (const [connection, sent] of runs) {
        sent.forEach((row, index) => {
          if (index > 0 && row.sequence < sent[index - 1].sequence) {
            throw new WireOrderViolation(
              `${key}: sequence ${row.sequence} was written after ${sent[index - 1].sequence} on ${connection}`
            )
          }
        })
      }

      const first = new Map()
      rows.forEach((row) => first.has(row.sequence) || first.set(row.sequence, row))
      const run = [...first.values()].sort((a, b) => a.sequence - b.sequence)
      run.forEach((row, index) => {
        if (index > 0 && row.sequence !== run[index - 1].sequence + run[index - 1].recordCount) {
          throw new WireOrderViolation(
            `${key}: sequences ${run.map(({ sequence }) => sequence)} are not an unbroken run`
          )
        }
      })
    }
  }

  /**
   * No `(producer id, epoch, sequence)` of a partition is written twice with different bytes: a
   * resend must be the same batch.
   * @throws {WireOrderViolation} naming the partition, producer and sequence that was rewritten
   */
  assertNoRewrite() {
    const seen = new Map()
    for (const row of this.produced()) {
      const key = `${partitionKey(row)}|${row.producerId}|${row.producerEpoch}|${row.sequence}`
      const earlier = seen.get(key)
      if (earlier && (earlier.digest !== row.digest || earlier.recordCount !== row.recordCount)) {
        throw new WireOrderViolation(`${key} was written twice with different bytes`)
      }
      seen.set(key, row)
    }
  }

  /**
   * A request of an older connection of a node never follows one of a newer connection for the
   * same partition.
   * @throws {WireOrderViolation} naming the partition and the two generations
   */
  assertNoStaleGeneration() {
    for (const [key, rows] of groupBy(
      this.produced(),
      (row) => `${row.node}|${partitionKey(row)}`
    )) {
      rows.forEach((row, index) => {
        if (index > 0 && row.connectionGeneration < rows[index - 1].connectionGeneration) {
          throw new WireOrderViolation(
            `${key}: generation ${row.connectionGeneration} wrote after generation ${rows[index - 1].connectionGeneration}`
          )
        }
      })
    }
  }

  /**
   * Every TxnOffsetCommit written before an EndTxn was answered before that EndTxn was written.
   * @throws {WireOrderViolation} naming the EndTxn and the TxnOffsetCommit still unanswered
   */
  assertEndTxnAfterOffsets() {
    const events = this.timeline
    for (const end of events.filter((e) => e.kind === 'request' && e.api === 'EndTxn')) {
      for (const offsets of events.filter(
        (e) => e.kind === 'request' && e.api === 'TxnOffsetCommit' && e.order < end.order
      )) {
        const answered = events.find(
          (e) =>
            e.kind === 'response' &&
            e.api === offsets.api &&
            e.correlationId === offsets.correlationId &&
            e.node === offsets.node &&
            e.generation === offsets.generation &&
            e.order > offsets.order
        )
        if (!answered || answered.order > end.order) {
          throw new WireOrderViolation(
            `EndTxn (correlation ${end.correlationId}) was written before TxnOffsetCommit (correlation ${offsets.correlationId}) was answered`
          )
        }
      }
    }
  }

  /**
   * The partition batches the test expects were all transmitted, and nothing else was.
   * @param {Array<{ topic: string, partition: number, sequence: number }>} expected what the test
   *   meant to send, by the topic key the requests carry
   * @throws {WireOrderViolation} naming the batches lost and the batches nobody expected
   */
  assertTransmitted(expected) {
    const keyOf = ({ topic, partition, sequence }) => `${topic}|${partition}|${sequence}`
    const sent = new Set(this.produced().map(keyOf))
    const wanted = new Set(expected.map(keyOf))
    const lost = [...wanted].filter((key) => !sent.has(key))
    const extra = [...sent].filter((key) => !wanted.has(key))

    if (lost.length > 0 || extra.length > 0) {
      throw new WireOrderViolation(
        `lost ${JSON.stringify(lost)}, unexpected ${JSON.stringify(extra)}`
      )
    }
  }

  /**
   * The timeline holds these events in this order, other events allowed between them. An event is
   * `'<api> request'`, `'<api> response'` or the label of a mark.
   * @param {string[]} labels
   * @throws {WireOrderViolation} naming the first event not found after the ones before it
   */
  assertOrder(labels) {
    const names = this.timeline.map((event) =>
      event.kind === 'mark' ? event.label : `${event.api} ${event.kind}`
    )
    let from = 0
    for (const [position, label] of labels.entries()) {
      const at = names.indexOf(label, from)
      if (at === -1) {
        throw new WireOrderViolation(
          `"${label}" did not follow ${JSON.stringify(labels.slice(0, position))}; saw ${JSON.stringify(names)}`
        )
      }
      from = at + 1
    }
  }
}

/** @returns {string} the identity of a row's partition */
const partitionKey = (row) => `${row.topic}|${row.partition}`

/** @returns {Map<string, object[]>} the items under their key, in the order given */
const groupBy = (items, keyOf) => {
  const groups = new Map()
  for (const item of items) {
    const key = keyOf(item)
    groups.set(key, [...(groups.get(key) ?? []), item])
  }

  return groups
}

/**
 * @param {object} record a request record
 * @returns {object[]} its rows: one per partition batch of a Produce, one for a transaction request
 */
function rowsOf(record) {
  const base = {
    order: record.order,
    api: record.api,
    apiKey: record.apiKey,
    node: record.node,
    connectionGeneration: record.generation,
    correlationId: record.correlationId,
  }

  if (record.api === PRODUCE) {
    return record.fields.topics.flatMap((entry) =>
      entry.partitions.flatMap(({ partition, batches }) =>
        (batches ?? []).map((batch) => ({
          ...base,
          transactionalId: record.fields.transactionalId,
          topic: entry.topic ?? entry.topicId,
          partition,
          producerId: batch.producerId,
          producerEpoch: batch.producerEpoch,
          sequence: batch.baseSequence,
          recordCount: batch.recordCount,
          transactional: batch.transactional,
          digest: batch.digest,
        }))
      )
    )
  }

  if (TRANSACTION_APIS.includes(record.api)) {
    const { transactionalId, producerId, producerEpoch, transactionResult } = record.fields
    return [{ ...base, transactionalId, producerId, producerEpoch, committed: transactionResult }]
  }

  return []
}

module.exports = { WireOrder, WireOrderViolation }
