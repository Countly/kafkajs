jest.mock('../../utils/shuffle', () => (list) => [...list])

const { Kafka } = require('../../../index')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { WireOrder } = require('../../../testHelpers/wireOrder')
const { TOPIC, TOPIC_ID } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { KafkaJSConnectionError } = require('../../errors')

const NODES = [0, 1, 2].map((id) => ({ id, host: `node${id}`, port: 9092 }))
const REPLICAS = [1, 2]
const RETRY = { retries: 40, initialRetryTime: 1, maxRetryTime: 5, factor: 0 }
const RETAINED_BATCHES = 5
const OUT_OF_ORDER_SEQUENCE_NUMBER = 45
/** Record counts tell the three sends apart on the wire. */
const COUNTS = { warmup: 2, lost: 3, next: 4 }

/**
 * One partition replicated on nodes 1 and 2, judged by the Kafka 4.4.0 leader's rules for a
 * client append: a batch matching one of the producer's last five batches is a duplicate and is
 * answered with its first offset; with state, a batch must continue the producer's sequence; with
 * no state for the producer id, any sequence is accepted (`ProducerAppendInfo.checkSequence`).
 * Every append reaches both replicas, so each replica's producer state holds it until `forget`.
 * @see https://github.com/apache/kafka/blob/4.4.0/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java
 * @see https://github.com/apache/kafka/blob/4.4.0/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateEntry.java
 */
class ReplicatedPartition {
  /** @type {Array<{ offset: number, digest: string }>} */
  log = []
  #nextOffset = 0
  /** @type {Map<number, Map<string, { epoch: number, batches: object[] }>>} */
  #states = new Map(REPLICAS.map((node) => [node, new Map()]))

  /**
   * Appends a client batch on a leader, or answers it as a duplicate or out of order.
   * @param {number} node the leader the batch was written to
   * @param {{ producerId: string, producerEpoch: number, baseSequence: number, recordCount: number, digest: string }} batch
   * @returns {{ errorCode: number, baseOffset: string, appended: boolean }}
   */
  append(node, { producerId, producerEpoch, baseSequence, recordCount, digest }) {
    const lastSequence = baseSequence + recordCount - 1
    const state = this.#states.get(node).get(producerId)
    if (state !== undefined && state.epoch === producerEpoch) {
      const duplicate = state.batches.find(
        ({ first, last }) => first === baseSequence && last === lastSequence
      )
      if (duplicate) return { errorCode: 0, baseOffset: String(duplicate.offset), appended: false }
    }
    if (state !== undefined) {
      const expected = state.epoch === producerEpoch ? state.batches.at(-1).last + 1 : 0
      if (baseSequence !== expected) {
        return { errorCode: OUT_OF_ORDER_SEQUENCE_NUMBER, baseOffset: '-1', appended: false }
      }
    }

    const offset = this.#nextOffset
    this.#nextOffset += recordCount
    this.log.push({ offset, digest })
    for (const states of this.#states.values()) {
      const entry = states.get(producerId)
      const kept = entry?.epoch === producerEpoch ? entry.batches : []
      const added = { first: baseSequence, last: lastSequence, offset }
      states.set(producerId, {
        epoch: producerEpoch,
        batches: [...kept, added].slice(-RETAINED_BATCHES),
      })
    }
    return { errorCode: 0, baseOffset: String(offset), appended: true }
  }

  /**
   * Drops every producer's state on a node, as a broker does when it reloads a snapshot whose
   * producers it then treats as expired.
   * @param {number} node
   * @returns {void}
   */
  forget(node) {
    this.#states.get(node).clear()
  }

  /**
   * @param {string} digest a batch's bytes digest
   * @returns {number[]} the offsets the batch was appended at
   */
  offsetsOf(digest) {
    return this.log.filter((entry) => entry.digest === digest).map(({ offset }) => offset)
  }
}

/**
 * The bounce schedule of an idempotent producer on one partition: the leader (node 1, epoch 2)
 * appends a batch and the batch reaches both replicas, then the leader dies before its answer is
 * written. Where the re-send lands depends on the arm:
 * - `follower`: node 2 leads at epoch 3 and is reachable.
 * - `restartRetained`/`restartLost`: node 2 leads at epoch 3 but cannot be reached; after two
 *   metadata refreshes node 1 is back and leads at epoch 4, with its producer state kept or lost.
 */
class LeaderChangeCase {
  #arm
  #leader = { id: 1, epoch: 2 }
  #phase = 'steady'
  #refreshesWhileLost = 0
  #acknowledged = false
  #lostWritten
  partition = new ReplicatedPartition()
  /** @type {string[]} */
  violations = []

  /** @param {'follower'|'restartRetained'|'restartLost'} arm */
  constructor(arm) {
    this.#arm = arm
    this.lostWritten = new Promise((resolve) => (this.#lostWritten = resolve))
    this.cluster = producerScript(createScriptedCluster({ nodes: NODES }))
      .answer('Metadata', () => this.#metadata())
      .answer('Produce', (fields, record) => this.#produce(fields, record))
    this.producer = new Kafka(this.cluster.kafkaOptions({ retry: RETRY })).producer({
      idempotent: true,
      maxInFlightRequests: 5,
      retry: RETRY,
    })
  }

  /**
   * Runs the schedule: a warm-up send, the send whose answer is lost, and a send issued while the
   * lost one waits for its re-send.
   * @returns {Promise<{ lost: object[], next: object[] }>} the public results of the last two
   * @throws {Error} when a send fails
   */
  async run() {
    await this.producer.connect()
    await this.#send(COUNTS.warmup)
    const lost = this.#send(COUNTS.lost)
    await this.lostWritten
    const next = this.#send(COUNTS.next)
    return { lost: await lost, next: await next }
  }

  /** @returns {Promise<void>} once the producer is disconnected and the cluster checked clean */
  async close() {
    await this.producer.disconnect()
    this.cluster.assertClean()
  }

  /**
   * @param {number} count records in the send
   * @returns {Promise<object[]>}
   */
  #send(count) {
    return this.producer.send({
      topic: TOPIC,
      acks: -1,
      messages: Array.from({ length: count }, (_, index) => ({
        partition: 0,
        value: `${count}:${index}`,
        timestamp: '1700000000000',
      })),
    })
  }

  /** @returns {object} the metadata answer for the current leader */
  #metadata() {
    if (this.#phase === 'lost' && this.#arm !== 'follower' && ++this.#refreshesWhileLost === 2) {
      if (this.#arm === 'restartLost') this.partition.forget(1)
      this.#leader = { id: 1, epoch: 4 }
      this.cluster.up(1)
      this.#phase = 'returned'
    }
    return {
      brokers: NODES.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      controllerId: 0,
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: TOPIC_ID,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: this.#leader.id,
              leaderEpoch: this.#leader.epoch,
              replicas: REPLICAS,
              isr: REPLICAS,
              offlineReplicas: [],
            },
          ],
        },
      ],
    }
  }

  /**
   * Takes a node down: later dials are refused, and the sockets the client holds to it are reset
   * on a later turn, after the answer being written now is lost.
   * @param {number} node
   * @returns {void}
   */
  #kill(node) {
    this.cluster.down(node)
    setImmediate(() => {
      for (const socket of this.cluster.sockets()) {
        if (socket.endpoint.host === `node${node}` && !socket.destroyed) {
          socket.emit('error', Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }))
        }
      }
    })
  }

  /**
   * Appends on the leader the request reached; loses the first answer of the lost send.
   * @param {object} fields the Produce request
   * @param {{ node: number }} record
   * @returns {object} the Produce answer
   * @throws {KafkaJSConnectionError} for the lost answer
   */
  #produce(fields, { node }) {
    const [topic] = fields.topics
    const [{ batches }] = topic.partitions
    const [batch] = batches
    if (batch.recordCount === COUNTS.next && !this.#acknowledged) {
      this.violations.push('a later batch was written before the lost batch was acknowledged')
    }
    const result =
      node === this.#leader.id
        ? this.partition.append(node, batch)
        : { errorCode: 6, baseOffset: '-1', appended: false }

    if (batch.recordCount === COUNTS.lost && this.#phase === 'steady') {
      this.#phase = 'lost'
      this.#leader = { id: 2, epoch: 3 }
      this.#kill(1)
      if (this.#arm !== 'follower') this.#kill(2)
      this.#lostWritten()
      throw new KafkaJSConnectionError('the leader died before its answer was written')
    }
    if (batch.recordCount === COUNTS.lost && result.errorCode === 0) this.#acknowledged = true

    return {
      responses: [
        {
          ...topic,
          partitions: [
            {
              partition: 0,
              errorCode: result.errorCode,
              baseOffset: result.baseOffset,
              logAppendTime: '-1',
              logStartOffset: '0',
              leader: null,
            },
          ],
        },
      ],
      nodeEndpoints: [],
    }
  }
}

/**
 * @param {object[]} rows Produce rows of the wire-order oracle
 * @param {number} count the send's record count
 * @returns {object[]} the rows of that send's batch, in transmission order
 */
const rowsOf = (rows, count) => rows.filter((row) => row.recordCount === count)

/**
 * @param {object} row a Produce row
 * @returns {Array} the batch's producer stamp and bytes
 */
const stampOf = ({ producerId, producerEpoch, sequence, recordCount, digest }) => [
  producerId,
  producerEpoch,
  sequence,
  recordCount,
  digest,
]

describe('Producer > a batch whose answer is lost across a leader change', () => {
  /** @type {LeaderChangeCase} */
  let leaderCase

  afterEach(async () => {
    await leaderCase?.close()
    leaderCase = undefined
  })

  it.each([
    ['follower', 'the new leader that replicated it', 2],
    ['restartRetained', 'the restarted old leader that kept its producer state', 1],
  ])('is re-sent unchanged and appended once when %s answers (%s)', async (arm, _, resentTo) => {
    leaderCase = new LeaderChangeCase(arm)
    const { lost, next } = await leaderCase.run()

    const wire = WireOrder.of(leaderCase.cluster)
    wire.assertNoRewrite()
    wire.assertSequencesInOrder()
    const attempts = rowsOf(wire.produced(), COUNTS.lost)
    expect(attempts.length).toBeGreaterThanOrEqual(2)
    expect(attempts[0].node).toBe(1)
    expect(attempts.at(-1).node).toBe(resentTo)
    for (const attempt of attempts) expect(stampOf(attempt)).toEqual(stampOf(attempts[0]))

    expect(leaderCase.partition.offsetsOf(attempts[0].digest)).toEqual([2])
    expect(lost).toEqual([expect.objectContaining({ partition: 0, errorCode: 0, baseOffset: '2' })])
    expect(rowsOf(wire.produced(), COUNTS.next)[0].sequence).toBe(
      attempts[0].sequence + COUNTS.lost
    )
    expect(next).toEqual([expect.objectContaining({ partition: 0, errorCode: 0, baseOffset: '5' })])
    expect(leaderCase.violations).toEqual([])
  })

  it('sends the identical stamp to a returning leader that lost its producer state, whose second append is broker state loss', async () => {
    leaderCase = new LeaderChangeCase('restartLost')
    const { lost, next } = await leaderCase.run()

    const wire = WireOrder.of(leaderCase.cluster)
    wire.assertNoRewrite()
    wire.assertSequencesInOrder()
    const attempts = rowsOf(wire.produced(), COUNTS.lost)
    expect(attempts.length).toBeGreaterThanOrEqual(2)
    expect(attempts[0].node).toBe(1)
    expect(attempts.at(-1).node).toBe(1)
    for (const attempt of attempts) expect(stampOf(attempt)).toEqual(stampOf(attempts[0]))

    expect(leaderCase.partition.offsetsOf(attempts[0].digest)).toEqual([2, 5])
    expect(lost).toEqual([expect.objectContaining({ partition: 0, errorCode: 0, baseOffset: '5' })])
    expect(rowsOf(wire.produced(), COUNTS.next)[0].sequence).toBe(
      attempts[0].sequence + COUNTS.lost
    )
    expect(next).toEqual([expect.objectContaining({ partition: 0, errorCode: 0, baseOffset: '8' })])
    expect(leaderCase.violations).toEqual([])
  })
})
