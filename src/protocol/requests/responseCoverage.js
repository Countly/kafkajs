const { KafkaJSError } = require('../../errors')

/**
 * How one API's answer is held to its request. Produce uses `repeats: 'refuse'` and
 * `missing: 'allow'`; OffsetCommit the defaults. `asSent` lets a pair appear as often as the
 * request carried it (a commit may repeat a partition and the broker answers each entry);
 * `refuse` never lets a pair appear twice.
 * @typedef {object} CoverageRule
 * @property {string} [rowTopic='topic'] the field of an answer entry that names its topic
 * @property {string} [rowPartition='partition'] the field of an answer row that names its partition
 * @property {'asSent'|'refuse'} [repeats='asSent'] how often one pair may appear
 * @property {'refuse'|'allow'} [missing='refuse'] whether a pair the request carried may be absent
 * @property {boolean} [skipUnnamed=false] skip an answer entry whose topic is null instead of
 *   refusing it as unrequested
 */

/**
 * @param {string} message
 * @returns {KafkaJSError} retriable
 */
const refusal = (message) => new KafkaJSError(message, { retriable: true })

/**
 * @param {string} api
 * @param {*} topic
 * @param {*} partition the partition the answer names, if it names one
 * @returns {KafkaJSError} retriable, for a row or topic the request did not carry
 */
const unrequested = (api, topic, partition) =>
  refusal(
    partition === undefined
      ? `${api} response names topic ${topic} that the request did not carry`
      : `${api} response names partition ${Number(partition)} of topic ${topic} that the request did not carry`
  )

/**
 * Whether the answer names exactly the pairs of the request in the request's order, read as flat
 * (topic, partition) sequences over the existing arrays: no allocation. A topic entry with no
 * partitions, or any difference, answers false and leaves the decision to the full check.
 * @param {{ topic: string, partitions: { partition: number|string }[] }[]} sent
 * @param {object[]} rows
 * @param {string} rowTopic
 * @param {string} rowPartition
 * @returns {boolean}
 */
const matchesInOrder = (sent, rows, rowTopic, rowPartition) => {
  let t = 0
  let p = 0
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r]
    const { partitions } = row
    if (partitions.length === 0) return false
    for (let i = 0; i < partitions.length; i++) {
      while (t < sent.length && p >= sent[t].partitions.length) {
        t++
        p = 0
      }
      if (t === sent.length) return false
      if (
        sent[t].topic !== row[rowTopic] ||
        Number(sent[t].partitions[p].partition) !== Number(partitions[i][rowPartition])
      ) {
        return false
      }
      p++
    }
  }
  while (t < sent.length && p >= sent[t].partitions.length) {
    t++
    p = 0
  }
  return t === sent.length
}

/**
 * Checks the rows of an answer against the request it answers, before the answer changes any
 * state. A row for a topic or partition the request did not carry is refused, also when its topic
 * entry lists no partitions; a pair answered more often than `repeats` allows is refused; and with
 * `missing: 'refuse'` so is a pair the answer leaves out. Repeated topic entries of the request are
 * merged and partition ids compare as numbers. An answer that names the request's pairs in the
 * request's order passes without allocating (`repeats: 'asSent'` only); any other answer is counted.
 * @param {string} api the API's name, for the message
 * @param {{ topic: string, partitions: { partition: number|string }[] }[]} sent the request's entries
 * @param {{ partitions: object[] }[]} rows the answer's entries
 * @param {CoverageRule} [rule]
 * @returns {void}
 * @throws {import('../../errors').KafkaJSError} retriable, naming the API, topic and partition
 */
const requireCoverage = (
  api,
  sent,
  rows,
  {
    rowTopic = 'topic',
    rowPartition = 'partition',
    repeats = 'asSent',
    missing = 'refuse',
    skipUnnamed = false,
  } = {}
) => {
  if (repeats === 'asSent' && matchesInOrder(sent, rows, rowTopic, rowPartition)) return

  const left = new Map()
  for (const { topic, partitions } of sent) {
    let counts = left.get(topic)
    if (counts === undefined) left.set(topic, (counts = new Map()))
    for (const { partition } of partitions) {
      const id = Number(partition)
      counts.set(id, repeats === 'refuse' ? 1 : (counts.get(id) ?? 0) + 1)
    }
  }

  for (const row of rows) {
    const topic = row[rowTopic]
    if (topic == null && skipUnnamed) continue
    const counts = left.get(topic)
    if (counts === undefined) {
      throw unrequested(api, topic, row.partitions[0]?.[rowPartition])
    }
    for (const entry of row.partitions) {
      const id = Number(entry[rowPartition])
      const remaining = counts.get(id)
      if (remaining === undefined) throw unrequested(api, topic, id)
      if (remaining === 0) {
        throw refusal(`${api} response names partition ${id} of topic ${topic} twice`)
      }
      counts.set(id, remaining - 1)
    }
  }

  if (missing === 'refuse') {
    for (const [topic, counts] of left) {
      for (const [id, remaining] of counts) {
        if (remaining > 0) {
          throw refusal(`${api} response does not name partition ${id} of topic ${topic}`)
        }
      }
    }
  }
}

module.exports = { requireCoverage }
