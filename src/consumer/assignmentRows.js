const Decoder = require('../protocol/decoder')
const { KafkaJSPartialMessageError } = require('../errors')
const { malformedGroupAnswer } = require('../protocol/requests/groupProtocolCheck')

const NULL_COUNT = -1

/** @param {Decoder} decoder @returns {number} */
const readPartition = (decoder) => decoder.readInt32()

/**
 * An INT32-counted array that keeps the null array (count -1) as null, which `readArray` reads as
 * empty.
 * @param {Decoder} decoder
 * @param {(decoder: Decoder) => *} reader
 * @returns {Array|null}
 */
const readNullableArray = (decoder, reader) => {
  if (decoder.canReadInt32() && decoder.buffer.readInt32BE(decoder.offset) === NULL_COUNT) {
    decoder.readInt32()
    return null
  }
  return decoder.readArray(reader)
}

/** @param {Decoder} decoder @returns {{ topic: string|null, partitions: number[]|null }} */
const readRow = (decoder) => ({
  topic: decoder.readString(),
  partitions: readNullableArray(decoder, readPartition),
})

/**
 * Reads a consumer protocol assignment (`ConsumerProtocolAssignment`) row by row, without merging
 * rows that name one topic, so a caller can check them before building a topic map.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json
 * @param {Buffer} buffer
 * @returns {{ version: number, topics: { topic: string|null, partitions: number[]|null }[]|null,
 *   userData: Buffer|null }|null} null when the buffer is shorter than its version; `topics` and a
 *   row's `partitions` are null for the null array
 * @throws {KafkaJSPartialMessageError} when the buffer ends inside a field
 */
const readAssignmentRows = (buffer) => {
  const decoder = new Decoder(buffer)
  if (!decoder.canReadInt16()) return null

  const version = decoder.readInt16()
  const topics = readNullableArray(decoder, readRow)
  return { version, topics, userData: decoder.readBytes() }
}

/**
 * The assignment a SyncGroup answer hands this member. The bytes come from the group leader, which
 * may be another client or a custom assigner, and Kafka relays them as written. Zero bytes mean
 * nothing assigned. A topic named in several rows gets the partitions of all of them, and a
 * repeated partition is kept once, in its first place, as the Java client's sets do; the repeats
 * are returned so the caller can warn about them. The cooperative ownership check of a leader
 * reads the assignments it uploads through this same function.
 * @param {Buffer|null} bytes the answer's assignment
 * @returns {{ assignment: Object<string, number[]>, userData: Buffer|null,
 *   repeatedTopics: string[], repeated: { topic: string, partition: number }[] }}
 *   `repeatedTopics` names each topic of more than one row once
 * @throws {import('../errors').KafkaJSError} retriable, type `MALFORMED_GROUP_ANSWER`, for null
 *   bytes, a truncated assignment, a null topic list, a row without a topic name, a null partition
 *   list or a negative partition
 */
const readSyncAssignment = (bytes) => {
  if (!Buffer.isBuffer(bytes)) {
    throw malformedGroupAnswer('SyncGroup response carries no assignment')
  }
  if (bytes.length === 0)
    return { assignment: {}, userData: null, repeatedTopics: [], repeated: [] }

  let rows
  try {
    rows = readAssignmentRows(bytes)
  } catch (error) {
    if (!(error instanceof KafkaJSPartialMessageError)) throw error
    throw malformedGroupAnswer(
      `SyncGroup response carries a truncated assignment: ${error.message}`
    )
  }
  if (rows === null) {
    throw malformedGroupAnswer('SyncGroup response carries an assignment shorter than its version')
  }
  if (rows.topics === null) {
    throw malformedGroupAnswer('SyncGroup response carries an assignment with a null topic list')
  }

  const partitionsOf = new Map()
  const repeatedTopics = new Set()
  const repeated = []
  for (const { topic, partitions } of rows.topics) {
    if (typeof topic !== 'string' || topic === '') {
      throw malformedGroupAnswer('SyncGroup response carries an assignment row with no topic')
    }
    if (partitions === null) {
      throw malformedGroupAnswer(
        `SyncGroup response carries a null partition list for topic ${topic}`
      )
    }
    let kept = partitionsOf.get(topic)
    if (kept === undefined) partitionsOf.set(topic, (kept = new Set()))
    else repeatedTopics.add(topic)
    for (const partition of partitions) {
      if (partition < 0) {
        throw malformedGroupAnswer(
          `SyncGroup response assigns partition ${partition} of topic ${topic}`
        )
      }
      if (kept.has(partition)) repeated.push({ topic, partition })
      else kept.add(partition)
    }
  }

  return {
    assignment: Object.fromEntries(
      Array.from(partitionsOf, ([topic, kept]) => [topic, Array.from(kept)])
    ),
    userData: rows.userData,
    repeatedTopics: Array.from(repeatedTopics),
    repeated,
  }
}

module.exports = { readAssignmentRows, readSyncAssignment }
