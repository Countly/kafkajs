const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')
const { exact } = require('../../offsetFetch/responseIdentity')
const {
  KafkaJSInvariantViolation,
  KafkaJSTransactionAbortableError,
} = require('../../../../errors')
const { sentTopics } = require('./request')
const responseV5 = require('../v5/response')

/**
 * TxnOffsetCommit Response (Version: 6) — flexible (KIP-482), response header v1, topic ids (KIP-1319)
 *
 * ThrottleTimeMs => INT32
 * Topics => COMPACT_ARRAY
 *   TopicId => UUID
 *   Partitions => COMPACT_ARRAY
 *     PartitionIndex => INT32
 *     ErrorCode => INT16
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v5's body keyed by `TopicId`, decoded to v5's shape (`topics[].topic`): the name is restored here
 * from the id-to-name map of the request that produced the response, never from current metadata,
 * which a recreated topic would have renamed. An id the request did not carry is kept as `topicId`
 * with a null `topic`, for `validate` to refuse. v6 can answer GROUP_ID_NOT_FOUND (69),
 * STALE_MEMBER_EPOCH (113) and UNKNOWN_TOPIC_ID (100) as a partition `errorCode`; `parse` throws
 * the first failed partition's error like any other.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitResponse.json (Apache Kafka 4.4.0)
 */
const decodeTopic = (topicNames) => (decoder) => {
  const topicId = decoder.readUuid()
  const partitions = Decoder.requireArray(
    decoder.readUVarIntArray(decodePartition),
    'TxnOffsetCommitResponse.Topics.Partitions'
  )
  decoder.readTaggedFields()

  const topic = topicNames.get(topicId)
  return topic === undefined ? { topic: null, topicId, partitions } : { topic, partitions }
}

const decodePartition = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
  }
  decoder.readTaggedFields()
  return partition
}

/**
 * @param {Buffer} rawData
 * @param {{ topicNames?: Map<string, string> }} [options] topic id to topic name, bound per request
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number,
 *   topics: { topic: string|null, topicId?: string, partitions: { partition: number, errorCode: number }[] }[] }>}
 */
const decode = async (rawData, { topicNames = new Map() } = {}) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const topics = Decoder.requireArray(
    decoder.readUVarIntArray(decodeTopic(topicNames)),
    'TxnOffsetCommitResponse.Topics'
  )
  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, topics }
}

/**
 * Checks that the answer correlates exactly with the request: every topic the request carried once,
 * every requested partition of it once, and nothing else - whatever order the broker used. A topic
 * the request left out for holding no partition (see `sentTopics`) is not expected: the broker
 * answers only the topics it forwards. The check runs before `parse`, so an answer that cannot be
 * correlated never reads as a commit that went through.
 *
 * A failed check is abortable, not fatal: nothing says what the coordinator did with the offsets,
 * but no producer state is damaged, so the transaction can be aborted and the producer goes on. The
 * identity violation is the abortable error's `cause`.
 * @param {{ topics: { topic: string|null, partitions: { partition: number }[] }[] }} data the
 *   decoded response
 * @param {{ topic: string, partitions: { partition: number|string }[] }[]} topics what the
 *   request committed
 * @returns {void}
 * @throws {KafkaJSTransactionAbortableError} for a duplicate, foreign or missing topic or
 *   partition; its `cause` is the `KafkaJSInvariantViolation` naming it
 */
const validate = ({ topics: answered }, topics) => {
  const requested = sentTopics(topics).map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition }) => ({ partition: Number(partition) })),
  }))
  const partitionsOf = new Map(requested.map(({ topic, partitions }) => [topic, partitions]))

  try {
    exact(answered, requested, 'topic', 'TxnOffsetCommit topic')
    for (const { topic, partitions } of answered) {
      exact(partitions, partitionsOf.get(topic), 'partition', 'TxnOffsetCommit partition')
    }
  } catch (cause) {
    if (!(cause instanceof KafkaJSInvariantViolation)) throw cause
    throw new KafkaJSTransactionAbortableError(
      `The TxnOffsetCommit answer does not match the request, so its offsets cannot be taken as committed: ${cause.message}`,
      { cause }
    )
  }
}

module.exports = { decode, parse, validate }

module.exports.schema = {
  ...responseV5.schema,
  version: 6,
  fields: responseV5.schema.fields.map((field) =>
    field.name === 'Topics.Name' ? { name: 'Topics.TopicId', type: 'uuid', versions: '6+' } : field
  ),
}
