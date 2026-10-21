const Decoder = require('../../../decoder')
const { parse } = require('../v8/response')
const { topicIdSchema } = require('../../topicIds')

/**
 * OffsetCommitResponse (Version: 10) — flexible (KIP-482), response header v1, topic ids (KIP-848)
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
 * v8's body keyed by `TopicId`, decoded to v8's shape (`responses[].topic`): the name is restored
 * here from the id-to-name map of the request that produced the response, because `parse` and its
 * callers run inside the protocol layer. An id the request did not carry is kept as `topicId` with
 * a null `topic`. UNKNOWN_TOPIC_ID (100, retriable) arrives as a partition `errorCode`, and `parse`
 * throws it like any other.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetCommitResponse.json (Apache Kafka 4.3.1)
 */
const decodeTopic = (topicNames) => (decoder) => {
  const topicId = decoder.readUuid()
  const partitions = decoder.readUVarIntArray(decodePartition) || []
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
 *   responses: { topic: string|null, topicId?: string|null, partitions: { partition: number, errorCode: number }[] }[] }>}
 */
const decode = async (rawData, { topicNames = new Map() } = {}) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const responses = decoder.readUVarIntArray(decodeTopic(topicNames)) || []

  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, responses }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = topicIdSchema(require('../v9/response').schema)
