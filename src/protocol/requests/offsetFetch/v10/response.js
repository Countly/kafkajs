const Decoder = require('../../../decoder')
const { parse } = require('../v8/response')
const { KafkaJSInvariantViolation } = require('../../../../errors')
const { topicIdSchema } = require('../../topicIds')

/**
 * OffsetFetch Response (Version: 10) — flexible (KIP-482), response header v1, topic ids (KIP-848)
 *
 * ThrottleTimeMs => INT32
 * Groups => COMPACT_ARRAY
 *   GroupId => COMPACT_STRING
 *   Topics => COMPACT_ARRAY
 *     TopicId => UUID
 *     Partitions => COMPACT_ARRAY
 *       PartitionIndex => INT32
 *       CommittedOffset => INT64
 *       CommittedLeaderEpoch => INT32
 *       Metadata => COMPACT_NULLABLE_STRING
 *       ErrorCode => INT16
 *       TAG_BUFFER
 *     TAG_BUFFER
 *   ErrorCode => INT16
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v9's body keyed by `TopicId`, flattened to v8's shape (`responses[].topic`, one group, the group's
 * error code at the top). Names are restored from the request's own id-to-name map, as in
 * OffsetCommit v10; an id it did not carry (an all-topics answer) is kept as `topicId` with a null
 * `topic`. UNKNOWN_TOPIC_ID (100, retriable) arrives as a partition `errorCode`, and `parse` throws it.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetFetchResponse.json (Apache Kafka 4.3.1)
 */
const decodeGroup = (topicNames) => (decoder) => {
  const group = {
    groupId: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray(decodeTopic(topicNames)) || [],
    errorCode: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return group
}

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
    offset: decoder.readInt64().toString(),
    committedLeaderEpoch: decoder.readInt32(),
    metadata: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return partition
}

/**
 * @param {Buffer} rawData
 * @param {{ topicNames?: Map<string, string> }} [options] topic id to topic name, bound per request
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, errorCode: number,
 *   responses: { topic: string|null, topicId?: string|null, partitions: object[] }[] }>}
 * @throws {KafkaJSInvariantViolation} when the answer does not hold exactly one group
 */
const decode = async (rawData, { topicNames = new Map() } = {}) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const groups = decoder.readUVarIntArray(decodeGroup(topicNames)) || []
  decoder.readTaggedFields()

  if (groups.length !== 1) {
    throw new KafkaJSInvariantViolation(
      `Expected exactly one group in an OffsetFetch v10 response, received ${groups.length}`
    )
  }

  const [group] = groups
  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode: group.errorCode,
    responses: group.topics,
  }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = topicIdSchema(require('../v9/response').schema)
