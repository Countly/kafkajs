const Decoder = require('../../../decoder')
const { parse } = require('../v2/response')
const { KafkaJSInvariantViolation } = require('../../../../errors')

/**
 * OffsetFetch Response (Version: 8) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * Groups => COMPACT_ARRAY
 *   GroupId => COMPACT_STRING
 *   Topics => COMPACT_ARRAY
 *     Name => COMPACT_STRING
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
 * v8 wraps the response in a `Groups` list (KIP-709). This client only ever requests one group,
 * so exactly one is expected back; the single group is flattened into the shape every earlier
 * version returned (`responses[].topic`/`.partitions[]`, top-level `errorCode` taken from the
 * group's own error code) rather than growing a new `groups[]` shape for callers to learn.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-709%3A+Extend+OffsetFetch+requests+to+accept+multiple+group+ids
 * @see clients/src/main/resources/common/message/OffsetFetchResponse.json (Apache Kafka 4.3.1)
 */

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const groups = decoder.readUVarIntArray(decodeGroup) || []
  decoder.readTaggedFields()

  if (groups.length !== 1) {
    throw new KafkaJSInvariantViolation(
      `Expected exactly one group in an OffsetFetch v8 response, received ${groups.length}`
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

const decodeGroup = (decoder) => {
  const group = {
    groupId: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray(decodeTopic) || [],
    errorCode: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return group
}

const decodeTopic = (decoder) => {
  const topic = {
    topic: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartition) || [],
  }

  decoder.readTaggedFields()
  return topic
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

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'OffsetFetch',
  version: 8,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '3+' },
    { name: 'Groups', type: '[]OffsetFetchResponseGroup', versions: '8+' },
    { name: 'Groups.GroupId', type: 'string', versions: '8+' },
    { name: 'Groups.Topics', type: '[]OffsetFetchResponseTopics', versions: '8+' },
    { name: 'Groups.Topics.Name', type: 'string', versions: '8-9' },
    { name: 'Groups.Topics.Partitions', type: '[]OffsetFetchResponsePartitions', versions: '8+' },
    { name: 'Groups.Topics.Partitions.PartitionIndex', type: 'int32', versions: '8+' },
    { name: 'Groups.Topics.Partitions.CommittedOffset', type: 'int64', versions: '8+' },
    { name: 'Groups.Topics.Partitions.CommittedLeaderEpoch', type: 'int32', versions: '8+' },
    { name: 'Groups.Topics.Partitions.Metadata', type: 'string', versions: '8+', nullable: true },
    { name: 'Groups.Topics.Partitions.ErrorCode', type: 'int16', versions: '8+' },
    { name: 'Groups.ErrorCode', type: 'int16', versions: '8+' },
  ],
}
