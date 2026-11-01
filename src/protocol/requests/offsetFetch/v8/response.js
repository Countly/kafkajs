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
 * v8 wraps the response in a `Groups` list (KIP-709). An explicit groups binding preserves
 * that list and checks requested identities. The legacy form still requests one group,
 * so exactly one is expected back; the single group is flattened into the shape every earlier
 * version returned (`responses[].topic`/`.partitions[]`, top-level `errorCode` taken from the
 * group's own error code) rather than growing a new `groups[]` shape for callers to learn.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-709%3A+Extend+OffsetFetch+requests+to+accept+multiple+group+ids
 * @see clients/src/main/resources/common/message/OffsetFetchResponse.json (Apache Kafka 4.3.1)
 * @param {Buffer} rawData response body
 * @param {object} [options] groups binding; omitted preserves the single-group shape
 * @returns {Promise<object>} correlated group results or the legacy flattened response
 * @throws {KafkaJSInvariantViolation} duplicate, missing or unrequested response identities
 */

const decode = async (rawData, options = {}) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const groups = Decoder.requireArray(
    decoder.readUVarIntArray(decodeGroup),
    'OffsetFetchResponse.Groups'
  )
  decoder.readTaggedFields()

  if (options.groups !== undefined) {
    const { exact, validateTopics } = require('../responseIdentity')
    exact(groups, options.groups, 'groupId', 'OffsetFetch group')
    const requestedGroups = new Map(options.groups.map((row) => [row.groupId, row]))
    for (const group of groups) validateTopics(group, requestedGroups.get(group.groupId).topics)
    return { throttleTime: 0, clientSideThrottleTime: throttleTime, groups }
  }

  if (groups.length !== 1) {
    throw new KafkaJSInvariantViolation(
      `Expected exactly one group in an OffsetFetch v8 response, received ${groups.length}`
    )
  }

  const [group] = groups
  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    groupId: group.groupId,
    errorCode: group.errorCode,
    responses: group.topics,
  }
}

const decodeGroup = (decoder) => {
  const group = {
    groupId: decoder.readUVarIntString(),
    topics: Decoder.requireArray(
      decoder.readUVarIntArray(decodeTopic),
      'OffsetFetchResponse.Groups.Topics'
    ),
    errorCode: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return group
}

const decodeTopic = (decoder) => {
  const topic = {
    topic: decoder.readUVarIntString(),
    partitions: Decoder.requireArray(
      decoder.readUVarIntArray(decodePartition),
      'OffsetFetchResponse.Groups.Topics.Partitions'
    ),
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
