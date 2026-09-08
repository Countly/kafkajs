const Decoder = require('../../../decoder')

/**
 * @param {Decoder} decoder
 * @returns {{ topicId: string|null, topicName: string|null, partitions: number[] }} one assigned
 *   topic, carried both by id and by name so a caller never has to resolve one from the other
 */
const decodeTopicPartitions = (decoder) => {
  const entry = {
    topicId: decoder.readUuid(),
    topicName: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray((partition) => partition.readInt32()),
  }

  decoder.readTaggedFields()
  return entry
}

/**
 * `Assignment` is a plain struct here, NOT a nullable one - unlike ConsumerGroupHeartbeat's - so
 * it carries no presence marker and an unassigned member simply has an empty topic list.
 * @see clients/src/main/resources/common/message/ConsumerGroupDescribeResponse.json (Apache Kafka 4.3.1)
 * @param {Decoder} decoder
 * @returns {{ topicPartitions: object[] }}
 */
const decodeAssignment = (decoder) => {
  const assignment = { topicPartitions: decoder.readUVarIntArray(decodeTopicPartitions) }
  decoder.readTaggedFields()
  return assignment
}

/**
 * @param {number} version 0 or 1
 * @returns {(decoder: Decoder) => object} a reader for one `Member` struct at that version
 */
const decodeMember = (version) => (decoder) => {
  const member = {
    memberId: decoder.readUVarIntString(),
    instanceId: decoder.readUVarIntString(),
    rackId: decoder.readUVarIntString(),
    memberEpoch: decoder.readInt32(),
    clientId: decoder.readUVarIntString(),
    clientHost: decoder.readUVarIntString(),
    subscribedTopicNames: decoder.readUVarIntArray((d) => d.readUVarIntString()),
    subscribedTopicRegex: decoder.readUVarIntString(),
    assignment: decodeAssignment(decoder),
    targetAssignment: decodeAssignment(decoder),
    // Raw as delivered: -1 unknown, 0 classic, 1 consumer. Null at v0, where the field does not
    // exist, so "the broker did not say" and "the broker said unknown" stay distinguishable.
    memberType: version >= 1 ? decoder.readInt8() : null,
  }

  decoder.readTaggedFields()
  return member
}

/**
 * @param {number} version 0 or 1
 * @returns {(decoder: Decoder) => object} a reader for one `DescribedGroup` struct at that version
 */
const decodeGroup = (version) => (decoder) => {
  const group = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    groupId: decoder.readUVarIntString(),
    groupState: decoder.readUVarIntString(),
    groupEpoch: decoder.readInt32(),
    assignmentEpoch: decoder.readInt32(),
    assignorName: decoder.readUVarIntString(),
    members: decoder.readUVarIntArray(decodeMember(version)),
    authorizedOperations: decoder.readInt32(),
  }

  decoder.readTaggedFields()
  return group
}

/**
 * ConsumerGroupDescribeResponse (Versions: 0-1) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * Groups => COMPACT_ARRAY
 *   ErrorCode => INT16
 *   ErrorMessage => COMPACT_NULLABLE_STRING
 *   GroupId => COMPACT_STRING
 *   GroupState => COMPACT_STRING
 *   GroupEpoch => INT32
 *   AssignmentEpoch => INT32
 *   AssignorName => COMPACT_STRING
 *   Members => COMPACT_ARRAY
 *     MemberId, InstanceId?, RackId?, MemberEpoch, ClientId, ClientHost,
 *     SubscribedTopicNames, SubscribedTopicRegex?, Assignment, TargetAssignment, MemberType (1+)
 *   AuthorizedOperations => INT32
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * `Assignment` and `TargetAssignment` here are plain structs, NOT nullable ones - unlike
 * ConsumerGroupHeartbeat's - so neither carries a presence marker.
 * @see clients/src/main/resources/common/message/ConsumerGroupDescribeResponse.json (Apache Kafka 4.3.1)
 *
 * @param {number} version 0 or 1; v1 adds MemberType (KIP-1099)
 * @returns {(rawData: Buffer) => Promise<object>} the decoder for that version; `throttleTime` is
 *   pinned at 0 and the broker's value is reported as `clientSideThrottleTime` (KIP-219), as
 *   every quota-aware codec here does
 */
const buildDecode = (version) => async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const groups = decoder.readUVarIntArray(decodeGroup(version))

  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, groups }
}

/**
 * Deliberately does NOT throw on a group-level error code.
 *
 * `GROUP_ID_NOT_FOUND` on this API means "not a consumer-protocol group, ask DescribeGroups",
 * and `UNSUPPORTED_VERSION` means "this broker cannot answer it" - both are routing signals the
 * caller acts on per group id, and a throw here would make either unreachable. Authorization and
 * coordinator errors are likewise reported per group; the caller's table decides.
 *
 * @param {object} data decoded response
 * @returns {Promise<object>} the response, unchanged
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/internals/DescribeConsumerGroupsHandler.java
 */
const parse = async (data) => data

module.exports = { buildDecode, decode: buildDecode(0), parse }

module.exports.schema = {
  api: 'ConsumerGroupDescribe',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Groups', type: '[]DescribedGroup', versions: '0+' },
    { name: 'Groups.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Groups.ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Groups.GroupId', type: 'string', versions: '0+' },
    { name: 'Groups.GroupState', type: 'string', versions: '0+' },
    { name: 'Groups.GroupEpoch', type: 'int32', versions: '0+' },
    { name: 'Groups.AssignmentEpoch', type: 'int32', versions: '0+' },
    { name: 'Groups.AssignorName', type: 'string', versions: '0+' },
    { name: 'Groups.Members', type: '[]Member', versions: '0+' },
    { name: 'Groups.Members.MemberId', type: 'string', versions: '0+' },
    { name: 'Groups.Members.InstanceId', type: 'string', versions: '0+', nullable: true },
    { name: 'Groups.Members.RackId', type: 'string', versions: '0+', nullable: true },
    { name: 'Groups.Members.MemberEpoch', type: 'int32', versions: '0+' },
    { name: 'Groups.Members.ClientId', type: 'string', versions: '0+' },
    { name: 'Groups.Members.ClientHost', type: 'string', versions: '0+' },
    { name: 'Groups.Members.SubscribedTopicNames', type: '[]string', versions: '0+' },
    {
      name: 'Groups.Members.SubscribedTopicRegex',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    { name: 'Groups.Members.Assignment', type: 'Assignment', versions: '0+' },
    {
      name: 'Groups.Members.Assignment.TopicPartitions',
      type: '[]TopicPartitions',
      versions: '0+',
    },
    {
      name: 'Groups.Members.Assignment.TopicPartitions.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'Groups.Members.Assignment.TopicPartitions.TopicName',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Members.Assignment.TopicPartitions.Partitions',
      type: '[]int32',
      versions: '0+',
    },
    { name: 'Groups.Members.TargetAssignment', type: 'Assignment', versions: '0+' },
    {
      name: 'Groups.Members.TargetAssignment.TopicPartitions',
      type: '[]TopicPartitions',
      versions: '0+',
    },
    {
      name: 'Groups.Members.TargetAssignment.TopicPartitions.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'Groups.Members.TargetAssignment.TopicPartitions.TopicName',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Members.TargetAssignment.TopicPartitions.Partitions',
      type: '[]int32',
      versions: '0+',
    },
    { name: 'Groups.AuthorizedOperations', type: 'int32', versions: '0+' },
  ],
}
