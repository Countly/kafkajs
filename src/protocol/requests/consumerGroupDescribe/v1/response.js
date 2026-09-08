const { buildDecode, parse } = require('../v0/response')

/**
 * ConsumerGroupDescribeResponse (Version: 1) — flexible (KIP-482), response header v1
 *
 * v1 adds exactly one field, `MemberType` (INT8, default -1), at the end of every `Member`
 * struct: -1 unknown, 0 a member still speaking the classic protocol, 1 a consumer-protocol
 * member. Everything else is v0's body, so this shares v0's reader at version 1 (KIP-1099).
 * @see clients/src/main/resources/common/message/ConsumerGroupDescribeResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(1), parse }

module.exports.schema = {
  api: 'ConsumerGroupDescribe',
  version: 1,
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
    { name: 'Groups.Members.MemberType', type: 'int8', versions: '1+' },
    { name: 'Groups.AuthorizedOperations', type: 'int32', versions: '0+' },
  ],
}
