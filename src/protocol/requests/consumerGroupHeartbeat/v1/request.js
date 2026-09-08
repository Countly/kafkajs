const Encoder = require('../../../encoder')
const { ConsumerGroupHeartbeat: apiKey } = require('../../apiKeys')
const { encodeName, encodeTopicPartitions } = require('../encodeFields')

/**
 * ConsumerGroupHeartbeatRequest (Version: 1) — flexible (KIP-482), request header v2
 *
 * v1 adds `SubscribedTopicRegex` between the topic-name list and the assignor, and is the version
 * on which the consumer generates its own member id (KIP-1082).
 *
 * GroupId => COMPACT_STRING
 * MemberId => COMPACT_STRING
 * MemberEpoch => INT32
 * InstanceId => COMPACT_NULLABLE_STRING
 * RackId => COMPACT_NULLABLE_STRING
 * RebalanceTimeoutMs => INT32
 * SubscribedTopicNames => COMPACT_ARRAY of COMPACT_STRING (nullable)
 * SubscribedTopicRegex => COMPACT_NULLABLE_STRING
 * ServerAssignor => COMPACT_NULLABLE_STRING
 * TopicPartitions => COMPACT_ARRAY (nullable)
 *   TopicId => UUID
 *   Partitions => COMPACT_ARRAY of INT32
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * Every field but the first three carries a "did not change since the last heartbeat" wire
 * default, and this codec's defaults are exactly those. A null `subscribedTopicNames` and an empty
 * one are DIFFERENT messages - null says "unchanged", the empty list says "no names", which is
 * what a regex subscription sends - and neither is coalesced into the other. The same holds for
 * the regex itself: `''` clears it, null leaves it as it was.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ConsumerGroupHeartbeatRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {string} options.memberId client-generated, kept for the process's lifetime (KIP-1082)
 * @param {number} options.memberEpoch 0 to join, -1 to leave, -2 for a static member rejoining
 * @param {string|null} [options.instanceId=null] static membership id, when it changed
 * @param {string|null} [options.rackId=null]
 * @param {number} [options.rebalanceTimeoutMs=-1] -1 when unchanged
 * @param {string[]|null} [options.subscribedTopicNames=null] null when unchanged; `[]` means none
 * @param {string|null} [options.subscribedTopicRegex=null] RE2 source; `''` clears it
 * @param {string|null} [options.serverAssignor=null]
 * @param {{topicId: string, partitions: number[]}[]|null} [options.topicPartitions=null] the
 *   partitions this member currently owns, null when unchanged
 */
module.exports = ({
  groupId,
  memberId,
  memberEpoch,
  instanceId = null,
  rackId = null,
  rebalanceTimeoutMs = -1,
  subscribedTopicNames = null,
  subscribedTopicRegex = null,
  serverAssignor = null,
  topicPartitions = null,
}) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'ConsumerGroupHeartbeat',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(groupId)
      .writeUVarIntString(memberId)
      .writeInt32(memberEpoch)
      .writeUVarIntString(instanceId)
      .writeUVarIntString(rackId)
      .writeInt32(rebalanceTimeoutMs)
      .writeUVarIntArray(subscribedTopicNames == null ? null : subscribedTopicNames.map(encodeName))
      .writeUVarIntString(subscribedTopicRegex)
      .writeUVarIntString(serverAssignor)
      .writeUVarIntArray(
        topicPartitions == null ? null : topicPartitions.map(encodeTopicPartitions)
      )
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'ConsumerGroupHeartbeat',
  version: 1,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'MemberId', type: 'string', versions: '0+' },
    { name: 'MemberEpoch', type: 'int32', versions: '0+' },
    { name: 'InstanceId', type: 'string', versions: '0+', nullable: true },
    { name: 'RackId', type: 'string', versions: '0+', nullable: true },
    { name: 'RebalanceTimeoutMs', type: 'int32', versions: '0+' },
    { name: 'SubscribedTopicNames', type: '[]string', versions: '0+', nullable: true },
    { name: 'SubscribedTopicRegex', type: 'string', versions: '1+', nullable: true },
    { name: 'ServerAssignor', type: 'string', versions: '0+', nullable: true },
    { name: 'TopicPartitions', type: '[]TopicPartitions', versions: '0+', nullable: true },
    { name: 'TopicPartitions.TopicId', type: 'uuid', versions: '0+' },
    { name: 'TopicPartitions.Partitions', type: '[]int32', versions: '0+' },
  ],
}
