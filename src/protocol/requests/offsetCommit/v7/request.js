const Encoder = require('../../../encoder')
const { OffsetCommit: apiKey } = require('../../apiKeys')

/**
 * The leader epoch of the *consumed record* whose offset is being committed. See v6/request.js
 * for why this client always sends the field's own default.
 * @see clients/src/main/resources/common/message/OffsetCommitRequest.json (Apache Kafka 4.3.1)
 */
const COMMITTED_LEADER_EPOCH = -1

/**
 * OffsetCommitRequest (Version: 7) => group_id generation_id member_id group_instance_id [topics]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   group_instance_id => NULLABLE_STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset committed_leader_epoch metadata
 *       partition => INT32
 *       offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => NULLABLE_STRING
 *
 * v7 adds GroupInstanceId (KIP-345 static membership), completing the plumbing started for
 * JoinGroup/SyncGroup/Heartbeat/LeaveGroup: a consumer configured with `groupInstanceId` now
 * sends it on offset commits too.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-345
 * @see clients/src/main/resources/common/message/OffsetCommitRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {number} options.groupGenerationId
 * @param {string} options.memberId
 * @param {string} [options.groupInstanceId=null] Static membership id (KIP-345)
 * @param {{topic: string, partitions: {partition: number, offset: string, metadata?: string}[]}[]} options.topics
 */
module.exports = ({ groupId, groupGenerationId, memberId, groupInstanceId = null, topics }) => ({
  apiKey,
  apiVersion: 7,
  apiName: 'OffsetCommit',
  encode: async () => {
    return new Encoder()
      .writeString(groupId)
      .writeInt32(groupGenerationId)
      .writeString(memberId)
      .writeString(groupInstanceId)
      .writeArray(topics.map(encodeTopic))
  },
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))

const encodePartition = ({ partition, offset, metadata = null }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(COMMITTED_LEADER_EPOCH)
    .writeString(metadata)

module.exports.schema = {
  api: 'OffsetCommit',
  version: 7,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'GenerationIdOrMemberEpoch', type: 'int32', versions: '1+' },
    { name: 'MemberId', type: 'string', versions: '1+' },
    { name: 'GroupInstanceId', type: 'string', versions: '7+', nullable: true },
    { name: 'Topics', type: '[]OffsetCommitRequestTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0-9' },
    { name: 'Topics.Partitions', type: '[]OffsetCommitRequestPartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CommittedOffset', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.CommittedLeaderEpoch', type: 'int32', versions: '6+' },
    { name: 'Topics.Partitions.CommittedMetadata', type: 'string', versions: '0+', nullable: true },
  ],
}
