const Encoder = require('../../../encoder')
const { OffsetCommit: apiKey } = require('../../apiKeys')

/**
 * The leader epoch of the *consumed record* whose offset is being committed. See v6/request.js
 * for why this client always sends the field's own default.
 * @see clients/src/main/resources/common/message/OffsetCommitRequest.json (Apache Kafka 4.3.1)
 */
const COMMITTED_LEADER_EPOCH = -1

/**
 * OffsetCommitRequest (Version: 8) — flexible (KIP-482), request header v2
 *
 * GroupId => COMPACT_STRING
 * GenerationIdOrMemberEpoch => INT32
 * MemberId => COMPACT_STRING
 * GroupInstanceId => COMPACT_NULLABLE_STRING
 * Topics => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY
 *     PartitionIndex => INT32
 *     CommittedOffset => INT64
 *     CommittedLeaderEpoch => INT32
 *     CommittedMetadata => COMPACT_NULLABLE_STRING
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v8 re-encodes v7's fields as compact with trailing tag buffers; nothing else changes.
 * @see https://kafka.apache.org/43/design/protocol
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
  apiVersion: 8,
  apiName: 'OffsetCommit',
  encode: async () => {
    return new Encoder()
      .writeUVarIntString(groupId)
      .writeInt32(groupGenerationId)
      .writeUVarIntString(memberId)
      .writeUVarIntString(groupInstanceId)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeTaggedFields()
  },
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

const encodePartition = ({ partition, offset, metadata = null }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(COMMITTED_LEADER_EPOCH)
    .writeUVarIntString(metadata)
    .writeTaggedFields()

module.exports.schema = {
  api: 'OffsetCommit',
  version: 8,
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
