const Encoder = require('../../../encoder')
const { OffsetCommit: apiKey } = require('../../apiKeys')

/**
 * The leader epoch of the *consumed record* whose offset is being committed. This client does
 * not track it yet - the fetch path it would come from has not moved - so the field's own
 * default is sent. Sending the current metadata epoch instead would be a different quantity and
 * would mislead a broker performing log-truncation checks.
 * @see clients/src/main/resources/common/message/OffsetCommitRequest.json (Apache Kafka 4.3.1)
 */
const COMMITTED_LEADER_EPOCH = -1

/**
 * OffsetCommitRequest (Version: 6) => group_id generation_id member_id [topics]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset committed_leader_epoch metadata
 *       partition => INT32
 *       offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => NULLABLE_STRING
 *
 * v6 adds CommittedLeaderEpoch (for fencing against stale-leader commits); this client always
 * sends the field's own default (see COMMITTED_LEADER_EPOCH above). RetentionTimeMs's range
 * closed at v4 and stays absent.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetCommitRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {number} options.groupGenerationId
 * @param {string} options.memberId
 * @param {{topic: string, partitions: {partition: number, offset: string, metadata?: string}[]}[]} options.topics
 */
module.exports = ({ groupId, groupGenerationId, memberId, topics }) => ({
  apiKey,
  apiVersion: 6,
  apiName: 'OffsetCommit',
  encode: async () => {
    return new Encoder()
      .writeString(groupId)
      .writeInt32(groupGenerationId)
      .writeString(memberId)
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
  version: 6,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'GenerationIdOrMemberEpoch', type: 'int32', versions: '1+' },
    { name: 'MemberId', type: 'string', versions: '1+' },
    { name: 'Topics', type: '[]OffsetCommitRequestTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0-9' },
    { name: 'Topics.Partitions', type: '[]OffsetCommitRequestPartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CommittedOffset', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.CommittedLeaderEpoch', type: 'int32', versions: '6+' },
    { name: 'Topics.Partitions.CommittedMetadata', type: 'string', versions: '0+', nullable: true },
  ],
}
