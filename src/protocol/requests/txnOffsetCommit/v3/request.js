const Encoder = require('../../../encoder')
const { TxnOffsetCommit: apiKey } = require('../../apiKeys')

/**
 * TxnOffsetCommit Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * TransactionalId => COMPACT_STRING
 * GroupId => COMPACT_STRING
 * ProducerId => INT64
 * ProducerEpoch => INT16
 * GenerationId => INT32
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
 * v3 adds the consumer's group metadata (KIP-447), so the group coordinator fences a commit
 * from a member that is no longer in the group's current generation. The defaults -1, '' and
 * null are the fields' own and commit without that fencing, as v0-v2 do.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitRequest.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
 *
 * @param {object} options
 * @param {string} options.transactionalId
 * @param {string} options.groupId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {number} [options.generationId=-1] the generation, or the member epoch under the
 *   consumer group protocol
 * @param {string} [options.memberId='']
 * @param {string|null} [options.groupInstanceId=null]
 * @param {{ topic: string, partitions: { partition: number|string, offset: string|number,
 *   leaderEpoch?: number|null, metadata?: string|null }[] }[]} options.topics
 * @returns {object} the protocol request
 */
module.exports = ({
  transactionalId,
  groupId,
  producerId,
  producerEpoch,
  generationId = -1,
  memberId = '',
  groupInstanceId = null,
  topics,
}) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'TxnOffsetCommit',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(transactionalId)
      .writeUVarIntString(groupId)
      .writeInt64(producerId)
      .writeInt16(producerEpoch)
      .writeInt32(generationId)
      .writeUVarIntString(memberId)
      .writeUVarIntString(groupInstanceId)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeTaggedFields(),
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

const encodePartition = ({ partition, offset, leaderEpoch, metadata = null }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(leaderEpoch ?? -1)
    .writeUVarIntString(metadata)
    .writeTaggedFields()

module.exports.schema = {
  api: 'TxnOffsetCommit',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'GenerationIdOrMemberEpoch', type: 'int32', versions: '3+' },
    { name: 'MemberId', type: 'string', versions: '3+' },
    { name: 'GroupInstanceId', type: 'string', versions: '3+', nullable: true },
    { name: 'Topics', type: '[]TxnOffsetCommitRequestTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0-5' },
    { name: 'Topics.Partitions', type: '[]TxnOffsetCommitRequestPartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CommittedOffset', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.CommittedLeaderEpoch', type: 'int32', versions: '2+' },
    {
      name: 'Topics.Partitions.CommittedMetadata',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
  ],
}
