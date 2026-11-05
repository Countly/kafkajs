const Encoder = require('../../../encoder')
const { TxnOffsetCommit: apiKey } = require('../../apiKeys')

/**
 * TxnOffsetCommit Request (Version: 2) => transactional_id group_id producer_id producer_epoch [topics]
 *   transactional_id => STRING
 *   group_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index committed_offset committed_leader_epoch committed_metadata
 *       partition_index => INT32
 *       committed_offset => INT64
 *       committed_leader_epoch => INT32
 *       committed_metadata => NULLABLE_STRING
 *
 * v2 adds the leader epoch of the record whose offset is committed (KIP-320); -1, the field's
 * default, when the caller has none. The group metadata arguments are accepted and not sent: they
 * exist from v3. The Java builder throws UnsupportedVersionException when it is set below v3
 * (TxnOffsetCommitRequest.Builder#build); this one drops it, which no Kafka 4.0+ broker can
 * trigger because every one offers v3 and above.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitRequest.json (Apache Kafka 4.3.1)
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/requests/TxnOffsetCommitRequest.java
 *
 * @param {object} options
 * @param {string} options.transactionalId
 * @param {string} options.groupId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {{ topic: string, partitions: { partition: number|string, offset: string|number,
 *   leaderEpoch?: number|null, metadata?: string|null }[] }[]} options.topics
 * @returns {object} the protocol request
 */
module.exports = ({ transactionalId, groupId, producerId, producerEpoch, topics }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'TxnOffsetCommit',
  encode: async () =>
    new Encoder()
      .writeString(transactionalId)
      .writeString(groupId)
      .writeInt64(producerId)
      .writeInt16(producerEpoch)
      .writeArray(topics.map(encodeTopic)),
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))

const encodePartition = ({ partition, offset, leaderEpoch, metadata = null }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(leaderEpoch ?? -1)
    .writeString(metadata)

module.exports.schema = {
  api: 'TxnOffsetCommit',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
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
