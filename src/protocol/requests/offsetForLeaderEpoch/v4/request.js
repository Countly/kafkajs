const Encoder = require('../../../encoder')
const { OffsetForLeaderEpoch: apiKey } = require('../../apiKeys')

/** See v3/request.js: consumers identify themselves with -1, and the field is always written. */
const CONSUMER_REPLICA_ID = -1

/**
 * OffsetForLeaderEpochRequest (Version: 4) - flexible (KIP-482), request header v2
 *
 * ReplicaId => INT32
 * Topics => COMPACT_ARRAY
 *   Topic => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY
 *     Partition => INT32
 *     CurrentLeaderEpoch => INT32
 *     LeaderEpoch => INT32
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v4 re-encodes v3's fields as compact with trailing tag buffers; no field is added or removed.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetForLeaderEpochRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{topic: string, partitions: {partition: number, currentLeaderEpoch?: number, leaderEpoch: number}[]}[]} options.topics
 */
module.exports = ({ topics }) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'OffsetForLeaderEpoch',
  encode: async () =>
    new Encoder()
      .writeInt32(CONSUMER_REPLICA_ID)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeTaggedFields(),
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

const encodePartition = ({ partition, currentLeaderEpoch = -1, leaderEpoch }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt32(currentLeaderEpoch)
    .writeInt32(leaderEpoch)
    .writeTaggedFields()

module.exports.schema = {
  api: 'OffsetForLeaderEpoch',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'ReplicaId', type: 'int32', versions: '3+' },
    { name: 'Topics', type: '[]OffsetForLeaderTopic', versions: '0+' },
    { name: 'Topics.Topic', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]OffsetForLeaderPartition', versions: '0+' },
    { name: 'Topics.Partitions.Partition', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CurrentLeaderEpoch', type: 'int32', versions: '2+' },
    { name: 'Topics.Partitions.LeaderEpoch', type: 'int32', versions: '0+' },
  ],
}
