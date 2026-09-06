const Encoder = require('../../../encoder')
const { OffsetForLeaderEpoch: apiKey } = require('../../apiKeys')

/**
 * Consumers identify themselves with -1. The message definition's own default is -2, which means
 * "debugging consumer"; the Java consumer sends -1, and so does this client, always writing the
 * field even though it is `ignorable`.
 * @see clients/src/main/resources/common/message/OffsetForLeaderEpochRequest.json (Apache Kafka 4.3.1)
 */
const CONSUMER_REPLICA_ID = -1

/**
 * OffsetForLeaderEpochRequest (Version: 3)
 *
 * ReplicaId => INT32
 * Topics => ARRAY
 *   Topic => STRING
 *   Partitions => ARRAY
 *     Partition => INT32
 *     CurrentLeaderEpoch => INT32
 *     LeaderEpoch => INT32
 *
 * v3 is the lowest version a consumer may send: earlier versions require CLUSTER permission,
 * which clients are not typically granted, while v3 requires only TOPIC Describe. `Topic` is a
 * `mapKey`, so each topic appears exactly once per request.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation
 *
 * @param {object} options
 * @param {{topic: string, partitions: {partition: number, currentLeaderEpoch?: number, leaderEpoch: number}[]}[]} options.topics
 */
module.exports = ({ topics }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'OffsetForLeaderEpoch',
  encode: async () =>
    new Encoder().writeInt32(CONSUMER_REPLICA_ID).writeArray(topics.map(encodeTopic)),
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))

const encodePartition = ({ partition, currentLeaderEpoch = -1, leaderEpoch }) =>
  new Encoder().writeInt32(partition).writeInt32(currentLeaderEpoch).writeInt32(leaderEpoch)

module.exports.schema = {
  api: 'OffsetForLeaderEpoch',
  version: 3,
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
