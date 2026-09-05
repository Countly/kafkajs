const Encoder = require('../../../encoder')
const { ListOffsets: apiKey } = require('../../apiKeys')

/**
 * The epoch the client believes is currently leading a partition, from
 * `MetadataCache#leaderEpoch` via `Cluster#fetchTopicsOffset`'s `addDefaultOffset`. Sent as the
 * field's own default when the cache has no cached epoch for the partition (including when a
 * caller builds a request directly, bypassing the cluster).
 * @see clients/src/main/resources/common/message/ListOffsetsRequest.json (Apache Kafka 4.3.1)
 */
const UNKNOWN_LEADER_EPOCH = -1

/**
 * ListOffsets Request (Version: 4) => replica_id isolation_level [topics]
 *   replica_id => INT32
 *   isolation_level => INT8
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition current_leader_epoch timestamp
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       timestamp => INT64
 *
 * v4 adds CurrentLeaderEpoch between PartitionIndex and Timestamp, so a broker can fence a
 * request built on metadata that has since moved and reply with FENCED_LEADER_EPOCH instead of
 * a wrong offset.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation
 * @see clients/src/main/resources/common/message/ListOffsetsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {number} options.replicaId
 * @param {number} options.isolationLevel
 * @param {{topic: string, partitions: {partition: number, timestamp: number|string, currentLeaderEpoch?: number}[]}[]} options.topics
 */
module.exports = ({ replicaId, isolationLevel, topics }) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'ListOffsets',
  encode: async () => {
    return new Encoder()
      .writeInt32(replicaId)
      .writeInt8(isolationLevel)
      .writeArray(topics.map(encodeTopic))
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))
}

const encodePartition = ({
  partition,
  timestamp = -1,
  currentLeaderEpoch = UNKNOWN_LEADER_EPOCH,
}) => {
  return new Encoder().writeInt32(partition).writeInt32(currentLeaderEpoch).writeInt64(timestamp)
}

module.exports.schema = {
  api: 'ListOffsets',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'ReplicaId', type: 'int32', versions: '0+' },
    { name: 'IsolationLevel', type: 'int8', versions: '2+' },
    { name: 'Topics', type: '[]ListOffsetsTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]ListOffsetsPartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CurrentLeaderEpoch', type: 'int32', versions: '4+' },
    { name: 'Topics.Partitions.Timestamp', type: 'int64', versions: '0+' },
  ],
}
