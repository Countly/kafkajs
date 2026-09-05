const requestV4 = require('../v4/request')

/**
 * ListOffsets Request (Version: 5) => replica_id isolation_level [topics]
 *   replica_id => INT32
 *   isolation_level => INT8
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition current_leader_epoch timestamp
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       timestamp => INT64
 *
 * v5 adds no fields; the wire shape is byte-identical to v4.
 * @see clients/src/main/resources/common/message/ListOffsetsRequest.json (Apache Kafka 4.3.1)
 */
module.exports = ({ replicaId, isolationLevel, topics }) =>
  Object.assign(requestV4({ replicaId, isolationLevel, topics }), { apiVersion: 5 })

module.exports.schema = {
  api: 'ListOffsets',
  version: 5,
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
