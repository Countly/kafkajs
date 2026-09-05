const requestV6 = require('../v6/request')

/**
 * ListOffsets Request (Version: 8) — flexible (KIP-482), request header v2
 *
 * v8 enables listing offsets by local log start offset (KIP-405, tiered storage); this client
 * never sends that sentinel, so the wire shape is v6's.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
 * @see clients/src/main/resources/common/message/ListOffsetsRequest.json (Apache Kafka 4.3.1)
 */
module.exports = ({ replicaId, isolationLevel, topics }) =>
  Object.assign(requestV6({ replicaId, isolationLevel, topics }), { apiVersion: 8 })

module.exports.schema = {
  api: 'ListOffsets',
  version: 8,
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
