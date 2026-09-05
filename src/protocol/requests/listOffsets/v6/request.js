const Encoder = require('../../../encoder')
const { ListOffsets: apiKey } = require('../../apiKeys')

const UNKNOWN_LEADER_EPOCH = -1

/**
 * ListOffsets Request (Version: 6) — flexible (KIP-482), request header v2
 *
 * ReplicaId => INT32
 * IsolationLevel => INT8
 * Topics => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY
 *     PartitionIndex => INT32
 *     CurrentLeaderEpoch => INT32 (default -1)
 *     Timestamp => INT64
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v6 is the flexible re-encoding of v5: compact strings/arrays and a tagged-field buffer at
 * every struct, no new fields.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
 * @see clients/src/main/resources/common/message/ListOffsetsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {number} options.replicaId
 * @param {number} options.isolationLevel
 * @param {{topic: string, partitions: {partition: number, timestamp: number|string, currentLeaderEpoch?: number}[]}[]} options.topics
 */
module.exports = ({ replicaId, isolationLevel, topics }) => ({
  apiKey,
  apiVersion: 6,
  apiName: 'ListOffsets',
  encode: async () => {
    return new Encoder()
      .writeInt32(replicaId)
      .writeInt8(isolationLevel)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeTaggedFields()
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()
}

const encodePartition = ({
  partition,
  timestamp = -1,
  currentLeaderEpoch = UNKNOWN_LEADER_EPOCH,
}) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt32(currentLeaderEpoch)
    .writeInt64(timestamp)
    .writeTaggedFields()
}

module.exports.schema = {
  api: 'ListOffsets',
  version: 6,
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
