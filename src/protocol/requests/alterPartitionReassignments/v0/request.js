const Encoder = require('../../../encoder')
const { AlterPartitionReassignments: apiKey } = require('../../apiKeys')

/**
 * AlterPartitionReassignmentsRequest (Version: 0) — flexible (KIP-482), request header v2
 *
 * TimeoutMs => INT32 (0+)
 * Topics => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   Partitions => COMPACT_ARRAY (0+)
 *     PartitionIndex => INT32 (0+)
 *     Replicas => COMPACT_ARRAY of INT32 (0+, nullable)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/AlterPartitionReassignmentsRequest.json
 *
 * @param {object} options
 * @param {{ topic: string, partitionAssignment: { partition: number, replicas: number[] }[] }[]} options.topics
 * @param {number} [options.timeout=5000]
 */
module.exports = ({ topics, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'AlterPartitionReassignments',
  encode: async () => {
    return new Encoder()
      .writeInt32(timeout)
      .writeUVarIntArray(topics.map(encodeTopics))
      .writeTaggedFields()
  },
})

const encodeTopics = ({ topic, partitionAssignment }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitionAssignment.map(encodePartitionAssignment))
    .writeTaggedFields()
}

const encodePartitionAssignment = ({ partition, replicas }) => {
  return new Encoder()
    .writeInt32(partition)
    .writeUVarIntArray(replicas.map(encodeReplicas))
    .writeTaggedFields()
}

const encodeReplicas = (replica) => {
  return new Encoder().writeInt32(replica)
}

module.exports.schema = {
  api: 'AlterPartitionReassignments',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
    { name: 'Topics', type: '[]ReassignableTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]ReassignablePartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.Replicas', type: '[]int32', versions: '0+', nullable: true },
  ],
}
