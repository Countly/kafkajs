const Encoder = require('../../../encoder')
const { ListPartitionReassignments: apiKey } = require('../../apiKeys')

/**
 * ListPartitionReassignmentsRequest (Version: 0) — flexible (KIP-482), request header v2
 *
 * TimeoutMs => INT32 (0+)
 * Topics => COMPACT_ARRAY (0+, nullable)
 *   Name => COMPACT_STRING (0+)
 *   PartitionIndexes => COMPACT_ARRAY of INT32 (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ListPartitionReassignmentsRequest.json
 *
 * @param {object} options
 * @param {{ topic: string, partitions: number[] }[]|null} [options.topics=null] `null` lists
 *        every partition currently being reassigned
 * @param {number} [options.timeout=5000]
 */
module.exports = ({ topics = null, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ListPartitionReassignments',
  encode: async () => {
    return new Encoder()
      .writeInt32(timeout)
      .writeUVarIntArray(topics === null ? topics : topics.map(encodeTopics))
      .writeTaggedFields()
  },
})

const encodeTopics = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartitions))
    .writeTaggedFields()
}

const encodePartitions = (partition) => {
  return new Encoder().writeInt32(partition)
}

module.exports.schema = {
  api: 'ListPartitionReassignments',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
    { name: 'Topics', type: '[]ListPartitionReassignmentsTopics', versions: '0+', nullable: true },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.PartitionIndexes', type: '[]int32', versions: '0+' },
  ],
}
