const Encoder = require('../../../encoder')
const { DeleteRecords: apiKey } = require('../../apiKeys')

/**
 * DeleteRecords Request (Version: 2) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   Partitions => COMPACT_ARRAY (0+)
 *     PartitionIndex => INT32 (0+)
 *     Offset => INT64 (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TimeoutMs => INT32 (0+)
 * TAG_BUFFER
 *
 * The v1 fields in compact framing. An offset of -1 truncates the partition to its high watermark.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteRecordsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ topic: string, partitions: { partition: number, offset: string|number|bigint }[] }[]} options.topics
 * @param {number} [options.timeout=5000]
 * @returns {object} the protocol request
 */
module.exports = ({ topics, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'DeleteRecords',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeInt32(timeout)
      .writeTaggedFields()
  },
})

const encodeTopic = ({ topic, partitions }) => {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()
}

const encodePartition = ({ partition, offset }) => {
  return new Encoder().writeInt32(partition).writeInt64(offset).writeTaggedFields()
}

module.exports.schema = {
  api: 'DeleteRecords',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]DeleteRecordsTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]DeleteRecordsPartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.Offset', type: 'int64', versions: '0+' },
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
  ],
}
