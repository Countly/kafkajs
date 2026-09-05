const Encoder = require('../../../encoder')
const { OffsetFetch: apiKey } = require('../../apiKeys')

/**
 * OffsetFetch Request (Version: 6) — flexible (KIP-482), request header v2
 *
 * GroupId => COMPACT_STRING
 * Topics => COMPACT_ARRAY (nullable)
 *   Name => COMPACT_STRING
 *   PartitionIndexes => COMPACT_ARRAY of INT32
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v6 re-encodes v5's fields as compact with trailing tag buffers; nothing else changes. An
 * empty topics list is encoded as the null compact array, which the broker reads as "every
 * topic" — the behaviour of every earlier version of this codec.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetFetchRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {{topic: string, partitions: {partition: number}[]}[]} options.topics
 */
module.exports = ({ groupId, topics }) => ({
  apiKey,
  apiVersion: 6,
  apiName: 'OffsetFetch',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(groupId)
      .writeUVarIntArray(topics == null || topics.length === 0 ? null : topics.map(encodeTopic))
      .writeTaggedFields(),
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

const encodePartition = ({ partition }) => new Encoder().writeInt32(partition)

module.exports.schema = {
  api: 'OffsetFetch',
  version: 6,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0-7' },
    { name: 'Topics', type: '[]OffsetFetchRequestTopic', versions: '0-7', nullable: true },
    { name: 'Topics.Name', type: 'string', versions: '0-7' },
    { name: 'Topics.PartitionIndexes', type: '[]int32', versions: '0-7' },
  ],
}
