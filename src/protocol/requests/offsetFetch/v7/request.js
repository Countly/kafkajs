const Encoder = require('../../../encoder')
const { OffsetFetch: apiKey } = require('../../apiKeys')

/**
 * OffsetFetch Request (Version: 7) — flexible (KIP-482), request header v2
 *
 * GroupId => COMPACT_STRING
 * Topics => COMPACT_ARRAY (nullable)
 *   Name => COMPACT_STRING
 *   PartitionIndexes => COMPACT_ARRAY of INT32
 *   TAG_BUFFER
 * RequireStable => BOOLEAN
 * TAG_BUFFER
 *
 * v7 adds `RequireStable` (KIP-447): when true, the coordinator holds back the offset of a
 * partition that has a transactional offset commit still pending and answers that partition with
 * the retriable UNSTABLE_OFFSET_COMMIT instead. Left out, it is the field's own default, false.
 * @see https://kafka.apache.org/43/design/protocol
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
 * @see clients/src/main/resources/common/message/OffsetFetchRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {{topic: string, partitions: {partition: number}[]}[]} options.topics
 * @param {boolean} [options.requireStable=false] ask for stable offsets only
 * @returns {object} the protocol request
 */
module.exports = ({ groupId, topics, requireStable = false }) => ({
  apiKey,
  apiVersion: 7,
  apiName: 'OffsetFetch',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(groupId)
      .writeUVarIntArray(topics == null || topics.length === 0 ? null : topics.map(encodeTopic))
      .writeBoolean(requireStable)
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
  version: 7,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0-7' },
    { name: 'Topics', type: '[]OffsetFetchRequestTopic', versions: '0-7', nullable: true },
    { name: 'Topics.Name', type: 'string', versions: '0-7' },
    { name: 'Topics.PartitionIndexes', type: '[]int32', versions: '0-7' },
    { name: 'RequireStable', type: 'bool', versions: '7+' },
  ],
}
