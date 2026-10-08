const Encoder = require('../../../encoder')
const { OffsetFetch: apiKey } = require('../../apiKeys')

/**
 * OffsetFetch Request (Version: 8) — flexible (KIP-482), request header v2
 *
 * Groups => COMPACT_ARRAY
 *   GroupId => COMPACT_STRING
 *   Topics => COMPACT_ARRAY (nullable)
 *     Name => COMPACT_STRING
 *     PartitionIndexes => COMPACT_ARRAY of INT32
 *     TAG_BUFFER
 *   TAG_BUFFER
 * RequireStable => BOOLEAN
 * TAG_BUFFER
 *
 * v8 replaces the single `GroupId`/`Topics` pair with a `Groups` list (KIP-709, batched
 * OffsetFetch). This client still asks for exactly one group per request — the caller-facing
 * shape is unchanged — so `groupId`/`topics` are wrapped into a one-element `Groups` array here.
 * An empty or absent topics list is encoded as the null compact array, which the broker reads as
 * "every topic for this group" — the behaviour of every earlier version of this codec.
 * `RequireStable` stays request-level and means what it means at v7.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-709%3A+Extend+OffsetFetch+requests+to+accept+multiple+group+ids
 * @see clients/src/main/resources/common/message/OffsetFetchRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {{topic: string, partitions: {partition: number}[]}[]} options.topics
 * @param {boolean} [options.requireStable=false] ask for stable offsets only (KIP-447, see v7)
 * @returns {object} the protocol request
 */
module.exports = ({ groupId, topics, requireStable = false }) => ({
  apiKey,
  apiVersion: 8,
  apiName: 'OffsetFetch',
  encode: async () =>
    new Encoder()
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString(groupId)
          .writeUVarIntArray(topics == null || topics.length === 0 ? null : topics.map(encodeTopic))
          .writeTaggedFields(),
      ])
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
  version: 8,
  direction: 'request',
  fields: [
    { name: 'Groups', type: '[]OffsetFetchRequestGroup', versions: '8+' },
    { name: 'Groups.GroupId', type: 'string', versions: '8+' },
    { name: 'Groups.Topics', type: '[]OffsetFetchRequestTopics', versions: '8+', nullable: true },
    { name: 'Groups.Topics.Name', type: 'string', versions: '8-9' },
    { name: 'Groups.Topics.PartitionIndexes', type: '[]int32', versions: '8+' },
    { name: 'RequireStable', type: 'bool', versions: '7+' },
  ],
}
