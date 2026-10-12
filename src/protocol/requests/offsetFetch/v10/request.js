const Encoder = require('../../../encoder')
const { OffsetFetch: apiKey } = require('../../apiKeys')
const { requireTopicIds, topicIdSchema } = require('../../topicIds')

/**
 * OffsetFetch Request (Version: 10) — flexible (KIP-482), request header v2, topic ids (KIP-848)
 *
 * Groups => COMPACT_ARRAY
 *   GroupId => COMPACT_STRING
 *   MemberId => COMPACT_NULLABLE_STRING
 *   MemberEpoch => INT32
 *   Topics => COMPACT_ARRAY (nullable)
 *     TopicId => UUID
 *     PartitionIndexes => COMPACT_ARRAY of INT32
 *     TAG_BUFFER
 *   TAG_BUFFER
 * RequireStable => BOOLEAN
 * TAG_BUFFER
 *
 * v9's body with `Groups.Topics.Name` replaced by `Groups.Topics.TopicId`. Callers keep naming
 * topics; the ids come from `topicIds`, and a named topic without one fails here, before anything
 * is sent. An empty or absent list asks for every topic and needs no ids.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetFetchRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options as v9, plus `topicIds`
 * @param {string} options.groupId
 * @param {{topic: string, partitions: {partition: number}[]}[]|null} [options.topics]
 * @param {Map<string, string>} [options.topicIds] topic name to topic id, for every topic in `topics`
 * @param {string|null} [options.memberId=null]
 * @param {number} [options.memberEpoch=-1]
 * @param {boolean} [options.requireStable=false] ask for stable offsets only (KIP-447)
 * @returns {object} the protocol request
 * @throws {import('../../../../errors').KafkaJSInvariantViolation} when a named topic has no usable id
 */
module.exports = ({
  groupId,
  topics,
  topicIds,
  memberId = null,
  memberEpoch = -1,
  requireStable = false,
}) => {
  const named = topics == null || topics.length === 0 ? null : topics
  const ids =
    named &&
    requireTopicIds(
      'OffsetFetch',
      named.map(({ topic }) => topic),
      topicIds
    )

  return {
    apiKey,
    apiVersion: 10,
    apiName: 'OffsetFetch',
    encode: async () =>
      new Encoder()
        .writeUVarIntArray([
          new Encoder()
            .writeUVarIntString(groupId)
            .writeUVarIntString(memberId)
            .writeInt32(memberEpoch)
            .writeUVarIntArray(
              named && named.map(({ partitions }, index) => encodeTopic(ids[index], partitions))
            )
            .writeTaggedFields(),
        ])
        .writeBoolean(requireStable)
        .writeTaggedFields(),
  }
}

const encodeTopic = (topicId, partitions) =>
  new Encoder()
    .writeUuid(topicId)
    .writeUVarIntArray(partitions.map(({ partition }) => new Encoder().writeInt32(partition)))
    .writeTaggedFields()

module.exports.schema = topicIdSchema(require('../v9/request').schema)
