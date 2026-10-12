const Encoder = require('../../../encoder')
const { OffsetCommit: apiKey } = require('../../apiKeys')
const { requireTopicIds, topicIdSchema } = require('../../topicIds')

/**
 * OffsetCommitRequest (Version: 10) — flexible (KIP-482), request header v2, topic ids (KIP-848)
 *
 * GroupId => COMPACT_STRING
 * GenerationIdOrMemberEpoch => INT32
 * MemberId => COMPACT_STRING
 * GroupInstanceId => COMPACT_NULLABLE_STRING
 * Topics => COMPACT_ARRAY
 *   TopicId => UUID
 *   Partitions => COMPACT_ARRAY
 *     PartitionIndex => INT32
 *     CommittedOffset => INT64
 *     CommittedLeaderEpoch => INT32
 *     CommittedMetadata => COMPACT_NULLABLE_STRING
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v9's body with `Topics.Name` replaced by `Topics.TopicId`. Callers keep naming topics; the ids come
 * from `topicIds`, and a topic without one fails here, before anything is sent.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetCommitRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options as v9, plus `topicIds`
 * @param {string} options.groupId
 * @param {number} options.groupGenerationId the member epoch under the consumer protocol
 * @param {string} options.memberId
 * @param {string|null} [options.groupInstanceId=null] static membership id (KIP-345)
 * @param {{topic: string, partitions: {partition: number, offset: string, leaderEpoch?: number, metadata?: string|null}[]}[]} [options.topics=[]]
 *   `leaderEpoch` defaults to -1 and `metadata` to null, as at v8
 * @param {Map<string, string>} options.topicIds topic name to topic id, for every topic in `topics`
 * @returns {object} the protocol request
 * @throws {import('../../../../errors').KafkaJSInvariantViolation} when a topic has no usable id
 */
module.exports = ({
  groupId,
  groupGenerationId,
  memberId,
  groupInstanceId = null,
  topics = [],
  topicIds,
}) => {
  const ids = requireTopicIds(
    'OffsetCommit',
    topics.map(({ topic }) => topic),
    topicIds
  )

  return {
    apiKey,
    apiVersion: 10,
    apiName: 'OffsetCommit',
    encode: async () =>
      new Encoder()
        .writeUVarIntString(groupId)
        .writeInt32(groupGenerationId)
        .writeUVarIntString(memberId)
        .writeUVarIntString(groupInstanceId)
        .writeUVarIntArray(
          topics.map(({ partitions }, index) => encodeTopic(ids[index], partitions))
        )
        .writeTaggedFields(),
  }
}

const encodeTopic = (topicId, partitions) =>
  new Encoder()
    .writeUuid(topicId)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

const encodePartition = ({ partition, offset, leaderEpoch = -1, metadata = null }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(leaderEpoch)
    .writeUVarIntString(metadata)
    .writeTaggedFields()

module.exports.schema = topicIdSchema(require('../v9/request').schema)
