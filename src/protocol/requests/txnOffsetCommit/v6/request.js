const Encoder = require('../../../encoder')
const { TxnOffsetCommit: apiKey } = require('../../apiKeys')
const { requireTopicIds } = require('../../topicIds')
const requestV5 = require('../v5/request')

/**
 * TxnOffsetCommit Request (Version: 6) — flexible (KIP-482), request header v2, topic ids (KIP-1319)
 *
 * TransactionalId => COMPACT_STRING
 * GroupId => COMPACT_STRING
 * ProducerId => INT64
 * ProducerEpoch => INT16
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
 * v5's body with `Topics.Name` replaced by `Topics.TopicId`. Callers keep naming topics; the ids
 * come from `topicIds`, and a topic without one fails here, before anything is sent. The Broker
 * selects this version only for a request whose every topic has a usable id and whose transaction
 * runs transaction version 2.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitRequest.json (Apache Kafka 4.4.0)
 *
 * @param {object} options the v5 builder's, plus `topicIds`
 * @param {string} options.transactionalId
 * @param {string} options.groupId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {number} [options.generationId=-1] the generation, or the member epoch under the
 *   consumer group protocol
 * @param {string} [options.memberId='']
 * @param {string|null} [options.groupInstanceId=null]
 * @param {{ topic: string, partitions: { partition: number|string, offset: string|number,
 *   leaderEpoch?: number|null, metadata?: string|null }[] }[]} [options.topics=[]]
 * @param {Map<string, string>} options.topicIds topic name to topic id, for every topic in `topics`
 * @returns {object} the protocol request
 * @throws {import('../../../../errors').KafkaJSInvariantViolation} when a topic has no usable id
 */
module.exports = ({
  transactionalId,
  groupId,
  producerId,
  producerEpoch,
  generationId = -1,
  memberId = '',
  groupInstanceId = null,
  topics = [],
  topicIds,
}) => {
  const ids = requireTopicIds(
    'TxnOffsetCommit',
    topics.map(({ topic }) => topic),
    topicIds,
    6
  )

  return {
    apiKey,
    apiVersion: 6,
    apiName: 'TxnOffsetCommit',
    encode: async () =>
      new Encoder()
        .writeUVarIntString(transactionalId)
        .writeUVarIntString(groupId)
        .writeInt64(producerId)
        .writeInt16(producerEpoch)
        .writeInt32(generationId)
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

const encodePartition = ({ partition, offset, leaderEpoch, metadata = null }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(leaderEpoch ?? -1)
    .writeUVarIntString(metadata)
    .writeTaggedFields()

module.exports.schema = {
  ...requestV5.schema,
  version: 6,
  fields: requestV5.schema.fields.map((field) =>
    field.name === 'Topics.Name' ? { name: 'Topics.TopicId', type: 'uuid', versions: '6+' } : field
  ),
}
