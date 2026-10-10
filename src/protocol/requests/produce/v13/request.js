const Long = require('../../../../utils/long')
const Encoder = require('../../../encoder')
const { Produce: apiKey } = require('../../apiKeys')
const { Types } = require('../../../message/compression')
const buildRecordBatch = require('../buildRecordBatch')
const { KafkaJSInvariantViolation } = require('../../../../errors')

/**
 * ProduceRequest (Version: 13) - flexible, request header v2, topic ids (KIP-516)
 *
 * Identical to v11/v12 except that `TopicData.Name` is replaced by `TopicData.TopicId` (uuid).
 * `TopicId`, like `Name` before it, is a `mapKey` in `ProduceRequest.json`: a v13 request must
 * carry each topic exactly once.
 * @see clients/src/main/resources/common/message/ProduceRequest.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers
 *
 * @param [transactionalId=null] {string} the transactional id or null if the producer is not transactional
 * @param acks {number} see producer request v0
 * @param timeout {number} see producer request v0
 * @param [compression=CompressionTypes.None] {CompressionTypes}
 * @param topicData {Array}
 * @param {Map<string, string>} topicIds topic name to topic id. `TopicId` is a `mapKey`, so each
 *   topic appears exactly once per request - which `src/producer/sendMessages.js` already
 *   guarantees by grouping every partition of a topic into one entry. (Fetch is the opposite:
 *   `FetchRequest.json` marks neither key, so duplicate topic entries are legal there.)
 */
module.exports = ({
  acks,
  timeout,
  transactionalId = null,
  producerId = Long.fromInt(-1),
  producerEpoch = 0,
  compression = Types.None,
  topicData,
  topicIds,
}) => ({
  apiKey,
  apiVersion: 13,
  apiName: 'Produce',
  expectResponse: () => acks !== 0,
  encode: async () => {
    const encodeTopic = topicEncoder(compression)
    const encodedTopicData = []

    for (const data of topicData) {
      encodedTopicData.push(
        await encodeTopic({ ...data, transactionalId, producerId, producerEpoch, topicIds })
      )
    }

    return new Encoder()
      .writeUVarIntString(transactionalId)
      .writeInt16(acks)
      .writeInt32(timeout)
      .writeUVarIntArray(encodedTopicData, 'object')
      .writeTaggedFields()
  },
})

/**
 * @param {Map<string, string>} topicIds
 * @param {string} topic
 * @returns {string} the topic's id
 * @throws {import('../../../../errors').KafkaJSInvariantViolation} when `topic` has no id - callers
 *   (`Broker#produce`'s v13 cap) never reach this codec without one for every topic, so this can
 *   only mean a caller bypassed that cap; an all-zero uuid would otherwise go out on the wire
 *   silently instead.
 */
const requireTopicId = (topicIds, topic) => {
  const topicId = topicIds.get(topic)

  if (topicId === undefined) {
    throw new KafkaJSInvariantViolation(
      `Produce v13 request is missing a topic id for topic "${topic}"`
    )
  }

  return topicId
}

const topicEncoder =
  (compression) =>
  async ({ topic, partitions, transactionalId, producerId, producerEpoch, topicIds }) => {
    const encodePartitions = partitionsEncoder(compression)
    const encodedPartitions = []

    for (const data of partitions) {
      encodedPartitions.push(
        await encodePartitions({ ...data, transactionalId, producerId, producerEpoch })
      )
    }

    return new Encoder()
      .writeUuid(requireTopicId(topicIds, topic))
      .writeUVarIntArray(encodedPartitions, 'object')
      .writeTaggedFields()
  }

const partitionsEncoder =
  (compression) =>
  async ({ partition, messages, transactionalId, firstSequence, producerId, producerEpoch }) => {
    const recordBatch = await buildRecordBatch({
      compression,
      messages,
      transactionalId,
      firstSequence,
      producerId,
      producerEpoch,
    })

    return new Encoder()
      .writeInt32(partition)
      .writeUVarInt(recordBatch.size() + 1)
      .writeEncoder(recordBatch)
      .writeTaggedFields()
  }

module.exports.schema = {
  api: 'Produce',
  version: 13,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '3+', nullable: true },
    { name: 'Acks', type: 'int16', versions: '0+' },
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
    { name: 'TopicData', type: '[]TopicProduceData', versions: '0+' },
    { name: 'TopicData.TopicId', type: 'uuid', versions: '13+' },
    { name: 'TopicData.PartitionData', type: '[]PartitionProduceData', versions: '0+' },
    { name: 'TopicData.PartitionData.Index', type: 'int32', versions: '0+' },
    { name: 'TopicData.PartitionData.Records', type: 'records', versions: '0+', nullable: true },
  ],
}
