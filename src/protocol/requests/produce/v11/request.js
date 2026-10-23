const Long = require('../../../../utils/long')
const Encoder = require('../../../encoder')
const { Produce: apiKey } = require('../../apiKeys')
const { Types } = require('../../../message/compression')
const buildRecordBatch = require('../buildRecordBatch')

/**
 * ProduceRequest (Version: 11) — flexible (KIP-482), request header v2
 *
 * TransactionalId => COMPACT_NULLABLE_STRING
 * Acks => INT16
 * TimeoutMs => INT32
 * TopicData => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   PartitionData => COMPACT_ARRAY
 *     Index => INT32
 *     Records => COMPACT_NULLABLE_RECORDS
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v11 adds no fields to the request or the response; it exists so the broker can answer
 * `TRANSACTION_ABORTABLE` (120, KIP-890) on it instead of a lower version. Recovering from that
 * error (aborting and restarting the transaction) is out of scope here — it surfaces the same way
 * every other non-retriable protocol error does. This request is otherwise byte-identical to v10.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ProduceRequest.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 *
 * @param [transactionalId=null] {string} the transactional id or null if the producer is not transactional
 * @param acks {number} see producer request v0
 * @param timeout {number} see producer request v0
 * @param [compression=CompressionTypes.None] {CompressionTypes}
 * @param [compressionLevel] {number} optional codec level
 * @param topicData {Array}
 */
module.exports = ({
  acks,
  timeout,
  transactionalId = null,
  producerId = Long.fromInt(-1),
  producerEpoch = 0,
  compression = Types.None,
  compressionLevel,
  topicData,
}) => ({
  apiKey,
  apiVersion: 11,
  apiName: 'Produce',
  expectResponse: () => acks !== 0,
  encode: async () => {
    const encodeTopic = topicEncoder(compression, compressionLevel)
    const encodedTopicData = []

    for (const data of topicData) {
      encodedTopicData.push(
        await encodeTopic({ ...data, transactionalId, producerId, producerEpoch })
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

const topicEncoder =
  (compression, compressionLevel) =>
  async ({ topic, partitions, transactionalId, producerId, producerEpoch }) => {
    const encodePartitions = partitionsEncoder(compression, compressionLevel)
    const encodedPartitions = []

    for (const data of partitions) {
      encodedPartitions.push(
        await encodePartitions({ ...data, transactionalId, producerId, producerEpoch })
      )
    }

    return new Encoder()
      .writeUVarIntString(topic)
      .writeUVarIntArray(encodedPartitions, 'object')
      .writeTaggedFields()
  }

const partitionsEncoder =
  (compression, compressionLevel) =>
  async ({ partition, messages, transactionalId, firstSequence, producerId, producerEpoch }) => {
    const recordBatch = await buildRecordBatch({
      compression,
      compressionLevel,
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
  version: 11,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '3+', nullable: true },
    { name: 'Acks', type: 'int16', versions: '0+' },
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
    { name: 'TopicData', type: '[]TopicProduceData', versions: '0+' },
    { name: 'TopicData.Name', type: 'string', versions: '0-12' },
    { name: 'TopicData.PartitionData', type: '[]PartitionProduceData', versions: '0+' },
    { name: 'TopicData.PartitionData.Index', type: 'int32', versions: '0+' },
    { name: 'TopicData.PartitionData.Records', type: 'records', versions: '0+', nullable: true },
  ],
}
