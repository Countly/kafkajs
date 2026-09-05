const Long = require('../../../../utils/long')
const Encoder = require('../../../encoder')
const { Produce: apiKey } = require('../../apiKeys')
const { Types } = require('../../../message/compression')
const buildRecordBatch = require('../buildRecordBatch')

/**
 * ProduceRequest (Version: 10) — flexible (KIP-482), request header v2
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
 * The request gains nothing over v9 — KIP-951's `CurrentLeader`/`NodeEndpoints` tags and v11's
 * `TRANSACTION_ABORTABLE` are both response-side changes, so v10's request is byte-identical to
 * v9's, only the negotiated `apiVersion` differs.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ProduceRequest.json (Apache Kafka 4.3.1)
 *
 * @param [transactionalId=null] {string} the transactional id or null if the producer is not transactional
 * @param acks {number} see producer request v0
 * @param timeout {number} see producer request v0
 * @param [compression=CompressionTypes.None] {CompressionTypes}
 * @param topicData {Array}
 */
module.exports = ({
  acks,
  timeout,
  transactionalId = null,
  producerId = Long.fromInt(-1),
  producerEpoch = 0,
  compression = Types.None,
  topicData,
}) => ({
  apiKey,
  apiVersion: 10,
  apiName: 'Produce',
  expectResponse: () => acks !== 0,
  encode: async () => {
    const encodeTopic = topicEncoder(compression)
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
  (compression) =>
  async ({ topic, partitions, transactionalId, producerId, producerEpoch }) => {
    const encodePartitions = partitionsEncoder(compression)
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
  version: 10,
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
