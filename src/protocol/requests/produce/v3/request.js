const Long = require('../../../../utils/long')
const Encoder = require('../../../encoder')
const { Produce: apiKey } = require('../../apiKeys')
const { Types } = require('../../../message/compression')
const buildRecordBatch = require('../buildRecordBatch')

/**
 * Produce Request (Version: 3) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 */

/**
 * @param [transactionalId=null] {String} The transactional id or null if the producer is not transactional
 * @param acks {Integer} See producer request v0
 * @param timeout {Integer} See producer request v0
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
  apiVersion: 3,
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
      .writeString(transactionalId)
      .writeInt16(acks)
      .writeInt32(timeout)
      .writeArray(encodedTopicData)
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

    return new Encoder().writeString(topic).writeArray(encodedPartitions)
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
      .writeInt32(recordBatch.size())
      .writeEncoder(recordBatch)
  }
