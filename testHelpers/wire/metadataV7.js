const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { metadataV7: expected } = require('./expected')

/**
 * Encodes one broker in the classic Metadata layout.
 * @param {object} broker node id, host, port and nullable rack
 * @returns {Encoder} encoded broker
 */
const brokerOf = (broker) =>
  new Encoder()
    .writeInt32(broker.nodeId)
    .writeString(broker.host)
    .writeInt32(broker.port)
    .writeString(broker.rack)

/**
 * Encodes a v7 partition, including the leader epoch and offline replicas.
 * @param {object} partition independent wire field literals
 * @returns {Encoder} encoded partition
 */
const partitionOf = (partition) =>
  new Encoder()
    .writeInt16(partition.errorCode)
    .writeInt32(partition.partitionIndex)
    .writeInt32(partition.leaderId)
    .writeInt32(partition.leaderEpoch)
    .writeArray(partition.replicas, 'int32')
    .writeArray(partition.isr, 'int32')
    .writeArray(partition.offlineReplicas, 'int32')

/**
 * Encodes a topic without the unsupported authorization mask or topic id.
 * @param {object} topic independent wire field literals
 * @returns {Encoder} encoded topic
 */
const topicOf = (topic) =>
  new Encoder()
    .writeInt16(topic.errorCode)
    .writeString(topic.name)
    .writeBoolean(topic.isInternal)
    .writeArray(topic.partitions.map(partitionOf))

/** Classic Metadata v7 pair. Independent Java vector acceptance is tracked by its wire tests. */
module.exports = {
  api: 'Metadata',
  apiKey: 3,
  version: 7,

  /**
   * @param {object} [fields] independent response fields over the default literals
   * @returns {Buffer} classic response body, with no header
   */
  buildResponse(fields = {}) {
    const response = { ...expected.response, ...fields }
    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeArray(response.brokers.map(brokerOf))
      .writeString(response.clusterId)
      .writeInt32(response.controllerId)
      .writeArray(response.topics.map(topicOf)).buffer
  },

  /**
   * @param {Buffer} body request body after its header
   * @returns {{topics: string[]|null, allowAutoTopicCreation: boolean}} literal request fields
   * @throws {Error} cut body or unconsumed bytes
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const count = decoder.readInt32()
    if (count < -1) throw new Error('Metadata v7 topics count is invalid')
    const topics = count === -1 ? null : Array.from({ length: count }, () => decoder.readString())
    const allowAutoTopicCreation = decoder.readBoolean()
    if (decoder.canReadBytes(1)) throw new Error('Metadata v7 request has trailing bytes')
    return { topics, allowAutoTopicCreation }
  },
}
