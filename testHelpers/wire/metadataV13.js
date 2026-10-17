const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { metadataV13: expected } = require('./expected')

const encodeBroker = ({ nodeId, host, port, rack }) =>
  new Encoder()
    .writeInt32(nodeId)
    .writeUVarIntString(host)
    .writeInt32(port)
    .writeUVarIntString(rack)
    .writeTaggedFields()

const encodeNodes = (nodes) => new Encoder().writeUVarIntArray(nodes, 'int32')

const encodePartition = (partition) =>
  new Encoder()
    .writeInt16(partition.errorCode)
    .writeInt32(partition.partitionIndex)
    .writeInt32(partition.leaderId)
    .writeInt32(partition.leaderEpoch)
    .writeEncoder(encodeNodes(partition.replicas))
    .writeEncoder(encodeNodes(partition.isr))
    .writeEncoder(encodeNodes(partition.offlineReplicas))
    .writeTaggedFields()

const encodeTopic = (topic) =>
  new Encoder()
    .writeInt16(topic.errorCode)
    .writeUVarIntString(topic.name)
    .writeUuid(topic.topicId)
    .writeBoolean(topic.isInternal)
    .writeUVarIntArray(topic.partitions.map(encodePartition))
    .writeInt32(topic.topicAuthorizedOperations)
    .writeTaggedFields()

/**
 * Metadata v13 (flexible). The request names topics by name or id, or asks for all of them; the
 * response lists the brokers, the controller and each topic's partitions with leader, epoch,
 * replicas and in-sync replicas, and ends with the top-level error code of v13.
 *
 * Evidence: both messages are Java's own bytes (`fixtures/v13_*_java.json`, bytes only), and
 * `./expected` holds the fields of their generator.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/MetadataResponse.json
 */
module.exports = {
  api: 'Metadata',
  apiKey: apiKeys.Metadata,
  version: 13,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `brokers` (`{ nodeId, host, port,
   *   rack }`), `clusterId`, `controllerId`, `topics` (`{ errorCode, name, topicId, isInternal,
   *   partitions, topicAuthorizedOperations }`, a partition being `{ errorCode, partitionIndex,
   *   leaderId, leaderEpoch, replicas, isr, offlineReplicas }`) and `errorCode`
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...expected.response, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeUVarIntArray(response.brokers.map(encodeBroker))
      .writeUVarIntString(response.clusterId)
      .writeInt32(response.controllerId)
      .writeUVarIntArray(response.topics.map(encodeTopic))
      .writeInt16(response.errorCode)
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ topics: Array<{ topicId: string|null, name: string|null }>|null,
   *   allowAutoTopicCreation: boolean, includeTopicAuthorizedOperations: boolean }} `topics` is
   *   null when the request asks for every topic
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const topics = decoder.readUVarIntArray((d) => {
      const topic = { topicId: d.readUuid(), name: d.readUVarIntString() }
      d.readTaggedFields()
      return topic
    })
    const allowAutoTopicCreation = decoder.readBoolean()
    const includeTopicAuthorizedOperations = decoder.readBoolean()
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('Metadata v13 request has a byte after its tag buffer')
    }

    return { topics, allowAutoTopicCreation, includeTopicAuthorizedOperations }
  },
}
