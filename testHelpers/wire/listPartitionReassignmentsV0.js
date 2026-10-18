const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { listPartitionReassignmentsV0: expected } = require('./expected')

const encodeNodes = (nodes) => new Encoder().writeUVarIntArray(nodes, 'int32')

const encodePartition = ({ partitionIndex, replicas, addingReplicas, removingReplicas }) =>
  new Encoder()
    .writeInt32(partitionIndex)
    .writeEncoder(encodeNodes(replicas))
    .writeEncoder(encodeNodes(addingReplicas))
    .writeEncoder(encodeNodes(removingReplicas))
    .writeTaggedFields()

const encodeTopic = ({ name, partitions }) =>
  new Encoder()
    .writeUVarIntString(name)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

/**
 * ListPartitionReassignments v0 (flexible). The request names the topics and partitions to ask
 * about, or none for every reassignment in flight; the response lists, per topic and partition, the
 * replicas, the replicas being added and the replicas being removed (no topic when nothing is in
 * flight).
 *
 * Evidence: a Kafka 4.3.1 broker's own bytes (`fixtures/v0_*_kafka431.json`, from a client that
 * asked for every reassignment while none was in flight); no Java vector exists. `./expected` holds
 * the fields the capture script's inputs and the message layout give for them, written by hand,
 * and, for a reassignment in flight, a hand-computed golden.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ListPartitionReassignmentsResponse.json
 */
module.exports = {
  api: 'ListPartitionReassignments',
  apiKey: apiKeys.ListPartitionReassignments,
  version: 0,

  /**
   * @param {object} [fields] over the capture's: `throttleTime`, `errorCode`, `errorMessage` and
   *   `topics` (`{ name, partitions }`, a partition being `{ partitionIndex, replicas,
   *   addingReplicas, removingReplicas }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...expected.capture.response, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeUVarIntString(response.errorMessage)
      .writeUVarIntArray(response.topics.map(encodeTopic))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ timeoutMs: number, topics: Array<{ name: string, partitionIndexes: number[] }>|null
   *   }} `topics` is null when the request asks for every reassignment
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      timeoutMs: decoder.readInt32(),
      topics: decoder.readUVarIntArray((topic) => {
        const entry = {
          name: topic.readUVarIntString(),
          partitionIndexes: topic.readUVarIntArray((index) => index.readInt32()),
        }
        topic.readTaggedFields()
        return entry
      }),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('ListPartitionReassignments v0 request has a byte after its tag buffer')
    }

    return request
  },
}
