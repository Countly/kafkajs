const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { listOffsetsV9: expected } = require('./expected')

const encodePartition = ({ partitionIndex, errorCode, timestamp, offset, leaderEpoch }) =>
  new Encoder()
    .writeInt32(partitionIndex)
    .writeInt16(errorCode)
    .writeInt64(timestamp)
    .writeInt64(offset)
    .writeInt32(leaderEpoch)
    .writeTaggedFields()

const encodeTopic = ({ name, partitions }) =>
  new Encoder()
    .writeUVarIntString(name)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

/**
 * ListOffsets v9 (flexible). The request asks, per partition, for the offset at a timestamp (-2 is
 * the earliest, -1 the latest); the response answers with the offset and its leader epoch.
 *
 * Evidence: both messages are Java's own bytes (`fixtures/v9_*_java.json`, bytes only), and
 * `./expected` holds the fields of their generator. Offsets and timestamps are decimal strings.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ListOffsetsResponse.json
 */
module.exports = {
  api: 'ListOffsets',
  apiKey: apiKeys.ListOffsets,
  version: 9,

  /**
   * @param {object} [fields] over the vector's: `throttleTime` and `topics` (`{ name, partitions }`,
   *   a partition being `{ partitionIndex, errorCode, timestamp, offset, leaderEpoch }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...expected.response, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeUVarIntArray(response.topics.map(encodeTopic))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ replicaId: number, isolationLevel: number, topics: Array<{ name: string,
   *   partitions: Array<{ partitionIndex: number, currentLeaderEpoch: number, timestamp: string }> }> }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const replicaId = decoder.readInt32()
    const isolationLevel = decoder.readInt8()
    const topics = decoder.readUVarIntArray((d) => {
      const name = d.readUVarIntString()
      const partitions = d.readUVarIntArray((p) => {
        const partition = {
          partitionIndex: p.readInt32(),
          currentLeaderEpoch: p.readInt32(),
          timestamp: p.readInt64().toString(),
        }
        p.readTaggedFields()
        return partition
      })
      d.readTaggedFields()
      return { name, partitions }
    })
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('ListOffsets v9 request has a byte after its tag buffer')
    }

    return { replicaId, isolationLevel, topics }
  },
}
