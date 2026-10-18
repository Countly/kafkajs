const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/txnOffsetCommit/fixtures/java/txnOffsetCommitResponse-v4-ok.json')

const encodePartition = ({ partition, errorCode }) =>
  new Encoder().writeInt32(partition).writeInt16(errorCode).writeTaggedFields()

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

/**
 * TxnOffsetCommit v4 (flexible), the version a transaction under transaction version 1 sends.
 * The request names the transactional id, the group (with its member and generation, `-1` and an
 * empty member for a caller outside the group) and the producer id and epoch, then the offsets per
 * partition; the response answers per partition. Version 5 has the same layout.
 *
 * Evidence: Java's structured vectors `txnOffsetCommitRequest-v4-{groupMetadata,legacy}.json` and
 * `txnOffsetCommitResponse-v4-{ok,error}.json` (`hex` and `fields`). Offsets and the producer id
 * are decimal strings.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/TxnOffsetCommitResponse.json
 */
module.exports = {
  api: 'TxnOffsetCommit',
  apiKey: apiKeys.TxnOffsetCommit,
  version: 4,

  /**
   * @param {object} [fields] over the vector's: `throttleTime` and `topics` (`{ topic, partitions:
   *   [{ partition, errorCode }] }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeUVarIntArray(response.topics.map(encodeTopic))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ transactionalId: string, groupId: string, producerId: string, producerEpoch:
   *   number, generationId: number, memberId: string, groupInstanceId: string|null, topics:
   *   Array<{ topic: string, partitions: Array<{ partition: number, offset: string, leaderEpoch:
   *   number, metadata: string|null }> }> }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      transactionalId: decoder.readUVarIntString(),
      groupId: decoder.readUVarIntString(),
      producerId: decoder.readInt64().toString(),
      producerEpoch: decoder.readInt16(),
      generationId: decoder.readInt32(),
      memberId: decoder.readUVarIntString(),
      groupInstanceId: decoder.readUVarIntString(),
      topics: decoder.readUVarIntArray((topic) => {
        const entry = {
          topic: topic.readUVarIntString(),
          partitions: topic.readUVarIntArray((partition) => {
            const committed = {
              partition: partition.readInt32(),
              offset: partition.readInt64().toString(),
              leaderEpoch: partition.readInt32(),
              metadata: partition.readUVarIntString(),
            }
            partition.readTaggedFields()
            return committed
          }),
        }
        topic.readTaggedFields()
        return entry
      }),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('TxnOffsetCommit v4 and v5 request has a byte after its tag buffer')
    }

    return request
  },
}
