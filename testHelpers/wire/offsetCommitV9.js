const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/offsetCommit/fixtures/java/offsetCommitResponse-v9-basic.json')

const encodePartition = ({ partition, errorCode }) =>
  new Encoder().writeInt32(partition).writeInt16(errorCode).writeTaggedFields()

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

/**
 * OffsetCommit v9 (flexible). The request commits an offset, leader epoch and metadata per
 * partition under the member's generation; the response answers each partition with an error
 * code. Offsets are decimal strings.
 *
 * Evidence: Java's structured vectors `offsetCommitRequest-v9-basic.json` and
 * `offsetCommitResponse-v9-basic.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/OffsetCommitResponse.json
 */
module.exports = {
  api: 'OffsetCommit',
  apiKey: apiKeys.OffsetCommit,
  version: 9,

  /**
   * @param {object} [fields] over the vector's: `throttleTime` and `topics` (`{ topic, partitions
   *   }`, a partition being `{ partition, errorCode }`)
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
   * @returns {{ groupId: string, groupGenerationId: number, memberId: string, groupInstanceId:
   *   string|null, topics: Array<{ topic: string, partitions: Array<{ partition: number, offset:
   *   string, leaderEpoch: number, metadata: string|null }> }> }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      groupId: decoder.readUVarIntString(),
      groupGenerationId: decoder.readInt32(),
      memberId: decoder.readUVarIntString(),
      groupInstanceId: decoder.readUVarIntString(),
      topics: decoder.readUVarIntArray((d) => {
        const topic = {
          topic: d.readUVarIntString(),
          partitions: d.readUVarIntArray((p) => {
            const partition = {
              partition: p.readInt32(),
              offset: p.readInt64().toString(),
              leaderEpoch: p.readInt32(),
              metadata: p.readUVarIntString(),
            }
            p.readTaggedFields()
            return partition
          }),
        }
        d.readTaggedFields()
        return topic
      }),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('OffsetCommit v9 request has a byte after its tag buffer')
    }

    return request
  },
}
