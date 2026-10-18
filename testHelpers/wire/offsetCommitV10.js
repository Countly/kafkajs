const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/offsetCommit/fixtures/java/offsetCommitResponse-v10-ok.json')

const encodePartition = ({ partition, errorCode }) =>
  new Encoder().writeInt32(partition).writeInt16(errorCode).writeTaggedFields()

const encodeTopic = ({ topicId, partitions }) =>
  new Encoder()
    .writeUuid(topicId)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

/**
 * OffsetCommit v10 (flexible, topics keyed by id), the version a consumer of the consumer group
 * protocol commits with on a broker that offers it. The request commits an offset, leader epoch and
 * metadata per partition under the member's epoch; the response answers each partition with an
 * error code (100, UNKNOWN_TOPIC_ID, for an id the broker does not know). Offsets are decimal
 * strings.
 *
 * Evidence: Java's structured vectors `offsetCommitRequest-v10-byTopicId.json` and
 * `offsetCommitResponse-v10-{ok,unknownTopicId}.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/OffsetCommitResponse.json
 */
module.exports = {
  api: 'OffsetCommit',
  apiKey: apiKeys.OffsetCommit,
  version: 10,

  /**
   * @param {object} [fields] over the vector's: `throttleTime` and `topics` (`{ topicId, partitions
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
   *   string|null, topics: Array<{ topicId: string, partitions: Array<{ partition: number, offset:
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
          topicId: d.readUuid(),
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
      throw new Error('OffsetCommit v10 request has a byte after its tag buffer')
    }

    return request
  },
}
