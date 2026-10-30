const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/offsetFetch/fixtures/java/offsetFetchResponse-v9-basic.json')

const encodePartition = ({ partition, offset, leaderEpoch, metadata, errorCode }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(leaderEpoch)
    .writeUVarIntString(metadata)
    .writeInt16(errorCode)
    .writeTaggedFields()

const encodeTopic = ({ topic, partitions }) =>
  new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map(encodePartition))
    .writeTaggedFields()

/**
 * OffsetFetch v9 (flexible, grouped). The request names one group, its member and epoch, the
 * partitions to read and whether to wait for stable offsets; the response answers the one group
 * with an offset and leader epoch per partition (-1 for none). Offsets are decimal strings.
 *
 * Evidence: Java's structured vectors `offsetFetchRequest-v9-memberFields.json` and
 * `offsetFetchResponse-v9-basic.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/OffsetFetchResponse.json
 */
module.exports = {
  api: 'OffsetFetch',
  apiKey: apiKeys.OffsetFetch,
  version: 9,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `groupId`, `errorCode` and
   *   `topics` (`{ topic, partitions }`, a partition being `{ partition, offset, leaderEpoch,
   *   metadata, errorCode }`); or `groups`, a list of `{ groupId, errorCode, topics }`, which
   *   replaces the three group fields and may hold none or several
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }
    const groups = response.groups ?? [response]
    const encoded = groups.map((entry) =>
      new Encoder()
        .writeUVarIntString(entry.groupId)
        .writeUVarIntArray(entry.topics.map(encodeTopic))
        .writeInt16(entry.errorCode)
        .writeTaggedFields()
    )

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeUVarIntArray(encoded)
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ groupId: string, memberId: string|null, memberEpoch: number, topics:
   *   Array<{ topic: string, partitions: Array<{ partition: number }> }>|null, requireStable:
   *   boolean }} the one group the request names; `topics` is null for every topic
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const [group] = decoder.readUVarIntArray((d) => {
      const read = {
        groupId: d.readUVarIntString(),
        memberId: d.readUVarIntString(),
        memberEpoch: d.readInt32(),
        topics: d.readUVarIntArray((t) => {
          const topic = {
            topic: t.readUVarIntString(),
            partitions: t.readUVarIntArray((p) => ({ partition: p.readInt32() })),
          }
          t.readTaggedFields()
          return topic
        }),
      }
      d.readTaggedFields()
      return read
    })
    const requireStable = decoder.readBoolean()
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('OffsetFetch v9 request has a byte after its tag buffer')
    }

    return { ...group, requireStable }
  },
}
