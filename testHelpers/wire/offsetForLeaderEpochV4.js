const Decoder = require('../../src/protocol/decoder')
const Encoder = require('../../src/protocol/encoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const {
  offsetForLeaderEpochResponse,
  decodeOffsetForLeaderEpochRequests,
} = require('../../src/consumer/__tests__/fixtures/fetchHarness')
const { offsetForLeaderEpochV4: expected } = require('./expected')

/**
 * @template T
 * @param {T|null} value what a compact string or array read returned
 * @param {string} what the field it was read for
 * @returns {T} the value
 * @throws {Error} when it is null: no field of this request is nullable
 */
const present = (value, what) => {
  if (value === null) {
    throw new Error(`OffsetForLeaderEpoch v4 request has null ${what}`)
  }

  return value
}

/**
 * Walks a request body field by field, as the message JSON lays it out: the replica id, the
 * compact array of topics, and in each topic its name, the compact array of partitions (three
 * integers and a tag section each) and a tag section, then the tag section of the request. A tag
 * section may hold unknown tags; a malformed one, or any byte after the last, is refused.
 * @param {Buffer} body the request body, after its header
 * @returns {void}
 * @throws {Error} when a field is null, cut short or malformed, or a byte follows the request's
 *   tag section
 */
const walk = (body) => {
  const decoder = new Decoder(body)
  decoder.readInt32()
  present(
    decoder.readUVarIntArray((topic) => {
      present(topic.readUVarIntString(), 'topic name')
      present(
        topic.readUVarIntArray((partition) => {
          partition.readInt32()
          partition.readInt32()
          partition.readInt32()
          partition.readTaggedFields()
        }),
        'partitions'
      )
      topic.readTaggedFields()
    }),
    'topics'
  )
  decoder.readTaggedFields()

  if (decoder.canReadBytes(1)) {
    throw new Error('OffsetForLeaderEpoch v4 request has a byte after its tag buffer')
  }
}

/**
 * OffsetForLeaderEpoch v4 (flexible), the existing builder and reader of the fetch harness reused
 * as they are, the reader for the fields and the builder for the response. The builder writes the
 * harness's one topic (`test-topic`). The reader stops before the request's last tag section, so a
 * walk of the whole body, field by field, decides what is well formed.
 *
 * Evidence: Java's own bytes (`fixtures/v4_*_java.json`, bytes only); the harness builder given
 * the fields of `./expected` reproduces the response vector, and its reader returns the request
 * vector's fields, which are the generator's constants.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/OffsetForLeaderEpochResponse.json
 */
module.exports = {
  api: 'OffsetForLeaderEpoch',
  apiKey: apiKeys.OffsetForLeaderEpoch,
  version: 4,

  /**
   * @param {object} [fields] over the vector's: `partitions` (`{ partition, errorCode, leaderEpoch,
   *   endOffset }`)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    return offsetForLeaderEpochResponse({ ...expected.response, ...fields }.partitions)
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ replicaId: number, topics: Array<{ topic: string, partitions: Array<{ partition:
   *   number, currentLeaderEpoch: number, leaderEpoch: number }> }> }}
   * @throws {Error} when the body is not exactly one well-formed request: a field null, cut short or
   *   malformed (the tag sections included), or a byte after the last tag section
   */
  readRequest(body) {
    walk(body)

    const header = new Encoder()
      .writeInt32(0)
      .writeInt16(apiKeys.OffsetForLeaderEpoch)
      .writeInt16(4)
      .writeInt32(0)
      .writeString('')
      .writeTaggedFields().buffer
    const [request] = decodeOffsetForLeaderEpochRequests([Buffer.concat([header, body])])

    return request
  },
}
