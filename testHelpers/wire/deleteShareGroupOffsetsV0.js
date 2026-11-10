const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const literal = require('./expected').deleteShareGroupOffsetsV0

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeTopic = (value, tags) =>
  new Encoder()
    .writeUVarIntString(value.topicName)
    .writeUuid(value.topicId)
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeResponse = (value, tags) =>
  new Encoder()
    .writeInt32(value.throttleTime)
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeUVarIntArray(value.responses.map((entry) => encodeTopic(entry, tags)))
    .writeTaggedFields(tags)

/** @param {Decoder} decoder Request reader. @returns {object} Literal topic name. @throws {Error} Truncated fields or tags. */
const readTopic = (decoder) => {
  const value = { topicName: decoder.readUVarIntString() }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder Request reader. @returns {object} Literal request fields. @throws {Error} Truncated fields or tags. */
const readBody = (decoder) => {
  const value = {
    groupId: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray(readTopic),
  }
  decoder.readTaggedFields()
  return value
}

/**
 * Independent DeleteShareGroupOffsets v0 wire pair; uses protocol primitives and literal released-source fields.
 * It models no broker state and imports no request encoder, response decoder or share codec helper.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/DeleteShareGroupOffsetsResponse.json
 */
module.exports = {
  api: 'DeleteShareGroupOffsets',
  apiKey: apiKeys.DeleteShareGroupOffsets,
  version: 0,
  /**
   * @param {object} [fields] Response fields over the literal defaults; throttleTime is the wire throttle.
   * @param {object} [options] Independent tagged-field fixture control.
   * @param {boolean} [options.tagged=false] Emit tag 10 with byte 0x61 at every structure.
   * @returns {Buffer} Response body without a header.
   * @throws {Error} Invalid primitive input.
   */
  buildResponse(fields = {}, { tagged = false } = {}) {
    const tags = tagged ? [{ tag: 10, value: Buffer.from([0x61]) }] : []
    return encodeResponse({ ...literal.response, ...fields }, tags).buffer
  },
  /**
   * @param {Buffer} body Request body without its header.
   * @returns {object} All request fields.
   * @throws {Error} Truncated body or trailing bytes after the tag buffer.
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const value = readBody(decoder)
    if (decoder.canReadBytes(1))
      throw new Error('DeleteShareGroupOffsets v0 request has a byte after its tag buffer')
    return value
  },
}
