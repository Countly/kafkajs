const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const literal = require('./expected').shareGroupHeartbeatV1

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeTopicPartitions = (value, tags) =>
  new Encoder()
    .writeUuid(value.topicId)
    .writeUVarIntArray(
      value.partitions === null
        ? null
        : value.partitions.map((entry) => new Encoder().writeInt32(entry))
    )
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeAssignment = (value, tags) =>
  new Encoder()
    .writeUVarIntArray(
      value.topicPartitions === null
        ? null
        : value.topicPartitions.map((entry) => encodeTopicPartitions(entry, tags))
    )
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeResponse = (value, tags) =>
  new Encoder()
    .writeInt32(value.throttleTime)
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeUVarIntString(value.memberId)
    .writeInt32(value.memberEpoch)
    .writeInt32(value.heartbeatIntervalMs)
    .writeEncoder(
      value.assignment === null
        ? new Encoder().writeInt8(-1)
        : new Encoder().writeInt8(1).writeEncoder(encodeAssignment(value.assignment, tags))
    )
    .writeTaggedFields(tags)

/** @param {Decoder} decoder Request reader. @returns {object} Literal request fields. @throws {Error} Truncated fields or tags. */
const readBody = (decoder) => {
  const value = {
    groupId: decoder.readUVarIntString(),
    memberId: decoder.readUVarIntString(),
    memberEpoch: decoder.readInt32(),
    rackId: decoder.readUVarIntString(),
    subscribedTopicNames: decoder.readUVarIntArray((entry) => entry.readUVarIntString()),
  }
  decoder.readTaggedFields()
  return value
}

/**
 * Independent ShareGroupHeartbeat v1 wire pair; uses protocol primitives and literal released-source fields.
 * It models no broker state and imports no request encoder, response decoder or share codec helper.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/ShareGroupHeartbeatResponse.json
 */
module.exports = {
  api: 'ShareGroupHeartbeat',
  apiKey: apiKeys.ShareGroupHeartbeat,
  version: 1,
  /**
   * @param {object} [fields] Response fields over the literal defaults; throttleTime is the wire throttle.
   * @param {object} [options] Independent tagged-field fixture control.
   * @param {boolean} [options.tagged=false] Emit tag 10 with byte 0x61 at every envelope structure.
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
      throw new Error('ShareGroupHeartbeat v1 request has a byte after its tag buffer')
    return value
  },
}
