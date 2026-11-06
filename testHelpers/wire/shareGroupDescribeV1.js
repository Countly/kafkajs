const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const literal = require('./expected').shareGroupDescribeV1

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeTopicPartitions = (value, tags) =>
  new Encoder()
    .writeUuid(value.topicId)
    .writeUVarIntString(value.topicName)
    .writeUVarIntArray(value.partitions, 'int32')
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeAssignment = (value, tags) =>
  new Encoder()
    .writeUVarIntArray(value.topicPartitions.map((entry) => encodeTopicPartitions(entry, tags)))
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeMember = (value, tags) =>
  new Encoder()
    .writeUVarIntString(value.memberId)
    .writeUVarIntString(value.rackId)
    .writeInt32(value.memberEpoch)
    .writeUVarIntString(value.clientId)
    .writeUVarIntString(value.clientHost)
    .writeUVarIntArray(
      value.subscribedTopicNames.map((name) => new Encoder().writeUVarIntString(name))
    )
    .writeEncoder(encodeAssignment(value.assignment, tags))
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeGroup = (value, tags) =>
  new Encoder()
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeUVarIntString(value.groupId)
    .writeUVarIntString(value.groupState)
    .writeInt32(value.groupEpoch)
    .writeInt32(value.assignmentEpoch)
    .writeUVarIntString(value.assignorName)
    .writeUVarIntArray(value.members.map((entry) => encodeMember(entry, tags)))
    .writeInt32(value.authorizedOperations)
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeResponse = (value, tags) =>
  new Encoder()
    .writeInt32(value.throttleTime)
    .writeUVarIntArray(value.groups.map((entry) => encodeGroup(entry, tags)))
    .writeTaggedFields(tags)

/** @param {Decoder} decoder Request reader. @returns {object} Literal request fields. @throws {Error} Truncated fields or tags. */
const readBody = (decoder) => {
  const value = {
    groupIds: decoder.readUVarIntArray((entry) => entry.readUVarIntString()),
    includeAuthorizedOperations: decoder.readBoolean(),
  }
  decoder.readTaggedFields()
  return value
}

/**
 * Independent ShareGroupDescribe v1 wire pair; uses protocol primitives and literal released-source fields.
 * It models no broker state and imports no request encoder, response decoder or share codec helper.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/ShareGroupDescribeResponse.json
 */
module.exports = {
  api: 'ShareGroupDescribe',
  apiKey: apiKeys.ShareGroupDescribe,
  version: 1,
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
      throw new Error('ShareGroupDescribe v1 request has a byte after its tag buffer')
    return value
  },
}
