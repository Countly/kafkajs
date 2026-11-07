const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const literal = require('./expected').describeShareGroupOffsetsV1

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodePartition = (value, tags) =>
  new Encoder()
    .writeInt32(value.partitionIndex)
    .writeInt64(value.startOffset)
    .writeInt32(value.leaderEpoch)
    .writeInt64(value.lag)
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeTopic = (value, tags) =>
  new Encoder()
    .writeUVarIntString(value.topicName)
    .writeUuid(value.topicId)
    .writeUVarIntArray(value.partitions.map((entry) => encodePartition(entry, tags)))
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeGroup = (value, tags) =>
  new Encoder()
    .writeUVarIntString(value.groupId)
    .writeUVarIntArray(value.topics.map((entry) => encodeTopic(entry, tags)))
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeResponse = (value, tags) =>
  new Encoder()
    .writeInt32(value.throttleTime)
    .writeUVarIntArray(value.groups.map((entry) => encodeGroup(entry, tags)))
    .writeTaggedFields(tags)

/** @param {Decoder} decoder Request reader. @returns {object} Literal topic selection. @throws {Error} Truncated fields or tags. */
const readTopic = (decoder) => {
  const value = {
    topicName: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray((entry) => entry.readInt32()),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder Request reader. @returns {object} Literal group selection; `topics` null means every topic. @throws {Error} Truncated fields or tags. */
const readGroup = (decoder) => {
  const value = {
    groupId: decoder.readUVarIntString(),
    topics: decoder.readUVarIntArray(readTopic),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder Request reader. @returns {object} Literal request fields. @throws {Error} Truncated fields or tags. */
const readBody = (decoder) => {
  const value = { groups: decoder.readUVarIntArray(readGroup) }
  decoder.readTaggedFields()
  return value
}

/**
 * Independent DescribeShareGroupOffsets v1 wire pair; uses protocol primitives and literal released-source fields.
 * It models no broker state and imports no request encoder, response decoder or share codec helper.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/DescribeShareGroupOffsetsResponse.json
 */
module.exports = {
  api: 'DescribeShareGroupOffsets',
  apiKey: apiKeys.DescribeShareGroupOffsets,
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
   * @returns {object} All request fields; a null topic list stays null, an empty one stays empty.
   * @throws {Error} Truncated body or trailing bytes after the tag buffer.
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const value = readBody(decoder)
    if (decoder.canReadBytes(1))
      throw new Error('DescribeShareGroupOffsets v1 request has a byte after its tag buffer')
    return value
  },
}
