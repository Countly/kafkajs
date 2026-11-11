const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const literal = require('./expected').shareAcknowledgeV2

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeCurrentLeader = (value, tags) =>
  new Encoder().writeInt32(value.leaderId).writeInt32(value.leaderEpoch).writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodePartitions = (value, tags) =>
  new Encoder()
    .writeInt32(value.partitionIndex)
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeEncoder(encodeCurrentLeader(value.currentLeader, tags))
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeResponses = (value, tags) =>
  new Encoder()
    .writeUuid(value.topicId)
    .writeUVarIntArray(
      value.partitions === null
        ? null
        : value.partitions.map((entry) => encodePartitions(entry, tags))
    )
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeNodeEndpoints = (value, tags) =>
  new Encoder()
    .writeInt32(value.nodeId)
    .writeUVarIntString(value.host)
    .writeInt32(value.port)
    .writeUVarIntString(value.rack)
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeResponse = (value, tags) =>
  new Encoder()
    .writeInt32(value.throttleTime)
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeInt32(value.acquisitionLockTimeoutMs)
    .writeUVarIntArray(
      value.responses === null ? null : value.responses.map((entry) => encodeResponses(entry, tags))
    )
    .writeUVarIntArray(
      value.nodeEndpoints === null
        ? null
        : value.nodeEndpoints.map((entry) => encodeNodeEndpoints(entry, tags))
    )
    .writeTaggedFields(tags)

/** @param {Decoder} decoder Request reader. @returns {object} Literal request fields. @throws {Error} Truncated fields or tags. */
const readAcknowledgementBatches = (decoder) => {
  const value = {
    firstOffset: decoder.readInt64().toString(),
    lastOffset: decoder.readInt64().toString(),
    acknowledgeTypes: decoder.readUVarIntArray((entry) => entry.readInt8()),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder Request reader. @returns {object} Literal request fields. @throws {Error} Truncated fields or tags. */
const readPartitions = (decoder) => {
  const value = {
    partitionIndex: decoder.readInt32(),
    acknowledgementBatches: decoder.readUVarIntArray(readAcknowledgementBatches),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder Request reader. @returns {object} Literal request fields. @throws {Error} Truncated fields or tags. */
const readTopics = (decoder) => {
  const value = {
    topicId: decoder.readUuid(),
    partitions: decoder.readUVarIntArray(readPartitions),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder Request reader. @returns {object} Literal request fields. @throws {Error} Truncated fields or tags. */
const readBody = (decoder) => {
  const value = {
    groupId: decoder.readUVarIntString(),
    memberId: decoder.readUVarIntString(),
    shareSessionEpoch: decoder.readInt32(),
    isRenewAck: decoder.readBoolean(),
    topics: decoder.readUVarIntArray(readTopics),
  }
  decoder.readTaggedFields()
  return value
}

/**
 * Independent ShareAcknowledge v2 wire pair; uses protocol primitives and literal released-source fields.
 * It models no broker state and imports no request encoder, response decoder or share codec helper.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/ShareAcknowledgeResponse.json
 */
module.exports = {
  api: 'ShareAcknowledge',
  apiKey: apiKeys.ShareAcknowledge,
  version: 2,
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
   * @param {Buffer} body Response body without its header.
   * @returns {object} Independently decoded response fields, including acknowledgement errors.
   * @throws {Error} Truncated fields or trailing bytes.
   */
  readResponse(body) {
    const decoder = new Decoder(body)
    const value = {
      throttleTime: decoder.readInt32(),
      errorCode: decoder.readInt16(),
      errorMessage: decoder.readUVarIntString(),
      acquisitionLockTimeoutMs: decoder.readInt32(),
      responses: decoder.readUVarIntArray((d) => {
        const topicId = d.readUuid()
        const partitions = d.readUVarIntArray((d) => {
          const row = {
            partitionIndex: d.readInt32(),
            errorCode: d.readInt16(),
            errorMessage: d.readUVarIntString(),
            currentLeader: { leaderId: d.readInt32(), leaderEpoch: d.readInt32() },
          }
          d.readTaggedFields()
          d.readTaggedFields()
          return row
        })
        d.readTaggedFields()
        return { topicId, partitions }
      }),
      nodeEndpoints: decoder.readUVarIntArray((d) => {
        const node = {
          nodeId: d.readInt32(),
          host: d.readUVarIntString(),
          port: d.readInt32(),
          rack: d.readUVarIntString(),
        }
        d.readTaggedFields()
        return node
      }),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1))
      throw new Error('ShareAcknowledge v2 response has a byte after its tag buffer')
    return value
  },
  /**
   * @param {Buffer} body Request body without its header.
   * @returns {object} All request fields; int64 offsets remain exact decimal strings.
   * @throws {Error} Truncated body or trailing bytes after the tag buffer.
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const value = readBody(decoder)
    if (decoder.canReadBytes(1))
      throw new Error('ShareAcknowledge v2 request has a byte after its tag buffer')
    return value
  },
}
