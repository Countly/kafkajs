const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const literal = require('./expected').shareFetchV2

/**
 * Uses the one independently declared record fixture when Java fields describe that exact record.
 * @param {object[]} [messages=[]] Literal Java record fields with hex key/value.
 * @returns {Buffer} The pinned record body or empty Records.
 * @throws {Error} A record other than the declared fixture requires explicit records hex.
 */
const recordsOf = (messages = []) => {
  if (messages.length === 0) return Buffer.alloc(0)
  if (
    messages.length !== 1 ||
    JSON.stringify(messages[0]) !== JSON.stringify(literal.recordMessage)
  ) {
    throw new Error('Supply records hex for a record outside the declared wire fixture')
  }
  return Buffer.from(literal.recordsHex, 'hex')
}

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeCurrentLeader = (value, tags) =>
  new Encoder().writeInt32(value.leaderId).writeInt32(value.leaderEpoch).writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodeAcquiredRecords = (value, tags) =>
  new Encoder()
    .writeInt64(value.firstOffset)
    .writeInt64(value.lastOffset)
    .writeInt16(value.deliveryCount)
    .writeTaggedFields(tags)

/** @param {object} value Literal wire fields. @param {object[]} tags Unknown tags for each structure. @returns {Encoder} Encoded structure. @throws {Error} Invalid primitive input. */
const encodePartitions = (value, tags) =>
  new Encoder()
    .writeInt32(value.partitionIndex)
    .writeInt16(value.errorCode)
    .writeUVarIntString(value.errorMessage)
    .writeInt16(value.acknowledgeErrorCode)
    .writeUVarIntString(value.acknowledgeErrorMessage)
    .writeEncoder(encodeCurrentLeader(value.currentLeader, tags))
    .writeUVarIntBytes(
      value.records !== undefined ? Buffer.from(value.records, 'hex') : recordsOf(value.messages)
    )
    .writeUVarIntArray(
      value.acquiredRecords === null
        ? null
        : value.acquiredRecords.map((entry) => encodeAcquiredRecords(entry, tags))
    )
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
const readForgottenTopicsData = (decoder) => {
  const value = {
    topicId: decoder.readUuid(),
    partitions: decoder.readUVarIntArray((entry) => entry.readInt32()),
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
    maxWaitMs: decoder.readInt32(),
    minBytes: decoder.readInt32(),
    maxBytes: decoder.readInt32(),
    maxRecords: decoder.readInt32(),
    batchSize: decoder.readInt32(),
    shareAcquireMode: decoder.readInt8(),
    isRenewAck: decoder.readBoolean(),
    topics: decoder.readUVarIntArray(readTopics),
    forgottenTopicsData: decoder.readUVarIntArray(readForgottenTopicsData),
  }
  decoder.readTaggedFields()
  return value
}

/**
 * Independent ShareFetch v2 wire pair; uses protocol primitives and literal released-source fields.
 * It models no broker state and imports no request encoder, response decoder or share codec helper.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/ShareFetchResponse.json
 */
module.exports = {
  api: 'ShareFetch',
  apiKey: apiKeys.ShareFetch,
  version: 2,
  /**
   * @param {object} [fields] Response fields over the literal defaults; throttleTime is the wire throttle.
   * @param {object} [options] Independent tagged-field fixture control.
   * @param {boolean} [options.tagged=false] Emit tag 10 with byte 0x61 at every envelope structure.
   * @returns {Buffer} Response body without a header; a partition's `records` is a hex string.
   * @throws {Error} Invalid primitive input or an undeclared record fixture.
   */
  buildResponse(fields = {}, { tagged = false } = {}) {
    const tags = tagged ? [{ tag: 10, value: Buffer.from([0x61]) }] : []
    return encodeResponse({ ...literal.response, ...fields }, tags).buffer
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
      throw new Error('ShareFetch v2 request has a byte after its tag buffer')
    return value
  },
}
