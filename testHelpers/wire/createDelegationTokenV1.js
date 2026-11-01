const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const readItems = require('./securityAdminArrays')
const expected = require('./expected').securityAdmin.createDelegationTokenV1
/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFields = (decoder) => {
  const value = {
    renewers: readItems(decoder, readRequestFieldsRenewers, false, false),
    maxLifetimeMs: decoder.readInt64().toString(),
  }
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFieldsRenewers = (decoder) => {
  const value = { principalType: decoder.readString(), principalName: decoder.readString() }
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFields = ({
  errorCode,
  principalType,
  principalName,
  issueTimestampMs,
  expiryTimestampMs,
  maxTimestampMs,
  tokenId,
  hmac,
  throttleTimeMs,
}) =>
  new Encoder()
    .writeInt16(errorCode)
    .writeString(principalType)
    .writeString(principalName)
    .writeInt64(issueTimestampMs)
    .writeInt64(expiryTimestampMs)
    .writeInt64(maxTimestampMs)
    .writeString(tokenId)
    .writeBytes(hmac)
    .writeInt32(throttleTimeMs)

/** Independent CreateDelegationToken v1 scripted pair, pinned to literal byte vectors.
 * @type {api:string,apiKey:number,version:number,buildResponse:Function,readRequest:Function}
 */
module.exports = {
  api: 'CreateDelegationToken',
  apiKey: 38,
  version: 1,
  /** Builds a response without importing any production request/response codec.
   * @param {object} [fields] overrides for literal response fields
   * @returns {Buffer} complete body including all nested tags
   */
  buildResponse(fields = {}) {
    return writeResponseFields({ ...expected.response, ...fields }).buffer
  },
  /** Reads exactly one request body and refuses any trailing bytes.
   * @param {Buffer} body request without its header
   * @returns {object} fields including null/empty selectors and decimal int64s
   * @throws {Error} malformed or trailing bytes
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const fields = readRequestFields(decoder)
    if (decoder.canReadBytes(1)) throw new KafkaJSPartialMessageError('Unexpected request bytes')
    return fields
  },
}
