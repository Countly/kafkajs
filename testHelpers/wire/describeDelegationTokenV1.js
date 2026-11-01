const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const readItems = require('./securityAdminArrays')
const expected = require('./expected').securityAdmin.describeDelegationTokenV1
/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFields = (decoder) => {
  const value = { owners: readItems(decoder, readRequestFieldsOwners, false, true) }
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFieldsOwners = (decoder) => {
  const value = { principalType: decoder.readString(), principalName: decoder.readString() }
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFields = ({ errorCode, tokens, throttleTimeMs }) =>
  new Encoder()
    .writeInt16(errorCode)
    .writeArray(
      tokens == null ? [] : tokens.map(writeResponseFieldsTokens),
      undefined,
      tokens == null ? -1 : tokens.length
    )
    .writeInt32(throttleTimeMs)

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsTokens = ({
  principalType,
  principalName,
  issueTimestamp,
  expiryTimestamp,
  maxTimestamp,
  tokenId,
  hmac,
  renewers,
}) =>
  new Encoder()
    .writeString(principalType)
    .writeString(principalName)
    .writeInt64(issueTimestamp)
    .writeInt64(expiryTimestamp)
    .writeInt64(maxTimestamp)
    .writeString(tokenId)
    .writeBytes(hmac)
    .writeArray(
      renewers == null ? [] : renewers.map(writeResponseFieldsTokensRenewers),
      undefined,
      renewers == null ? -1 : renewers.length
    )

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsTokensRenewers = ({ principalType, principalName }) =>
  new Encoder().writeString(principalType).writeString(principalName)

/** Independent DescribeDelegationToken v1 scripted pair, pinned to literal byte vectors.
 * @type {api:string,apiKey:number,version:number,buildResponse:Function,readRequest:Function}
 */
module.exports = {
  api: 'DescribeDelegationToken',
  apiKey: 41,
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
