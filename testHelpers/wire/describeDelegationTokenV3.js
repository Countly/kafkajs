const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const readItems = require('./securityAdminArrays')
const expected = require('./expected').securityAdmin.describeDelegationTokenV3
/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFields = (decoder) => {
  const value = { owners: readItems(decoder, readRequestFieldsOwners, true, true) }
  decoder.readTaggedFields()
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFieldsOwners = (decoder) => {
  const value = {
    principalType: decoder.readUVarIntString(),
    principalName: decoder.readUVarIntString(),
  }
  decoder.readTaggedFields()
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
    .writeUVarIntArray(tokens == null ? null : tokens.map(writeResponseFieldsTokens))
    .writeInt32(throttleTimeMs)
    .writeTaggedFields()

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsTokens = ({
  principalType,
  principalName,
  tokenRequesterPrincipalType,
  tokenRequesterPrincipalName,
  issueTimestamp,
  expiryTimestamp,
  maxTimestamp,
  tokenId,
  hmac,
  renewers,
}) =>
  new Encoder()
    .writeUVarIntString(principalType)
    .writeUVarIntString(principalName)
    .writeUVarIntString(tokenRequesterPrincipalType)
    .writeUVarIntString(tokenRequesterPrincipalName)
    .writeInt64(issueTimestamp)
    .writeInt64(expiryTimestamp)
    .writeInt64(maxTimestamp)
    .writeUVarIntString(tokenId)
    .writeUVarIntBytes(hmac)
    .writeUVarIntArray(renewers == null ? null : renewers.map(writeResponseFieldsTokensRenewers))
    .writeTaggedFields()

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsTokensRenewers = ({ principalType, principalName }) =>
  new Encoder()
    .writeUVarIntString(principalType)
    .writeUVarIntString(principalName)
    .writeTaggedFields()

/** Independent DescribeDelegationToken v3 scripted pair, pinned to literal byte vectors.
 * @type {api:string,apiKey:number,version:number,buildResponse:Function,readRequest:Function}
 */
module.exports = {
  api: 'DescribeDelegationToken',
  apiKey: 41,
  version: 3,
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
