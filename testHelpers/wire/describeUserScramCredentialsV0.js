const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const readItems = require('./securityAdminArrays')
const expected = require('./expected').securityAdmin.describeUserScramCredentialsV0
/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFields = (decoder) => {
  const value = { users: readItems(decoder, readRequestFieldsUsers, true, true) }
  decoder.readTaggedFields()
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFieldsUsers = (decoder) => {
  const value = { name: decoder.readUVarIntString() }
  decoder.readTaggedFields()
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFields = ({ throttleTimeMs, errorCode, errorMessage, results }) =>
  new Encoder()
    .writeInt32(throttleTimeMs)
    .writeInt16(errorCode)
    .writeUVarIntString(errorMessage)
    .writeUVarIntArray(results == null ? null : results.map(writeResponseFieldsResults))
    .writeTaggedFields()

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsResults = ({ user, errorCode, errorMessage, credentialInfos }) =>
  new Encoder()
    .writeUVarIntString(user)
    .writeInt16(errorCode)
    .writeUVarIntString(errorMessage)
    .writeUVarIntArray(
      credentialInfos == null
        ? null
        : credentialInfos.map(writeResponseFieldsResultsCredentialInfos)
    )
    .writeTaggedFields()

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsResultsCredentialInfos = ({ mechanism, iterations }) =>
  new Encoder().writeInt8(mechanism).writeInt32(iterations).writeTaggedFields()

/** Independent DescribeUserScramCredentials v0 scripted pair, pinned to literal byte vectors.
 * @type {api:string,apiKey:number,version:number,buildResponse:Function,readRequest:Function}
 */
module.exports = {
  api: 'DescribeUserScramCredentials',
  apiKey: 50,
  version: 0,
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
