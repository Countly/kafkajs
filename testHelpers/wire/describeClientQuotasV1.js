const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const readItems = require('./securityAdminArrays')
const expected = require('./expected').securityAdmin.describeClientQuotasV1
/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFields = (decoder) => {
  const value = {
    components: readItems(decoder, readRequestFieldsComponents, true, false),
    strict: decoder.readBoolean(),
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
const readRequestFieldsComponents = (decoder) => {
  const value = {
    entityType: decoder.readUVarIntString(),
    matchType: decoder.readInt8(),
    match: decoder.readUVarIntString(),
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
const writeResponseFields = ({ throttleTimeMs, errorCode, errorMessage, entries }) =>
  new Encoder()
    .writeInt32(throttleTimeMs)
    .writeInt16(errorCode)
    .writeUVarIntString(errorMessage)
    .writeUVarIntArray(entries == null ? null : entries.map(writeResponseFieldsEntries))
    .writeTaggedFields()

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsEntries = ({ entity, values }) =>
  new Encoder()
    .writeUVarIntArray(entity == null ? null : entity.map(writeResponseFieldsEntriesEntity))
    .writeUVarIntArray(values == null ? null : values.map(writeResponseFieldsEntriesValues))
    .writeTaggedFields()

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsEntriesEntity = ({ entityType, entityName }) =>
  new Encoder().writeUVarIntString(entityType).writeUVarIntString(entityName).writeTaggedFields()

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsEntriesValues = ({ key, value }) =>
  new Encoder().writeUVarIntString(key).writeDouble(value).writeTaggedFields()

/** Independent DescribeClientQuotas v1 scripted pair, pinned to literal byte vectors.
 * @type {api:string,apiKey:number,version:number,buildResponse:Function,readRequest:Function}
 */
module.exports = {
  api: 'DescribeClientQuotas',
  apiKey: 48,
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
