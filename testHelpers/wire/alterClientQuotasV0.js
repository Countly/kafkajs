const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const { KafkaJSPartialMessageError } = require('../../src/errors')
const readItems = require('./securityAdminArrays')
const expected = require('./expected').securityAdmin.alterClientQuotasV0
/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFields = (decoder) => {
  const value = {
    entries: readItems(decoder, readRequestFieldsEntries, false, false),
    validateOnly: decoder.readBoolean(),
  }
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFieldsEntries = (decoder) => {
  const value = {
    entity: readItems(decoder, readRequestFieldsEntriesEntity, false, false),
    ops: readItems(decoder, readRequestFieldsEntriesOps, false, false),
  }
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFieldsEntriesEntity = (decoder) => {
  const value = { entityType: decoder.readString(), entityName: decoder.readString() }
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const readRequestFieldsEntriesOps = (decoder) => {
  const value = {
    key: decoder.readString(),
    value: decoder.readDouble(),
    remove: decoder.readBoolean(),
  }
  return value
}

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFields = ({ throttleTimeMs, entries }) =>
  new Encoder()
    .writeInt32(throttleTimeMs)
    .writeArray(
      entries == null ? [] : entries.map(writeResponseFieldsEntries),
      undefined,
      entries == null ? -1 : entries.length
    )

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsEntries = ({ errorCode, errorMessage, entity }) =>
  new Encoder()
    .writeInt16(errorCode)
    .writeString(errorMessage)
    .writeArray(
      entity == null ? [] : entity.map(writeResponseFieldsEntriesEntity),
      undefined,
      entity == null ? -1 : entity.length
    )

/**
 * Processes a nested wire struct in schema order.
 * @param {object} value fields or positioned decoder
 * @returns {object} encoder or decoded fields
 * @throws {Error} malformed input or framing
 */
const writeResponseFieldsEntriesEntity = ({ entityType, entityName }) =>
  new Encoder().writeString(entityType).writeString(entityName)

/** Independent AlterClientQuotas v0 scripted pair, pinned to literal byte vectors.
 * @type {api:string,apiKey:number,version:number,buildResponse:Function,readRequest:Function}
 */
module.exports = {
  api: 'AlterClientQuotas',
  apiKey: 49,
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
