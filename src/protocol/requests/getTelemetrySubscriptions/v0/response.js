const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/** Bounds on broker-supplied subscription metadata, counted in entries and raw UTF-8 bytes. */
const LIMITS = Object.freeze({ codecs: 128, prefixes: 256, prefixBytes: 256, totalBytes: 16384 })
/** ThrottleTimeMs, ErrorCode, ClientInstanceId and SubscriptionId. */
const LEADING_BYTES = 26
/** PushIntervalMs, TelemetryMaxBytes and DeltaTemporality. */
const SCALAR_BYTES = 9

/**
 * Reads a compact array count without allocating the array.
 * @param {Decoder} scan cursor positioned on the count
 * @param {string} field the protocol field name
 * @param {number} max largest accepted entry count
 * @returns {number} the entry count
 * @throws {KafkaJSPartialMessageError} on the compact null marker or a count above max
 */
const readCount = (scan, field, max) => {
  const length = scan.readUVarInt()
  if (length === 0) throw new KafkaJSPartialMessageError(`${field} cannot be null`)
  if (length - 1 > max) throw new KafkaJSPartialMessageError(`${field} exceeds ${max} entries`)
  return length - 1
}

/**
 * Refuses nulls and oversized subscription metadata by walking compact counts and string length
 * prefixes and skipping their bytes, so no array is built and no string is converted first.
 * Strings are bounded by raw UTF-8 bytes. Tags and trailing bytes are left to decode.
 * @param {Buffer} rawData response body only, after response header v1
 * @returns {void}
 * @throws {KafkaJSPartialMessageError} on truncation, a null field, or a count or byte limit breach
 */
const preflight = (rawData) => {
  const scan = new Decoder(rawData)
  scan.ensureReadable(LEADING_BYTES, 'GetTelemetrySubscriptions response')
  scan.offset = LEADING_BYTES
  const codecs = readCount(scan, 'AcceptedCompressionTypes', LIMITS.codecs)
  scan.ensureReadable(codecs + SCALAR_BYTES, 'AcceptedCompressionTypes')
  scan.offset += codecs + SCALAR_BYTES
  const prefixes = readCount(scan, 'RequestedMetrics', LIMITS.prefixes)
  scan.ensureReadable(prefixes, 'RequestedMetrics')
  let total = 0
  for (let index = 0; index < prefixes; index++) {
    const length = scan.readUVarInt()
    if (length === 0)
      throw new KafkaJSPartialMessageError('RequestedMetrics element cannot be null')
    const bytes = length - 1
    if (bytes > LIMITS.prefixBytes)
      throw new KafkaJSPartialMessageError(
        `RequestedMetrics element exceeds ${LIMITS.prefixBytes} bytes`
      )
    total += bytes
    if (total > LIMITS.totalBytes)
      throw new KafkaJSPartialMessageError(`RequestedMetrics exceeds ${LIMITS.totalBytes} bytes`)
    scan.ensureReadable(bytes, 'RequestedMetrics element')
    scan.offset += bytes
  }
}

/**
 * Decodes the complete flexible telemetry response before interpreting its error code.
 * Metadata limits and illegal nulls are refused by a preflight before any array is built or string
 * converted; the generic decoder limits are unchanged.
 * Quota time is exposed as clientSideThrottleTime so the connection observes it before parse.
 * Unknown tags are skipped by the inherited bounded decoder; trailing bytes are refused.
 * @param {Buffer} rawData response body only, after response header v1
 * @returns {Promise<object>} decoded protocol fields; throttleTime is zero under KIP-219
 * @throws {KafkaJSPartialMessageError} on truncation, illegal nulls, limit breaches or trailing bytes
 */
const decode = async (rawData) => {
  preflight(rawData)
  const decoder = new Decoder(rawData)
  const response = {
    throttleTime: 0,
    clientSideThrottleTime: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    clientInstanceId: decoder.readUuid(),
    subscriptionId: decoder.readInt32(),
    acceptedCompressionTypes: decoder.readUVarIntArray((item) => item.readInt8()),
    pushIntervalMs: decoder.readInt32(),
    telemetryMaxBytes: decoder.readInt32(),
    deltaTemporality: decoder.readBoolean(),
    requestedMetrics: decoder.readUVarIntArray((item) => item.readUVarIntString()),
  }
  decoder.readTaggedFields()
  if (decoder.canReadBytes(1))
    throw new KafkaJSPartialMessageError('Unexpected telemetry response bytes')
  return response
}

/**
 * Returns the unchanged successful response or throws the existing numeric protocol error.
 * Retains quota time on refusals for the dedicated telemetry error policy; no retrier changes.
 * @param {object} data the result of decode
 * @returns {Promise<object>} data on error code zero
 * @throws {import('../../../../errors').KafkaJSProtocolError} for a broker error, including 35
 */
const parse = async (data) => {
  if (failure(data.errorCode)) {
    const error = createErrorFromCode(data.errorCode)
    error.clientSideThrottleTime = data.clientSideThrottleTime
    throw error
  }
  return data
}

module.exports = { decode, parse }

module.exports.schema = {
  api: 'GetTelemetrySubscriptions',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ClientInstanceId', type: 'uuid', versions: '0+' },
    { name: 'SubscriptionId', type: 'int32', versions: '0+' },
    { name: 'AcceptedCompressionTypes', type: '[]int8', versions: '0+' },
    { name: 'PushIntervalMs', type: 'int32', versions: '0+' },
    { name: 'TelemetryMaxBytes', type: 'int32', versions: '0+' },
    { name: 'DeltaTemporality', type: 'bool', versions: '0+' },
    { name: 'RequestedMetrics', type: '[]string', versions: '0+' },
  ],
}
