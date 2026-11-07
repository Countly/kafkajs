const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * Decodes the complete flexible telemetry response before interpreting its error code.
 * Quota time is exposed as clientSideThrottleTime so the connection observes it before parse.
 * Unknown tags are skipped by the inherited bounded decoder; trailing bytes are refused.
 * @param {Buffer} rawData response body only, after response header v1
 * @returns {Promise<object>} decoded protocol fields; throttleTime is zero under KIP-219
 * @throws {KafkaJSPartialMessageError} on truncation, illegal nulls or trailing bytes
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const response = {
    throttleTime: 0,
    clientSideThrottleTime: decoder.readInt32(),
    errorCode: decoder.readInt16(),
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
  api: 'PushTelemetry',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
  ],
}
