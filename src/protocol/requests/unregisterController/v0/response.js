const { KafkaJSPartialMessageError } = require('../../../../errors')
const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')

/**
 * Decodes the whole body with strict framing and client-side throttling.
 * @param {Buffer} raw Body after the response header.
 * @returns {Promise<{throttleTimeMs: number, throttleTime: number, clientSideThrottleTime: number, errorCode: number, errorMessage: string|null}>}
 *   Null and empty messages stay distinct; unknown tagged fields are skipped.
 * @throws {KafkaJSPartialMessageError} On truncation, malformed tags or trailing bytes.
 */
const decode = async (raw) => {
  const decoder = new Decoder(raw)
  const throttleTimeMs = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  decoder.readTaggedFields()
  if (decoder.canReadBytes(1))
    throw new KafkaJSPartialMessageError('UnregisterController response has trailing bytes')
  return {
    throttleTimeMs,
    throttleTime: 0,
    clientSideThrottleTime: throttleTimeMs,
    errorCode,
    errorMessage,
  }
}

/**
 * Turns a broker error into the standard protocol error, keeping the broker's own text.
 * @param {{errorCode: number, errorMessage: string|null}} response Decoded body.
 * @returns {Promise<object>} The same response on success.
 * @throws {KafkaJSProtocolError} Typed by code; `brokerErrorMessage` is null, '' or the broker text,
 *   and non-empty text is appended to `message`.
 */
const parse = async (response) => {
  if (response.errorCode === 0) return response
  const error = createErrorFromCode(response.errorCode)
  error.brokerErrorMessage = response.errorMessage ?? null
  if (response.errorMessage) error.message = `${error.message}: ${response.errorMessage}`
  throw error
}

module.exports = { decode, parse }

module.exports.schema = {
  api: 'UnregisterController',
  version: 0,
  direction: 'response',
  fields: [
    {
      name: 'ThrottleTimeMs',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'ErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
  ],
}
