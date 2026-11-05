const Decoder = require('../../../decoder')
const Encoder = require('../../../encoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * SaslAuthenticate Response (Version: 0) => error_code error_message sasl_auth_bytes
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   sasl_auth_bytes => BYTES
 */

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)
  const errorMessage = decoder.readString()

  // This is necessary to make the response compatible with the original
  // mechanism protocols. They expect a byte response, which starts with
  // the size
  const authBytesEncoder = new Encoder().writeBytes(decoder.readBytes())
  const authBytes = authBytesEncoder.buffer

  return {
    errorCode,
    errorMessage,
    authBytes,
  }
}

/**
 * Throws a failed answer as the protocol error of its code, with that code's fixed text. The
 * broker's `error_message` is free text the server chose and may echo what the client sent, so it
 * is not copied into the error.
 * @param {{ errorCode: number }} data the decoded response
 * @returns {Promise<object>} the response, when it succeeded
 * @throws {import('../../../../errors').KafkaJSProtocolError} for a failure code
 */
const parse = async (data) => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
