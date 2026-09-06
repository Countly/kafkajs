const Decoder = require('../../../decoder')
const Encoder = require('../../../encoder')
const { parse: parseV0 } = require('../v0/response')
const { failIfVersionNotSupported } = require('../../../error')

/**
 * SaslAuthenticateResponse (Version: 2) - flexible (KIP-482), response header v1
 *
 * ErrorCode => INT16
 * ErrorMessage => COMPACT_NULLABLE_STRING
 * AuthBytes => COMPACT_BYTES
 * SessionLifetimeMs => INT64
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/SaslAuthenticateResponse.json (Apache Kafka 4.3.1)
 * @param {Buffer} rawData
 * @returns {Promise<{errorCode: number, errorMessage: string|null, authBytes: Buffer, sessionLifetimeMs: string}>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)
  const errorMessage = decoder.readUVarIntString()

  // Re-wrapped with an INT32 length so a mechanism provider's response.decode() still finds the
  // prefix it has always been handed. The compact framing stops at this codec's boundary.
  const authBytes = new Encoder().writeBytes(decoder.readUVarIntBytes()).buffer
  const sessionLifetimeMs = decoder.readInt64().toString()

  decoder.readTaggedFields()

  return { errorCode, errorMessage, authBytes, sessionLifetimeMs }
}

module.exports = { decode, parse: parseV0 }

module.exports.schema = {
  api: 'SaslAuthenticate',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'AuthBytes', type: 'bytes', versions: '0+' },
    { name: 'SessionLifetimeMs', type: 'int64', versions: '1+' },
  ],
}
