const Encoder = require('../../../encoder')
const Decoder = require('../../../decoder')
const { SaslAuthenticate: apiKey } = require('../../apiKeys')
const { KafkaJSSASLAuthenticationError } = require('../../../../errors')

const INT32_PREFIX_SIZE = 4
const NULL_LENGTH = -1

/**
 * Every SASL mechanism provider in this client - built-in and custom alike - returns
 * INT32-length-prefixed bytes from `request.encode()`, and reads that same prefix back in
 * `response.decode()`. That contract is public and documented, so v2 does not change it: it
 * strips the prefix here and writes the remainder as COMPACT_BYTES.
 *
 * @param {Buffer} authBytes the provider's INT32-prefixed buffer
 * @param {string} mechanism named in the error, so a provider bug is attributable
 * @returns {Buffer|null} the payload, or null for the legal `writeBytes(null)` encoding
 * @throws {KafkaJSSASLAuthenticationError} when the prefix disagrees with the buffer's length,
 *   which is a programming error in the provider and better caught here than on the wire
 */
const stripLengthPrefix = (authBytes, mechanism) => {
  const decoder = new Decoder(authBytes)
  const length = decoder.readInt32()

  if (length === NULL_LENGTH) {
    return null
  }

  if (length !== authBytes.length - INT32_PREFIX_SIZE) {
    throw new KafkaJSSASLAuthenticationError(
      `SASL mechanism "${mechanism}" produced a malformed auth payload: its length prefix says ${length} bytes but ${
        authBytes.length - INT32_PREFIX_SIZE
      } follow`
    )
  }

  return authBytes.slice(INT32_PREFIX_SIZE)
}

/**
 * SaslAuthenticateRequest (Version: 2) - flexible (KIP-482), request header v2
 *
 * AuthBytes => COMPACT_BYTES
 * TAG_BUFFER
 *
 * SaslHandshake, on the same connection, is not flexible and keeps request header v1.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/SaslAuthenticateRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {Buffer} options.authBytes INT32-length-prefixed, as every mechanism provider produces
 * @param {string} [options.mechanism='unknown'] used only in the error message above
 */
module.exports = ({ authBytes, mechanism = 'unknown' }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'SaslAuthenticate',
  encode: async () =>
    new Encoder().writeUVarIntBytes(stripLengthPrefix(authBytes, mechanism)).writeTaggedFields(),
})

module.exports.schema = {
  api: 'SaslAuthenticate',
  version: 2,
  direction: 'request',
  fields: [{ name: 'AuthBytes', type: 'bytes', versions: '0+' }],
}
