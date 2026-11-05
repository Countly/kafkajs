const { trustedError } = require('../../../../broker/saslAuthenticator/failure')
const Decoder = require('../../../decoder')
const { KafkaJSSASLAuthenticationError } = require('../../../../errors')

const ATTRIBUTE_REGEX = /^([a-zA-Z])=(.*)$/
const SERVER_ATTRIBUTES = new Set(['r', 's', 'i', 'e', 'v'])

/**
 * A SCRAM server message (RFC 5802 section 5.1): comma-separated `<letter>=<value>` attributes. Only the
 * attributes a server sends are accepted (`r`, `s` and `i` first, then `v` or `e`); the reserved
 * mandatory extension `m` fails the exchange, as RFC 5802 requires of its parser, and so does an
 * unknown or a repeated attribute. Errors name the problem, never the received text.
 */
module.exports = {
  /**
   * @param {Buffer} rawData INT32-prefixed mechanism bytes
   * @returns {Promise<Buffer|null>} the server message
   */
  decode: async (rawData) => {
    return new Decoder(rawData).readBytes()
  },

  /**
   * @param {Buffer|null} data the server message
   * @returns {Promise<{ original: string, r?: string, s?: string, i?: string, e?: string, v?: string }>}
   * @throws {KafkaJSSASLAuthenticationError} for a malformed message or an attribute it may not carry
   */
  parse: async (data) => {
    const original = Buffer.isBuffer(data) ? data.toString() : ''
    const attributes = {}
    for (const part of original.split(',')) {
      const match = ATTRIBUTE_REGEX.exec(part)
      if (!match) {
        throw trustedError(KafkaJSSASLAuthenticationError, 'Malformed SCRAM server message')
      }

      const [, key, value] = match
      if (key === 'm') {
        throw trustedError(
          KafkaJSSASLAuthenticationError,
          'The SCRAM server message requires an unsupported mandatory extension'
        )
      }

      if (!SERVER_ATTRIBUTES.has(key) || Object.hasOwn(attributes, key)) {
        throw trustedError(
          KafkaJSSASLAuthenticationError,
          'The SCRAM server message has an unknown or repeated attribute'
        )
      }

      attributes[key] = value
    }

    return { original, ...attributes }
  },
}
