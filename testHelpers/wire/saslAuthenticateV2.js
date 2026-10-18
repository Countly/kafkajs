const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { saslAuthenticateV2: expected } = require('./expected')

/**
 * SaslAuthenticate v2 (flexible). The request carries the mechanism's payload; the response an
 * error code, an optional error message, the broker's payload and the session lifetime in
 * milliseconds (0 for no limit). The payloads are opaque bytes, read back as Buffers.
 *
 * Evidence: both messages are Java's own bytes (`fixtures/v2_*_java.json`, bytes only), and
 * `./expected` holds the fields of their generator. The lifetime is a decimal string.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/SaslAuthenticateResponse.json
 */
module.exports = {
  api: 'SaslAuthenticate',
  apiKey: apiKeys.SaslAuthenticate,
  version: 2,

  /**
   * @param {object} [fields] over the vector's: `errorCode`, `errorMessage`, `authBytes` (a Buffer
   *   or null) and `sessionLifetimeMs`
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...expected.response, ...fields }

    return new Encoder()
      .writeInt16(response.errorCode)
      .writeUVarIntString(response.errorMessage)
      .writeUVarIntBytes(response.authBytes)
      .writeInt64(response.sessionLifetimeMs)
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ authBytes: Buffer|null }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = { authBytes: decoder.readUVarIntBytes() }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('SaslAuthenticate v2 request has a byte after its tag buffer')
    }

    return request
  },
}
