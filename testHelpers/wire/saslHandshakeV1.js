const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')

/**
 * SaslHandshake v1 (not flexible, request header v1). The request names the mechanism the client
 * wants; the response is an error code and the mechanisms the broker enables.
 *
 * Evidence: no vector exists for this message. Both directions are checked against the
 * hand-computed goldens of `./expected` (`saslHandshakeV1`), assembled from the message layout
 * with plain `Buffer` calls, never with a codec of this repository: the request is an INT16-prefixed
 * string, the response an INT16 error code and an INT32-counted array of INT16-prefixed strings.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/SaslHandshakeResponse.json
 */
module.exports = {
  api: 'SaslHandshake',
  apiKey: apiKeys.SaslHandshake,
  version: 1,

  /**
   * @param {{ errorCode?: number, enabledMechanisms?: string[] }} [fields] over the defaults: no
   *   error and `PLAIN` enabled
   * @returns {Buffer} the response body
   */
  buildResponse({ errorCode = 0, enabledMechanisms = ['PLAIN'] } = {}) {
    return new Encoder().writeInt16(errorCode).writeArray(enabledMechanisms, 'string').buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ mechanism: string }}
   * @throws {Error} when the body is cut short or a byte follows the mechanism
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = { mechanism: decoder.readString() }
    if (decoder.canReadBytes(1)) {
      throw new Error('SaslHandshake v1 request has a byte after its mechanism')
    }

    return request
  },
}
