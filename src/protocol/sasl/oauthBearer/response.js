const Decoder = require('../../decoder')

/**
 * The broker's answer to the initial client response (RFC 7628 section 3.2). No bytes mean it
 * accepted the token; any bytes are an error challenge the client must answer (section 3.2.3).
 */
module.exports = {
  /**
   * @param {Buffer} rawData INT32-prefixed mechanism bytes
   * @returns {Promise<Buffer|null>} the bytes the broker sent
   */
  decode: async (rawData) => new Decoder(rawData).readBytes(),

  /**
   * @param {Buffer|null} data the bytes the broker sent
   * @returns {Promise<boolean>} true when the broker sent an error challenge
   */
  parse: async (data) => data != null && data.length > 0,
}
