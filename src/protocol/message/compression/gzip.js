const { promisify } = require('util')
const zlib = require('zlib')

const gzip = promisify(zlib.gzip)
const unzip = promisify(zlib.unzip)

module.exports = {
  /**
   * @param {Encoder} encoder
   * @param {{ level?: number }} [options] gzip level; omitted keeps the zlib default
   * @returns {Promise<Buffer>} compressed bytes
   * @throws {Error} for an invalid zlib option or compression failure
   */
  async compress(encoder, { level } = {}) {
    return await gzip(encoder.buffer, { level })
  },

  /**
   * @param {Buffer} buffer
   * @param {{ maxOutputLength?: number }} [options] the most bytes to produce; Node refuses the
   *   output with `ERR_BUFFER_TOO_LARGE` while it is made, not after
   * @returns {Promise<Buffer>}
   * @throws {RangeError} `ERR_BUFFER_TOO_LARGE` past `maxOutputLength`
   * @throws {Error} for a corrupt or truncated stream (a zlib error with a `Z_` code)
   */
  async decompress(buffer, { maxOutputLength } = {}) {
    return await unzip(buffer, { maxOutputLength })
  },
}
