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
   * @returns {Promise}
   */
  async decompress(buffer) {
    return await unzip(buffer)
  },
}
