const { promisify } = require('util')
const zlib = require('zlib')

// node:zlib gained zstd in Node 22.15 / 23.8 and it is still Stability 1 (Experimental)
// through Node 26, so detect it instead of assuming it. Kafka's own default level is 3.
const DEFAULT_COMPRESSION_LEVEL = 3

// eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature-detected, not called unless supported
const isSupported = typeof zlib.zstdCompressSync === 'function'

/* eslint-disable n/no-unsupported-features/node-builtins -- guarded by isSupported above */
const zstdCompress = isSupported ? promisify(zlib.zstdCompress) : null
const zstdDecompress = isSupported ? promisify(zlib.zstdDecompress) : null
/* eslint-enable n/no-unsupported-features/node-builtins */

module.exports = {
  isSupported,

  /**
   * @param {{ buffer: Buffer }} encoder
   * @param {{ level?: number }} [options]
   * @returns {Promise<Buffer>}
   */
  async compress(encoder, { level = DEFAULT_COMPRESSION_LEVEL } = {}) {
    return await zstdCompress(encoder.buffer, {
      // eslint-disable-next-line n/no-unsupported-features/node-builtins -- guarded by isSupported above
      params: { [zlib.constants.ZSTD_c_compressionLevel]: level },
    })
  },

  /**
   * @param {Buffer} buffer
   * @returns {Promise<Buffer>}
   */
  async decompress(buffer) {
    return await zstdDecompress(buffer)
  },
}
