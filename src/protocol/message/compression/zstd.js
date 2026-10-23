/**
 * @module protocol/message/compression/zstd
 * @description Zstandard compression (KIP-110) via `node:zlib`'s `zstdCompress`/`zstdDecompress`,
 * available on Node >= 22.15 / >= 23.8 and still Stability 1 (Experimental) through Node 26 —
 * detected at load time rather than assumed, so requiring this module never throws. Kafka's own
 * default compression level is 3.
 */
const { promisify } = require('util')
const zlib = require('zlib')
const { KafkaJSNotImplemented } = require('../../../errors')

const DEFAULT_COMPRESSION_LEVEL = 3
const NOT_SUPPORTED_MESSAGE =
  'ZSTD compression requires zstd support in node:zlib (Node >= 22.15 or >= 23.8)'

// eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature-detected, not called unless supported
const isSupported = typeof zlib.zstdCompressSync === 'function'

/* eslint-disable n/no-unsupported-features/node-builtins -- guarded by isSupported above */
const zstdCompress = isSupported ? promisify(zlib.zstdCompress) : null
const zstdDecompress = isSupported ? promisify(zlib.zstdDecompress) : null
/* eslint-enable n/no-unsupported-features/node-builtins */

const unavailable = () => {
  throw new KafkaJSNotImplemented(NOT_SUPPORTED_MESSAGE)
}

module.exports = {
  /** @type {boolean} Whether this Node build has zstd support in `node:zlib`; `index.js` uses
   *  this to decide between returning this codec and throwing `KafkaJSNotImplemented`. */
  isSupported,

  /** The message thrown when `isSupported` is false; exported so `index.js` and this module's
   *  own guard never drift apart. */
  NOT_SUPPORTED_MESSAGE,

  /**
   * @param {{ buffer: Buffer }} encoder
   * @param {{ level?: number }} [options] Compression level (default 3, Kafka's own default).
   *   The producer forwards its configured level through every Produce request version.
   * @returns {Promise<Buffer>}
   * @throws {KafkaJSNotImplemented} when this Node build has no zstd support in `node:zlib`
   */
  async compress(encoder, { level = DEFAULT_COMPRESSION_LEVEL } = {}) {
    if (!isSupported) unavailable()
    return await zstdCompress(encoder.buffer, {
      // eslint-disable-next-line n/no-unsupported-features/node-builtins -- guarded by isSupported above
      params: { [zlib.constants.ZSTD_c_compressionLevel]: level },
    })
  },

  /**
   * @param {Buffer} buffer
   * @param {{ maxOutputLength?: number }} [options] the most bytes to produce; Node refuses the
   *   output with `ERR_BUFFER_TOO_LARGE` while it is made, not after
   * @returns {Promise<Buffer>}
   * @throws {KafkaJSNotImplemented} when this Node build has no zstd support in `node:zlib`
   * @throws {RangeError} `ERR_BUFFER_TOO_LARGE` past `maxOutputLength`
   * @throws {Error} for a corrupt or truncated stream
   */
  async decompress(buffer, { maxOutputLength } = {}) {
    if (!isSupported) unavailable()
    return await zstdDecompress(buffer, { maxOutputLength })
  },
}
