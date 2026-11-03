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

const FRAME_MAGIC = 0xfd2fb528
const BLOCK_RLE = 1
const FCS_BYTES = [0, 2, 4, 8]
const DICTIONARY_ID_BYTES = [0, 1, 2, 4]

/**
 * Throws what Node 24.20+/26.6+ throws for a stream cut short, so that every Node version
 * refuses it the same way.
 *
 * @returns {never}
 * @throws {Error} `code: 'Z_BUF_ERROR'`, `errno: -5`
 */
const throwTruncated = () => {
  throw Object.assign(new Error('unexpected end of file'), { code: 'Z_BUF_ERROR', errno: -5 })
}

/**
 * Walks the first frame of a zstd stream (RFC 8878 section 3.1) and throws when it ends before
 * its last block or checksum. Node 22's `zlib` returns the part it could decode, with no error, for such
 * input; Node 24.20+/26.6+ reject it (nodejs/node#64593). Like `zlib`, it looks at the first
 * frame only. An unknown magic and a reserved block type are left to `zlib`, which rejects them
 * on every version; the reserved header bit does not change the layout, so such a frame is walked
 * too and its bit left to `zlib`. O(blocks), allocates nothing.
 *
 * @param {Buffer} buffer
 * @returns {void}
 * @throws {Error} `Z_BUF_ERROR` when the first frame is cut short
 */
const assertFrameComplete = (buffer) => {
  const end = buffer.length
  if (end < 5) throwTruncated()
  if (buffer.readUInt32LE(0) !== FRAME_MAGIC) return

  const descriptor = buffer[4]
  const singleSegment = (descriptor >> 5) & 1
  let fcsBytes = FCS_BYTES[descriptor >> 6]
  if (fcsBytes === 0 && singleSegment) fcsBytes = 1
  let offset = 5 + (singleSegment ? 0 : 1) + DICTIONARY_ID_BYTES[descriptor & 3] + fcsBytes

  for (let last = false; !last;) {
    if (offset + 3 > end) throwTruncated()
    const header = buffer.readUIntLE(offset, 3)
    const type = (header >> 1) & 3
    if (type === 3) return
    last = (header & 1) === 1
    offset += 3 + (type === BLOCK_RLE ? 1 : header >> 3)
  }
  if (descriptor & 0x04) offset += 4
  if (offset > end) throwTruncated()
}

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
   * @throws {Error} for a corrupt stream; `code: 'Z_BUF_ERROR'` for a truncated one, on every Node
   *   version (Node 22's `zlib` alone would return the decoded part without an error)
   */
  async decompress(buffer, { maxOutputLength } = {}) {
    if (!isSupported) unavailable()
    assertFrameComplete(buffer)
    return await zstdDecompress(buffer, { maxOutputLength })
  },
}
