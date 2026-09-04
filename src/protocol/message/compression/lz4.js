/**
 * @module protocol/message/compression/lz4
 * @description Kafka's LZ4 frame format (KIP-57). Node has no built-in LZ4, so this codec is
 * backed by the optional native dependency `lz4-napi`, which runs on the libuv threadpool and
 * does not block the event loop. Install it to enable LZ4: `npm install lz4-napi`.
 */
const { KafkaJSNotImplemented } = require('../../../errors')

let lz4 = null
try {
  lz4 = require('lz4-napi')
} catch (e) {
  lz4 = null
}

const NOT_SUPPORTED_MESSAGE =
  'LZ4 compression requires the optional dependency "lz4-napi". Install it with `npm install lz4-napi`, ' +
  'or register your own codec with CompressionCodecs[CompressionTypes.LZ4].'

const unavailable = () => {
  throw new KafkaJSNotImplemented(NOT_SUPPORTED_MESSAGE)
}

module.exports = {
  /** @type {boolean} Whether `lz4-napi` loaded successfully; `index.js` uses this to decide
   *  between returning this codec and throwing `KafkaJSNotImplemented`. */
  isSupported: lz4 != null,

  /** The message thrown when `isSupported` is false; exported so `index.js` and this module's
   *  own guard never drift apart. */
  NOT_SUPPORTED_MESSAGE,

  /**
   * @param {{ buffer: Buffer }} encoder
   * @returns {Promise<Buffer>}
   * @throws {KafkaJSNotImplemented} when `lz4-napi` is not installed
   */
  async compress(encoder) {
    if (!lz4) unavailable()
    return await lz4.compressFrame(encoder.buffer)
  },

  /**
   * @param {Buffer} buffer
   * @returns {Promise<Buffer>}
   * @throws {KafkaJSNotImplemented} when `lz4-napi` is not installed
   */
  async decompress(buffer) {
    if (!lz4) unavailable()
    return await lz4.decompressFrame(buffer)
  },
}
