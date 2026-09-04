const { KafkaJSNotImplemented } = require('../../../errors')

/**
 * Kafka uses the LZ4 frame format (KIP-57). Node has no built-in LZ4, so this codec is backed
 * by the optional native dependency `lz4-napi`, which runs on the libuv threadpool and does
 * not block the event loop. Install it to enable LZ4:  npm install lz4-napi
 */
let lz4 = null
try {
  lz4 = require('lz4-napi')
} catch (e) {
  lz4 = null
}

const unavailable = () => {
  throw new KafkaJSNotImplemented(
    'LZ4 compression requires the optional dependency "lz4-napi". Install it with `npm install lz4-napi`, ' +
      'or register your own codec with CompressionCodecs[CompressionTypes.LZ4].'
  )
}

module.exports = {
  isSupported: lz4 != null,

  /**
   * @param {{ buffer: Buffer }} encoder
   * @returns {Promise<Buffer>}
   */
  async compress(encoder) {
    if (!lz4) unavailable()
    return await lz4.compressFrame(encoder.buffer)
  },

  /**
   * @param {Buffer} buffer
   * @returns {Promise<Buffer>}
   */
  async decompress(buffer) {
    if (!lz4) unavailable()
    return await lz4.decompressFrame(buffer)
  },
}
