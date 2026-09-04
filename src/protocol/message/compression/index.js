const { KafkaJSNotImplemented } = require('../../../errors')

const COMPRESSION_CODEC_MASK = 0x07

const Types = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
  LZ4: 3,
  ZSTD: 4,
}

const Codecs = {
  [Types.GZIP]: () => require('./gzip'),
  [Types.Snappy]: () => {
    throw new KafkaJSNotImplemented('Snappy compression not implemented')
  },
  [Types.LZ4]: () => {
    const lz4 = require('./lz4')
    if (!lz4.isSupported) {
      throw new KafkaJSNotImplemented(lz4.NOT_SUPPORTED_MESSAGE)
    }
    return lz4
  },
  [Types.ZSTD]: () => {
    const zstd = require('./zstd')
    if (!zstd.isSupported) {
      throw new KafkaJSNotImplemented(zstd.NOT_SUPPORTED_MESSAGE)
    }
    return zstd
  },
}

/**
 * @param {number} type one of `Types`
 * @returns {?object} the codec, or `null` for `Types.None`/an unregistered type
 * @throws {KafkaJSNotImplemented} for Snappy (never implemented), or for LZ4/ZSTD when their
 *   platform requirement isn't met (see each codec's `NOT_SUPPORTED_MESSAGE`)
 */
const lookupCodec = (type) => (Codecs[type] ? Codecs[type]() : null)

/**
 * @param {number} attributes the record batch / message-set attributes byte
 * @returns {?object} the codec encoded in the low 3 bits, or `null` for none
 * @throws {KafkaJSNotImplemented} same as `lookupCodec`
 */
const lookupCodecByAttributes = (attributes) => {
  const codec = Codecs[attributes & COMPRESSION_CODEC_MASK]
  return codec ? codec() : null
}

module.exports = {
  Types,
  Codecs,
  lookupCodec,
  lookupCodecByAttributes,
  COMPRESSION_CODEC_MASK,
}
