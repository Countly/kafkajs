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
      throw new KafkaJSNotImplemented(
        'LZ4 compression requires the optional dependency "lz4-napi". Install it with `npm install lz4-napi`, ' +
          'or register your own codec with CompressionCodecs[CompressionTypes.LZ4].'
      )
    }
    return lz4
  },
  [Types.ZSTD]: () => {
    const zstd = require('./zstd')
    if (!zstd.isSupported) {
      throw new KafkaJSNotImplemented(
        'ZSTD compression requires zstd support in node:zlib (Node >= 22.15 or >= 23.8)'
      )
    }
    return zstd
  },
}

const lookupCodec = (type) => (Codecs[type] ? Codecs[type]() : null)
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
