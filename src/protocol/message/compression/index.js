const { KafkaJSCorruptRecordError, KafkaJSNotImplemented } = require('../../../errors')

const COMPRESSION_CODEC_MASK = 0x07

const Types = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
  LZ4: 3,
  ZSTD: 4,
}

const NAMES = {
  [Types.GZIP]: 'gzip',
  [Types.Snappy]: 'snappy',
  [Types.LZ4]: 'lz4',
  [Types.ZSTD]: 'zstd',
}

const Codecs = {
  [Types.GZIP]: () => require('./gzip'),
  [Types.Snappy]: () => require('./snappy'),
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
 * @throws {KafkaJSNotImplemented} for LZ4/ZSTD when their
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

/**
 * @param {number} attributes the batch attributes; the low 3 bits select the codec
 * @returns {string|undefined} 'gzip', 'snappy', 'lz4' or 'zstd'; `undefined` when none is selected
 */
const codecName = (attributes) => NAMES[attributes & COMPRESSION_CODEC_MASK]

/**
 * @param {*} error whatever a codec threw
 * @returns {boolean} whether the failure says the output passed the bound: the typed refusal of
 *   the in-tree codecs, or the `RangeError` Node raises when `maxOutputLength` is exceeded.
 *   Reading a hostile value is contained.
 */
const exceededBound = (error) => {
  try {
    return error.reason === 'decompressed-size' || error.code === 'ERR_BUFFER_TOO_LARGE'
  } catch {
    return false
  }
}

/**
 * Decompresses one batch body within `limit` bytes. This is the boundary between a codec and the
 * decoder: the bound reaches the codec as `decompress(buffer, { maxOutputLength })`, and every
 * failure leaves as a `KafkaJSCorruptRecordError`, never as the `RangeError` the retrier treats as
 * unrecoverable. A codec that enforces the bound itself (gzip, zstd, Snappy, LZ4) refuses the
 * output before or while it is made; a codec registered by the caller may ignore the argument, so
 * its output is checked afterwards. The error carries no offsets: the record batch decoder adds
 * them.
 * @param {{ decompress: function(Buffer, { maxOutputLength: number }): Promise<Buffer> }} codec
 *   the codec `lookupCodecByAttributes` returned for `attributes`
 * @param {number} attributes the batch attributes; the low 3 bits select the codec
 * @param {Buffer} buffer the compressed bytes
 * @param {number} limit the most decompressed bytes allowed
 * @returns {Promise<Buffer>} at most `limit` bytes
 * @throws {KafkaJSCorruptRecordError} `decompressed-size` when the output passes `limit`,
 *   `decompression` for any other failure of the codec, with the failure as `cause`
 */
const decompressBounded = async (codec, attributes, buffer, limit) => {
  const name = codecName(attributes)
  const refuse = (reason, message, cause) =>
    new KafkaJSCorruptRecordError(message, { reason, codec: name, limit, cause })
  const tooLarge = `${name} output is larger than ${limit} bytes`
  let output

  try {
    output = await codec.decompress(buffer, { maxOutputLength: limit })
  } catch (error) {
    throw exceededBound(error)
      ? refuse('decompressed-size', tooLarge, error)
      : refuse('decompression', `${name} data could not be decompressed`, error)
  }

  if (output.length > limit) {
    throw refuse('decompressed-size', tooLarge)
  }

  return output
}

module.exports = {
  Types,
  Codecs,
  lookupCodec,
  lookupCodecByAttributes,
  decompressBounded,
  codecName,
  COMPRESSION_CODEC_MASK,
}
