const zlib = require('zlib')
const { KafkaJSCorruptRecordError } = require('../../../errors')
const { Types, lookupCodec, decompressBounded, codecName } = require('./index')
const { TYPES, COMPRESSORS } = require('./fixtures/compressedBatch')

// eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection, not a call
const hasZstd = typeof zlib.zstdCompressSync === 'function'
let hasLz4 = true
try {
  require('lz4-napi')
} catch (e) {
  hasLz4 = false
}

const SIZE = 100000
const PLAIN = Buffer.alloc(SIZE, 7)
const CODECS = ['gzip', 'snappy', ...(hasZstd ? ['zstd'] : []), ...(hasLz4 ? ['lz4'] : [])]

/** The refusal a call produced, or the failed expectation if it resolved. */
const refusal = async (promise) => {
  const error = await promise.then(
    () => null,
    (e) => e
  )
  expect(error).toBeInstanceOf(KafkaJSCorruptRecordError)
  return error
}

describe('Protocol > Compression > the decompression bound', () => {
  describe('the codec name', () => {
    test.each([
      [1, 'gzip'],
      [2, 'snappy'],
      [3, 'lz4'],
      [4, 'zstd'],
      [0, undefined],
      [0x11, 'gzip'],
    ])('batch attributes %i name %s', (attributes, name) => {
      expect(codecName(attributes)).toBe(name)
    })
  })

  describe.each(CODECS)('%s', (name) => {
    const attributes = TYPES[name]
    let codec
    let compressed

    beforeAll(async () => {
      codec = lookupCodec(attributes)
      compressed = await COMPRESSORS[name](PLAIN)
    })

    test('returns an output exactly at the limit', async () => {
      await expect(decompressBounded(codec, attributes, compressed, SIZE)).resolves.toEqual(PLAIN)
    })

    test('refuses an output one byte over the limit as decompressed-size', async () => {
      const error = await refusal(decompressBounded(codec, attributes, compressed, SIZE - 1))

      expect(error).toMatchObject({ reason: 'decompressed-size', codec: name, limit: SIZE - 1 })
      expect(error).not.toHaveProperty('nextOffset')
      expect(error.offset).toBeUndefined()
      expect(error.retriable).toBe(false)
    })

    test('refuses a stream cut short as decompression, with the failure as its cause', async () => {
      const error = await refusal(
        decompressBounded(codec, attributes, compressed.subarray(0, compressed.length - 10), SIZE)
      )

      expect(error).toMatchObject({ reason: 'decompression', codec: name, limit: SIZE })
      expect(error.cause).toBeDefined()
    })

    test('refuses bytes that are not the codec as decompression', async () => {
      const junk = Buffer.from('this is not a compressed stream, whatever the codec')

      await expect(decompressBounded(codec, attributes, junk, SIZE)).rejects.toMatchObject({
        reason: 'decompression',
        codec: name,
      })
    })
  })

  describe('Snappy lengths the stream declares', () => {
    const codec = lookupCodec(Types.Snappy)
    // The XERIAL header: the 8-byte magic, a version of 1 and a compatible version of 1.
    const HEADER = Buffer.from('82534e41505059000000000100000001', 'hex')
    const block = (declared, bytes) => {
      const body = Buffer.concat([declared, bytes])
      const length = Buffer.alloc(4)
      length.writeUInt32BE(body.length)
      return Buffer.concat([length, body])
    }

    test('a raw block declaring more than the limit is refused before any output is allocated', async () => {
      const allocate = jest.spyOn(Buffer, 'allocUnsafe')
      try {
        // 2^31 as a varint: 0x80 0x80 0x80 0x80 0x08, then one literal byte
        const lying = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x08, 0x00, 0x61])

        await expect(
          decompressBounded(codec, TYPES.snappy, lying, 2147483647)
        ).rejects.toMatchObject({ reason: 'decompressed-size', codec: 'snappy' })
        expect(allocate.mock.calls.map(([size]) => size)).not.toContain(2147483648)
      } finally {
        allocate.mockRestore()
      }
    })

    test('xerial blocks whose declared lengths add up past the limit are refused as a sum', async () => {
      const allocate = jest.spyOn(Buffer, 'allocUnsafe')
      try {
        // two blocks declaring 60000 bytes each, one literal byte in each
        const declared = Buffer.from([0xe0, 0xd4, 0x03])
        const lying = Buffer.concat([
          HEADER,
          block(declared, Buffer.from([0, 0x61])),
          block(declared, Buffer.from([0, 0x61])),
        ])

        await expect(decompressBounded(codec, TYPES.snappy, lying, SIZE)).rejects.toMatchObject({
          reason: 'decompressed-size',
          limit: SIZE,
        })
        expect(allocate.mock.calls.map(([size]) => size)).not.toContain(120000)
      } finally {
        allocate.mockRestore()
      }
    })

    test('a declared length the data does not fill is decompression, not a short output', async () => {
      const short = Buffer.concat([HEADER, block(Buffer.from([10]), Buffer.from([0, 0x61]))])

      await expect(decompressBounded(codec, TYPES.snappy, short, SIZE)).rejects.toMatchObject({
        reason: 'decompression',
        codec: 'snappy',
      })
    })
  })

  describe('a failure of any kind', () => {
    const codecThrowing = (thrown) => ({
      decompress: () => {
        throw thrown
      },
    })

    test.each([
      [
        'a typed refusal naming the size',
        'decompressed-size',
        Object.assign(new Error('x'), { reason: 'decompressed-size' }),
      ],
      [
        'the RangeError of Node maxOutputLength',
        'decompressed-size',
        Object.assign(new RangeError('x'), { code: 'ERR_BUFFER_TOO_LARGE' }),
      ],
      ['a RangeError of another kind', 'decompression', new RangeError('Invalid array length')],
      ['a TypeError', 'decompression', new TypeError('x')],
      ['a string', 'decompression', 'boom'],
      ['undefined', 'decompression', undefined],
    ])('%s is refused as %s', async (_, reason, thrown) => {
      const error = await refusal(decompressBounded(codecThrowing(thrown), 1, Buffer.alloc(1), 10))

      expect(error).toMatchObject({ reason, codec: 'gzip', limit: 10 })
      expect(error.cause).toBe(thrown)
    })

    test('a value whose properties throw when read is still a typed refusal', async () => {
      const hostile = {
        get reason() {
          throw new Error('reason')
        },
        get code() {
          throw new Error('code')
        },
      }

      await expect(
        decompressBounded(codecThrowing(hostile), 1, Buffer.alloc(1), 10)
      ).rejects.toMatchObject({ reason: 'decompression', codec: 'gzip' })
    })

    test('a rejected promise is converted the same way as a throw', async () => {
      const codec = { decompress: () => Promise.reject(new RangeError('Invalid array length')) }

      await expect(decompressBounded(codec, 4, Buffer.alloc(1), 10)).rejects.toMatchObject({
        reason: 'decompression',
        codec: 'zstd',
      })
    })
  })
})
