const { Types, Codecs } = require('./index')
const { KafkaJSCorruptRecordError, KafkaJSNotImplemented } = require('../../../errors')
const { record } = require('../../requests/fetch/__tests__/fixtures/recordBatchBytes')
const { compressedBatch, recordsField, decodeField } = require('./fixtures/compressedBatch')

/** A body that is one record with a value of `bytes` bytes, as a codec would hand it back. */
const oneRecord = (bytes) => record({ key: 'k', value: 'v'.repeat(bytes) })

describe('Protocol > Compression > a codec registered by the caller', () => {
  const original = Codecs[Types.Snappy]
  let plugin

  const register = (decompress) => {
    plugin = { compress: jest.fn(), decompress: jest.fn(decompress) }
    Codecs[Types.Snappy] = () => plugin
  }

  /** Decodes a Snappy batch of `count` records whose compressed body is opaque to the test. */
  const decodeWith = async (count, options) =>
    decodeField(
      recordsField(
        await compressedBatch({ codec: 'snappy', body: Buffer.from('opaque'), records: count })
      ),
      options
    )

  afterEach(() => {
    Codecs[Types.Snappy] = original
  })

  test('takes the place of the built-in codec', async () => {
    const body = oneRecord(10)
    register(async () => body)

    const { messages, corruption } = await decodeWith(1)

    expect(corruption).toBeUndefined()
    expect(messages.map(({ key, value }) => [key.toString(), value.toString()])).toEqual([
      ['k', 'vvvvvvvvvv'],
    ])
    expect(plugin.decompress).toHaveBeenCalledTimes(1)
  })

  test('a lookup that throws KafkaJSNotImplemented is a missing codec, not a corrupt batch', async () => {
    const missing = new KafkaJSNotImplemented('Snappy is not available')
    Codecs[Types.Snappy] = () => {
      throw missing
    }

    await expect(decodeWith(1)).rejects.toBe(missing)
  })

  test('one that resolves with something that is not a buffer is a programming error, not a corrupt batch', async () => {
    register(async () => undefined)

    await expect(decodeWith(1)).rejects.toBeInstanceOf(TypeError)
  })

  test('receives the compressed bytes and the bound as maxOutputLength', async () => {
    const body = oneRecord(10)
    register(async () => body)

    await decodeWith(1, { maxDecompressedBatchBytes: 5120 })

    expect(plugin.decompress).toHaveBeenCalledWith(Buffer.from('opaque'), { maxOutputLength: 5120 })
  })

  test('receives the default bound, 1 GiB, when the consumer sets none', async () => {
    const body = oneRecord(10)
    register(async () => body)

    await decodeWith(1)

    expect(plugin.decompress).toHaveBeenCalledWith(expect.any(Buffer), {
      maxOutputLength: 1073741824,
    })
  })

  test('is never post-checked as over when its output is exactly the bound', async () => {
    const body = oneRecord(600)
    register(async () => body)

    const { messages, corruption } = await decodeWith(1, {
      maxDecompressedBatchBytes: body.length,
    })

    expect(corruption).toBeUndefined()
    expect(messages).toHaveLength(1)
  })

  test('is refused afterwards when its output is over the bound, whatever it was asked', async () => {
    const body = oneRecord(600)
    register(async () => body)

    const { messages, corruption } = await decodeWith(1, {
      maxDecompressedBatchBytes: body.length - 1,
    })

    expect(messages).toEqual([])
    expect(corruption).toMatchObject({ reason: 'decompressed-size', offset: '0' })
    expect(corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
    expect(corruption.error).toMatchObject({
      codec: 'snappy',
      limit: body.length - 1,
      nextOffset: '1',
    })
    expect(plugin.decompress).toHaveBeenCalledTimes(1)
  })

  test('has already allocated its whole output when one that ignores the bound is refused', async () => {
    const produced = []
    register(async () => {
      const output = Buffer.alloc(3 * 1048576, 1)
      produced.push(output.length)
      return output
    })

    const { corruption } = await decodeWith(1, { maxDecompressedBatchBytes: 1048576 })

    expect(produced).toEqual([3 * 1048576])
    expect(corruption).toMatchObject({ reason: 'decompressed-size' })
  })

  test.each([
    ['a RangeError', new RangeError('Invalid typed array length')],
    ['an Error', new Error('invalid input')],
    ['a string', 'boom'],
  ])('that throws %s does not let it reach the retrier', async (_, thrown) => {
    register(async () => {
      throw thrown
    })

    const { corruption } = await decodeWith(1)

    expect(corruption).toMatchObject({ reason: 'decompression' })
    expect(corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
    expect(corruption.error.cause).toBe(thrown)
    expect(corruption.error).toMatchObject({ codec: 'snappy', nextOffset: '1' })
  })
})
