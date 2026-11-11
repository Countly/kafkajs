const Decoder = require('./decoder')
const Encoder = require('./encoder')
const HeaderDecoder = require('./recordBatch/header/v0/decoder')
const { KafkaJSPartialMessageError } = require('../errors')

const HEADER_COUNT = 1000001

/** @returns {Buffer} a signed-varint count followed by empty-name, null-value headers */
const wideHeaders = () =>
  Buffer.concat([
    new Encoder().writeVarInt(HEADER_COUNT).buffer,
    Buffer.alloc(HEADER_COUNT * 2).fill(Buffer.from([0, 1])),
  ])

describe('Decoder byte-bounded header arrays', () => {
  it('keeps protocol arrays count-capped while allowing more than 1000000 headers', async () => {
    const int32Prefix = new Encoder().writeInt32(HEADER_COUNT).buffer
    const compactPrefix = new Encoder().writeUVarInt(HEADER_COUNT + 1).buffer
    for (const [name, prefix] of [
      ['readArray', int32Prefix],
      ['readUVarIntArray', compactPrefix],
      ['readArrayAsync', int32Prefix],
      ['readUVarIntArrayAsync', compactPrefix],
    ]) {
      const decoder = new Decoder(Buffer.concat([prefix, Buffer.alloc(HEADER_COUNT)]))

      await expect(
        Promise.resolve().then(() => decoder[name]((input) => input.readInt8()))
      ).rejects.toThrow(KafkaJSPartialMessageError)
      expect(decoder.offset).toBe(prefix.length)
    }

    const decoder = new Decoder(wideHeaders())
    const headers = decoder.readVarIntArrayBytesBounded(HeaderDecoder)

    expect(headers).toHaveLength(HEADER_COUNT)
    expect(headers.every(({ key, value }) => key === '' && value === null)).toBe(true)
    expect(decoder.offset).toBe(decoder.buffer.length)
  }, 60000)

  it('does not preallocate from the header count before the first header fails', () => {
    const decoder = new Decoder(wideHeaders())
    const sentinel = new Error('first header failed')
    const original = global.Array
    const capacities = []
    global.Array = new Proxy(original, {
      construct(target, args, newTarget) {
        if (typeof args[0] === 'number') capacities.push(args[0])
        return Reflect.construct(target, args, newTarget)
      },
    })
    let failure
    try {
      decoder.readVarIntArrayBytesBounded(() => {
        throw sentinel
      })
    } catch (error) {
      failure = error
    } finally {
      global.Array = original
    }

    expect(failure).toBe(sentinel)
    expect(Math.max(0, ...capacities)).toBeLessThanOrEqual(1000)
    expect(decoder.offset).toBe(new Encoder().writeVarInt(HEADER_COUNT).buffer.length)
  })
})
