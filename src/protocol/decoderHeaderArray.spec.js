const Decoder = require('./decoder')
const Encoder = require('./encoder')
const HeaderDecoder = require('./recordBatch/header/v0/decoder')
const { varint } = require('./requests/fetch/__tests__/fixtures/recordBatchBytes')
const { KafkaJSCorruptRecordError, KafkaJSPartialMessageError } = require('../errors')

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
    const budget = { remaining: HEADER_COUNT, limit: HEADER_COUNT }
    const headers = decoder.readVarIntArrayBytesBounded(HeaderDecoder, budget)

    expect(headers).toHaveLength(HEADER_COUNT)
    expect(headers.every(({ key, value }) => key === '' && value === null)).toBe(true)
    expect(decoder.offset).toBe(decoder.buffer.length)
    expect(budget).toEqual({ remaining: 0, limit: HEADER_COUNT })
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
      decoder.readVarIntArrayBytesBounded(
        () => {
          throw sentinel
        },
        { remaining: HEADER_COUNT, limit: HEADER_COUNT }
      )
    } catch (error) {
      failure = error
    } finally {
      global.Array = original
    }

    expect(failure).toBe(sentinel)
    expect(Math.max(0, ...capacities)).toBeLessThanOrEqual(1000)
    expect(decoder.offset).toBe(new Encoder().writeVarInt(HEADER_COUNT).buffer.length)
  })

  it('throws before reading when the count is above the headers the budget has left', () => {
    const header = Buffer.concat([varint(1), Buffer.from('a'), varint(1), Buffer.from('1')])
    const decoder = new Decoder(Buffer.concat([varint(3), header, header, header]))
    const reader = jest.fn(HeaderDecoder)
    const budget = { remaining: 2, limit: 8 }
    let failure
    try {
      decoder.readVarIntArrayBytesBounded(reader, budget)
    } catch (error) {
      failure = error
    }

    expect(failure).toBeInstanceOf(KafkaJSCorruptRecordError)
    expect(failure).toMatchObject({ reason: 'count', limit: 8 })
    expect(failure.message).toBe(
      'Record header count 3 is above the 2 headers left of the batch header ceiling 8'
    )
    expect(reader).not.toHaveBeenCalled()
    expect(decoder.offset).toBe(1)
    expect(budget).toEqual({ remaining: 2, limit: 8 })
  })

  it('refuses a budget that is missing, not a number, NaN or negative', () => {
    const bytes = Buffer.concat([varint(1), varint(0), varint(-1)])
    for (const budget of [
      undefined,
      { remaining: '8', limit: 8 },
      { remaining: NaN, limit: 8 },
      { remaining: 8, limit: NaN },
      { remaining: -1, limit: 8 },
      { remaining: 8, limit: -1 },
    ]) {
      const decoder = new Decoder(bytes)

      expect(() => decoder.readVarIntArrayBytesBounded(HeaderDecoder, budget)).toThrow(TypeError)
      expect(decoder.offset).toBe(0)
    }
  })
})
