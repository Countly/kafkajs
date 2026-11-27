const buildRecordBatch = require('./buildRecordBatch')
const recordBatchDecoder = require('../../recordBatch/v0/decoder')
const Decoder = require('../../decoder')
const Encoder = require('../../encoder')
const Long = require('../../../utils/long')
const { Types: Compression, Codecs } = require('../../message/compression')
const { KafkaJSInvalidConfigurationError, KafkaJSNonRetriableError } = require('../../../errors')

const FIRST_TIMESTAMP_OFFSET = 27
const MAX_TIMESTAMP_OFFSET = 35
const SEND_TIME = 1800000000000
const MONDAY = new Date('2024-01-01T00:00:00.000Z')
const WEDNESDAY = new Date('2024-01-03T00:00:00.000Z')
const FRIDAY = new Date('2024-01-05T00:00:00.000Z')
const SAFE_TIMESTAMPS_BATCH =
  '00000000000000000000006600000000020f22abfb0000000000050000018bcfe56800000001a3185c5000ffffffffffffffff' +
  '000000000000000000060e000400010261000e000202010262000e000604010263000e000006010264000e00080801026500' +
  '180080a0b787e9050a01026600'

/**
 * Encodes `messages` as one record batch and reads it back: the header's first and max timestamps
 * from their byte offsets, and every record's timestamp through the batch decoder.
 * @param {object[]} messages
 * @returns {Promise<{ first: bigint, max: bigint, timestamps: bigint[] }>}
 */
const encode = async (messages) => {
  const { buffer } = await buildRecordBatch({
    compression: Compression.None,
    messages,
    producerId: Long.fromValue(-1),
    producerEpoch: 0,
  })
  const { records } = await recordBatchDecoder(new Decoder(buffer))

  return {
    first: buffer.readBigInt64BE(FIRST_TIMESTAMP_OFFSET),
    max: buffer.readBigInt64BE(MAX_TIMESTAMP_OFFSET),
    timestamps: records.map(({ timestamp }) => BigInt(timestamp)),
  }
}

describe('Protocol > Requests > Produce > buildRecordBatch > timestamps', () => {
  beforeEach(() => {
    jest.spyOn(Date, 'now').mockReturnValue(SEND_TIME)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  test.each([
    ['in order', [MONDAY, WEDNESDAY, FRIDAY]],
    ['out of order', [FRIDAY, MONDAY, WEDNESDAY]],
  ])('Date timestamps %s: the header spans the earliest to the latest day', async (_, dates) => {
    const batch = await encode(dates.map((timestamp) => ({ value: 'v', timestamp })))

    expect(batch).toEqual({
      first: BigInt(MONDAY.getTime()),
      max: BigInt(FRIDAY.getTime()),
      timestamps: dates.map((date) => BigInt(date.getTime())),
    })
  })

  test.each([
    ['ascending', [999, 1000]],
    ['descending', [1000, 999]],
  ])('numbers of different lengths, %s: 999 is first and 1000 is max', async (_, timestamps) => {
    const batch = await encode(timestamps.map((timestamp) => ({ value: 'v', timestamp })))

    expect(batch).toEqual({ first: 999n, max: 1000n, timestamps: timestamps.map(BigInt) })
  })

  test.each([
    ['a number', 1700000000000],
    ['a numeric string', '1700000000000'],
    ['a Date', new Date(1700000000000)],
    ['a BigInt', 1700000000000n],
    ['a Long', Long.fromNumber(1700000000000)],
  ])('%s is written as that millisecond', async (_, timestamp) => {
    expect(await encode([{ value: 'v', timestamp }])).toEqual({
      first: 1700000000000n,
      max: 1700000000000n,
      timestamps: [1700000000000n],
    })
  })

  test('every accepted form can share one batch', async () => {
    const batch = await encode([
      { value: 'v', timestamp: '1700000000002' },
      { value: 'v', timestamp: new Date(1700000000001) },
      { value: 'v', timestamp: 1700000000003n },
      { value: 'v', timestamp: Long.fromNumber(1700000000000) },
      { value: 'v', timestamp: 1700000000004 },
    ])

    expect(batch).toEqual({
      first: 1700000000000n,
      max: 1700000000004n,
      timestamps: [1700000000002n, 1700000000001n, 1700000000003n, 1700000000000n, 1700000000004n],
    })
  })

  test.each([[0], [0n], [null], [undefined]])(
    'a timestamp of %p is the send time, in the header and in the record',
    async (timestamp) => {
      expect(await encode([{ value: 'v', timestamp }])).toEqual({
        first: BigInt(SEND_TIME),
        max: BigInt(SEND_TIME),
        timestamps: [BigInt(SEND_TIME)],
      })
    }
  )

  test('a record without a timestamp takes the send time, and the header covers it', async () => {
    const batch = await encode([
      { value: 'v', timestamp: 1700000000000 },
      { value: 'v', timestamp: 0 },
      { value: 'v' },
    ])

    expect(batch).toEqual({
      first: 1700000000000n,
      max: BigInt(SEND_TIME),
      timestamps: [1700000000000n, BigInt(SEND_TIME), BigInt(SEND_TIME)],
    })
  })

  test.each([
    ['the string "0"', '0'],
    ['Date(0)', new Date(0)],
    ['a Long of 0', Long.fromInt(0)],
  ])(
    '%s is the epoch, as in 2.2.4: only a falsy value means the send time',
    async (_, timestamp) => {
      expect(await encode([{ value: 'v', timestamp }])).toEqual({
        first: 0n,
        max: 0n,
        timestamps: [0n],
      })
    }
  )

  test('a batch with no records still carries the send time in its header', async () => {
    const { buffer } = await buildRecordBatch({
      compression: Compression.None,
      messages: [],
      producerId: Long.fromValue(-1),
      producerEpoch: 0,
    })

    expect(buffer.readBigInt64BE(FIRST_TIMESTAMP_OFFSET)).toEqual(BigInt(SEND_TIME))
    expect(buffer.readBigInt64BE(MAX_TIMESTAMP_OFFSET)).toEqual(BigInt(SEND_TIME))
  })
})

describe('Protocol > Requests > Produce > buildRecordBatch > timestamps beyond 2^53', () => {
  beforeEach(() => {
    jest.spyOn(Date, 'now').mockReturnValue(SEND_TIME)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  test.each([['9007199254740993'], ['9223372036854775807']])(
    'the numeric string %s is written exactly, in the header and in the record',
    async (timestamp) => {
      expect(await encode([{ value: 'v', timestamp }])).toEqual({
        first: BigInt(timestamp),
        max: BigInt(timestamp),
        timestamps: [BigInt(timestamp)],
      })
    }
  )

  test.each([
    ['a BigInt', 2n ** 53n + 1n, 2n ** 53n + 1n],
    ['a Long', Long.fromValue('9007199254740993'), 2n ** 53n + 1n],
    ['a number', 2 ** 60, 2n ** 60n],
    ['a BigInt of the int64 maximum', 2n ** 63n - 1n, 2n ** 63n - 1n],
  ])('%s is written exactly', async (_, timestamp, expected) => {
    expect(await encode([{ value: 'v', timestamp }])).toEqual({
      first: expected,
      max: expected,
      timestamps: [expected],
    })
  })

  test('a mixed batch with the largest record first keeps the header and every delta exact', async () => {
    const batch = await encode([
      { value: 'v', timestamp: '9007199254740995' },
      { value: 'v', timestamp: '9007199254740993' },
      { value: 'v', timestamp: '9007199254740994' },
    ])

    expect(batch).toEqual({
      first: 9007199254740993n,
      max: 9007199254740995n,
      timestamps: [9007199254740995n, 9007199254740993n, 9007199254740994n],
    })
  })

  test('every accepted form can share one batch above 2^53', async () => {
    const base = 2n ** 60n
    const batch = await encode([
      { value: 'v', timestamp: (base + 3n).toString() },
      { value: 'v', timestamp: base + 1n },
      { value: 'v', timestamp: Long.fromValue(base + 2n) },
      { value: 'v', timestamp: 2 ** 60 },
    ])

    expect(batch).toEqual({
      first: base,
      max: base + 3n,
      timestamps: [base + 3n, base + 1n, base + 2n, base],
    })
  })

  test('safe and unsafe timestamps share one batch, the send time among them', async () => {
    const batch = await encode([
      { value: 'v', timestamp: '9007199254740993' },
      { value: 'v', timestamp: new Date(MONDAY) },
      { value: 'v', timestamp: 1700000000000 },
      { value: 'v' },
    ])

    expect(batch).toEqual({
      first: 1700000000000n,
      max: 9007199254740993n,
      timestamps: [9007199254740993n, BigInt(MONDAY.getTime()), 1700000000000n, BigInt(SEND_TIME)],
    })
  })

  test('the widest int64 delta between two records is written exactly', async () => {
    const batch = await encode([
      { value: 'v', timestamp: '9223372036854775807' },
      { value: 'v', timestamp: 1 },
    ])

    expect(batch).toEqual({
      first: 1n,
      max: 2n ** 63n - 1n,
      timestamps: [2n ** 63n - 1n, 1n],
    })
  })

  test.each([
    ['a timestamp that is not a number', 'abc'],
    ['a numeric string in exponent form', '1e16'],
    ['a negative timestamp', -5],
    ['a timestamp above the int64 maximum', '9223372036854775808'],
  ])(
    '%s beside one above 2^53 is refused, naming its index, as the producer refuses it',
    async (_, timestamp) => {
      const encoding = encode([
        { value: 'v', timestamp: '9007199254740993' },
        { value: 'v', timestamp },
      ])

      await expect(encoding).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      await expect(encoding).rejects.toThrow('Invalid message at index 1: its timestamp must be')
    }
  )

  test('a batch of safe timestamps is encoded to the same bytes as before', async () => {
    const { buffer } = await buildRecordBatch({
      compression: Compression.None,
      messages: [
        { value: 'a', timestamp: '1700000000002' },
        { value: 'b', timestamp: new Date(1700000000001) },
        { value: 'c', timestamp: 1700000000003n },
        { value: 'd', timestamp: Long.fromNumber(1700000000000) },
        { value: 'e', timestamp: 1700000000004 },
        { value: 'f' },
      ],
      producerId: Long.fromValue(-1),
      producerEpoch: 0,
    })

    expect(buffer.toString('hex')).toEqual(SAFE_TIMESTAMPS_BATCH)
  })
})

describe('Protocol > Requests > Produce > buildRecordBatch > timestamps at the int64 bounds', () => {
  const INT64_MAX = 2n ** 63n - 1n

  beforeEach(() => {
    jest.spyOn(Date, 'now').mockReturnValue(SEND_TIME)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  test.each([
    ['a numeric string', INT64_MAX.toString(), INT64_MAX],
    ['a BigInt', INT64_MAX, INT64_MAX],
    ['a Long', Long.fromValue(INT64_MAX), INT64_MAX],
    ['the largest number below 2^63', 2 ** 63 - 1024, 2n ** 63n - 1024n],
  ])(
    '%s at the upper bound is written exactly, in the header and in the record',
    async (_, timestamp, expected) => {
      expect(await encode([{ value: 'v', timestamp }])).toEqual({
        first: expected,
        max: expected,
        timestamps: [expected],
      })
    }
  )

  test.each([
    ['the string 0', '0'],
    ['a Long of 0', Long.fromNumber(0)],
    ['the Date of the epoch', new Date(0)],
  ])('%s is the literal epoch, not the send time', async (_, timestamp) => {
    expect(await encode([{ value: 'v', timestamp }])).toEqual({
      first: 0n,
      max: 0n,
      timestamps: [0n],
    })
  })

  test('a batch from the epoch to the int64 maximum writes its widest delta exactly', async () => {
    const batch = await encode([
      { value: 'high', timestamp: INT64_MAX.toString() },
      { value: 'low', timestamp: '0' },
    ])

    expect(batch).toEqual({ first: 0n, max: INT64_MAX, timestamps: [INT64_MAX, 0n] })
  })
})

describe('Protocol > Requests > Produce > buildRecordBatch > records read once into owned bytes', () => {
  const CHANGED = 'A Buffer changed size while its send was being read'
  const saved = { ...Codecs }

  beforeEach(() => {
    jest.spyOn(Date, 'now').mockReturnValue(SEND_TIME)
  })

  afterEach(() => {
    jest.restoreAllMocks()
    Object.assign(Codecs, saved)
  })

  /**
   * @param {object[]} messages
   * @param {object} [options] over an uncompressed batch of no producer
   * @returns {Promise<Buffer>} the batch bytes
   */
  const build = async (messages, options = {}) =>
    (
      await buildRecordBatch({
        compression: Compression.None,
        producerId: Long.fromValue(-1),
        producerEpoch: 0,
        ...options,
        messages,
      })
    ).buffer

  /**
   * @param {object[]} messages
   * @returns {Promise<object[]>} the records of their batch, as the consumer reads them back
   */
  const records = async (messages) =>
    (await recordBatchDecoder(new Decoder(await build(messages)))).records

  /** A Buffer over `length` bytes of `store` from `offset`, of a fixed length. */
  const bufferOver = (store, offset, length) => {
    const view = new Uint8Array(store, offset, length)
    Object.setPrototypeOf(view, Buffer.prototype)
    return view
  }

  /** A resizable store of 8 bytes holding 1 to 8, and a fixed Buffer over its bytes 4 to 7. */
  const resizable = () => {
    const store = new ArrayBuffer(8, { maxByteLength: 16 })
    new Uint8Array(store).set([1, 2, 3, 4, 5, 6, 7, 8])
    return { store, view: bufferOver(store, 4, 4) }
  }

  /** Registers `codec` in place of Snappy. */
  const register = (codec) => {
    Codecs[Compression.Snappy] = () => codec
  }

  test('100 records with 3 headers each build no Encoder and allocate exactly two stores', async () => {
    const messages = Array.from({ length: 100 }, (_, i) => ({
      key: `k${i}`,
      value: `v${i}`,
      headers: { a: '1', b: Buffer.from('2'), c: '3' },
    }))
    const encoders = jest.spyOn(Encoder, 'nextPowerOfTwo')
    const zeroed = jest.spyOn(Buffer, 'alloc')
    const stores = jest.spyOn(Buffer, 'allocUnsafeSlow')

    await build(messages)

    expect(encoders).not.toHaveBeenCalled()
    expect(zeroed).not.toHaveBeenCalled()
    expect(stores).toHaveBeenCalledTimes(2)
  })

  test('a compressed batch adds one store, the records section the codec is handed', async () => {
    register({ compress: async (encoder) => encoder.buffer })
    const stores = jest.spyOn(Buffer, 'allocUnsafeSlow')

    await build([{ value: 'v' }, { value: 'w' }], { compression: Compression.Snappy })

    expect(stores).toHaveBeenCalledTimes(3)
  })

  test('a timestamp object beside one above 2^53 is converted once, and both are written exactly', async () => {
    const valueOf = jest.fn(() => 1700000000000)

    const read = await records([
      { value: 'a', timestamp: { valueOf } },
      { value: 'b', timestamp: '9007199254740993' },
    ])

    expect(valueOf).toHaveBeenCalledTimes(1)
    expect(read.map(({ timestamp }) => timestamp)).toEqual(['1700000000000', '9007199254740993'])
  })

  test.each([
    ['a transferred Buffer', 4],
    ['a transferred empty Buffer', 0],
  ])('%s is refused, typed', async (_, length) => {
    const store = new ArrayBuffer(length)
    const value = bufferOver(store, 0, length)
    structuredClone(store, { transfer: [store] })

    const building = build([{ value }])

    await expect(building).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    await expect(building).rejects.toThrow(CHANGED)
  })

  test('a Buffer out of bounds of its resizable store, shrunk before the call, is refused', async () => {
    const { store, view } = resizable()
    store.resize(2)

    const building = build([{ key: view, value: 'v' }])

    await expect(building).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    await expect(building).rejects.toThrow(CHANGED)
  })

  test("a later record's getter that shrinks an earlier Buffer's store out of bounds refuses the batch", async () => {
    const { store, view } = resizable()
    const later = {
      value: 'x',
      get timestamp() {
        store.resize(2)
        return undefined
      },
    }

    const building = build([{ value: view }, later])

    await expect(building).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    await expect(building).rejects.toThrow(CHANGED)
  })

  test('a store shrunk and regrown in bounds by a later getter, then overwritten at the same length, is written as visible', async () => {
    const { store, view } = resizable()
    const regrows = {
      value: 'x',
      get timestamp() {
        store.resize(2)
        store.resize(8)
        return undefined
      },
    }
    const overwrites = {
      value: 'y',
      get timestamp() {
        new Uint8Array(store).set([9, 9, 9], 5)
        return undefined
      },
    }

    const [record] = await records([{ value: view }, regrows, overwrites])

    expect(record.value).toEqual(Buffer.from([0, 9, 9, 9]))
  })

  test('a Buffer over a SharedArrayBuffer, and one over a growable store grown during the call, are written', async () => {
    const shared = new SharedArrayBuffer(4)
    new Uint8Array(shared).set([1, 2, 3, 4])
    const growable = new SharedArrayBuffer(4, { maxByteLength: 16 })
    new Uint8Array(growable).set([5, 6, 7, 8])
    const grows = {
      value: 'x',
      get timestamp() {
        growable.grow(16)
        return undefined
      },
    }

    const [first, second] = await records([
      { value: bufferOver(shared, 0, 4) },
      { value: bufferOver(growable, 0, 4) },
      grows,
    ])

    expect(first.value).toEqual(Buffer.from([1, 2, 3, 4]))
    expect(second.value).toEqual(Buffer.from([5, 6, 7, 8]))
  })

  test('a valid empty Buffer is written empty', async () => {
    const [record] = await records([{ value: Buffer.alloc(0) }])

    expect(record.value).toEqual(Buffer.alloc(0))
  })

  test('a non-enumerable field and an inherited one are read once and written; an own undefined shadows the prototype', async () => {
    const reads = { key: 0, value: 0 }
    const message = Object.create({
      get key() {
        reads.key += 1
        return 'inherited key'
      },
      timestamp: 5,
    })
    Object.defineProperty(message, 'value', {
      enumerable: false,
      get() {
        reads.value += 1
        return 'hidden value'
      },
    })
    message.timestamp = undefined

    const [record] = await records([message])

    expect(reads).toEqual({ key: 1, value: 1 })
    expect(record.key).toEqual(Buffer.from('inherited key'))
    expect(record.value).toEqual(Buffer.from('hidden value'))
    expect(record.timestamp).toBe(String(SEND_TIME))
  })

  test("a Buffer's own copy, length and buffer never run: its real bytes are written", async () => {
    const value = Buffer.from('real')
    const touched = jest.fn(() => {
      throw new Error('touched')
    })
    Object.defineProperty(value, 'copy', { value: touched })
    Object.defineProperty(value, 'length', { get: touched })
    Object.defineProperty(value, 'buffer', { get: touched })

    const [record] = await records([{ value }])

    expect(touched).not.toHaveBeenCalled()
    expect(record.value).toEqual(Buffer.from('real'))
  })

  test('a codec is handed a fresh, unpooled records section of exactly the records, with its level', async () => {
    const handed = []
    register({
      compress: async (encoder, options) => {
        handed.push({ size: encoder.size(), buf: encoder.buf, options })
        return Buffer.from(encoder.buffer)
      },
    })

    await build([{ key: 'k', value: 'v' }], {
      compression: Compression.Snappy,
      compressionLevel: 4,
    })

    const [{ size, buf, options }] = handed
    expect(options).toEqual({ level: 4 })
    expect(buf.length).toBe(size)
    expect(buf.byteOffset).toBe(0)
    expect(buf.buffer.byteLength).toBe(size)
  })

  test("a codec's output with its own length, copy and buffer is written by its real bytes; none of them runs", async () => {
    const touched = jest.fn(() => {
      throw new Error('touched')
    })
    register({
      compress: async () => {
        const output = Buffer.from('compressed')
        Object.defineProperty(output, 'length', { get: touched })
        Object.defineProperty(output, 'copy', { value: touched })
        Object.defineProperty(output, 'buffer', { get: touched })
        return output
      },
    })

    const batch = await build([{ value: 'v' }], { compression: Compression.Snappy })

    expect(touched).not.toHaveBeenCalled()
    expect(batch.length).toBe(61 + 10)
    expect(batch.subarray(61).toString()).toBe('compressed')
  })

  test('a codec answering a Uint8Array that is not a Buffer refuses the batch, typed', async () => {
    register({ compress: async () => new Uint8Array(3) })

    const building = build([{ value: 'v' }], { compression: Compression.Snappy })

    await expect(building).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    await expect(building).rejects.toThrow('The compression codec did not return a Buffer')
  })

  test('a codec answering a Buffer whose store it transferred refuses the batch, typed', async () => {
    register({
      compress: async () => {
        const output = Buffer.alloc(4, 1)
        structuredClone(output.buffer, { transfer: [output.buffer] })
        return output
      },
    })

    const building = build([{ value: 'v' }], { compression: Compression.Snappy })

    await expect(building).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    await expect(building).rejects.toThrow(
      'The compression codec returned a Buffer that cannot be read'
    )
  })
})
