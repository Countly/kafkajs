const buildRecordBatch = require('./buildRecordBatch')
const recordBatchDecoder = require('../../recordBatch/v0/decoder')
const Decoder = require('../../decoder')
const Long = require('../../../utils/long')
const { Types: Compression } = require('../../message/compression')

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
    ['a numeric string in exponent form', '1e16', 10n ** 16n],
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

  test('a timestamp that is not a number stays encodable beside one above 2^53', async () => {
    const { timestamps } = await encode([
      { value: 'v', timestamp: '9007199254740993' },
      { value: 'v', timestamp: 'abc' },
    ])

    expect(timestamps[0]).toEqual(9007199254740993n)
  })

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
