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
