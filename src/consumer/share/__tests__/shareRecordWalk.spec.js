const { ShareRecordWalk, parseOffset } = require('../shareRecordWalk')
const { KafkaJSNonRetriableError } = require('../../../errors')
const { mulberry32 } = require('../../../../testHelpers/schedule')
const range = (first, last, deliveryCount = 1) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  deliveryCount,
})
const failure = (Class, message) => expect.objectContaining({ name: Class.name, message })
const record = (offset, isControlRecord = false) => ({ offset: String(offset), isControlRecord })
const span = (first, last) => ({ firstOffset: String(first), lastOffset: String(last) })
const EMPTY_DELIVERY = () => ({ records: [], deliveryCounts: new Uint16Array(0) })

const checkProperty = (acquiredRecords, messages, corruption) => {
  const owned = new Map()
  for (const row of acquiredRecords)
    for (let n = BigInt(row.firstOffset); n <= BigInt(row.lastOffset); n++) {
      if (!owned.has(String(n))) owned.set(String(n), row.deliveryCount)
    }
  const seenRecords = new Map()
  for (const message of messages)
    if (!seenRecords.has(message.offset)) seenRecords.set(message.offset, message)
  const frontier = messages.length ? BigInt(messages[messages.length - 1].offset) : -1n
  const expected = new Map()
  for (const [offset, deliveryCount] of owned) {
    const row = seenRecords.get(offset)
    expected.set(
      offset,
      corruption && BigInt(offset) > frontier
        ? 'release'
        : row && !row.isControlRecord
          ? `deliver:${deliveryCount}`
          : 'gap'
    )
  }
  const output = ShareRecordWalk.walk({ acquiredRecords, messages, corruption })
  const actual = new Map()
  const put = (offset, type) => {
    expect(actual.has(offset)).toBe(false)
    actual.set(offset, type)
  }
  const { records, deliveryCounts } = output.deliver
  expect(deliveryCounts).toBeInstanceOf(Uint16Array)
  expect(deliveryCounts).toHaveLength(records.length)
  expect(deliveryCounts.buffer.byteLength).toBe(deliveryCounts.byteLength)
  let delivered = -1n
  records.forEach((item, i) => {
    expect(BigInt(item.offset) > delivered).toBe(true)
    delivered = BigInt(item.offset)
    put(item.offset, `deliver:${deliveryCounts[i]}`)
  })
  for (const [key, type] of [
    ['gaps', 'gap'],
    ['release', 'release'],
  ]) {
    let previous = -1n
    for (const row of output[key]) {
      expect(BigInt(row.firstOffset) > previous).toBe(true)
      previous = BigInt(row.lastOffset)
      for (let n = BigInt(row.firstOffset); n <= BigInt(row.lastOffset); n++) put(String(n), type)
    }
  }
  expect(output.reject).toEqual([])
  expect(actual).toEqual(expected)
}

describe('deterministic acquired record walk', () => {
  test('delivers only acquired data, gaps missing and control records, and preserves identity', () => {
    const messages = [
      record(10),
      record(11),
      record(13),
      record(14, true),
      record(15),
      record(20),
      record(21),
    ]
    const before = structuredClone(messages)
    const output = ShareRecordWalk.walk({
      acquiredRecords: [range(10, 14), range(20, 21, 2)],
      messages,
    })
    expect(output.deliver.records.map((e) => e.offset)).toEqual(['10', '11', '13', '20', '21'])
    expect(output.deliver.deliveryCounts).toEqual(new Uint16Array([1, 1, 1, 2, 2]))
    const delivered = [0, 1, 2, 5, 6].map((source) => messages[source])
    delivered.forEach((original, i) => expect(output.deliver.records[i]).toBe(original))
    expect(messages).toEqual(before)
    expect(output.gaps).toEqual([span(12, 12), span(14, 14)])
    expect(output.release).toEqual([])
  })
  test('pairs each count with its record at the 1 and 32767 count boundaries', () => {
    const messages = [record(1), record(2), record(3)]
    const { deliver } = ShareRecordWalk.walk({
      acquiredRecords: [range(1, 1, 1), range(2, 2, 32767), range(3, 3, 1)],
      messages,
    })
    expect(deliver.records).toHaveLength(3)
    deliver.records.forEach((item, i) => expect(item).toBe(messages[i]))
    expect(deliver.deliveryCounts).toEqual(new Uint16Array([1, 32767, 1]))
  })
  test.each([
    ['all records delivered', [1, 2, 3], [range(1, 3)], 3],
    ['fewer deliveries than decoded records', [1, 2, 3], [range(2, 2)], 1],
    ['no delivery from decoded records', [1, 2, 3], [], 0],
    ['a repeated decoded offset', [1, 1], [range(1, 1)], 1],
  ])(
    'retains an exactly sized counts buffer when %s',
    (_label, offsets, acquiredRecords, count) => {
      const { deliver } = ShareRecordWalk.walk({
        acquiredRecords,
        messages: offsets.map((offset) => record(offset)),
      })
      expect(deliver.records).toHaveLength(count)
      expect(deliver.deliveryCounts).toHaveLength(count)
      expect(deliver.deliveryCounts.byteOffset).toBe(0)
      expect(deliver.deliveryCounts.buffer.byteLength).toBe(2 * count)
    }
  )
  test('logs the original typed decode failure without deriving a failed interval', () => {
    const error = new (require('../../../errors').KafkaJSCorruptRecordError)('checksum failed', {
      reason: 'checksum',
      offset: '11',
    })
    const logger = { warn: jest.fn(), error: jest.fn() }
    const input = {
      acquiredRecords: [range(10, 12)],
      messages: [record(10)],
      corruption: { reason: 'checksum', offset: '11', error },
    }
    ShareRecordWalk.walk(input, logger)
    expect(logger.error).toHaveBeenCalledTimes(1)
    expect(logger.error.mock.calls[0][1].error).toBe(error)
  })
  test('earliest input row wins conflicting overlaps even when rows arrive unsorted', () => {
    const logger = { warn: jest.fn() }
    expect(
      ShareRecordWalk.normalize([range(5, 8, 2), range(1, 10, 1), range(6, 7, 3)], logger)
    ).toEqual([range(1, 4, 1), range(5, 8, 2), range(9, 10, 1)])
    expect(logger.warn).toHaveBeenCalledTimes(1)
    expect(ShareRecordWalk.normalize([range(1, 3), range(3, 5), range(6, 9)], logger)).toEqual([
      range(1, 9),
    ])
    expect(logger.warn).toHaveBeenCalledTimes(1)
  })
  test.each(['checksum', 'length', 'decompressed-size', 'count', 'codec'])(
    'a %s halt preserves prefix and releases the unknown remainder',
    (reason) => {
      for (const messages of [[], [record(10), record(12, true)]]) {
        const expected = {
          deliver: messages.length
            ? { records: [messages[0]], deliveryCounts: new Uint16Array([1]) }
            : EMPTY_DELIVERY(),
          gaps: messages.length ? [span(11, 12)] : [],
          release: messages.length ? [span(13, 1000009)] : [span(10, 1000009)],
          reject: [],
        }
        for (const failed of [
          { offset: '0' },
          { offset: '10', lastOffsetDelta: 9999999 },
          { offset: '9223372036854775807', length: 1 },
          { offset: '11', nextOffset: '999999999' },
        ]) {
          const error = Object.assign(
            new (require('../../../errors').KafkaJSCorruptRecordError)(reason, {
              reason,
              offset: failed.offset,
            }),
            failed
          )
          const corruption = { reason, offset: failed.offset, error }
          expect(Object.keys(corruption)).toEqual(['reason', 'offset', 'error'])
          expect(
            ShareRecordWalk.walk({ acquiredRecords: [range(10, 1000009)], messages, corruption })
          ).toEqual(expected)
        }
      }
    }
  )
  test('an incomplete trailing batch leaves remaining acquired offsets as gaps', async () => {
    const Decoder = require('../../../protocol/decoder')
    const decodeMessages = require('../../../protocol/requests/fetch/v4/decodeMessages')
    const incomplete = Buffer.alloc(13)
    incomplete.writeBigInt64BE(0n, 0)
    incomplete.writeInt32BE(100, 8)
    const compactRecords = Buffer.concat([Buffer.from([14]), incomplete])
    const decoder = new Decoder(compactRecords)
    const decoded = await decodeMessages(decoder, {
      compact: true,
      checkCrcs: true,
      maxDecompressedBatchBytes: 1073741824,
    })
    expect(decoder.offset).toBe(compactRecords.length)
    expect(decoded).toEqual({ messages: [] })
    expect(ShareRecordWalk.walk({ ...decoded, acquiredRecords: [range(0, 0, 1)] })).toEqual({
      deliver: EMPTY_DELIVERY(),
      gaps: [span(0, 0)],
      release: [],
      reject: [],
    })
  })
  test('complete empty decode is one million-offset gap, halted empty decode is one release', () => {
    const input = { acquiredRecords: [range(0, 999999)], messages: [] }
    expect(ShareRecordWalk.walk(input)).toEqual({
      deliver: EMPTY_DELIVERY(),
      gaps: [span(0, 999999)],
      release: [],
      reject: [],
    })
    expect(
      ShareRecordWalk.walk({
        ...input,
        corruption: { reason: 'checksum', offset: '0', error: new Error() },
      })
    ).toEqual({ deliver: EMPTY_DELIVERY(), gaps: [], release: [span(0, 999999)], reject: [] })
  })
  test.each(['9007199254740993', '9223372036854775807'])(
    'retains exact boundary offset %s',
    (offset) => {
      const item = record(offset)
      expect(
        ShareRecordWalk.walk({ acquiredRecords: [range(offset, offset, 32767)], messages: [item] })
      ).toEqual({
        deliver: { records: [item], deliveryCounts: new Uint16Array([32767]) },
        gaps: [],
        release: [],
        reject: [],
      })
    }
  )
  test.each([
    [-1, 'Offset must be an exact nonnegative integer'],
    [1.5, 'Offset must be an exact nonnegative integer'],
    [9007199254740992, 'Offset must be an exact nonnegative integer'],
    ['-1', 'Offset must be an exact nonnegative integer'],
    ['01', 'Offset must be an exact nonnegative integer'],
    ['9223372036854775808', 'Offset exceeds signed int64'],
    ['1e3', 'Offset must be an exact nonnegative integer'],
    [{}, 'Offset must be an exact nonnegative integer'],
    [null, 'Offset must be an exact nonnegative integer'],
  ])('rejects malformed offset %p', (value, message) =>
    expect(() => parseOffset(value)).toThrow(failure(KafkaJSNonRetriableError, message))
  )
  test('rejects malformed ranges, counts and descending messages', () => {
    for (const row of [range(2, 1), range(1, 2, 0), range(1, 2, 32768)])
      expect(() => ShareRecordWalk.normalize([row])).toThrow(
        failure(KafkaJSNonRetriableError, 'Invalid acquired range or delivery count')
      )
    expect(() =>
      ShareRecordWalk.walk({ acquiredRecords: [range(1, 2)], messages: [record(2), record(1)] })
    ).toThrow(failure(KafkaJSNonRetriableError, 'Record offsets must be ascending'))
  })
  test.each([[0], [-1], [32768], [65537], [1.5], [NaN], ['1']])(
    'refuses delivery count %p before it can reach the typed counts array',
    (deliveryCount) => {
      const input = {
        acquiredRecords: [range(1, 1, 1), { ...range(3, 3), deliveryCount }],
        messages: [record(1), record(3)],
      }
      expect(() => ShareRecordWalk.walk(input)).toThrow(
        failure(KafkaJSNonRetriableError, 'Invalid acquired range or delivery count')
      )
    }
  )
  test('refuses a coercible delivery count without converting it', () => {
    const convert = jest.fn(() => 1)
    const input = {
      acquiredRecords: [{ ...range(1, 1), deliveryCount: { valueOf: convert } }],
      messages: [record(1)],
    }
    expect(() => ShareRecordWalk.walk(input)).toThrow(
      failure(KafkaJSNonRetriableError, 'Invalid acquired range or delivery count')
    )
    expect(convert).not.toHaveBeenCalled()
  })
  test.each([
    [
      'a malformed offset after the last acquired range',
      { acquiredRecords: [range(1, 1)], messages: [record(1), record(2), { offset: 'oops' }] },
      'Offset must be an exact nonnegative integer',
    ],
    [
      'a descending offset after the last acquired range',
      { acquiredRecords: [range(1, 1)], messages: [record(1), record(5), record(4)] },
      'Record offsets must be ascending',
    ],
    [
      'a malformed offset with no acquired range',
      { acquiredRecords: [], messages: [record(1), { offset: '-1' }] },
      'Offset must be an exact nonnegative integer',
    ],
    [
      'a descending offset in a halted prefix',
      {
        acquiredRecords: [range(1, 9)],
        messages: [record(3), record(2)],
        corruption: { reason: 'count', offset: '4', error: new Error('halted') },
      },
      'Record offsets must be ascending',
    ],
  ])('refuses %s', (_label, input, message) =>
    expect(() => ShareRecordWalk.walk(input)).toThrow(failure(KafkaJSNonRetriableError, message))
  )
  test('parses each decoded offset once instead of once per pass', () => {
    const messages = Array.from({ length: 1000 }, (_, i) => record(i))
    const parse = jest.spyOn(global, 'BigInt')
    let calls
    try {
      ShareRecordWalk.walk({ acquiredRecords: [range(0, 999)], messages })
    } finally {
      calls = parse.mock.calls.length
      parse.mockRestore()
    }
    expect(calls - messages.length).toBeLessThan(10)
  })
  test('deduplicates repeated decoded offsets and handles more than the argument limit', () => {
    const repeated = [record(1), record(1)]
    const single = ShareRecordWalk.walk({ acquiredRecords: [range(1, 1)], messages: repeated })
    expect(single.deliver.records).toHaveLength(1)
    expect(single.deliver.records[0]).toBe(repeated[0])
    expect(single.deliver.deliveryCounts).toEqual(new Uint16Array([1]))
    const messages = Array.from({ length: 130000 }, (_, i) => record(i))
    const { deliver } = ShareRecordWalk.walk({ acquiredRecords: [range(0, 129999)], messages })
    expect(deliver.records).toHaveLength(130000)
    expect(deliver.deliveryCounts).toHaveLength(130000)
  })
  test.each(['0', '9007199254740993', '9223372036854775798'])(
    'exact small-set properties at base %s',
    (baseText) => {
      const base = BigInt(baseText)
      for (const intervals of [
        [
          [0, 2],
          [6, 8],
        ],
        [
          [0, 3],
          [4, 8],
        ],
        [
          [0, 6],
          [3, 8],
        ],
        [
          [5, 7],
          [0, 9],
          [4, 8],
        ],
      ]) {
        const acquiredRecords = intervals.map(([a, b], i) =>
          range(base + BigInt(a), base + BigInt(b), i + 1)
        )
        const messages = [0, 2, 3, 5, 8, 9].map((n, i) => record(base + BigInt(n), i === 2))
        checkProperty(acquiredRecords, messages)
        checkProperty(acquiredRecords, messages.slice(0, 3), {
          reason: 'checksum',
          offset: String(base + 30n),
          error: new Error(),
        })
      }
    }
  )
})

describe('fixed-seed acquired walk properties', () => {
  test.each(Array.from({ length: 200 }, (_, i) => [i]))('case %i', (index) => {
    const seed = 2026102300 + index
    const random = mulberry32(seed)
    const base = [0n, 9007199254740993n, 9223372036854775700n][seed % 3]
    const acquiredRecords = Array.from({ length: 8 }, () => {
      const first = Math.floor(random() * 40)
      return range(
        base + BigInt(first),
        base + BigInt(first + Math.floor(random() * 8)),
        1 + Math.floor(random() * 5)
      )
    })
    const messages = Array.from({ length: 50 }, (_, n) =>
      record(base + BigInt(n), n % 7 === 0)
    ).filter(() => random() > 0.5)
    checkProperty(acquiredRecords, messages)
    checkProperty(acquiredRecords, messages.slice(0, Math.floor(messages.length / 2)), {
      reason: 'count',
      offset: String(base),
      error: new Error(),
    })
  })
})
