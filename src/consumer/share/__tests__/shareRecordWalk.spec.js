const { ShareRecordWalk, parseOffset } = require('../shareRecordWalk')
const { KafkaJSNonRetriableError } = require('../../../errors')
const { mulberry32 } = require('../../../../testHelpers/schedule')
const range = (first, last, deliveryCount = 1) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  deliveryCount,
})
const record = (offset, isControlRecord = false) => ({ offset: String(offset), isControlRecord })
const span = (first, last) => ({ firstOffset: String(first), lastOffset: String(last) })

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
  for (const item of output.deliver) put(item.record.offset, `deliver:${item.deliveryCount}`)
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
    const output = ShareRecordWalk.walk({
      acquiredRecords: [range(10, 14), range(20, 21, 2)],
      messages,
    })
    expect(output.deliver.map((e) => [e.record.offset, e.deliveryCount])).toEqual([
      ['10', 1],
      ['11', 1],
      ['13', 1],
      ['20', 2],
      ['21', 2],
    ])
    expect(output.deliver[0].record).toBe(messages[0])
    expect(output.gaps).toEqual([span(12, 12), span(14, 14)])
    expect(output.release).toEqual([])
  })
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
          deliver: messages.length ? [{ record: messages[0], deliveryCount: 1 }] : [],
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
      deliver: [],
      gaps: [span(0, 0)],
      release: [],
      reject: [],
    })
  })
  test('complete empty decode is one million-offset gap, halted empty decode is one release', () => {
    const input = { acquiredRecords: [range(0, 999999)], messages: [] }
    expect(ShareRecordWalk.walk(input)).toEqual({
      deliver: [],
      gaps: [span(0, 999999)],
      release: [],
      reject: [],
    })
    expect(
      ShareRecordWalk.walk({
        ...input,
        corruption: { reason: 'checksum', offset: '0', error: new Error() },
      })
    ).toEqual({ deliver: [], gaps: [], release: [span(0, 999999)], reject: [] })
  })
  test.each(['9007199254740993', '9223372036854775807'])(
    'retains exact boundary offset %s',
    (offset) => {
      const item = record(offset)
      expect(
        ShareRecordWalk.walk({ acquiredRecords: [range(offset, offset, 32767)], messages: [item] })
      ).toEqual({
        deliver: [{ record: item, deliveryCount: 32767 }],
        gaps: [],
        release: [],
        reject: [],
      })
    }
  )
  test.each([-1, 1.5, 9007199254740992, '-1', '01', '9223372036854775808', '1e3', {}, null])(
    'rejects malformed offset %p',
    (value) => expect(() => parseOffset(value)).toThrow(KafkaJSNonRetriableError)
  )
  test('rejects malformed ranges, counts and descending messages', () => {
    for (const row of [range(2, 1), range(1, 2, 0), range(1, 2, 32768)])
      expect(() => ShareRecordWalk.normalize([row])).toThrow(KafkaJSNonRetriableError)
    expect(() =>
      ShareRecordWalk.walk({ acquiredRecords: [range(1, 2)], messages: [record(2), record(1)] })
    ).toThrow(KafkaJSNonRetriableError)
  })
  test('deduplicates repeated decoded offsets and handles more than the argument limit', () => {
    expect(
      ShareRecordWalk.walk({ acquiredRecords: [range(1, 1)], messages: [record(1), record(1)] })
        .deliver
    ).toHaveLength(1)
    const messages = Array.from({ length: 130000 }, (_, i) => record(i))
    expect(
      ShareRecordWalk.walk({ acquiredRecords: [range(0, 129999)], messages }).deliver
    ).toHaveLength(130000)
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
  test.each(Array.from({ length: 200 }, (_, i) => 2026102300 + i))('seed %i', (seed) => {
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
