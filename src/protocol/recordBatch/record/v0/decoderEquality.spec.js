const Decoder = require('../../../decoder')
const RecordBatchDecoder = require('../../v0/decoder')
const { RecordBatch } = require('../../v0')
const RecordEncoder = require('../../record/v0')
const { Types: CompressionTypes } = require('../../../message/compression')
const reference = require('./decoderEquality.reference.json')

const TIMESTAMP_TYPE_FLAG_MASK = 0x8
const CONTROL_FLAG_MASK = 0x20

/**
 * Byte offset of the low half of the batch `Attributes` int16, counted from the start of an encoded
 * batch: FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4) + Magic(1) + CRC(4) = 21 puts the
 * field at bytes 21-22, and the flags live in the low half of that big-endian int16, so 21 + 1 = 22.
 * @see https://kafka.apache.org/43/implementation/message-format
 */
const ATTRIBUTES_LOW_BYTE_OFFSET = 22

const FIRST_TIMESTAMP = 1700000000000
const MAX_TIMESTAMP = 1700000000500

/**
 * Builds a real record batch through the production encoder.
 * @param {object} options passed straight to the batch encoder
 * @returns {Promise<Buffer>} the encoded batch, ready to be decoded
 */
const buildBatch = async (options) => Buffer.from((await RecordBatch(options)).buffer)

/**
 * Sets a batch-attribute flag the encoder cannot set (timestamp type, control batch) on an already
 * encoded batch.
 * @param {Buffer} buffer an encoded record batch
 * @param {number} mask the attribute bit to set
 * @returns {Buffer} a copy with the flag set; its CRC is deliberately left stale, so every decode
 *   in this spec runs with `checkCrcs: false`
 */
const withAttributeFlag = (buffer, mask) => {
  const patched = Buffer.from(buffer)
  patched[ATTRIBUTES_LOW_BYTE_OFFSET] |= mask
  return patched
}

/**
 * Three records with non-zero offset and timestamp deltas and one header.
 * @param {Buffer|null} [lastKey=null] the key of the third record
 */
const threeRecords = (lastKey = null) => [
  RecordEncoder({
    key: Buffer.from('key-a'),
    value: Buffer.from('a'),
    offsetDelta: 0,
    timestampDelta: 0,
    headers: { 'header-key': Buffer.from('header-value') },
  }),
  RecordEncoder({
    key: Buffer.from('key-b'),
    value: Buffer.from('b'),
    offsetDelta: 1,
    timestampDelta: 250,
  }),
  RecordEncoder({ key: lastKey, value: Buffer.from('c'), offsetDelta: 2, timestampDelta: 500 }),
]

/**
 * The pinned corpus. Each entry builds one batch; the committed reference file holds what the
 * decoder produced for it before the offset fast path and the header loop were introduced.
 */
const CORPUS = {
  'create-time batch with headers': () =>
    buildBatch({
      firstOffset: '1000',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: MAX_TIMESTAMP,
      records: threeRecords(),
    }),

  'log-append-time batch': async () =>
    withAttributeFlag(
      await buildBatch({
        firstOffset: '1000',
        firstTimestamp: FIRST_TIMESTAMP,
        maxTimestamp: MAX_TIMESTAMP,
        records: threeRecords(),
      }),
      TIMESTAMP_TYPE_FLAG_MASK
    ),

  'control batch': async () =>
    withAttributeFlag(
      await buildBatch({
        firstOffset: '1000',
        firstTimestamp: FIRST_TIMESTAMP,
        maxTimestamp: MAX_TIMESTAMP,
        // A control record always has a key of at least 4 bytes, a version and a type: the
        // decoder refuses one that has none, as the Kafka reader does.
        records: threeRecords(Buffer.from('key-c')),
      }),
      CONTROL_FLAG_MASK
    ),

  'gzip-compressed batch': () =>
    buildBatch({
      firstOffset: '1000',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: MAX_TIMESTAMP,
      compression: CompressionTypes.GZIP,
      records: threeRecords(),
    }),

  'duplicate header keys': () =>
    buildBatch({
      firstOffset: '0',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: FIRST_TIMESTAMP,
      records: [
        RecordEncoder({
          value: Buffer.from('a'),
          headers: { k: ['1', '2', '3'], single: 'only' },
        }),
      ],
    }),

  'a header key of __proto__': () =>
    buildBatch({
      firstOffset: '0',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: FIRST_TIMESTAMP,
      records: [
        RecordEncoder({
          value: Buffer.from('a'),
          // Computed key: the literal `__proto__:` syntax would set the prototype of this object
          // instead of adding a header.
          headers: { ['__proto__']: Buffer.from('polluted'), safe: Buffer.from('kept') },
        }),
      ],
    }),

  'offsets beyond 2^31': () =>
    buildBatch({
      firstOffset: '4294967290',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: MAX_TIMESTAMP,
      records: threeRecords(),
    }),

  'offsets beyond 2^53': () =>
    buildBatch({
      firstOffset: '9223372036854775800',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: MAX_TIMESTAMP,
      records: threeRecords(),
    }),

  'an above-safe first offset with a negative delta': () =>
    buildBatch({
      firstOffset: '9007199254740993',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: MAX_TIMESTAMP,
      // Offset deltas are zigzag varints, so a negative one is encodable and has to decode exactly:
      // Number('9007199254740993') has already lost the trailing 1, which only the base guard catches.
      records: [RecordEncoder({ value: Buffer.from('a'), offsetDelta: -10 })],
    }),

  'a safe first offset with a sum that rounds': () =>
    buildBatch({
      firstOffset: String(Number.MAX_SAFE_INTEGER),
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: MAX_TIMESTAMP,
      records: [RecordEncoder({ value: Buffer.from('a'), offsetDelta: 6 })],
    }),

  'a safe first offset with an unsafe sum': () =>
    buildBatch({
      firstOffset: String(Number.MAX_SAFE_INTEGER),
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: MAX_TIMESTAMP,
      records: [RecordEncoder({ value: Buffer.from('a'), offsetDelta: 5 })],
    }),
}

/** Committed broker captures that carry record batches: one plain, one gzip-compressed. */
const FETCH_FIXTURES = {
  'fetch v11 response': {
    decode: require('../../../requests/fetch/v11/response').decode,
    payload: require('../../../requests/fetch/fixtures/v11_response.json'),
  },
  'fetch v4 gzip response': {
    decode: require('../../../requests/fetch/v4/response').decode,
    payload: require('../../../requests/fetch/fixtures/v4_response_gzip.json'),
  },
}

/** Strips class identity so the comparison is against the committed JSON reference. */
const plain = (value) => JSON.parse(JSON.stringify(value))

const decodeCorpusBatch = async (name) =>
  RecordBatchDecoder(new Decoder(await CORPUS[name]()), { checkCrcs: false })

describe('Protocol > RecordBatch > record decoder', () => {
  test('a multi-digit first offset with non-zero deltas produces arithmetic, not concatenation', async () => {
    const buffer = await buildBatch({
      firstOffset: '1000',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: FIRST_TIMESTAMP,
      records: [
        RecordEncoder({ value: Buffer.from('a'), offsetDelta: 0 }),
        RecordEncoder({ value: Buffer.from('b'), offsetDelta: 1 }),
        RecordEncoder({ value: Buffer.from('c'), offsetDelta: 2 }),
      ],
    })

    const { records } = await RecordBatchDecoder(new Decoder(buffer), { checkCrcs: false })
    expect(records.map(({ offset }) => offset)).toEqual(['1000', '1001', '1002'])
  })

  test('renders offsets without Number#toString, whose cache costs the consumer hot path', async () => {
    const buffer = await buildBatch({
      firstOffset: '1000',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: FIRST_TIMESTAMP,
      records: [
        RecordEncoder({ value: Buffer.from('a'), offsetDelta: 0 }),
        RecordEncoder({ value: Buffer.from('b'), offsetDelta: 1 }),
      ],
    })

    const original = Number.prototype.toString
    const calls = []
    // eslint-disable-next-line no-extend-native
    Number.prototype.toString = function (...args) {
      calls.push(original.apply(this, args))
      return original.apply(this, args)
    }

    let records
    try {
      ;({ records } = await RecordBatchDecoder(new Decoder(buffer), { checkCrcs: false }))
    } finally {
      // eslint-disable-next-line no-extend-native
      Number.prototype.toString = original
    }

    expect(records.map(({ offset }) => offset)).toEqual(['1000', '1001'])
    expect(calls).toEqual([])
  })

  test('an above-safe-integer first offset falls back to the exact 64-bit path', async () => {
    const buffer = await buildBatch({
      firstOffset: '9223372036854775800',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: FIRST_TIMESTAMP,
      records: [RecordEncoder({ value: Buffer.from('a'), offsetDelta: 0 })],
    })

    const { records } = await RecordBatchDecoder(new Decoder(buffer), { checkCrcs: false })
    expect(records[0].offset).toEqual('9223372036854775800')
  })

  test('a safe first offset whose sum is unsafe falls back to the exact 64-bit path', async () => {
    const buffer = await buildBatch({
      firstOffset: String(Number.MAX_SAFE_INTEGER),
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: FIRST_TIMESTAMP,
      records: [RecordEncoder({ value: Buffer.from('a'), offsetDelta: 5 })],
    })

    const { records } = await RecordBatchDecoder(new Decoder(buffer), { checkCrcs: false })
    expect(records[0].offset).toEqual('9007199254740996')
  })

  test('an above-safe first offset with a negative delta is still exact', async () => {
    // Number('9007199254740993') is already 9007199254740992, so only the guard on the base keeps
    // this record's offset exact.
    const { records } = await decodeCorpusBatch('an above-safe first offset with a negative delta')

    expect(records[0].offset).toEqual('9007199254740983')
  })

  test('a sum that a double cannot represent is still exact', async () => {
    // Number.MAX_SAFE_INTEGER + 6 rounds to ...996 as a double; the exact offset is ...997.
    const { records } = await decodeCorpusBatch('a safe first offset with a sum that rounds')

    expect(records[0].offset).toEqual('9007199254740997')
  })

  test('duplicate header keys keep their exact semantics', async () => {
    const buffer = await buildBatch({
      firstOffset: '0',
      firstTimestamp: FIRST_TIMESTAMP,
      maxTimestamp: FIRST_TIMESTAMP,
      records: [
        RecordEncoder({
          value: Buffer.from('a'),
          headers: { k: ['1', '2', '3'], single: 'only' },
        }),
      ],
    })

    const { records } = await RecordBatchDecoder(new Decoder(buffer), { checkCrcs: false })
    expect(records[0].headers.k).toEqual([Buffer.from('1'), Buffer.from('2'), Buffer.from('3')])
    expect(records[0].headers.single).toEqual(Buffer.from('only'))
  })

  test('a header key of __proto__ stays an own property and leaves the prototype alone', async () => {
    const { records } = await decodeCorpusBatch('a header key of __proto__')
    const { headers } = records[0]

    expect(Object.getPrototypeOf(headers)).toBe(Object.prototype)
    expect(Object.keys(headers)).toEqual(['__proto__', 'safe'])
    expect(headers.safe).toEqual(Buffer.from('kept'))
  })

  test.each(Object.keys(CORPUS))(
    '%s decodes to the same values it did before the change',
    async (name) => {
      // The reference output was dumped from the decoder as it stood before this change, so the
      // assertion is equality against the previous behaviour rather than a hand-written expectation.
      expect(plain(await decodeCorpusBatch(name))).toEqual(reference.batches[name])
    }
  )

  test.each(Object.keys(FETCH_FIXTURES))(
    '%s decodes to the same values it did before the change',
    async (name) => {
      const { decode, payload } = FETCH_FIXTURES[name]
      const data = await decode(Buffer.from(payload), { checkCrcs: false })

      expect(plain(data.responses)).toEqual(reference.fetchFixtures[name])
    }
  )

  test.each(Object.keys(CORPUS))('%s keeps every decoded field type', async (name) => {
    const { records } = await decodeCorpusBatch(name)

    expect(records.length).toBeGreaterThan(0)
    for (const record of records) {
      expect(typeof record.offset).toEqual('string')
      expect(typeof record.timestamp).toEqual('string')
      expect(record.value).toBeInstanceOf(Buffer)
      expect(record.key === null || Buffer.isBuffer(record.key)).toEqual(true)
      expect(Object.getPrototypeOf(record.headers)).toBe(Object.prototype)
    }
  })
})
