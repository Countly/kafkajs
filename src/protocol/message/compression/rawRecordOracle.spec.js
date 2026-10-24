const {
  batch,
  record,
  varint,
} = require('../../requests/fetch/__tests__/fixtures/recordBatchBytes')
const { sha256 } = require('./fixtures/librdkafkaCapture')
const {
  deriveProjection,
  parseRecordSection,
  validateCapture,
  verifyStoredBody,
} = require('./fixtures/rawRecordOracle')

/*
 * Every byte here is written by hand from the documented record layout. These are unit tests of the
 * oracle's parser and manifest checks; none of it is, or stands in for, an authentic librdkafka
 * capture, and passing them closes nothing about one.
 */

const hex = (text) => Buffer.from(text).toString('hex')

/** A deep copy in this realm, so the oracle's strict equality sees the same prototypes. */
const clone = (value) => JSON.parse(JSON.stringify(value))

const BASE_OFFSET = '9007199254740993'
const BASE_TIMESTAMP = '1700000000000'

/** The records of the hand-built capture: duplicate and interleaved headers, null versus empty. */
const RECORDS = [
  {
    offsetDelta: 0,
    timestampDelta: 0,
    key: 'k0',
    value: 'v0',
    headers: [
      ['b', 'B1'],
      ['a', 'A1'],
      ['b', 'B2'],
    ],
  },
  {
    offsetDelta: 1,
    timestampDelta: 5,
    key: null,
    value: '',
    headers: [
      ['é', null],
      ['empty', ''],
    ],
  },
  { offsetDelta: 2, timestampDelta: 7, key: '', value: null, headers: [] },
]

/** What the parser must read from `RECORDS`, written out and not derived. */
const RAW_RECORDS = [
  {
    offsetDelta: '0',
    timestampDelta: '0',
    keyHex: hex('k0'),
    valueHex: hex('v0'),
    headers: [
      { keyHex: hex('b'), keyUtf8: 'b', valueHex: hex('B1') },
      { keyHex: hex('a'), keyUtf8: 'a', valueHex: hex('A1') },
      { keyHex: hex('b'), keyUtf8: 'b', valueHex: hex('B2') },
    ],
  },
  {
    offsetDelta: '1',
    timestampDelta: '5',
    keyHex: null,
    valueHex: '',
    headers: [
      { keyHex: hex('é'), keyUtf8: 'é', valueHex: null },
      { keyHex: hex('empty'), keyUtf8: 'empty', valueHex: '' },
    ],
  },
  { offsetDelta: '2', timestampDelta: '7', keyHex: '', valueHex: null, headers: [] },
]

/** The public projection of `RAW_RECORDS`: keys by UTF-8 bytes, 'empty' before 'é'. */
const PUBLIC_RECORDS = [
  {
    offset: BASE_OFFSET,
    timestamp: BASE_TIMESTAMP,
    keyHex: hex('k0'),
    valueHex: hex('v0'),
    headers: [
      { key: 'a', values: [hex('A1')] },
      { key: 'b', values: [hex('B1'), hex('B2')] },
    ],
  },
  {
    offset: '9007199254740994',
    timestamp: '1700000000005',
    keyHex: null,
    valueHex: '',
    headers: [
      { key: 'empty', values: [''] },
      { key: 'é', values: [null] },
    ],
  },
  {
    offset: '9007199254740995',
    timestamp: '1700000000007',
    keyHex: '',
    valueHex: null,
    headers: [],
  },
]

const sectionOf = (records = RECORDS) => Buffer.concat(records.map((fields) => record(fields)))

/**
 * A hand-built Snappy-flagged batch, its record section and a manifest that agrees with them. The
 * compressed body is opaque bytes: nothing here decompresses it.
 * @param {object} [overrides]
 * @param {number} [overrides.attributes=2]
 * @returns {{ batch: Buffer, data: Buffer, manifest: object }}
 */
const handBuilt = ({ attributes = 2 } = {}) => {
  const data = sectionOf()
  const body = Buffer.from('opaque compressed bytes of a hand-built batch')
  const bytes = batch({
    attributes,
    baseOffset: BigInt(BASE_OFFSET),
    firstTimestamp: BigInt(BASE_TIMESTAMP),
    maxTimestamp: BigInt(BASE_TIMESTAMP) + 7n,
    lastOffsetDelta: 2,
    count: 3,
    records: [body],
  })
  return {
    batch: bytes,
    data,
    manifest: {
      producer: { name: 'hand-built', version: '0', configuration: { note: 'unit test only' } },
      provenance: {
        source: 'written by hand in a parser unit test',
        acquiredAt: '2026-10-24',
        releaseOrEnvironment: 'none',
        commands: ['none'],
        referenceDecoder: 'none',
      },
      sha256: sha256(bytes),
      compressedSha256: sha256(body),
      recordDataSha256: sha256(data),
      batch: {
        magic: 2,
        attributes,
        recordCount: 3,
        framing: 'raw',
        baseOffset: BASE_OFFSET,
        baseTimestamp: BASE_TIMESTAMP,
        lastOffsetDelta: 2,
      },
      rawRecords: clone(RAW_RECORDS),
      expectedPublicRecords: clone(PUBLIC_RECORDS),
    },
  }
}

/**
 * Recomputes the manifest digests of a capture that was altered, so only the alteration is wrong.
 * @param {{ batch: Buffer, data: Buffer, manifest: object }} capture
 * @returns {{ batch: Buffer, data: Buffer, manifest: object }}
 */
const resealed = ({ batch: bytes, data, manifest }) => ({
  batch: bytes,
  data,
  manifest: {
    ...manifest,
    sha256: sha256(bytes),
    compressedSha256: sha256(bytes.subarray(61)),
    recordDataSha256: sha256(data),
  },
})

describe('Protocol > the raw record oracle (hand-built parser fixtures, not authentic evidence)', () => {
  describe('parseRecordSection', () => {
    test('reads hand-built records in wire order with every duplicate header and its position', () => {
      expect(parseRecordSection(sectionOf(), 3)).toEqual(RAW_RECORDS)
    })

    test('keeps null apart from empty for a key, a value and a header value', () => {
      const [first] = parseRecordSection(
        sectionOf([
          {
            key: null,
            value: '',
            headers: [
              ['n', null],
              ['e', ''],
            ],
          },
        ])
      )

      expect(first).toMatchObject({ keyHex: null, valueHex: '' })
      expect(first.headers.map(({ valueHex }) => valueHex)).toEqual([null, ''])
    })

    test('reads a delta past 2^53 and a negative one exactly, as decimal strings', () => {
      const [first] = parseRecordSection(
        sectionOf([{ offsetDelta: -5, timestampDelta: 2n ** 62n + 1n }])
      )

      expect(first).toMatchObject({ offsetDelta: '-5', timestampDelta: '4611686018427387905' })
    })

    test('accepts a section of no records when none is expected', () => {
      expect(parseRecordSection(Buffer.alloc(0), 0)).toEqual([])
      expect(parseRecordSection(sectionOf())).toHaveLength(3)
    })

    test.each([
      ['a section cut inside a record', () => sectionOf().subarray(0, -1), /runs past|ends inside/],
      [
        'a record longer than its fields',
        () => Buffer.concat([varint(10), record({ key: 'k' }).subarray(1), Buffer.alloc(3)]),
        /longer than its fields/,
      ],
      [
        'trailing bytes after the last record',
        () => Buffer.concat([sectionOf(), Buffer.from([0x00])]),
        /ends inside/,
      ],
      [
        'a varint of six bytes',
        () => Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x01]),
        /longer than 5 bytes/,
      ],
      [
        'a varint past 32 bits',
        () => Buffer.from([0xff, 0xff, 0xff, 0xff, 0x1f]),
        /exceeds 32 bits/,
      ],
      ['a negative record length', () => varint(-1), /negative length/],
      [
        'a non-zero record attribute',
        () => Buffer.concat([varint(8), Buffer.from([1]), Buffer.alloc(7)]),
        /non-zero attribute/,
      ],
      [
        'a header key that is not valid UTF-8',
        () => sectionOf([{ headers: [[Buffer.from([0xff, 0xfe]), 'x']] }]),
        /not valid UTF-8/,
      ],
      [
        'a negative value length below -1',
        () =>
          Buffer.concat([
            varint(6),
            Buffer.from([0]),
            varint(0),
            varint(0),
            varint(-1),
            varint(-2),
            varint(0),
          ]),
        /negative length/,
      ],
      [
        'a header count past its record',
        () =>
          Buffer.concat([
            varint(6),
            Buffer.from([0]),
            varint(0),
            varint(0),
            varint(-1),
            varint(-1),
            varint(5),
          ]),
        /runs past its record/,
      ],
      [
        'a negative header count',
        () =>
          Buffer.concat([
            varint(6),
            Buffer.from([0]),
            varint(0),
            varint(0),
            varint(-1),
            varint(-1),
            varint(-1),
          ]),
        /negative header count/,
      ],
    ])('refuses %s', (_name, build, message) => {
      expect(() => parseRecordSection(build())).toThrow(message)
    })

    test('refuses a section that holds another number of records than expected', () => {
      expect(() => parseRecordSection(sectionOf(), 2)).toThrow(/holds 3 records, not 2/)
      expect(() => parseRecordSection(sectionOf(), 4)).toThrow(/holds 3 records, not 4/)
    })
  })

  describe('deriveProjection', () => {
    test('derives absolute offsets and timestamps, groups duplicates and sorts keys by UTF-8 bytes', () => {
      expect(
        deriveProjection(RAW_RECORDS, { baseOffset: BASE_OFFSET, baseTimestamp: BASE_TIMESTAMP })
      ).toEqual(PUBLIC_RECORDS)
    })
  })

  describe('validateCapture', () => {
    test('accepts a hand-built capture whose manifest agrees with its bytes', () => {
      const capture = handBuilt()

      const validated = validateCapture(capture)

      expect(validated.records).toEqual(RAW_RECORDS)
      expect(validated.body).toEqual(capture.batch.subarray(61))
      expect(validated.data).toBe(capture.data)
    })

    test.each([
      [
        'a raw record value that differs',
        (m) => (m.rawRecords[1].valueHex = hex('x')),
        /raw records differ at 1/,
      ],
      [
        'raw headers in another order',
        (m) => m.rawRecords[0].headers.reverse(),
        /raw records differ at 0/,
      ],
      [
        'a header key text that differs from its bytes',
        (m) => (m.rawRecords[1].headers[0].keyUtf8 = 'e'),
        /raw records differ at 1/,
      ],
      ['a raw record missing', (m) => m.rawRecords.pop(), /raw records differ at 2/],
      [
        'public values of one key in another order',
        (m) => m.expectedPublicRecords[0].headers[1].values.reverse(),
        /public records differ at 0/,
      ],
      [
        'public keys sorted another way',
        (m) => m.expectedPublicRecords[1].headers.reverse(),
        /public records differ at 1/,
      ],
      [
        'a public null that became empty',
        (m) => (m.expectedPublicRecords[1].headers[1].values[0] = ''),
        /public records differ at 1/,
      ],
      [
        'a public offset that is a number',
        (m) => (m.expectedPublicRecords[2].offset = Number('9007199254740995')),
        /public records differ at 2/,
      ],
      [
        'a base offset that is a number',
        (m) => (m.batch.baseOffset = Number(BASE_OFFSET)),
        /baseOffset is not an exact decimal string/,
      ],
      ['a missing producer name', (m) => delete m.producer.name, /producer.name is missing/],
      ['no provenance commands', (m) => (m.provenance.commands = []), /provenance.commands/],
      ['a digest that is not hex', (m) => (m.sha256 = 'ABC'), /sha256 is not a lower-case/],
      [
        'a record count that is not an integer',
        (m) => (m.batch.recordCount = '3.5'),
        /recordCount/,
      ],
      ['a missing raw record list', (m) => delete m.rawRecords, /rawRecords is missing/],
    ])('refuses a manifest with %s', (_name, alter, message) => {
      const capture = handBuilt()
      alter(capture.manifest)

      expect(() => validateCapture(capture)).toThrow(message)
    })

    test.each([
      [
        'a batch whose digest the manifest does not name',
        (c) => ({
          ...c,
          batch: Buffer.concat([c.batch.subarray(0, 70), Buffer.from([1]), c.batch.subarray(71)]),
        }),
        /batch does not match the manifest SHA-256/,
      ],
      [
        'an acquired section whose digest the manifest does not name',
        (c) => ({ ...c, data: Buffer.concat([c.data, Buffer.from([0])]) }),
        /acquired record section does not match/,
      ],
    ])('refuses %s', (_name, alter, message) => {
      expect(() => validateCapture(alter(handBuilt()))).toThrow(message)
    })

    test.each([
      [
        'more than one batch',
        (c) => ({ ...c, batch: Buffer.concat([c.batch, c.batch]) }),
        /not exactly one record batch/,
      ],
      [
        'bytes after the batch',
        (c) => ({ ...c, batch: Buffer.concat([c.batch, Buffer.from([0])]) }),
        /not exactly one record batch/,
      ],
      [
        'a wrong magic',
        (c) => {
          const bytes = Buffer.from(c.batch)
          bytes.writeInt8(1, 16)
          return { ...c, batch: bytes }
        },
        /not magic 2/,
      ],
      [
        'a CRC that does not match',
        (c) => {
          const bytes = Buffer.from(c.batch)
          bytes[70] ^= 1
          return { ...c, batch: bytes }
        },
        /CRC32C does not match/,
      ],
      ['a batch that is not Snappy', () => handBuilt({ attributes: 1 }), /not Snappy compressed/],
      ['LogAppendTime', () => handBuilt({ attributes: 2 | 8 }), /LogAppendTime/],
      [
        'a transactional batch',
        () => handBuilt({ attributes: 2 | 0x10 }),
        /transactional or a control/,
      ],
      ['a control batch', () => handBuilt({ attributes: 2 | 0x20 }), /transactional or a control/],
    ])('refuses %s even when the digests were recomputed', (_name, alter, message) => {
      expect(() => validateCapture(resealed(alter(handBuilt())))).toThrow(message)
    })

    test.each([
      ['the record count', (m) => (m.batch.recordCount = 2), /record count differs/],
      ['the last offset delta', (m) => (m.batch.lastOffsetDelta = 3), /last offset delta differs/],
      ['the base offset', (m) => (m.batch.baseOffset = '9'), /base offset differs/],
      ['the base timestamp', (m) => (m.batch.baseTimestamp = '1'), /base timestamp differs/],
      ['the attributes', (m) => (m.batch.attributes = 10), /attributes differs/],
      ['the magic', (m) => (m.batch.magic = 1), /not the magic the manifest records/],
    ])('refuses a manifest whose batch disagrees with the bytes on %s', (_name, alter, message) => {
      const capture = handBuilt()
      alter(capture.manifest)

      expect(() => validateCapture(capture)).toThrow(message)
    })

    test('refuses record bytes whose offset deltas do not increase or do not end at the last one', () => {
      const unordered = handBuilt()
      unordered.data = sectionOf([
        { ...RECORDS[0], offsetDelta: 1 },
        { ...RECORDS[1], offsetDelta: 1 },
        RECORDS[2],
      ])
      const short = handBuilt()
      short.data = sectionOf([RECORDS[0], RECORDS[1], { ...RECORDS[2], offsetDelta: 3 }])

      expect(() => validateCapture(resealed(unordered))).toThrow(/not increasing from zero/)
      expect(() => validateCapture(resealed(short))).toThrow(/last offset delta/)
    })
  })

  describe('verifyStoredBody', () => {
    test('accepts the stored compressed records and a decompression equal to the reference', () => {
      const validated = validateCapture(handBuilt())

      expect(() => verifyStoredBody(validated, Buffer.from(validated.body))).not.toThrow()
      expect(() =>
        verifyStoredBody(validated, Buffer.from(validated.body), Buffer.from(validated.data))
      ).not.toThrow()
    })

    test('refuses stored bytes that differ, and a decompression that differs from the reference', () => {
      const validated = validateCapture(handBuilt())

      expect(() => verifyStoredBody(validated, Buffer.from('another body'))).toThrow(
        /stored compressed records differ/
      )
      expect(() =>
        verifyStoredBody(
          validated,
          validated.body,
          Buffer.concat([validated.data, Buffer.from([0])])
        )
      ).toThrow(/differ from the acquired reference section/)
    })

    test('refuses a reference section that no longer matches the manifest digest', () => {
      const validated = validateCapture(handBuilt())
      const forged = { ...validated, data: Buffer.from('forged') }

      expect(() => verifyStoredBody(forged, validated.body, Buffer.from('forged'))).toThrow(
        /do not match the manifest SHA-256/
      )
    })
  })
})
