const Long = require('../../../utils/long')
const Decoder = require('../../decoder')
const recordBatchDecoder = require('./decoder')
const { writeRecordBatch } = require('./writer')
const buildRecordBatch = require('../../requests/produce/buildRecordBatch')
const { captureEntries } = require('../entries')
const { Types, Codecs } = require('../../message/compression')
const { KafkaJSNonRetriableError } = require('../../../errors')
const { oracleRecordBatch } = require('../../requests/produce/fixtures/recordBatchOracle')

const SEND_TIME = 1800000000000
const IDENTITY = {
  producerId: Long.fromValue(1000),
  producerEpoch: 3,
  firstSequence: 7,
  transactionalId: 'tx',
}

/** A codec of the caller's that frames the records with one byte and never compresses them. */
const framingCodec = () => ({
  compress: async (encoder) => Buffer.concat([Buffer.from([0x55]), encoder.buffer]),
  decompress: async (buffer) => buffer.subarray(1),
})

/**
 * @param {object[]} messages
 * @param {object} [options] batch options as `buildRecordBatch` takes them
 * @returns {Promise<Buffer>} what the writer writes for them
 */
const written = (messages, options = {}) =>
  writeRecordBatch({
    entries: captureEntries(messages, { now: SEND_TIME }),
    compression: options.compression,
    compressionLevel: options.compressionLevel,
    producerId: options.producerId,
    producerEpoch: options.producerEpoch,
    firstSequence: options.firstSequence,
    transactional: !!options.transactionalId,
  })

/**
 * @param {Promise<Buffer>} promise
 * @returns {Promise<object>} the bytes as hex, or the error's class and message
 */
const outcome = (promise) =>
  promise.then(
    (bytes) => ({ bytes: bytes.toString('hex') }),
    (error) => ({ error: error.constructor.name, message: error.message })
  )

/**
 * @param {Buffer} batch
 * @returns {Promise<object[]>} its records as the consumer decodes them
 */
const decodedRecords = async (batch) => {
  const { records } = await recordBatchDecoder(new Decoder(batch))
  return records.map(({ key, value, headers, timestamp, offset }) => ({
    key,
    value,
    headers,
    timestamp,
    offset,
  }))
}

/** Every recognised field of a record as the consumer reads it back, timestamps aside. */
const asWritten = (messages) =>
  messages.map(({ key, value, headers }) => {
    const decoded = {}
    if (headers != null) {
      for (const name of Object.keys(headers)) {
        const list = Array.isArray(headers[name])
          ? headers[name].filter(() => true)
          : [headers[name]]
        const read = Buffer.from(name).toString()
        for (const header of list) {
          const bytes = Buffer.from(header)
          const existing = decoded[read]
          decoded[read] =
            existing === undefined
              ? bytes
              : Array.isArray(existing)
                ? [...existing, bytes]
                : [existing, bytes]
        }
      }
    }
    return {
      key: key == null ? null : Buffer.from(key),
      value: value == null ? null : Buffer.from(value),
      headers: decoded,
    }
  })

const range = (count, make) => Array.from({ length: count }, (_, i) => make(i))

const CORPUS = [
  ['an empty batch', () => []],
  ['a null key and a null value', () => [{ key: null, value: null }]],
  ['an absent key and value', () => [{}]],
  ['an empty key and an empty value', () => [{ key: '', value: Buffer.alloc(0) }]],
  [
    'ASCII, multi-byte and lone-surrogate strings',
    () => [
      { key: 'ascii', value: 'héllo ✓ 😀' },
      { key: '\ud800', value: 'a\udc00b', headers: { '\udbff': '\udfff' } },
    ],
  ],
  [
    'pooled slices',
    () => {
      const pool = Buffer.from('0123456789abcdef')
      return [
        {
          key: pool.subarray(2, 5),
          value: pool.subarray(5, 15),
          headers: { h: pool.subarray(0, 1) },
        },
      ]
    },
  ],
  [
    'no header, one header and 20 headers',
    () => [
      { value: 'a', headers: {} },
      { value: 'b', headers: { h: 'v' } },
      {
        value: 'c',
        headers: Object.fromEntries(range(20, (i) => [`h${i}`, Buffer.from(`v${i}`)])),
      },
    ],
  ],
  [
    'repeated header names, empty header values and holes',
    // eslint-disable-next-line no-sparse-arrays
    () => [{ value: 'v', headers: { a: ['1', '', Buffer.alloc(0)], b: '', c: ['x', , 'y'] } }],
  ],
  ['a string headers container', () => [{ value: 'v', headers: 'ab' }]],
  ['a number headers container', () => [{ value: 'v', headers: 42 }]],
  [
    'every timestamp form',
    () => [
      { value: 'a', timestamp: 1700000000000 },
      { value: 'b', timestamp: '1700000000001' },
      { value: 'c', timestamp: new Date(1700000000002) },
      { value: 'd', timestamp: 1700000000003n },
      { value: 'e', timestamp: Long.fromNumber(1700000000004) },
      { value: 'f' },
      { value: 'g', timestamp: 0 },
      { value: 'h', timestamp: '0' },
      { value: 'i', timestamp: true },
      { value: 'j', timestamp: false },
      { value: 'k', timestamp: 0n },
    ],
  ],
  [
    'timestamps beyond 2^53',
    () => [
      { value: 'a', timestamp: '9007199254740993' },
      { value: 'b', timestamp: 2n ** 62n },
      { value: 'c', timestamp: Long.fromValue(2n ** 63n - 1n) },
      { value: 'd' },
    ],
  ],
  [
    'timestamp deltas at every varlong boundary',
    () =>
      [0, 63, 64, 8191, 8192, 2 ** 20 - 1, 2 ** 20, 2 ** 27 - 1, 2 ** 27, 2 ** 34, 2 ** 41, 2 ** 48]
        .concat([2 ** 52 - 1])
        .map((delta) => ({ value: 'v', timestamp: 1 + delta })),
  ],
  [
    'timestamp deltas past 2^53',
    () =>
      [0n, 2n ** 53n, 2n ** 56n, 2n ** 62n, 2n ** 63n - 2n].map((delta) => ({
        value: 'v',
        timestamp: 1n + delta,
      })),
  ],
  [
    'two records',
    () => [
      { key: 'k1', value: 'v1' },
      { key: 'k2', value: 'v2' },
    ],
  ],
  [
    '1 000 records',
    () => range(1000, (i) => ({ key: `k${i}`, value: `v${i}`, timestamp: 1700000000000 + i })),
  ],
  [
    'key and value lengths at every varint boundary',
    () =>
      [63, 64, 8191, 8192, 1048575, 1048576].map((n) => ({
        key: 'k'.repeat(n),
        value: Buffer.alloc(n, 1),
      })),
  ],
  [
    'header name and value lengths at every varint boundary',
    () =>
      [63, 64, 8191, 8192].map((n) => ({
        value: 'v',
        headers: { ['n'.repeat(n)]: Buffer.alloc(n, 2) },
      })),
  ],
  [
    'header counts of 63 and 64',
    () =>
      [63, 64].map((n) => ({
        value: 'v',
        headers: Object.fromEntries(range(n, (i) => [`h${i}`, 'x'])),
      })),
  ],
  [
    'record lengths at every varint boundary',
    () => [57, 58, 8184, 8185].map((n) => ({ value: 'v'.repeat(n), timestamp: 5 })),
  ],
  ['65 records: offset deltas past one varint byte', () => range(65, () => ({ value: 'v' }))],
  ['8 193 records: offset deltas past two varint bytes', () => range(8193, () => ({ value: '' }))],
]

const CODECS = [
  ['gzip', Types.GZIP, 'builtIn'],
  ['gzip at level 1', Types.GZIP, 'builtIn', 1],
  ['snappy', Types.Snappy, 'builtIn'],
  ['lz4', Types.LZ4, 'builtIn'],
  ['zstd', Types.ZSTD, 'builtIn'],
  ["a codec of the caller's", Types.Snappy, 'framing'],
]

const SHAPES = [
  ['no record', () => []],
  ['one record', () => [{ key: 'k', value: 'v', headers: { h: 'x' }, timestamp: 1700000000000 }]],
  [
    '1 000 mixed records',
    () =>
      range(1000, (i) => ({
        key: i % 3 === 0 ? null : `k${i}`,
        value: i % 5 === 0 ? Buffer.alloc(i % 200, i % 256) : 'v'.repeat(i % 300),
        headers: i % 2 === 0 ? { a: `${i}`, b: ['x', 'y'] } : undefined,
        timestamp: 1700000000000 + ((i * 7919) % 1000),
      })),
  ],
]

describe('Protocol > RecordBatch > v0 > writer', () => {
  const saved = { ...Codecs }

  beforeEach(() => {
    jest.spyOn(Date, 'now').mockReturnValue(SEND_TIME)
  })

  afterEach(() => {
    jest.restoreAllMocks()
    Object.assign(Codecs, saved)
  })

  describe.each([
    ['with the default identity', {}],
    ['transactional, with a producer id, epoch and sequence', IDENTITY],
  ])('uncompressed, %s', (_, identity) => {
    test.each(CORPUS)('%s: the same bytes as the record encoders', async (_, messages) => {
      expect(await outcome(written(messages(), identity))).toEqual(
        await outcome(oracleRecordBatch({ ...identity, messages: messages() }))
      )
    })
  })

  test.each(CORPUS)(
    '%s: the batch decoder reads back every record as written',
    async (_, messages) => {
      const batch = await written(messages(), IDENTITY)
      const oracle = await decodedRecords(
        await oracleRecordBatch({ ...IDENTITY, messages: messages() })
      )

      const records = await decodedRecords(batch)

      expect(records.map(({ key, value, headers }) => ({ key, value, headers }))).toEqual(
        asWritten(messages())
      )
      expect(records).toEqual(oracle)
    }
  )

  describe.each(CODECS)('compressed with %s', (_, compression, kind, compressionLevel) => {
    beforeEach(() => {
      if (kind === 'framing') Codecs[compression] = framingCodec
    })

    test.each(SHAPES)('%s: the same bytes as the record encoders', async (_, messages) => {
      const options = { ...IDENTITY, compression, compressionLevel }

      expect(await outcome(written(messages(), options))).toEqual(
        await outcome(oracleRecordBatch({ ...options, messages: messages() }))
      )
    })

    test.each(SHAPES)(
      '%s: the batch decoder reads back every record as written',
      async (_, messages) => {
        const options = { ...IDENTITY, compression, compressionLevel }
        const batch = await outcome(written(messages(), options))
        if (batch.error) {
          // The codec is not available on this Node.js: the oracle refuses it the same way.
          expect(await outcome(oracleRecordBatch({ ...options, messages: messages() }))).toEqual(
            batch
          )
          return
        }

        const records = await decodedRecords(Buffer.from(batch.bytes, 'hex'))

        expect(records.map(({ key, value, headers }) => ({ key, value, headers }))).toEqual(
          asWritten(messages())
        )
      }
    )
  })

  test.each(CORPUS.slice(0, 12))(
    '%s: buildRecordBatch writes the same bytes from plain records and from entries captured earlier',
    async (_, messages) => {
      const options = { ...IDENTITY, compression: Types.None }
      const plain = await buildRecordBatch({ ...options, messages: messages() })
      const entries = captureEntries(messages(), { now: SEND_TIME })
      Date.now.mockReturnValue(SEND_TIME + 1)

      const captured = await buildRecordBatch({ ...options, messages: entries })

      expect(captured.size()).toBe(captured.buffer.length)
      expect(captured.buffer.toString('hex')).toBe(plain.buffer.toString('hex'))
      expect(plain.buffer.toString('hex')).toBe(
        (await outcome(oracleRecordBatch({ ...options, messages: messages() }))).bytes
      )
    }
  )

  test('what a codec does to its input or output after answering changes nothing written', async () => {
    const later = []
    Codecs[Types.Snappy] = () => ({
      compress: async (encoder) => {
        const input = encoder.buffer
        const output = Buffer.alloc(input.length + 1)
        output.set(input, 1)
        later.push(() => {
          output.fill(0xff)
          input.fill(0)
          structuredClone(output.buffer, { transfer: [output.buffer] })
          structuredClone(input.buffer, { transfer: [input.buffer] })
        })
        return output
      },
    })
    const messages = () => SHAPES[1][1]()

    const batch = await written(messages(), { compression: Types.Snappy })
    const before = batch.toString('hex')
    later.forEach((change) => change())
    await new Promise(setImmediate)

    Codecs[Types.Snappy] = () => ({
      compress: async (encoder) => Buffer.concat([Buffer.alloc(1), encoder.buffer]),
    })
    const expected = await oracleRecordBatch({ compression: Types.Snappy, messages: messages() })
    expect(batch.toString('hex')).toBe(before)
    expect(before).toBe(expected.toString('hex'))
  })

  test.each([
    ['undefined', () => undefined],
    ['a Proxy of a Buffer', () => new Proxy(Buffer.from('x'), {})],
  ])('a codec answering %s refuses the batch', async (_, answer) => {
    Codecs[Types.Snappy] = () => ({ compress: async () => answer() })

    const writing = written([{ value: 'v' }], { compression: Types.Snappy })

    await expect(writing).rejects.toThrow(
      new KafkaJSNonRetriableError('The compression codec did not return a Buffer')
    )
    await expect(writing).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
  })

  test('given only its entries, a batch is uncompressed, outside a transaction and of no producer', async () => {
    const batch = await writeRecordBatch({
      entries: captureEntries([{ value: 'v' }], { now: SEND_TIME }),
    })

    expect(batch.toString('hex')).toBe(
      (await oracleRecordBatch({ messages: [{ value: 'v' }] })).toString('hex')
    )
    expect(batch.readBigInt64BE(43)).toBe(-1n)
  })

  test.each([
    ['a compression with no codec', 5],
    ['a null compression', null],
  ])('%s fails as the record batch encoder failed', async (_, compression) => {
    const mine = await outcome(written([{ value: 'v' }], { compression }))

    expect(mine).toEqual(
      await outcome(oracleRecordBatch({ compression, messages: [{ value: 'v' }] }))
    )
    expect(mine.error).toBe('TypeError')
  })
})
