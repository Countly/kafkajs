const Decoder = require('../../decoder')
const { KafkaJSCorruptRecordError, KafkaJSPartialMessageError } = require('../../../errors')
const {
  batch,
  record,
  varint,
} = require('../../requests/fetch/__tests__/fixtures/recordBatchBytes')
const { recordsField, decodeField } = require('../../message/compression/fixtures/compressedBatch')

const MIN_RECORD_BYTES = 7
const BIG_TIMEOUT = 60000

/** @param {string} name @param {string} content @returns {Buffer} one header as it is written on the wire */
const header = (name, content) =>
  Buffer.concat([
    varint(name.length),
    Buffer.from(name),
    varint(content.length),
    Buffer.from(content),
  ])

/**
 * One record with no key or value whose header count is written as given, whatever follows it.
 * @param {number} count the header count the record declares
 * @param {Buffer} [headerBytes] the bytes after the count
 * @returns {Buffer} the record, its length varint first
 */
const recordDeclaring = (count, headerBytes = Buffer.alloc(0)) => {
  const body = Buffer.concat([
    Buffer.from([0]),
    varint(0),
    varint(0),
    varint(-1),
    varint(-1),
    varint(count),
    headerBytes,
  ])
  return Buffer.concat([varint(body.length), body])
}

/** @returns {Promise<{ messages: object[], corruption?: object }>} the records of a batch holding `records` */
const decodeRecords = (records, options) => decodeField(recordsField(batch({ records })), options)

/**
 * Runs `run` with the global `Array` replaced by a forwarding Proxy that records every
 * construction, restoring it before anything is awaited.
 * @param {() => Promise<*>} run starts the work and returns its promise
 * @returns {Promise<{ outcome: *, capacities: number[] }>} what `run` settled to and the numeric
 *   first argument of each `new Array(...)` while it ran synchronously
 */
const observeArrayCapacities = async (run) => {
  const original = global.Array
  const capacities = []
  global.Array = new Proxy(original, {
    construct(target, args, newTarget) {
      if (typeof args[0] === 'number') capacities.push(args[0])
      return Reflect.construct(target, args, newTarget)
    },
  })
  let pending
  try {
    pending = run()
  } finally {
    global.Array = original
  }
  return { outcome: await pending, capacities }
}

describe('Protocol > RecordBatch > v0 > bounds inside a record batch', () => {
  describe('record headers', () => {
    const HEADER_COUNT = 1000001
    const NAMES = ['a', 'b', 'c']
    const NAME_LENGTH = varint(1)
    const VALUE_LENGTH = varint(4)
    const HEADER_BYTES = NAME_LENGTH.length + 1 + VALUE_LENGTH.length + 4

    /** @returns {Buffer} `HEADER_COUNT` headers named a, b, c in turn, each valued with its own index */
    const wideHeaders = () => {
      const bytes = Buffer.alloc(HEADER_COUNT * HEADER_BYTES)
      for (let i = 0; i < HEADER_COUNT; i++) {
        let at = i * HEADER_BYTES
        NAME_LENGTH.copy(bytes, at)
        at += NAME_LENGTH.length
        bytes[at++] = NAMES[i % NAMES.length].charCodeAt(0)
        VALUE_LENGTH.copy(bytes, at)
        at += VALUE_LENGTH.length
        bytes.writeUInt32BE(i, at)
      }
      return bytes
    }

    /** @returns {{ readName: jest.SpyInstance }} a spy on the reader every header key goes through */
    const watchHeaderNames = () => ({ readName: jest.spyOn(Decoder.prototype, 'readVarIntString') })

    afterEach(() => jest.restoreAllMocks())

    test('preserves duplicate order, null and empty values, and special own header keys', async () => {
      const entries = [
        ['repeat', null],
        ['__proto__', 'first'],
        ['constructor', 'constructor-value'],
        ['repeat', ''],
        ['toString', 'toString-value'],
        ['__proto__', null],
        ['repeat', 'last'],
        ['__proto__', 'last-proto'],
        ['', 'empty-name'],
      ]

      const { messages, corruption } = await decodeRecords([record({ headers: entries })])

      expect(corruption).toBeUndefined()
      expect(messages).toHaveLength(1)
      const { headers } = messages[0]
      expect(Object.getPrototypeOf(headers)).toBe(Object.prototype)
      expect(Object.keys(headers)).toEqual(['repeat', '__proto__', 'constructor', 'toString', ''])
      expect(headers.repeat).toEqual([null, Buffer.alloc(0), Buffer.from('last')])
      expect(Object.getOwnPropertyDescriptor(headers, '__proto__')).toEqual({
        value: [Buffer.from('first'), null, Buffer.from('last-proto')],
        writable: true,
        enumerable: true,
        configurable: true,
      })
      expect(headers.constructor).toEqual(Buffer.from('constructor-value'))
      expect(headers.toString).toEqual(Buffer.from('toString-value'))
      expect(headers['']).toEqual(Buffer.from('empty-name'))
    })

    test('a header whose value overruns the record is corrupt even when its count fits', async () => {
      const truncated = Buffer.concat([varint(1), Buffer.from('k'), varint(2), Buffer.from('v')])

      const { messages, corruption } = await decodeRecords([recordDeclaring(1, truncated)])

      expect(messages).toEqual([])
      expect(corruption).toMatchObject({ reason: 'length', offset: '0' })
      expect(corruption.error.cause).toBeInstanceOf(KafkaJSPartialMessageError)
    })

    test(
      `a record with ${HEADER_COUNT} headers decodes`,
      async () => {
        const { messages, corruption } = await decodeRecords([
          recordDeclaring(HEADER_COUNT, wideHeaders()),
        ])

        expect(corruption).toBeUndefined()
        expect(messages).toHaveLength(1)
        const { headers } = messages[0]
        expect(Object.keys(headers)).toEqual(NAMES)
        let firstWrong = -1
        const seen = NAMES.map(() => 0)
        for (let i = 0; i < HEADER_COUNT; i++) {
          const turn = i % NAMES.length
          const value = headers[NAMES[turn]][seen[turn]++]
          if (value.readUInt32BE(0) !== i && firstWrong === -1) firstWrong = i
        }
        expect(firstWrong).toBe(-1)
        expect(NAMES.map((name) => headers[name].length)).toEqual(seen)
        expect(seen.reduce((total, held) => total + held, 0)).toBe(HEADER_COUNT)
      },
      BIG_TIMEOUT
    )

    test("a header count above the record's remaining bytes is a corrupt record with reason length and reads no header", async () => {
      const twoHeaders = Buffer.concat([header('a', '1'), header('b', '2')])
      const { readName } = watchHeaderNames()

      const control = await decodeRecords([recordDeclaring(2, twoHeaders)])
      expect(control.messages[0].headers).toEqual({ a: Buffer.from('1'), b: Buffer.from('2') })
      expect(readName).toHaveBeenCalledTimes(2)

      for (const count of [twoHeaders.length + 1, 1000001, 2147483647]) {
        readName.mockClear()

        const { messages, corruption } = await decodeRecords([recordDeclaring(count, twoHeaders)])

        expect(messages).toEqual([])
        expect(corruption).toMatchObject({ reason: 'length', offset: '0' })
        expect(corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
        expect(corruption.error.cause).toBeInstanceOf(KafkaJSPartialMessageError)
        expect(readName).not.toHaveBeenCalled()
      }
    })

    test('a negative header count other than -1 is a corrupt record with reason length and reads no header', async () => {
      const { readName } = watchHeaderNames()

      for (const count of [-2, -1000001]) {
        const { messages, corruption } = await decodeRecords([
          recordDeclaring(count, header('a', '1')),
        ])

        expect(messages).toEqual([])
        expect(corruption).toMatchObject({ reason: 'length', offset: '0' })
        expect(corruption.error.cause).toBeInstanceOf(KafkaJSPartialMessageError)
        expect(readName).not.toHaveBeenCalled()
      }
    })

    test('a header count of -1 is an empty header set', async () => {
      const { readName } = watchHeaderNames()
      const first = recordDeclaring(-1)
      const second = record({ offsetDelta: 1, key: 'k', headers: [['x', 'y']] })

      const { messages, corruption } = await decodeRecords([first, second])

      expect(corruption).toBeUndefined()
      expect(messages.map(({ offset }) => offset)).toEqual(['0', '1'])
      expect(messages[0].headers).toEqual({})
      expect(Object.getPrototypeOf(messages[0].headers)).toBe(Object.prototype)
      expect(messages[1].key).toEqual(Buffer.from('k'))
      expect(messages[1].headers).toEqual({ x: Buffer.from('y') })
      expect(readName).toHaveBeenCalledTimes(1)
    })
  })

  describe('the record array', () => {
    test('the record array never has more slots than one per 7 bytes held', async () => {
      const HELD = 700
      const maximum = Math.floor(HELD / MIN_RECORD_BYTES)
      const malformedFirstRecord = Buffer.concat([varint(-1), Buffer.alloc(HELD - 1)])

      for (const count of [maximum, maximum + 1, 2147483647]) {
        const { outcome, capacities } = await observeArrayCapacities(() =>
          decodeField(recordsField(batch({ records: [malformedFirstRecord], count })))
        )

        expect(outcome.messages).toEqual([])
        expect(Math.max(0, ...capacities)).toBeLessThanOrEqual(maximum)
        if (count === maximum) {
          expect(outcome.corruption).toMatchObject({ reason: 'length' })
          expect(capacities).toContain(maximum)
        } else {
          expect(outcome.corruption).toMatchObject({ reason: 'count' })
        }
      }
    })
  })
})
