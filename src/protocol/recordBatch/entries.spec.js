const Long = require('../../utils/long')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
} = require('../../errors')
const { captureEntries, timestampOf, RecordEntries, probe, assertWritten } = require('./entries')

const NOW = 1800000000000

const capture = (messages) => captureEntries(messages, { now: NOW })
const decoded = (entries) =>
  Array.from({ length: entries.count }, (_, index) => entries.decodeEntry(index))

describe('Protocol > RecordBatch > entries', () => {
  test('a record is decoded as written: keys, values, nulls, empties and repeated headers', () => {
    const entries = capture([
      {
        key: 'k',
        value: Buffer.from('x'.repeat(200)),
        headers: { a: '1', b: ['2', '3', '4'], c: Buffer.from('5'), ['__proto__']: 'p' },
        timestamp: 5,
      },
      { key: null, value: '' },
      { value: Buffer.alloc(0), headers: {} },
    ])

    const [first, second, third] = decoded(entries)

    expect(first.key).toEqual(Buffer.from('k'))
    expect(first.value).toEqual(Buffer.from('x'.repeat(200)))
    expect(Object.keys(first.headers)).toEqual(['a', 'b', 'c', '__proto__'])
    expect(first.headers.a).toEqual(Buffer.from('1'))
    expect(first.headers.b).toEqual([Buffer.from('2'), Buffer.from('3'), Buffer.from('4')])
    expect(first.headers.c).toEqual(Buffer.from('5'))
    expect(Object.getOwnPropertyDescriptor(first.headers, '__proto__').value).toEqual(
      Buffer.from('p')
    )
    expect(Object.getPrototypeOf(first.headers)).toBe(Object.prototype)
    expect(first.timestamp).toBe('5')
    expect(second).toEqual({ key: null, value: Buffer.alloc(0), headers: {}, timestamp: `${NOW}` })
    expect(third).toEqual({ key: null, value: Buffer.alloc(0), headers: {}, timestamp: `${NOW}` })
  })

  test('an entry is its 16-byte header and its tail', () => {
    const entries = capture([
      { key: null, value: 'abc' },
      { key: 'k', value: null, headers: { h: 'v' } },
    ])

    expect(entries.entryBytes(0)).toBe(16 + 1 + 4 + 1)
    expect(entries.entryBytes(1)).toBe(16 + 2 + 1 + 1 + 2 + 2)
    expect(entries.store.length).toBe(entries.entryBytes(0) + entries.entryBytes(1))
    expect(entries.store.readUInt32BE(12)).toBe(0)
    expect(entries.store.readUInt32BE(entries.entryBytes(0) + 12)).toBe(1)
  })

  test('each decoded record lives in one fresh store of its own, apart from the entries', () => {
    const entries = capture([{ key: 'k', value: 'v' }])

    const one = entries.decodeEntry(0)
    const two = entries.decodeEntry(0)
    one.value[0] = 0x21

    expect(one.key.buffer).toBe(one.value.buffer)
    expect(one.key.buffer).not.toBe(two.key.buffer)
    expect(one.key.buffer).not.toBe(entries.store.buffer)
    expect(entries.decodeEntry(0).value).toEqual(Buffer.from('v'))
  })

  test('only captured entries are recognised, and no trap runs to tell', () => {
    const trap = jest.fn(() => {
      throw new Error('a trap ran')
    })
    const proxy = new Proxy({}, { has: trap, get: trap, getPrototypeOf: trap })

    expect(RecordEntries.is(capture([]))).toBe(true)
    expect(RecordEntries.is(null)).toBe(false)
    expect(RecordEntries.is('entries')).toBe(false)
    expect(RecordEntries.is({ store: Buffer.alloc(0), starts: [] })).toBe(false)
    expect(RecordEntries.is(proxy)).toBe(false)
    expect(trap).not.toHaveBeenCalled()
  })

  test('no record gives the send time as first and max; records give their lowest and highest', () => {
    const empty = capture([])
    const some = capture([
      { value: 'a', timestamp: 7 },
      { value: 'b', timestamp: 3 },
      { value: 'c', timestamp: 9 },
      { value: 'd', timestamp: 5 },
    ])

    expect(empty).toMatchObject({ first: NOW, max: NOW, count: 0 })
    expect(empty.store.length).toBe(0)
    expect(some).toMatchObject({ first: 3, max: 9, count: 4 })
    expect([0, 1, 2, 3].map((index) => some.timestamp(index))).toEqual([7, 3, 9, 5])
  })

  test('a batch above 2^53 keeps every timestamp exact, as a bigint', () => {
    const entries = capture([
      { value: 'a', timestamp: 2n ** 60n + 1n },
      { value: 'b', timestamp: 5 },
    ])

    expect(entries).toMatchObject({ first: 5, max: 2n ** 60n + 1n })
    expect([entries.timestamp(0), entries.timestamp(1)]).toEqual([2n ** 60n + 1n, 5n])
    expect(decoded(entries).map(({ timestamp }) => timestamp)).toEqual([
      (2n ** 60n + 1n).toString(),
      '5',
    ])
  })

  test.each([
    ['undefined', undefined, NOW],
    ['null', null, NOW],
    ['the number 0', 0, NOW],
    ['the number -0', -0, NOW],
    ['the bigint 0', 0n, NOW],
    ['false', false, NOW],
    ['a positive number', 1700000000000, 1700000000000],
    ['true', true, 1],
    ['the empty string', '', null],
    ['a numeric string', '1700000000000', 1700000000000],
    ['a hexadecimal string with spaces', ' 0x10 ', 16],
    ['the string 0', '0', 0],
    ['a string in exponent form', '1e16', null],
    ['a string that is not a number', 'abc', null],
    ['a negative string', '-1', null],
    ['the int64 maximum as a string', '9223372036854775807', 2n ** 63n - 1n],
    ['one past the int64 maximum as a string', '9223372036854775808', null],
    ['a Date', new Date(1700000000000), 1700000000000],
    ['the Date of the epoch', new Date(0), 0],
    ['an invalid Date', new Date(NaN), null],
    ['a Date before the epoch', new Date(-1), null],
    ['a Long', Long.fromValue(7), 7],
    ['a Long above 2^53', Long.fromValue(2n ** 60n), 2n ** 60n],
    ['a bigint above 2^53', 2n ** 60n, 2n ** 60n],
    ['the int64 maximum as a bigint', 2n ** 63n - 1n, 2n ** 63n - 1n],
    ['2^63 as a bigint', 2n ** 63n, null],
    ['a negative bigint', -1n, null],
    ['a negative number', -5, null],
    ['a fraction', 1.5, null],
    ['NaN', NaN, null],
    ['Infinity', Infinity, null],
    ['a number above 2^53', 2 ** 60, 2n ** 60n],
    ['2^63 as a number', 2 ** 63, null],
    ['a symbol', Symbol('t'), null],
    ['an object whose valueOf is a number', { valueOf: () => 12 }, 12],
    [
      'an object whose valueOf throws',
      {
        valueOf() {
          throw new Error('boom')
        },
      },
      null,
    ],
    ['a Number object of 0', Object(0), 0],
  ])('timestampOf: %s gives the expected int64', (_, timestamp, expected) => {
    expect(timestampOf(timestamp, NOW)).toEqual(expected)
  })

  test('a timestamp object is converted once', () => {
    const valueOf = jest.fn(() => 1700000000000)

    const entries = capture([
      { value: 'a', timestamp: { valueOf } },
      { value: 'b', timestamp: '9007199254740993' },
    ])

    expect(valueOf).toHaveBeenCalledTimes(1)
    expect(decoded(entries).map(({ timestamp }) => timestamp)).toEqual([
      '1700000000000',
      '9007199254740993',
    ])
  })

  test.each([
    ['a number key', { key: 5, value: 'v' }, 'key', 'a string, a Buffer or null'],
    ['an object value', { value: {} }, 'value', 'a string, a Buffer or null'],
    ['a Uint8Array value', { value: new Uint8Array(2) }, 'value', 'a string, a Buffer or null'],
    [
      'a Proxy of a Buffer as key',
      { key: new Proxy(Buffer.from('k'), {}), value: 'v' },
      'key',
      'a string, a Buffer or null',
    ],
    [
      'null headers',
      { value: 'v', headers: null },
      'headers',
      'an object whose values are strings, Buffers or arrays of them',
    ],
    [
      'a null header value',
      { value: 'v', headers: { h: null } },
      'headers',
      'an object whose values are strings, Buffers or arrays of them',
    ],
    [
      'a number header value',
      { value: 'v', headers: { h: 1 } },
      'headers',
      'an object whose values are strings, Buffers or arrays of them',
    ],
    [
      'an object in a header-value array',
      { value: 'v', headers: { h: ['a', {}] } },
      'headers',
      'an object whose values are strings, Buffers or arrays of them',
    ],
    [
      'a timestamp that is not a number',
      { value: 'v', timestamp: 'abc' },
      'timestamp',
      'an integer from 0 to 2^63 - 1: a number, a numeric string, a Date, a BigInt or a Long',
    ],
  ])('%s is refused, naming the index and the field', (_, message, field, rule) => {
    const capturing = () => capture([{ value: 'ok' }, message])

    expect(capturing).toThrow(KafkaJSInvalidConfigurationError)
    expect(capturing).toThrow(`Invalid message at index 1: its ${field} must be ${rule}`)
  })

  test.each([
    ['a hole', [{ value: 'a' }, , { value: 'c' }]], // eslint-disable-line no-sparse-arrays
    ['null', [{ value: 'a' }, null]],
    ['a string', [{ value: 'a' }, 'b']],
    ['a number', [{ value: 'a' }, 2]],
  ])('%s in place of a record is refused, naming its index', (_, messages) => {
    const capturing = () => capture(messages)

    expect(capturing).toThrow(KafkaJSNonRetriableError)
    expect(capturing).toThrow('Invalid message at index 1: it must be an object')
  })

  test('a string headers container gives one header per UTF-16 code unit; holes in a value array are skipped', () => {
    const [strings, holes, single] = decoded(
      capture([
        { value: 'v', headers: 'ab' },
        { value: 'v', headers: { h: ['a', , 'b'] } }, // eslint-disable-line no-sparse-arrays
        { value: 'v', headers: { h: ['a'] } },
      ])
    )

    expect(strings.headers).toEqual({ 0: Buffer.from('a'), 1: Buffer.from('b') })
    expect(holes.headers).toEqual({ h: [Buffer.from('a'), Buffer.from('b')] })
    expect(single.headers).toEqual({ h: Buffer.from('a') })
  })

  test.each([
    ['a number', 42],
    ['a boolean', true],
    ['a bigint', 5n],
    ['a symbol', Symbol('h')],
  ])('%s as a headers container gives no header', (_, headers) => {
    expect(decoded(capture([{ value: 'v', headers }]))[0].headers).toEqual({})
  })

  test('the probe refuses a detached view with the message it is given', () => {
    const store = new ArrayBuffer(2)
    const view = new Uint8Array(store)
    structuredClone(store, { transfer: [store] })

    expect(() => probe(view, 'unreadable')).toThrow(new KafkaJSNonRetriableError('unreadable'))
    expect(probe(new Uint8Array(2))).toBeUndefined()
  })

  test('a write that does not fill its buffer exactly is an invariant violation', () => {
    expect(() => assertWritten(Buffer.alloc(4), 3)).toThrow(KafkaJSInvariantViolation)
    expect(() => assertWritten(Buffer.alloc(4), 3)).toThrow('3 of 4 bytes were written')
    expect(assertWritten(Buffer.alloc(4), 4)).toBeUndefined()
  })
})
