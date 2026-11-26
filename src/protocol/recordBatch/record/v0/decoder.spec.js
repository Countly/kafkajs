const { isDeepStrictEqual } = require('node:util')
const Decoder = require('../../../decoder')
const recordDecoder = require('./decoder')
const TimestampTypes = require('../../../timestampTypes')
const { record } = require('../../../requests/fetch/__tests__/fixtures/recordBatchBytes')

const MESSAGE_KEYS = [
  'magicByte',
  'attributes',
  'timestamp',
  'offset',
  'key',
  'value',
  'headers',
  'isControlRecord',
  'batchContext',
]

const RECORD_KINDS = [
  ['an ordinary record', false],
  ['a control record', true],
]

/**
 * @param {boolean} isControlBatch
 * @returns {object} a fresh batch context for the v0 record fixture
 */
const fixtureBatchContext = (isControlBatch) => ({
  firstOffset: '0',
  firstTimestamp: '1509827900073',
  magicByte: 2,
  isControlBatch,
})

/**
 * The message the v0 record fixture decodes to, written out from the fixture bytes.
 * @param {object} batchContext
 * @returns {object}
 */
const fixtureMessage = (batchContext) => ({
  magicByte: 2,
  attributes: 0,
  timestamp: '1509827900073',
  offset: '0',
  key: Buffer.from('key-0'),
  value: Buffer.from('some-value-0'),
  headers: {
    'header-key-0': Buffer.from('header-value-0'),
    'header-key-1': [Buffer.from('header-value-1'), Buffer.from('header-value-2')],
  },
  isControlRecord: batchContext.isControlBatch,
  batchContext,
})

/**
 * Decodes the v0 record fixture and calls `install` after the last read from the decoder, just
 * before the message is built. Installing earlier would break `Decoder` and `Long`, whose own
 * `offset` and `value` fields are assigned.
 * @param {object} batchContext
 * @param {() => void} install
 * @returns {object}
 */
const decodeInstallingBeforeBuild = (batchContext, install) => {
  const decoder = new Decoder(Buffer.from(require('../../fixtures/v0_record.json')))
  const readHeaders = decoder.readVarIntArrayBytesBounded
  decoder.readVarIntArrayBytesBounded = (reader, budget) => {
    const headers = readHeaders.call(decoder, reader, budget)
    install()
    return headers
  }
  return recordDecoder(decoder, batchContext)
}

/**
 * @param {object} message
 * @returns {object} the descriptors an object literal gives to each own key of `message`
 */
const literalDescriptors = (message) =>
  Object.fromEntries(
    MESSAGE_KEYS.map((name) => [
      name,
      { value: message[name], writable: true, enumerable: true, configurable: true },
    ])
  )

describe('Protocol > RecordBatch > Record > v0', () => {
  test('decodes', async () => {
    const decoder = new Decoder(Buffer.from(require('../../fixtures/v0_record.json')))

    expect(
      recordDecoder(decoder, {
        firstOffset: '0',
        firstTimestamp: '1509827900073',
        magicByte: 2,
      })
    ).toEqual({
      offset: '0',
      magicByte: 2,
      attributes: 0,
      batchContext: {
        firstOffset: '0',
        firstTimestamp: '1509827900073',
        magicByte: 2,
      },
      timestamp: '1509827900073',
      headers: {
        'header-key-0': Buffer.from('header-value-0'),
        'header-key-1': [Buffer.from('header-value-1'), Buffer.from('header-value-2')],
      },
      key: Buffer.from('key-0'),
      value: Buffer.from('some-value-0'),
      isControlRecord: false, // Default to false
    })
  })

  test('uses record batch maxTimestamp when topic is configured with timestamp type LOG_APPEND_TIME', async () => {
    const decoder = new Decoder(Buffer.from(require('../../fixtures/v0_record.json')))
    const batchContext = {
      firstOffset: '0',
      firstTimestamp: '1509827900073',
      magicByte: 2,
      maxTimestamp: '1597425188809',
      timestampType: TimestampTypes.LOG_APPEND_TIME,
    }

    const decoded = recordDecoder(decoder, batchContext)

    expect(decoded.batchContext).toEqual(batchContext)
    expect(decoded.timestamp).toEqual(batchContext.maxTimestamp)
  })

  test('decodes control record', async () => {
    const decoder = new Decoder(Buffer.from(require('../../fixtures/v0_record.json')))

    expect(
      recordDecoder(decoder, {
        firstOffset: '0',
        firstTimestamp: '1509827900073',
        magicByte: 2,
        isControlBatch: true,
      })
    ).toEqual({
      offset: '0',
      magicByte: 2,
      attributes: 0,
      batchContext: {
        firstOffset: '0',
        firstTimestamp: '1509827900073',
        isControlBatch: true,
        magicByte: 2,
      },
      timestamp: '1509827900073',
      headers: {
        'header-key-0': Buffer.from('header-value-0'),
        'header-key-1': [Buffer.from('header-value-1'), Buffer.from('header-value-2')],
      },
      key: Buffer.from('key-0'),
      value: Buffer.from('some-value-0'),
      isControlRecord: true,
    })
  })

  test('a header named like a property of every object is kept as data, and a repeated one makes an array', () => {
    const bytes = record({
      key: 'k',
      value: 'v',
      headers: [
        ['__proto__', 'a'],
        ['constructor', 'b'],
        ['toString', 'c'],
        ['constructor', 'd'],
      ],
    })
    const body = new Decoder(bytes).readVarIntBytes()

    const { headers } = recordDecoder(new Decoder(body), {
      firstOffset: '0',
      firstTimestamp: '0',
      magicByte: 2,
    })

    expect(Object.getOwnPropertyNames(headers)).toEqual(['__proto__', 'constructor', 'toString'])
    expect(Object.getOwnPropertyDescriptor(headers, '__proto__').value).toEqual(Buffer.from('a'))
    expect(Object.getOwnPropertyDescriptor(headers, 'constructor').value).toEqual([
      Buffer.from('b'),
      Buffer.from('d'),
    ])
    expect(Object.getOwnPropertyDescriptor(headers, 'toString').value).toEqual(Buffer.from('c'))
    expect(Object.getPrototypeOf(headers)).toBe(Object.prototype)
  })

  test.each(RECORD_KINDS)(
    '%s decodes to a plain object with the own properties, prototype and equality of an object literal',
    (_, isControlBatch) => {
      const batchContext = fixtureBatchContext(isControlBatch)
      const expected = fixtureMessage(batchContext)

      const decoded = recordDecoder(
        new Decoder(Buffer.from(require('../../fixtures/v0_record.json'))),
        batchContext
      )

      expect(Reflect.ownKeys(decoded)).toEqual(MESSAGE_KEYS)
      expect(decoded.isControlRecord).toBe(isControlBatch)
      expect(decoded.batchContext).toBe(batchContext)
      expect(Object.getOwnPropertyDescriptors(decoded)).toStrictEqual(literalDescriptors(expected))
      expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype)
      expect(decoded.constructor).toBe(Object)
      expect(decoded instanceof Object).toBe(true)
      expect(Object.prototype.toString.call(decoded)).toBe('[object Object]')
      expect(Object.isExtensible(decoded)).toBe(true)
      expect(decoded).toStrictEqual(expected)
      expect({ ...decoded }).toStrictEqual(expected)
      expect(isDeepStrictEqual(decoded, expected)).toBe(true)
      expect(JSON.stringify(decoded)).toBe(JSON.stringify(expected))
      expect(isDeepStrictEqual(structuredClone(decoded), structuredClone(expected))).toBe(true)
    }
  )

  describe('with a message key on Object.prototype', () => {
    const INHERITED = Symbol('inherited')

    /**
     * Decodes the fixture once per message key with `descriptor` installed on `Object.prototype`
     * under that key, and records what the decoded message owns before the prototype is restored.
     * @param {object} batchContext
     * @param {(calls: Array) => PropertyDescriptor} descriptorFor
     * @returns {Array<object>} per key: `name`, own `keys`, `descriptors`, `prototype`, `message`
     *   and the setter `calls`
     */
    const decodeWithEachKeyInherited = (batchContext, descriptorFor) =>
      MESSAGE_KEYS.map((name) => {
        expect(name in Object.prototype).toBe(false)
        const calls = []
        let observed
        try {
          const message = decodeInstallingBeforeBuild(batchContext, () =>
            // eslint-disable-next-line no-extend-native -- removed in finally
            Object.defineProperty(Object.prototype, name, descriptorFor(calls))
          )
          observed = {
            name,
            keys: Reflect.ownKeys(message),
            descriptors: Object.getOwnPropertyDescriptors(message),
            prototype: Object.getPrototypeOf(message),
            message,
          }
        } finally {
          delete Object.prototype[name]
        }
        observed.calls = calls
        return observed
      })

    /**
     * @param {object} batchContext
     * @param {Array} observations from `decodeWithEachKeyInherited`
     */
    const expectLiteralMessages = (batchContext, observations) => {
      const expected = fixtureMessage(batchContext)
      expect(observations.map(({ name }) => name)).toEqual(MESSAGE_KEYS)
      for (const { keys, descriptors, prototype, message, calls } of observations) {
        expect(calls).toEqual([])
        expect(keys).toEqual(MESSAGE_KEYS)
        expect(descriptors).toStrictEqual(literalDescriptors(expected))
        expect(prototype).toBe(Object.prototype)
        expect(message).toStrictEqual(expected)
      }
      expect(MESSAGE_KEYS.filter((name) => name in Object.prototype)).toEqual([])
    }

    test.each(RECORD_KINDS)(
      '%s still owns every key when each key in turn is an inherited read-only property',
      (_, isControlBatch) => {
        const batchContext = fixtureBatchContext(isControlBatch)

        const observations = decodeWithEachKeyInherited(batchContext, () => ({
          value: INHERITED,
          writable: false,
          enumerable: false,
          configurable: true,
        }))

        expectLiteralMessages(batchContext, observations)
      }
    )

    test.each(RECORD_KINDS)(
      '%s still owns every key and runs no setter when each key in turn is an inherited accessor',
      (_, isControlBatch) => {
        const batchContext = fixtureBatchContext(isControlBatch)

        const observations = decodeWithEachKeyInherited(batchContext, (calls) => ({
          get: () => INHERITED,
          set: (value) => calls.push(value),
          enumerable: false,
          configurable: true,
        }))

        expectLiteralMessages(batchContext, observations)
      }
    )
  })
})
