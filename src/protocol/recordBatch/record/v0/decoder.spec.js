const { isDeepStrictEqual } = require('node:util')
const Decoder = require('../../../decoder')
const recordDecoder = require('./decoder')
const TimestampTypes = require('../../../timestampTypes')
const { record } = require('../../../requests/fetch/__tests__/fixtures/recordBatchBytes')

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

  test('a decoded message is a plain object, indistinguishable from an object literal', () => {
    const decoded = recordDecoder(
      new Decoder(Buffer.from(require('../../fixtures/v0_record.json'))),
      {
        firstOffset: '0',
        firstTimestamp: '1509827900073',
        magicByte: 2,
      }
    )
    const literal = {
      magicByte: decoded.magicByte,
      attributes: decoded.attributes,
      timestamp: decoded.timestamp,
      offset: decoded.offset,
      key: decoded.key,
      value: decoded.value,
      headers: decoded.headers,
      isControlRecord: decoded.isControlRecord,
      batchContext: decoded.batchContext,
    }

    expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype)
    expect(decoded.constructor).toBe(Object)
    expect(Object.prototype.toString.call(decoded)).toBe('[object Object]')
    expect(Reflect.ownKeys(decoded)).toEqual([
      'magicByte',
      'attributes',
      'timestamp',
      'offset',
      'key',
      'value',
      'headers',
      'isControlRecord',
      'batchContext',
    ])
    expect(Object.getOwnPropertyDescriptors(decoded)).toEqual(
      Object.getOwnPropertyDescriptors(literal)
    )
    expect(Object.isExtensible(decoded)).toBe(true)
    expect(isDeepStrictEqual(decoded, literal)).toBe(true)
    expect(decoded).toStrictEqual(literal)
    expect({ ...decoded }).toStrictEqual(literal)
    expect(JSON.stringify(decoded)).toBe(JSON.stringify(literal))
    expect(isDeepStrictEqual(structuredClone(decoded), structuredClone(literal))).toBe(true)
  })
})
