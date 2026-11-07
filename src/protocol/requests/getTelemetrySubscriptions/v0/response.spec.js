const response = require('./response')
const Decoder = require('../../../decoder')
const { compare, expectedFields } = require('../../../../../scripts/protocol/schema')
const vectors = [
  require('../fixtures/java/getTelemetrySubscriptionsResponse-v0-basic.json'),
  require('../fixtures/java/getTelemetrySubscriptionsResponse-v0-empty.json'),
  require('../fixtures/java/getTelemetrySubscriptionsResponse-v0-min.json'),
  require('../fixtures/java/getTelemetrySubscriptionsResponse-v0-max.json'),
  require('../fixtures/java/getTelemetrySubscriptionsResponse-v0-unsupported.json'),
  require('../fixtures/java/getTelemetrySubscriptionsResponse-v0-stale.json'),
  require('../fixtures/java/getTelemetrySubscriptionsResponse-v0-tooLarge.json'),
]

describe('GetTelemetrySubscriptions v0 response', () => {
  test.each(vectors)('decodes independent $case fields', async (vector) => {
    const data = await response.decode(Buffer.from(vector.hex, 'hex'))
    expect(data).toEqual({
      ...vector.fields,
      throttleTime: 0,
      clientSideThrottleTime: vector.fields.throttleTime,
    })
    if (vector.fields.errorCode === 0) {
      await expect(response.parse(data)).resolves.toBe(data)
    } else {
      await expect(response.parse(data)).rejects.toMatchObject({
        code: vector.fields.errorCode,
        clientSideThrottleTime: vector.fields.throttleTime,
      })
    }
  })
  test('declares the exact released schema', () => {
    expect(
      compare(
        { file: 'response', schema: response.schema },
        expectedFields('GetTelemetrySubscriptions', 'response', 0)
      )
    ).toEqual([])
  })
  test('skips one unknown top-level tag without changing fields', async () => {
    const body = Buffer.from(vectors[0].hex, 'hex')
    const tagged = Buffer.concat([body.subarray(0, -1), Buffer.from('010703aabbcc', 'hex')])
    expect(await response.decode(tagged)).toEqual(await response.decode(body))
  })
  test('refuses every strict prefix and a trailing byte', async () => {
    const body = Buffer.from(vectors[0].hex, 'hex')
    for (let size = 0; size < body.length; size++) {
      await expect(response.decode(body.subarray(0, size))).rejects.toThrow()
    }
    await expect(response.decode(Buffer.concat([body, Buffer.from([0])]))).rejects.toThrow(
      'Unexpected'
    )
  })
  test('refuses null codec arrays, prefix arrays and prefix elements', async () => {
    const body = Buffer.from(vectors[1].hex, 'hex')
    const codecs = Buffer.from(body)
    codecs[26] = 0
    const prefixes = Buffer.from(body)
    prefixes[36] = 0
    const element = Buffer.concat([body.subarray(0, 36), Buffer.from([2, 0, 0])])
    await expect(response.decode(codecs)).rejects.toThrow('AcceptedCompressionTypes')
    await expect(response.decode(prefixes)).rejects.toThrow('RequestedMetrics')
    await expect(response.decode(element)).rejects.toThrow('RequestedMetrics element')
  })
})

const CLIENT_ID = Buffer.from('0102030405060708090a0b0c0d0e0f10', 'hex')

/**
 * Encodes an unsigned varint independently of the decoder under test.
 * @param {number} value non-negative integer below 2^32
 * @returns {Buffer} the 1-5 byte encoding
 */
const uvarint = (value) => {
  const bytes = []
  for (; value >= 0x80; value = Math.floor(value / 0x80)) bytes.push((value % 0x80) | 0x80)
  return Buffer.from([...bytes, value])
}

/**
 * Encodes a compact array of compact strings.
 * @param {string[]} strings elements, written by UTF-8 byte length
 * @returns {Buffer} count then each length-prefixed element
 */
const compactStrings = (strings) =>
  Buffer.concat([
    uvarint(strings.length + 1),
    ...strings.map((text) =>
      Buffer.concat([uvarint(Buffer.byteLength(text) + 1), Buffer.from(text)])
    ),
  ])

/**
 * Encodes a successful response body byte by byte.
 * @param {{codecs?:number[], selection?:Buffer}} fields signed codec values and the encoded prefix array
 * @returns {Buffer} body after response header v1, ending in an empty tag buffer
 */
const wire = ({ codecs = [], selection = compactStrings([]) } = {}) =>
  Buffer.concat([
    Buffer.from([0, 0, 0, 0, 0, 0]),
    CLIENT_ID,
    Buffer.from([0, 0, 0, 7]),
    uvarint(codecs.length + 1),
    Buffer.from(codecs),
    Buffer.from([0, 0, 0x03, 0xe8, 0, 0x10, 0, 0, 1]),
    selection,
    Buffer.from([0]),
  ])

const ANY_CODECS = [-128, 127, ...Array.from({ length: 126 }, (_, index) => index - 63)]
const FULL_PREFIXES = Array.from({ length: 256 }, (_, index) => String(index).padEnd(64, 'x'))
const AT_LIMIT = { codecs: ANY_CODECS, selection: compactStrings(FULL_PREFIXES) }

/**
 * Decodes while journalling every array read, string read and UTF-8 conversion.
 * @param {Buffer} body response body
 * @returns {Promise<{outcome:{value?:object,error?:Error},arrays:number,strings:number,utf8:number}>}
 */
const withAllocationJournal = async (body) => {
  const arrays = jest.spyOn(Decoder.prototype, 'readUVarIntArray')
  const strings = jest.spyOn(Decoder.prototype, 'readUVarIntString')
  const conversions = jest.spyOn(Buffer.prototype, 'toString')
  try {
    const outcome = await response.decode(body).then(
      (value) => ({ value }),
      (error) => ({ error })
    )
    return {
      outcome,
      arrays: arrays.mock.calls.length,
      strings: strings.mock.calls.length,
      utf8: conversions.mock.calls.filter(([encoding]) => encoding === 'utf8').length,
    }
  } finally {
    arrays.mockRestore()
    strings.mockRestore()
    conversions.mockRestore()
  }
}

const OVER_LIMIT = [
  [
    'one codec past 128',
    { codecs: Array(129).fill(0) },
    'AcceptedCompressionTypes exceeds 128 entries',
  ],
  [
    'one prefix past 256',
    { selection: compactStrings(Array(257).fill('a')) },
    'RequestedMetrics exceeds 256 entries',
  ],
  [
    'a hostile compact prefix count',
    { selection: Buffer.from([0xff, 0xff, 0xff, 0xff, 0x0f]) },
    'RequestedMetrics exceeds 256 entries',
  ],
  [
    'one prefix of 257 bytes',
    { selection: compactStrings(['a'.repeat(257)]) },
    'RequestedMetrics element exceeds 256 bytes',
  ],
  [
    'one prefix of 258 UTF-8 bytes in 86 characters',
    { selection: compactStrings(['\u20ac'.repeat(86)]) },
    'RequestedMetrics element exceeds 256 bytes',
  ],
  [
    'a total of 16385 bytes',
    { selection: compactStrings([...Array(255).fill('a'.repeat(64)), 'a'.repeat(65)]) },
    'RequestedMetrics exceeds 16384 bytes',
  ],
]

describe('GetTelemetrySubscriptions v0 response metadata limits', () => {
  test('decodes the exact codec, prefix and total-byte limits with signed codecs intact', async () => {
    const data = await response.decode(wire(AT_LIMIT))
    expect(data.acceptedCompressionTypes).toEqual(ANY_CODECS)
    expect(data.acceptedCompressionTypes).toEqual(expect.arrayContaining([-128, 127]))
    expect(data.requestedMetrics).toEqual(FULL_PREFIXES)
    expect(data).toMatchObject({
      clientInstanceId: '01020304-0506-0708-090a-0b0c0d0e0f10',
      subscriptionId: 7,
      pushIntervalMs: 1000,
      telemetryMaxBytes: 1048576,
      deltaTemporality: true,
    })
    await expect(response.parse(data)).resolves.toBe(data)
  })

  test('bounds one prefix by UTF-8 bytes, not by characters', async () => {
    const exact = '\u00e9'.repeat(128)
    expect(exact).toHaveLength(128)
    const data = await response.decode(wire({ selection: compactStrings([exact]) }))
    expect(data.requestedMetrics).toEqual([exact])
    await expect(
      response.decode(wire({ selection: compactStrings([exact + 'a']) }))
    ).rejects.toMatchObject({
      name: 'KafkaJSPartialMessageError',
      message: 'RequestedMetrics element exceeds 256 bytes',
    })
  })

  test.each(OVER_LIMIT)('refuses %s with its own diagnostic', async (_, fields, message) => {
    await expect(response.decode(wire(fields))).rejects.toMatchObject({
      name: 'KafkaJSPartialMessageError',
      retriable: false,
      message,
    })
  })

  test.each(OVER_LIMIT)(
    'builds no array and converts no string before refusing %s',
    async (_, fields, message) => {
      const journal = await withAllocationJournal(wire(fields))
      expect(journal.outcome.error).toMatchObject({ message })
      expect([journal.arrays, journal.strings, journal.utf8]).toEqual([0, 0, 0])
    }
  )

  test('the allocation journal sees both arrays and every string at the exact limits', async () => {
    const journal = await withAllocationJournal(wire(AT_LIMIT))
    expect(journal.outcome.value.requestedMetrics).toHaveLength(256)
    expect([journal.arrays, journal.strings, journal.utf8]).toEqual([2, 256, 256])
  })

  test('refuses a null codec array before reading a following oversized selection', async () => {
    const body = wire({ selection: compactStrings(Array(257).fill('a')) })
    body[26] = 0
    const journal = await withAllocationJournal(body)
    expect(journal.outcome.error).toMatchObject({
      message: 'AcceptedCompressionTypes cannot be null',
    })
    expect([journal.arrays, journal.strings, journal.utf8]).toEqual([0, 0, 0])
  })

  test('refuses a DeltaTemporality byte other than 0 or 1 before allocation', async () => {
    const body = wire({ codecs: [0], selection: compactStrings(['a']) })
    expect(body[36]).toBe(1)
    body[36] = 2
    const journal = await withAllocationJournal(body)
    expect(journal.outcome.error).toMatchObject({
      name: 'KafkaJSPartialMessageError',
      message: 'DeltaTemporality must be 0 or 1',
    })
    expect([journal.arrays, journal.strings, journal.utf8]).toEqual([0, 0, 0])
  })

  test('refuses a body over 65536 bytes before reading any field', async () => {
    const body = Buffer.concat([wire({ codecs: [0] }), Buffer.alloc(65536)])
    const journal = await withAllocationJournal(body)
    expect(journal.outcome.error).toMatchObject({
      name: 'KafkaJSPartialMessageError',
      message: 'GetTelemetrySubscriptions response exceeds 65536 bytes',
    })
    expect([journal.arrays, journal.strings, journal.utf8]).toEqual([0, 0, 0])
  })

  test('keeps tags, truncation and trailing-byte refusal at the exact limits', async () => {
    const body = wire(AT_LIMIT)
    const tagged = Buffer.concat([body.subarray(0, -1), Buffer.from('010703aabbcc', 'hex')])
    expect(await response.decode(tagged)).toEqual(await response.decode(body))
    await expect(response.decode(Buffer.concat([body, Buffer.from([0])]))).rejects.toMatchObject({
      message: 'Unexpected telemetry response bytes',
    })
    for (const size of [10, 27, 100, 160, body.length - 30, body.length - 66, body.length - 1]) {
      await expect(response.decode(body.subarray(0, size))).rejects.toMatchObject({
        name: 'KafkaJSPartialMessageError',
        message: expect.stringContaining('Tried to read'),
      })
    }
  })
})
