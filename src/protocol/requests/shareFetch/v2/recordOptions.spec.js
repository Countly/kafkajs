jest.mock('../../fetch/v4/decodeMessages', () =>
  jest.fn(jest.requireActual('../../fetch/v4/decodeMessages'))
)
const decodeMessages = require('../../fetch/v4/decodeMessages')
const codec = require('./response')
const vector = require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json')

describe('ShareFetch v2 Records options', () => {
  const bytes = Buffer.from(vector.hex, 'hex')

  beforeEach(() => decodeMessages.mockClear())

  test('hands the caller options to the inherited decoder of every partition, with compact forced', async () => {
    const options = { checkCrcs: false, maxDecompressedBatchBytes: 5120 }
    await codec.decode(bytes, options)
    expect(decodeMessages.mock.calls.map(([, forwarded]) => forwarded)).toEqual([
      { ...options, compact: true },
      { ...options, compact: true },
    ])
  })

  test('reads Records as compact when the caller asks for the classic form', async () => {
    await codec.decode(bytes, { compact: false })
    expect(decodeMessages.mock.calls.map(([, forwarded]) => forwarded)).toEqual([
      { compact: true },
      { compact: true },
    ])
  })

  test('forwards only compact when the caller passes no options', async () => {
    await codec.decode(bytes)
    expect(decodeMessages.mock.calls.map(([, forwarded]) => forwarded)).toEqual([
      { compact: true },
      { compact: true },
    ])
  })
})
