const request = require('./request')
const { compare, expectedFields } = require('../../../../../scripts/protocol/schema')
const vectors = [
  require('../fixtures/java/getTelemetrySubscriptionsRequest-v0-zero.json'),
  require('../fixtures/java/getTelemetrySubscriptionsRequest-v0-stable.json'),
]

describe('GetTelemetrySubscriptions v0 request', () => {
  test.each(vectors)('encodes independent $case bytes', async (vector) => {
    const fields = { ...vector.fields }
    if ('metrics' in fields) fields.metrics = Buffer.from(fields.metrics, 'hex')
    const encoded = await request(fields).encode()
    expect(encoded.buffer.toString('hex')).toBe(vector.hex)
  })
  test('declares the exact released schema', () => {
    expect(
      compare(
        { file: 'request', schema: request.schema },
        expectedFields('GetTelemetrySubscriptions', 'request', 0)
      )
    ).toEqual([])
  })
  test('defaults to the all-zero first UUID', async () => {
    expect((await request().encode()).buffer.toString('hex')).toBe(vectors[0].hex)
  })
})
