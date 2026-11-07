const request = require('./request')
const { compare, expectedFields } = require('../../../../../scripts/protocol/schema')
const vectors = [
  require('../fixtures/java/pushTelemetryRequest-v0-basic.json'),
  require('../fixtures/java/pushTelemetryRequest-v0-empty.json'),
  require('../fixtures/java/pushTelemetryRequest-v0-min.json'),
  require('../fixtures/java/pushTelemetryRequest-v0-max.json'),
  require('../fixtures/java/pushTelemetryRequest-v0-terminating.json'),
  require('../fixtures/java/pushTelemetryRequest-v0-zero.json'),
]

describe('PushTelemetry v0 request', () => {
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
        expectedFields('PushTelemetry', 'request', 0)
      )
    ).toEqual([])
  })
  test.each([null, undefined, ''])('refuses non-Buffer Metrics %p', async (metrics) => {
    await expect(request({ subscriptionId: 0, metrics }).encode()).rejects.toThrow(
      'non-null Buffer'
    )
  })
})
