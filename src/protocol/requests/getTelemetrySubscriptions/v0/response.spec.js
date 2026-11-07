const response = require('./response')
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
