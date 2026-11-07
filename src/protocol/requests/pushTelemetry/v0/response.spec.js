const response = require('./response')
const { compare, expectedFields } = require('../../../../../scripts/protocol/schema')
const vectors = [
  require('../fixtures/java/pushTelemetryResponse-v0-ok.json'),
  require('../fixtures/java/pushTelemetryResponse-v0-unsupported.json'),
  require('../fixtures/java/pushTelemetryResponse-v0-invalid.json'),
  require('../fixtures/java/pushTelemetryResponse-v0-throttled.json'),
  require('../fixtures/java/pushTelemetryResponse-v0-stale.json'),
  require('../fixtures/java/pushTelemetryResponse-v0-tooLarge.json'),
]

describe('PushTelemetry v0 response', () => {
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
        expectedFields('PushTelemetry', 'response', 0)
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
})
