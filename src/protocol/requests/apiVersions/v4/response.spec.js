const { decode, parse } = require('./response')

describe('Protocol > Requests > ApiVersions > v4', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))

    expect(data).toEqual({
      errorCode: 0,
      apiVersions: [
        { apiKey: 0, minVersion: 3, maxVersion: 13 },
        { apiKey: 18, minVersion: 0, maxVersion: 4 },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 0,
      supportedFeatures: [{ name: 'metadata.version', minVersion: 1, maxVersion: 25 }],
      finalizedFeaturesEpoch: '7',
      finalizedFeatures: [{ name: 'metadata.version', maxVersionLevel: 25, minVersionLevel: 1 }],
      zkMigrationReady: false,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('skips a tagged field it does not know', async () => {
    const Encoder = require('../../../encoder')
    const body = new Encoder()
      .writeInt16(0)
      .writeUVarIntArray([
        new Encoder().writeInt16(18).writeInt16(0).writeInt16(4).writeTaggedFields(),
      ])
      .writeInt32(0)
      .writeTaggedFields([
        { tag: 1, value: new Encoder().writeInt64(-1) },
        { tag: 99, value: Buffer.from([0xde, 0xad, 0xbe, 0xef]) },
      ]).buffer

    const data = await decode(body)
    expect(data.apiVersions).toEqual([{ apiKey: 18, minVersion: 0, maxVersion: 4 }])
    expect(data.finalizedFeaturesEpoch).toEqual('-1')
    expect(data.zkMigrationReady).toBe(false)
  })

  test('uses the schema defaults when no tagged field is present', async () => {
    const Encoder = require('../../../encoder')
    const body = new Encoder()
      .writeInt16(0)
      .writeUVarIntArray([])
      .writeInt32(0)
      .writeTaggedFields().buffer

    const data = await decode(body)
    expect(data).toEqual({
      errorCode: 0,
      apiVersions: [],
      throttleTime: 0,
      clientSideThrottleTime: 0,
      supportedFeatures: [],
      finalizedFeaturesEpoch: '-1',
      finalizedFeatures: [],
      zkMigrationReady: false,
    })
  })

  test('decodes a v0-shaped UNSUPPORTED_VERSION body, api_keys included', async () => {
    // 00 23            errorCode 35
    // 00 00 00 01      one api key (classic INT32-prefixed array)
    // 00 12 00 00 00 02  apiKey 18, minVersion 0, maxVersion 2
    const body = Buffer.from([
      0x00, 0x23, 0x00, 0x00, 0x00, 0x01, 0x00, 0x12, 0x00, 0x00, 0x00, 0x02,
    ])

    await expect(decode(body)).resolves.toEqual({
      errorCode: 35,
      apiVersions: [{ apiKey: 18, minVersion: 0, maxVersion: 2 }],
    })
  })

  test('decodes a pre-2.4 empty api_keys array', async () => {
    const body = Buffer.from([0x00, 0x23, 0x00, 0x00, 0x00, 0x00])
    await expect(decode(body)).resolves.toEqual({ errorCode: 35, apiVersions: [] })
  })

  test('a non-35 error still uses the flexible decode', async () => {
    const Encoder = require('../../../encoder')
    // INVALID_REQUEST (42) is returned at the version we asked for, so the body is flexible
    const body = new Encoder()
      .writeInt16(42)
      .writeUVarIntArray([])
      .writeInt32(0)
      .writeTaggedFields().buffer

    const data = await decode(body)
    expect(data.errorCode).toEqual(42)
    expect(data.apiVersions).toEqual([])
    expect(data.zkMigrationReady).toBe(false)
  })
})
