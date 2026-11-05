const { decode, parse } = require('./response')

/** One Apache 4.4.0 response vector: the bytes Java wrote and the fields it wrote them from. */
const vector = (file) => require(`../fixtures/java/${file}.json`)
const v5 = (name) => vector(`apiVersionsResponse-v5-${name}`)

/**
 * What a decoder holds for a vector, written out from the vector's fields: the list is named
 * `apiVersions`, the one throttle time is the client-side one, and a broker that sent no
 * `ZkMigrationReady` leaves it false.
 */
const heldFor = ({ fields }) => ({
  errorCode: fields.errorCode,
  apiVersions: fields.apiKeys,
  throttleTime: 0,
  clientSideThrottleTime: fields.throttleTime,
  supportedFeatures: fields.supportedFeatures,
  finalizedFeaturesEpoch: fields.finalizedFeaturesEpoch,
  finalizedFeatures: fields.finalizedFeatures.map(({ name, minVersionLevel, maxVersionLevel }) => ({
    name,
    minVersionLevel,
    maxVersionLevel,
  })),
  zkMigrationReady: false,
})

describe('Protocol > Requests > ApiVersions > v5', () => {
  test.each(['ok', 'features0', 'rebootstrapRequired', 'unknownTags'])(
    'decodes the Apache 4.4.0 %s vector',
    async (name) => {
      const found = v5(name)

      await expect(decode(Buffer.from(found.hex, 'hex'))).resolves.toEqual(heldFor(found))
    }
  )

  test('parse returns what a broker that agrees with the identity answered', async () => {
    const data = await decode(Buffer.from(v5('ok').hex, 'hex'))

    await expect(parse(data)).resolves.toBe(data)
  })

  test('keeps a supported feature whose minimum version is 0', async () => {
    const data = await decode(Buffer.from(v5('features0').hex, 'hex'))

    expect(data.supportedFeatures).toEqual([
      { name: 'metadata.version', minVersion: 1, maxVersion: 25 },
      { name: 'transaction.version', minVersion: 0, maxVersion: 2 },
    ])
  })

  test('parse raises REBOOTSTRAP_REQUIRED as a typed error that is not retriable', async () => {
    const data = await decode(Buffer.from(v5('rebootstrapRequired').hex, 'hex'))

    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'REBOOTSTRAP_REQUIRED',
      code: 129,
      retriable: false,
    })
  })

  test('decodes the v0-shaped UNSUPPORTED_VERSION answer Apache writes at version 0', async () => {
    const found = vector('apiVersionsResponse-v0-unsupportedVersion')
    const data = await decode(Buffer.from(found.hex, 'hex'))

    expect(data).toEqual({ errorCode: 35, apiVersions: found.fields.apiKeys })
    await expect(parse(data)).rejects.toMatchObject({
      type: 'UNSUPPORTED_VERSION',
      apiVersions: [{ apiKey: 18, minVersion: 0, maxVersion: 4 }],
    })
  })

  test('is wire-identical to v4: the same body decodes to the same fields', async () => {
    const { decode: decodeV4 } = require('../v4/response')
    const body = Buffer.from(v5('ok').hex, 'hex')

    await expect(decode(body)).resolves.toEqual(await decodeV4(body))
  })
})
