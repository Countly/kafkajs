const {
  KafkaJSConnectionError,
  KafkaJSSASLAuthenticationError,
  KafkaJSInvalidConfigurationError,
} = require('../../errors')
const createAdmin = require('..')
const { createLogger, LEVELS } = require('../../loggers')
const { createErrorFromCode } = require('../../protocol/error')
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
/** Creates a public admin with brokerless routes. @param {object} [overrides] route overrides @returns {object} fixture @throws {Error} invalid fixture */
const fixture = (overrides = {}, retryOptions = retry) => {
  const broker = { nodeId: 1 }
  const cluster = {
    metadata: jest.fn(async () => ({ brokers: [], controllerId: -1, clusterId: 'legacy' })),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => broker),
    findConnectedBroker: jest.fn(async () => broker),
    findBroker: jest.fn(async () => broker),
    ...overrides,
  }
  const logger = createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
  return { broker, cluster, admin: createAdmin({ logger, cluster, retry: retryOptions }) }
}

const features = [
  { name: 'a', maxVersionLevel: 1 },
  { name: 'b', maxVersionLevel: 2 },
  { name: 'c', maxVersionLevel: 3 },
]
test('fresh feature reads never use the connect snapshot', async () => {
  const f = fixture()
  f.broker.finalizedFeatures = { 'metadata.version': { maxVersionLevel: 1 } }
  f.broker.apiVersions = jest
    .fn()
    .mockResolvedValueOnce({
      supportedFeatures: [{ name: '__proto__', minVersion: 0, maxVersion: 1 }],
      finalizedFeatures: [{ name: 'metadata.version', minVersionLevel: 2, maxVersionLevel: 2 }],
      finalizedFeaturesEpoch: '9007199254740993',
    })
    .mockResolvedValueOnce({
      finalizedFeatures: [{ name: 'metadata.version', minVersionLevel: 3, maxVersionLevel: 3 }],
      finalizedFeaturesEpoch: '-1',
    })
  const a = await f.admin.describeFeatures({ brokerId: 1 })
  const b = await f.admin.describeFeatures()
  expect(a.finalizedFeatures['metadata.version'].maxVersionLevel).toBe(2)
  expect(b.finalizedFeatures['metadata.version'].maxVersionLevel).toBe(3)
  expect(a.finalizedFeaturesEpoch).toBe('9007199254740993')
  expect(b.finalizedFeaturesEpoch).toBeNull()
  expect(Object.hasOwn(a.supportedFeatures, '__proto__')).toBe(true)
  expect(f.broker.apiVersions.mock.calls).toEqual([
    [{ includeFeatures: true }],
    [{ includeFeatures: true }],
  ])
  expect(f.cluster.findBroker).toHaveBeenCalledWith({ nodeId: 1 })
  expect(f.broker.finalizedFeatures['metadata.version'].maxVersionLevel).toBe(1)
})
test('feature errors preserve raw messages and retry only unresolved entries', async () => {
  const f = fixture()
  f.broker.updateFeatures = jest
    .fn()
    .mockResolvedValueOnce({
      results: [
        { feature: 'a', errorCode: 0 },
        { feature: 'b', errorCode: 42, errorMessage: 'bad' },
        { feature: 'c', errorCode: 7 },
      ],
    })
    .mockResolvedValueOnce({ results: [{ feature: 'c', errorCode: 0 }] })
  const rows = await f.admin.updateFeatures({ features, validateOnly: true })
  expect(rows.map((r) => r.errorCode)).toEqual([0, 42, 0])
  expect(rows[1].errorMessage).toBe('bad')
  expect(rows[1].error.type).toBe('INVALID_REQUEST')
  expect(f.broker.updateFeatures.mock.calls[1][0]).toEqual({
    featureUpdates: [{ feature: 'c', maxVersionLevel: 3, upgradeType: 'upgrade' }],
    validateOnly: true,
  })
})
test('v2 aggregate success completes exactly the requested features', async () => {
  const f = fixture()
  f.broker.updateFeatures = jest.fn(async () => ({ results: null }))
  expect((await f.admin.updateFeatures({ features })).map((r) => [r.feature, r.errorCode])).toEqual(
    [
      ['a', 0],
      ['b', 0],
      ['c', 0],
    ]
  )
})
test.each(
  [
    [],
    [
      { feature: 'a', errorCode: 0 },
      { feature: 'a', errorCode: 0 },
    ],
    [{ feature: 'foreign', errorCode: 0 }],
  ].map((results) => [results])
)('v0 and v1 results need exact identities', async (results) => {
  const f = fixture()
  f.broker.updateFeatures = jest.fn(async () => ({ results }))
  await expect(f.admin.updateFeatures({ features })).rejects.toMatchObject({
    name: 'KafkaJSInvariantViolation',
  })
})
test.each([
  null,
  {},
  { features: [] },
  { features: [{ name: ' ', maxVersionLevel: 0 }] },
  { features: [{ name: 'x', maxVersionLevel: '1' }] },
  { features: [{ name: 'x', maxVersionLevel: -1 }] },
  { features: [{ name: 'x', maxVersionLevel: 32768 }] },
  { features: [{ name: 'x', maxVersionLevel: 1.2 }] },
  { features: [{ name: 'x', maxVersionLevel: NaN }] },
  { features: [{ name: 'x', maxVersionLevel: 1, upgradeType: null }] },
  { features: [{ name: 'x', maxVersionLevel: 1, upgradeType: 'bad' }] },
  {
    features: [
      { name: 'x', maxVersionLevel: 1 },
      { name: 'x', maxVersionLevel: 2 },
    ],
  },
  { features, validateOnly: null },
])('invalid feature update rejects before discovery', async (input) => {
  const f = fixture()
  await expect(f.admin.updateFeatures(input)).rejects.toBeInstanceOf(
    KafkaJSInvalidConfigurationError
  )
  expect(f.cluster.findControllerBroker).not.toHaveBeenCalled()
})
test.each([null, -1, 1.2, '1', NaN, 2147483648])(
  'invalid brokerId %j is refused before lookup',
  async (brokerId) => {
    const f = fixture()
    await expect(f.admin.describeFeatures({ brokerId })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    expect(f.cluster.findBroker).not.toHaveBeenCalled()
  }
)
test('validateOnly causes no follow-up request or local cache change', async () => {
  const f = fixture()
  f.cluster.features = { a: 1 }
  f.broker.updateFeatures = jest.fn(async () => ({ results: null }))
  await f.admin.updateFeatures({ features, validateOnly: true })
  expect(f.broker.updateFeatures).toHaveBeenCalledTimes(1)
  expect(f.cluster.refreshMetadata).not.toHaveBeenCalled()
  expect(f.cluster.features).toEqual({ a: 1 })
  expect(f.broker.updateFeatures.mock.calls[0][0].validateOnly).toBe(true)
})

test.each([0, 1, 2])(
  'update v%i gates validation and downgrade options before wire',
  async (version) => {
    const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
    const pair = require('../../../testHelpers/wire/updateFeaturesV' + version)
    const h = createClusterApiBroker({
      versions: { 57: { minVersion: version, maxVersion: version } },
      respond: () =>
        pair.buildResponse({
          throttleTimeMs: 0,
          results: [{ feature: 'a', errorCode: 0, errorMessage: null }],
        }),
    })
    const featureUpdates = [{ feature: 'a', maxVersionLevel: 0, upgradeType: 'upgrade' }]
    try {
      const r = await h.broker.updateFeatures({ featureUpdates })
      expect(r.results === null).toBe(version === 2)
      const sent = pair.readRequest(h.requests[0].body)
      expect(sent.featureUpdates[0].maxVersionLevel).toBe(0)
      if (version === 0) {
        expect(sent.featureUpdates[0].allowDowngrade).toBe(false)
        await expect(
          h.broker.updateFeatures({ featureUpdates, validateOnly: true })
        ).rejects.toMatchObject({ name: 'KafkaJSServerDoesNotSupportApiKey', apiKey: 57 })
        for (const upgradeType of ['safe-downgrade', 'unsafe-downgrade'])
          await expect(
            h.broker.updateFeatures({ featureUpdates: [{ ...featureUpdates[0], upgradeType }] })
          ).rejects.toMatchObject({ name: 'KafkaJSServerDoesNotSupportApiKey' })
        expect(h.requests).toHaveLength(1)
      } else {
        await h.broker.updateFeatures({
          featureUpdates: [{ ...featureUpdates[0], upgradeType: 'unsafe-downgrade' }],
          validateOnly: true,
        })
        const fields = pair.readRequest(h.requests[1].body)
        expect(fields.validateOnly).toBe(true)
        expect(fields.featureUpdates[0].upgradeType).toBe(3)
      }
    } finally {
      await h.pool.destroy()
    }
  }
)
test('a v2 top-level feature error stays a typed call failure', async () => {
  const f = fixture()
  f.broker.updateFeatures = jest.fn(async () => {
    throw createErrorFromCode(42)
  })
  await expect(f.admin.updateFeatures({ features })).rejects.toMatchObject({ code: 42 })
})

test('authentication refusal before feature update work rejects with the original error', async () => {
  const f = fixture()
  const original = new KafkaJSSASLAuthenticationError('Authentication refused')
  f.broker.updateFeatures = jest.fn().mockRejectedValue(original)
  await expect(f.admin.updateFeatures({ features })).rejects.toBe(original)
  expect(f.broker.updateFeatures).toHaveBeenCalledTimes(1)
})

test('a negative feature level uses the configuration error class', async () => {
  const f = fixture()
  await expect(
    f.admin.updateFeatures({ features: [{ name: 'a', maxVersionLevel: -1 }] })
  ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
  expect(f.cluster.findControllerBroker).not.toHaveBeenCalled()
})

test('a negative brokerId uses the configuration error class', async () => {
  const f = fixture()
  await expect(f.admin.describeFeatures({ brokerId: -1 })).rejects.toBeInstanceOf(
    KafkaJSInvalidConfigurationError
  )
  expect(f.cluster.findConnectedBroker).not.toHaveBeenCalled()
  expect(f.cluster.findBroker).not.toHaveBeenCalled()
})

test('a feature name containing NUL rejects before controller lookup', async () => {
  const f = fixture()
  f.broker.updateFeatures = jest.fn(async () => ({
    results: [{ feature: 'a\0b', errorCode: 0, errorMessage: null }],
  }))
  await expect(
    f.admin.updateFeatures({ features: [{ name: 'a\0b', maxVersionLevel: 1 }] })
  ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
  expect(f.cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled()
  expect(f.cluster.findControllerBroker).not.toHaveBeenCalled()
  expect(f.broker.updateFeatures).not.toHaveBeenCalled()
})

test('a transient controller refresh failure preserves feature successes', async () => {
  const f = fixture({}, { ...retry, retries: 2 })
  const original = new KafkaJSConnectionError('refresh failed')
  f.cluster.refreshMetadata.mockRejectedValueOnce(original)
  f.broker.updateFeatures = jest
    .fn()
    .mockResolvedValueOnce({
      results: [
        { feature: 'a', errorCode: 0, errorMessage: null },
        { feature: 'b', errorCode: 41, errorMessage: 'moved' },
      ],
    })
    .mockImplementation(async ({ featureUpdates }) => ({
      results: featureUpdates.map(({ feature }) => ({
        feature,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
  const rows = await f.admin.updateFeatures({ features: features.slice(0, 2) })
  expect(f.broker.updateFeatures.mock.calls[1][0].featureUpdates.map((f) => f.feature)).toEqual([
    'b',
  ])
  expect(rows.map((f) => f.errorCode)).toEqual([0, 0])
  expect(f.broker.updateFeatures).toHaveBeenCalledTimes(2)
  expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(2)
  expect(f.cluster.refreshMetadata.mock.invocationCallOrder[1]).toBeLessThan(
    f.broker.updateFeatures.mock.invocationCallOrder[1]
  )
})

test('an exhausted controller refresh failure preserves feature successes', async () => {
  const f = fixture()
  const original = new KafkaJSConnectionError('refresh failed')
  f.cluster.refreshMetadata.mockRejectedValue(original)
  f.broker.updateFeatures = jest.fn(async ({ featureUpdates }) => ({
    results: featureUpdates.map(({ feature }) => ({
      feature,
      errorCode: feature === 'a' ? 0 : 41,
      errorMessage: null,
    })),
  }))
  const rows = await f.admin.updateFeatures({ features: features.slice(0, 2) })
  expect(rows.map((f) => f.errorCode)).toEqual([0, -1])
  expect(rows[0].error).toBeNull()
  expect(rows[1].error).toBe(original)
  expect(f.broker.updateFeatures).toHaveBeenCalledTimes(1)
  expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
})

test('no retry budget preserves feature rows without controller refresh', async () => {
  const f = fixture({}, { ...retry, retries: 0 })
  f.cluster.refreshMetadata.mockRejectedValue(new KafkaJSConnectionError('refresh failed'))
  f.broker.updateFeatures = jest.fn(async () => ({
    results: [
      { feature: 'a', errorCode: 0, errorMessage: null },
      { feature: 'b', errorCode: 41, errorMessage: 'moved' },
    ],
  }))
  const rows = await f.admin.updateFeatures({ features: features.slice(0, 2) })
  expect(rows.map((f) => f.errorCode)).toEqual([0, 41])
  expect(rows[0].error).toBeNull()
  expect(rows[1].error.code).toBe(41)
  expect(f.cluster.refreshMetadata).not.toHaveBeenCalled()
  expect(f.broker.updateFeatures).toHaveBeenCalledTimes(1)
})

test.each(['KafkaJSSASLAuthenticationError', 'KafkaJSServerDoesNotSupportApiKey'])(
  'controller refresh %s rejects the mutation call with the original error',
  async (name) => {
    const f = fixture()
    const ErrorClass = require('../../errors')[name]
    const original = new ErrorClass('refresh refused')
    f.cluster.refreshMetadata.mockRejectedValue(original)
    f.broker.updateFeatures = jest.fn(async () => ({
      results: [
        { feature: 'a', errorCode: 0 },
        { feature: 'b', errorCode: 41 },
      ],
    }))
    await expect(f.admin.updateFeatures({ features: features.slice(0, 2) })).rejects.toBe(original)
    expect(f.broker.updateFeatures).toHaveBeenCalledTimes(1)
    expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  }
)

test('deadline during controller refresh forbids another mutation request', async () => {
  jest.useFakeTimers()
  const baseline = jest.getTimerCount()
  let release
  const f = fixture()
  f.cluster.refreshMetadata.mockImplementation(
    () =>
      new Promise((resolve) => {
        release = resolve
      })
  )
  f.broker.updateFeatures = jest.fn(async () => ({
    results: [
      { feature: 'a', errorCode: 0 },
      { feature: 'b', errorCode: 41 },
    ],
  }))
  const result = f.admin
    .updateFeatures({ ...{ features: features.slice(0, 2) }, apiTimeout: 20 })
    .catch((e) => e)
  try {
    await jest.advanceTimersByTimeAsync(5)
    expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(15)
    expect(await result).toMatchObject({ name: 'KafkaJSTimeout' })
    release()
    await Promise.resolve()
    await Promise.resolve()
    expect(f.broker.updateFeatures).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(baseline)
  } finally {
    release?.()
    jest.useRealTimers()
  }
})
