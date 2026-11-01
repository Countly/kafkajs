const {
  KafkaJSInvalidConfigurationError,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSSASLAuthenticationError,
} = require('../../errors')
const createAdmin = require('..')
const { createLogger, LEVELS } = require('../../loggers')
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
/** Creates a public admin with brokerless routes. @param {object} [overrides] route overrides @returns {object} fixture @throws {Error} invalid fixture */
const fixture = (overrides = {}) => {
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
  return { broker, cluster, admin: createAdmin({ logger, cluster, retry }) }
}

test('configuration listing preserves the public shape', async () => {
  const f = fixture()
  f.broker.listConfigResources = jest.fn(async () => ({
    configResources: [{ resourceName: 'subscription', resourceType: 16 }],
  }))
  await expect(f.admin.listConfigResources({ resourceTypes: [16] })).resolves.toEqual({
    resources: [{ name: 'subscription', type: 16 }],
  })
})
test('v0 resources acquire only the protocol default type', async () => {
  const f = fixture()
  f.broker.listConfigResources = jest.fn(async () => ({
    configResources: [{ resourceName: 'subscription' }],
  }))
  await expect(f.admin.listConfigResources({ resourceTypes: [16] })).resolves.toEqual({
    resources: [{ name: 'subscription', type: 16 }],
  })
})
test.each([
  null,
  [],
  { resourceTypes: null },
  { resourceTypes: [16, 16] },
  { resourceTypes: [null] },
  { resourceTypes: ['16'] },
  { resourceTypes: [99] },
  { resourceTypes: [1.5] },
])('invalid resource types reject before lookup', async (options) => {
  const f = fixture()
  await expect(f.admin.listConfigResources(options)).rejects.toBeInstanceOf(
    KafkaJSInvalidConfigurationError
  )
  expect(f.cluster.findConnectedBroker).not.toHaveBeenCalled()
})
test('unrequested and duplicate config resource identities reject', async () => {
  const f = fixture()
  f.broker.listConfigResources = jest.fn(async () => ({
    configResources: [
      { resourceName: 'same', resourceType: 16 },
      { resourceName: 'same', resourceType: 16 },
    ],
  }))
  await expect(f.admin.listConfigResources()).rejects.toMatchObject({
    name: 'KafkaJSInvariantViolation',
  })
  f.broker.listConfigResources.mockResolvedValue({
    configResources: [{ resourceName: 'a', resourceType: 32 }],
  })
  await expect(f.admin.listConfigResources({ resourceTypes: [16] })).rejects.toMatchObject({
    name: 'KafkaJSInvariantViolation',
  })
})
test('no deprecated client metrics listing method is added', () => {
  expect(fixture().admin.listClientMetricsResources).toBeUndefined()
})

test.each([0, 1])(
  'resource listing v%i preserves the exact v0 filter equivalence',
  async (version) => {
    const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
    const pair = require('../../../testHelpers/wire/listConfigResourcesV' + version)
    const h = createClusterApiBroker({
      versions: { 74: { minVersion: version, maxVersion: version } },
      respond: () => pair.buildResponse({ throttleTimeMs: 0 }),
    })
    try {
      await h.broker.listConfigResources({ resourceTypes: [16] })
      expect(pair.readRequest(h.requests[0].body)).toEqual(version ? { resourceTypes: [16] } : {})
      if (!version) {
        for (const resourceTypes of [[32], [16, 32]])
          await expect(h.broker.listConfigResources({ resourceTypes })).rejects.toMatchObject({
            name: 'KafkaJSServerDoesNotSupportApiKey',
            apiKey: 74,
          })
        expect(h.requests).toHaveLength(1)
      }
    } finally {
      await h.pool.destroy()
    }
  }
)

test('an unknown resource type uses the configuration error class', async () => {
  const f = fixture()
  await expect(f.admin.listConfigResources({ resourceTypes: [99] })).rejects.toBeInstanceOf(
    KafkaJSInvalidConfigurationError
  )
  expect(f.cluster.findConnectedBroker).not.toHaveBeenCalled()
})

test.each([
  [
    'ListConfigResources',
    74,
    'listConfigResources',
    { resourceTypes: [32] },
    'resourceTypes',
    1,
    1,
  ],
  ['ElectLeaders', 43, 'electLeaders', { electionType: 'unclean' }, 'electionType', 1, 2],
  [
    'DescribeCluster',
    60,
    'describeCluster',
    { includeFencedBrokers: true },
    'includeFencedBrokers',
    2,
    2,
  ],
  [
    'UpdateFeatures',
    57,
    'updateFeatures',
    {
      featureUpdates: [{ feature: 'feature', maxVersionLevel: 1, upgradeType: 'upgrade' }],
      validateOnly: true,
    },
    'validateOnly',
    1,
    2,
  ],
  [
    'UpdateFeatures',
    57,
    'updateFeatures',
    { featureUpdates: [{ feature: 'feature', maxVersionLevel: 1, upgradeType: 'safe-downgrade' }] },
    'upgradeType',
    1,
    2,
  ],
  [
    'UpdateFeatures',
    57,
    'updateFeatures',
    {
      featureUpdates: [{ feature: 'feature', maxVersionLevel: 1, upgradeType: 'unsafe-downgrade' }],
    },
    'upgradeType',
    1,
    2,
  ],
])(
  'unsupported %s option %s %j names both ranges before dispatch',
  async (apiName, apiKey, method, options, option, required, clientMax) => {
    const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
    const h = createClusterApiBroker({
      versions: { [apiKey]: { minVersion: 0, maxVersion: 0 } },
      respond: () => {
        throw new Error('Unsupported request was sent')
      },
    })
    try {
      const error = await h.broker[method](options).catch((error) => error)
      expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
      expect(error).toMatchObject({ apiKey, apiName, retriable: false })
      expect(error.message).toContain(option)
      expect(error.message).toContain(`requires ${apiName} v${required}+`)
      expect(error.message).toContain(`client implements v0-v${clientMax}`)
      expect(error.message).toContain('broker supports v0-v0')
      expect(error.cause).toBeUndefined()
      expect(h.requests).toHaveLength(0)
    } finally {
      await h.pool.destroy()
    }
  }
)

test.each([
  [{}, 'no range'],
  [{ 74: { minVersion: 2, maxVersion: 3 } }, 'v2-v3'],
  [{ 74: { maxVersion: 0 } }, 'v0-v0'],
])(
  'unavailable resource filtering names both ranges for %j before dispatch',
  async (versions, offered) => {
    const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
    const h = createClusterApiBroker({
      versions,
      respond: () => {
        throw new Error('Unsupported request was sent')
      },
    })
    try {
      const error = await h.broker
        .listConfigResources({ resourceTypes: [32] })
        .catch((error) => error)
      expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
      expect(error).toMatchObject({ apiKey: 74, apiName: 'ListConfigResources', retriable: false })
      expect(error.message).toContain('resourceTypes requires ListConfigResources v1+')
      expect(error.message).toContain('client implements v0-v1')
      expect(error.message).toContain(`broker supports ${offered}`)
      expect(error.cause).toBeUndefined()
      expect(h.requests).toHaveLength(0)
    } finally {
      await h.pool.destroy()
    }
  }
)

test('cluster capability selection preserves the original failure before dispatch', async () => {
  const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
  const h = createClusterApiBroker({
    versions: {},
    respond: () => {
      throw new Error('Unexpected request')
    },
  })
  const original = new KafkaJSSASLAuthenticationError('Authentication refused')
  jest.spyOn(h.broker, 'negotiatedVersion').mockImplementation(() => {
    throw original
  })
  try {
    await expect(h.broker.listConfigResources({ resourceTypes: [32] })).rejects.toBe(original)
    expect(h.requests).toHaveLength(0)
  } finally {
    await h.pool.destroy()
  }
})
