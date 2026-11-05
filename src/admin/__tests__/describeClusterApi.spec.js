const {
  KafkaJSInvalidConfigurationError,
  KafkaJSServerDoesNotSupportApiKey,
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

const metadata = {
  brokers: [{ nodeId: 0, host: 'host', port: 9092, rack: 'rack' }],
  controllerId: -1,
  clusterId: 'cluster',
  clusterAuthorizedOperations: -2147483648,
}
test('the default call retains the exact legacy Metadata invocation and shape', async () => {
  const f = fixture({ metadata: jest.fn(async () => metadata) })
  await expect(f.admin.describeCluster()).resolves.toEqual({
    brokers: [{ nodeId: 0, host: 'host', port: 9092 }],
    controller: null,
    clusterId: 'cluster',
  })
  expect(f.cluster.metadata.mock.calls).toEqual([[{ topics: [] }]])
  expect(f.cluster.findConnectedBroker).not.toHaveBeenCalled()
})
test('a requested flag selects key60 and projects only additive fields', async () => {
  const f = fixture()
  f.broker.negotiatedVersion = () => 2
  f.broker.describeCluster = jest.fn(async () => ({
    brokers: [{ brokerId: 1, host: 'h', port: 9092, rack: null, isFenced: true }],
    controllerId: 1,
    clusterId: 'cluster',
    clusterAuthorizedOperations: 0,
  }))
  await expect(
    f.admin.describeCluster({ includeAuthorizedOperations: true, includeFencedBrokers: true })
  ).resolves.toEqual({
    brokers: [{ nodeId: 1, host: 'h', port: 9092, fenced: true }],
    controller: 1,
    clusterId: 'cluster',
    authorizedOperations: 0,
  })
  expect(f.broker.describeCluster).toHaveBeenCalledWith({
    includeClusterAuthorizedOperations: true,
    includeFencedBrokers: true,
  })
})
test.each([-2147483648, null, 0, 2147483647])(
  'Metadata fallback retains raw mask %j or null',
  async (mask) => {
    const f = fixture()
    f.broker.negotiatedVersion = () => null
    f.broker.metadata = jest.fn(async () => ({ ...metadata, clusterAuthorizedOperations: mask }))
    const result = await f.admin.describeCluster({ includeAuthorizedOperations: true })
    expect(result.authorizedOperations).toBe(mask === -2147483648 ? null : mask)
    expect(f.broker.metadata).toHaveBeenCalledWith([], { includeClusterAuthorizedOperations: true })
  }
)
test.each([
  [{}, 'no range'],
  [{ 60: { minVersion: 3, maxVersion: 4 } }, 'v3-v4'],
])(
  'fenced listing names both ranges without Metadata fallback for %j',
  async (versions, offered) => {
    const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
    const h = createClusterApiBroker({
      versions,
      respond: () => {
        throw new Error('Unsupported request was sent')
      },
    })
    const f = fixture({ findConnectedBroker: jest.fn(async () => h.broker) })
    const metadata = jest.spyOn(h.broker, 'metadata')
    try {
      const error = await f.admin
        .describeCluster({ includeFencedBrokers: true })
        .catch((error) => error)
      expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
      expect(error).toMatchObject({ apiKey: 60, apiName: 'DescribeCluster', retriable: false })
      expect(error.message).toContain('includeFencedBrokers requires DescribeCluster v2')
      expect(error.message).toContain('client implements v0-v2')
      expect(error.message).toContain(`broker supports ${offered}`)
      expect(metadata).not.toHaveBeenCalled()
      expect(h.requests).toHaveLength(0)
    } finally {
      await h.pool.destroy()
    }
  }
)
test.each([null, 'true', 0, 1])('invalid flags are refused before route lookup', async (value) => {
  const f = fixture()
  await expect(
    f.admin.describeCluster({ includeAuthorizedOperations: value })
  ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
  expect(f.cluster.findConnectedBroker).not.toHaveBeenCalled()
})

test('public calls traverse negotiated bytes on the strict socket seam', async () => {
  const { Kafka } = require('../../../index')
  const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
  const wire = createScriptedCluster({
    nodes: [{ id: 0, host: 'node0', port: 9092 }],
    profile: 'clusterAdmin',
  })
  producerScript(wire, { topic: 'orders', partitions: 3 })
  const admin = new Kafka(wire.kafkaOptions({ retry, logLevel: 0 })).admin({ retry })
  try {
    await admin.connect()
    await admin.describeCluster()
    const { readRequestHeader } = require('testHelpers')
    const frames = wire.fake.state.writes.filter((frame) => readRequestHeader(frame).apiKey === 3)
    const frame = frames.at(-1)
    const header = readRequestHeader(frame)
    expect(header.apiVersion).toBe(13)
    expect(frame.subarray(header.byteLength).toString('hex')).toBe('00000000')
    wire.answer('ElectLeaders', ({ topicPartitions }) => ({
      replicaElectionResults: (topicPartitions ?? []).map((t) => ({
        topic: t.topic,
        partitionResult: t.partitions.map((partitionId) => ({
          partitionId,
          errorCode: 0,
          errorMessage: null,
        })),
      })),
    }))
    const elected = await admin.electLeaders({
      topicPartitions: [{ topic: 'orders', partitions: [0, 2] }],
    })
    expect(elected[0].partitions[0].error).toBeNull()
    expect(elected[0].partitions.map((p) => p.errorCode)).toEqual([0, 0])
    const updated = await admin.updateFeatures({
      features: [{ name: 'metadata.version', maxVersionLevel: 25 }],
      validateOnly: true,
    })
    expect(updated[0].errorCode).toBe(0)
    // A fresh read on a connection that has negotiated asks v5 (a v4 broker refuses it), then v4.
    const answered = () =>
      wire.requests().filter((r) => r.api === 'ApiVersions' && r.version === 4).length
    const before = answered()
    wire.answer('ApiVersions', () => ({
      ...wire.apiVersionsAnswer(),
      supportedFeatures: [{ name: 'metadata.version', minVersion: 1, maxVersion: 25 }],
      finalizedFeatures: [{ name: 'metadata.version', minVersionLevel: 25, maxVersionLevel: 25 }],
      finalizedFeaturesEpoch: '9007199254740993',
    }))
    const fresh = await admin.describeFeatures()
    expect(fresh.finalizedFeaturesEpoch).toBe('9007199254740993')
    expect(fresh.finalizedFeatures['metadata.version'].maxVersionLevel).toBe(25)
    await admin.describeFeatures()
    expect(answered()).toBe(before + 2)
    const described = await admin.describeCluster({ includeFencedBrokers: true })
    expect(described.brokers[0].fenced).toBe(true)
    const listed = await admin.listConfigResources({ resourceTypes: [16] })
    expect(listed.resources).toEqual([{ type: 16, name: 'subscription' }])
    expect(wire.requests().find((r) => r.api === 'UpdateFeatures').fields.validateOnly).toBe(true)
  } finally {
    await admin.disconnect()
  }
  wire.assertClean()
})

test.each([0, 1, 2])('cluster v%i rejects unrepresentable fenced listing', async (version) => {
  const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
  const pair = require('../../../testHelpers/wire/describeClusterV' + version)
  const h = createClusterApiBroker({
    versions: { 60: { minVersion: version, maxVersion: version } },
    respond: () => pair.buildResponse({ throttleTimeMs: 0 }),
  })
  try {
    await h.broker.describeCluster({
      includeClusterAuthorizedOperations: true,
      includeFencedBrokers: false,
    })
    const request = pair.readRequest(h.requests[0].body)
    expect(request.includeClusterAuthorizedOperations).toBe(true)
    if (version < 2) {
      await expect(h.broker.describeCluster({ includeFencedBrokers: true })).rejects.toMatchObject({
        name: 'KafkaJSServerDoesNotSupportApiKey',
        apiKey: 60,
      })
      expect(h.requests).toHaveLength(1)
    } else {
      await h.broker.describeCluster({ includeFencedBrokers: true })
      expect(pair.readRequest(h.requests[1].body).includeFencedBrokers).toBe(true)
    }
  } finally {
    await h.pool.destroy()
  }
})

test.each([7, 10, 13])(
  'Metadata authorization fallback chooses a representable range from v%i',
  async (maximum) => {
    const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
    const h = createClusterApiBroker({
      versions: { 3: { minVersion: maximum === 13 ? 13 : 7, maxVersion: maximum } },
      respond: () => Buffer.alloc(0),
    })
    const selected = []
    h.pool.send = async (protocol) => {
      selected.push(protocol)
      return {
        brokers: [],
        controllerId: -1,
        clusterId: 'c',
        clusterAuthorizedOperations: maximum === 10 ? 0 : undefined,
      }
    }
    try {
      const result = await h.broker.metadata([], { includeClusterAuthorizedOperations: true })
      expect(selected[0].request.apiVersion).toBe(maximum)
      const body = (await selected[0].request.encode()).buffer
      if (maximum === 10) expect(body.toString('hex')).toBe('0001010000')
      expect(result.clusterAuthorizedOperations).toBe(maximum === 10 ? 0 : undefined)
    } finally {
      await h.pool.destroy()
    }
  }
)

test('a nonboolean cluster flag uses the configuration error class', async () => {
  const f = fixture()
  await expect(
    f.admin.describeCluster({ includeAuthorizedOperations: 'true' })
  ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
  expect(f.cluster.metadata).not.toHaveBeenCalled()
  expect(f.cluster.findConnectedBroker).not.toHaveBeenCalled()
})
