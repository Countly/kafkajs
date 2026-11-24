const createAdmin = require('..')
const { createLogger, LEVELS } = require('../../loggers')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSConnectionError,
  KafkaJSSASLAuthenticationError,
  KafkaJSInvalidConfigurationError,
} = require('../../errors')
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

const selection = [{ topic: 'orders', partitions: [0, 1, 2] }]
/** Builds identity-bearing election rows. @param {number[]} codes per-partition codes @returns {object} response */
const answer = (codes) => ({
  replicaElectionResults: [
    {
      topic: 'orders',
      partitionResult: codes.map((errorCode, partitionId) => ({
        partitionId,
        errorCode,
        errorMessage: errorCode ? 'reason' : null,
      })),
    },
  ],
})
test('public cluster methods are functions', () => {
  const { admin } = fixture()
  for (const name of ['electLeaders', 'describeFeatures', 'updateFeatures', 'listConfigResources'])
    expect(typeof admin[name]).toBe('function')
})
test('a partition answered with its own code, REQUEST_TIMED_OUT included, is not sent again', async () => {
  const f = fixture()
  f.broker.electLeaders = jest
    .fn()
    .mockResolvedValueOnce(answer([0, 80, 7]))
    .mockResolvedValueOnce(answer([0, 0, 0]))
  const rows = await f.admin.electLeaders({ topicPartitions: selection })
  expect(rows[0].partitions.map((p) => p.errorCode)).toEqual([0, 80, 7])
  expect(rows[0].partitions[1].error.type).toBe('PREFERRED_LEADER_NOT_AVAILABLE')
  expect(rows[0].partitions[2].error.type).toBe('REQUEST_TIMED_OUT')
  expect(f.broker.electLeaders).toHaveBeenCalledTimes(1)
})
test('resends a whole-request NOT_CONTROLLER once and nothing else', async () => {
  const f = fixture({}, { ...retry, retries: 3 })
  f.broker.electLeaders = jest
    .fn()
    .mockRejectedValueOnce(createErrorFromCode(41))
    .mockResolvedValueOnce(answer([0, 41, 7]))
    .mockResolvedValue(answer([0, 0, 0]))
  const rows = await f.admin.electLeaders({ topicPartitions: selection })
  expect(rows[0].partitions.map((p) => p.errorCode)).toEqual([0, 41, 7])
  expect(f.broker.electLeaders.mock.calls).toEqual([
    [{ electionType: 'preferred', topicPartitions: selection }],
    [{ electionType: 'preferred', topicPartitions: selection }],
  ])
  expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
})
test.each([null, selection])(
  'passes KafkaJSUnknownOutcomeError through unchanged for %j',
  async (topicPartitions) => {
    const { KafkaJSUnknownOutcomeError } = require('../../errors')
    const unknown = new KafkaJSUnknownOutcomeError('lost', { requests: [] })
    const f = fixture()
    f.broker.electLeaders = jest.fn().mockRejectedValue(unknown)
    await expect(f.admin.electLeaders({ topicPartitions })).rejects.toBe(unknown)
    expect(f.broker.electLeaders).toHaveBeenCalledTimes(1)
  }
)
test('controller errors relocate once before retry', async () => {
  const f = fixture()
  f.broker.electLeaders = jest
    .fn()
    .mockRejectedValueOnce(createErrorFromCode(41))
    .mockResolvedValueOnce(answer([0, 0, 0]))
  await f.admin.electLeaders({ topicPartitions: selection })
  expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  expect(f.cluster.findControllerBroker).toHaveBeenCalledTimes(2)
})
test('all partitions sends one null election and keeps each partition result', async () => {
  const f = fixture()
  f.broker.electLeaders = jest
    .fn()
    .mockResolvedValueOnce(answer([0, 80, 7]))
    .mockResolvedValueOnce(answer([0, 0, 0]))
  const rows = await f.admin.electLeaders({ topicPartitions: null })
  expect(rows[0].partitions.map((p) => p.errorCode)).toEqual([0, 80, 7])
  expect(f.broker.electLeaders.mock.calls.map((c) => c[0].topicPartitions)).toEqual([null])
})
test('empty all-partition answer is a valid empty result', async () => {
  const f = fixture()
  f.broker.electLeaders = jest.fn(async () => ({ replicaElectionResults: [] }))
  await expect(f.admin.electLeaders({})).resolves.toEqual([])
})
test.each([31, 41])('exhausted top-level code %i throws', async (code) => {
  const f = fixture()
  f.broker.electLeaders = jest.fn(async () => {
    throw createErrorFromCode(code)
  })
  await expect(f.admin.electLeaders({ topicPartitions: selection })).rejects.toMatchObject({ code })
})
test.each([
  {
    replicaElectionResults: [
      {
        topic: 'orders',
        partitionResult: [
          { partitionId: 0, errorCode: 0 },
          { partitionId: 0, errorCode: 0 },
        ],
      },
    ],
  },
  {
    replicaElectionResults: [
      { topic: 'foreign', partitionResult: [{ partitionId: 0, errorCode: 0 }] },
    ],
  },
  { replicaElectionResults: [] },
  {
    replicaElectionResults: [
      { topic: 'orders', partitionResult: [{ partitionId: 0, errorCode: 0 }] },
      { topic: 'orders', partitionResult: [] },
    ],
  },
])('malformed election identities never publish success', async (response) => {
  const f = fixture()
  f.broker.electLeaders = jest.fn(async () => response)
  await expect(f.admin.electLeaders({ topicPartitions: selection })).rejects.toMatchObject({
    name: 'KafkaJSInvariantViolation',
  })
})
test.each([
  null,
  [],
  { electionType: null },
  { electionType: 'random' },
  { topicPartitions: 'all' },
  { topicPartitions: [{ topic: 'a', partitions: [] }] },
  { topicPartitions: [{ topic: 'a', partitions: [0, 0] }] },
  { topicPartitions: [{ topic: 'a', partitions: [NaN] }] },
  { topicPartitions: [{ topic: 'a', partitions: [-1] }] },
  { topicPartitions: [{ topic: 'a', partitions: ['0'] }] },
  { topicPartitions: [{ topic: 'a', partitions: [2147483648] }] },
])('invalid election input rejects before routing', async (options) => {
  const f = fixture()
  await expect(f.admin.electLeaders(options)).rejects.toBeInstanceOf(
    KafkaJSInvalidConfigurationError
  )
  expect(f.cluster.findControllerBroker).not.toHaveBeenCalled()
})
test('deadline during route lookup forbids a later election request', async () => {
  jest.useFakeTimers()
  const baseline = jest.getTimerCount()
  let release
  const f = fixture({
    findControllerBroker: jest.fn(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    ),
  })
  f.broker.electLeaders = jest.fn()
  const result = f.admin.electLeaders({ topicPartitions: selection, apiTimeout: 5 }).catch((e) => e)
  await jest.advanceTimersByTimeAsync(5)
  expect(await result).toMatchObject({ name: 'KafkaJSTimeout' })
  release(f.broker)
  await Promise.resolve()
  await Promise.resolve()
  expect(f.broker.electLeaders).not.toHaveBeenCalled()
  expect(jest.getTimerCount()).toBe(baseline)
  jest.useRealTimers()
})

test.each([0, 1, 2])('election v%i transmits only representable fields', async (version) => {
  const { createClusterApiBroker } = require('./fixtures/clusterApiHarness')
  const pair = require('../../../testHelpers/wire/electLeadersV' + version)
  const h = createClusterApiBroker({
    versions: { 43: { minVersion: version, maxVersion: version } },
    respond: () => pair.buildResponse({ throttleTimeMs: 0 }),
  })
  try {
    await h.broker.electLeaders({ electionType: 'preferred', topicPartitions: null })
    expect(pair.readRequest(h.requests[0].body).topicPartitions).toBeNull()
    expect(h.requests[0].apiVersion).toBe(version)
    if (version === 0) {
      await expect(h.broker.electLeaders({ electionType: 'unclean' })).rejects.toMatchObject({
        name: 'KafkaJSServerDoesNotSupportApiKey',
        apiKey: 43,
      })
      expect(h.requests).toHaveLength(1)
    } else {
      await h.broker.electLeaders({ electionType: 'unclean', topicPartitions: null })
      expect(pair.readRequest(h.requests[1].body).electionType).toBe(1)
    }
  } finally {
    await h.pool.destroy()
  }
})

test('authentication refusal before election work rejects with the original error', async () => {
  const f = fixture()
  const original = new KafkaJSSASLAuthenticationError('Authentication refused')
  f.broker.electLeaders = jest.fn().mockRejectedValue(original)
  await expect(f.admin.electLeaders({ topicPartitions: selection })).rejects.toBe(original)
  expect(f.broker.electLeaders).toHaveBeenCalledTimes(1)
})

test('invalid electionType uses the configuration error class', async () => {
  const f = fixture()
  await expect(f.admin.electLeaders({ electionType: 'random' })).rejects.toBeInstanceOf(
    KafkaJSInvalidConfigurationError
  )
  expect(f.cluster.findControllerBroker).not.toHaveBeenCalled()
})

test('a topic name containing NUL rejects before controller lookup', async () => {
  const f = fixture()
  f.broker.electLeaders = jest.fn(async () => ({
    replicaElectionResults: [
      { topic: 'a\0b', partitionResult: [{ partitionId: 0, errorCode: 0, errorMessage: null }] },
    ],
  }))
  await expect(
    f.admin.electLeaders({ topicPartitions: [{ topic: 'a\0b', partitions: [0] }] })
  ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
  expect(f.cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled()
  expect(f.cluster.findControllerBroker).not.toHaveBeenCalled()
  expect(f.broker.electLeaders).not.toHaveBeenCalled()
})

test('an explicit empty election selection returns no work without routing', async () => {
  const f = fixture()
  f.broker.electLeaders = jest.fn()
  await expect(f.admin.electLeaders({ topicPartitions: [] })).resolves.toEqual([])
  expect(f.cluster.metadata).not.toHaveBeenCalled()
  expect(f.cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled()
  expect(f.cluster.refreshMetadata).not.toHaveBeenCalled()
  expect(f.cluster.findControllerBroker).not.toHaveBeenCalled()
  expect(f.broker.electLeaders).not.toHaveBeenCalled()
})

test('the shared topic-partition type has one public declaration', () => {
  const declarations = require('fs').readFileSync(
    require('path').join(__dirname, '../../../types/index.d.ts'),
    'utf8'
  )
  expect(declarations.match(/^export type TopicPartitionList\s*=/gm)).toHaveLength(1)
})

test.each([null, [{ topic: 'orders', partitions: [0, 1] }]])(
  'a partition answered NOT_CONTROLLER is its result, with no refresh and no resend, for %j',
  async (topicPartitions) => {
    const f = fixture({}, { ...retry, retries: 2 })
    f.broker.electLeaders = jest.fn(async () => answer([0, 41]))
    const rows = await f.admin.electLeaders({ topicPartitions })
    expect(rows[0].partitions.map((p) => p.errorCode)).toEqual([0, 41])
    expect(rows[0].partitions[1].error.code).toBe(41)
    expect(f.broker.electLeaders).toHaveBeenCalledTimes(1)
    expect(f.cluster.refreshMetadata).not.toHaveBeenCalled()
  }
)

test.each([null, [{ topic: 'orders', partitions: [0, 1] }]])(
  'no retry budget preserves election rows without controller refresh for %j',
  async (topicPartitions) => {
    const f = fixture({}, { ...retry, retries: 0 })
    f.cluster.refreshMetadata.mockRejectedValue(new KafkaJSConnectionError('refresh failed'))
    f.broker.electLeaders = jest.fn(async () => answer([0, 41]))
    const rows = await f.admin.electLeaders({ topicPartitions })
    expect(rows[0].partitions.map((p) => p.errorCode)).toEqual([0, 41])
    expect(rows[0].partitions[0].error).toBeNull()
    expect(rows[0].partitions[1].error.code).toBe(41)
    expect(f.cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(f.broker.electLeaders).toHaveBeenCalledTimes(1)
  }
)

test.each(['KafkaJSSASLAuthenticationError', 'KafkaJSServerDoesNotSupportApiKey'])(
  'controller refresh %s after a whole-request NOT_CONTROLLER rejects the call with the original error',
  async (name) => {
    const f = fixture()
    const ErrorClass = require('../../errors')[name]
    const original = new ErrorClass('refresh refused')
    f.cluster.refreshMetadata.mockRejectedValue(original)
    f.broker.electLeaders = jest.fn(async () => {
      throw createErrorFromCode(41)
    })
    await expect(
      f.admin.electLeaders({
        topicPartitions: [{ topic: 'orders', partitions: [0, 1] }],
      })
    ).rejects.toBe(original)
    expect(f.broker.electLeaders).toHaveBeenCalledTimes(1)
    expect(f.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  }
)
