const createAdmin = require('..')
const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSConnectionError, KafkaJSInvariantViolation } = require('../../errors')

const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }

/** Creates a public admin with brokerless routes. @param {object} [overrides] route overrides @returns {object} fixture */
const fixture = (overrides = {}) => {
  const broker = { nodeId: 1 }
  const cluster = {
    metadata: jest.fn(),
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

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

describe('cluster admin edges', () => {
  describe('electLeaders', () => {
    test('without options it asks the controller for a preferred election of every partition', async () => {
      const { admin, broker } = fixture()
      broker.electLeaders = jest.fn(async () => ({
        replicaElectionResults: [
          {
            topic: 'orders',
            partitionResult: [
              { partitionId: 0, errorCode: 0, errorMessage: null },
              { partitionId: 1, errorCode: 0, errorMessage: null },
            ],
          },
        ],
      }))

      const result = await admin.electLeaders()

      expect(broker.electLeaders).toHaveBeenCalledTimes(1)
      expect(broker.electLeaders).toHaveBeenCalledWith({
        electionType: 'preferred',
        topicPartitions: null,
      })
      expect(
        result.map(({ topic, partitions }) => [topic, partitions.map((p) => p.partition)])
      ).toEqual([['orders', [0, 1]]])
    })

    test('rejects a controller answer without election results', async () => {
      const { admin, broker } = fixture()
      broker.electLeaders = jest.fn(async () => ({}))

      const error = await rejection(admin.electLeaders())

      expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
      expect(error.message).toContain('Missing election results')
    })
  })

  describe('describeFeatures', () => {
    test('a retriable failure of the broker read is retried', async () => {
      const { admin, broker } = fixture()
      broker.apiVersions = jest
        .fn()
        .mockRejectedValueOnce(new KafkaJSConnectionError('flap'))
        .mockResolvedValueOnce({
          supportedFeatures: [{ name: 'metadata.version', minVersion: 1, maxVersion: 5 }],
          finalizedFeatures: [{ name: 'metadata.version', minVersionLevel: 3, maxVersionLevel: 3 }],
          finalizedFeaturesEpoch: '7',
        })

      const result = await admin.describeFeatures()

      expect(broker.apiVersions).toHaveBeenCalledTimes(2)
      expect(result).toEqual({
        supportedFeatures: { 'metadata.version': { minVersion: 1, maxVersion: 5 } },
        finalizedFeatures: { 'metadata.version': { minVersionLevel: 3, maxVersionLevel: 3 } },
        finalizedFeaturesEpoch: '7',
      })
    })

    test('a broker that reports no finalized features or epoch yields empty features and a null epoch', async () => {
      const { admin, broker } = fixture()
      broker.apiVersions = jest.fn(async () => ({ supportedFeatures: [] }))

      const result = await admin.describeFeatures()

      expect(result).toEqual({
        supportedFeatures: {},
        finalizedFeatures: {},
        finalizedFeaturesEpoch: null,
      })
    })
  })

  describe('describeCluster', () => {
    test('an unsupported authorization mask is null and unfenced brokers carry no fenced flag', async () => {
      const { admin, broker } = fixture()
      broker.negotiatedVersion = () => 2
      broker.describeCluster = jest.fn(async () => ({
        brokers: [{ brokerId: 1, host: 'h', port: 9092, rack: null, isFenced: true }],
        controllerId: 1,
        clusterId: 'cluster',
        clusterAuthorizedOperations: -2147483648,
      }))

      const result = await admin.describeCluster({ includeAuthorizedOperations: true })

      expect(broker.describeCluster).toHaveBeenCalledWith({
        includeClusterAuthorizedOperations: true,
        includeFencedBrokers: false,
      })
      expect(result).toEqual({
        brokers: [{ nodeId: 1, host: 'h', port: 9092 }],
        controller: 1,
        clusterId: 'cluster',
        authorizedOperations: null,
      })
    })
  })
})
