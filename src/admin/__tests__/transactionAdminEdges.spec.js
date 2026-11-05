const createAdmin = require('..')
const { createLogger, LEVELS } = require('../../loggers')
const {
  KafkaJSConnectionError,
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSTimeout,
} = require('../../errors')

const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }

/** Builds a brokerless public admin. @param {object} [overrides] cluster routes @returns {object} fixture */
const fixture = (overrides = {}) => {
  const broker = { nodeId: 1 }
  const cluster = {
    addTargetTopic: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    findBroker: jest.fn(async () => broker),
    findLeaderForPartitions: jest.fn((_topic, partitions) => ({ 1: partitions })),
    findGroupCoordinator: jest.fn(async () => broker),
    brokerPool: { brokers: { 1: broker } },
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

describe('transaction administration edges', () => {
  describe('describeProducers', () => {
    test('rejects one topic named twice before any routing', async () => {
      const { admin, cluster } = fixture()

      const error = await rejection(
        admin.describeProducers({
          topicPartitions: [
            { topic: 'orders', partitions: [0] },
            { topic: 'orders', partitions: [1] },
          ],
        })
      )

      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.message).toContain('Duplicate topic')
      expect(cluster.addTargetTopic).not.toHaveBeenCalled()
    })

    test('rejects a broker answer without topics', async () => {
      const { admin, broker } = fixture()
      broker.describeProducers = jest.fn(async () => ({}))

      const error = await rejection(
        admin.describeProducers({ topicPartitions: [{ topic: 'orders', partitions: [0] }] })
      )

      expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
      expect(error.message).toContain('Missing producer topics')
    })

    test('a partition with no single leader is a retriable local failure that is retried', async () => {
      const { admin, cluster } = fixture({ findLeaderForPartitions: jest.fn(() => ({})) })

      const [topic] = await admin.describeProducers({
        topicPartitions: [{ topic: 'orders', partitions: [0] }],
      })

      expect(cluster.findLeaderForPartitions).toHaveBeenCalledTimes(2)
      expect(topic.partitions).toHaveLength(1)
      expect(topic.partitions[0]).toMatchObject({
        partition: 0,
        errorCode: -1,
        errorMessage: 'Partition leader unavailable',
      })
      expect(topic.partitions[0].error.retriable).toBe(true)
    })
  })

  describe('describeTransactions', () => {
    test('rejects a coordinator answer without transactions', async () => {
      const { admin, broker } = fixture()
      broker.describeTransactions = jest.fn(async () => ({}))

      const error = await rejection(admin.describeTransactions({ transactionalIds: ['tx-a'] }))

      expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
      expect(error.message).toContain('Missing transaction results')
    })

    test('a deadline that ends a stalled coordinator lookup rejects with a timeout', async () => {
      jest.useFakeTimers()
      try {
        const { admin } = fixture({ findGroupCoordinator: jest.fn(() => new Promise(() => {})) })
        const result = rejection(
          admin.describeTransactions({ transactionalIds: ['tx-a'], apiTimeout: 10 })
        )

        await jest.advanceTimersByTimeAsync(10)
        const error = await result

        expect(error).toBeInstanceOf(KafkaJSTimeout)
        expect(error.message).toContain('describeTransactions')
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        jest.useRealTimers()
      }
    })
  })

  describe('listTransactions', () => {
    test('sends the given filters with producer ids normalized to decimal strings', async () => {
      const { admin, broker } = fixture()
      broker.listTransactions = jest.fn(async () => ({
        transactions: [],
        unknownStateFilters: [],
      }))

      await admin.listTransactions({
        states: ['Ongoing', ''],
        producerIds: ['9007199254740993', 5],
      })

      expect(broker.listTransactions).toHaveBeenCalledWith({
        states: ['Ongoing', ''],
        producerIds: ['9007199254740993', '5'],
        durationFilter: '-1',
        transactionalIdPattern: null,
      })
    })

    test('a retriable failure without a protocol code is retried', async () => {
      const { admin, broker } = fixture()
      broker.listTransactions = jest
        .fn()
        .mockRejectedValueOnce(new KafkaJSConnectionError('flap'))
        .mockResolvedValueOnce({ transactions: [], unknownStateFilters: ['Future'] })

      const result = await admin.listTransactions()

      expect(broker.listTransactions).toHaveBeenCalledTimes(2)
      expect(result).toEqual({ transactions: [], unknownStateFilters: ['Future'] })
    })
  })
})
