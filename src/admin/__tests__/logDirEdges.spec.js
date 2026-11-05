const createAdmin = require('..')
const ConfigResourceTypes = require('../../protocol/configResourceTypes')
const { KafkaJSInvariantViolation } = require('../../errors')

const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

/** An admin over one broker, `nodeId` 1, whose methods are the given mocks. */
const adminOver = (methods) => {
  const broker = { nodeId: 1, ...methods }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findBroker: jest.fn(async () => broker),
    findConnectedBroker: jest.fn(async () => broker),
  }
  return { broker, cluster, admin: createAdmin({ cluster, logger, retry }) }
}

const REPLICA = { topic: 'orders', partition: 0, brokerId: 1 }

describe('log directory edges', () => {
  test('describeLogDirs rejects an answer that lists one directory twice', async () => {
    const dir = { errorCode: 0, path: '/data/a', topics: [] }
    const { admin } = adminOver({
      describeLogDirs: jest.fn(async () => ({ errorCode: 0, logDirs: [dir, dir] })),
    })

    const error = await rejection(admin.describeLogDirs({ brokers: [1] }))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid directory identity')
  })

  test('alterReplicaLogDirs rejects an answer about a replica that was not requested', async () => {
    const { admin } = adminOver({
      alterReplicaLogDirs: jest.fn(async () => ({
        topics: [{ topic: 'foreign', partitions: [{ partition: 9, errorCode: 0 }] }],
      })),
    })

    const error = await rejection(
      admin.alterReplicaLogDirs({ replicas: [{ ...REPLICA, logDir: '/data/b' }] })
    )

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid item result identity')
  })

  test('describeReplicaLogDirs reports a broker-level refusal on every replica it asked about', async () => {
    const { admin } = adminOver({
      describeLogDirs: jest.fn(async () => ({ errorCode: 31, logDirs: [] })),
    })

    const asked = [REPLICA, { topic: 'payments', partition: 3, brokerId: 2 }]

    const results = await admin.describeReplicaLogDirs({ replicas: asked })

    expect(results).toHaveLength(2)
    expect(
      results.map((result) => ({
        topic: result.topic,
        partition: result.partition,
        brokerId: result.brokerId,
        currentLogDir: result.currentLogDir,
        futureLogDir: result.futureLogDir,
        errorCode: result.errorCode,
        errorType: result.error.type,
        errorMessage: result.errorMessage,
      }))
    ).toEqual(
      asked.map((replica) => ({
        ...replica,
        currentLogDir: null,
        futureLogDir: null,
        errorCode: 31,
        errorType: 'CLUSTER_AUTHORIZATION_FAILED',
        errorMessage: null,
      }))
    )
  })
})

describe('configuration resource edges', () => {
  test('a retriable failure to reach a broker is retried', async () => {
    const { admin, cluster, broker } = adminOver({
      listConfigResources: jest.fn(async () => ({
        configResources: [
          { resourceType: ConfigResourceTypes.CLIENT_METRICS, resourceName: 'subscription' },
        ],
      })),
    })
    cluster.findConnectedBroker
      .mockRejectedValueOnce(Object.assign(new Error('flap'), { retriable: true }))
      .mockResolvedValue(broker)

    const result = await admin.listConfigResources()

    expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(2)
    expect(result).toEqual({
      resources: [{ type: ConfigResourceTypes.CLIENT_METRICS, name: 'subscription' }],
    })
  })
})
