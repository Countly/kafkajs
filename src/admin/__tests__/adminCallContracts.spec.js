const createAdmin = require('..')
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('../../../index')
const { KafkaJSInvalidConfigurationError, KafkaJSNonRetriableError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const logger = { namespace: () => logger, debug() {}, info() {}, warn: jest.fn(), error: jest.fn() }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const ACL = {
  resourceType: AclResourceTypes.TOPIC,
  resourceName: 'orders',
  resourcePatternType: ResourcePatternTypes.LITERAL,
  principal: 'User:alice',
  host: '*',
  operation: AclOperationTypes.READ,
  permissionType: AclPermissionTypes.ALLOW,
}

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

/** A cluster whose every seam records a call, so a test can prove none was made. */
const idleCluster = () => ({
  addTargetTopic: jest.fn(async () => {}),
  refreshMetadata: jest.fn(async () => {}),
  refreshMetadataIfNecessary: jest.fn(async () => {}),
  findControllerBroker: jest.fn(),
  findTopicPartitionMetadata: jest.fn(() => [{ partitionId: 0 }]),
  fetchTopicsOffset: jest.fn(),
  metadata: jest.fn(),
})
const callsOf = (cluster) => Object.values(cluster).flatMap((seam) => seam.mock.calls)

afterEach(() => jest.clearAllMocks())

describe('admin argument validation', () => {
  test.each([
    [
      'createPartitions',
      [{ topicPartitions: [] }],
      KafkaJSNonRetriableError,
      'Empty topic partitions array',
    ],
    ['deleteGroups', [null], KafkaJSNonRetriableError, 'Invalid groupIds array null'],
    ['deleteGroups', [[1]], KafkaJSNonRetriableError, 'Invalid groupId name'],
    ['createAcls', [{ acl: [] }], KafkaJSNonRetriableError, 'Empty ACL array'],
    ['deleteAcls', [{ filters: [] }], KafkaJSNonRetriableError, 'Empty ACL Filter array'],
    [
      'listPartitionReassignments',
      [{ topics: 'orders' }],
      KafkaJSNonRetriableError,
      'Invalid topics array orders',
    ],
    [
      'fetchTopicOffsetsByTimestamp',
      [undefined, 'maxTimestamp'],
      KafkaJSNonRetriableError,
      'Invalid topic undefined',
    ],
    [
      'fetchTopicOffsetsByTimestamp',
      ['orders', 'maxTimestamp', null],
      KafkaJSInvalidConfigurationError,
      'isolationLevel options must be an object',
    ],
    [
      'deleteTopics',
      [],
      KafkaJSInvalidConfigurationError,
      'Exactly one of topics or topicIds is required',
    ],
  ])(
    '%s rejects invalid case %# before touching the cluster',
    async (method, args, ErrorClass, message) => {
      const cluster = idleCluster()
      const admin = createAdmin({ logger, cluster, retry })

      const error = await rejection(admin[method](...args))

      expect(error).toBeInstanceOf(ErrorClass)
      expect(error.message).toContain(message)
      expect(callsOf(cluster)).toEqual([])
    }
  )
})

describe('admin controller moves', () => {
  test.each([
    ['createAcls', { acl: [ACL] }, 'Could not create ACL', undefined, true],
    ['describeAcls', ACL, 'Could not describe ACL', { resources: [] }, { resources: [] }],
    [
      'deleteAcls',
      { filters: [ACL] },
      'Could not delete ACL',
      { filterResponses: [] },
      { filterResponses: [] },
    ],
  ])(
    '%s logs a NOT_CONTROLLER answer and retries against a refreshed controller',
    async (method, options, warning, answer, expected) => {
      const moved = createErrorFromCode(41)
      const broker = {
        [method]: jest.fn().mockRejectedValueOnce(moved).mockResolvedValueOnce(answer),
      }
      const cluster = idleCluster()
      cluster.findControllerBroker.mockResolvedValue(broker)
      const admin = createAdmin({ logger, cluster, retry })

      const result = await admin[method](options)

      expect(result).toEqual(expected)
      expect(broker[method]).toHaveBeenCalledTimes(2)
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
      expect(logger.warn).toHaveBeenCalledTimes(1)
      expect(logger.warn).toHaveBeenCalledWith(warning, {
        error: moved.message,
        retryCount: 0,
        retryTime: expect.any(Number),
      })
    }
  )
})

describe('admin named offset specifications', () => {
  test('a stale partition answer refreshes metadata and asks again', async () => {
    const cluster = idleCluster()
    cluster.fetchTopicsOffset
      .mockRejectedValueOnce(createErrorFromCode(6))
      .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: '7' }] }])
    const admin = createAdmin({ logger, cluster, retry })

    const offsets = await admin.fetchTopicOffsetsByTimestamp('orders', 'maxTimestamp')

    expect(offsets).toEqual([{ partition: 0, offset: '7' }])
    expect(cluster.fetchTopicsOffset).toHaveBeenCalledTimes(2)
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })
})

describe('admin event listeners', () => {
  test('a listener that rejects is logged with its event name and stack', async () => {
    const failure = new Error('listener failed')
    const cluster = { connect: jest.fn(async () => {}), disconnect: jest.fn(async () => {}) }
    const admin = createAdmin({ logger, cluster, retry })
    admin.on(admin.events.CONNECT, async () => {
      throw failure
    })

    await admin.connect()
    await new Promise((resolve) => setImmediate(resolve))
    await admin.disconnect()

    expect(logger.error).toHaveBeenCalledWith('Failed to execute listener: listener failed', {
      eventName: admin.events.CONNECT,
      stack: failure.stack,
    })
  })
})
