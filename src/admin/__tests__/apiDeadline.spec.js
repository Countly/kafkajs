const createAdmin = require('..')
const { KafkaJSTimeout, KafkaJSError, KafkaJSNumberOfRetriesExceeded } = require('../../errors')

const held = () => {
  let resolve
  let reject
  const promise = new Promise((yes, no) => {
    resolve = yes
    reject = no
  })
  return { promise, resolve, reject }
}
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const observe = (promise) => {
  const result = { settled: false }
  promise.then(
    (value) => Object.assign(result, { settled: true, value }),
    (error) => Object.assign(result, { settled: true, error })
  )
  return result
}
const flush = async () => {
  for (let n = 0; n < 30; n++) await Promise.resolve()
}

describe('admin invocation deadlines', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it.each([null, undefined, false, 0, ''])('preserves rejected value %p', async (reason) => {
    const { runWithDeadline } = require('../deadline')
    const result = await runWithDeadline('failure', 100, () => Promise.reject(reason)).then(
      (value) => ({ status: 'fulfilled', value }),
      (error) => ({ status: 'rejected', error })
    )
    expect(result).toEqual({ status: 'rejected', error: reason })
  })

  it.each([null, undefined, false, 0, ''])('preserves polling rejection %p', async (reason) => {
    const { runWithDeadline, waitForOf } = require('../deadline')
    const result = observe(
      runWithDeadline('poll', 100, () => waitForOf(() => Promise.reject(reason), { delay: 1 }))
    )
    await jest.advanceTimersByTimeAsync(1)
    expect(result.settled).toBe(true)
    expect(Object.prototype.hasOwnProperty.call(result, 'error')).toBe(true)
    expect(result.error).toBe(reason)
  })

  it('preserves describeCluster rejection without apiTimeout', async () => {
    const admin = createAdmin({
      logger,
      cluster: {
        metadata: jest.fn().mockRejectedValue(null),
      },
    })
    await expect(admin.describeCluster()).rejects.toBeNull()
  })

  it('factory deadline ends a stalled lookup', async () => {
    const response = held()
    const admin = createAdmin({
      logger,
      cluster: { metadata: () => response.promise },
      retry: { retries: 0 },
    })
    const result = observe(admin.listTopics({ apiTimeout: 10 }))
    await jest.advanceTimersByTimeAsync(9)
    expect(result.settled).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    expect(result.settled).toBe(true)
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(result.error.message).toContain('listTopics')
    expect(result.error.message).toContain('10 ms')
    expect(result.error.message).toContain('outcome is unknown')
    response.resolve({ topicMetadata: [{ topic: 'late' }] })
    await flush()
    expect(result.value).toBeUndefined()
  })

  it('clears the timer on early success and ordinary failure', async () => {
    const admin = createAdmin({
      logger,
      cluster: { metadata: async () => ({ topicMetadata: [] }) },
    })
    await expect(admin.listTopics()).resolves.toEqual([])
    expect(jest.getTimerCount()).toBe(0)
    const error = new Error('original')
    const failed = createAdmin({
      logger,
      cluster: {
        metadata: async () => {
          throw error
        },
      },
    })
    await expect(failed.listTopics()).rejects.toBe(error)
  })

  it.each([0, -1, 0.5, NaN, Infinity, null, '10', true, {}, 2147483648])(
    'refuses invalid per-call budget %p before lookup',
    async (value) => {
      const metadata = jest.fn()
      const admin = createAdmin({ logger, cluster: { metadata } })
      await expect(admin.listTopics({ apiTimeout: value })).rejects.toMatchObject({
        name: 'KafkaJSInvalidConfigurationError',
      })
      expect(metadata).not.toHaveBeenCalled()
    }
  )

  it('inherits the original deadline and rejects a withheld-timer continuation', async () => {
    const { runWithDeadline, checked } = require('../deadline')
    const gate = held()
    const now = jest.spyOn(performance, 'now').mockReturnValue(0)
    let published = false
    const result = observe(
      runWithDeadline('outer', 10, async () => {
        await runWithDeadline('inner', 500, async () => checked(await gate.promise))
        checked(null)
        published = true
      })
    )
    await flush()
    now.mockReturnValue(11)
    gate.resolve()
    await flush()
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(result.error.message).toContain('outer')
    expect(published).toBe(false)
  })

  it('keeps retry exhaustion counts and clears an expired backoff', async () => {
    const { runWithDeadline, retrierOf } = require('../deadline')
    const error = new KafkaJSError('retry', { retriable: true })
    const attempted = jest.fn(async () => {
      throw error
    })
    const retry = { retries: 2, initialRetryTime: 1, maxRetryTime: 1, factor: 0, multiplier: 1 }
    const result = observe(runWithDeadline('attempts', 100, () => retrierOf(retry)(attempted)))
    await jest.advanceTimersByTimeAsync(5)
    expect(attempted).toHaveBeenCalledTimes(3)
    expect(result.error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    const long = observe(
      runWithDeadline('backoff', 5, () =>
        retrierOf({ ...retry, initialRetryTime: 50, maxRetryTime: 50 })(attempted)
      )
    )
    await jest.advanceTimersByTimeAsync(5)
    expect(long.error).toBeInstanceOf(KafkaJSTimeout)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('keeps another metadata caller alive and stops discovery after its last waiter leaves', async () => {
    const { shareMetadata, runWithDeadline, retrierOf } = require('../deadline')
    const error = new KafkaJSError('metadata pending', { retriable: true })
    const attempt = jest.fn(async () => {
      throw error
    })
    const shared = shareMetadata(() =>
      retrierOf({ retries: 100, initialRetryTime: 10, maxRetryTime: 10, factor: 0, multiplier: 1 })(
        attempt
      )
    )
    const short = observe(runWithDeadline('short', 5, shared))
    const long = observe(runWithDeadline('long', 25, shared))
    await flush()
    expect(attempt).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(5)
    expect(short.error).toBeInstanceOf(KafkaJSTimeout)
    expect(long.settled).toBe(false)
    await jest.advanceTimersByTimeAsync(15)
    expect(attempt).toHaveBeenCalledTimes(3)
    await jest.advanceTimersByTimeAsync(5)
    expect(long.error).toBeInstanceOf(KafkaJSTimeout)
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(100)
    expect(attempt).toHaveBeenCalledTimes(3)
  })

  it('does not abandon an ordinary metadata waiter when the timed waiter leaves', async () => {
    const { shareMetadata, runWithDeadline } = require('../deadline')
    const response = held()
    const work = jest.fn(() => response.promise)
    const shared = shareMetadata(work)
    const short = observe(runWithDeadline('short', 5, shared))
    const ordinary = observe(shared())
    await jest.advanceTimersByTimeAsync(5)
    expect(short.error).toBeInstanceOf(KafkaJSTimeout)
    expect(ordinary.settled).toBe(false)
    response.resolve('metadata')
    await flush()
    expect(ordinary.value).toBe('metadata')
    expect(work).toHaveBeenCalledTimes(1)
  })

  it('does not start a lazy operation after expiry', async () => {
    const { runWithDeadline, currentDeadline } = require('../deadline')
    const response = held()
    const start = jest.fn()
    const result = observe(
      runWithDeadline('lazy', 5, async () => {
        const captured = currentDeadline()
        await response.promise
        await captured.wait(start)
      })
    )
    await jest.advanceTimersByTimeAsync(5)
    response.resolve()
    await flush()
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(start).not.toHaveBeenCalled()
  })

  it('retains item outcomes and retries only the family-selected unresolved subset', async () => {
    const { itemRetry } = require('../itemRetry')
    const items = ['ok', 'refused', 'busy'].map((id) => ({ id }))
    const send = jest
      .fn()
      .mockResolvedValueOnce([
        { id: 'busy', errorCode: 14, errorMessage: 'loading' },
        { id: 'refused', errorCode: 30, errorMessage: 'denied' },
        { id: 'ok', errorCode: 0, errorMessage: null },
      ])
      .mockResolvedValueOnce([{ id: 'busy', errorCode: 0 }])
    const result = observe(
      itemRetry(
        {
          items,
          keyOf: (row) => row.id,
          retry: { retries: 1, initialRetryTime: 1, factor: 0 },
          retryable: (row) => row.errorCode === 14,
          callFailure: () => false,
          sanitize: (value) => value,
        },
        send
      )
    )
    await jest.advanceTimersByTimeAsync(5)
    expect(send.mock.calls[1][0]).toEqual([{ id: 'busy' }])
    expect(result.value.map((row) => [row.id, row.errorCode])).toEqual([
      ['ok', 0],
      ['refused', 30],
      ['busy', 0],
    ])
  })
  it('broker selection starts no second connection after its caller expires', async () => {
    const BrokerPool = require('../../cluster/brokerPool')
    const { runWithDeadline } = require('../deadline')
    const first = held()
    const pool = {
      brokers: { 1: { isConnected: () => false }, 2: { isConnected: () => false } },
      findBroker: jest
        .fn()
        .mockImplementationOnce(() => first.promise)
        .mockResolvedValue({}),
      shouldRebootstrapOnAllBrokersUnavailable: () => false,
      connect: jest.fn(),
    }
    const result = observe(
      runWithDeadline('selection', 5, () => BrokerPool.prototype.findConnectedBroker.call(pool))
    )
    await jest.advanceTimersByTimeAsync(5)
    first.reject(new Error('first broker failed'))
    await flush()
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(pool.findBroker).toHaveBeenCalledTimes(1)
    expect(pool.connect).not.toHaveBeenCalled()
  })

  it('the real metadata pool coalesces callers and stops retrying after its final waiter', async () => {
    const BrokerPool = require('../../cluster/brokerPool')
    const { runWithDeadline } = require('../deadline')
    const pool = new BrokerPool({
      logger,
      retry: { retries: 100, initialRetryTime: 10, maxRetryTime: 10, multiplier: 1, factor: 0 },
    })
    const broker = {
      connectionPool: { host: 'broker', port: 9092 },
      metadata: jest.fn(async () => {
        const error = new KafkaJSError('pending', { retriable: true })
        error.type = 'LEADER_NOT_AVAILABLE'
        throw error
      }),
    }
    pool.seedBroker = broker
    pool.findConnectedBroker = jest.fn(async () => broker)
    const short = observe(runWithDeadline('first', 5, () => pool.refreshMetadata(['t'])))
    const long = observe(runWithDeadline('second', 25, () => pool.refreshMetadata(['t'])))
    await jest.advanceTimersByTimeAsync(5)
    expect(short.error).toBeInstanceOf(KafkaJSTimeout)
    expect(long.settled).toBe(false)
    await jest.advanceTimersByTimeAsync(20)
    expect(long.error).toBeInstanceOf(KafkaJSTimeout)
    expect(broker.metadata).toHaveBeenCalledTimes(3)
    expect(jest.getTimerCount()).toBe(0)
    await jest.advanceTimersByTimeAsync(100)
    expect(broker.metadata).toHaveBeenCalledTimes(3)
  })

  it.each([false, true])(
    'removes an unsent throttled request with enforcement=%p',
    async (enforceRequestTimeout) => {
      const RequestQueue = require('../../network/requestQueue')
      const { runWithDeadline, currentDeadline } = require('../deadline')
      const writes = jest.fn()
      const queue = new RequestQueue({
        logger,
        requestTimeout: 500,
        enforceRequestTimeout,
        isConnected: () => true,
      })
      queue.maybeThrottle(100)
      const result = observe(
        runWithDeadline(
          'queued',
          5,
          () =>
            new Promise((resolve, reject) =>
              queue.push({
                entry: {
                  correlationId: 1,
                  apiName: 'Metadata',
                  apiKey: 3,
                  apiVersion: 7,
                  resolve,
                  reject,
                },
                expectResponse: true,
                sendRequest: writes,
                guard: currentDeadline().guard,
              })
            )
        )
      )
      await jest.advanceTimersByTimeAsync(5)
      expect(result.error).toBeInstanceOf(KafkaJSTimeout)
      queue.destroy()
      expect(queue.pending).toHaveLength(0)
      expect(queue.inflight.size).toBe(0)
      expect(writes).not.toHaveBeenCalled()
      queue.destroy()
    }
  )

  const resource = {
    type: 2,
    name: 't',
    configEntries: [{ name: 'cleanup.policy', value: 'delete', operation: 0 }],
  }
  const acl = {
    resourceType: 2,
    resourceName: 't',
    resourcePatternType: 3,
    principal: 'User:x',
    host: '*',
    operation: 3,
    permissionType: 3,
  }
  const callSites = [
    ['createTopics', [{ topics: [{ topic: 't' }], waitForLeaders: false }], 'createTopics'],
    ['createPartitions', [{ topicPartitions: [{ topic: 't', count: 2 }] }], 'createPartitions'],
    ['deleteTopics', [{ topics: ['t'] }], 'deleteTopics'],
    ['fetchTopicOffsets', ['t'], 'fetchTopicsOffset'],
    ['fetchTopicOffsetsByTimestamp', ['t', 1], 'fetchTopicsOffset'],
    ['describeConfigs', [{ resources: [resource], includeSynonyms: false }], 'describeConfigs'],
    ['alterConfigs', [{ resources: [resource], validateOnly: false }], 'alterConfigs'],
    ['incrementalAlterConfigs', [{ resources: [resource] }], 'incrementalAlterConfigs'],
    [
      'fetchTopicMetadata',
      [{ topics: ['t'], useDescribeTopicPartitions: true }],
      'describeTopicPartitions',
    ],
    ['listGroups', [{}], 'listGroups'],
    ['deleteGroups', [['g']], 'deleteGroups'],
    [
      'removeMembersFromConsumerGroup',
      [{ groupId: 'g', members: [{ groupInstanceId: 'i' }] }],
      'leaveGroup',
    ],
    [
      'deleteTopicRecords',
      [{ topic: 't', partitions: [{ partition: 0, offset: '0' }] }],
      'deleteRecords',
    ],
    ['createAcls', [{ acl: [acl] }], 'createAcls'],
    ['describeAcls', [acl], 'describeAcls'],
    ['deleteAcls', [{ filters: [acl] }], 'deleteAcls'],
    [
      'alterPartitionReassignments',
      [{ topics: [{ topic: 't', partitionAssignment: [{ partition: 0, replicas: [0] }] }] }],
      'alterPartitionReassignments',
    ],
    ['listPartitionReassignments', [{}], 'listPartitionReassignments'],
    ['describeGroups', [['g']], 'consumerGroupDescribe'],
    ['describeGroups', [['g']], 'describeGroups'],
  ]
  it.each(
    callSites.flatMap(([method, args, stalled]) =>
      [false, true].map((enforceRequestTimeout) => ({
        method,
        args,
        stalled,
        enforceRequestTimeout,
      }))
    )
  )(
    'bounds $method at the $stalled answer with enforcement=$enforceRequestTimeout',
    async ({ method, args, stalled, enforceRequestTimeout }) => {
      const response = held()
      const sent = jest.fn(() => response.promise)
      const broker = new Proxy(
        { nodeId: 0 },
        {
          get(target, key) {
            if (key === 'then') return undefined
            if (key === stalled) return sent
            if (key === 'consumerGroupDescribe')
              return async () => ({ groups: [{ groupId: 'g', errorCode: 69 }] })
            return target[key]
          },
        }
      )
      const cluster = {
        enforceRequestTimeout,
        brokerPool: { brokers: { 0: broker } },
        allowAutoTopicCreation: false,
        refreshMetadata: async () => {},
        refreshMetadataIfNecessary: async () => {},
        addTargetTopic: async () => {},
        findControllerBroker: async () => broker,
        findConnectedBroker: async () => broker,
        findBroker: async () => broker,
        findGroupCoordinator: async () => broker,
        getNodeIds: () => ['0'],
        findTopicPartitionMetadata: () => [{ partitionId: 0, leader: 0 }],
        findLeaderForPartitions: () => ({ 0: [0] }),
        fetchTopicsOffset:
          stalled === 'fetchTopicsOffset'
            ? sent
            : async () => [{ topic: 't', partitions: [{ partition: 0, offset: '1' }] }],
      }
      const admin = createAdmin({ cluster, logger, apiTimeout: 10, retry: { retries: 0 } })
      const result = observe(admin[method](...args))
      await jest.advanceTimersByTimeAsync(0)
      expect(sent).toHaveBeenCalledTimes(1)
      await jest.advanceTimersByTimeAsync(10)
      expect(result.error).toBeInstanceOf(KafkaJSTimeout)
      response.reject(new Error('late answer failed'))
      await jest.advanceTimersByTimeAsync(0)
      expect(sent).toHaveBeenCalledTimes(1)
    }
  )
  it('keeps the default at sixty seconds and accepts the maximum integer budget', async () => {
    const { validateApiTimeout } = require('../deadline')
    expect(validateApiTimeout(2147483647)).toBe(2147483647)
    expect(validateApiTimeout(1)).toBe(1)
    const response = held()
    const admin = createAdmin({ logger, cluster: { metadata: () => response.promise } })
    const result = observe(admin.listTopics())
    await jest.advanceTimersByTimeAsync(59999)
    expect(result.settled).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    response.resolve({ topicMetadata: [] })
    await flush()
  })

  it('keeps broker timeout separate and permits a fresh mutation after an unknown outcome', async () => {
    const response = held()
    const createTopics = jest
      .fn()
      .mockImplementationOnce(() => response.promise)
      .mockResolvedValue(undefined)
    const cluster = {
      refreshMetadata: async () => {},
      findControllerBroker: async () => ({ createTopics }),
    }
    const admin = createAdmin({ cluster, logger, retry: { retries: 0 } })
    const request = {
      topics: [{ topic: 't' }],
      waitForLeaders: false,
      timeout: 30000,
      apiTimeout: 5,
    }
    const first = observe(admin.createTopics(request))
    await jest.advanceTimersByTimeAsync(5)
    expect(first.error).toBeInstanceOf(KafkaJSTimeout)
    expect(createTopics.mock.calls[0][0].timeout).toBe(30000)
    response.resolve()
    await flush()
    await expect(admin.createTopics(request)).resolves.toBe(true)
    expect(createTopics).toHaveBeenCalledTimes(2)
  })

  it('retains the last typed broker item error on exhaustion and the original late transport error', async () => {
    const { itemRetry } = require('../itemRetry')
    const options = {
      items: [{ id: 'a' }, { id: 'b' }],
      keyOf: (row) => row.id,
      retry: { retries: 0 },
      retryable: (row) => row.errorCode === 14,
      callFailure: () => false,
      sanitize: (value) => value,
    }
    const rows = [
      { id: 'a', errorCode: 0 },
      { id: 'b', errorCode: 14, errorMessage: 'loading' },
    ]
    const answer = await itemRetry(options, async () => rows)
    expect(answer.map((row) => row.errorCode)).toEqual([0, 14])
    expect(answer[1].error.code).toBe(14)
    const transport = new KafkaJSError('transport failed', { retriable: true })
    const send = jest.fn().mockResolvedValueOnce(rows).mockRejectedValueOnce(transport)
    const result = observe(
      itemRetry({ ...options, retry: { retries: 1, initialRetryTime: 1, factor: 0 } }, send)
    )
    await jest.advanceTimersByTimeAsync(5)
    expect(result.value[0].errorCode).toBe(0)
    expect(result.value[1].errorCode).toBe(-1)
    expect(result.value[1].error).toBe(transport)
  })
})

describe('recovery and cleanup enrollment', () => {
  const { runWithDeadline, currentDeadline, withConsumerOperation } = require('../deadline')
  const BrokerPool = require('../../cluster/brokerPool')
  const Queue = require('../../network/requestQueue')
  const { ConsumerOperation } = require('../../consumer/groupProtocol/interface')

  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.clearAllTimers()
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  const recovery = () => {
    const gate = held()
    const old = { id: 'old' }
    const replacement = { id: 'replacement' }
    const unused = { id: 'unused' }
    const pool = new BrokerPool({ logger })
    pool.seedBroker = old
    pool.buildConnectionPool = jest.fn(() => gate.promise)
    pool.createBroker = jest.fn(() => replacement)
    pool.closePool = jest.fn()
    pool.closeObsolete = jest.fn()
    pool.brokers = { 0: { isConnected: () => false } }
    pool.findBroker = jest.fn(async () => {
      throw new Error('unavailable')
    })
    pool.shouldRebootstrapOnAllBrokersUnavailable = () => true
    pool.connect = jest.fn(async () => {})
    return { pool, gate, old, replacement, unused }
  }

  it.each([false, true])(
    'keeps a peer across shared recovery with withheld clock delivery=%p',
    async (withheld) => {
      const { pool, gate, replacement, unused } = recovery()
      const now = withheld ? jest.spyOn(performance, 'now').mockReturnValue(0) : null
      const short = observe(runWithDeadline('short', 5, () => pool.findConnectedBroker()))
      const peer = observe(runWithDeadline('peer', 25, () => pool.findConnectedBroker()))
      await flush()
      if (withheld) now.mockReturnValue(6)
      else await jest.advanceTimersByTimeAsync(5)
      gate.resolve(unused)
      await flush()
      expect(short.error).toBeInstanceOf(KafkaJSTimeout)
      expect(peer.error).toBeUndefined()
      expect(peer.settled).toBe(true)
      expect(peer.value).toBe(replacement)
      expect(pool.seedBroker).toBe(replacement)
      expect(pool.buildConnectionPool).toHaveBeenCalledTimes(1)
      expect(pool.closePool).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  it('enrolls recovery from metadata failure beside broker selection', async () => {
    const { pool, gate, replacement, unused } = recovery()
    const short = observe(runWithDeadline('short', 5, () => pool.findConnectedBroker()))
    await flush()
    const peer = observe(
      runWithDeadline('refresh', 25, () => pool.onMetadataFailure({ type: 'REBOOTSTRAP_REQUIRED' }))
    )
    await flush()
    await jest.advanceTimersByTimeAsync(5)
    gate.resolve(unused)
    await flush()
    expect(short.error).toBeInstanceOf(KafkaJSTimeout)
    expect(peer.error).toBeUndefined()
    expect(peer.settled).toBe(true)
    expect(pool.seedBroker).toBe(replacement)
    expect(pool.buildConnectionPool).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  it.each([false, true])(
    'closes only the unused build after the last recovery caller leaves with withheld clock delivery=%p',
    async (withheld) => {
      const { pool, gate, old, unused } = recovery()
      const now = withheld ? jest.spyOn(performance, 'now').mockReturnValue(0) : null
      const result = observe(runWithDeadline('short', 5, () => pool.rebootstrap('timeout')))
      await flush()
      if (withheld) now.mockReturnValue(6)
      else await jest.advanceTimersByTimeAsync(5)
      gate.resolve(unused)
      await flush()
      expect(result.error).toBeInstanceOf(KafkaJSTimeout)
      expect(pool.seedBroker).toBe(old)
      expect(pool.closePool).toHaveBeenCalledWith(unused)
      expect(pool.closeObsolete).not.toHaveBeenCalledWith(old)
      expect(pool.createBroker).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  it('does not publish setup inventory from an abandoned recovery build', async () => {
    const { pool, gate, unused } = recovery()
    const { ConnectionSetupPolicy } = require('../../network/connectionSetup')
    unused.setupPolicy = ConnectionSetupPolicy.resolve({})
    pool.connectionPoolBuilder = { build: () => gate.promise, bootstrapEndpoints: ['seed:9092'] }
    pool.buildConnectionPool = BrokerPool.prototype.buildConnectionPool.bind(pool)
    const result = observe(runWithDeadline('short', 5, () => pool.rebootstrap('timeout')))
    await jest.advanceTimersByTimeAsync(5)
    gate.resolve(unused)
    await flush()
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(pool.setupStates).toBeNull()
    expect(pool.closePool).toHaveBeenCalledWith(unused)
    expect(pool.createBroker).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  it('keeps the producer fence through a recovery build', async () => {
    const { pool, gate, old, unused } = recovery()
    const Generation = require('../../producer/generation')
    const fence = new Generation()
    const reason = new Error('producer closed')
    const result = observe(pool.findConnectedBroker({ fence }))
    await flush()
    fence.close(reason)
    gate.resolve(unused)
    await flush()
    expect(result.error).toBe(reason)
    expect(pool.seedBroker).toBe(old)
    expect(pool.closePool).toHaveBeenCalledWith(unused)
    expect(pool.createBroker).not.toHaveBeenCalled()
    expect(pool.connect).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  it('starts a successful fresh recovery while an abandoned build is held', async () => {
    const { pool, gate, old, unused, replacement } = recovery()
    const fresh = held()
    pool.buildConnectionPool
      .mockImplementationOnce(() => gate.promise)
      .mockImplementationOnce(() => fresh.promise)
    const short = observe(runWithDeadline('short', 5, () => pool.rebootstrap('timeout')))
    await jest.advanceTimersByTimeAsync(5)
    const peer = observe(runWithDeadline('fresh', 25, () => pool.rebootstrap('timeout')))
    await flush()
    const builds = pool.buildConnectionPool.mock.calls.length
    fresh.resolve({ id: 'fresh-pool' })
    await flush()
    const published = pool.seedBroker
    gate.resolve(unused)
    await flush()
    expect(short.error).toBeInstanceOf(KafkaJSTimeout)
    expect(builds).toBe(2)
    expect(peer.error).toBeUndefined()
    expect(peer.settled).toBe(true)
    expect(published).toBe(replacement)
    expect(pool.seedBroker).toBe(replacement)
    expect(pool.closePool).toHaveBeenCalledWith(unused)
    expect(pool.closeObsolete.mock.calls.filter(([broker]) => broker === old)).toHaveLength(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('keeps a concurrently replaced seed and routing table after recovery build', async () => {
    const { pool, gate, unused } = recovery()
    const result = observe(runWithDeadline('recovery', 25, () => pool.rebootstrap('timeout')))
    await flush()
    const retained = { id: 'retained' }
    const table = { 4: retained }
    pool.seedBroker = retained
    pool.brokers = table
    gate.resolve(unused)
    await flush()
    expect(result.error).toBeDefined()
    expect(pool.seedBroker).toBe(retained)
    expect(pool.brokers).toBe(table)
    expect(pool.closePool).toHaveBeenCalledWith(unused)
    expect(pool.closeObsolete).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  it.each([false, true])(
    'preserves real consumer retirement after queue enrollment with enforcement=%p',
    async (enforceRequestTimeout) => {
      const operation = new ConsumerOperation()
      const queue = new Queue({
        logger,
        requestTimeout: 500,
        enforceRequestTimeout,
        isConnected: () => true,
      })
      queue.scheduleRequestTimeoutCheck()
      queue.maybeThrottle(100)
      const sent = jest.fn()
      const result = observe(
        withConsumerOperation(
          operation,
          () =>
            new Promise((resolve, reject) =>
              queue.push({
                entry: {
                  correlationId: 1,
                  apiName: 'DescribeGroups',
                  apiKey: 15,
                  apiVersion: 5,
                  resolve,
                  reject,
                },
                expectResponse: true,
                sendRequest: sent,
                guard: () => operation.refusal(),
              })
            )
        )
      )
      const reason = new Error('captured consumer retired')
      operation.retire(reason)
      await flush()
      queue.destroy()
      expect(result.settled).toBe(true)
      expect(result.error).toBe(reason)
      expect(sent).not.toHaveBeenCalled()
      expect(queue.pending).toHaveLength(0)
      expect(queue.inflight.size).toBe(0)
      expect(queue.throttleCheckTimeoutId).toBeNull()
      expect(operation.disposers.size).toBe(0)
      expect(operation.waiters.size).toBe(0)
      expect(operation.timer).toBeNull()
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  it.each([false, true])(
    'refuses expired queue enrollment while retaining a queued peer with enforcement=%p',
    async (enforceRequestTimeout) => {
      const queue = new Queue({
        logger,
        requestTimeout: 500,
        enforceRequestTimeout,
        isConnected: () => true,
      })
      queue.scheduleRequestTimeoutCheck()
      queue.maybeThrottle(100)
      const sent = jest.fn()
      const peerSent = jest.fn()
      const peer = observe(
        new Promise((resolve, reject) =>
          queue.push({
            entry: {
              correlationId: 2,
              apiName: 'Metadata',
              apiKey: 3,
              apiVersion: 7,
              resolve,
              reject,
            },
            expectResponse: true,
            sendRequest: peerSent,
          })
        )
      )
      const now = jest.spyOn(performance, 'now').mockReturnValue(0)
      let scope
      const result = observe(
        runWithDeadline(
          'enrollment',
          5,
          () =>
            new Promise((resolve, reject) => {
              scope = currentDeadline()
              const own = scope.own.bind(scope)
              scope.own = (cleanup) => {
                now.mockReturnValue(6)
                return own(cleanup)
              }
              queue.push({
                entry: {
                  correlationId: 1,
                  apiName: 'Metadata',
                  apiKey: 3,
                  apiVersion: 7,
                  resolve,
                  reject,
                },
                expectResponse: true,
                sendRequest: sent,
                guard: scope.guard,
              })
            })
        )
      )
      try {
        await flush()
        expect(result.error).toBeInstanceOf(KafkaJSTimeout)
        expect(queue.pending.map((request) => request.correlationId)).toEqual([2])
        expect(sent).not.toHaveBeenCalled()
        expect(scope.cleanups.size).toBe(0)
        expect(peer.settled).toBe(false)
        expect(queue.throttleCheckTimeoutId).not.toBeNull()
        await jest.advanceTimersByTimeAsync(100)
        expect(peerSent).toHaveBeenCalledTimes(1)
        queue.fulfillRequest({ correlationId: 2, payload: Buffer.alloc(0), size: 0 })
        await flush()
        expect(peer.error).toBeUndefined()
        expect(peer.settled).toBe(true)
        expect(queue.pending).toHaveLength(0)
        expect(queue.inflight.size).toBe(0)
        expect(queue.throttleCheckTimeoutId).toBeNull()
        queue.destroy()
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        queue.settleAll(new Error('fixture completed'))
        queue.destroy()
      }
    }
  )

  it('does not start lazy work when the actual clock expires during wait enrollment', async () => {
    const now = jest.spyOn(performance, 'now').mockReturnValue(0)
    const work = jest.fn(() => 'late')
    let scope
    const result = observe(
      runWithDeadline('enrollment', 5, () => {
        scope = currentDeadline()
        const own = scope.own.bind(scope)
        scope.own = (cleanup) => {
          now.mockReturnValue(6)
          return own(cleanup)
        }
        return scope.wait(work)
      })
    )
    await flush()
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(work).not.toHaveBeenCalled()
    expect(scope.cleanups.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('polling enrollment and recovery publication', () => {
  const {
    runWithDeadline,
    withConsumerOperation,
    currentDeadline,
    waitForOf,
  } = require('../deadline')
  const { ConsumerOperation } = require('../../consumer/groupProtocol/interface')
  const BrokerPool = require('../../cluster/brokerPool')
  const { ConnectionSetupPolicy } = require('../../network/connectionSetup')
  const Generation = require('../../producer/generation')
  const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
  const held = () => {
    let resolve
    const promise = new Promise((yes) => {
      resolve = yes
    })
    return { promise, resolve }
  }
  const observe = (promise) => {
    const result = { settled: false }
    promise.then(
      (value) => Object.assign(result, { settled: true, value }),
      (error) => Object.assign(result, { settled: true, error })
    )
    return result
  }
  const flush = async () => {
    for (let i = 0; i < 100; i++) await Promise.resolve()
  }
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.clearAllTimers()
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it.each([true, false])(
    'keeps shared metadata when the real consumer caller retires, consumer first=%p',
    async (consumerFirst) => {
      const gate = held()
      const operation = new ConsumerOperation()
      const pool = new BrokerPool({ logger, retry: { retries: 0 } })
      let discovery
      const broker = {
        metadata: jest.fn(() => {
          discovery = currentDeadline()
          return gate.promise
        }),
        connectionPool: { host: 'seed', port: 9092 },
      }
      pool.seedBroker = broker
      pool.findConnectedBroker = jest.fn(async () => broker)
      const consumer = () =>
        observe(withConsumerOperation(operation, () => pool.refreshMetadata(['t'])))
      const admin = () => observe(runWithDeadline('peer', 25, () => pool.refreshMetadata(['t'])))
      let a, b
      if (consumerFirst) {
        a = consumer()
        await flush()
        b = admin()
      } else {
        b = admin()
        await flush()
        a = consumer()
      }
      await flush()
      expect(broker.metadata).toHaveBeenCalledTimes(1)
      expect(discovery.callers.size).toBe(2)
      const reason = new Error('consumer retired')
      operation.retire(reason)
      await flush()
      expect(a.error).toBe(reason)
      expect(b.settled).toBe(false)
      expect(discovery.callers.size).toBe(1)
      const response = { brokers: [], topicMetadata: [], controllerId: 0 }
      gate.resolve(response)
      await flush()
      expect(b.error).toBeUndefined()
      expect(b.settled).toBe(true)
      expect(pool.metadata).toBe(response)
      expect(discovery.callers.size).toBe(0)
      expect(discovery.cleanups.size).toBe(0)
      expect(operation.disposers.size).toBe(0)
      expect(operation.waiters.size).toBe(0)
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  it.each(['fence', 'routing'])(
    'does not publish setup inventory after the recovery %s is obsolete',
    async (mode) => {
      const gate = held()
      const pool = new BrokerPool({
        logger,
        connectionPoolBuilder: { build: () => gate.promise, bootstrapEndpoints: ['seed:9092'] },
      })
      const old = { id: 'old' }
      pool.seedBroker = old
      pool.closePool = jest.fn()
      pool.closeObsolete = jest.fn()
      pool.createBroker = jest.fn(() => ({ id: 'new' }))
      const fence = new Generation()
      const reason = new Error('closed')
      const result = observe(pool.rebootstrap('timeout', { fence }))
      await flush()
      const retained = { id: 'replacement' }
      const table = mode === 'fence' ? pool.brokers : {}
      if (mode === 'fence') fence.close(reason)
      else {
        pool.seedBroker = retained
        pool.brokers = table
      }
      const unused = { setupPolicy: ConnectionSetupPolicy.resolve({}) }
      gate.resolve(unused)
      await flush()
      expect(result.settled).toBe(true)
      expect(result.error).toBeDefined()
      if (mode === 'fence') expect(result.error).toBe(reason)
      expect(pool.closePool).toHaveBeenCalledWith(unused)
      expect(pool.closePool).toHaveBeenCalledTimes(1)
      expect(pool.closeObsolete).not.toHaveBeenCalled()
      expect(pool.seedBroker).toBe(mode === 'fence' ? old : retained)
      expect(pool.brokers).toBe(table)
      expect(pool.setupStates).toBeNull()
      expect(unused.setupStates).toBeUndefined()
      expect(pool.createBroker).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  it('does not arm polling timers when the deadline expires during polling enrollment', async () => {
    const now = jest.spyOn(performance, 'now').mockReturnValue(0)
    const predicate = jest.fn(() => true)
    let scope
    const result = observe(
      runWithDeadline('poll', 5, () => {
        scope = currentDeadline()
        const own = scope.own.bind(scope)
        let registrations = 0
        scope.own = (cleanup) => {
          if (++registrations === 2) now.mockReturnValue(6)
          return own(cleanup)
        }
        return waitForOf(predicate)
      })
    )
    await flush()
    expect(result.settled).toBe(true)
    expect(result.error).toBe(scope.timeout())
    expect(predicate).not.toHaveBeenCalled()
    expect(scope.cleanups.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
  })
  it('preserves polling delay and accumulated predicate time', async () => {
    const predicate = jest.fn().mockReturnValueOnce(false).mockReturnValueOnce('ready')
    const result = observe(runWithDeadline('poll', 200, () => waitForOf(predicate, { delay: 10 })))
    await jest.advanceTimersByTimeAsync(9)
    expect(predicate).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(1)
    expect(predicate).toHaveBeenLastCalledWith(10)
    expect(result.settled).toBe(false)
    await jest.advanceTimersByTimeAsync(10)
    expect(predicate.mock.calls).toEqual([[10], [20]])
    expect(result.value).toBe('ready')
    expect(jest.getTimerCount()).toBe(0)
  })

  it('preserves the polling local cap and timeout message', async () => {
    const predicate = jest.fn(() => false)
    const result = observe(
      runWithDeadline('poll', 200, () =>
        waitForOf(predicate, { delay: 10, maxWait: 5, timeoutMessage: 'local cap' })
      )
    )
    await jest.advanceTimersByTimeAsync(5)
    expect(result.error).toBeInstanceOf(KafkaJSTimeout)
    expect(result.error.message).toBe('local cap')
    expect(predicate).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  it('preserves ignored local polling caps under the original API budget', async () => {
    const predicate = jest.fn().mockReturnValueOnce(false).mockReturnValueOnce('ready')
    const result = observe(
      runWithDeadline('poll', 200, () =>
        waitForOf(predicate, { delay: 10, maxWait: 5, ignoreTimeout: true })
      )
    )
    await jest.advanceTimersByTimeAsync(5)
    expect(result.settled).toBe(false)
    expect(predicate).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(15)
    expect(predicate.mock.calls).toEqual([[10], [20]])
    expect(result.value).toBe('ready')
    expect(jest.getTimerCount()).toBe(0)
  })
})
