const Cluster = require('../index')
const createAdmin = require('../../admin')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../instrumentationEvents')
const { createErrorFromCode } = require('../../protocol/error')
const { requests } = require('../../protocol/requests')
const {
  KafkaJSBrokerNotFound,
  KafkaJSConnectionError,
  KafkaJSDeliveryTimeout,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSSASLAuthenticationError,
  KafkaJSTimeout,
} = require('../../errors')
const { newLogger, connectionOpts } = require('testHelpers')

const TOPIC = 'test-topic'
const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'
const NODES = [
  { nodeId: 1, host: '127.0.0.1', port: 9092, rack: null },
  { nodeId: 2, host: '127.0.0.1', port: 9095, rack: null },
]
// Explicit, so that an attempt count means the same under every run mode: three attempts in all.
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 1 }

/** A Metadata answer, as the codec's `parse` returns it, naming `TOPIC` under `topicId`. */
const answer = ({ topicId = ID_A, leader = 1, leaderEpoch = 5 } = {}) => ({
  throttleTime: 0,
  clientSideThrottleTime: 0,
  brokers: NODES,
  clusterId: 'c',
  controllerId: 1,
  topicMetadata: [
    {
      topicErrorCode: 0,
      topic: TOPIC,
      topicId,
      isInternal: false,
      partitionMetadata: [
        {
          partitionErrorCode: 0,
          partitionId: 0,
          leader,
          leaderEpoch,
          replicas: [1, 2],
          isr: [1, 2],
          offlineReplicas: [],
        },
      ],
    },
  ],
})

/** A broker that looks connected and answers `metadata()` with `jest.fn()`'s queue. */
const fakeBroker = ({ nodeId, host, port, rack }) => ({
  nodeId,
  allowAutoTopicCreation: false,
  isConnected: () => true,
  connect: async () => {},
  disconnect: async () => {},
  metadata: jest.fn(),
  connectionPool: { host, port, rack },
})

/**
 * A real `Cluster` over two fake brokers, node 1 tried first (`Math.random` is pinned so the
 * shuffle in `withBroker` keeps the order). `fresh` primes the pool's metadata so the refresh
 * pre-step has nothing to do; otherwise the cache is empty and the pre-step asks node 1 first.
 * `retry` is the client's retry option, as `new Kafka({ retry })` gives it to the cluster.
 */
const build = ({ fresh = true, allowAutoTopicCreation = false, retry = RETRY } = {}) => {
  jest.spyOn(Math, 'random').mockReturnValue(0.99)
  const emitter = new InstrumentationEventEmitter()
  const recreated = []
  emitter.addListener(CLUSTER_TOPIC_RECREATED, (event) => recreated.push(event.payload))
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    retry,
    allowAutoTopicCreation,
    instrumentationEmitter: emitter,
  })
  const brokers = NODES.map(fakeBroker)
  cluster.brokerPool.brokers = Object.fromEntries(brokers.map((broker) => [broker.nodeId, broker]))
  cluster.brokerPool.seedBroker = brokers[0]
  if (fresh) {
    cluster.brokerPool.metadata = answer()
    cluster.brokerPool.metadataExpireAt = Date.now() + 300000
    cluster.metadataCache.update(cluster.brokerPool.metadata, { full: false })
  }

  return { cluster, brokers, recreated }
}

/** The failures every boundary is tried with: retriable ones, then the ones that cannot succeed. */
const CODE_3 = ['code 3 (UNKNOWN_TOPIC_OR_PARTITION)', () => createErrorFromCode(3)]
const CODE_5 = ['code 5 (LEADER_NOT_AVAILABLE)', () => createErrorFromCode(5)]
const TRANSPORT = [
  'a transport error',
  () => new KafkaJSConnectionError('Connection error: ECONNRESET'),
]
const AUTHENTICATION = [
  'an authentication failure',
  () => new KafkaJSSASLAuthenticationError('SASL authentication failed'),
]
const AUTHORIZATION = ['authorization (code 29)', () => createErrorFromCode(29)]
const RETRIABLE = [CODE_3, CODE_5, TRANSPORT]
const NON_RETRIABLE = [AUTHENTICATION, AUTHORIZATION]
const KINDS = [...RETRIABLE, ...NON_RETRIABLE]

/** What a call may touch besides its result: the pool's last answer and the cache, both ways. */
const cacheState = (cluster) => ({
  pool: cluster.brokerPool.metadata,
  poolText: JSON.stringify(cluster.brokerPool.metadata),
  cached: {
    id: cluster.topicId(TOPIC),
    leader: cluster.metadataCache.leader(TOPIC, 0),
    epoch: cluster.metadataCache.leaderEpoch(TOPIC, 0),
  },
  routed: JSON.stringify(cluster.findTopicPartitionMetadata(TOPIC)),
})

/** A failed call leaves the cache as it was, emits no recreation event and adds no target topic. */
const expectUntouched = (cluster, before, recreated) => {
  expect(cacheState(cluster).pool).toBe(before.pool)
  expect(cacheState(cluster)).toEqual(before)
  expect(recreated).toEqual([])
  expect(cluster.targetTopics.size).toBe(0)
}

describe('Cluster > metadata contract', () => {
  afterEach(() => jest.restoreAllMocks())

  describe('stage 1: the refresh pre-step', () => {
    test('does nothing while the cached metadata is fresh and names the topics', async () => {
      const { cluster, brokers } = build()
      brokers[0].metadata.mockResolvedValue(answer({ leader: 2 }))
      const refresh = jest.spyOn(cluster.brokerPool, 'refreshMetadata')

      const result = await cluster.metadata({ topics: [TOPIC] })

      expect(refresh).not.toHaveBeenCalled()
      expect(brokers[0].metadata).toHaveBeenCalledTimes(1)
      expect(result.topicMetadata[0].partitionMetadata[0].leader).toBe(2)
    })

    test('refreshes first when the cache is fresh but lacks a named topic', async () => {
      const { cluster, brokers } = build()
      brokers[0].metadata.mockResolvedValue(answer())
      const refresh = jest.spyOn(cluster.brokerPool, 'refreshMetadata')

      await cluster.metadata({ topics: [TOPIC, 'other-topic'] })

      expect(refresh).toHaveBeenCalledTimes(1)
      expect(refresh).toHaveBeenCalledWith([TOPIC, 'other-topic'])
    })

    test('with no argument asks for every topic, the way listTopics and a regex subscribe call it', async () => {
      const { cluster, brokers } = build()
      brokers[0].metadata.mockResolvedValue(answer())

      await cluster.metadata()

      expect(brokers[0].metadata).toHaveBeenCalledTimes(1)
      expect(brokers[0].metadata).toHaveBeenCalledWith([], {
        includeTopicAuthorizedOperations: false,
      })
    })

    test('refreshes an expired cache, which writes the metadata cache, then asks the broker again', async () => {
      const { cluster, brokers } = build({ fresh: false })
      brokers[0].metadata.mockResolvedValue(answer())

      await cluster.metadata({ topics: [TOPIC] })

      expect(brokers[0].metadata).toHaveBeenCalledTimes(2)
      expect(cluster.topicId(TOPIC)).toBe(ID_A)
      expect(cluster.targetTopics.size).toBe(0)
    })

    test('emits TOPIC_RECREATED when the refresh sees a new topic id, exactly as any other route does', async () => {
      const { cluster, brokers, recreated } = build({ fresh: false })
      brokers[0].metadata.mockResolvedValue(answer({ topicId: ID_A }))
      await cluster.metadata({ topics: [TOPIC] })
      cluster.brokerPool.metadataExpireAt = 0
      brokers[0].metadata.mockResolvedValue(answer({ topicId: ID_B }))

      await cluster.metadata({ topics: [TOPIC] })

      expect(recreated).toHaveLength(1)
      expect(recreated[0]).toMatchObject({ topic: TOPIC })
      expect(cluster.topicId(TOPIC)).toBe(ID_B)
    })

    test('retries a LEADER_NOT_AVAILABLE answered by the refresh', async () => {
      const { cluster, brokers } = build({ fresh: false })
      brokers[0].metadata.mockRejectedValueOnce(createErrorFromCode(5)).mockResolvedValue(answer())

      await expect(cluster.metadata({ topics: [TOPIC] })).resolves.toMatchObject({
        clusterId: 'c',
      })
    })

    test('retries a raw LEADER_NOT_AVAILABLE that escapes the pre-step', async () => {
      const { cluster, brokers } = build()
      const refresh = jest
        .spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary')
        .mockRejectedValueOnce(createErrorFromCode(5))
      brokers[0].metadata.mockResolvedValue(answer())

      await expect(cluster.metadata({ topics: [TOPIC] })).resolves.toMatchObject({ clusterId: 'c' })

      expect(refresh).toHaveBeenCalledTimes(2)
    })

    test('throws any other refresh error unchanged, after one attempt, writing nothing', async () => {
      const { cluster, brokers } = build({ fresh: false })
      const denied = createErrorFromCode(29)
      brokers[0].metadata.mockRejectedValue(denied)

      await expect(cluster.metadata({ topics: [TOPIC] })).rejects.toBe(denied)

      expect(brokers[0].metadata).toHaveBeenCalledTimes(1)
      expect(brokers[1].metadata).not.toHaveBeenCalled()
      expect(cluster.topicId(TOPIC)).toBeUndefined()
    })
  })

  describe('stage 1, each kind of failure at the refresh boundary', () => {
    const failRefresh = (cluster, failure) =>
      jest.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary').mockRejectedValue(failure)

    test('a raw code 5 escaping the pre-step is retried until the budget is spent', async () => {
      const { cluster, brokers, recreated } = build()
      const before = cacheState(cluster)
      const failure = CODE_5[1]()
      const refresh = failRefresh(cluster, failure)

      const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause).toBe(failure)
      expect(refresh).toHaveBeenCalledTimes(RETRY.retries + 1)
      brokers.forEach(({ metadata }) => expect(metadata).not.toHaveBeenCalled())
      expectUntouched(cluster, before, recreated)
    })

    test.each([CODE_3, TRANSPORT, AUTHENTICATION, AUTHORIZATION])(
      '%s bails with the same object after one attempt',
      async (_name, make) => {
        const { cluster, brokers, recreated } = build()
        const before = cacheState(cluster)
        const failure = make()
        const refresh = failRefresh(cluster, failure)

        await expect(cluster.metadata({ topics: [TOPIC] })).rejects.toBe(failure)

        expect(refresh).toHaveBeenCalledTimes(1)
        brokers.forEach(({ metadata }) => expect(metadata).not.toHaveBeenCalled())
        expectUntouched(cluster, before, recreated)
      }
    )
  })

  describe('stage 1 through the real refresh', () => {
    test('a refresh that keeps answering LEADER_NOT_AVAILABLE spends its own budget, then the method rejects at once, wrapped', async () => {
      const { cluster, brokers } = build({ fresh: false })
      brokers.forEach(({ metadata }) => metadata.mockRejectedValue(createErrorFromCode(5)))
      const refresh = jest.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary')

      const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

      expect(error.name).toBe('KafkaJSNonRetriableError')
      expect(error.type).toBe('LEADER_NOT_AVAILABLE')
      expect(error.cause.type).toBe('LEADER_NOT_AVAILABLE')
      expect(refresh).toHaveBeenCalledTimes(1)
      const calls = brokers.reduce((n, { metadata }) => n + metadata.mock.calls.length, 0)
      expect(calls).toBe(RETRY.retries + 1)
    })
  })

  describe('a partition without a leader', () => {
    /** Answers `metadata()` through the codec's own parse for the request it was given. */
    const answerThroughParse = (brokers, data) =>
      brokers.forEach(({ metadata }) =>
        metadata.mockImplementation(async (topics, options = {}) =>
          requests.Metadata.protocol({ version: 12 })({ topics, ...options }).response.parse(data)
        )
      )
    const leaderlessAnswer = () => {
      const data = answer({ leader: -1 })
      data.topicMetadata[0].partitionMetadata[0].partitionErrorCode = 5
      return data
    }

    test('fails a refresh that does not allow it, after the whole retry budget', async () => {
      const { cluster, brokers } = build({ fresh: false })
      answerThroughParse(brokers, leaderlessAnswer())
      cluster.targetTopics.add(TOPIC)

      const error = await cluster.refreshMetadata().catch((e) => e)

      expect(error.type).toBe('LEADER_NOT_AVAILABLE')
      const calls = brokers.reduce((n, { metadata }) => n + metadata.mock.calls.length, 0)
      expect(calls).toBe(RETRY.retries + 1)
      expect(cluster.brokerPool.metadata).toBeFalsy()
    })

    test('installs it as leader -1 in one attempt when the refresh allows it', async () => {
      const { cluster, brokers } = build({ fresh: false })
      answerThroughParse(brokers, leaderlessAnswer())
      cluster.targetTopics.add(TOPIC)

      await cluster.refreshMetadata({ allowLeaderless: true })

      const calls = brokers.reduce((n, { metadata }) => n + metadata.mock.calls.length, 0)
      expect(calls).toBe(1)
      expect(cluster.metadataCache.leader(TOPIC, 0)).toBe(-1)
    })
  })

  describe('stage 2: selection and failover', () => {
    test('retries an empty broker table as a retriable error until the budget is spent', async () => {
      const { cluster } = build()
      cluster.brokerPool.brokers = {}
      const withBroker = jest.spyOn(cluster.brokerPool, 'withBroker')

      const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause).toBeInstanceOf(KafkaJSBrokerNotFound)
      expect(withBroker).toHaveBeenCalledTimes(RETRY.retries + 1)
    })

    test('retries when no broker can be connected to, retaining the last connection error', async () => {
      const { cluster, brokers } = build()
      const failures = NODES.map(
        ({ host, port }) =>
          new KafkaJSConnectionError('connect ECONNREFUSED', { broker: `${host}:${port}` })
      )
      brokers.forEach((broker, index) => {
        broker.isConnected = () => false
        broker.connect = jest.fn(async () => {
          throw failures[index]
        })
      })

      const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause).toBe(failures[1])
      brokers.forEach((broker) => {
        expect(broker.connect).toHaveBeenCalledTimes(RETRY.retries + 1)
        expect(broker.metadata).not.toHaveBeenCalled()
      })
    })

    test('ends in KafkaJSDeliveryTimeout when retry.maxTotalTime passes before the budget is spent', async () => {
      const retry = { retries: 5, initialRetryTime: 100, maxRetryTime: 100, maxTotalTime: 50 }
      const { cluster, brokers } = build({ retry })
      const unreachable = new KafkaJSConnectionError('connect ECONNREFUSED')
      brokers.forEach((broker) => {
        broker.isConnected = () => false
        broker.connect = jest.fn(async () => {
          throw unreachable
        })
      })

      const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.cause).toBe(unreachable)
      expect(brokers[0].connect).toHaveBeenCalledTimes(1)
      brokers.forEach(({ metadata }) => expect(metadata).not.toHaveBeenCalled())
    })

    const failLookup = (cluster, failure) =>
      jest.spyOn(cluster.brokerPool, 'findBroker').mockRejectedValue(failure)

    test.each([
      [...CODE_3, 1],
      [...CODE_5, 1],
      [...TRANSPORT, 2],
    ])(
      'retries %s raised while finding the broker, then gives up with the original cause',
      async (_name, make, lookupsPerAttempt) => {
        const { cluster, brokers, recreated } = build()
        const before = cacheState(cluster)
        const failure = make()
        const lookup = failLookup(cluster, failure)

        const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

        expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
        expect(error.cause).toBe(failure)
        expect(lookup).toHaveBeenCalledTimes(lookupsPerAttempt * (RETRY.retries + 1))
        brokers.forEach(({ metadata }) => expect(metadata).not.toHaveBeenCalled())
        expectUntouched(cluster, before, recreated)
      }
    )

    test.each(NON_RETRIABLE)(
      'does not retry %s raised while finding the broker: one attempt, wrapped, original cause kept',
      async (_name, make) => {
        const { cluster, brokers, recreated } = build()
        const before = cacheState(cluster)
        const failure = make()
        const lookup = failLookup(cluster, failure)

        const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

        expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
        expect(error).not.toBe(failure)
        expect(error.cause).toBe(failure)
        expect(lookup).toHaveBeenCalledTimes(1)
        brokers.forEach(({ metadata }) => expect(metadata).not.toHaveBeenCalled())
        expectUntouched(cluster, before, recreated)
      }
    )
  })

  describe('stage 3: the call on a broker', () => {
    test('a connection failure followed by a callback failure preserves the callback error', async () => {
      const { cluster, brokers } = build()
      brokers[0].isConnected = () => false
      brokers[0].connect = async () => {
        throw new KafkaJSConnectionError('connect ECONNREFUSED')
      }
      const denied = createErrorFromCode(29)
      brokers[1].metadata.mockRejectedValue(denied)

      await expect(cluster.metadata({ topics: [TOPIC] })).rejects.toBe(denied)

      expect(brokers[0].metadata).not.toHaveBeenCalled()
      expect(brokers[1].metadata).toHaveBeenCalledTimes(1)
    })

    test('returns the first answer: a failure on one broker is swallowed and the next is asked', async () => {
      const { cluster, brokers } = build()
      brokers[0].metadata.mockRejectedValue(createErrorFromCode(5))
      brokers[1].metadata.mockResolvedValue(answer({ leader: 2 }))

      const result = await cluster.metadata({ topics: [TOPIC] })

      expect(result.topicMetadata[0].partitionMetadata[0].leader).toBe(2)
      expect(brokers[0].metadata).toHaveBeenCalledTimes(1)
      expect(brokers[1].metadata).toHaveBeenCalledTimes(1)
    })

    test('when one call fails and the next broker cannot be connected to, the attempt is retried and the call error is lost', async () => {
      const { cluster, brokers } = build()
      const unreachable = new KafkaJSConnectionError('connect ECONNREFUSED', {
        broker: '127.0.0.1:9095',
      })
      brokers[0].metadata.mockRejectedValue(createErrorFromCode(29))
      brokers[1].isConnected = () => false
      brokers[1].connect = jest.fn(async () => {
        throw unreachable
      })

      const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(error.cause).toBe(unreachable)
      expect(brokers[0].metadata).toHaveBeenCalledTimes(RETRY.retries + 1)
      expect(brokers[1].metadata).not.toHaveBeenCalled()
    })

    const withCause = (cause) => new KafkaJSNonRetriableError('denied by the broker', { cause })
    const FAILURES = [
      ...KINDS,
      ['an error that wraps a cause', () => withCause(new Error('the root cause'))],
    ]

    test.each(FAILURES)(
      'when every broker fails with %s, rejects with the last broker error itself, after one pass',
      async (_name, failure) => {
        const { cluster, brokers, recreated } = build()
        const before = cacheState(cluster)
        const first = failure()
        const last = failure()
        brokers[0].metadata.mockRejectedValue(first)
        brokers[1].metadata.mockRejectedValue(last)

        const error = await cluster.metadata({ topics: [TOPIC] }).catch((e) => e)

        expect(error).toBe(last)
        expect(error.constructor).toBe(last.constructor)
        expect(error.cause).toBe(last.cause)
        expect(brokers[0].metadata).toHaveBeenCalledTimes(1)
        expect(brokers[1].metadata).toHaveBeenCalledTimes(1)
        expectUntouched(cluster, before, recreated)
      }
    )

    test('never resolves null, whatever the brokers answer with', async () => {
      const { cluster, brokers } = build()
      brokers.forEach(({ metadata }) => metadata.mockRejectedValue(new Error('broken')))

      await expect(cluster.metadata({ topics: [TOPIC] })).rejects.toThrow('broken')
    })
  })

  describe('the snapshot itself', () => {
    test('is the broker current answer, never merged into the cache, never an event, never a target topic', async () => {
      const { cluster, brokers, recreated } = build()
      const cached = cluster.brokerPool.metadata
      brokers[0].metadata.mockResolvedValue(answer({ topicId: ID_B, leader: 2, leaderEpoch: 9 }))

      const snapshot = await cluster.metadata({ topics: [TOPIC] })

      expect(snapshot.topicMetadata[0].topicId).toBe(ID_B)
      expect(snapshot.topicMetadata[0].partitionMetadata[0].leader).toBe(2)
      expect(cluster.brokerPool.metadata).toBe(cached)
      expect(cluster.topicId(TOPIC)).toBe(ID_A)
      expect(cluster.metadataCache.leaderEpoch(TOPIC, 0)).toBe(5)
      expect(recreated).toEqual([])
      expect(cluster.targetTopics.size).toBe(0)
    })

    test.each(KINDS)(
      'a call that fails with %s after a successful refresh keeps what the refresh wrote and emits nothing more',
      async (_name, make) => {
        const { cluster, brokers, recreated } = build({ fresh: false })
        const refresh = jest.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary')
        const failure = make()
        brokers[0].metadata.mockResolvedValueOnce(answer({ topicId: ID_A }))
        brokers[0].metadata.mockRejectedValue(failure)
        brokers[1].metadata.mockRejectedValue(failure)

        await expect(cluster.metadata({ topics: [TOPIC] })).rejects.toBe(failure)

        expect(refresh).toHaveBeenCalledTimes(1)
        expect(brokers[0].metadata).toHaveBeenCalledTimes(2)
        expect(brokers[1].metadata).toHaveBeenCalledTimes(1)
        expect(cacheState(cluster)).toMatchObject({
          cached: { id: ID_A, leader: 1, epoch: 5 },
          poolText: JSON.stringify(answer({ topicId: ID_A })),
        })
        expect(recreated).toEqual([])
        expect(cluster.targetTopics.size).toBe(0)
      }
    )
  })

  describe('after a leader hint only leader and leaderEpoch change', () => {
    test('replicas, isr and offlineReplicas stay as the last metadata response said', async () => {
      const { cluster } = build()
      const [partition] = cluster.findTopicPartitionMetadata(TOPIC)
      const before = { ...partition }

      await expect(
        cluster.applyLeaderHint({ topic: TOPIC, partition: 0, leaderId: 2, leaderEpoch: 6 })
      ).resolves.toBe(true)

      expect(cluster.findTopicPartitionMetadata(TOPIC)[0]).toEqual({
        ...before,
        leader: 2,
        leaderEpoch: 6,
      })
      expect(cluster.metadataCache.leader(TOPIC, 0)).toBe(2)
      expect(partition.replicas).toEqual([1, 2])
      expect(partition.isr).toEqual([1, 2])
      expect(partition.offlineReplicas).toEqual([])
    })
  })
})

describe('Admin > fetchTopicMetadata keeps its own outer retry over Cluster#metadata', () => {
  const adminOver = (cluster) => createAdmin({ logger: newLogger(), retry: RETRY, cluster })

  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  test('resolves after a LEADER_NOT_AVAILABLE from every broker, then an answer', async () => {
    const { cluster, brokers } = build({ allowAutoTopicCreation: true })
    brokers.forEach(({ metadata }) =>
      metadata.mockRejectedValueOnce(createErrorFromCode(5)).mockResolvedValue(answer())
    )

    await expect(adminOver(cluster).fetchTopicMetadata({ topics: [TOPIC] })).resolves.toMatchObject(
      { topics: [{ name: TOPIC }] }
    )
  })

  test.each([
    ['LEADER_NOT_AVAILABLE', 5, false],
    ['UNKNOWN_TOPIC_OR_PARTITION with auto topic creation', 3, true],
  ])('ends a persistent %s with KafkaJSTimeout', async (_name, code, allowAutoTopicCreation) => {
    jest.useFakeTimers()
    const { cluster, brokers } = build({ allowAutoTopicCreation })
    brokers.forEach(({ metadata }) => metadata.mockRejectedValue(createErrorFromCode(code)))

    const outcome = adminOver(cluster)
      .fetchTopicMetadata({ topics: [TOPIC] })
      .catch((e) => e)
    await jest.advanceTimersByTimeAsync(10500)

    await expect(outcome).resolves.toBeInstanceOf(KafkaJSTimeout)
  })

  test('ends with KafkaJSTimeout at 10 s when the brokers cannot be reached within 10 s', async () => {
    jest.useFakeTimers()
    const { cluster, brokers } = build()
    brokers.forEach((broker) => {
      broker.isConnected = () => false
      broker.connect = jest.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 4000))
        throw new KafkaJSConnectionError('Connection timeout')
      })
    })

    let settled = false
    const outcome = adminOver(cluster)
      .fetchTopicMetadata({ topics: [TOPIC] })
      .catch((e) => e)
      .finally(() => {
        settled = true
      })
    await jest.advanceTimersByTimeAsync(9999)
    expect(settled).toBe(false)
    await jest.advanceTimersByTimeAsync(1)

    await expect(outcome).resolves.toBeInstanceOf(KafkaJSTimeout)
    await jest.advanceTimersByTimeAsync(5000)
  })

  test.each([
    ['an authorization error', 29, false],
    ['UNKNOWN_TOPIC_OR_PARTITION without auto topic creation', 3, false],
  ])('propagates %s unchanged, at once', async (_name, code, allowAutoTopicCreation) => {
    const { cluster, brokers } = build({ allowAutoTopicCreation })
    const failure = createErrorFromCode(code)
    brokers.forEach(({ metadata }) => metadata.mockRejectedValue(failure))

    await expect(adminOver(cluster).fetchTopicMetadata({ topics: [TOPIC] })).rejects.toBe(failure)
  })
})
