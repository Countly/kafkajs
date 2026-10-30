const createConsumer = require('../index')
const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const OffsetOperations = require('../offsetManager/offsetOperations')
const waitFor = require('../../utils/waitFor')
const { KafkaJSProtocolError, KafkaJSNonRetriableError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')

const TOPIC = 'created-later'
const HEARTBEAT_MS = 5000
const METADATA_MAX_AGE = 300000
const TOPIC_ID = 'kOJl1vDcS4Ct5xLg5rGqaQ'
const PARTITIONS = [0, 1, 2]
const UNKNOWN_TOPIC_OR_PARTITION = 3
const INVALID_TOPIC_EXCEPTION = 17
const TOPIC_AUTHORIZATION_FAILED = 29

/** @param {number} code protocol error code @returns {Error} what a refused metadata refresh rejects with */
const refused = (code) => new KafkaJSProtocolError(createErrorFromCode(code))

/**
 * @param {number} [code] the error the target refresh fails with; none lets it succeed
 * @returns {object} the cluster surface `subscribe` and `run` touch
 */
const fakeCluster = (code) => ({
  connect: jest.fn(async () => {
    throw new KafkaJSNonRetriableError('nothing to connect to')
  }),
  disconnect: jest.fn(async () => {}),
  refreshMetadata: jest.fn(async () => {}),
  refreshMetadataIfNecessary: jest.fn(async () => {}),
  useConsumerMetadataScope: jest.fn(),
  releaseConsumerMetadataScope: jest.fn(),
  resetTargetTopics: jest.fn(async (_names, { publish } = {}) => publish?.()),
  removeBroker: jest.fn(),
  isConnected: () => true,
  addMultipleTargetTopics: jest.fn(async () => {
    if (code !== undefined) throw refused(code)
  }),
  onTopicRecreated: () => () => {},
  offsetOperations: () => new OffsetOperations(),
  retry: {},
})

/** @param {string} groupProtocol @param {object} cluster @returns {object} a consumer over the stub */
const consumerOver = (groupProtocol, cluster) =>
  createConsumer({
    cluster,
    groupId: 'group-a',
    logger: newLogger(),
    metadataMaxAge: METADATA_MAX_AGE,
    groupProtocol,
  })

/** The first group a case built; module-level because jest keeps an earlier case's mock factory. */
let captured = null

describe('Consumer > subscribing to a topic that does not exist yet', () => {
  it('resolves under the consumer protocol and keeps the name as a desired subscription', async () => {
    const consumer = consumerOver('consumer', fakeCluster(UNKNOWN_TOPIC_OR_PARTITION))

    await expect(consumer.subscribe({ topics: [TOPIC] })).resolves.toBeUndefined()
    await expect(consumer.subscribe({ topic: TOPIC })).resolves.toBeUndefined()
  })

  it('still rejects under the classic protocol, as 2.2.4 did', async () => {
    const consumer = consumerOver('classic', fakeCluster(UNKNOWN_TOPIC_OR_PARTITION))

    await expect(consumer.subscribe({ topics: [TOPIC] })).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
  })

  it.each([
    ['INVALID_TOPIC_EXCEPTION', INVALID_TOPIC_EXCEPTION],
    ['TOPIC_AUTHORIZATION_FAILED', TOPIC_AUTHORIZATION_FAILED],
  ])('still rejects %s under the consumer protocol', async (type, code) => {
    const consumer = consumerOver('consumer', fakeCluster(code))

    await expect(consumer.subscribe({ topics: [TOPIC] })).rejects.toMatchObject({ type })
  })

  /**
   * Runs a consumer-protocol consumer whose connect fails right after its group is built, and
   * returns the group's first construction options with the unregistered names as they were then.
   * @param {object} cluster the stub cluster
   * @param {(consumer: object) => Promise<void>} act the subscriptions made before `run`
   * @returns {Promise<object>} the options `ConsumerGroup` received
   */
  const firstGroupOptions = async (cluster, act) => {
    captured = null

    await jest.isolateModulesAsync(async () => {
      const Real = require('../consumerGroup')
      jest.doMock(
        '../consumerGroup',
        () =>
          class extends Real {
            constructor(options) {
              super(options)
              captured ??= { ...options, unregisteredTopics: [...options.unregisteredTopics] }
            }
          }
      )
      const consumer = require('../index')({
        cluster,
        groupId: 'group-a',
        logger: newLogger(),
        metadataMaxAge: METADATA_MAX_AGE,
        groupProtocol: 'consumer',
      })
      await expect(act(consumer)).resolves.toBeUndefined()
      await consumer.run({ eachMessage: async () => {} })
      await consumer.disconnect()
    })

    return captured
  }

  it('hands the run the names the cluster could not register yet', async () => {
    const options = await firstGroupOptions(fakeCluster(UNKNOWN_TOPIC_OR_PARTITION), (consumer) =>
      consumer.subscribe({ topics: [TOPIC] })
    )

    expect(options.unregisteredTopics).toEqual([TOPIC])
    expect(options.topics).toEqual([TOPIC])
  })

  it('forgets a name a later subscribe registered', async () => {
    const cluster = fakeCluster()
    cluster.addMultipleTargetTopics.mockRejectedValueOnce(refused(UNKNOWN_TOPIC_OR_PARTITION))

    const options = await firstGroupOptions(cluster, async (consumer) => {
      await consumer.subscribe({ topics: [TOPIC] })
      await consumer.subscribe({ topics: [TOPIC] })
    })

    expect(options.unregisteredTopics).toEqual([])
  })

  it('forgets every name on unsubscribe', async () => {
    const options = await firstGroupOptions(
      fakeCluster(UNKNOWN_TOPIC_OR_PARTITION),
      async (consumer) => {
        await consumer.subscribe({ topics: [TOPIC] })
        await consumer.unsubscribe()
        await consumer.subscribe({ topics: ['other'] })
      }
    )

    expect(options.unregisteredTopics).toEqual(['other'])
  })
})

/** Every runner the scripted cases build, torn down after each. */
const built = []

/**
 * A real consumer-protocol ConsumerGroup, RebalanceCoordinator and Runner over a scripted
 * coordinator and a cluster whose topic does not exist until `create()`. The cluster only knows the
 * topic's partitions once the name is a registered target, as a real one does.
 *
 * @param {object} options
 * @param {Set<string>} options.unregisteredTopics the group's names awaiting a cluster target
 * @param {{now: number}} options.clock the scripted clock `Date.now` reads; each trip moves it one heartbeat
 * @returns {object} the group, the scripted pieces and the trip driver
 */
const build = ({ unregisteredTopics, clock }) => {
  let created = false
  const targets = new Set()
  const log = []
  const cache = new Map()
  const script = [
    { memberEpoch: 1, heartbeatIntervalMs: HEARTBEAT_MS, assignment: { topicPartitions: [] } },
    {
      memberEpoch: 2,
      heartbeatIntervalMs: HEARTBEAT_MS,
      assignment: { topicPartitions: [{ topicId: TOPIC_ID, partitions: PARTITIONS }] },
    },
    { memberEpoch: 2, heartbeatIntervalMs: HEARTBEAT_MS, assignment: null },
  ]
  let index = 0
  const coordinator = {
    isConnected: () => true,
    nodeId: 0,
    consumerGroupHeartbeat: jest.fn(async (request) => {
      log.push({ kind: 'heartbeat', request })
      return { throttleTime: 0, errorCode: 0, memberId: 'member-1', ...script[index++ % 3] }
    }),
    offsetCommit: jest.fn(async () => {}),
    offsetFetch: jest.fn(async () => ({ responses: [] })),
  }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadataForTopicIds: jest.fn(async (ids) => {
      log.push({ kind: 'by-id', ids })
      if (created) cache.set(TOPIC_ID, TOPIC)
      return { resolved: [], unresolved: [] }
    }),
    committedOffsets: () => ({}),
    topicName: (topicId) => cache.get(topicId),
    topicId: () => undefined,
    findTopicPartitionMetadata: (topic) =>
      topic === TOPIC && targets.has(TOPIC)
        ? PARTITIONS.map((partitionId) => ({ partitionId, leader: 0 }))
        : [],
    findBroker: jest.fn(async () => ({ fetch: async () => ({ responses: [] }) })),
    findGroupCoordinator: jest.fn(async () => coordinator),
    addMultipleTargetTopics: jest.fn(async (names) => {
      log.push({ kind: 'target', names })
      for (const name of names) targets.add(name)
    }),
  }
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const group = new ConsumerGroup({
    retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 2 },
    cluster,
    groupId: 'absent-topic-group',
    groupProtocol: 'consumer',
    topics: [TOPIC],
    topicConfigurations: { [TOPIC]: {} },
    unregisteredTopics,
    logger: newLogger(),
    instrumentationEmitter,
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    metadataMaxAge: METADATA_MAX_AGE,
  })
  group.coordinator = coordinator
  group.getNodeIds = () => [0]
  group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()

  const runner = new Runner({
    consumerGroup: group,
    instrumentationEmitter,
    logger: newLogger(),
    eachBatch: async () => {},
    concurrency: 1,
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    autoCommit: false,
    onCrash: jest.fn(),
  })
  runner.running = true
  runner.heartbeatManager.disable()
  built.push(runner)

  const trip = async () => {
    clock.now += HEARTBEAT_MS + 1
    if (group.memberEpoch <= 0) {
      runner.rebalanceCoordinator.runRound(null, 'initial')
      await waitFor(() => group.groupMembership.immediatePending, { delay: 5, maxWait: 5000 })
    }
    await group.heartbeat({ interval: 0 })
    await runner.rebalanceCoordinator.idle()
  }

  return { group, cluster, targets, log, trip, create: () => (created = true) }
}

describe('Consumer > an assigned topic the member subscribed to before it existed', () => {
  const clock = { now: 1_000_000 }
  beforeEach(() => {
    clock.now = 1_000_000
    jest.spyOn(Date, 'now').mockImplementation(() => clock.now)
  })
  afterEach(async () => {
    jest.restoreAllMocks()
    for (const runner of built.splice(0)) {
      runner.running = false
      runner.heartbeatManager.stop()
      runner.pollTimer.stop()
      await runner.fetchManager.stop()
    }
  })

  it('names the id at once, registers the target, and owns the partitions with no metadata age elapsing', async () => {
    const h = build({ unregisteredTopics: new Set([TOPIC]), clock })

    await h.trip()
    expect(h.group.assigned()).toEqual([])
    expect(h.cluster.addMultipleTargetTopics).not.toHaveBeenCalled()

    h.create()
    await h.trip()
    await h.trip()

    // Three heartbeats of scripted time passed, far short of the metadata age: only the immediate
    // by-id path can have named the topic.
    expect(clock.now - 1_000_000).toBeLessThan(METADATA_MAX_AGE)
    expect(h.log.filter(({ kind }) => kind === 'by-id')).toEqual([
      { kind: 'by-id', ids: [TOPIC_ID] },
    ])
    expect(h.log.filter(({ kind }) => kind === 'target')).toEqual([
      { kind: 'target', names: [TOPIC] },
    ])
    expect(h.targets.has(TOPIC)).toBe(true)
    expect(h.group.assigned()).toEqual([{ topic: TOPIC, partitions: PARTITIONS }])
  })

  it('registers the target once, and not again after a later assignment change', async () => {
    const unregistered = new Set([TOPIC])
    const h = build({ unregisteredTopics: unregistered, clock })

    h.create()
    await h.trip()
    await h.trip()
    await h.trip()

    expect(h.cluster.addMultipleTargetTopics).toHaveBeenCalledTimes(1)
    expect(unregistered.size).toBe(0)
  })

  it('leaves a name that subscribe registered alone', async () => {
    const h = build({ unregisteredTopics: new Set(), clock })

    h.create()
    await h.trip()
    await h.trip()

    expect(h.cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
  })
})
