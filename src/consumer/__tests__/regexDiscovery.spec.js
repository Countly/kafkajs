const Subscription = require('../subscription')
const Cluster = require('../../cluster')
const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')
const { buildRequest } = require('../groupProtocol/heartbeatState')
const ConsumerGroupProtocol = require('../groupProtocol/consumer')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createPatternGroup } = require('./fixtures/patternGroup')
const { Kafka, logLevel } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { metadataV13 } = require('../../../testHelpers/wire/expected')
const { lanes } = require('../../../testHelpers/lane')
const {
  liveCase,
  liveRun,
  caseTimeout,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { CleanupStack } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  newLogger,
  connectionOpts,
  createTopic,
  plainTextBrokers,
  secureRandom,
  waitFor,
} = require('testHelpers')

const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'

/**
 * A Metadata answer that fails when it is awaited, so it is never an unhandled rejection before.
 * @param {Error} error
 * @returns {{then: Function}}
 */
const failing = (error) => ({ then: (_resolve, reject) => reject(error) })

/** Builds metadata with independently specified names, ids and internal flags.
 * @param {Array<string|object>} rows
 * @returns {object}
 */
const metadata = (rows) => ({
  brokers: [{ nodeId: 1, host: 'localhost', port: 9092, rack: null }],
  topicMetadata: rows.map((row) => ({
    topicErrorCode: 0,
    isInternal: false,
    topicId: null,
    partitionMetadata: [],
    ...(typeof row === 'string' ? { topic: row } : row),
  })),
})

/** Creates a gate whose promise is always released by the test's finally block.
 * @returns {{promise: Promise, release: Function}}
 */
const gate = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}

/** Uses real cluster/cache/pool code with two queued Metadata answers and no socket.
 * @param {Array<Promise|object>} answers
 * @returns {{cluster: object, broker: object}}
 */
const clusterWith = (answers) => {
  const cluster = new Cluster({ ...connectionOpts(), logger: newLogger(), retry: { retries: 0 } })
  const broker = {
    nodeId: 1,
    allowAutoTopicCreation: true,
    connectionPool: { host: 'localhost', port: 9092, rack: null },
    metadata: jest.fn(async () => answers.shift()),
  }
  cluster.brokerPool.seedBroker = broker
  cluster.brokerPool.brokers = { 1: broker }
  cluster.brokerPool.findConnectedBroker = async () => broker
  return { cluster, broker }
}

describe('subscription matching and metadata scope', () => {
  test.each(['g', 'y', 'gy'])(
    'ignores %s while preserving partial matching and caller state',
    (flags) => {
      const pattern = new RegExp('topic-', flags)
      pattern.lastIndex = 3
      const subscription = new Subscription()
      subscription.add({ topics: [pattern], policy: { kind: 'earliest' } })
      for (const names of [
        ['my-topic-1', 'my-topic-2'],
        ['my-topic-2', 'my-topic-1'],
      ]) {
        expect(subscription.evaluate(metadata(names)).names).toEqual(['my-topic-1', 'my-topic-2'])
        expect(pattern.lastIndex).toBe(3)
      }
    }
  )

  test('preserves other flags, anchors, explicit internal names and last call policy', () => {
    const subscription = new Subscription()
    subscription.add({ topics: [/^ORDER-/i, '__consumer_offsets'], policy: { kind: 'earliest' } })
    subscription.add({ topics: [/order-2/i], policy: { kind: 'latest' } })
    const snapshot = subscription.evaluate(
      metadata([
        'order-1',
        'order-2',
        'my-order-3',
        { topic: '__consumer_offsets', isInternal: true },
        { topic: 'ORDER-secret', isInternal: true },
        { topic: 'ORDER-error', topicErrorCode: 29 },
        { topic: 'ORDER-leaderless', partitionMetadata: [{ partitionId: 0, leader: -1 }] },
      ])
    )
    expect(snapshot.names).toEqual(['ORDER-leaderless', '__consumer_offsets', 'order-1', 'order-2'])
    expect(snapshot.topicConfigurations.get('order-1').offsetReset.kind).toBe('earliest')
    expect(snapshot.topicConfigurations.get('order-2').offsetReset.kind).toBe('latest')
  })

  test('prototype-looking literals and patterns have ordinary last-call precedence', () => {
    const subscription = new Subscription()
    const caller = /constructor|toString/gy
    caller.lastIndex = 9
    subscription.add({ topics: ['__proto__', caller], policy: { kind: 'earliest' } })
    subscription.add({ topics: ['constructor'], policy: { kind: 'none' } })
    const result = subscription.evaluate(metadata(['__proto__', 'constructor', 'toString']))
    expect(result.names).toEqual(['__proto__', 'constructor', 'toString'])
    expect(result.topicConfigurations.get('constructor').offsetReset.kind).toBe('none')
    expect(caller.lastIndex).toBe(9)
    result.names.length = 0
    result.topicConfigurations.clear()
    expect(
      subscription.evaluate(metadata(['__proto__', 'constructor', 'toString'])).names
    ).toHaveLength(3)
  })

  test('first pattern and each retired run refuse previous metadata tickets', () => {
    const subscription = new Subscription()
    const named = subscription.captureMetadata()
    subscription.add({ topics: [/orders/], policy: { kind: 'latest' } })
    expect(named.isCurrent()).toBe(false)
    const first = subscription.captureMetadata()
    const second = subscription.captureMetadata()
    expect(first.isCurrent()).toBe(false)
    expect(second.isCurrent()).toBe(true)
    const target = subscription.captureScope()
    subscription.retireRun()
    expect(second.isCurrent()).toBe(false)
    expect(target()).toBe(false)
    const olderDeclaration = subscription.captureMetadata()
    subscription.add({ topics: ['literal'], policy: { kind: 'earliest' } })
    expect(olderDeclaration.isCurrent()).toBe(false)
  })

  test('the share key moves with generation and declaration revision, not request sequence', () => {
    const subscription = new Subscription()
    const initial = subscription.shareKey()
    expect(subscription.shareKey()).toBe(initial)
    subscription.captureMetadata()
    subscription.evaluate(metadata(['orders-1']))
    expect(subscription.shareKey()).toBe(initial)
    subscription.add({ topics: ['literal'], policy: { kind: 'earliest' } })
    const declared = subscription.shareKey()
    expect(declared).not.toBe(initial)
    subscription.retireMetadata()
    expect(subscription.shareKey()).not.toBe(declared)
    expect(new Subscription().shareKey()).not.toBe(initial)
  })

  test('an older full reply cannot install over a newer full reply', async () => {
    const held = gate()
    const newer = metadata(['new-topic'])
    const { cluster } = clusterWith([held.promise, newer])
    const scope = new Subscription()
    const onMetadata = jest.fn()
    const first = cluster.brokerPool.refreshMetadata([], { metadataScope: scope, onMetadata })
    await Promise.resolve()
    try {
      await cluster.brokerPool.refreshMetadata([], { metadataScope: scope, onMetadata })
      held.release(metadata(['old-topic']))
      await first
      expect(cluster.brokerPool.metadata).toBe(newer)
      expect([...cluster.metadataCache.topics.keys()]).toEqual(['new-topic'])
      expect(onMetadata.mock.calls.map(([m]) => m.topicMetadata[0].topic)).toEqual(['new-topic'])
    } finally {
      held.release(metadata(['old-topic']))
      await first
    }
  })

  test('an old named reply cannot install after entry to pattern scope', async () => {
    const held = gate()
    const { cluster } = clusterWith([held.promise])
    const scope = new Subscription()
    const onMetadata = jest.fn()
    const first = cluster.brokerPool.refreshMetadata(['old'], { metadataScope: scope, onMetadata })
    await Promise.resolve()
    try {
      scope.add({ topics: [/orders/], policy: { kind: 'latest' } })
      held.release(metadata(['old']))
      await first
      expect(cluster.brokerPool.metadata).toBeNull()
      expect(cluster.metadataCache.topics.size).toBe(0)
      expect(onMetadata).not.toHaveBeenCalled()
    } finally {
      held.release(metadata(['old']))
      await first
    }
  })

  test('pattern scope asks all topics with auto creation disabled, including zero matches', async () => {
    const response = metadata(['unrelated'])
    const { cluster, broker } = clusterWith([response])
    const scope = new Subscription()
    scope.add({ topics: [/orders/], policy: { kind: 'latest' } })
    await cluster.brokerPool.refreshMetadata(['stale-match'], { metadataScope: scope })
    expect(broker.metadata).toHaveBeenCalledWith([], {
      allowAutoTopicCreation: false,
      guard: expect.any(Function),
    })
    expect(scope.evaluate(response).names).toEqual([])
  })

  test('a stale outer membership guard prevents installation and discovery', async () => {
    const held = gate()
    const { cluster } = clusterWith([held.promise])
    const scope = new Subscription()
    let current = true
    const onMetadata = jest.fn()
    const pending = cluster.brokerPool.refreshMetadata([], {
      metadataScope: scope,
      isCurrent: () => current,
      onMetadata,
    })
    await Promise.resolve()
    try {
      current = false
      held.release(metadata(['obsolete']))
      await pending
      expect(cluster.brokerPool.metadata).toBeNull()
      expect(cluster.metadataCache.topics.size).toBe(0)
      expect(onMetadata).not.toHaveBeenCalled()
    } finally {
      held.release(metadata(['obsolete']))
      await pending
    }
  })

  test('replacement targets prune both caches and keep prototype-looking names indexed', async () => {
    const { cluster } = clusterWith([metadata(['__proto__', 'gone'])])
    await cluster.brokerPool.refreshMetadata(['__proto__', 'gone'])
    cluster.targetTopics = new Set(['__proto__', 'gone'])
    expect(cluster.findTopicPartitionMetadata('__proto__')).toEqual([])
    await cluster.resetTargetTopics(['__proto__'])
    expect([...cluster.targetTopics]).toEqual(['__proto__'])
    expect(cluster.metadataCache.topics.has('gone')).toBe(false)
    expect(cluster.brokerPool.metadata.topicMetadata.map(({ topic }) => topic)).toEqual([
      '__proto__',
    ])
    expect(cluster.findTopicPartitionMetadata('gone')).toEqual([])
    const before = cluster.brokerPool.metadata
    await cluster.resetTargetTopics([], { isCurrent: () => false })
    expect(cluster.brokerPool.metadata).toBe(before)
    expect([...cluster.targetTopics]).toEqual(['__proto__'])
  })

  test('index follows replacement metadata and preserves first-row and hint references', () => {
    const { cluster } = clusterWith([])
    const first = { partitionId: 0, leader: 1 }
    const second = { partitionId: 0, leader: 2 }
    cluster.brokerPool.metadata = metadata([
      { topic: 'constructor', partitionMetadata: [first] },
      { topic: 'constructor', partitionMetadata: [second] },
    ])
    expect(cluster.findTopicPartitionMetadata('constructor')).toEqual([first])
    first.leader = 3
    expect(cluster.findTopicPartitionMetadata('constructor')[0].leader).toBe(3)
    cluster.brokerPool.metadata = metadata([{ topic: 'constructor', partitionMetadata: [second] }])
    expect(cluster.findTopicPartitionMetadata('constructor')).toEqual([second])
  })

  test('first pattern registration fences an already-started unscoped snapshot pre-step', async () => {
    const held = gate()
    const { cluster } = clusterWith([held.promise])
    const pending = cluster.brokerPool.refreshMetadata(['old'])
    await Promise.resolve()
    try {
      const scope = new Subscription()
      scope.add({ topics: [/orders/], policy: { kind: 'latest' } })
      cluster.useConsumerMetadataScope(scope)
      held.release(metadata(['old']))
      await pending
      expect(cluster.brokerPool.metadata).toBeNull()
      expect(cluster.metadataCache.topics.size).toBe(0)
      const current = metadata(['orders-1'])
      cluster.brokerPool.seedBroker.metadata.mockImplementationOnce(async () => current)
      await cluster.brokerPool.refreshMetadata(['old'])
      expect(cluster.brokerPool.seedBroker.metadata).toHaveBeenLastCalledWith([], {
        allowAutoTopicCreation: false,
        guard: expect.any(Function),
      })
      expect(cluster.brokerPool.metadata).toBe(current)
      cluster.releaseConsumerMetadataScope({})
      expect(cluster.brokerPool.consumerMetadataScope).toBe(scope)
      cluster.releaseConsumerMetadataScope(scope)
      expect(cluster.brokerPool.consumerMetadataScope).toBeNull()
    } finally {
      held.release(metadata(['old']))
      await pending
    }
  })
})

describe('discovery refreshes and their scope', () => {
  test('a newer failed refresh never authorizes an older full reply', async () => {
    const held = gate()
    const newer = metadata(['new-topic'])
    const boom = new Error('metadata unavailable')
    const { cluster } = clusterWith([held.promise, failing(boom), newer])
    const scope = new Subscription()
    const onMetadata = jest.fn()
    const first = cluster.brokerPool.refreshMetadata([], { metadataScope: scope, onMetadata })
    await Promise.resolve()
    try {
      await expect(
        cluster.brokerPool.refreshMetadata([], { metadataScope: scope, onMetadata })
      ).rejects.toThrow('metadata unavailable')
      held.release(metadata(['old-topic']))
      await first
      expect(cluster.brokerPool.metadata).toBeNull()
      expect(cluster.metadataCache.topics.size).toBe(0)
      expect(onMetadata).not.toHaveBeenCalled()

      await cluster.brokerPool.refreshMetadata([], { metadataScope: scope, onMetadata })
      expect(cluster.brokerPool.metadata).toBe(newer)
      expect(onMetadata).toHaveBeenCalledTimes(1)
    } finally {
      held.release(metadata(['old-topic']))
      await first
    }
  })

  test('target replacement waits for the lock and refuses a retired scope', async () => {
    const { cluster, broker } = clusterWith([metadata(['keep', 'drop'])])
    await cluster.brokerPool.refreshMetadata(['keep', 'drop'])
    cluster.targetTopics = new Set(['keep', 'drop'])
    const scope = new Subscription()
    const isCurrent = scope.captureScope()
    await cluster.mutatingTargetTopics.acquire()
    const refused = cluster.resetTargetTopics(['keep'], { isCurrent })
    scope.retireMetadata()
    await cluster.mutatingTargetTopics.release()
    await refused

    expect([...cluster.targetTopics]).toEqual(['keep', 'drop'])
    expect(cluster.metadataCache.topics.has('drop')).toBe(true)
    expect(cluster.findTopicPartitionMetadata('drop')).toEqual([])
    expect(broker.metadata).toHaveBeenCalledTimes(1)

    await cluster.resetTargetTopics(['keep'], { isCurrent: scope.captureScope() })
    expect([...cluster.targetTopics]).toEqual(['keep'])
    expect(cluster.metadataCache.topics.has('drop')).toBe(false)
    expect(broker.metadata).toHaveBeenCalledTimes(1)
  })

  describe('concurrent refreshes join only within one subscription key', () => {
    const closures = [
      ['refreshMetadata', 'refreshMetadataIfNecessary'],
      ['refreshMetadataIfNecessary', 'refreshMetadata'],
    ]

    test.each(closures)('through %s', async (method, other) => {
      const gates = [gate(), gate(), gate(), gate(), gate(), gate()]
      const { cluster, broker } = clusterWith(gates.map(({ promise }) => promise))
      const scope = new Subscription()
      const onMetadata = jest.fn()
      const call = (name, options) =>
        cluster[name]({ metadataScope: scope, onMetadata, ...options })
      const settled = []
      try {
        settled.push(call(method), call(method))
        await new Promise(setImmediate)
        expect(broker.metadata).toHaveBeenCalledTimes(1)

        settled.push(call(other))
        await new Promise(setImmediate)
        expect(broker.metadata).toHaveBeenCalledTimes(2)

        scope.add({ topics: ['literal'], policy: { kind: 'earliest' } })
        settled.push(call(method))
        await new Promise(setImmediate)
        expect(broker.metadata).toHaveBeenCalledTimes(3)

        scope.retireMetadata()
        settled.push(call(method))
        await new Promise(setImmediate)
        expect(broker.metadata).toHaveBeenCalledTimes(4)

        settled.push(call(method, { metadataScope: new Subscription() }))
        await new Promise(setImmediate)
        expect(broker.metadata).toHaveBeenCalledTimes(5)

        // Only the reply of the request issued after the last declaration and retirement installs.
        for (const { release } of gates.slice(0, 3)) release(metadata(['stale']))
        await Promise.all(settled.slice(0, 4))
        expect(cluster.brokerPool.metadata).toBeNull()
        expect(onMetadata).not.toHaveBeenCalled()
        gates[3].release(metadata(['current']))
        await settled[4]
        expect(cluster.brokerPool.metadata.topicMetadata[0].topic).toBe('current')
        expect(onMetadata).toHaveBeenCalledTimes(1)
      } finally {
        for (const { release } of gates) release(metadata(['stale']))
        await Promise.allSettled(settled)
      }

      await Promise.allSettled(settled)
      const before = broker.metadata.mock.calls.length
      cluster.brokerPool.metadataExpireAt = 0
      broker.metadata.mockImplementationOnce(async () => metadata(['fresh']))
      await call(method)
      expect(broker.metadata).toHaveBeenCalledTimes(before + 1)
    })

    test('unscoped calls do not join across a registration or a release', async () => {
      const gates = [gate(), gate(), gate()]
      const { cluster, broker } = clusterWith(gates.map(({ promise }) => promise))
      const scope = new Subscription()
      const settled = []
      try {
        settled.push(cluster.refreshMetadata(), cluster.refreshMetadata())
        await new Promise(setImmediate)
        expect(broker.metadata).toHaveBeenCalledTimes(1)

        cluster.useConsumerMetadataScope(scope)
        settled.push(cluster.refreshMetadata())
        await new Promise(setImmediate)
        expect(broker.metadata).toHaveBeenCalledTimes(2)

        cluster.releaseConsumerMetadataScope(scope)
        settled.push(cluster.refreshMetadata())
        await new Promise(setImmediate)
        expect(broker.metadata).toHaveBeenCalledTimes(3)
      } finally {
        for (const { release } of gates) release(metadata(['stale']))
        await Promise.allSettled(settled)
      }
    })
  })

  test('zero matches discover a later topic at the next eligible opportunity', async () => {
    const harness = createPatternGroup({ catalog: {} })
    const { group } = harness
    const requestRejoin = jest.fn()
    group.rebalanceCoordinator = { isRebalancing: () => false, requestRejoin }
    await harness.refresh()
    await harness.round()
    expect(harness.rebalances).toEqual([[]])
    expect(group.subscriptionPending()).toBe(false)

    harness.set('orders-1', ID_A)
    const forced = harness.cluster.refreshMetadata.mock.calls.length
    await group.prepareFetch(0, group.idleWait, group.ownership.token())

    expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(forced)
    expect(harness.cluster.refreshMetadataIfNecessary).toHaveBeenLastCalledWith(
      expect.objectContaining({ metadataScope: harness.subscription })
    )
    expect(harness.subscription.desired.names).toEqual(['orders-1'])
    expect(requestRejoin).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'revoked', reason: 'subscription-changed' })
    )

    await harness.round()
    expect(harness.rebalances.at(-1)).toEqual(['orders-1'])
    expect(harness.applied()).toEqual(['orders-1'])
  })

  test('a follower cannot recreate a deleted match through the leader union', async () => {
    const harness = createPatternGroup({ catalog: { 'orders-1': ID_A } })
    const { group, cluster } = harness
    await harness.refresh()
    cluster.groupMetadata = jest.fn(async () => ({
      brokers: [],
      topicMetadata: [{ topic: 'orders-deleted', topicErrorCode: 3, partitionMetadata: [] }],
    }))
    const follower = {
      memberId: 'follower',
      memberMetadata: MemberMetadata.encode({ version: 0, topics: ['orders-deleted'] }),
    }

    const round = group.beginRebalanceRound().attempt()
    const { groupTopics, groupTopicMetadata } = await group.groupMembership.leaderMetadata(
      [follower],
      ['orders-1'],
      round
    )

    expect(cluster.groupMetadata).toHaveBeenCalledTimes(1)
    expect(cluster.groupMetadata).toHaveBeenCalledWith(
      expect.objectContaining({ topics: ['orders-deleted'], allowAutoTopicCreation: false })
    )
    expect(groupTopics).toEqual(['orders-1', 'orders-deleted'])
    expect(groupTopicMetadata.has('orders-deleted')).toBe(false)
    expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
    expect(group.topicsSubscribed).toEqual([])

    const literal = createPatternGroup({ catalog: {} })
    const literalGroup = literal.group
    literalGroup.subscription = null
    literal.cluster.allowAutoTopicCreation = true
    literal.cluster.groupMetadata = cluster.groupMetadata
    cluster.groupMetadata.mockClear()
    const literalRound = literalGroup.beginRebalanceRound().attempt()
    await literalGroup.groupMembership.leaderMetadata([follower], ['orders-1'], literalRound)
    expect(cluster.groupMetadata).toHaveBeenCalledWith(
      expect.objectContaining({ allowAutoTopicCreation: true })
    )
  })

  test('a changed desired list during heartbeat encoding is sent on the following tick', () => {
    const protocol = new ConsumerGroupProtocol({
      cluster: {},
      groupId: 'group-a',
      rebalanceTimeout: 30000,
      topics: ['orders-a'],
      logger: newLogger(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })
    protocol.memberEpoch = 1

    const captured = buildRequest(protocol, { sendAllFields: true })
    protocol.updateSubscription({ names: ['orders-b'], revision: 2 })
    expect(captured.SubscribedTopicNames).toEqual(['orders-a'])
    protocol.sentFields.remember(captured)
    expect(protocol.sentFields.subscribedTopicNames).toEqual(['orders-a'])

    expect(buildRequest(protocol).SubscribedTopicNames).toEqual(['orders-b'])
    const leave = buildRequest(protocol, { leaveEpoch: -1 })
    expect(leave.SubscribedTopicNames).toBeUndefined()
    expect(leave.RebalanceTimeoutMs).toBeUndefined()
  })
})

/**
 * The fault-free scenario on the scripted wire: a pattern consumer joins with the one topic that
 * matches, a second matching topic appears in the next Metadata answers, and the next metadata
 * refresh opportunity makes it rejoin with both names. Every request is an answer the scripted
 * cluster expected; nothing real is started.
 * @returns {Promise<void>}
 */
const scriptedDiscovery = async () => {
  const names = ['orders-1']
  const seam = groupScript(createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] }))
  seam
    .answer('Metadata', () => ({
      ...metadataV13.response,
      brokers: [{ nodeId: 0, host: 'node0', port: 9092, rack: null }],
      controllerId: 0,
      topics: names.map((name, index) => ({
        ...metadataV13.response.topics[0],
        name,
        topicId: `01020304-0506-0708-090a-0b0c0d0e0f1${index}`,
        partitions: [],
      })),
    }))
    .answer('Fetch', async ({ maxWaitMs }) => {
      await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      return { partitions: [] }
    })
    .answer('SyncGroup', () => ({
      memberAssignment: MemberAssignment.encode({ version: 1, assignment: {} }).toString('hex'),
    }))
  const consumer = new Kafka(seam.kafkaOptions()).consumer({
    groupId: 'scripted-discovery',
    metadataMaxAge: 500,
    maxWaitTimeInMs: 100,
  })
  const reached = (condition) =>
    waitFor(condition, { maxWait: 5000 }).then(
      () => true,
      () => false
    )
  const joins = () => seam.requests().filter(({ api }) => api === 'JoinGroup')
  const joined = (join) =>
    MemberMetadata.decode(Buffer.from(join.fields.groupProtocols[0].metadata, 'hex')).topics.sort()
  try {
    await consumer.connect()
    await consumer.subscribe({ topics: [/^orders-/] })
    const subscriptionMetadata = seam
      .requests()
      .filter(({ api }) => api === 'Metadata')
      .at(-1)
    expect(subscriptionMetadata.fields.topics).toBeNull()
    expect(subscriptionMetadata.fields.allowAutoTopicCreation).toBe(false)
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    expect(await reached(() => joins().length === 1)).toBe(true)
    expect(joined(joins()[0])).toEqual(['orders-1'])

    names.push('orders-2')
    expect(await reached(() => joins().length === 2)).toBe(true)
    expect(joined(joins()[1])).toEqual(['orders-1', 'orders-2'])
    seam.assertClean()
  } finally {
    await consumer.disconnect()
  }
}

/**
 * The same scenario on a real cluster, through the public clients: a topic created after the
 * subscription is joined and delivered within the unblocked-fixture bound.
 * @param {AbortSignal} scope the case scope
 * @param {CleanupStack} cleanup run by the hook after the case
 * @returns {Promise<void>}
 */
const brokerDiscovery = async (scope, cleanup) => {
  const id = secureRandom()
  const prefix = `pattern-lane-${id}`
  const kafka = new Kafka({
    clientId: prefix,
    brokers: plainTextBrokers(),
    logLevel: logLevel.NOTHING,
  })
  const producer = kafka.producer()
  const consumer = kafka.consumer({ groupId: `${prefix}-group`, metadataMaxAge: 5000 })
  const delivered = new Set()
  cleanup.add('consumer', () => consumer.disconnect())
  cleanup.add('producer', () => producer.disconnect())
  await producer.connect()
  await createTopic({ topic: `${prefix}-a`, partitions: 1 })
  await consumer.connect()
  await consumer.subscribe({ topics: [new RegExp(`^${prefix}-`)], fromBeginning: true })
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, message }) => delivered.add(`${topic}|${message.value}`),
  })
  await producer.send({ topic: `${prefix}-a`, messages: [{ value: 'first' }] })
  await pollUntil(scope, 30000, () => delivered.has(`${prefix}-a|first`), { label: 'The first' })

  await createTopic({ topic: `${prefix}-b`, partitions: 1 })
  await producer.send({ topic: `${prefix}-b`, messages: [{ value: 'later' }] })
  await pollUntil(scope, 20000, () => delivered.has(`${prefix}-b|later`), { label: 'The later' })
}

describe.each(lanes())('a topic created after a pattern subscription on the %s lane', (lane) => {
  let cleanup

  beforeEach(() => {
    cleanup = new CleanupStack()
  })

  afterEach(async () => {
    if (lane === 'docker' && liveRun.takeAdmission()) {
      await cleanup.run(async () => {}, { budgetMs: 30000 })
    }
  }, 32000)

  if (lane === 'fake') {
    test('joins again with both names at the next metadata refresh', scriptedDiscovery)
  } else {
    test(
      'is joined and delivered within the discovery bound',
      liveCase(60000, (scope) => brokerDiscovery(scope, cleanup)),
      caseTimeout(60000)
    )
  }
})
