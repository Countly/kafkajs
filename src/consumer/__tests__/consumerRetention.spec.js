const ConsumerGroup = require('../consumerGroup')
const ConsumerProtocol = require('../groupProtocol/consumer')
const SubscriptionState = require('../subscriptionState')
const SeekOffsets = require('../seekOffsets')
const Subscription = require('../subscription')
const { FetchSessions } = require('../fetchSession')
const dictionary = require('../../utils/dictionary')
const ResetObligations = require('../offsetManager/resetObligations')
const { createFetchGroup, TOPIC } = require('./fixtures/fetchHarness')

describe.each(['classic', 'consumer'])('%s retention core', (kind) => {
  it('bounds pause, configuration, replica, grown-name and queued-seek state over churn', () => {
    const group = Object.create(ConsumerGroup.prototype)
    const protocol = Object.create(ConsumerProtocol.prototype)
    protocol.subscribedTopicNames = []
    protocol.grownTopicNames = new Set()
    protocol.assignedTopicNames = new Map()
    protocol.unresolvedTopicIds = new Set()
    protocol.unresolvedAttempts = new Map()
    group.groupMembership = kind === 'consumer' ? protocol : { kind }
    group.subscribedTopicRegex = kind === 'consumer' ? '.*' : null
    group.subscriptionState = new SubscriptionState()
    group.topicConfigurations = Object.create(null)
    group.preferredReadReplicasPerTopicPartition = Object.create(null)
    group.seekOffset = new SeekOffsets()
    for (let cycle = 0; cycle < 500; cycle++) {
      const names = Array.from({ length: 50 }, (_, n) => `orders-${cycle}-${n}`)
      for (const topic of names) {
        group.topicConfigurations[topic] = {}
        group.preferredReadReplicasPerTopicPartition[topic] = {
          0: { nodeId: 1 },
        }
        group.seekOffset.set(topic, 0, '1')
        group.subscriptionState.pause([{ topic }])
        protocol.grownTopicNames.add(topic)
      }
      group.retainSubscription(names)
      group.subscriptionState.assign(names.map((topic) => ({ topic, partitions: [0] })))
      group.pruneAssignmentState()
      expect(Object.keys(group.topicConfigurations)).toHaveLength(50)
      expect(Object.keys(group.subscriptionState.subscriptionStatesByTopic)).toHaveLength(50)
      expect(Object.keys(group.preferredReadReplicasPerTopicPartition)).toHaveLength(50)
      expect(group.seekOffset.size).toBe(50)
      if (kind === 'consumer') expect(protocol.grownTopicNames.size).toBe(50)
      protocol.target = { byTopicId: new Map() }
      protocol.adoptLocalAssignment(cycle, { byTopicId: new Map() })
      group.retainSubscription([])
      group.subscriptionState.assign([])
      group.pruneAssignmentState()
      expect(Object.keys(group.topicConfigurations)).toHaveLength(0)
      expect(Object.keys(group.subscriptionState.subscriptionStatesByTopic)).toHaveLength(0)
      expect(Object.keys(group.preferredReadReplicasPerTopicPartition)).toHaveLength(0)
      expect(group.seekOffset.size).toBe(0)
      expect(protocol.grownTopicNames.size).toBe(0)
    }
  })

  it('every retained consumer structure follows its live owners after repeated replacement', () => {
    const group = Object.create(ConsumerGroup.prototype)
    const protocol = Object.create(ConsumerProtocol.prototype)
    protocol.subscribedTopicNames = []
    protocol.grownTopicNames = new Set()
    protocol.assignedTopicNames = new Map()
    protocol.unresolvedTopicIds = new Set()
    protocol.unresolvedAttempts = new Map()
    group.groupMembership = kind === 'consumer' ? protocol : { kind }
    group.subscribedTopicRegex = '^orders-'
    group.subscription = new Subscription()
    group.subscription.add({ topics: [/^orders-/], policy: { kind: 'earliest' } })
    group.subscriptionState = new SubscriptionState()
    group.topicConfigurations = Object.create(null)
    group.preferredReadReplicasPerTopicPartition = Object.create(null)
    group.seekOffset = new SeekOffsets()
    group.obligations = new ResetObligations()
    group.offsetManager = null
    const sessions = new FetchSessions({ sendClose: async () => {}, requestTimeout: 1, logger: {} })
    const metadataOf = (names, cycle) => ({
      topicMetadata: names.map((topic, n) => ({ topic, topicId: `id-${cycle}-${n}` })),
    })
    const publish = (names, cycle) => {
      const metadata = metadataOf(names, cycle)
      group.subscription.evaluate(metadata)
      for (const { topic, topicId } of group.subscription.observe(metadata)) {
        group.obligations.owe(topic, topicId)
      }
      group.publishSubscription(group.subscription.pin())
    }
    for (let cycle = 0; cycle < 500; cycle++) {
      const names = Array.from({ length: 50 }, (_, n) => `orders-${cycle}-${n}`)
      publish(names, cycle)
      for (const topic of names) {
        group.preferredReadReplicasPerTopicPartition[topic] = { 0: { nodeId: 1 } }
        group.seekOffset.set(topic, 0, '1')
        group.subscriptionState.pause([{ topic }])
        if (kind === 'consumer') protocol.grownTopicNames.add(topic)
      }
      group.subscriptionState.assign(names.map((topic) => ({ topic, partitions: [0] })))
      group.pruneAssignmentState()
      const nodes = [cycle * 3, cycle * 3 + 1, cycle * 3 + 2]
      for (const node of nodes) sessions.forNode(node)
      sessions.retain(nodes)

      expect(group.subscription.desired.names).toHaveLength(50)
      expect(group.subscription.applied.names).toHaveLength(50)
      expect(group.subscription.observed.size).toBe(50)
      expect(Object.keys(group.topicConfigurations)).toHaveLength(50)
      expect(Object.keys(group.subscriptionState.subscriptionStatesByTopic)).toHaveLength(50)
      expect(Object.keys(group.preferredReadReplicasPerTopicPartition)).toHaveLength(50)
      expect(group.seekOffset.size).toBe(50)
      expect(group.obligations.size()).toBeLessThanOrEqual(50)
      expect(sessions.sessions.size).toBe(3)
      if (kind === 'consumer') expect(protocol.grownTopicNames.size).toBe(50)
      if (kind === 'consumer') protocol.grownTopicNames.clear()

      group.subscriptionState.assign([])
      publish([], cycle)
      group.pruneAssignmentState()
      expect(group.subscription.desired.names).toHaveLength(0)
      expect(group.subscription.applied.names).toHaveLength(0)
      expect(group.subscription.observed.size).toBe(0)
      expect(Object.keys(group.topicConfigurations)).toHaveLength(0)
      expect(Object.keys(group.subscriptionState.subscriptionStatesByTopic)).toHaveLength(0)
      expect(Object.keys(group.preferredReadReplicasPerTopicPartition)).toHaveLength(0)
      expect(group.seekOffset.size).toBe(0)
      expect(group.obligations.size()).toBe(0)
    }
    sessions.retireAll()
    expect(sessions.sessions.size).toBe(0)
  })
})

jest.mock('../index', () => jest.fn())
jest.mock('../disposal', () => ({ disposeConsumer: jest.fn(async () => {}) }))
const createConsumer = require('../index')
const createAdmin = require('../../admin')
const Cluster = require('../../cluster')
const GroupRegistry = require('../../cluster/groupRegistry')
const { EventEmitter } = require('events')

/** @returns {object} actual shared registry and public admin with no broker operations */
const adminRegistry = (
  failure = null,
  { offsets, operations } = { offsets: new Map(), operations: new Map() }
) => {
  const cluster = Object.create(Cluster.prototype)
  cluster.groupRegistry = GroupRegistry.forStores(offsets, operations)
  cluster.groupEntries = new Map()
  cluster.groupScopes = new Map()
  cluster.brokerPool = { disconnect: jest.fn(async () => {}) }
  cluster.findTopicPartitionMetadata = () => [{ partitionId: 0 }]
  cluster.connect = async () => {}
  cluster.addTargetTopic = async () => {}
  cluster.refreshMetadataIfNecessary = async () => {}
  cluster.defaultOffset = ({ fromBeginning }) => (fromBeginning ? -2 : -1)
  cluster.fetchTopicsOffset = async ([{ topic, partitions }]) => [
    { topic, partitions: partitions.map(({ partition }) => ({ partition, offset: '5' })) },
  ]
  cluster.findGroupCoordinator = async () => ({
    offsetFetch: async () => ({
      responses: [{ topic: 'orders', partitions: [{ partition: 0, offset: '-1' }] }],
    }),
    offsetCommit: async () => ({}),
  })
  const logger = {
    namespace() {
      return this
    },
    debug() {},
    info() {},
    warn() {},
  }
  const admin = createAdmin({ cluster, logger })
  createConsumer.mockImplementation(({ groupId }) => {
    cluster.offsetOperations({ groupId })
    const events = new EventEmitter()
    return {
      events: { GROUP_JOIN: 'join', FETCH: 'fetch' },
      groupMetadata: () => ({ generationId: 1, memberId: 'member', groupInstanceId: null }),
      on: (event, handler) => events.on(event, handler),
      subscribe: async () => {
        if (failure) throw failure
      },
      describeGroup: async () => ({ state: 'Empty' }),
      pause() {},
      seek() {},
      stop: async () => {},
      run: async () => {
        queueMicrotask(() => {
          events.emit('join')
          events.emit('fetch')
        })
      },
    }
  })
  return { cluster, admin, offsets, operations }
}

describe('admin registry operation lifetime', () => {
  afterEach(() => jest.clearAllMocks())
  it('admin operations release many-group registry state before disconnect', async () => {
    const { cluster, admin, offsets, operations } = adminRegistry()
    try {
      for (let n = 0; n < 500; n++) {
        await admin.setOffsets({
          groupId: `group-${n}`,
          topic: 'orders',
          partitions: [{ partition: 0, offset: '0' }],
        })
      }
      expect(cluster.groupEntries.size).toBe(0)
      expect(cluster.groupRegistry.entries.size).toBe(0)
      expect(offsets.size).toBe(0)
      expect(operations.size).toBe(0)
    } finally {
      await admin.disconnect()
    }
    expect(cluster.groupEntries.size).toBe(0)
    expect(cluster.groupRegistry.entries.size).toBe(0)
  })
  it('releases fifty failed public operations without masking their error', async () => {
    const failure = new Error('subscription failed')
    const { cluster, admin } = adminRegistry(failure)
    for (let n = 0; n < 50; n++) {
      await expect(
        admin.setOffsets({
          groupId: `failed-${n}`,
          topic: 'orders',
          partitions: [{ partition: 0, offset: '0' }],
        })
      ).rejects.toBe(failure)
    }
    expect(cluster.groupScopes.size).toBe(0)
    expect(cluster.groupEntries.size).toBe(0)
    expect(cluster.groupRegistry.entries.size).toBe(0)
    cluster.brokerPool.disconnect.mockRejectedValue(failure)
    await expect(admin.disconnect()).rejects.toBe(failure)
    expect(cluster.groupRegistry.entries.size).toBe(0)
  })
  it('keeps a concurrent holder and pending ticket until each actual owner retires', async () => {
    const { cluster, offsets, operations } = adminRegistry()
    const other = {}
    const shared = cluster.groupRegistry.acquire('group', other)
    let ticket
    await cluster.withGroup('group', async () => {
      const ledger = cluster.offsetOperations({ groupId: 'group' })
      expect(ledger).toBe(shared.ledger)
      ticket = ledger.begin([{ topic: 'orders', partition: 0 }])
    })
    expect(cluster.groupEntries.size).toBe(0)
    expect(cluster.groupRegistry.entries.size).toBe(1)
    cluster.groupRegistry.release('group', other)
    expect(cluster.groupRegistry.entries.size).toBe(1)
    shared.ledger.end(ticket)
    expect(cluster.groupRegistry.entries.size).toBe(0)
    expect(offsets.size).toBe(0)
    expect(operations.size).toBe(0)
  })
  it('releases a failed scope and shares overlapping scopes without early eviction', async () => {
    const { cluster } = adminRegistry()
    const original = new Error('offset operation failed')
    await expect(
      cluster.withGroup('failed', async () => {
        throw original
      })
    ).rejects.toBe(original)
    expect(cluster.groupRegistry.entries.size).toBe(0)
    let release
    const held = new Promise((resolve) => {
      release = resolve
    })
    let firstLedger
    const first = cluster.withGroup('same', async () => {
      firstLedger = cluster.offsetOperations({ groupId: 'same' })
      await held
    })
    await cluster.withGroup('same', async () => {
      expect(cluster.offsetOperations({ groupId: 'same' })).toBe(firstLedger)
    })
    expect(cluster.groupRegistry.entries.size).toBe(1)
    release()
    await first
    expect(cluster.groupScopes.size).toBe(0)
    expect(cluster.groupEntries.size).toBe(0)
    expect(cluster.groupRegistry.entries.size).toBe(0)
  })
})

describe('independent holders and durable reset debt', () => {
  afterEach(() => jest.clearAllMocks())
  it('independent holders and durable reset debt outlive only their actual work', async () => {
    const { cluster, admin, offsets, operations } = adminRegistry()
    const registry = cluster.groupRegistry
    const run = {
      setOffsets: (groupId) =>
        admin.setOffsets({ groupId, topic: 'orders', partitions: [{ partition: 0, offset: '0' }] }),
      resetOffsets: (groupId) => admin.resetOffsets({ groupId, topic: 'orders', earliest: true }),
      fetchOffsets: (groupId) => admin.fetchOffsets({ groupId, topics: ['orders'] }),
    }
    for (const operation of Object.keys(run)) {
      await run[operation](`idle-${operation}`)
      expect(cluster.groupEntries.size).toBe(0)
      expect(cluster.groupScopes.size).toBe(0)
      expect(registry.entries.size).toBe(0)
    }

    const consumer = {}
    const producer = {}
    const shared = registry.acquire('shared', consumer)
    registry.acquire('shared', producer)
    const empty = shared.ledger.begin([])
    const debt = new ResetObligations()
    debt.owe('orders', 'new-incarnation')
    debt.attachTo(shared.ledger)
    await Promise.all([run.setOffsets('shared'), run.setOffsets('other')])
    expect(registry.entries.has('other')).toBe(false)
    expect(registry.entries.get('shared').holders.size).toBe(2)
    registry.release('shared', consumer)
    expect(registry.entries.has('shared')).toBe(true)
    registry.release('shared', producer)
    expect(registry.entries.has('shared')).toBe(true)
    shared.ledger.end(empty)
    debt.unlink()
    expect(registry.entries.has('shared')).toBe(false)
    expect(debt.isOwed('orders', 0)).toBe(true)
    expect(offsets.size).toBe(0)
    expect(operations.size).toBe(0)

    const failure = new Error('subscription failed')
    const failing = adminRegistry(failure)
    await expect(
      failing.admin.setOffsets({
        groupId: 'failed',
        topic: 'orders',
        partitions: [{ partition: 0, offset: '0' }],
      })
    ).rejects.toBe(failure)
    expect(failing.cluster.groupRegistry.entries.size).toBe(0)
    await admin.disconnect()
    await failing.admin.disconnect()
    expect(registry.entries.size).toBe(0)
    expect(cluster.groupEntries.size).toBe(0)
  })
})

describe('recreation debt', () => {
  it('recreation debt survives stop and partial assignment until a confirmed commit', () => {
    const debt = new ResetObligations()
    const known = [0, 1, 2]
    debt.owe('orders', 'new-id')
    debt.unlink()
    expect(debt.isOwed('orders', 0)).toBe(true)

    debt.settle('orders', [], known)
    debt.settle('orders', [0], undefined)
    expect(debt.size()).toBe(1)
    debt.discharge('orders', 0, 'stale-id')
    expect(debt.isOwed('orders', 0)).toBe(true)
    expect(debt.current('orders', undefined)).toBe('new-id')

    debt.discharge('orders', 0, 'new-id')
    debt.settle('orders', [0], known)
    expect(debt.size()).toBe(1)
    expect(debt.isOwed('orders', 1)).toBe(true)
    debt.settle('orders', [], [])
    expect(debt.size()).toBe(1)

    debt.discharge('orders', 1, 'new-id')
    debt.discharge('orders', 2)
    debt.settle('orders', [0, 1, 2], known)
    expect(debt.size()).toBe(0)
    expect(debt.isOwed('orders', 0)).toBe(false)
  })
  it('owes a newer incarnation after an ambiguous or recreated answer', () => {
    const debt = new ResetObligations()
    debt.owe('orders', 'first')
    expect(debt.current('orders', 'second')).toBeUndefined()
    expect(debt.size()).toBe(0)
    debt.owe('orders', 'second')
    debt.discharge('orders', 0, 'second')
    debt.owe('orders', 'third')
    expect(debt.isOwed('orders', 0)).toBe(true)
  })
})

describe('retention on the real apply paths', () => {
  let h
  afterEach(async () => {
    if (h) await h.broker.disconnect()
    h = null
  })

  describe.each([true, false])(
    'both assignment branches drop orphan seeks and replicas',
    (rebuild) => {
      it(`rebuildOffsets ${rebuild}`, async () => {
        h = await createFetchGroup({ partitions: [0, 1] })
        const { group } = h
        group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()
        await group.applyRebalanceResult({ assignment: { [TOPIC]: [0, 1] } })
        group.pause([{ topic: TOPIC, partitions: [0] }])
        group.seek({ topic: TOPIC, partition: 0, offset: '3' })
        group.seek({ topic: TOPIC, partition: 1, offset: '4' })
        group.preferredReadReplicasPerTopicPartition.gone = { 0: { nodeId: 1 } }
        group.preferredReadReplicasPerTopicPartition[TOPIC] = { 0: { nodeId: 1 } }

        await group.applyRebalanceResult(
          { assignment: { [TOPIC]: [0] } },
          { rebuildOffsets: rebuild }
        )

        expect(group.seekOffset.has(TOPIC, 0)).toBe(true)
        expect(group.seekOffset.has(TOPIC, 1)).toBe(false)
        expect(Object.keys(group.preferredReadReplicasPerTopicPartition)).toEqual([TOPIC])
        expect(group.subscriptionState.isPaused(TOPIC, 0)).toBe(true)
      })
    }
  )

  it('five hundred paused removed names leave only the live subscription', () => {
    const names = Array.from({ length: 500 }, (_, n) => `removed-${n}`)
    const topicConfigurations = dictionary([...names, '__proto__'].map((name) => [name, {}]))
    const group = Object.create(ConsumerGroup.prototype)
    group.groupMembership = { kind: 'classic' }
    group.topicConfigurations = topicConfigurations
    group.subscriptionState = new SubscriptionState()
    group.preferredReadReplicasPerTopicPartition = dictionary()
    group.seekOffset = new SeekOffsets()
    group.subscriptionState.pause([...names, '__proto__'].map((topic) => ({ topic })))

    group.retainSubscription(['__proto__'])
    expect(Object.keys(group.subscriptionState.subscriptionStatesByTopic)).toEqual(['__proto__'])
    expect(Object.keys(group.topicConfigurations)).toEqual(['__proto__'])

    group.subscriptionState.assign([{ topic: '__proto__', partitions: [0] }])
    group.subscriptionState.assign([])
    group.pruneAssignmentState()
    expect(Object.keys(group.subscriptionState.subscriptionStatesByTopic)).toEqual(['__proto__'])
  })
})

describe('completed admin offset operations', () => {
  afterEach(() => jest.clearAllMocks())

  /** @returns {Promise<void>} runs the three public operations that end in setOffsets */
  const operations = async (admin, groupId) => {
    await admin.setOffsets({
      groupId,
      topic: 'orders',
      partitions: [{ partition: 0, offset: '0' }],
    })
    await admin.resetOffsets({ groupId, topic: 'orders', earliest: true })
    await admin.fetchOffsets({ groupId, topics: ['orders'], resolveOffsets: true })
  }

  const expectNoHolders = ({ cluster, offsets, operations: ledgers }) => {
    expect(cluster.groupScopes.size).toBe(0)
    expect(cluster.groupEntries.size).toBe(0)
    expect(cluster.groupRegistry.entries.size).toBe(0)
    expect(offsets.size).toBe(0)
    expect(ledgers.size).toBe(0)
  }

  it('completed admin offset operations retain no historical group holders', async () => {
    const seam = adminRegistry()
    const { admin, cluster } = seam
    for (let n = 0; n < 500; n++) await operations(admin, `group-${n}`)
    expectNoHolders(seam)
    await admin.disconnect()
    await admin.connect()
    for (let n = 0; n < 50; n++) await operations(admin, `again-${n}`)
    expectNoHolders(seam)
    expect(cluster.brokerPool.disconnect).toHaveBeenCalledTimes(1)

    const failure = new Error('subscription failed')
    const failing = adminRegistry(failure)
    for (let n = 0; n < 50; n++) {
      await expect(
        failing.admin.resetOffsets({ groupId: `failed-${n}`, topic: 'orders' })
      ).rejects.toBe(failure)
    }
    expectNoHolders(failing)
  })

  it('admin teardown preserves other owners and retires its own holders', async () => {
    const stores = { offsets: new Map(), operations: new Map() }
    const { cluster, admin } = adminRegistry(null, stores)
    const { cluster: other } = adminRegistry(null, stores)
    const failure = new Error('offset work failed')
    const registry = cluster.groupRegistry
    const pair = other.groupEntry('group')
    const holders = () => [...registry.entries.get('group').holders]

    await admin.setOffsets({
      groupId: 'group',
      topic: 'orders',
      partitions: [{ partition: 0, offset: '0' }],
    })
    await cluster.withGroup('group', async () => {
      expect(cluster.offsetOperations({ groupId: 'group' })).toBe(pair.ledger)
      await cluster.withGroup('group', async () => {
        expect(cluster.groupScopes.get('group').count).toBe(2)
      })
      expect(cluster.groupScopes.get('group').count).toBe(1)
    })
    await expect(
      cluster.withGroup('group', async () => {
        throw failure
      })
    ).rejects.toBe(failure)
    expect(cluster.groupScopes.size).toBe(0)
    expect(cluster.groupEntries.size).toBe(0)
    expect(registry.entries.get('group')).toBe(pair)
    expect(holders()).toEqual([other])

    const empty = pair.ledger.begin([])
    const ticket = pair.ledger.begin([{ topic: 'orders', partition: 0 }])
    const detach = pair.ledger.attach(new ResetObligations())
    await other.disconnect()
    expect(registry.entries.get('group')).toBe(pair)
    pair.ledger.end(empty)
    pair.ledger.end(ticket)
    expect(registry.entries.get('group')).toBe(pair)
    detach()
    expect(registry.entries.size).toBe(0)
    expect(stores.offsets.size).toBe(0)
    expect(stores.operations.size).toBe(0)

    other.groupEntry('later')
    other.brokerPool.disconnect.mockRejectedValue(failure)
    await expect(other.disconnect()).rejects.toBe(failure)
    expect(other.groupEntries.size).toBe(0)
    expect(registry.entries.size).toBe(0)
  })
})
