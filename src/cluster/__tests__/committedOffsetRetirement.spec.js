const Cluster = require('../index')
const GroupRegistry = require('../groupRegistry')
const CommittedOffsetRevisions = require('../committedOffsetRevisions')
const createConsumer = require('../../consumer/index')
const ConsumerGroup = require('../../consumer/consumerGroup')
const GroupOwnership = require('../../consumer/groupOwnership')
const Runner = require('../../consumer/runner')
const OffsetManager = require('../../consumer/offsetManager')
const ResetObligations = require('../../consumer/offsetManager/resetObligations')
const TransactionOffsets = require('../../producer/state/transactionOffsets')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../instrumentationEvents')
const { KafkaJSCommitFailedError } = require('../../errors')
const { newLogger } = require('testHelpers')

const GROUP = 'retire-group'
const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'
const EARLIEST = { kind: 'earliest' }
const DURATION = { kind: 'by_duration', durationMs: 1000 }

/** Lets every pending request reach its stub. */
const flush = () => new Promise(setImmediate)

/** A promise and the function that settles it. */
const deferred = () => {
  let resolve
  const promise = new Promise((r) => (resolve = r))
  return { promise, resolve }
}

/**
 * Holds every call of `mock` until the case resolves its gate, then answers it.
 * @param {jest.Mock} mock
 * @param {(index: number, ...args: any[]) => any} answer the call's result, by call order
 * @returns {Array<{ resolve: () => void }>} one gate per call, in call order
 */
const gateCalls = (mock, answer) => {
  const gates = []
  mock.mockImplementation(async (...args) => {
    const index = gates.length
    const gate = deferred()
    gates.push(gate)
    await gate.promise
    return answer(index, ...args)
  })
  return gates
}

/** An OffsetFetch answer committing `offset` on every requested cell. */
const offsetFetchAnswer = (topics, offset) => ({
  responses: topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition }) => ({
      partition,
      offset,
      committedLeaderEpoch: -1,
      metadata: null,
      errorCode: 0,
    })),
  })),
})

/** A ListOffsets answer giving each requested cell `offsetOf(request)`. */
const listOffsetsAnswer = (requests, offsetOf) =>
  requests.map((request) => ({
    topic: request.topic,
    partitions: request.partitions.map(({ partition }) => ({
      partition,
      offset: offsetOf(request),
    })),
  }))

/**
 * One group's shared pair, through the real registry, as every cluster of one `Kafka` sees it.
 * @returns {{ registry: GroupRegistry, cache: object, ledger: object, stamps: () => {topics: number, cells: number} }}
 */
const createGroup = () => {
  const registry = GroupRegistry.forStores(new Map(), new Map())
  const { offsets, ledger } = registry.acquire(GROUP, {})
  return {
    registry,
    cache: offsets,
    ledger,
    stamps: () => CommittedOffsetRevisions.sizeOf(offsets),
  }
}

/** A consumer's table, linked to the group's ledger the way a running group links it. */
const linkedTable = (ledger) => {
  const table = new ResetObligations()
  table.attachTo(ledger)
  return table
}

/**
 * A real `OffsetManager` over scripted answers: OffsetFetch answers `committed(topic, partition)`
 * (none by default) and ListOffsets answers `listed(topic, partition)`. Every answer can be held.
 * @param {object} group the pair from `createGroup`
 * @param {object} options
 * @param {Record<string, number[]>} options.assignment
 * @param {ResetObligations} options.obligations the consumer's table
 * @returns {object} the manager and its stubs
 */
const createManager = (
  { cache, ledger },
  {
    assignment,
    obligations,
    committed = () => '-1',
    listed = () => '5',
    topicIds = {},
    autoCommit = false,
    ownership = null,
    hold = {},
  }
) => {
  const coordinator = {
    isConnected: () => true,
    offsetFetch: jest.fn(async ({ topics }) => {
      if (hold.fetch) await hold.fetch.promise
      return {
        responses: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            offset: committed(topic, partition),
            committedLeaderEpoch: -1,
            metadata: null,
            errorCode: 0,
          })),
        })),
      }
    }),
    offsetCommit: jest.fn(async () => {
      const { commit, commitError } = hold
      if (commit) await commit.promise
      if (commitError) throw commitError
    }),
  }
  const cluster = {
    committedOffsets: () => cache,
    topicId: (topic) => topicIds[topic],
    fetchTopicsOffset: jest.fn(async (requests) => {
      if (hold.list) await hold.list.promise
      return requests.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          offset: listed(topic, partition),
        })),
      }))
    }),
    findGroupCoordinator: jest.fn(async () => coordinator),
    refreshMetadata: jest.fn(async () => {}),
  }
  const topicConfigurations = Object.create(null)
  for (const topic of Object.keys(assignment))
    topicConfigurations[topic] = { resetPolicy: EARLIEST }
  const manager = new OffsetManager({
    cluster,
    coordinator,
    operations: ledger,
    obligations,
    memberAssignment: assignment,
    autoCommit,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    ownership,
    topicConfigurations,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: GROUP,
    generationId: 1,
    memberId: 'member',
    logger: newLogger(),
  })
  return { manager, coordinator, cluster }
}

/**
 * Moves a manager to `next`, revoking every partition it held that `next` does not name.
 * @param {OffsetManager} manager
 * @param {Record<string, number[]>} next
 */
const reassign = (manager, next) => {
  const revoked = Object.entries(manager.memberAssignment)
    .map(([topic, partitions]) => ({
      topic,
      partitions: partitions.filter((partition) => !(next[topic] || []).includes(partition)),
    }))
    .filter(({ partitions }) => partitions.length > 0)
  for (const topic of Object.keys(next)) {
    manager.topicConfigurations[topic] ??= { resetPolicy: EARLIEST }
  }
  manager.updateAssignment({
    revoked,
    memberAssignment: next,
    generationId: manager.generationId + 1,
    memberId: 'member',
    coordinator: manager.coordinator,
  })
}

/** The stamp a manager's private position on a cell carries. */
const stampOf = (manager, topic, partition) => manager.resetPositions[topic]?.[partition]?.revision

const has = (object, key) => Object.prototype.hasOwnProperty.call(object, key)

describe('Cluster > committed offset retirement', () => {
  describe('contract', () => {
    test('2000 retired topics leave no topic stamps, cells or cache entries', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      const { manager } = createManager(group, { assignment: { anchor: [0] }, obligations: table })
      await manager.resolveOffsets()
      const anchor = stampOf(manager, 'anchor', 0)

      for (let n = 0; n < 2000; n++) {
        const topic = `rotating-${n}`
        reassign(manager, { anchor: [0], [topic]: [0, 1] })
        await manager.resolveOffsets()
        expect(manager.heldOffsetAt(topic, 1)).toBe('5')
        reassign(manager, { anchor: [0] })
      }

      expect(Object.keys(group.cache)).toEqual(['anchor'])
      expect(group.stamps()).toEqual({ topics: 1, cells: 1 })
      expect(stampOf(manager, 'anchor', 0)).toBe(anchor)
    })

    test('a capture from before retirement is stale immediately after retirement', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      const { manager } = createManager(group, {
        assignment: { kept: [0], gone: [0] },
        obligations: table,
      })
      await manager.resolveOffsets()
      const stamp = stampOf(manager, 'gone', 0)
      const revisions = CommittedOffsetRevisions.forCache(group.cache)
      expect(revisions.isCurrent('gone', 0, stamp)).toBe(true)

      reassign(manager, { kept: [0] })

      expect(has(group.cache, 'gone')).toBe(false)
      expect(revisions.isCurrent('gone', 0, stamp)).toBe(false)
      expect(revisions.isCurrent('kept', 0, stampOf(manager, 'kept', 0))).toBe(true)
    })

    test('a capture from before retirement stays stale after re-add', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      let listed = '5'
      const { manager } = createManager(group, {
        assignment: { topic: [0, 1] },
        obligations: table,
        listed: () => listed,
      })
      await manager.resolveOffsets()
      const old = [stampOf(manager, 'topic', 0), stampOf(manager, 'topic', 1)]
      const revisions = CommittedOffsetRevisions.forCache(group.cache)

      reassign(manager, {})
      expect(has(group.cache, 'topic')).toBe(false)
      reassign(manager, { topic: [0, 1] })
      listed = '9'
      await manager.resolveOffsets()

      const fresh = [stampOf(manager, 'topic', 0), stampOf(manager, 'topic', 1)]
      for (const [partition, stamp] of old.entries()) {
        expect(revisions.isCurrent('topic', partition, stamp)).toBe(false)
        expect(fresh[partition]).not.toBe(stamp)
        expect(revisions.isCurrent('topic', partition, fresh[partition])).toBe(true)
        expect(manager.heldOffsetAt('topic', partition)).toBe('9')
      }
    })

    test('a capture from before retirement stays stale after recreation', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      const { manager } = createManager(group, {
        assignment: { topic: [0, 1], other: [0] },
        obligations: table,
      })
      await manager.resolveOffsets()
      const revisions = CommittedOffsetRevisions.forCache(group.cache)
      const before = [0, 1].map((partition) => stampOf(manager, 'topic', partition))
      const other = stampOf(manager, 'other', 0)
      const held = group.ledger.begin([{ topic: 'topic', partition: 0 }])

      manager.resetOffsetsForTopic('topic')

      expect(group.ledger.isSuperseded(held, 'topic', 0)).toBe(true)
      for (const [partition, stamp] of before.entries()) {
        expect(revisions.isCurrent('topic', partition, stamp)).toBe(false)
      }
      expect(revisions.isCurrent('other', 0, other)).toBe(true)
      group.ledger.end(held)
      await manager.resolveOffsets()
      expect(revisions.isCurrent('topic', 0, stampOf(manager, 'topic', 0))).toBe(true)
      for (const [partition, stamp] of before.entries()) {
        expect(revisions.isCurrent('topic', partition, stamp)).toBe(false)
      }

      reassign(manager, { other: [0] })
      expect(has(group.cache, 'topic')).toBe(false)
      expect(group.stamps()).toEqual({ topics: 1, cells: 1 })
    })

    test('retirement waits for an outstanding read or seek on the topic', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      const hold = { fetch: deferred() }
      const { manager } = createManager(group, {
        assignment: { read: [0], seek: [0], kept: [0] },
        obligations: table,
        autoCommit: true,
        hold,
      })
      const resolving = manager.resolveOffsets()
      await flush()
      hold.commit = deferred()
      const seeking = manager.seek({ topic: 'seek', partition: 0, offset: '12' })
      await flush()

      reassign(manager, { kept: [0] })

      expect(has(group.cache, 'read')).toBe(true)
      expect(has(group.cache, 'seek')).toBe(true)
      hold.fetch.resolve()
      await resolving
      expect(has(group.cache, 'read')).toBe(false)
      expect(has(group.cache, 'seek')).toBe(true)
      hold.commit.resolve()
      await seeking
      expect(has(group.cache, 'seek')).toBe(false)
      expect(group.ledger.size()).toBe(0)
      expect(Object.keys(group.cache)).toEqual(['kept'])
    })

    test('reset debt survives a group rebuild across retirement', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      table.owe('topic', ID_B)
      table.owe('other', ID_B)
      const topicIds = { topic: ID_B, other: ID_B }
      const first = createManager(group, {
        assignment: { topic: [0] },
        obligations: table,
        committed: () => '9',
        topicIds,
      })
      await first.manager.resolveOffsets()
      expect(first.manager.heldOffsetAt('topic', 0)).toBe('5')

      table.unlink()

      expect(has(group.cache, 'topic')).toBe(false)
      expect(table.isOwed('topic', 0)).toBe(true)
      expect(table.isOwed('other', 0)).toBe(true)

      table.attachTo(group.ledger)
      const rebuilt = createManager(group, {
        assignment: { topic: [0] },
        obligations: table,
        committed: () => '9',
        topicIds,
      })
      await rebuilt.manager.resolveOffsets()
      expect(rebuilt.manager.heldOffsetAt('topic', 0)).toBe('5')
      expect(table.isOwed('topic', 0)).toBe(true)

      rebuilt.coordinator.offsetCommit.mockRejectedValueOnce(new Error('commit failed'))
      await expect(
        rebuilt.manager.commitOffsets({
          topics: [{ topic: 'topic', partitions: [{ partition: 0, offset: '5' }] }],
        })
      ).rejects.toThrow('commit failed')
      expect(table.isOwed('topic', 0)).toBe(true)

      await rebuilt.manager.commitOffsets({
        topics: [{ topic: 'topic', partitions: [{ partition: 0, offset: '5' }] }],
      })
      expect(table.isOwed('topic', 0)).toBe(false)
      expect(table.isOwed('other', 0)).toBe(true)
    })

    test('a confirmed transaction publication keeps its topic until settled', () => {
      const { cluster, cache, ledger } = createRealCluster()
      const table = linkedTable(ledger)
      table.claimAssignment(['topic'])
      cluster.markOffsetAsCommitted({ groupId: GROUP, topic: 'topic', partition: 0, offset: '3' })
      const offsets = new TransactionOffsets(newLogger())
      const call = offsets.snapshot(ledger, GROUP, [
        { topic: 'topic', partitions: [{ partition: 0, offset: '7' }] },
      ])

      table.claimAssignment([])
      table.retireUnowned()
      expect(has(cache, 'topic')).toBe(true)

      offsets.stage(call)
      ledger.retireUnowned()
      expect(cache.topic[0]).toBe('3')

      offsets.publish(cluster)

      expect(has(cache, 'topic')).toBe(false)
      expect(ledger.size()).toBe(0)
      expect(CommittedOffsetRevisions.sizeOf(cache)).toEqual({ topics: 0, cells: 0 })
    })

    test('an abandoned or late transaction publication does not resurrect a retired topic', () => {
      const { cluster, cache, ledger } = createRealCluster()
      const table = linkedTable(ledger)
      table.claimAssignment(['topic', 'other'])
      cluster.markOffsetAsCommitted({ groupId: GROUP, topic: 'topic', partition: 0, offset: '3' })
      cluster.markOffsetAsCommitted({ groupId: GROUP, topic: 'other', partition: 0, offset: '3' })
      const offsets = new TransactionOffsets(newLogger())
      const unstaged = offsets.snapshot(ledger, GROUP, [
        { topic: 'topic', partitions: [{ partition: 0, offset: '7' }] },
      ])
      const partial = offsets.snapshot(ledger, GROUP, [
        { topic: 'topic', partitions: [{ partition: 1, offset: '7' }] },
        { topic: 'other', partitions: [{ partition: 0, offset: '7' }] },
      ])
      offsets.stage(partial)
      const newer = offsets.snapshot(ledger, GROUP, [
        { topic: 'other', partitions: [{ partition: 0, offset: '8' }] },
      ])
      offsets.stage(newer)

      table.claimAssignment([])
      table.retireUnowned()
      expect(Object.keys(cache).sort()).toEqual(['other', 'topic'])

      offsets.release()
      expect(Object.keys(cache)).toEqual([])
      offsets.stage(unstaged)
      offsets.stage(partial)
      offsets.publish(cluster)
      expect(Object.keys(cache)).toEqual([])

      table.claimAssignment(['topic'])
      const { manager } = createManager(
        { cache, ledger },
        { assignment: { topic: [0] }, obligations: table }
      )
      offsets.publish(cluster)
      expect(manager.heldOffsetAt('topic', 0)).toBeUndefined()
      expect(ledger.size()).toBe(0)
    })

    test('a throwing publication still releases its topic', () => {
      const { cluster, cache, ledger } = createRealCluster()
      const offsets = new TransactionOffsets({
        error: () => {
          throw new Error('logger failed')
        },
      })
      const call = offsets.snapshot(ledger, GROUP, [
        { topic: 'topic', partitions: [{ partition: 0, offset: '7' }] },
      ])
      offsets.stage(call)
      const publish = jest.spyOn(cluster, 'markOffsetAsCommitted').mockImplementation(() => {
        cache.topic = { 0: '7' }
        throw new Error('publication failed')
      })

      expect(() => offsets.publish(cluster)).not.toThrow()

      expect(publish).toHaveBeenCalledTimes(1)
      expect(has(cache, 'topic')).toBe(false)
      expect(ledger.size()).toBe(0)
      expect(ledger.openTickets).toBe(0)
    })

    test("a live sibling's private reset position survives an unrelated retirement", async () => {
      const group = createGroup()
      const tableA = linkedTable(group.ledger)
      const tableB = linkedTable(group.ledger)
      const a = createManager(group, {
        assignment: { shared: [0], unrelated: [0] },
        obligations: tableA,
        listed: () => '4',
      })
      const b = createManager(group, {
        assignment: { shared: [1] },
        obligations: tableB,
        listed: () => '6',
      })
      await a.manager.resolveOffsets()
      await b.manager.resolveOffsets()
      const revisions = CommittedOffsetRevisions.forCache(group.cache)

      reassign(a.manager, { shared: [0] })
      expect(has(group.cache, 'unrelated')).toBe(false)
      reassign(b.manager, {})

      expect(has(group.cache, 'shared')).toBe(true)
      expect(revisions.isCurrent('shared', 0, stampOf(a.manager, 'shared', 0))).toBe(true)
      expect(a.manager.heldOffsetAt('shared', 0)).toBe('4')
      reassign(a.manager, {})
      expect(Object.keys(group.cache)).toEqual([])
    })

    test('a repeated same-value commit after retirement is current', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      const { manager, coordinator } = createManager(group, {
        assignment: { topic: [0] },
        obligations: table,
      })
      const commit = () =>
        manager.commitOffsets({
          topics: [{ topic: 'topic', partitions: [{ partition: 0, offset: '5' }] }],
        })
      await manager.resolveOffsets()
      await commit()
      reassign(manager, {})
      expect(has(group.cache, 'topic')).toBe(false)
      reassign(manager, { topic: [0] })
      await manager.resolveOffsets()
      const shadow = stampOf(manager, 'topic', 0)
      expect(manager.heldOffsetAt('topic', 0)).toBe('5')

      await commit()

      expect(coordinator.offsetCommit).toHaveBeenCalledTimes(2)
      expect(group.cache.topic).toEqual({ 0: '5' })
      expect(stampOf(manager, 'topic', 0)).toBeUndefined()
      expect(CommittedOffsetRevisions.forCache(group.cache).isCurrent('topic', 0, shadow)).toBe(
        false
      )
      expect(manager.heldOffsetAt('topic', 0)).toBe('5')
    })
  })

  describe('owner lifecycle', () => {
    afterEach(() => jest.restoreAllMocks())

    test('a subscribed but unassigned member keeps its topic, and stop then unsubscribe retires it', async () => {
      const harness = createConsumerHarness()
      const sibling = linkedTable(harness.ledger())
      sibling.claimSubscription(['sibling-topic'])
      const { consumer } = await harness.run({ topics: ['subscribed'] })
      harness.publish('subscribed')
      harness.publish('sibling-topic')

      expect(has(harness.cache(), 'subscribed')).toBe(true)
      await consumer.stop()
      harness.ledger().retireUnowned()
      expect(has(harness.cache(), 'subscribed')).toBe(true)

      await consumer.unsubscribe()

      expect(has(harness.cache(), 'subscribed')).toBe(false)
      expect(has(harness.cache(), 'sibling-topic')).toBe(true)
      expect(sibling.subscribed).toEqual(new Set(['sibling-topic']))
    })

    test('assignment ownership ends at stop and returns with the next run', async () => {
      const harness = createConsumerHarness()
      const { consumer, group } = await harness.run({ topics: ['subscribed'] })
      harness.assign(group, { assigned: [0] })
      harness.publish('assigned')
      expect(has(harness.cache(), 'assigned')).toBe(true)

      await consumer.stop()

      expect(has(harness.cache(), 'assigned')).toBe(false)
      const next = await harness.run({ restart: consumer })
      harness.assign(next.group, { assigned: [0] })
      harness.publish('assigned')
      harness.ledger().retireUnowned()
      expect(has(harness.cache(), 'assigned')).toBe(true)
    })

    test('broker-regex grown names and client-pattern publications own their topics', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      const consumerGroup = Object.create(ConsumerGroup.prototype)
      Object.assign(consumerGroup, {
        obligations: table,
        topicsSubscribed: [],
        topicConfigurations: Object.create(null),
        regexSubscription: { fromBeginning: true },
        unregisteredTopics: new Set(),
        subscriptionState: { retainTopics: () => {} },
        cluster: { registerAssignedTopics: async (names) => names },
      })
      const write = (topic) => {
        const ticket = group.ledger.begin([{ topic, partition: 0 }])
        group.cache[topic] = { 0: '1' }
        group.ledger.end(ticket)
      }

      await consumerGroup.growSubscription(['grown'])
      write('grown')
      expect(has(group.cache, 'grown')).toBe(true)

      consumerGroup.retainSubscription(['published'])
      write('published')
      expect(Object.keys(group.cache)).toEqual(['published'])
      consumerGroup.retainSubscription([])
      expect(Object.keys(group.cache)).toEqual([])
    })

    test('a grouped manual assignment replaced while stopped retires the dropped topic and keeps the rest', async () => {
      const harness = createConsumerHarness()
      const sibling = linkedTable(harness.ledger())
      sibling.claimSubscription(['sibling-topic'])
      const consumer = harness.create()
      await consumer.assign([
        { topic: 'kept', partitions: [0] },
        { topic: 'dropped', partitions: [0] },
      ])
      const { group } = await harness.run({ restart: consumer })
      const { manager } = harness.assign(group, { kept: [0], dropped: [0] })
      await manager.resolveOffsets()
      for (const topic of ['kept', 'dropped', 'sibling-topic']) harness.publish(topic)
      const revisions = CommittedOffsetRevisions.forCache(harness.cache())
      const stamps = {
        kept: revisions.capture('kept', 0),
        dropped: revisions.capture('dropped', 0),
        sibling: revisions.capture('sibling-topic', 0),
      }
      await consumer.stop()
      harness.ledger().retireUnowned()
      expect(Object.keys(harness.cache()).sort()).toEqual(['dropped', 'kept', 'sibling-topic'])

      await consumer.assign([{ topic: 'kept', partitions: [0] }])

      expect(Object.keys(harness.cache()).sort()).toEqual(['kept', 'sibling-topic'])
      expect(revisions.isCurrent('dropped', 0, stamps.dropped)).toBe(false)
      expect(revisions.isCurrent('kept', 0, stamps.kept)).toBe(true)
      expect(revisions.isCurrent('sibling-topic', 0, stamps.sibling)).toBe(true)
      expect(group.obligations.subscribed).toEqual(new Set(['kept']))
      expect(sibling.subscribed).toEqual(new Set(['sibling-topic']))
      harness.publish('dropped')
      expect(has(harness.cache(), 'dropped')).toBe(false)
      expect(revisions.isCurrent('dropped', 0, stamps.dropped)).toBe(false)
    })

    test('a groupless manual assignment replaced while stopped cleans its private cache and stamps without registry state', async () => {
      const harness = createConsumerHarness({ groupless: true })
      const consumer = harness.create()
      await consumer.assign([
        { topic: 'kept', partitions: [0] },
        { topic: 'dropped', partitions: [0] },
      ])
      const { group } = await harness.run({ restart: consumer })
      const offsets = group.privateOffsets
      offsets.kept = { 0: '3' }
      offsets.dropped = { 0: '4' }
      const revisions = CommittedOffsetRevisions.forCache(offsets)
      const stamps = {
        kept: revisions.capture('kept', 0),
        dropped: revisions.capture('dropped', 0),
      }
      await consumer.stop()
      expect(Object.keys(offsets).sort()).toEqual(['dropped', 'kept'])

      await consumer.assign([{ topic: 'kept', partitions: [0] }])

      expect(Object.keys(offsets)).toEqual(['kept'])
      expect(CommittedOffsetRevisions.sizeOf(offsets)).toEqual({ topics: 1, cells: 1 })
      expect(revisions.isCurrent('dropped', 0, stamps.dropped)).toBe(false)
      expect(revisions.isCurrent('kept', 0, stamps.kept)).toBe(true)
      offsets.dropped = { 0: '4' }
      expect(revisions.isCurrent('dropped', 0, stamps.dropped)).toBe(false)
      expect(harness.stores.offsets.size).toBe(0)
      expect(harness.stores.operations.size).toBe(0)
    })

    test('an initial client-pattern subscription makes its matched names owners before the first assignment', async () => {
      const harness = createConsumerHarness({ listed: ['pat-a', 'pat-b', 'other'] })
      const consumer = harness.create()
      await consumer.subscribe({ topics: [/^pat-/], fromBeginning: true })
      const { group } = await harness.run({ restart: consumer })
      for (const topic of ['pat-a', 'pat-b', 'other']) harness.publish(topic)
      const revisions = CommittedOffsetRevisions.forCache(harness.cache())
      const stamps = {
        a: revisions.capture('pat-a', 0),
        other: revisions.capture('other', 0),
      }

      harness.ledger().retireUnowned()

      expect(group.obligations.subscribed).toEqual(new Set(['pat-a', 'pat-b']))
      expect(Object.keys(harness.cache()).sort()).toEqual(['pat-a', 'pat-b'])
      expect(revisions.isCurrent('pat-a', 0, stamps.a)).toBe(true)
      expect(revisions.isCurrent('other', 0, stamps.other)).toBe(false)

      await consumer.stop()
      await consumer.unsubscribe()
      expect(Object.keys(harness.cache())).toEqual([])
      expect(revisions.isCurrent('pat-a', 0, stamps.a)).toBe(false)
    })

    test('a failed disconnect still releases the closing consumer and retires its topics, keeping the sibling', async () => {
      const harness = createConsumerHarness({
        topicIds: { assigned: ID_A },
        disconnect: async () => {
          throw new Error('teardown failed')
        },
      })
      const sibling = linkedTable(harness.ledger())
      sibling.claimAssignment(['sibling-topic'])
      const { consumer, group } = await harness.run({ topics: ['subscribed'] })
      harness.assign(group, { assigned: [0] })
      for (const topic of ['assigned', 'subscribed', 'sibling-topic']) harness.publish(topic)
      const table = group.obligations
      table.owe('assigned', ID_A)
      const revisions = CommittedOffsetRevisions.forCache(harness.cache())
      const stamps = {
        assigned: revisions.capture('assigned', 0),
        sibling: revisions.capture('sibling-topic', 0),
      }
      const ledger = harness.ledger()
      const hooks = { onTopicIdle: ledger.onTopicIdle, onRetire: ledger.onRetire }

      await expect(consumer.disconnect()).rejects.toThrow('teardown failed')

      expect(table.subscribed.size).toBe(0)
      expect(table.assigned.size).toBe(0)
      expect(table.size()).toBe(0)
      expect(table.ledger).toBeNull()
      expect(harness.ledger().attached.has(table)).toBe(false)
      expect(harness.ledger().attached.has(sibling)).toBe(true)
      expect(Object.keys(harness.cache())).toEqual(['sibling-topic'])
      expect(revisions.isCurrent('assigned', 0, stamps.assigned)).toBe(false)
      expect(revisions.isCurrent('sibling-topic', 0, stamps.sibling)).toBe(true)
      expect(harness.ledger().isOwned('sibling-topic')).toBe(true)

      expect(harness.ledger()).toBe(ledger)
      expect(ledger.onTopicIdle).toBe(hooks.onTopicIdle)
      expect(ledger.onRetire).toBe(hooks.onRetire)
      const ticket = ledger.begin([{ topic: 'fresh', partition: 0 }])
      harness.cache().fresh = { 0: '1' }
      const fresh = revisions.capture('fresh', 0)
      ledger.end(ticket)
      expect(has(harness.cache(), 'fresh')).toBe(false)
      expect(revisions.isCurrent('fresh', 0, fresh)).toBe(false)
      expect(Object.keys(harness.cache())).toEqual(['sibling-topic'])

      sibling.releaseAssignment()
      expect(Object.keys(harness.cache())).toEqual([])
      expect(revisions.isCurrent('sibling-topic', 0, stamps.sibling)).toBe(false)
    })

    test('a same-ledger relink sweeps nothing and a transfer cleans the old ledger', () => {
      const old = createGroup()
      const table = linkedTable(old.ledger)
      table.claimAssignment(['topic'])
      const ticket = old.ledger.begin([{ topic: 'topic', partition: 0 }])
      old.cache.topic = { 0: '1' }
      old.ledger.end(ticket)
      const retire = jest.spyOn(old.ledger, 'retireUnowned')

      table.attachTo(old.ledger)
      expect(retire).not.toHaveBeenCalled()
      expect(old.ledger.attached.has(table)).toBe(true)

      const next = createGroup()
      table.attachTo(next.ledger)

      expect(retire).toHaveBeenCalledTimes(1)
      expect(old.ledger.attached.has(table)).toBe(false)
      expect(next.ledger.attached.has(table)).toBe(true)
      expect(has(old.cache, 'topic')).toBe(false)
    })

    test("disconnect clears both owner sets and retires the consumer's topics", async () => {
      const harness = createConsumerHarness()
      const sibling = linkedTable(harness.ledger())
      sibling.claimAssignment(['sibling-topic'])
      const { consumer, group } = await harness.run({ topics: ['subscribed'] })
      harness.assign(group, { assigned: [0] })
      harness.publish('assigned')
      harness.publish('subscribed')
      harness.publish('sibling-topic')
      const table = group.obligations

      await consumer.disconnect()

      expect(table.subscribed.size).toBe(0)
      expect(table.assigned.size).toBe(0)
      expect(table.ledger).toBeNull()
      expect(Object.keys(harness.cache())).toEqual(['sibling-topic'])
      expect(harness.ledger().attached.has(sibling)).toBe(true)
    })

    test('a crash-style unlink keeps the debt and the owner sets for the relink', () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      table.owe('topic', ID_A)
      table.claimSubscription(['topic'])
      table.claimAssignment(['topic'])

      table.unlink()

      expect(table.isOwed('topic', 0)).toBe(true)
      expect(table.subscribed).toEqual(new Set(['topic']))
      expect(table.assigned).toEqual(new Set(['topic']))
      expect(group.ledger.attached.has(table)).toBe(false)
      table.attachTo(group.ledger)
      expect(group.ledger.isOwned('topic')).toBe(true)
    })

    test('owner sets are snapshots of what the caller passed', () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      const names = ['topic']
      table.claimSubscription(names)
      table.claimAssignment(names)
      names.push('later')

      expect(table.holds('topic')).toBe(true)
      expect(table.holds('later')).toBe(false)
    })
  })

  describe('ledger hooks', () => {
    test('topic idle fires once per topic after the whole ticket is released', () => {
      const group = createGroup()
      const seen = []
      const original = group.ledger.onTopicIdle
      group.ledger.onTopicIdle = (topic) => {
        seen.push([topic, group.ledger.size(), group.ledger.openTickets])
        original(topic)
      }
      const shared = group.ledger.begin([
        { topic: 'one', partition: 0 },
        { topic: 'one', partition: 0 },
        { topic: 'one', partition: 1 },
        { topic: 'two', partition: 0 },
      ])
      const other = group.ledger.begin([{ topic: 'two', partition: 0 }])
      const stuck = group.ledger.begin([{ topic: 'stuck', partition: 0 }])
      for (const topic of ['one', 'two', 'stuck']) group.cache[topic] = { 0: '1' }

      group.ledger.end(shared)
      group.ledger.end(shared)
      expect(seen).toEqual([['one', 2, 2]])
      expect(Object.keys(group.cache).sort()).toEqual(['stuck', 'two'])

      group.ledger.end(other)
      expect(seen).toEqual([
        ['one', 2, 2],
        ['two', 1, 1],
      ])
      expect(Object.keys(group.cache)).toEqual(['stuck'])
      group.ledger.end(stuck)
      expect(Object.keys(group.cache)).toEqual([])
    })

    test('a callback saved from a swept entry cannot change its replacement', () => {
      const offsets = new Map()
      const operations = new Map()
      const registry = GroupRegistry.forStores(offsets, operations)
      const holder = {}
      const first = registry.acquire(GROUP, holder)
      const savedIdle = first.ledger.onTopicIdle
      const savedRetire = first.ledger.onRetire
      registry.release(GROUP, holder)
      expect(first.ledger.onTopicIdle).toBeNull()
      expect(first.ledger.onRetire).toBeNull()
      expect(first.ledger.onIdle).toBeNull()

      const second = registry.acquire(GROUP, holder)
      second.offsets.topic = { 0: '1' }
      savedIdle('topic')
      savedRetire()

      expect(second.offsets.topic).toEqual({ 0: '1' })
      expect(second.ledger).not.toBe(first.ledger)
      registry.release(GROUP, holder)
    })

    test('owner checks read the tables without settling or allocating anything', () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      table.owe('topic', ID_A)
      const discharge = jest.spyOn(table, 'discharge')
      const retire = jest.spyOn(group.ledger, 'retireUnowned')

      expect(group.ledger.isOwned('topic')).toBe(true)
      expect(group.ledger.isOwned('nobody')).toBe(false)

      expect(discharge).not.toHaveBeenCalled()
      expect(retire).not.toHaveBeenCalled()
      expect(table.isOwed('topic', 0)).toBe(true)
      expect(group.ledger.size()).toBe(0)
    })

    test('a debt that disappears through a contradicting topic id requests retirement', () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      table.owe('topic', ID_A)
      group.cache.topic = { 0: '1' }
      group.ledger.retireUnowned()
      expect(has(group.cache, 'topic')).toBe(true)

      expect(table.current('topic', ID_B)).toBeUndefined()

      expect(has(group.cache, 'topic')).toBe(false)
    })
  })

  describe('real offset operations under the registry hooks', () => {
    test('two held resolutions of one manager in either order fill the cell once from the committed answer', async () => {
      for (const order of [
        [0, 1],
        [1, 0],
      ]) {
        const group = createGroup()
        const { manager, coordinator } = createManager(group, {
          assignment: { topic: [0] },
          obligations: linkedTable(group.ledger),
        })
        const gates = gateCalls(coordinator.offsetFetch, (_, { topics }) =>
          offsetFetchAnswer(topics, '11')
        )
        const resolving = [manager.resolveOffsets(), manager.resolveOffsets()]
        await flush()
        expect(gates).toHaveLength(2)

        gates[order[0]].resolve()
        await resolving[order[0]]
        const revisions = CommittedOffsetRevisions.forCache(group.cache)
        const stamp = revisions.capture('topic', 0)
        group.ledger.retireUnowned()
        expect(group.cache.topic).toEqual({ 0: '11' })
        expect(group.ledger.isOwned('topic')).toBe(true)
        expect(group.ledger.openTickets).toBe(1)

        gates[order[1]].resolve()
        await resolving[order[1]]

        expect(revisions.isCurrent('topic', 0, stamp)).toBe(true)
        expect(group.cache.topic).toEqual({ 0: '11' })
        expect(manager.heldOffsetAt('topic', 0)).toBe('11')
        expect(group.ledger.openTickets).toBe(0)
        expect(group.ledger.size()).toBe(0)
      }
    })

    test('two held resolutions of one manager in either order keep the first derived reset answer', async () => {
      for (const order of [
        [0, 1],
        [1, 0],
      ]) {
        const group = createGroup()
        const { manager, cluster } = createManager(group, {
          assignment: { topic: [0] },
          obligations: linkedTable(group.ledger),
        })
        const answers = ['5', '6']
        const gates = gateCalls(cluster.fetchTopicsOffset, (index, requests) =>
          listOffsetsAnswer(requests, () => answers[index])
        )
        const resolving = [manager.resolveOffsets(), manager.resolveOffsets()]
        await flush()
        expect(gates).toHaveLength(2)

        gates[order[0]].resolve()
        await resolving[order[0]]
        const position = manager.resetPositions.topic[0]
        group.ledger.retireUnowned()
        expect(position.offset).toBe(answers[order[0]])
        expect(group.ledger.isOwned('topic')).toBe(true)
        expect(group.ledger.openTickets).toBe(1)

        gates[order[1]].resolve()
        await resolving[order[1]]

        expect(manager.resetPositions.topic[0]).toBe(position)
        expect(manager.heldOffsetAt('topic', 0)).toBe(answers[order[0]])
        expect(
          CommittedOffsetRevisions.forCache(group.cache).isCurrent('topic', 0, position.revision)
        ).toBe(true)
        expect(group.cache.topic?.[0]).toBeUndefined()
        expect(group.ledger.openTickets).toBe(0)
        expect(group.ledger.size()).toBe(0)
      }
    })

    test('sibling managers with different reset policies keep their own position on one cell in either order', async () => {
      for (const order of [
        [0, 1],
        [1, 0],
      ]) {
        const group = createGroup()
        const members = [EARLIEST, DURATION].map((policy) => {
          const member = createManager(group, {
            assignment: { topic: [0] },
            obligations: linkedTable(group.ledger),
          })
          member.manager.topicConfigurations.topic = { resetPolicy: policy }
          member.gates = gateCalls(member.cluster.fetchTopicsOffset, (_, requests) =>
            listOffsetsAnswer(requests, (request) => (request.fromBeginning ? '0' : '2'))
          )
          return member
        })
        const resolving = members.map(({ manager }) => manager.resolveOffsets())
        await flush()

        members[order[0]].gates[0].resolve()
        await resolving[order[0]]
        group.ledger.retireUnowned()
        expect(group.ledger.isOwned('topic')).toBe(true)
        expect(group.ledger.openTickets).toBe(1)
        members[order[1]].gates[0].resolve()
        await resolving[order[1]]

        expect(members.map(({ manager }) => manager.heldOffsetAt('topic', 0))).toEqual(['0', '2'])
        expect(members.map(({ manager }) => manager.nextOffset('topic', 0).toString())).toEqual([
          '0',
          '2',
        ])
        expect(group.cache.topic?.[0]).toBeUndefined()
        expect(group.ledger.openTickets).toBe(0)
        expect(group.ledger.size()).toBe(0)
      }
    })

    test('a held commit is superseded by a newer seek and a held seek by a newer commit', async () => {
      const group = createGroup()
      const hold = {}
      const { manager, coordinator } = createManager(group, {
        assignment: { topic: [0, 1, 2] },
        obligations: linkedTable(group.ledger),
        autoCommit: true,
        hold,
      })
      const commit = (partition, offset) =>
        manager.commitOffsets({
          topics: [{ topic: 'topic', partitions: [{ partition, offset }] }],
        })
      const held = async (work) => {
        const gate = deferred()
        hold.commit = gate
        const pending = work()
        await flush()
        hold.commit = null
        return { pending, release: gate.resolve }
      }

      const supersededCommit = await held(() => commit(0, '3'))
      await expect(manager.seek({ topic: 'topic', partition: 0, offset: '8' })).resolves.toBe(true)
      supersededCommit.release()
      await supersededCommit.pending
      expect(group.cache.topic[0]).toBeUndefined()

      const validCommit = await held(() => commit(1, '9'))
      hold.commitError = new Error('seek failed')
      await expect(manager.seek({ topic: 'topic', partition: 1, offset: '20' })).rejects.toThrow(
        'seek failed'
      )
      hold.commitError = null
      validCommit.release()
      await validCommit.pending
      expect(group.cache.topic[1]).toBe('9')

      const supersededSeek = await held(() =>
        manager.seek({ topic: 'topic', partition: 2, offset: '30' })
      )
      await commit(2, '4')
      supersededSeek.release()
      await expect(supersededSeek.pending).resolves.toBe(false)
      expect(group.cache.topic[2]).toBe('4')

      expect(coordinator.offsetCommit).toHaveBeenCalledTimes(6)
      expect(group.ledger.size()).toBe(0)
      expect(Object.keys(group.cache)).toEqual(['topic'])
    })

    test('a held duration reset keeps its topic until it settles', async () => {
      const group = createGroup()
      const hold = { list: deferred() }
      const table = linkedTable(group.ledger)
      const { manager } = createManager(group, {
        assignment: { topic: [0], kept: [0] },
        obligations: table,
        hold,
      })
      const resetting = manager.resetByDuration({ topic: 'topic', partition: 0 })
      await flush()
      table.claimAssignment(['kept'])
      table.retireUnowned()
      expect(has(group.cache, 'topic')).toBe(true)

      hold.list.resolve()
      await resetting

      expect(has(group.cache, 'topic')).toBe(false)
      expect(group.ledger.size()).toBe(0)
    })

    test('a commit held across a membership renewal neither publishes nor discharges, and retires after settlement', async () => {
      const group = createGroup()
      const ownership = new GroupOwnership()
      const table = linkedTable(group.ledger)
      const gate = deferred()
      const hold = { commit: gate }
      const { manager } = createManager(group, {
        assignment: { topic: [0, 1], kept: [0] },
        obligations: table,
        topicIds: { topic: ID_A, kept: ID_B },
        ownership,
        hold,
      })
      table.claimAssignment(['topic', 'kept'])
      table.owe('topic', ID_A)
      const opening = group.ledger.begin([{ topic: 'topic', partition: 1 }])
      group.cache.topic = { 1: '2' }
      group.ledger.end(opening)
      const revisions = CommittedOffsetRevisions.forCache(group.cache)
      const stamp = revisions.capture('topic', 0)
      const committing = manager.commitOffsets({
        topics: [{ topic: 'topic', partitions: [{ partition: 0, offset: '7' }] }],
      })
      await flush()
      hold.commit = null

      ownership.renew()
      table.claimAssignment(['kept'])
      group.ledger.retireUnowned()
      expect(group.cache.topic).toEqual({ 1: '2' })
      expect(group.ledger.openTickets).toBe(1)
      const renewed = manager.commitOffsets({
        topics: [{ topic: 'kept', partitions: [{ partition: 0, offset: '3' }] }],
      })
      gate.resolve()

      await expect(committing).rejects.toThrow(KafkaJSCommitFailedError)
      await expect(renewed).resolves.toBeUndefined()

      expect(group.cache.topic).toEqual({ 1: '2' })
      expect(revisions.isCurrent('topic', 0, stamp)).toBe(true)
      expect(table.isOwed('topic', 0)).toBe(true)
      expect(group.ledger.openTickets).toBe(0)
      table.retain(new Set(['kept']))
      expect(has(group.cache, 'topic')).toBe(false)
      expect(revisions.isCurrent('topic', 0, stamp)).toBe(false)
      expect(group.cache.kept).toEqual({ 0: '3' })
      expect(group.ledger.size()).toBe(0)
    })

    test('an auto-committed duration reset creates the missing cache key, invalidates stamps and discharges the debt', async () => {
      const group = createGroup()
      const table = linkedTable(group.ledger)
      const { manager, coordinator } = createManager(group, {
        assignment: { topic: [0] },
        obligations: table,
        topicIds: { topic: ID_A },
        autoCommit: true,
        listed: () => '9',
      })
      manager.topicConfigurations.topic = { resetPolicy: DURATION }
      table.claimAssignment(['topic'])
      table.owe('topic', ID_A)
      delete group.cache.topic
      const revisions = CommittedOffsetRevisions.forCache(group.cache)
      const stamp = revisions.capture('topic', 0)
      expect(has(group.cache, 'topic')).toBe(false)

      await expect(manager.resetByDuration({ topic: 'topic', partition: 0 })).resolves.toBe(true)

      expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
      expect(group.cache.topic).toEqual({ 0: '9' })
      expect(manager.heldOffsetAt('topic', 0)).toBe('9')
      expect(manager.resetPositions.topic?.[0]).toBeUndefined()
      expect(revisions.isCurrent('topic', 0, stamp)).toBe(false)
      expect(table.isOwed('topic', 0)).toBe(false)
      expect(group.ledger.openTickets).toBe(0)
      expect(group.ledger.size()).toBe(0)
      await manager.resolveOffsets()
      expect(table.size()).toBe(0)
      group.ledger.retireUnowned()
      expect(group.cache.topic).toEqual({ 0: '9' })

      table.claimAssignment([])
      reassign(manager, {})
      expect(has(group.cache, 'topic')).toBe(false)
      expect(manager.heldOffsetAt('topic', 0)).toBeUndefined()
    })

    test('a real topic recreation with held work leaves old captures stale and new work current', async () => {
      const harness = createConsumerHarness({ topicIds: { topic: ID_A } })
      const { group } = await harness.run({ topics: ['topic', 'other'] })
      const { manager } = harness.assign(group, { topic: [0, 1], other: [0] })
      await manager.resolveOffsets()
      const revisions = CommittedOffsetRevisions.forCache(harness.cache())
      const old = [0, 1].map((partition) => stampOf(manager, 'topic', partition))
      const other = stampOf(manager, 'other', 0)
      const held = harness.ledger().begin([{ topic: 'topic', partition: 0 }], { mutation: true })
      for (const stamp of old)
        expect(revisions.isCurrent('topic', old.indexOf(stamp), stamp)).toBe(true)

      harness.topicIds.topic = ID_B
      harness.emitRecreated({ topic: 'topic', previousTopicId: ID_A, topicId: ID_B })

      expect(harness.ledger().isSuperseded(held, 'topic', 0)).toBe(true)
      for (const [partition, stamp] of old.entries()) {
        expect(revisions.isCurrent('topic', partition, stamp)).toBe(false)
      }
      expect(revisions.isCurrent('other', 0, other)).toBe(true)
      harness.ledger().end(held)
      await manager.resolveOffsets()
      expect(revisions.isCurrent('topic', 0, stampOf(manager, 'topic', 0))).toBe(true)
      expect(group.obligations.isOwed('topic', 0)).toBe(true)
    })
  })

  describe('stamps', () => {
    test('stamps stay unique past the safe-integer limit', () => {
      const revisions = CommittedOffsetRevisions.forCache({})
      revisions.seq = Number.MAX_SAFE_INTEGER - 1
      const last = revisions.capture('topic', 0)
      revisions.advance('topic', 0)
      const first = revisions.capture('topic', 0)
      revisions.advance('topic', 0)
      const second = revisions.capture('topic', 0)

      expect(last).toBe(Number.MAX_SAFE_INTEGER)
      expect(first).not.toBe(last)
      expect(second).not.toBe(first)
      expect(revisions.isCurrent('topic', 0, second)).toBe(true)
      expect(revisions.isCurrent('topic', 0, first)).toBe(false)
      expect(revisions.isCurrent('topic', 0, last)).toBe(false)
      expect(revisions.seq).toBe(Number.MAX_SAFE_INTEGER)
    })

    test('a size read creates no revision source', () => {
      const cache = {}
      expect(CommittedOffsetRevisions.sizeOf(cache)).toEqual({ topics: 0, cells: 0 })
      expect(CommittedOffsetRevisions.peek(cache)).toBeUndefined()
      const revisions = CommittedOffsetRevisions.forCache(cache)
      revisions.capture('topic', 0)
      revisions.capture('topic', 1)
      expect(CommittedOffsetRevisions.sizeOf(cache)).toEqual({ topics: 1, cells: 2 })
    })
  })

  describe("one manager's local tables", () => {
    test("2000 cooperative reassignments leave one manager's local tables at its assignment", async () => {
      const group = createGroup()
      const { manager } = createManager(group, {
        assignment: { anchor: [0] },
        obligations: linkedTable(group.ledger),
      })
      await manager.resolveOffsets()
      manager.resolveOffset({ topic: 'anchor', partition: 0, offset: '41', leaderEpoch: 7 })

      for (let n = 0; n < 2000; n++) {
        const topic = `rotating-${n}`
        reassign(manager, { anchor: [0], [topic]: [0, 1] })
        await manager.resolveOffsets()
        manager.resolveOffset({ topic, partition: 1, offset: '3', leaderEpoch: 1 })
        reassign(manager, { anchor: [0], [topic]: [1] })
        expect(Object.keys(manager.resolvedOffsets[topic])).toEqual(['1'])
        reassign(manager, { anchor: [0] })
      }

      for (const table of ['resetPositions', 'resolvedOffsets', 'resolvedEpochs']) {
        expect(Object.keys(manager[table])).toEqual(['anchor'])
      }
      expect(manager.resolvedOffsetAt('anchor', 0)).toBe('42')
      expect(manager.lastConsumedEpoch('anchor', 0)).toBe(7)
    })

    test('a late next-position or reset call for a revoked topic recreates no key', async () => {
      const group = createGroup()
      const { manager } = createManager(group, {
        assignment: { gone: [0], kept: [0] },
        obligations: linkedTable(group.ledger),
      })
      await manager.resolveOffsets()
      reassign(manager, { kept: [0] })

      expect(manager.nextOffset('gone', 0, '17').toString()).toBe('17')
      expect(manager.nextOffset('gone', 0, '-1').toString()).toBe('0')
      manager.resetOffset({ topic: 'gone', partition: 0 })
      manager.holdResetPosition('gone', 0, '3')

      for (const table of ['resetPositions', 'resolvedOffsets', 'resolvedEpochs']) {
        expect(has(manager[table], 'gone')).toBe(false)
      }
      reassign(manager, { kept: [0], gone: [0] })
      await manager.resolveOffsets()
      expect(manager.nextOffset('gone', 0).toString()).toBe('5')
    })
  })
})

/**
 * A real broker-free `Cluster` sharing its stores with the registry, so the cache publication
 * path (`markOffsetAsCommitted`) and the ledger are the production ones.
 */
function createRealCluster() {
  const offsets = new Map()
  const operations = new Map()
  const cluster = new Cluster({
    brokers: ['localhost:9092'],
    logger: newLogger(),
    offsets,
    operations,
  })
  return {
    cluster,
    cache: cluster.committedOffsets({ groupId: GROUP }),
    ledger: cluster.offsetOperations({ groupId: GROUP }),
  }
}

/**
 * A real consumer over a stub cluster whose group stores come from the real registry: `run()`
 * builds a real `ConsumerGroup` (the runner's network loop is stubbed), and `assign` installs a
 * real `OffsetManager` on it the way a sync does.
 */
function createConsumerHarness({
  topicIds = {},
  groupless = false,
  listed = [],
  disconnect = async () => {},
} = {}) {
  const stores = { offsets: new Map(), operations: new Map() }
  const registry = GroupRegistry.forStores(stores.offsets, stores.operations)
  const holder = {}
  const entry = () => registry.acquire(GROUP, holder)
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const ids = { ...topicIds }
  const cluster = {
    instrumentationEmitter,
    connect: async () => {},
    disconnect,
    metadata: async () => ({ topicMetadata: [] }),
    stageMetadata: async ({ topics }) => ({
      topicMetadata: topics.map((topic) => ({
        topic,
        partitionMetadata: [0, 1, 2].map((partitionId) => ({ partitionId })),
      })),
    }),
    useConsumerMetadataScope: () => {},
    refreshMetadata: async ({ onMetadata } = {}) =>
      onMetadata?.({
        topicMetadata: listed.map((topic) => ({
          topic,
          partitionMetadata: [{ partitionId: 0 }],
        })),
      }),
    addMultipleTargetTopics: async () => {},
    registerAssignedTopics: async (names) => names,
    resetTargetTopics: async (names, { isCurrent, publish }) => {
      if (isCurrent()) publish?.()
      return true
    },
    releaseConsumerMetadataScope: () => {},
    useBrokerRegex: () => {},
    refreshMetadataIfNecessary: async () => {},
    committedOffsets: () => entry().offsets,
    offsetOperations: () => entry().ledger,
    defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
    topicId: (topic) => ids[topic],
    onTopicRecreated: (handler) =>
      instrumentationEmitter.addListener(CLUSTER_TOPIC_RECREATED, handler),
  }
  return {
    stores,
    topicIds: ids,
    cache: () => entry().offsets,
    ledger: () => entry().ledger,
    emitRecreated: (payload) => instrumentationEmitter.emit(CLUSTER_TOPIC_RECREATED, payload),
    /** Writes a committed cell under a ticket, as a real publication does. */
    publish(topic) {
      const ledger = entry().ledger
      const ticket = ledger.begin([{ topic, partition: 0 }])
      entry().offsets[topic] = { ...entry().offsets[topic], 0: '1' }
      ledger.end(ticket)
    },
    /** @returns {object} a consumer built over the harness cluster, not yet running */
    create() {
      return createConsumer({
        cluster,
        ...(groupless ? {} : { groupId: GROUP }),
        logger: newLogger(),
        instrumentationEmitter,
        metadataMaxAge: 300000,
        retry: { retries: 0 },
      })
    },
    async run({ topics = [], restart = null } = {}) {
      const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
      const consumer =
        restart ??
        createConsumer({
          cluster,
          groupId: GROUP,
          logger: newLogger(),
          instrumentationEmitter,
          metadataMaxAge: 300000,
          retry: { retries: 0 },
        })
      for (const topic of topics) await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })
      const group = start.mock.instances[start.mock.instances.length - 1].consumerGroup
      start.mockRestore()
      return { consumer, group }
    },
    assign(group, assignment) {
      const built = createManager(
        { cache: entry().offsets, ledger: group.operations },
        { assignment, obligations: group.obligations, topicIds: ids }
      )
      group.offsetManager = built.manager
      return built
    },
  }
}
