const Subscription = require('../subscription')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const ResetObligations = require('../offsetManager/resetObligations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createPatternGroup } = require('./fixtures/patternGroup')
const { newLogger, waitFor } = require('testHelpers')

const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'

/** @returns {Promise<void>} yields one event-loop turn */
const turn = () => new Promise(setImmediate)

/**
 * What the next Fetch of a partition would ask for.
 * @param {object} harness a `createPatternGroup` result
 * @param {string} topic
 * @param {number} [partition]
 * @returns {string}
 */
const nextOffset = (harness, topic, partition = 0) =>
  harness.group.offsetManager.nextOffset(topic, partition).toString()

/** Builds accepted discovery metadata without network work.
 * @param {string[]} names
 * @returns {object}
 */
const metadata = (names) => ({
  topicMetadata: names.map((topic) => ({ topic, isInternal: false })),
})

describe('applied subscription revisions', () => {
  test('keeps applied configuration through an in-flight removal and coalesces readdition', () => {
    const subscription = new Subscription()
    subscription.add({ topics: [/orders/], policy: { kind: 'earliest' } })
    subscription.evaluate(metadata(['orders-1']))
    subscription.apply(subscription.pin())
    const initial = subscription.applied.revision
    subscription.evaluate(metadata([]))
    const removing = subscription.pin()
    expect(subscription.applied.names).toEqual(['orders-1'])
    expect(subscription.applied.topicConfigurations.get('orders-1').offsetReset.kind).toBe(
      'earliest'
    )
    subscription.evaluate(metadata(['orders-1', 'orders-2']))
    expect(subscription.pin()).toBe(removing)
    expect(subscription.apply(removing)).toBe(true)
    expect(subscription.applied.names).toEqual([])
    expect(subscription.needsApply()).toBe(true)
    const following = subscription.pin()
    expect(following.revision).toBeGreaterThan(initial)
    expect(following.names).toEqual(['orders-1', 'orders-2'])
    subscription.apply(following)
    expect(subscription.needsApply()).toBe(false)
  })

  test('a failed or retired round never publishes and a rebuild keeps declarations', () => {
    const subscription = new Subscription()
    subscription.add({ topics: [/orders/], policy: { kind: 'latest' } })
    subscription.evaluate(metadata(['orders-1']))
    const failed = subscription.pin()
    subscription.abandon(failed)
    expect(subscription.apply(failed)).toBe(false)
    const stopped = subscription.pin()
    subscription.retireRun()
    expect(subscription.apply(stopped)).toBe(false)
    expect(subscription.hasPatterns()).toBe(true)
    expect(subscription.needsApply()).toBe(true)
    subscription.clear()
    expect(subscription.literals.size).toBe(0)
    expect(subscription.patterns).toEqual([])
    expect(subscription.desired.names).toEqual([])
    expect(subscription.applied.topicConfigurations.size).toBe(0)
  })

  test('unchanged metadata preserves revision and no removed-name history accumulates', () => {
    const subscription = new Subscription()
    subscription.add({ topics: [/topic/], policy: { kind: 'earliest' } })
    const first = subscription.evaluate(metadata(['topic-0']))
    expect(subscription.evaluate(metadata(['topic-0'])).revision).toBe(first.revision)
    for (let index = 0; index < 1100; index++) {
      subscription.evaluate(metadata([`topic-${index}`]))
      subscription.apply(subscription.pin())
      expect(subscription.desired.topicConfigurations.size).toBe(1)
      expect(subscription.applied.topicConfigurations.size).toBe(1)
    }
    subscription.evaluate(metadata([]))
    subscription.apply(subscription.pin())
    expect(subscription.desired.topicConfigurations.size).toBe(0)
    expect(subscription.applied.topicConfigurations.size).toBe(0)
    expect(subscription.patterns).toHaveLength(1)
  })

  test('initial and continuous incarnations keep commits while post-initial additions owe reset', () => {
    const subscription = new Subscription()
    subscription.add({ topics: [/topic/], policy: { kind: 'earliest' } })
    const initial = { topicMetadata: [{ topic: 'topic-0', topicId: 'initial-id' }] }
    subscription.evaluate(initial)
    expect(subscription.observe(initial)).toEqual([])
    expect(subscription.observe(initial)).toEqual([])
    const added = {
      topicMetadata: [...initial.topicMetadata, { topic: 'topic-1', topicId: 'new-id' }],
    }
    subscription.evaluate(added)
    expect(subscription.observe(added)).toEqual([{ topic: 'topic-1', topicId: 'new-id' }])
    expect(subscription.observe(added)).toEqual([])
  })

  test('the oldest of more than 1024 dropped names returns without any continuity proof', () => {
    const subscription = new Subscription()
    subscription.add({ topics: [/topic/], policy: { kind: 'earliest' } })
    for (let index = 0; index < 1100; index++) {
      const current = { topicMetadata: [{ topic: `topic-${index}`, topicId: `id-${index}` }] }
      subscription.evaluate(current)
      subscription.observe(current)
      subscription.apply(subscription.pin())
      expect(subscription.observed.size).toBe(1)
    }
    const recreated = { topicMetadata: [{ topic: 'topic-0', topicId: 'recreated-id' }] }
    subscription.evaluate(recreated)
    expect(subscription.observe(recreated)).toEqual([{ topic: 'topic-0', topicId: 'recreated-id' }])
    subscription.evaluate(metadata([]))
    subscription.observe(metadata([]))
    expect(subscription.observed.size).toBe(0)
    subscription.clear()
    expect(subscription.observed.size).toBe(0)
  })
})

describe('declaration checkpoints and retained debts', () => {
  test('a restored checkpoint puts the declarations back and retires metadata issued meanwhile', () => {
    const subscription = new Subscription()
    subscription.add({ topics: ['orders-0'], policy: { kind: 'earliest' } })
    subscription.evaluate(metadata([]))
    const checkpoint = subscription.checkpoint()
    const ticket = subscription.captureMetadata()

    subscription.add({ topics: [/orders-/], policy: { kind: 'latest' } })
    subscription.evaluate(metadata(['orders-1']))
    expect(subscription.hasPatterns()).toBe(true)
    subscription.restore(checkpoint)

    expect(subscription.hasPatterns()).toBe(false)
    expect([...subscription.literals.keys()]).toEqual(['orders-0'])
    expect(subscription.desired.names).toEqual(['orders-0'])
    expect(ticket.isCurrent()).toBe(false)
  })

  test('configurations keep prototype-looking names as own keys under the offset manager shape', () => {
    const subscription = new Subscription()
    subscription.add({ topics: ['__proto__'], policy: { kind: 'by_duration', durationMs: 5 } })
    const configurations = Subscription.configurationsOf(subscription.evaluate(metadata([])))

    expect(Object.keys(configurations)).toEqual(['__proto__'])
    expect(Object.getOwnPropertyDescriptor(configurations, '__proto__').value).toEqual({
      fromBeginning: false,
      resetPolicy: { kind: 'by_duration', durationMs: 5 },
    })
  })

  test('retaining a set of topics drops every other debt and leaves the kept ones as they are', () => {
    const obligations = new ResetObligations()
    obligations.owe('kept', ID_A)
    obligations.discharge('kept', 0, ID_A)
    obligations.owe('dropped', ID_B)

    obligations.retain(new Set(['kept']))

    expect(obligations.size()).toBe(1)
    expect(obligations.isOwed('kept', 0)).toBe(false)
    expect(obligations.isOwed('kept', 1)).toBe(true)
    expect(obligations.current('dropped')).toBeUndefined()
  })
})

describe('discovery through a consumer group', () => {
  test('a removed regex-only topic retires its state only after apply', async () => {
    const harness = createPatternGroup({
      declarations: [/^orders-/, 'audit'],
      catalog: { 'orders-0': ID_A, audit: ID_B },
    })
    const { group } = harness
    await harness.refresh()
    await harness.round()
    group.pause([{ topic: 'orders-0', partitions: [0] }])
    group.pause([{ topic: 'audit' }])
    group.resume([{ topic: 'audit', partitions: [1] }])
    group.seek({ topic: 'orders-0', partition: 1, offset: '5' })
    group.preferredReadReplicasPerTopicPartition['orders-0'] = {
      0: { nodeId: 2, expireAt: Date.now() + 60000 },
    }

    harness.drop('orders-0')
    await harness.refresh()
    const gate = harness.gate()
    const incremental = harness.round({ rebuildOffsets: false })
    try {
      await waitFor(() => harness.rebalances.length === 2)
      expect(harness.rebalances[1]).toEqual(['audit'])
      expect(harness.applied()).toEqual(['audit', 'orders-0'])
      expect(group.topicConfigurations['orders-0']).toBeDefined()
      expect(group.subscriptionState.isPaused('orders-0', 0)).toBe(true)
      expect([...group.seekOffset]).toHaveLength(1)
      expect(group.preferredReadReplicasPerTopicPartition['orders-0']).toBeDefined()
    } finally {
      gate.release()
      await incremental
    }

    expect(harness.applied()).toEqual(['audit'])
    expect(group.topicConfigurations['orders-0']).toBeUndefined()
    expect(group.subscriptionState.isPaused('orders-0', 0)).toBe(false)
    expect([...group.seekOffset]).toHaveLength(0)
    expect(group.preferredReadReplicasPerTopicPartition['orders-0']).toBeUndefined()
    expect(group.subscriptionState.isPaused('audit', 0)).toBe(true)
    expect(group.subscriptionState.isPaused('audit', 1)).toBe(false)
    expect(harness.cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
  })

  test('desired changes during a running round survive its ignored rejoin request', async () => {
    const harness = createPatternGroup({ catalog: { 'orders-1': ID_A } })
    const { group } = harness
    const coordinator = new RebalanceCoordinator({
      runner: { stopRequested: false, onCrash: jest.fn() },
      consumerGroup: group,
      fetchManager: { start: jest.fn(async () => {}), stop: jest.fn(async () => {}) },
      heartbeatManager: { ensureStarted: jest.fn() },
      pollTimer: { reset: jest.fn() },
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
      groupId: 'group-a',
    })
    group.attach({ rebalanceCoordinator: coordinator, heartbeatManager: { ensureStarted() {} } })
    await harness.refresh()

    const gate = harness.gate()
    const chain = coordinator.runRound('subscription-changed', 'revoked')
    try {
      await waitFor(() => harness.rebalances.length === 1)
      harness.drop('orders-1')
      await harness.refresh()
      harness.set('orders-1', ID_A)
      harness.set('orders-2', ID_B)
      await harness.refresh()

      expect(coordinator.isRebalancing()).toBe(true)
      coordinator.requestRejoin({
        kind: 'revoked',
        reason: 'subscription-changed',
        error: new Error('x'),
      })
      expect(harness.rebalances).toHaveLength(1)
      expect(harness.applied()).toEqual([])
    } finally {
      gate.release()
      await chain
    }

    expect(harness.rebalances).toEqual([['orders-1'], ['orders-1', 'orders-2']])
    expect(harness.applied()).toEqual(['orders-1', 'orders-2'])
    expect(group.subscriptionPending()).toBe(false)
  })

  test('a fetch opportunity asks for one revoked reconciliation while the revision is unapplied', async () => {
    const harness = createPatternGroup({ catalog: { 'orders-1': ID_A } })
    const { group } = harness
    const requestRejoin = jest.fn()
    group.rebalanceCoordinator = { isRebalancing: () => false, requestRejoin }
    await harness.refresh()
    await harness.round()
    await group.prepareFetch(0, group.idleWait, group.ownership.token())
    expect(requestRejoin).not.toHaveBeenCalled()

    harness.set('orders-2', ID_B)
    await harness.refresh()
    await group.prepareFetch(0, group.idleWait, group.ownership.token())
    expect(requestRejoin).toHaveBeenCalledTimes(1)
    expect(requestRejoin).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'revoked', reason: 'subscription-changed' })
    )

    await harness.round()
    await group.prepareFetch(0, group.idleWait, group.ownership.token())
    expect(requestRejoin).toHaveBeenCalledTimes(1)
  })

  test('a retired run never publishes its pinned revision', async () => {
    const harness = createPatternGroup({ catalog: { 'orders-1': ID_A } })
    await harness.refresh()
    const gate = harness.gate()
    const pending = harness.round().catch((error) => error)
    await waitFor(() => harness.rebalances.length === 1)

    harness.subscription.retireRun()
    gate.release()
    const outcome = await pending

    expect(outcome).toBeInstanceOf(Error)
    expect(harness.applied()).toEqual([])
    expect(harness.subscription.needsApply()).toBe(true)
  })

  test('the oldest recreated discovery ignores a stale group commit after more than 1024 removals', async () => {
    const harness = createPatternGroup({ catalog: { 'orders-0': ID_A }, committed: '99' })
    await harness.refresh()
    await harness.round()
    expect(harness.applied()).toEqual(['orders-0'])
    await harness.group.offsetManager.resolveOffsets()
    expect(nextOffset(harness, 'orders-0')).toBe('99')

    for (let index = 1; index <= 1100; index++) {
      harness.catalog.clear()
      harness.set(`orders-${index}`, `id-${index}`)
      await harness.refresh()
      await harness.round()
      expect(harness.subscription.observed.size).toBe(1)
      expect(harness.group.obligations.size()).toBeLessThanOrEqual(1)
    }

    harness.catalog.clear()
    harness.set('orders-0', ID_B)
    await harness.refresh()
    await harness.round()
    await harness.group.offsetManager.resolveOffsets()

    expect(harness.brokerHolds('orders-0', 0)).toBe('99')
    expect(nextOffset(harness, 'orders-0')).toBe('0')
    expect(harness.group.obligations.size()).toBe(1)

    harness.subscription.clear()
    expect(harness.subscription.observed.size).toBe(0)
  })

  test.each([
    [undefined, false],
    ['00000000-0000-0000-0000-000000000000', false],
    [undefined, true],
  ])('withholds unresolved addition %s with retained id %s', async (id, retained) => {
    const harness = createPatternGroup({ catalog: { 'orders-0': ID_A }, committed: '99' })
    const { group, cluster } = harness
    await harness.refresh()
    await harness.round()
    harness.set('orders-1', id)
    await harness.refresh()
    await harness.round()
    const topicId = cluster.topicId
    if (retained) cluster.topicId = (topic) => (topic === 'orders-1' ? ID_A : topicId(topic))
    await group.offsetManager.resolveOffsets()
    expect(
      group.coordinator.offsetFetch.mock.calls.flatMap(([request]) =>
        request.topics.map(({ topic }) => topic)
      )
    ).not.toContain('orders-1')
    expect(group.obligations.current('orders-1')).toBeNull()
    expect(nextOffset(harness, 'orders-0')).toBe('99')
    // A local position must not bypass the unresolved-incarnation gate.
    group.offsetManager.committedOffsets()['orders-1'][0] = '0'
    group.offsetManager.committedOffsets()['orders-1'][1] = '0'
    expect(group.buildFetchRequests([{ topic: 'orders-1', partitions: [0, 1] }])).toEqual([])
    cluster.topicId = topicId
    harness.set('orders-1', ID_B)
    await harness.refresh()
    await harness.round()
    await group.offsetManager.resolveOffsets()
    expect(nextOffset(harness, 'orders-1')).toBe('0')
    expect(group.buildFetchRequests([{ topic: 'orders-1', partitions: [0, 1] }])).toHaveLength(1)
  })

  test('discovery debt survives a rebuild before its first durable commit', async () => {
    const harness = createPatternGroup({ catalog: { 'orders-0': ID_A }, committed: '99' })
    await harness.refresh()
    await harness.round()
    expect(harness.applied()).toEqual(['orders-0'])
    await harness.group.offsetManager.resolveOffsets()
    expect(nextOffset(harness, 'orders-0')).toBe('99')

    harness.drop('orders-0')
    await harness.refresh()
    await harness.round()
    harness.set('orders-0', ID_B)
    await harness.refresh()
    await harness.round()
    await harness.group.offsetManager.resolveOffsets()
    expect(nextOffset(harness, 'orders-0')).toBe('0')
    expect(harness.brokerHolds('orders-0', 0)).toBe('99')

    await harness.round()
    await harness.group.offsetManager.resolveOffsets()
    expect(nextOffset(harness, 'orders-0')).toBe('0')
    expect(harness.brokerHolds('orders-0', 0)).toBe('99')
    expect(harness.group.coordinator.offsetCommit).not.toHaveBeenCalled()

    await harness.group.commitOffsets({
      topics: [{ topic: 'orders-0', partitions: [{ partition: 0, offset: '4' }] }],
    })
    expect(harness.brokerHolds('orders-0', 0)).toBe('4')
    await harness.round()
    await harness.group.offsetManager.resolveOffsets()
    expect(nextOffset(harness, 'orders-0')).toBe('4')
  })

  test.each(['seek', 'commit'])(
    'a current-incarnation %s wins over a discovery reset still being answered',
    async (kind) => {
      const harness = createPatternGroup({ catalog: { 'orders-0': ID_A }, committed: '99' })
      await harness.refresh()
      await harness.round()
      harness.drop('orders-0')
      await harness.refresh()
      await harness.round()
      harness.set('orders-0', ID_B)
      await harness.refresh()
      await harness.round()

      const held = {}
      harness.cluster.fetchTopicsOffset.mockImplementationOnce(
        (topics) =>
          new Promise((resolve) => {
            held.release = () =>
              resolve(
                topics.map(({ topic, partitions }) => ({
                  topic,
                  partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
                }))
              )
          })
      )
      const resolving = harness.group.offsetManager.resolveOffsets()
      for (let turns = 0; turns < 50 && held.release === undefined; turns++) await turn()
      expect(held.release).toBeDefined()
      if (kind === 'seek') {
        await harness.group.offsetManager.seek({ topic: 'orders-0', partition: 0, offset: '7' })
      } else {
        harness.group.offsetManager.resolveOffset({ topic: 'orders-0', partition: 0, offset: '6' })
        await harness.group.commitOffsets()
      }
      held.release()
      await resolving
      await turn()

      expect(nextOffset(harness, 'orders-0')).toBe('7')
    }
  )
})
