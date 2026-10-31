const ConsumerGroup = require('../consumerGroup')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const Cluster = require('../../cluster')
const IdleWait = require('../idleWait')
const createRetry = require('../../retry')
const { stampGeneration } = require('../rebalanceErrorContract')
const { KafkaJSConnectionError } = require('../../errors')
const { newLogger } = require('testHelpers')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { watch, flushAll } = require('../../producer/__tests__/fixtures/bufferHarness')

/** @returns {object} actual round owner with non-network lifecycle boundaries */
const buildGroup = () => {
  const group = Object.create(ConsumerGroup.prototype)
  Object.assign(group, {
    idleWait: new IdleWait(),
    rebalanceTimeout: 1000,
    sessionTimeout: 45000,
    topics: ['A'],
    topicsSubscribed: ['A'],
    cluster: { allowAutoTopicCreation: true },
    logger: { warn: jest.fn() },
    subscriptionState: { clearPauses: jest.fn() },
    seekOffset: new Map(),
    rebalanceCoordinator: { runner: { stopRequested: false } },
    groupMembership: {
      kind: 'classic',
      memberId: 'member',
      generationId: 1,
      onStopRequested: jest.fn(),
    },
    retrier: createRetry({ initialRetryTime: 2000, factor: 0, retries: 1 }),
    backoff: { initial: () => 2000 },
    coordinatorLookup: { releaseUnfenced() {} },
  })
  return group
}

/** @param {Promise<*>} task @returns {object} settlement observed without an unhandled rejection */
const observe = (task) => {
  const state = { settled: false, value: null }
  state.done = task.then(
    (value) => {
      state.settled = true
      state.value = value
    },
    (error) => {
      state.settled = true
      state.value = error
    }
  )
  return state
}

/** @param {object} group @param {object} effects @returns {object} counted OffsetManager boundary */
const managerFor = (group, effects) => ({
  useMembership: jest.fn(() => {
    effects.membership++
  }),
  clearPendingSeeks: jest.fn(),
  commitOffsets: jest.fn(async (_, { guard } = {}) => {
    const refusal = guard?.()
    if (refusal) throw refusal
    effects.commit++
    throw stampGeneration(
      Object.assign(new Error('stale epoch'), { type: 'STALE_MEMBER_EPOCH' }),
      1
    )
  }),
})

/** @param {object} group @returns {void} terminal stop through the real lifecycle method */
const stop = (group) => {
  group.rebalanceCoordinator.runner.stopRequested = true
  group.onStopRequested()
}

describe('round-owned retry and stale-epoch waits', () => {
  beforeEach(() => {
    jest.useFakeTimers()
    jest.setSystemTime(0)
    jest.spyOn(performance, 'now').mockImplementation(() => Date.now())
  })
  afterEach(() => {
    jest.restoreAllMocks()
    jest.clearAllTimers()
    jest.useRealTimers()
  })

  it.each(['expiry', 'stop', 'replacement'])(
    'settles during retry backoff on %s without later effects',
    async (reason) => {
      const group = buildGroup()
      const effects = { frame: 0, membership: 0, apply: 0 }
      group.groupMembership.rebalance = jest.fn(async () => {
        if (group.groupMembership.rebalance.mock.calls.length === 1) {
          throw Object.assign(new Error('rebalance'), { type: 'REBALANCE_IN_PROGRESS' })
        }
        effects.frame++
        effects.membership++
        return { assignment: {} }
      })
      const round = group.beginRebalanceRound()
      const task = observe(
        group.joinAndSync(round).then((value) => {
          effects.apply++
          return value
        })
      )
      await jest.advanceTimersByTimeAsync(0)
      expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(1)
      let replacement
      if (reason === 'expiry') await jest.advanceTimersByTimeAsync(1000)
      else {
        await jest.advanceTimersByTimeAsync(50)
        if (reason === 'stop') stop(group)
        else replacement = group.beginRebalanceRound()
        await jest.advanceTimersByTimeAsync(0)
      }
      const atBoundary = { settled: task.settled, timers: jest.getTimerCount(), error: task.value }
      try {
        await jest.advanceTimersByTimeAsync(5000)
        await task.done
        expect(atBoundary.settled).toBe(true)
        expect(atBoundary.timers).toBe(0)
        expect(atBoundary.error.type).toBe(
          reason === 'stop' ? 'CONSUMER_STOPPING' : 'JOIN_DEADLINE_EXCEEDED'
        )
        expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(1)
        expect(effects).toEqual({ frame: 0, membership: 0, apply: 0 })
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        round.retire()
        replacement?.retire()
      }
    }
  )

  it.each(['expiry', 'stop', 'replacement'])(
    'cleans the classic outage backoff on %s',
    async (reason) => {
      const group = buildGroup()
      const original = new (require('../../errors').KafkaJSConnectionError)('connection lost')
      group.groupMembership.rebalance = jest.fn(async () => {
        throw original
      })
      const round = group.beginRebalanceRound()
      const task = observe(group.joinAndSync(round))
      await jest.advanceTimersByTimeAsync(0)
      expect(group.logger.warn).toHaveBeenCalledTimes(1)
      let replacement
      if (reason === 'expiry') await jest.advanceTimersByTimeAsync(1000)
      else {
        await jest.advanceTimersByTimeAsync(50)
        if (reason === 'stop') stop(group)
        else replacement = group.beginRebalanceRound()
        await jest.advanceTimersByTimeAsync(0)
      }
      const atBoundary = { settled: task.settled, timers: jest.getTimerCount() }
      try {
        await jest.advanceTimersByTimeAsync(5000)
        await task.done
        expect(atBoundary).toEqual({ settled: true, timers: 0 })
        expect(group.groupMembership.rebalance).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        round.retire()
        replacement?.retire()
      }
    }
  )

  it('retains the error rules and changes retry tokens within one budget', async () => {
    const group = buildGroup()
    group.retrier = createRetry({ initialRetryTime: 100, factor: 0, retries: 1 })
    const contexts = []
    group.groupMembership.rebalance = jest.fn(async ({ round }) => {
      contexts.push(round)
      if (contexts.length === 1)
        throw Object.assign(new Error('rebalance'), { type: 'REBALANCE_IN_PROGRESS' })
      return { assignment: {} }
    })
    const round = group.beginRebalanceRound()
    const task = observe(group.joinAndSync(round))
    await jest.advanceTimersByTimeAsync(100)
    await task.done
    expect(task.value).toEqual({ assignment: {} })
    expect(contexts[0].leaderAttempt).toBe(round.leaderAttempt)
    expect(contexts[1].leaderAttempt).not.toBe(contexts[0].leaderAttempt)
    expect(contexts.map((context) => context.roundDeadlineAt)).toEqual([1000, 1000])
    const original = Object.assign(new Error('authorization'), { retriable: false })
    group.groupMembership.rebalance.mockRejectedValue(original)
    await expect(group.joinAndSync(round)).rejects.toBe(original)
    group.groupMembership.rebalance.mockRejectedValue(
      Object.assign(new Error('rebalance'), { type: 'REBALANCE_IN_PROGRESS' })
    )
    const exhausted = observe(group.joinAndSync(round))
    await jest.advanceTimersByTimeAsync(100)
    await exhausted.done
    expect(exhausted.value.name).toBe('KafkaJSNumberOfRetriesExceeded')
    expect(jest.getTimerCount()).toBe(0)
    round.retire()
  })

  it('fences retained local callbacks after round replacement', async () => {
    const group = buildGroup()
    let callbacks, release
    group.generatePartitionsPerSubscribedTopic = jest.fn()
    group.growSubscription = jest.fn()
    group.groupMembership.rebalance = jest.fn((options) => {
      callbacks = options
      return new Promise((resolve) => {
        release = resolve
      })
    })
    const round = group.beginRebalanceRound()
    const task = observe(group.joinAndSync(round))
    await jest.advanceTimersByTimeAsync(0)
    const replacement = group.beginRebalanceRound()
    await jest.advanceTimersByTimeAsync(0)
    try {
      release({ assignment: {} })
      await task.done
      expect(() => callbacks.beforeSync()).toThrow()
      expect(() => callbacks.growSubscription(['B'])).toThrow()
      expect(group.generatePartitionsPerSubscribedTopic).not.toHaveBeenCalled()
      expect(group.growSubscription).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      round.retire()
      replacement.retire()
    }
  })

  it.each(['expiry', 'stop', 'replacement'])(
    'retires a late stale-epoch poll on %s before updating a manager',
    async (reason) => {
      const group = buildGroup()
      group.groupMembership.kind = 'consumer'
      group.autoCommit = true
      const effects = { membership: 0, commit: 0, apply: 0 }
      const oldManager = managerFor(group, effects)
      let release
      oldManager.commitOffsets.mockImplementationOnce(
        (_, { guard }) =>
          new Promise((_, reject) => {
            release = () => {
              const refusal = guard?.()
              reject(
                refusal ||
                  stampGeneration(
                    Object.assign(new Error('stale epoch'), { type: 'STALE_MEMBER_EPOCH' }),
                    1
                  )
              )
            }
          })
      )
      group.offsetManager = oldManager
      const round = group.beginRebalanceRound()
      await jest.advanceTimersByTimeAsync(750)
      const task = observe(
        round.wait(async () => {
          await group.commitBeforeRelease([{ topic: 'A', partitions: [0] }], { round })
          const refusal = round.guard()
          if (refusal) throw refusal
          effects.apply++
        })
      )
      await jest.advanceTimersByTimeAsync(50)
      release()
      await jest.advanceTimersByTimeAsync(0)
      let replacement
      let liveManager
      if (reason === 'expiry') await jest.advanceTimersByTimeAsync(200)
      else {
        await jest.advanceTimersByTimeAsync(50)
        if (reason === 'stop') stop(group)
        else {
          replacement = group.beginRebalanceRound()
          liveManager = managerFor(group, effects)
          group.offsetManager = liveManager
        }
        await jest.advanceTimersByTimeAsync(0)
      }
      const atBoundary = {
        settled: task.settled,
        timers: jest.getTimerCount(),
        membership: effects.membership,
      }
      try {
        group.generationId = 2
        await jest.advanceTimersByTimeAsync(5000)
        await task.done
        expect(atBoundary.settled).toBe(true)
        expect(atBoundary.timers).toBe(0)
        expect(effects.membership).toBe(atBoundary.membership)
        expect(effects).toEqual({ membership: 1, commit: 0, apply: 0 })
        expect(oldManager.commitOffsets).toHaveBeenCalledTimes(1)
        if (liveManager) {
          expect(liveManager.useMembership).not.toHaveBeenCalled()
          expect(liveManager.commitOffsets).not.toHaveBeenCalled()
        }
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        round.retire()
        replacement?.retire()
      }
    }
  )

  it('spends only the remaining budget when no newer epoch arrives before expiry', async () => {
    const group = buildGroup()
    group.groupMembership.kind = 'consumer'
    group.autoCommit = true
    const effects = { membership: 0, commit: 0 }
    group.offsetManager = managerFor(group, effects)
    const round = group.beginRebalanceRound()
    await jest.advanceTimersByTimeAsync(850)
    const task = observe(group.commitBeforeRelease([{ topic: 'A', partitions: [0] }], { round }))
    await jest.advanceTimersByTimeAsync(150)
    const atBoundary = { settled: task.settled, timers: jest.getTimerCount() }
    try {
      group.generationId = 2
      await jest.advanceTimersByTimeAsync(5000)
      await task.done
      expect(atBoundary).toEqual({ settled: true, timers: 0 })
      expect(effects).toEqual({ membership: 1, commit: 1 })
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      round.retire()
    }
  })

  it('allows a newer epoch inside the original budget and preserves standalone leave waiting', async () => {
    const group = buildGroup()
    group.groupMembership.kind = 'consumer'
    group.autoCommit = true
    const effects = { membership: 0, commit: 0 }
    group.offsetManager = managerFor(group, effects)
    group.offsetManager.commitOffsets
      .mockImplementationOnce(async () => {
        throw stampGeneration(
          Object.assign(new Error('stale epoch'), { type: 'STALE_MEMBER_EPOCH' }),
          1
        )
      })
      .mockResolvedValueOnce(undefined)
    const round = group.beginRebalanceRound()
    await jest.advanceTimersByTimeAsync(800)
    const scoped = observe(group.commitBeforeRelease([{ topic: 'A', partitions: [0] }], { round }))
    await jest.advanceTimersByTimeAsync(0)
    group.generationId = 2
    await jest.advanceTimersByTimeAsync(100)
    expect(scoped.settled).toBe(true)
    expect(group.offsetManager.useMembership).toHaveBeenCalledTimes(2)
    expect(group.offsetManager.commitOffsets).toHaveBeenCalledTimes(2)
    expect(jest.getTimerCount()).toBe(0)
    round.retire()
    const error = stampGeneration(
      Object.assign(new Error('stale epoch'), { type: 'STALE_MEMBER_EPOCH' }),
      2
    )
    const standalone = observe(group.awaitFreshEpochFor(error, Date.now() + 1000))
    await jest.advanceTimersByTimeAsync(0)
    group.generationId = 3
    await jest.advanceTimersByTimeAsync(100)
    await standalone.done
    expect(standalone.value).toBe(true)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('a coordinator lookup the stop abandons', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  /** @returns {Promise<object>} a classic member whose join is about to look up the coordinator */
  const member = async () => {
    const schedule = new Schedule()
    const wire = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], schedule })
    )
    const { clientId, brokers, socketFactory } = wire.kafkaOptions()
    const cluster = new Cluster({
      logger: newLogger(),
      clientId,
      brokers,
      socketFactory,
      connectionTimeout: 1000,
      requestTimeout: 5000,
      retry: { retries: 5, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
    })
    await cluster.connect()
    const group = buildGroup()
    group.cluster = cluster
    const protocol = new ClassicGroupProtocol({
      cluster,
      groupId: 'group',
      sessionTimeout: 6000,
      rebalanceTimeout: 1000,
      assigners: [],
      instrumentationEmitter: { emit: jest.fn() },
      logger: newLogger(),
    })
    return { schedule, wire, cluster, group, protocol }
  }

  /** @param {object} owner the member @returns {object} the watched join, started on a fresh round */
  const join = ({ group, protocol }) =>
    watch(
      protocol.rebalance({
        topics: ['A'],
        topicsSubscribed: ['A'],
        beforeSync() {},
        round: group.beginRebalanceRound().attempt(),
      })
    )

  it('does not ask again when a held lookup fails after the stop', async () => {
    const owner = await member()
    const gate = owner.schedule.hold({ api: 'FindCoordinator' })
    const joining = join(owner)
    await gate.arrived()
    const frames = owner.wire.requests().length

    stop(owner.group)
    gate.fail(new KafkaJSConnectionError('connection reset'))
    await jest.advanceTimersByTimeAsync(1000)
    await flushAll()

    expect(joining.state).toBe('rejected')
    expect(owner.wire.requests()).toHaveLength(frames)
    expect(jest.getTimerCount()).toBe(0)
    owner.wire.assertClean()
    await owner.cluster.disconnect({ drainTimeout: 0 })
  })

  it('stops dialling a coordinator that stays down', async () => {
    const owner = await member()
    await owner.cluster.brokerPool.seedBroker.disconnect()
    owner.wire.down(0)
    const joining = join(owner)
    await jest.advanceTimersByTimeAsync(15)
    await flushAll()
    const sockets = owner.wire.sockets().length
    expect(sockets).toBeGreaterThan(1)

    stop(owner.group)
    await jest.advanceTimersByTimeAsync(1000)
    await flushAll()

    expect(joining.state).toBe('rejected')
    expect(owner.wire.sockets()).toHaveLength(sockets)
    expect(jest.getTimerCount()).toBe(0)
    await owner.cluster.disconnect({ drainTimeout: 0 })
  })
})
