const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const Cluster = require('../../cluster')
const createFetcher = require('../fetcher')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')
const { createFetchGroup, fetchResponse, NODE_ID, TOPIC } = require('./fixtures/fetchHarness')

/** @returns {object} an idle group with no transport or offset work */
const idleGroup = () => {
  const logger = {
    namespace() {
      return this
    },
    info() {},
    debug() {},
    warn() {},
  }
  const group = new ConsumerGroup({
    cluster: { refreshMetadataIfNecessary: async () => {} },
    groupId: 'idle-group',
    topics: ['orders'],
    topicConfigurations: { orders: {} },
    logger,
    instrumentationEmitter: { emit() {} },
    assigners: [],
    sessionTimeout: 45000,
    rebalanceTimeout: 60000,
    maxWaitTimeInMs: 400,
  })
  group.checkForStaleAssignment = () => {}
  group.filterPartitionsByNode = (_, assigned) => assigned
  group.seekOffsets = async () => {}
  group.validatePositions = async () => {}
  group.buildFetchRequests = () => []
  group.subscriptionState.assign([{ topic: 'orders', partitions: [0] }])
  group.pause([{ topic: 'orders' }])
  return group
}

/** @returns {Promise<void>} drains continuations before idle registration */
const entered = async () => {
  for (let n = 0; n < 12; n++) await Promise.resolve()
}

describe('interruptible idle fetch', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.clearAllTimers()
    jest.useRealTimers()
  })
  it.each(['resume', 'stop'])('%s ends the idle wait before its timer', async (action) => {
    const group = idleGroup()
    if (action === 'stop') group.maxWaitTime = 5000
    let settled = false
    const pending = group.fetch(0).then(() => {
      settled = true
    })
    await entered()
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(50)
    if (action === 'resume') group.resume([{ topic: 'orders', partitions: [0] }])
    else group.onStopRequested()
    await entered()
    const observed = settled
    await jest.advanceTimersByTimeAsync(5000)
    await pending
    expect(observed).toBe(true)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('idle waiter ownership', () => {
  let IdleWait
  beforeEach(() => {
    jest.useFakeTimers()
    IdleWait = require('../idleWait')
  })
  afterEach(() => {
    jest.restoreAllMocks()
    jest.clearAllTimers()
    jest.useRealTimers()
  })
  it('wakes all waiters once without saving a wake credit', async () => {
    const idle = new IdleWait()
    const settled = jest.fn()
    const pending = [idle.wait(400).then(settled), idle.wait(400).then(settled)]
    expect(jest.getTimerCount()).toBe(2)
    idle.wake()
    idle.wake()
    await Promise.all(pending)
    expect(settled).toHaveBeenCalledTimes(2)
    expect(jest.getTimerCount()).toBe(0)
    let fresh = false
    const next = idle.wait(400).then(() => {
      fresh = true
    })
    await entered()
    expect(fresh).toBe(false)
    idle.dispose()
    await next
    expect(jest.getTimerCount()).toBe(0)
  })
  it('an old scheduled callback cannot settle a fresh run waiter', async () => {
    const callbacks = []
    const original = global.setTimeout
    jest.spyOn(global, 'setTimeout').mockImplementation((callback, ms) => {
      callbacks.push(callback)
      return original(callback, ms)
    })
    const old = new IdleWait()
    const oldSettled = jest.fn()
    const pending = [old.wait(5000).then(oldSettled), old.wait(5000).then(oldSettled)]
    const oldCallbacks = [...callbacks]
    old.wake()
    old.dispose()
    await Promise.all(pending)
    const fresh = new IdleWait()
    const newSettled = jest.fn()
    const next = fresh.wait(5000).then(newSettled)
    oldCallbacks.forEach((callback) => callback())
    await entered()
    expect(oldSettled).toHaveBeenCalledTimes(2)
    expect(newSettled).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(1)
    fresh.dispose()
    await next
    await old.wait(5000)
    expect(newSettled).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })
  it('natural timeout unlinks and a thousand later wakes schedule nothing', async () => {
    const idle = new IdleWait()
    const settled = jest.fn()
    const pending = idle.wait(400).then(settled)
    await jest.advanceTimersByTimeAsync(400)
    await pending
    for (let n = 0; n < 1000; n++) idle.wake()
    expect(settled).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })
})

const fakeTimers = () => jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })

/** @returns {Promise<void>} lets virtual time and the continuations it releases run */
const settle = async () => {
  await jest.advanceTimersByTimeAsync(0)
  await entered()
}

/** @returns {{promise: Promise<*>, release: Function, fail: Function}} a step held open from outside */
const gate = () => {
  const held = {}
  held.promise = new Promise((resolve, reject) => {
    held.release = resolve
    held.fail = reject
  })
  return held
}

/** Lets continuations, virtual time and real immediates run. @returns {Promise<void>} */
const drain = async (rounds = 6) => {
  for (let n = 0; n < rounds; n++) {
    await new Promise(setImmediate)
    await settle()
  }
}

/**
 * Drains until a call settles, so a runaway preparation fails an assertion instead of hanging.
 * @param {Promise<*>} pending
 * @returns {Promise<*>} what it resolved with
 */
const finish = async (pending) => {
  const state = { done: false }
  const tracked = pending.then((value) => {
    state.done = true
    return value
  })
  for (let n = 0; n < 200 && !state.done; n++) await drain(1)
  expect(state.done).toBe(true)
  return tracked
}

const partitionsOf = (topicPartitions) => topicPartitions.map(({ partitions }) => [...partitions])

/**
 * Records every preparation step of a group, in order, and lets a spec hold or follow one.
 * @param {object} h the fetch harness
 * @param {{before?: Record<string, (n: number) => *>, after?: Record<string, (n: number) => *>}} [hooks]
 *   per step (`seek`, `validate`, `build`), called with the step's ordinal within this instrument
 * @returns {Array<[string, number[][]|number[]]>} the live event list
 */
const instrument = (h, { before = {}, after = {} } = {}) => {
  const { group, broker } = h
  const events = []
  const ordinals = {}
  const step = async (label, topicPartitions, run) => {
    events.push([label, partitionsOf(topicPartitions)])
    ordinals[label] = (ordinals[label] || 0) + 1
    await before[label]?.(ordinals[label])
    const result = await run()
    await after[label]?.(ordinals[label])
    return result
  }
  for (const [method, label] of [
    ['seekOffsets', 'seek'],
    ['validatePositions', 'validate'],
  ]) {
    const original = group[method].bind(group)
    group[method] = (topicPartitions, ...rest) =>
      step(label, topicPartitions, () => original(topicPartitions, ...rest))
  }
  const build = group.buildFetchRequests.bind(group)
  group.buildFetchRequests = (topicPartitions) => {
    events.push(['build', partitionsOf(topicPartitions)])
    return build(topicPartitions)
  }
  const fetch = broker.fetch.bind(broker)
  broker.fetch = (request) => {
    events.push([
      'frame',
      request.topics.flatMap(({ partitions }) => partitions.map((p) => p.partition)),
    ])
    return fetch(request)
  }
  return events
}

/** Counts the live registrations on a run's fence, outside its own routing subscription. */
const trackRegistrations = (fence) => {
  const register = fence.register.bind(fence)
  const tracker = { active: 0 }
  fence.register = (cancel) => {
    tracker.active += 1
    const unregister = register(cancel)
    let done = false
    return () => {
      if (!done) tracker.active -= 1
      done = true
      unregister()
    }
  }
  return tracker
}

describe('idle wait on a real fetch round', () => {
  let h
  afterEach(async () => {
    jest.clearAllTimers()
    jest.useRealTimers()
    if (h) await h.broker.disconnect()
  })

  /** @returns {Promise<object>} a group whose every partition is paused, ready for idle rounds */
  const pausedGroup = async (options) => {
    h = await createFetchGroup(options)
    fakeTimers()
    h.group.maxWaitTime = 400
    h.group.pause([{ topic: TOPIC }])
    return h
  }

  /** Starts a fetch and records how often it settled. */
  const startFetch = (nodeId) => {
    const settled = jest.fn()
    const done = h.group.fetch(nodeId).then(settled)
    return { settled, done }
  }

  it('a wake just before registration is reflected in the build', async () => {
    await pausedGroup()
    const built = []
    const build = h.group.buildFetchRequests.bind(h.group)
    h.group.buildFetchRequests = (topicPartitions) => {
      built.push(build(topicPartitions))
      return built[built.length - 1]
    }
    const validate = h.group.validatePositions.bind(h.group)
    h.group.validatePositions = async (topicPartitions) => {
      await validate(topicPartitions)
      h.group.resume([{ topic: TOPIC, partitions: [0] }])
    }
    const { done } = startFetch(NODE_ID)
    await settle()
    await done
    expect(built).toHaveLength(1)
    expect(built[0][0].partitions.map(({ partition }) => partition)).toEqual([0])
    expect(h.fetchRequestPartitions()).toEqual([0])
    expect(jest.getTimerCount()).toBe(0)
  })

  describe('routing that changes while positions are prepared', () => {
    const moves = [
      [
        'a leader hint',
        (g) => g.group.applyFetchLeaderHint(TOPIC, 0, { leaderId: 1, leaderEpoch: 6 }, undefined),
      ],
      ['a metadata refresh', (g) => g.installRouting({ 0: 1 })],
      [
        'a by-ID write-through',
        (g) => {
          g.leaders.set(0, 1)
          g.metadataCache.updateTopicsById({
            brokers: [{ nodeId: 0 }, { nodeId: 1 }],
            topicMetadata: [
              {
                topic: TOPIC,
                topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
                partitionMetadata: [{ partitionId: 0, leader: 1, leaderEpoch: 9 }],
              },
            ],
          })
        },
      ],
    ]

    /** The moved partition is prepared on its destination before the first frame carries it. */
    const expectPreparedBeforeSend = (events) => {
      const frame = events.findIndex(([label]) => label === 'frame')
      expect(frame).toBeGreaterThan(-1)
      const before = events.slice(0, frame)
      expect(before).toContainEqual(['seek', [[0]]])
      expect(before).toContainEqual(['validate', [[0]]])
      expect(events[frame]).toEqual(['frame', [0]])
      expect(events.filter(([label]) => label === 'seek').length).toBeLessThanOrEqual(2)
      expect(events.filter(([label]) => label === 'validate').length).toBeLessThanOrEqual(2)
    }

    /** A destination node that owns nothing yet; partition 0 is led by node 0. */
    const destination = async () => {
      h = await createFetchGroup({
        knownNodes: [0, 1],
        partitionLeaders: { 0: 0 },
        applyLeaderHintMovesLeaderTo: 1,
      })
      fakeTimers()
      h.group.maxWaitTime = 5000
    }

    const combos = moves.flatMap(([name, move]) =>
      ['seek', 'validate'].map((label) => [name, label, move])
    )

    it.each(combos)(
      '%s during a held %s is prepared and sent in the same call',
      async (_, label, move) => {
        await destination()
        const held = gate()
        const events = instrument(h, {
          before: { [label]: (n) => (n === 1 ? held.promise : undefined) },
        })
        const pending = h.group.fetch(1)
        await drain()
        await move(h)
        held.release()
        await finish(pending)
        expectPreparedBeforeSend(events)
        expect(jest.getTimerCount()).toBe(0)
        expect(h.group.idleWait.waiters.size).toBe(0)
      }
    )

    it('a hint whose reply is still held invalidates the destination at the cache mutation', async () => {
      await destination()
      const reply = gate()
      h.cluster.applyLeaderHint = jest.fn(({ partition, leaderId, leaderEpoch }) => {
        h.leaders.set(partition, leaderId)
        h.metadataCache.applyLeaderHint(TOPIC, partition, { leaderId, leaderEpoch })
        return reply.promise
      })
      const held = gate()
      const events = instrument(h, {
        before: { seek: (n) => (n === 1 ? held.promise : undefined) },
      })
      const pending = h.group.fetch(1)
      await drain()
      const hint = h.group.applyFetchLeaderHint(
        TOPIC,
        0,
        { leaderId: 1, leaderEpoch: 6 },
        undefined
      )
      held.release()
      await finish(pending)
      expectPreparedBeforeSend(events)
      reply.release(true)
      await expect(hint).resolves.toBe(true)
    })

    it('a hint the cache rejects neither invalidates nor wakes the round', async () => {
      await destination()
      const held = gate()
      const events = instrument(h, {
        before: { seek: (n) => (n === 1 ? held.promise : undefined) },
      })
      const version = h.group.routingVersion
      const pending = h.group.fetch(1)
      await drain()
      await h.group.applyFetchLeaderHint(TOPIC, 0, { leaderId: 1, leaderEpoch: 4 }, undefined)
      held.release()
      await drain()
      expect(h.group.routingVersion).toBe(version)
      expect(events.filter(([label]) => label === 'seek')).toHaveLength(1)
      expect(jest.getTimerCount()).toBe(1)
      h.group.onStopRequested()
      await finish(pending)
    })

    it('re-filtering only before the build admits a partition it never prepared', async () => {
      await destination()
      const held = gate()
      const events = instrument(h, {
        before: { seek: (n) => (n === 1 ? held.promise : undefined) },
      })
      h.group.prepareFetch = async (nodeId, run, token) => {
        const first = h.group.captureFetchPreparation(nodeId, run, token)
        await h.group.seekOffsets(first.parts, first)
        await h.group.validatePositions(first.parts, first)
        return h.group.captureFetchPreparation(nodeId, run, token)
      }
      const pending = h.group.fetch(1)
      await drain()
      await moves[1][1](h)
      held.release()
      await finish(pending)
      expect(() => expectPreparedBeforeSend(events)).toThrow()
    })

    it('a preferred replica dropped during the held await moves the partition back to its leader', async () => {
      h = await createFetchGroup({
        knownNodes: [0, 1],
        partitionLeaders: { 0: 0 },
        preferredReadReplica: 1,
      })
      fakeTimers()
      h.group.maxWaitTime = 5000
      h.group.positionValidator.markValidated(TOPIC, 0, 5)
      const held = gate()
      const events = instrument(h, {
        before: { seek: (n) => (n === 1 ? held.promise : undefined) },
      })
      const pending = h.group.fetch(1)
      await drain()
      await h.group.recoverFromOffsetOutOfRange({ topic: TOPIC, partition: 0 })
      held.release()
      await drain()
      expect(events.filter(([label]) => label === 'seek')).toEqual([
        ['seek', [[0]]],
        ['seek', [[]]],
      ])
      expect(events.some(([label]) => label === 'frame')).toBe(false)
      h.group.onStopRequested()
      await finish(pending)
    })

    it('an idle wait on a preferred-replica lease sleeps no longer than the lease', async () => {
      h = await createFetchGroup({
        knownNodes: [0, 1],
        partitionLeaders: { 0: 0 },
        preferredReadReplica: 1,
      })
      fakeTimers()
      h.group.maxWaitTime = 5000
      h.group.positionValidator.markValidated(TOPIC, 0, 5)
      h.group.preferredReadReplicasPerTopicPartition[TOPIC][0].expireAt = Date.now() + 1000
      h.group.pause([{ topic: TOPIC }])
      const { settled, done } = startFetch(1)
      await settle()
      expect(jest.getTimerCount()).toBe(1)
      await jest.advanceTimersByTimeAsync(999)
      expect(settled).not.toHaveBeenCalled()
      await jest.advanceTimersByTimeAsync(1)
      await done
      expect(settled).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    })
  })

  it('a wake just after registration returns the round at the wake time', async () => {
    await pausedGroup()
    const { settled, done } = startFetch(NODE_ID)
    await settle()
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(50)
    expect(settled).not.toHaveBeenCalled()
    h.group.resume([{ topic: TOPIC, partitions: [0] }])
    await entered()
    expect(settled).toHaveBeenCalledTimes(1)
    await done
    expect(jest.getTimerCount()).toBe(0)
  })

  const sources = [
    ['resume', () => h.group.resume([{ topic: TOPIC }])],
    ['seek', () => h.group.seek({ topic: TOPIC, partition: 0, offset: '5' })],
    ['applied assignment', () => h.sync()],
    ['invalidateTopic', () => h.group.invalidateTopic(TOPIC)],
    [
      'applied halt-clear seek',
      async () => {
        h.group.haltedPartitions.set(`${TOPIC}|0`, {})
        h.group.seekOffset.set(TOPIC, 0, '5')
        await h.group.seekOffsets([{ topic: TOPIC, partitions: [0] }])
        expect(h.group.haltedPartitions.has(`${TOPIC}|0`)).toBe(false)
      },
    ],
    [
      'revoke halt-clear',
      async () => {
        await h.sync()
        h.group.haltedPartitions.set(`${TOPIC}|1`, {})
        await h.group.applyRebalanceResult(
          { assignment: { [TOPIC]: [0] } },
          { rebuildOffsets: false }
        )
        expect(h.group.haltedPartitions.has(`${TOPIC}|1`)).toBe(false)
      },
    ],
    [
      'accepted leader hint',
      () =>
        h.group.applyFetchLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: 6 }, undefined),
    ],
    ['stop', () => h.group.onStopRequested()],
  ]

  describe('each wake source settles every current waiter once', () => {
    it.each(sources)('%s', async (_, wake) => {
      await pausedGroup({
        partitions: [0, 1],
        knownNodes: [0, 1],
        partitionLeaders: { 0: 0, 1: 1 },
      })
      h.group.maxWaitTime = 5000
      const fetches = [startFetch(0), startFetch(1)]
      await settle()
      expect(jest.getTimerCount()).toBe(2)
      await wake()
      await settle()
      for (const { settled } of fetches) expect(settled).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
      await Promise.all(fetches.map(({ done }) => done))
    })

    it('an installed refresh wakes waiters once and a rejected hint schedules nothing', async () => {
      await pausedGroup({
        partitions: [0, 1],
        knownNodes: [0, 1],
        partitionLeaders: { 0: 0, 1: 1 },
      })
      h.group.maxWaitTime = 5000
      const fetches = [startFetch(0), startFetch(1)]
      await settle()
      expect(jest.getTimerCount()).toBe(2)
      await h.group.applyFetchLeaderHint(TOPIC, 0, { leaderId: NODE_ID, leaderEpoch: 4 }, undefined)
      await settle()
      expect(jest.getTimerCount()).toBe(2)
      h.installRouting()
      await settle()
      for (const { settled } of fetches) expect(settled).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
      const next = startFetch(0)
      await settle()
      expect(next.settled).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(1)
      await jest.advanceTimersByTimeAsync(5000)
      await Promise.all([...fetches, next].map(({ done }) => done))
      expect(jest.getTimerCount()).toBe(0)
    })

    it('a refresh installed by another consumer wakes a node that is already asleep', async () => {
      await pausedGroup({ partitions: [0], knownNodes: [0, 1], partitionLeaders: { 0: 0 } })
      h.group.maxWaitTime = 5000
      const { settled, done } = startFetch(1)
      await settle()
      expect(jest.getTimerCount()).toBe(1)
      h.installRouting({ 0: 1 })
      await settle()
      expect(settled).toHaveBeenCalledTimes(1)
      await done
      expect(jest.getTimerCount()).toBe(0)
    })
  })

  it('a thousand resume events do not spin fetchers', async () => {
    await pausedGroup({ partitions: [0, 1], knownNodes: [0, 1], partitionLeaders: { 0: 0, 1: 1 } })
    h.group.maxWaitTime = 5000
    const brokerFetch = jest.spyOn(h.broker, 'fetch')
    let rounds = 0
    const loop = { running: true }
    const loops = [0, 1].map(async (nodeId) => {
      while (loop.running) {
        rounds += 1
        await h.group.fetch(nodeId)
        await new Promise(setImmediate)
      }
    })
    await settle()
    expect(rounds).toBe(2)
    const waiters = 2
    for (let event = 0; event < 1000; event++) {
      h.group.resume([{ topic: TOPIC }])
      h.group.pause([{ topic: TOPIC }])
      await settle()
    }
    expect(rounds).toBeLessThanOrEqual(1000 + waiters)
    expect(brokerFetch).not.toHaveBeenCalled()
    loop.running = false
    h.group.onStopRequested()
    await settle()
    await Promise.all(loops)
    const finished = rounds
    await jest.advanceTimersByTimeAsync(5000)
    expect(rounds).toBe(finished)
    expect(jest.getTimerCount()).toBe(0)
    expect(h.group.idleWait.waiters.size).toBe(0)
  })

  it('all lifecycle wake sources settle only the captured run waiters', async () => {
    const yielded = jest.spyOn(global, 'setImmediate')
    try {
      for (const [name, wake] of sources) {
        await pausedGroup({
          partitions: [0, 1],
          knownNodes: [0, 1],
          partitionLeaders: { 0: 0, 1: 1 },
        })
        h.group.maxWaitTime = 5000
        const saved = []
        const timeout = global.setTimeout
        const timers = jest.spyOn(global, 'setTimeout').mockImplementation((callback, ms) => {
          saved.push(callback)
          return timeout(callback, ms)
        })
        const timersBefore = jest.getTimerCount()
        const old = [startFetch(0), startFetch(1)]
        await settle()
        expect(jest.getTimerCount() - timersBefore).toBe(2)
        yielded.mockClear()
        await wake()
        expect(yielded).not.toHaveBeenCalled()
        await settle()
        for (const { settled } of old) expect(settled).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(timersBefore)
        await Promise.all(old.map(({ done }) => done))

        const replaced = h.group.idleWait
        h.group.startIdleWait()
        expect(h.group.idleWait).not.toBe(replaced)
        const fresh = jest.fn()
        const waiting = h.group.idleWait.wait(5000).then(fresh)
        expect(jest.getTimerCount() - timersBefore).toBe(1)
        for (const callback of saved.slice(0, 2)) callback()
        await settle()
        expect(fresh).not.toHaveBeenCalled()
        expect(jest.getTimerCount() - timersBefore).toBe(1)
        h.group.idleWait.wake()
        await waiting
        expect(fresh).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(timersBefore)
        timers.mockRestore()
        h.group.onStopRequested()
        await h.broker.disconnect()
        h = null
        expect(name).toEqual(expect.any(String))
      }
    } finally {
      yielded.mockRestore()
    }
  })

  it('forgetting the final session partition precedes an idle wait without a spin', async () => {
    h = await createFetchGroup({ fetchSessions: true, position: '5' })
    fakeTimers()
    h.group.maxWaitTime = 5000
    h.setResponse(fetchResponse([{ partition: 0, errorCode: 0 }], { sessionId: 7 }))
    await h.group.fetch(NODE_ID)
    h.group.pause([{ topic: TOPIC }])
    h.setResponse(fetchResponse([], { sessionId: 7, includeTopic: false }))
    const sent = h.fetchFrames().length

    const forgetting = startFetch(NODE_ID)
    await settle()
    await forgetting.done
    const frames = h.fetchFrames()
    expect(frames).toHaveLength(sent + 1)
    expect(frames[sent]).toMatchObject({ sessionId: 7, topics: [] })
    expect(frames[sent].forgottenTopics.flatMap(({ partitions }) => partitions)).toEqual([0])
    expect(jest.getTimerCount()).toBe(0)

    let rounds = 0
    const loop = { running: true }
    const worker = (async () => {
      while (loop.running) {
        rounds += 1
        await h.group.fetch(NODE_ID)
        await new Promise(setImmediate)
      }
    })()
    await settle()
    expect(rounds).toBe(1)
    expect(jest.getTimerCount()).toBe(1)
    const waiters = 1
    for (let event = 0; event < 1000; event++) {
      h.group.resume([{ topic: TOPIC }])
      h.group.pause([{ topic: TOPIC }])
      await settle()
    }
    expect(rounds).toBeLessThanOrEqual(1000 + waiters)
    expect(h.fetchFrames()).toHaveLength(sent + 1)
    loop.running = false
    h.group.onStopRequested()
    await settle()
    await worker
    const finished = rounds
    await jest.advanceTimersByTimeAsync(5000)
    expect(rounds).toBe(finished)
    expect(jest.getTimerCount()).toBe(0)
    expect(h.group.idleWait.waiters.size).toBe(0)
  })

  it('disconnect ends an idle five second fetch within fifty milliseconds', async () => {
    await pausedGroup()
    const start = Date.now()
    h.group.maxWaitTime = 5000
    h.cluster.getNodeIds = () => [NODE_ID]
    h.group.leave = async () => {}
    const runner = new Runner({
      logger: newLogger(),
      consumerGroup: h.group,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      concurrency: 1,
      eachBatch: async () => {},
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
      autoCommit: false,
      onCrash: (error) => {
        throw error
      },
    })
    runner.heartbeat = async () => {}
    const order = []
    const onStop = h.group.onStopRequested.bind(h.group)
    h.group.onStopRequested = () => {
      order.push('onStopRequested')
      onStop()
    }
    const stopFetchers = runner.fetchManager.stop
    runner.fetchManager.stop = () => {
      order.push('fetchManager.stop')
      return stopFetchers()
    }
    h.group.startIdleWait()
    h.group.pause([{ topic: TOPIC }])
    runner.running = true
    const fetching = runner.fetchManager.start()
    await settle()
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(50)
    const stopped = runner.stop()
    await settle()
    await Promise.all([stopped, fetching])
    expect(order).toEqual(['onStopRequested', 'fetchManager.stop'])
    expect(Date.now() - start).toBe(50)
    expect(jest.getTimerCount()).toBe(0)
  })
})

describe('bounded fetch preparation', () => {
  let h
  afterEach(async () => {
    jest.clearAllTimers()
    jest.useRealTimers()
    if (h) await h.broker.disconnect()
  })

  /** Stops a runaway preparation so a regression fails an assertion instead of hanging. */
  const contain = (counts) => {
    if (counts.validations > 60) h.group.onStopRequested()
  }

  /**
   * A fixed schedule of fetch calls against a route that changes in every validation completion.
   * @param {object} options harness options
   * @param {(owner: number) => number} next the leader of partition 0 after each completion
   * @param {number} [rounds] how many calls to make
   * @returns {Promise<{calls: object[], frames: number[][]}>} what each call did
   */
  const churn = async (options, next, rounds = 20) => {
    h = await createFetchGroup(options)
    fakeTimers()
    h.group.maxWaitTime = 5000
    h.setResponse(() =>
      fetchResponse(
        h.fetchRequestPartitions().map((partition) => ({ partition, errorCode: 0 })),
        { apiVersion: 18 }
      )
    )
    const counts = { filters: 0, seeks: 0, validations: 0, builds: 0 }
    const order = []
    let owner = h.leaders.get(0)
    const filter = h.group.filterPartitionsByNode.bind(h.group)
    h.group.filterPartitionsByNode = (...args) => {
      counts.filters += 1
      return filter(...args)
    }
    const attempts = []
    const events = instrument(h, {
      before: {
        seek: (n) => {
          if (n % 2 === 0) order.push('attempt2')
          counts.seeks += 1
        },
      },
      after: {
        validate: (n) => {
          counts.validations += 1
          if (n % 2 === 1) setImmediate(() => order.push('immediate'))
          owner = next(owner)
          h.installRouting({ 0: owner })
          contain(counts)
        },
      },
    })
    const calls = []
    const frames = []
    for (let round = 0; round < rounds; round++) {
      Object.assign(counts, { filters: 0, seeks: 0, validations: 0 })
      order.length = 0
      events.length = 0
      const pending = h.group.fetch(0)
      const result = await finish(pending)
      attempts.push(order.length)
      calls.push({
        ...counts,
        order: [...order],
        events: events.map(([label, parts]) => [label, parts]),
        result,
      })
      frames.push(...events.filter(([label]) => label === 'frame').map(([, parts]) => parts))
      await drain(2)
    }
    return { calls, frames, events }
  }

  it('prepares at most twice per call, yields once between, and builds the prepared snapshot', async () => {
    const { calls, frames } = await churn(
      { partitions: [0, 1], knownNodes: [0, 1], partitionLeaders: { 0: 0, 1: 0 } },
      (owner) => (owner === 0 ? 1 : 0)
    )
    expect(calls).toHaveLength(20)
    for (const call of calls) {
      expect(call.filters).toBeLessThanOrEqual(2)
      expect(call.seeks).toBeLessThanOrEqual(2)
      expect(call.validations).toBeLessThanOrEqual(2)
      expect(call.order).toEqual(['immediate', 'attempt2'])
      const seeks = call.events.filter(([label]) => label === 'seek')
      const builds = call.events.filter(([label]) => label === 'build')
      expect(builds).toHaveLength(1)
      const lastPrepared = seeks[seeks.length - 1][1].flat()
      for (const partition of builds[0][1].flat()) expect(lastPrepared).toContain(partition)
    }
    expect(frames).toHaveLength(20)
    for (const frame of frames) expect(frame).toContain(1)
    expect(h.group.idleWait.waiters.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('an all-eligible snapshot is still built and sent while routing changes continuously', async () => {
    const { calls, frames } = await churn(
      { partitions: [0], knownNodes: [0], partitionLeaders: { 0: 0 } },
      (owner) => owner
    )
    for (const call of calls) {
      expect(call.seeks).toBe(2)
      expect(call.events.filter(([label]) => label === 'build')).toEqual([['build', [[0]]]])
    }
    expect(frames).toEqual(Array(20).fill([0]))
  })

  it('an invalidated empty snapshot registers no idle waiter and returns at once', async () => {
    const { calls } = await churn(
      { partitions: [0], knownNodes: [0, 1, 2], partitionLeaders: { 0: 1 } },
      (owner) => (owner === 1 ? 2 : 1),
      6
    )
    for (const call of calls) {
      expect(call.result).toEqual([])
      expect(call.seeks).toBe(2)
      expect(call.events.filter(([label]) => label === 'frame')).toEqual([])
    }
    expect(h.group.idleWait.waiters.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('a timer callback runs before any further attempts when the fetcher drives the round', async () => {
    jest.useRealTimers()
    h = await createFetchGroup({
      partitions: [0],
      knownNodes: [0, 1],
      partitionLeaders: { 0: 0 },
    })
    const counts = { validations: 0 }
    let owner = 0
    instrument(h, {
      after: {
        validate: () => {
          counts.validations += 1
          owner = owner === 0 ? 1 : 0
          h.installRouting({ 0: owner })
          contain(counts)
        },
      },
    })
    const fetcher = createFetcher({
      nodeId: 0,
      workerQueue: { push: async () => {} },
      partitionAssignments: new Map(),
      fetch: (nodeId) => h.group.fetch(nodeId),
      logger: newLogger(),
    })
    let atImmediate = null
    const timer = { ran: false }
    setImmediate(() => {
      atImmediate = counts.validations
    })
    setTimeout(() => {
      timer.ran = true
    }, 0)
    const running = fetcher.start()
    for (let n = 0; n < 200 && !timer.ran; n++) await new Promise(setImmediate)
    h.group.onStopRequested()
    await fetcher.stop()
    await running
    expect(atImmediate).toBeLessThanOrEqual(1)
    expect(timer.ran).toBe(true)
  })

  it.each([
    [
      'assignment replaced with equal values',
      (g) => g.group.subscriptionState.assign([{ topic: TOPIC, partitions: [0] }]),
    ],
    ['topic incarnation invalidated', (g) => g.group.invalidateTopic(TOPIC)],
  ])('%s refuses the preparation, and a stable one prepares normally', async (_, replace) => {
    h = await createFetchGroup({ partitions: [0], partitionLeaders: { 0: 0 } })
    fakeTimers()
    h.group.maxWaitTime = 5000
    h.offsetManager.coordinator = {
      isConnected: () => true,
      offsetFetch: async () => ({
        responses: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '7' }] }],
      }),
    }
    let replacing = true
    const counts = { validations: 0 }
    const events = instrument(h, {
      after: {
        validate: () => {
          counts.validations += 1
          if (replacing) replace(h)
          contain(counts)
        },
      },
    })
    const lost = await finish(h.group.fetch(0))
    expect(lost).toEqual([])
    expect(events.filter(([label]) => label === 'build')).toEqual([])
    expect(events.filter(([label]) => label === 'frame')).toEqual([])
    expect(jest.getTimerCount()).toBe(0)
    expect(h.group.idleWait.waiters.size).toBe(0)
    replacing = false
    h.offsetManager.committedOffsets()[TOPIC][0] = '7'
    const batches = await finish(h.group.fetch(0))
    expect(batches.length).toBeGreaterThanOrEqual(0)
    expect(events.filter(([label]) => label === 'frame')).toEqual([['frame', [0]]])
  })
})

describe('stop during an unresolved preparation', () => {
  let h
  const unhandled = []
  const onUnhandled = (reason) => unhandled.push(reason)
  beforeEach(() => {
    unhandled.length = 0
    process.on('unhandledRejection', onUnhandled)
  })
  afterEach(async () => {
    process.off('unhandledRejection', onUnhandled)
    jest.clearAllTimers()
    jest.useRealTimers()
    if (h) await h.broker.disconnect()
  })

  const kinds = {
    'offset resolution': {
      options: {},
      hold: () => {
        h.offsetManager.committedOffsets()[TOPIC][0] = '-1'
        const gates = [gate(), gate()]
        h.offsetManager.coordinator = {
          isConnected: () => true,
          offsetFetch: jest.fn(() => gates.shift().promise),
        }
        return {
          late: { responses: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '9' }] }] },
          current: () => ({ gates: [] }),
          gates: [...gates],
          effect: () => h.offsetManager.committedOffsets()[TOPIC][0],
          untouched: '-1',
        }
      },
    },
    'epoch validation': {
      options: { committedEpoch: 3 },
      hold: () => {
        const gates = [gate(), gate()]
        const queue = [...gates]
        h.broker.offsetForLeaderEpoch = jest.fn(() => queue.shift().promise)
        jest.spyOn(h.offsetManager, 'seek')
        return {
          late: {
            topics: [
              {
                topic: TOPIC,
                partitions: [{ partition: 0, errorCode: 0, leaderEpoch: 4, endOffset: '4' }],
              },
            ],
          },
          gates,
          effect: () => h.offsetManager.seek.mock.calls.length,
          untouched: 0,
        }
      },
    },
  }

  const setup = async (kind) => {
    h = await createFetchGroup({
      partitions: [0],
      knownNodes: [0],
      position: '10',
      ...kinds[kind].options,
    })
    fakeTimers()
    h.group.maxWaitTime = 5000
    const held = kinds[kind].hold()
    const events = instrument(h)
    return { held, events }
  }

  it.each(Object.keys(kinds))(
    '%s: stop settles the round locally before the gate opens',
    async (kind) => {
      const { held, events } = await setup(kind)
      expect(h.metadataCache.routingListeners.size).toBe(1)
      const settled = jest.fn()
      const pending = h.group.fetch(0).then(settled)
      await drain()
      expect(settled).not.toHaveBeenCalled()
      h.group.onStopRequested()
      await jest.advanceTimersByTimeAsync(50)
      await entered()
      expect(settled).toHaveBeenCalledTimes(1)
      await pending
      expect(h.group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
      expect(events.some(([label]) => label === 'build' || label === 'frame')).toBe(false)
      expect(h.metadataCache.routingListeners.size).toBe(0)
      expect(h.group.idleWait.waiters.size).toBe(0)
      expect(jest.getTimerCount()).toBe(0)
      held.gates[0].release(held.late)
      await drain()
      expect(held.effect()).toBe(held.untouched)
      expect(events.some(([label]) => label === 'build' || label === 'frame')).toBe(false)
      expect(unhandled).toEqual([])
    }
  )

  it.each(
    Object.keys(kinds).flatMap((kind) => [
      [kind, 'success'],
      [kind, 'rejection'],
    ])
  )(
    '%s: a late %s of the old run changes nothing in the replacement run',
    async (kind, outcome) => {
      const { held, events } = await setup(kind)
      const recover = jest.spyOn(h.group, 'recoverFromFetch')
      const emit = jest.spyOn(h.group.instrumentationEmitter, 'emit')
      const pending = h.group.fetch(0)
      await drain()
      h.group.onStopRequested()
      await finish(pending)
      h.group.startIdleWait()
      expect(h.metadataCache.routingListeners.size).toBe(1)
      const replacement = h.group.fetch(0)
      await drain()
      expect(h.group.positionValidator.isAwaiting(TOPIC, 0)).toBe(kind === 'epoch validation')
      if (outcome === 'success') held.gates[0].release(held.late)
      else held.gates[0].fail(new Error('late failure'))
      await drain()
      expect(h.group.positionValidator.isAwaiting(TOPIC, 0)).toBe(kind === 'epoch validation')
      expect(held.effect()).toBe(held.untouched)
      expect(recover).not.toHaveBeenCalled()
      expect(emit).not.toHaveBeenCalled()
      expect(events.filter(([label]) => label === 'frame')).toEqual([])
      expect(h.metadataCache.routingListeners.size).toBe(1)
      held.gates[1].release(
        kind === 'epoch validation'
          ? {
              topics: [
                {
                  topic: TOPIC,
                  partitions: [{ partition: 0, errorCode: 0, leaderEpoch: 4, endOffset: '20' }],
                },
              ],
            }
          : { responses: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '9' }] }] }
      )
      await finish(replacement)
      expect(events.filter(([label]) => label === 'frame')).toEqual([['frame', [0]]])
      expect(unhandled).toEqual([])
      h.group.onStopRequested()
    }
  )

  it.each(Object.keys(kinds))(
    '%s: disconnect through the runner ends within fifty milliseconds',
    async (kind) => {
      const { held } = await setup(kind)
      h.cluster.getNodeIds = () => [NODE_ID]
      h.group.leave = async () => {}
      const runner = new Runner({
        logger: newLogger(),
        consumerGroup: h.group,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        concurrency: 1,
        eachBatch: async () => {},
        heartbeatInterval: 3000,
        maxPollInterval: 300000,
        autoCommit: false,
        onCrash: (error) => {
          throw error
        },
      })
      runner.heartbeat = async () => {}
      h.group.startIdleWait()
      runner.running = true
      const start = Date.now()
      const fetching = runner.fetchManager.start()
      await drain()
      const stopped = jest.fn()
      const stopping = runner.stop().then(stopped)
      await jest.advanceTimersByTimeAsync(50)
      await drain(3)
      expect(stopped).toHaveBeenCalledTimes(1)
      await Promise.all([stopping, fetching])
      expect(Date.now() - start).toBe(50)
      expect(h.group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
      held.gates[0].release(held.late)
      await drain()
      expect(held.effect()).toBe(held.untouched)
      expect(unhandled).toEqual([])
    }
  )
})

describe('idle registration boundary', () => {
  let h
  afterEach(async () => {
    jest.clearAllTimers()
    jest.useRealTimers()
    if (h) await h.broker.disconnect()
  })

  const pausedRound = async () => {
    h = await createFetchGroup({ partitions: [0], knownNodes: [0, 1], partitionLeaders: { 0: 0 } })
    fakeTimers()
    h.group.maxWaitTime = 5000
    h.group.pause([{ topic: TOPIC }])
    const settled = jest.fn()
    return { settled, run: () => h.group.fetch(0).then(settled) }
  }

  it('a change after the build and before insertion admits no idle wait', async () => {
    const { settled, run } = await pausedRound()
    const add = jest.spyOn(h.group.idleWait.waiters, 'add')
    const build = h.group.buildFetchRequests.bind(h.group)
    h.group.buildFetchRequests = (parts) => {
      const requests = build(parts)
      h.installRouting()
      return requests
    }
    const done = run()
    await drain()
    expect(settled).toHaveBeenCalledTimes(1)
    expect(add).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
    await done
  })

  it('a change between the check and the insertion finishes the waiter at the post-check', async () => {
    const { settled, run } = await pausedRound()
    const waiters = h.group.idleWait.waiters
    const add = waiters.add.bind(waiters)
    waiters.add = (waiter) => {
      h.installRouting()
      add(waiter)
      return waiters
    }
    const done = run()
    await drain()
    expect(settled).toHaveBeenCalledTimes(1)
    expect(waiters.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
    await done
  })

  it('a change right after insertion finishes that waiter through the notification', async () => {
    const { settled, run } = await pausedRound()
    const waiters = h.group.idleWait.waiters
    const add = waiters.add.bind(waiters)
    waiters.add = (waiter) => {
      add(waiter)
      h.installRouting()
      return waiters
    }
    const done = run()
    await drain()
    expect(settled).toHaveBeenCalledTimes(1)
    expect(waiters.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
    await done
  })

  it('a change after the post-check wakes the waiter through the notification', async () => {
    const { settled, run } = await pausedRound()
    const done = run()
    await drain()
    expect(jest.getTimerCount()).toBe(1)
    expect(settled).not.toHaveBeenCalled()
    h.installRouting()
    await drain()
    expect(settled).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
    await done
  })

  it('a fresh empty round keeps its timer, and a thousand earlier empty wakes grant it no credit', async () => {
    const { settled, run } = await pausedRound()
    h.installRouting()
    const version = h.group.routingVersion
    for (let n = 0; n < 1000; n++) h.group.idleWait.wake()
    const done = run()
    await drain()
    expect(h.group.routingVersion).toBe(version)
    expect(jest.getTimerCount()).toBe(1)
    expect(settled).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(4999)
    expect(settled).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(1)
    await done
    expect(settled).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('holds one routing listener per live run, never per round or node', async () => {
    await pausedRound()
    const cache = h.metadataCache
    expect(cache.routingListeners.size).toBe(1)
    const rounds = [h.group.fetch(0), h.group.fetch(1)]
    await drain()
    expect(cache.routingListeners.size).toBe(1)
    h.group.startIdleWait()
    expect(cache.routingListeners.size).toBe(1)
    h.group.onStopRequested()
    expect(cache.routingListeners.size).toBe(0)
    h.group.onStopRequested()
    await Promise.all(rounds)
    expect(cache.routingListeners.size).toBe(0)
  })

  it('stable routing costs one filter, one seek, one validation and no retry yield', async () => {
    h = await createFetchGroup({
      partitions: [0, 1],
      knownNodes: [0],
      partitionLeaders: { 0: 0, 1: 0 },
    })
    fakeTimers()
    const tracker = trackRegistrations(h.group.idleWait.fence)
    const filter = jest.spyOn(h.group, 'filterPartitionsByNode')
    const yielded = jest.spyOn(global, 'setImmediate')
    const events = instrument(h)
    const prepared = await h.group.prepareFetch(0, h.group.idleWait, h.group.ownership.token())
    expect(prepared.parts).toEqual([{ topic: TOPIC, partitions: [0, 1] }])
    expect(filter).toHaveBeenCalledTimes(1)
    expect(events.filter(([label]) => label === 'seek')).toHaveLength(1)
    expect(events.filter(([label]) => label === 'validate')).toHaveLength(1)
    expect(yielded).not.toHaveBeenCalled()
    expect(tracker.active).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
    yielded.mockRestore()
  })
})

describe('run fence and preparation snapshot', () => {
  beforeEach(() => jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] }))
  afterEach(() => {
    jest.clearAllTimers()
    jest.useRealTimers()
  })

  it('a stale evidence waiter is never registered and a retired run closes its fence once', async () => {
    const IdleWait = require('../idleWait')
    const run = new IdleWait()
    const add = jest.spyOn(run.waiters, 'add')
    await run.wait(400, () => false)
    expect(add).not.toHaveBeenCalled()
    const cancelled = jest.fn()
    run.fence.register(cancelled)
    run.dispose()
    run.dispose()
    expect(cancelled).toHaveBeenCalledTimes(1)
    expect(run.fence.closed).toBe(true)
    await run.wait(400)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('a fenced delay clears its timer when the fence closes and unregisters when it ends', async () => {
    const IdleWait = require('../idleWait')
    const { fencedDelay } = require('../fetchPreparation')
    const run = new IdleWait()
    const tracker = trackRegistrations(run.fence)
    const ended = fencedDelay(run.fence, 400)
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(400)
    await ended
    expect(tracker.active).toBe(0)
    const cancelled = fencedDelay(run.fence, 400)
    const rejected = cancelled.catch((error) => error)
    run.dispose()
    expect(await rejected).toMatchObject({ type: 'CONSUMER_STOPPING' })
    expect(jest.getTimerCount()).toBe(0)
    await expect(fencedDelay(run.fence)).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
  })

  it('a snapshot is current by one version compare and bounds an idle wait by its lease', async () => {
    const { FetchPreparation } = require('../fetchPreparation')
    const group = idleGroup()
    const run = group.idleWait
    const token = group.ownership.token()
    const open = new FetchPreparation(group, run, [], Infinity, token)
    expect(open.isCurrent()).toBe(true)
    expect(open.idleDuration(5000)).toBe(5000)
    const leased = new FetchPreparation(group, run, [], Date.now() + 1000, token)
    expect(leased.idleDuration(5000)).toBe(1000)
    expect(leased.idleDuration(400)).toBe(400)
    group.routingChanged()
    expect(open.isCurrent()).toBe(false)
    expect(open.guard()).toBeNull()
    await jest.advanceTimersByTimeAsync(1000)
    expect(leased.idleDuration(5000)).toBe(0)
    group.onStopRequested()
  })

  it('a replaced assignment, manager or membership refuses once and a routing change does not', () => {
    const { FetchPreparation, PreparationSuperseded } = require('../fetchPreparation')
    const group = idleGroup()
    const make = () =>
      new FetchPreparation(group, group.idleWait, [], Infinity, group.ownership.token())
    const preparation = make()
    group.routingChanged()
    expect(preparation.guard()).toBeNull()
    group.preparationChanged()
    const refusal = preparation.guard()
    expect(refusal).toBeInstanceOf(PreparationSuperseded)
    expect(preparation.guard()).toBe(refusal)
    expect(preparation.isSuperseded(refusal)).toBe(true)
    expect(() => preparation.check()).toThrow(refusal)
    const byManager = make()
    group.offsetManager = { clearPendingSeeks() {} }
    expect(byManager.guard()).toBeInstanceOf(PreparationSuperseded)
    const byMembership = make()
    group.ownership.renew()
    expect(byMembership.guard()).toBeInstanceOf(PreparationSuperseded)
    const retired = make()
    group.onStopRequested()
    expect(retired.guard()).toMatchObject({ type: 'CONSUMER_STOPPING' })
  })
})

describe('a refused guard stops the broker request before it is written', () => {
  let h
  afterEach(async () => {
    if (h) await h.broker.disconnect()
  })

  it.each([
    ['fetch', { topics: [{ topic: TOPIC, partitions: [{ partition: 0, fetchOffset: '0' }] }] }],
    ['listOffsets', { topics: [{ topic: TOPIC, partitions: [{ partition: 0 }] }] }],
    ['offsetFetch', { groupId: 'g', topics: [{ topic: TOPIC, partitions: [{ partition: 0 }] }] }],
    [
      'offsetForLeaderEpoch',
      { topics: [{ topic: TOPIC, partitions: [{ partition: 0, leaderEpoch: 1 }] }] },
    ],
  ])('%s', async (method, request) => {
    h = await createFetchGroup()
    const refusal = new Error('refused by the round')
    const written = h.requests().length
    await expect(h.broker[method]({ ...request, guard: () => refusal })).rejects.toBe(refusal)
    expect(h.requests()).toHaveLength(written)
  })
})

describe('terminal run cancellation of work started by a run', () => {
  let h
  const unhandled = []
  const onUnhandled = (reason) => unhandled.push(reason)
  beforeEach(() => {
    unhandled.length = 0
    process.on('unhandledRejection', onUnhandled)
  })
  afterEach(async () => {
    process.off('unhandledRejection', onUnhandled)
    jest.clearAllTimers()
    jest.useRealTimers()
    if (h) await h.broker.disconnect()
  })

  /** Retires the running run and starts its replacement, as a stop and restart do. */
  const restart = () => {
    const retired = h.group.idleWait
    h.group.onStopRequested()
    h.group.startIdleWait()
    return retired
  }

  /** Routes `cluster.applyLeaderHint` through the production method over a held endpoint step. */
  const holdEndpoints = (endpoints) => {
    h.cluster.applyLeaderHint = jest.fn((hint, options) =>
      Cluster.prototype.applyLeaderHint.call(
        { metadataCache: h.metadataCache, brokerPool: { applyNodeEndpoints: () => endpoints() } },
        hint,
        options
      )
    )
  }

  const staleWithHint = (partition, leaderId = 1) => ({
    partition,
    errorCode: 6,
    currentLeader: { leaderId, leaderEpoch: 9 },
  })

  it('a hint still applying its endpoints at stop publishes nothing into the replacement run', async () => {
    h = await createFetchGroup({ knownNodes: [0, 1] })
    fakeTimers()
    const endpoints = gate()
    holdEndpoints(() => endpoints.promise)
    h.setResponse(fetchResponse([staleWithHint(0)]))
    const pending = h.group.fetch(0)
    await drain()
    expect(h.cluster.applyLeaderHint).toHaveBeenCalledTimes(1)
    restart()
    const version = h.group.routingVersion
    endpoints.release()
    await expect(finish(pending)).resolves.toEqual([])
    expect(h.metadataCache.leaderEpoch(TOPIC, 0)).toBe(5)
    expect(h.group.routingVersion).toBe(version)
    expect(unhandled).toEqual([])
  })

  it('the partitions after a held hint are never handled after stop', async () => {
    h = await createFetchGroup({ partitions: [0, 1], knownNodes: [0, 1] })
    fakeTimers()
    const reply = gate()
    h.cluster.applyLeaderHint = jest.fn(() => reply.promise)
    h.setResponse(fetchResponse([staleWithHint(0), staleWithHint(1)]))
    const pending = h.group.fetch(0)
    await drain()
    restart()
    reply.release(true)
    await expect(finish(pending)).resolves.toEqual([])
    expect(h.cluster.applyLeaderHint).toHaveBeenCalledTimes(1)
  })

  it('a response walked past a held refresh returns no batch to the replacement run', async () => {
    h = await createFetchGroup({ partitions: [0, 1], knownNodes: [0] })
    fakeTimers()
    const refresh = gate()
    h.cluster.refreshMetadata = jest.fn(() => refresh.promise)
    h.setResponse(fetchResponse([staleWithHint(0, 7), { partition: 1, errorCode: 0 }]))
    const pending = h.group.fetch(0)
    await drain()
    expect(h.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    const retired = restart()
    expect(h.cluster.refreshMetadata).toHaveBeenCalledWith(
      expect.objectContaining({ fence: retired.fence })
    )
    refresh.release()
    await expect(finish(pending)).resolves.toEqual([])
  })

  it('a top-level error refresh is bound to the run that received it', async () => {
    h = await createFetchGroup()
    fakeTimers()
    const refresh = gate()
    h.cluster.refreshMetadata = jest.fn(() => refresh.promise)
    h.setResponse(fetchResponse([], { topLevel: 70 }))
    const pending = h.group.fetch(0)
    await drain()
    const retired = restart()
    expect(h.cluster.refreshMetadata).toHaveBeenCalledWith(
      expect.objectContaining({ fence: retired.fence })
    )
    refresh.release()
    await expect(finish(pending)).resolves.toEqual([])
  })

  it.each([
    ['stale metadata', { type: 'NOT_LEADER_FOR_PARTITION' }],
    ['a missing broker', { name: 'KafkaJSBrokerNotFound' }],
  ])('a recovery refresh after %s that outlives its run requests no rejoin', async (_, fields) => {
    h = await createFetchGroup()
    fakeTimers()
    const refresh = gate()
    h.cluster.refreshMetadata = jest.fn(() => refresh.promise)
    h.broker.fetch = jest.fn(async () => {
      throw Object.assign(new Error('failed'), fields)
    })
    const pending = h.group.fetch(0)
    await drain()
    const retired = restart()
    expect(h.cluster.refreshMetadata).toHaveBeenCalledWith(
      expect.objectContaining({ fence: retired.fence })
    )
    refresh.release()
    await expect(finish(pending)).resolves.toEqual([])
    expect(h.requestRejoin).not.toHaveBeenCalled()
    expect(unhandled).toEqual([])
  })

  it('an out-of-range error recovered through the round failure commits nothing after stop', async () => {
    h = await createFetchGroup({ autoCommit: true })
    fakeTimers()
    const discovery = gate()
    h.offsetManager.coordinator = { isConnected: () => false }
    h.cluster.findGroupCoordinator = jest.fn(() => discovery.promise)
    h.broker.fetch = jest.fn(async () => {
      throw Object.assign(new Error('out of range'), {
        name: 'KafkaJSOffsetOutOfRange',
        topic: TOPIC,
        partition: 0,
      })
    })
    const pending = h.group.fetch(0)
    await drain()
    const retired = restart()
    expect(h.cluster.findGroupCoordinator.mock.calls[0][0]).toMatchObject({ fence: retired.fence })
    discovery.release(h.broker)
    await expect(finish(pending)).resolves.toEqual([])
    expect(h.offsetCommit).not.toHaveBeenCalled()
    expect(h.offsetManager.committedOffsets()[TOPIC][0]).toBe('0')
  })

  it('an out-of-range reset waiting for its coordinator commits nothing after stop', async () => {
    h = await createFetchGroup({ autoCommit: true })
    fakeTimers()
    const discovery = gate()
    h.offsetManager.coordinator = { isConnected: () => false }
    h.cluster.findGroupCoordinator = jest.fn(() => discovery.promise)
    h.setResponse(fetchResponse([{ partition: 0, errorCode: 1 }]))
    const pending = h.group.fetch(0)
    await drain()
    expect(h.cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    const retired = restart()
    expect(h.cluster.findGroupCoordinator.mock.calls[0][0]).toMatchObject({ fence: retired.fence })
    discovery.release(h.broker)
    await expect(finish(pending)).resolves.toEqual([])
    expect(h.offsetCommit).not.toHaveBeenCalled()
    expect(h.offsetManager.committedOffsets()[TOPIC][0]).toBe('0')
    expect(h.offsetManager.coordinator.isConnected()).toBe(false)
  })

  it('a run retired as the metadata wait settles starts no preparation', async () => {
    h = await createFetchGroup()
    fakeTimers()
    const filter = jest.spyOn(h.group, 'filterPartitionsByNode')
    h.cluster.refreshMetadataIfNecessary = () => {
      Promise.resolve().then(() => Promise.resolve().then(() => h.group.onStopRequested()))
      return Promise.resolve()
    }
    await expect(finish(h.group.fetch(0))).resolves.toEqual([])
    expect(filter).not.toHaveBeenCalled()
  })

  it('a run retired as the retry yield ends starts no second preparation', async () => {
    h = await createFetchGroup({ knownNodes: [0, 1], partitionLeaders: { 0: 0 } })
    fakeTimers()
    const capture = jest.spyOn(h.group, 'captureFetchPreparation')
    const original = global.setImmediate
    const spy = jest.spyOn(global, 'setImmediate')
    const events = instrument(h, {
      before: { validate: (n) => (n === 1 ? h.installRouting({ 0: 1 }) : undefined) },
      after: {
        validate: (n) => {
          if (n !== 1) return
          spy.mockImplementationOnce((callback, ...rest) =>
            original(() => {
              callback(...rest)
              h.group.onStopRequested()
            })
          )
        },
      },
    })
    try {
      await expect(finish(h.group.fetch(1))).resolves.toEqual([])
    } finally {
      spy.mockRestore()
    }
    expect(capture).toHaveBeenCalledTimes(1)
    expect(events.filter(([label]) => label === 'validate')).toHaveLength(1)
  })
})

describe('validation response continuation', () => {
  it('leaves validation enabled once its run retires during the last outcome', async () => {
    const group = idleGroup()
    group.offsetManager = { clearPendingSeeks() {} }
    const { FetchPreparation } = require('../fetchPreparation')
    const prep = new FetchPreparation(group, group.idleWait, [], Infinity, group.ownership.token())
    group.offsetManager.committedOffsets = () => ({ orders: { 0: '0', 1: '0' } })
    group.offsetManager.nextOffset = () => ({ toString: () => '0' })
    group.offsetManager.lastConsumedEpoch = () => 1
    group.offsetManager.seek = async () => true
    group.markChanged = () => {}
    group.logger.warn = jest.fn()
    jest
      .spyOn(group.positionValidator, 'complete')
      .mockReturnValueOnce({ action: 'fatal' })
      .mockReturnValueOnce({ action: 'truncated', divergingOffset: '0' })
    group.instrumentationEmitter = new InstrumentationEventEmitter()
    group.instrumentationEmitter.addListener(
      require('../instrumentationEvents').events.LOG_TRUNCATION,
      () => {
        group.onStopRequested()
        group.startIdleWait()
      }
    )
    const disable = jest.spyOn(group.positionValidator, 'disable')
    const markAll = jest.spyOn(group, 'markAllValidated')
    await group
      .applyValidationResponse(
        [
          {
            topic: 'orders',
            entries: [
              { partition: 0, generation: 0 },
              { partition: 1, generation: 0 },
            ],
          },
        ],
        {
          topics: [
            {
              topic: 'orders',
              partitions: [
                { partition: 0, errorCode: 0 },
                { partition: 1, errorCode: 0 },
              ],
            },
          ],
        },
        prep
      )
      .catch(() => {})
    expect(prep.fence.closed).toBe(true)
    expect(disable).not.toHaveBeenCalled()
    expect(markAll).not.toHaveBeenCalled()
    group.onStopRequested()
  })
})
