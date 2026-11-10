const { ClassicFaultEpisode } = require('./fixtures/classicFaultEpisode')
const { CloseRig, TOPIC, gate } = require('./fixtures/closeRig')
const CoordinatorLookup = require('../coordinatorLookup')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const ConsumerGroupProtocol = require('../groupProtocol/consumer')
const { ConsumerOperation } = require('../groupProtocol/interface')
const GroupOwnership = require('../groupOwnership')
const OffsetManager = require('../offsetManager')
const Generation = require('../../producer/generation')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { KafkaJSDeliveryTimeout } = require('../../errors')
const { createPatternGroup } = require('./fixtures/patternGroup')
const { newLogger } = require('testHelpers')

const HELD_MS = 1500
const HEARTBEAT_INTERVAL_MS = 50
const EPISODE_TIMEOUT_MS = 25000

/** @param {number} ms @returns {Promise<void>} resolves after the real delay */
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * A healthy classic episode with a heartbeat every 50 ms and a request timeout longer than any
 * hold, so a request still pending is never mistaken for a serial retry of an expired one.
 */
const row = {
  api: 'Heartbeat',
  fault: 'none',
  step: 'healthy',
  expectedState: 'healthy',
  expectedWire: { afterLossOffsetCommits: 0, duplicateAssignments: 0 },
  maxRounds: 0,
  config: {
    groupProtocol: 'classic',
    requestTimeout: 10000,
    retry: { retries: 6, initialRetryTime: 100, factor: 0 },
  },
}

describe('Consumer > coordinator lookups under a lost coordinator', () => {
  it(
    'adds no lookup and no heartbeat for the ticks that pass while one lookup is pending',
    async () => {
      const episode = new ClassicFaultEpisode(row)
      try {
        await episode.open()
        await episode.execute()
        const { schedule } = episode.cluster
        schedule.once({ api: 'Heartbeat' }, () => new Error('The scripted connection was cut'))
        const lookup = schedule.hold({ api: 'FindCoordinator' })
        const held = await lookup.arrived()
        const from = episode.trace.findIndex(
          ({ api, node, correlationId }) =>
            api === 'FindCoordinator' && node === held.node && correlationId === held.correlationId
        )

        await pause(HELD_MS)
        const during = episode.trace.slice(from).map(({ api }) => api)
        lookup.release()
        await pause(300)
        const after = episode.trace.slice(from).map(({ api }) => api)

        expect(HELD_MS / HEARTBEAT_INTERVAL_MS).toBeGreaterThan(20)
        expect(during.filter((api) => api === 'FindCoordinator')).toEqual(['FindCoordinator'])
        expect(during.filter((api) => api === 'Heartbeat')).toEqual([])
        expect(after.filter((api) => api === 'FindCoordinator')).toHaveLength(1)
        expect(after).toContain('Heartbeat')
      } finally {
        await episode.close()
      }
    },
    EPISODE_TIMEOUT_MS
  )
})

/**
 * A cluster whose lookups wait for the test, and reject when the fence they were given closes, as
 * the real retrier does.
 * @returns {{ cluster: object, calls: {fence: Generation, resolve: Function, reject: Function}[] }}
 */
const waitingCluster = () => {
  const calls = []
  const cluster = {
    findGroupCoordinator: jest.fn(
      ({ fence }) =>
        new Promise((resolve, reject) => {
          calls.push({ fence, resolve, reject })
          fence.register(reject)
        })
    ),
  }
  return { cluster, calls }
}

const BROKER = { nodeId: 1, isConnected: () => true }
const failure = (message) => new Error(message)

describe('CoordinatorLookup', () => {
  /** @type {ReturnType<typeof waitingCluster>} */
  let wire
  /** @type {CoordinatorLookup} */
  let lookup
  beforeEach(() => {
    wire = waitingCluster()
    lookup = new CoordinatorLookup({ cluster: wire.cluster, groupId: 'g' })
  })

  test('callers, fenced or not, wait for one lookup', async () => {
    const answers = [lookup.find(), lookup.find(new Generation()), lookup.find(new Generation())]
    expect(wire.calls).toHaveLength(1)
    expect(wire.cluster.findGroupCoordinator).toHaveBeenCalledWith({
      groupId: 'g',
      fence: wire.calls[0].fence,
    })
    wire.calls[0].resolve(BROKER)
    await expect(Promise.all(answers)).resolves.toEqual([BROKER, BROKER, BROKER])
  })

  test('a caller that gives up leaves the others waiting for the same lookup', async () => {
    const leaving = new Generation()
    const gone = lookup.find(leaving).catch((error) => error)
    const staying = lookup.find(new Generation())
    leaving.close(failure('closed'))

    expect((await gone).message).toBe('closed')
    expect(wire.calls[0].fence.closed).toBe(false)
    wire.calls[0].resolve(BROKER)
    await expect(staying).resolves.toBe(BROKER)
    expect(wire.calls).toHaveLength(1)
  })

  test('the last caller leaving cancels the lookup, so a closed caller never causes a dial', async () => {
    const first = new Generation()
    const second = new ConsumerOperation()
    const answers = [first, second].map((fence) => lookup.find(fence).catch((error) => error))
    first.close(failure('first closed'))
    expect(wire.calls[0].fence.closed).toBe(false)
    second.retire(failure('second closed'))

    expect((await Promise.all(answers)).map(({ message }) => message)).toEqual([
      'first closed',
      'second closed',
    ])
    expect(wire.calls[0].fence.closed).toBe(true)
    expect(wire.calls[0].fence.error.message).toBe('second closed')
  })

  test('an unfenced caller keeps the lookup running when the only fenced caller leaves', async () => {
    const leaving = new Generation()
    const unfenced = lookup.find()
    lookup.find(leaving).catch(() => {})
    leaving.close(failure('closed'))

    expect(wire.calls[0].fence.closed).toBe(false)
    wire.calls[0].resolve(BROKER)
    await expect(unfenced).resolves.toBe(BROKER)
  })

  test('a caller whose deadline passed before the answer gets its own refusal, not the answer', async () => {
    let now = 0
    const operation = new ConsumerOperation({ deadlineAt: 1000, now: () => now })
    const answer = lookup.find(operation).catch((error) => error)
    now = 1000
    wire.calls[0].resolve(BROKER)

    expect(await answer).toBeInstanceOf(KafkaJSDeliveryTimeout)
    operation.finish()
  })

  test('a refused caller starts and joins nothing', async () => {
    const closed = new Generation()
    closed.close(failure('closed'))

    await expect(lookup.find(closed)).rejects.toThrow('closed')
    expect(wire.calls).toHaveLength(0)
  })

  test('retire cancels the lookup, fails its waiters, and the next caller starts a new one', async () => {
    const waiting = lookup.find().catch((error) => error)
    lookup.retire(failure('stopping'))

    expect((await waiting).message).toBe('stopping')
    expect(wire.calls[0].fence.closed).toBe(true)
    const next = lookup.find()
    expect(wire.calls).toHaveLength(2)
    wire.calls[1].resolve(BROKER)
    await expect(next).resolves.toBe(BROKER)
  })

  test('releaseUnfenced fails the unfenced callers and leaves the fenced ones their own lookup', async () => {
    const unfenced = lookup.find().catch((error) => error)
    const fenced = lookup.find(new Generation())
    lookup.releaseUnfenced(failure('stopping'))

    expect((await unfenced).message).toBe('stopping')
    expect(wire.calls[0].fence.closed).toBe(false)
    const later = lookup.find()
    expect(wire.calls).toHaveLength(1)
    wire.calls[0].resolve(BROKER)
    await expect(Promise.all([fenced, later])).resolves.toEqual([BROKER, BROKER])
  })

  test('releasing the only callers ends the lookup, and the next caller starts a new one', async () => {
    const unfenced = lookup.find().catch((error) => error)
    lookup.releaseUnfenced(failure('stopping'))

    expect((await unfenced).message).toBe('stopping')
    expect(wire.calls[0].fence.error.message).toBe('stopping')
    lookup.find().catch(() => {})
    expect(wire.calls).toHaveLength(2)
  })

  test('a lookup that ignores its fence is no longer shared once its last caller has left', async () => {
    const calls = []
    const stubborn = new CoordinatorLookup({
      cluster: {
        findGroupCoordinator: jest.fn(() => new Promise((resolve) => calls.push(resolve))),
      },
      groupId: 'g',
    })
    const gone = stubborn.find().catch((error) => error)
    stubborn.releaseUnfenced(failure('stopping'))
    await gone
    stubborn.find().catch(() => {})

    expect(calls).toHaveLength(2)
  })

  test('detach lets the waiters finish and gives the next caller a lookup of its own', async () => {
    const old = lookup.find()
    lookup.detach()
    const replacement = lookup.find()

    expect(wire.calls).toHaveLength(2)
    expect(wire.calls[0].fence.closed).toBe(false)
    wire.calls[0].resolve({ nodeId: 'old' })
    wire.calls[1].resolve({ nodeId: 'new' })
    await expect(old).resolves.toEqual({ nodeId: 'old' })
    await expect(replacement).resolves.toEqual({ nodeId: 'new' })
  })

  test('a lookup that settles late does not clear the newer one', async () => {
    const old = lookup.find()
    lookup.detach()
    const newer = lookup.find()
    wire.calls[0].resolve(BROKER)
    await old
    const sharing = lookup.find()

    expect(wire.calls).toHaveLength(2)
    wire.calls[1].resolve({ nodeId: 'new' })
    await expect(Promise.all([newer, sharing])).resolves.toEqual([
      { nodeId: 'new' },
      { nodeId: 'new' },
    ])
  })

  test('a failed lookup fails every waiter and the next caller starts again', async () => {
    const waiting = [lookup.find(), lookup.find(new Generation())].map((answer) =>
      answer.catch((error) => error.message)
    )
    wire.calls[0].reject(failure('unreachable'))

    await expect(Promise.all(waiting)).resolves.toEqual(['unreachable', 'unreachable'])
    lookup.find().catch(() => {})
    expect(wire.calls).toHaveLength(2)
  })
})

describe('Consumer > one coordinator lookup per membership, by protocol', () => {
  const OPTIONS = {
    groupId: 'g',
    rebalanceTimeout: 30000,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
  }

  describe('classic', () => {
    /** @returns {{ protocol: ClassicGroupProtocol, wire: ReturnType<typeof waitingCluster> }} */
    const classic = () => {
      const wire = waitingCluster()
      const protocol = new ClassicGroupProtocol({
        ...OPTIONS,
        cluster: wire.cluster,
        sessionTimeout: 30000,
        assigners: [],
      })
      return { protocol, wire }
    }

    test('the heartbeat and every join wait for one lookup, and it installs once', async () => {
      const { protocol, wire } = classic()
      protocol.coordinatorUnknown = true
      const answers = [
        protocol.liveCoordinator(),
        protocol.connect({ fence: new Generation() }),
        protocol.connect({ fence: new Generation() }),
        protocol.connect(),
      ]

      expect(wire.calls).toHaveLength(1)
      wire.calls[0].resolve(BROKER)
      await Promise.all(answers)
      expect(protocol.coordinator).toBe(BROKER)
      expect(protocol.coordinatorUnknown).toBe(false)
    })

    test('a lookup of a retired membership installs nothing and its replacement looks up for itself', async () => {
      const { protocol, wire } = classic()
      const retired = protocol.connect().catch((error) => error)
      protocol.forgetMemberId()
      const replacement = protocol.connect()

      expect(wire.calls).toHaveLength(2)
      wire.calls[0].resolve({ nodeId: 'old' })
      expect((await retired).type).toBe('JOIN_DEADLINE_EXCEEDED')
      expect(protocol.coordinator).toBeNull()
      wire.calls[1].resolve(BROKER)
      await replacement
      expect(protocol.coordinator).toBe(BROKER)
    })
  })

  describe('consumer', () => {
    test('the heartbeat and every commit wait for one lookup, and a replaced membership installs nothing', async () => {
      const wire = waitingCluster()
      const ownership = new GroupOwnership()
      const protocol = new ConsumerGroupProtocol({ ...OPTIONS, cluster: wire.cluster, ownership })
      const answers = [
        protocol.connect(),
        protocol.coordinatorLookup.find(new Generation()),
        protocol.coordinatorLookup.find(new Generation()),
      ]
      ownership.renew()

      expect(wire.calls).toHaveLength(1)
      wire.calls[0].resolve(BROKER)
      await expect(Promise.all(answers)).resolves.toEqual([BROKER, BROKER, BROKER])
      expect(protocol.coordinator).toBeNull()
    })
  })

  describe('the offset manager', () => {
    test('concurrent commits wait for the membership lookup instead of asking the cluster', async () => {
      const wire = waitingCluster()
      const shared = new CoordinatorLookup({ cluster: wire.cluster, groupId: 'g' })
      const manager = new OffsetManager({
        cluster: { ...wire.cluster, committedOffsets: () => ({}) },
        coordinator: { isConnected: () => false },
        memberAssignment: { topic1: [0] },
        groupId: 'g',
        generationId: 1,
        memberId: 'member-1',
        instrumentationEmitter: new InstrumentationEventEmitter(),
        findCoordinator: (fence) => shared.find(fence),
      })
      const fences = [new Generation(), new Generation(), new Generation()]
      const answers = fences.map((fence) => manager.getCoordinator({ guard: () => null, fence }))
      const heartbeat = shared.find()

      expect(wire.calls).toHaveLength(1)
      wire.calls[0].resolve(BROKER)
      await expect(Promise.all([...answers, heartbeat])).resolves.toEqual([
        BROKER,
        BROKER,
        BROKER,
        BROKER,
      ])
    })
  })
})

describe('Consumer > an applied assignment ends its membership lookup', () => {
  test('the caller after an apply starts a lookup of its own, and the one before still finishes', async () => {
    const harness = createPatternGroup({ catalog: { 'orders-a': 'a-id' } })
    const { group, cluster } = harness
    const old = gate()
    const answers = [{ nodeId: 'old' }, { nodeId: 'new' }]
    cluster.findGroupCoordinator.mockImplementationOnce(() => old.promise)
    cluster.findGroupCoordinator.mockImplementationOnce(async () => answers[1])
    const before = group.coordinatorLookup.find()
    const renew = jest.spyOn(group.ownership, 'renew')
    const detach = jest.spyOn(group.coordinatorLookup, 'detach')

    await harness.round()
    const after = group.coordinatorLookup.find()

    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    expect(detach).toHaveBeenCalledTimes(1)
    expect(detach.mock.invocationCallOrder[0]).toBeGreaterThan(renew.mock.invocationCallOrder[0])
    old.resolve(answers[0])
    await expect(Promise.all([before, after])).resolves.toEqual(answers)
  })
})

jest.setTimeout(30000)

const BOUND_MS = 600

/** @param {Promise<unknown>} work @returns {Promise<string>} 'committed', or the error's type or name */
const settle = (work) =>
  work.then(
    () => 'committed',
    (error) => error.type || error.name
  )

/**
 * @param {CloseRig} rig
 * @returns {number} the FindCoordinator frames written before the last OffsetCommit, the join's
 *   own included; a classic leave looks the coordinator up again afterwards
 */
const lookupsBeforeLastCommit = (rig) => {
  const apis = rig.cluster.requests().map(({ api }) => api)
  return apis.slice(0, apis.lastIndexOf('OffsetCommit')).filter((api) => api === 'FindCoordinator')
    .length
}
const COMMIT = [{ topic: TOPIC, partition: 0, offset: '2' }]
const HEARTBEAT = { classic: 'Heartbeat', consumer: 'ConsumerGroupHeartbeat' }

/**
 * A started consumer whose coordinator reads as disconnected, so every caller that needs it really
 * looks it up, while the first lookup (the heartbeat's) is held on the actual wire.
 */
class HeldLookup {
  /**
   * @param {'classic'|'consumer'} protocol
   * @param {object} [retry] consumer retry options
   * @param {object} [started] `CloseRig#started` options laid over the defaults
   * @param {Promise<unknown>} [ready] settles before the lookup is held
   * @returns {Promise<HeldLookup>} a started consumer with its heartbeat lookup held
   */
  static async open(protocol, retry = {}, started = {}, ready = Promise.resolve()) {
    const rig = await CloseRig.open({ protocol, retry: { retries: 5, ...retry } })
    const lookups = rig.lookup()
    const { consumer } = await rig.started({
      consumer: protocol === 'classic' ? { heartbeatInterval: 50 } : {},
      ...started,
      run: { autoCommit: false, ...started.run },
    })
    await ready
    const { schedule } = rig.cluster
    const lookup = schedule.hold({ api: 'FindCoordinator' })
    schedule.once({ api: HEARTBEAT[protocol] }, () => new Error('The scripted connection was cut'))
    lookups.makeStale()
    await lookup.arrived()
    return new HeldLookup(rig, consumer, lookup, lookups)
  }

  /**
   * @param {CloseRig} rig
   * @param {object} consumer
   * @param {object} lookup the held FindCoordinator
   * @param {import('./fixtures/closeRig').LookupSwitch} lookups counts the cluster's lookups
   */
  constructor(rig, consumer, lookup, lookups) {
    this.rig = rig
    this.consumer = consumer
    this.lookup = lookup
    this.lookups = lookups
    this.released = false
    this.mark = lookups.calls.length - 1
    this.markFrames = rig.frames('FindCoordinator').length - 1
  }

  /** @returns {number} the lookups the cluster started since the held one, itself included */
  started() {
    return this.lookups.calls.length - this.mark
  }

  /** @returns {number} the FindCoordinator frames written since the held one, itself included */
  frames() {
    return this.rig.frames('FindCoordinator').length - this.markFrames
  }

  /** @returns {void} answers the held lookup, once */
  release() {
    if (this.released) return
    this.released = true
    this.lookup.release()
  }

  /** @returns {Promise<void>} releases the lookup and disposes the rig */
  async dispose() {
    this.release()
    await this.rig.dispose()
  }
}

describe.each(['classic', 'consumer'])(
  'Consumer > one coordinator lookup per membership, %s',
  (protocol) => {
    /** @type {HeldLookup} */
    let held
    afterEach(async () => {
      await held.dispose()
    })

    test('commits made while a heartbeat lookup is held share it instead of starting their own', async () => {
      held = await HeldLookup.open(protocol)
      const commits = [1, 2, 3].map(() => held.consumer.commitOffsets(COMMIT))
      await pause(250)

      expect([held.started(), held.frames()]).toEqual([1, 1])

      held.release()
      await Promise.all(commits)
      expect(held.rig.frames('OffsetCommit').length).toBeGreaterThanOrEqual(1)
    })

    test('a commit whose deadline expires leaves the others to finish through the one lookup', async () => {
      held = await HeldLookup.open(protocol, { maxTotalTime: BOUND_MS })
      const early = held.consumer.commitOffsets(COMMIT).catch((error) => error)
      await pause(BOUND_MS - 250)
      const late = held.consumer.commitOffsets(COMMIT)

      expect(await early).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect([held.started(), held.frames()]).toEqual([1, 1])

      held.release()
      await late
      expect(held.rig.frames('OffsetCommit').length).toBeGreaterThanOrEqual(1)
    })

    test('a graceful stop leaves the commits waiting on the held lookup to finish, each under its own fence', async () => {
      const entered = gate()
      const proceed = gate()
      const owner = {}
      const handlerOutcome = []
      let first = true
      held = await HeldLookup.open(
        protocol,
        {},
        {
          delivered: 0,
          run: {
            eachMessage: async () => {
              if (!first) return
              first = false
              entered.resolve()
              await proceed.promise
              handlerOutcome.push(await settle(owner.consumer.commitOffsets(COMMIT)))
            },
          },
        },
        entered.promise
      )
      owner.consumer = held.consumer
      proceed.resolve()
      const outside = held.consumer.commitOffsets(COMMIT)
      await pause(150)
      const stopping = held.consumer.stop()
      await pause(100)

      held.release()
      await Promise.all([stopping, outside])

      expect(handlerOutcome).toEqual(['CONSUMER_STOPPING'])
      expect(lookupsBeforeLastCommit(held.rig)).toBe(2)
      expect(held.rig.frames('OffsetCommit')).toHaveLength(1)
    })

    test('a graceful stop lets a close-callback commit share the held lookup', async () => {
      const owner = {}
      const outcome = []
      held = await HeldLookup.open(
        protocol,
        {},
        {
          run: {
            rebalanceListener: {
              onPartitionsRevoked: async () => {
                if (owner.consumer) outcome.push(await settle(owner.consumer.commitOffsets(COMMIT)))
              },
            },
          },
        }
      )
      owner.consumer = held.consumer
      const outside = held.consumer.commitOffsets(COMMIT)
      await pause(100)
      const stopping = held.consumer.stop()
      await pause(150)

      held.release()
      await Promise.all([stopping, outside])

      expect(outcome).toEqual(['committed'])
      expect(lookupsBeforeLastCommit(held.rig)).toBe(2)
      expect(held.rig.frames('OffsetCommit')).toHaveLength(2)
    })

    test('a forced close fails a commit waiting on the held lookup and nothing is written after', async () => {
      held = await HeldLookup.open(protocol)
      const commit = held.consumer.commitOffsets(COMMIT).catch((error) => error)
      await pause(100)
      await held.consumer.disconnect({ timeout: 0 })
      const framesAtClose = held.rig.cluster.requests().length

      expect((await commit).type).toBe('CONSUMER_STOPPING')
      held.release()
      await pause(250)

      const after = held.rig.cluster
        .requests()
        .slice(framesAtClose)
        .map(({ api }) => api)
      expect(
        after.filter((api) => ['OffsetCommit', 'Fetch', HEARTBEAT[protocol]].includes(api))
      ).toEqual([])
      expect(held.rig.frames('OffsetCommit')).toHaveLength(0)
    })

    test('a membership that stopped sends no frame when its held lookup is answered', async () => {
      held = await HeldLookup.open(protocol)
      await held.consumer.stop()
      const framesAtStop = held.rig.cluster.requests().length

      held.release()
      await pause(250)

      const after = held.rig.cluster
        .requests()
        .slice(framesAtStop)
        .map(({ api }) => api)
      expect(
        after.filter((api) => ['OffsetCommit', 'Fetch', HEARTBEAT[protocol]].includes(api))
      ).toEqual([])
    })
  }
)
