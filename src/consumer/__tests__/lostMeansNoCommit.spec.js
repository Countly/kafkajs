const { Kafka } = require('../../../index')
const { KafkaJSCommitFailedError } = require('../../errors')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const OffsetManager = require('../offsetManager')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('./fixtures/fetchHarness')
const { waitForCondition } = require('./fixtures/liveScenario')

const UNKNOWN_MEMBER_ID = 25
const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9092 },
]

/** Lets the answer just written reach the client and what it wakes run. */
const flush = async () => {
  for (let turn = 0; turn < 10; turn++) {
    await new Promise(setImmediate)
  }
}

/** Polls until `predicate` holds, and fails naming `what` when it never does. */
const until = async (predicate, what) => {
  if (!(await waitForCondition(predicate, { attempts: 400, delay: 5 }))) {
    throw new Error(`${what} did not happen`)
  }
}

/** Waits for a promise, failing with `what` after `ms` instead of leaving the test to time out. */
const within = (promise, ms, what) => {
  let timer
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not happen within ${ms} ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

/**
 * Wraps a gate so that `release` is safe at any moment: the held call goes once, as soon as it is
 * there, however often and however early `release` is called.
 * @param {{ arrived: () => Promise<object>, release: () => void }} gate
 * @returns {{ arrived: () => Promise<object>, release: () => void }}
 */
const releasableOnce = (gate) => {
  let arrived = false
  let wanted = false
  let released = false
  const go = () => {
    if (arrived && wanted && !released) {
      released = true
      gate.release()
    }
  }
  gate.arrived().then(() => {
    arrived = true
    go()
  })

  return {
    arrived: () => gate.arrived(),
    release: () => {
      wanted = true
      go()
    },
  }
}

/**
 * Holds the next coordinator lookup of an offset commit in the client, before it reaches the wire,
 * until `release`: a heartbeat that lost its coordinator looks it up on the wire too, so holding
 * the next FindCoordinator there could hold the heartbeat's instead.
 * @returns {{ arrived: () => Promise<void>, release: () => void }} a gate `releasableOnce` takes
 */
const holdNextCommitLookup = () => {
  let arrive
  let release
  const arrival = new Promise((resolve) => {
    arrive = resolve
  })
  const released = new Promise((resolve) => {
    release = resolve
  })
  const getCoordinator = OffsetManager.prototype.getCoordinator
  jest.spyOn(OffsetManager.prototype, 'getCoordinator').mockImplementationOnce(async function () {
    arrive()
    await released
    return getCoordinator.call(this)
  })
  return { arrived: () => arrival, release: () => release() }
}

/**
 * A member that is lost may not commit: the coordinator has handed its partitions on, so an
 * OffsetCommit it sends could write over the new owner's progress. These cases run the real
 * consumer over the scripted cluster and hold the request a commit waits on.
 *
 * The invariant case asserts that the membership fence prevents the late commit; setup and
 * settlement are checked by ordinary hooks and a guard case.
 */
describe('Consumer > commits under a lost membership', () => {
  let consumer
  let rejoin
  let scenario
  let beforeRelease
  let outcome
  /** What teardown lets go of, oldest first; each is safe in any state the scenario reached. */
  const drains = []

  /**
   * A commit that waits for the coordinator lookup while the coordinator tells the member it is
   * lost. Resolves once both are true: the lookup is held and the member has been told. Whatever
   * it creates is registered in `drains` at once, so a setup that fails midway still lets go.
   * @returns {Promise<{ cluster: object, lookup: object, commitOf: () => Promise<void> }>}
   *   `commitOf()` is the pending commit, which settles once the lookup is released
   */
  const commitWaitingForLookupOfLostMember = async () => {
    const cluster = groupScript(createScriptedCluster({ nodes: NODES }), { coordinator: 1 })
    drains.push(() => cluster.up(1))
    // Gives the hook bounds margin; unrelated commit errors must still reject the hook.
    consumer = new Kafka(cluster.kafkaOptions({ requestTimeout: 20000 })).consumer({
      groupId: 'lost-group',
      autoCommit: false,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 20,
    })
    const inHandler = jest.fn()
    let finishHandler
    const handlerMayFinish = new Promise((resolve) => {
      finishHandler = resolve
    })
    drains.push(finishHandler)
    let commit
    let lookup
    let armed
    const lookupArmed = new Promise((resolve) => {
      armed = resolve
    })
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      eachMessage: async () => {
        inHandler()
        await handlerMayFinish
        // The coordinator's connection resets and the node stays down, so nothing reconnects it
        // before the commit starts and the commit has to look the coordinator up again.
        cluster.down(1)
        cluster
          .sockets()
          .filter(({ endpoint }) => endpoint.host === 'node1')
          .forEach((socket) => socket.emit('error', new Error('connection reset')))
        await flush()
        lookup = releasableOnce(holdNextCommitLookup())
        drains.push(lookup.release)
        armed()
        commit = consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }])
        await commit.catch(() => {})
      },
    })
    await until(() => inHandler.mock.calls.length > 0, 'the handler starting')

    finishHandler()
    await within(lookupArmed, 5000, 'the commit starting')
    await within(lookup.arrived(), 5000, 'the held coordinator lookup')
    cluster.up(1)

    const heartbeats = () => cluster.requests().filter(({ api }) => api === 'Heartbeat').length
    const before = heartbeats()
    cluster.schedule.once({ api: 'Heartbeat' }, { errorCode: UNKNOWN_MEMBER_ID })
    await until(() => heartbeats() > before, 'the heartbeat that tells the member it was lost')
    await until(
      () =>
        rejoin.mock.calls.some(
          ([verdict]) => verdict.kind === 'lost' && verdict.error?.code === UNKNOWN_MEMBER_ID
        ),
      'the client receiving the lost-membership verdict'
    )

    return { cluster, lookup, commitOf: () => commit }
  }

  beforeAll(() => prepareRecordBatches())

  beforeEach(async () => {
    rejoin = jest.spyOn(RebalanceCoordinator.prototype, 'requestRejoin')
    scenario = await within(commitWaitingForLookupOfLostMember(), 10000, 'the lost-member scenario')
    const { cluster, lookup, commitOf } = scenario
    beforeRelease = {
      commits: cluster.requests().filter(({ api }) => api === 'OffsetCommit'),
      lost: rejoin.mock.calls.some(
        ([verdict]) => verdict.kind === 'lost' && verdict.error?.code === UNKNOWN_MEMBER_ID
      ),
    }
    expect(beforeRelease).toEqual({ commits: [], lost: true })
    lookup.release()
    outcome = await within(
      commitOf().then(
        () => null,
        (error) => {
          if (!(error instanceof KafkaJSCommitFailedError)) {
            throw error
          }
          return error
        }
      ),
      5000,
      'the released commit settling'
    )
    cluster.assertClean()
  })

  afterEach(async () => {
    try {
      drains.splice(0).forEach((drain) => drain())
      await within(consumer?.disconnect(), 5000, 'consumer disconnect')
    } finally {
      jest.restoreAllMocks()
      consumer = rejoin = scenario = beforeRelease = outcome = undefined
    }
  })

  it('reaches the held lookup and the loss before it releases the lookup', () => {
    expect(beforeRelease).toEqual({ commits: [], lost: true })
  })

  it('sends no OffsetCommit for a commit that was waiting for the coordinator lookup when the member was lost', () => {
    expect(scenario.cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
    expect(outcome).toBeInstanceOf(KafkaJSCommitFailedError)
  })
})

const GroupOwnership = require('../groupOwnership')
const handlerContext = require('../handlerContext')
const Runner = require('../runner')
const ConsumerGroupProtocol = require('../groupProtocol/consumer')
const { KafkaJSProtocolError, isLostMember } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const {
  stampOwnership,
  stampOffsetCommit,
  sentOwnership,
  obsoleteOwnership,
  learnOwnershipLoss,
  REBALANCE_PATHS,
  pollIntervalExceededError,
} = require('../rebalanceErrorContract')
const {
  createScheduledOffsetManager,
  groupOver,
  TOPIC: SCHEDULED_TOPIC,
} = require('../offsetManager/__tests__/fixtures/scheduledOffsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')
const { Schedule } = require('../../../testHelpers/schedule')
const {
  events: { COMMIT_OFFSETS },
} = require('../instrumentationEvents')

/** @param {number} code @returns {Error} a real decoded broker error with literal input code */
const brokerError = (code) => new KafkaJSProtocolError(createErrorFromCode(code))

/**
 * A real group, manager and runner with no running fetch manager. All asynchronous crossings are
 * driven by gates, so no background loop can replace membership before a case has asserted it.
 * @param {{kind?: string, autoCommit?: boolean, eachMessage?: Function, eachBatch?: Function}} options
 * @returns {object} the production collaborators and explicit cleanup
 */
const ownershipFixture = ({
  kind = 'classic',
  isStatic = false,
  autoCommit = true,
  eachMessage,
  eachBatch,
} = {}) => {
  const offsets = createScheduledOffsetManager({ autoCommit, partitions: [0, 1] })
  const group = groupOver(offsets, [{ topic: SCHEDULED_TOPIC, partitions: [0, 1] }])
  // The real kind accessor belongs to the membership prototype; the fixture replaces membership
  // only when it needs to exercise the consumer protocol's error classifier below.
  Object.defineProperty(group, 'kind', { value: kind })
  group.groupMembership.groupInstanceId = isStatic ? 'static-instance' : null
  offsets.manager.ownership = group.ownership
  offsets.manager.membershipProtocol = kind
  offsets.cluster.topicId = () => undefined
  offsets.cluster.defaultOffset = () => '-2'
  group.acquired.set(`${SCHEDULED_TOPIC}|0`, { acquiredAt: 1, changedAt: 1 })
  group.acquired.set(`${SCHEDULED_TOPIC}|1`, { acquiredAt: 2, changedAt: 2 })
  group.acquireClock = 2
  const runner = new Runner({
    consumerGroup: group,
    onCrash: jest.fn(),
    instrumentationEmitter: group.instrumentationEmitter,
    logger: newLogger(),
    eachMessage,
    eachBatch,
    eachBatchAutoResolve: false,
    concurrency: 1,
    heartbeatInterval: 1000,
    maxPollInterval: 300000,
    retry: { retries: 2, initialRetryTime: 20, factor: 0, multiplier: 1 },
    autoCommit,
  })
  runner.running = true
  group.heartbeat = jest.fn(async () => {})
  const batch = (partition = 0) => ({
    topic: SCHEDULED_TOPIC,
    partition,
    assignmentEpoch: group.assignmentEpoch,
    acquireClock: group.acquireClock,
    messages: [{ offset: '0', value: Buffer.from('value') }],
    firstOffset: () => '0',
    lastOffset: () => '0',
    leaderEpochAt: () => -1,
    isEmpty: () => false,
  })
  return {
    ...offsets,
    group,
    runner,
    ownership: group.ownership,
    batch,
    close: async () => {
      runner.heartbeatManager.shutdown()
      runner.pollTimer.stop()
      group.obligations.clear()
      runner.running = false
      expect(offsets.operations.size()).toBe(0)
    },
  }
}

/**
 * Checks every public refusal field, including identity of the causal broker error.
 * @param {unknown} error the settled rejection
 * @param {Error|undefined} cause its exact original loss
 * @param {string} reason the specified refusal reason
 * @returns {void}
 */
const expectRefusal = (error, cause, reason = 'membership-lost') => {
  expect(error).toBeInstanceOf(KafkaJSCommitFailedError)
  expect(error.reason).toBe(reason)
  expect(error.cause).toBe(cause)
  expect(error.retriable).toBe(false)
  expect(error.category).toBe('applicationRecoverable')
  if (reason === 'stale-assignment') {
    expect(error.type).toBe('STALE_ASSIGNMENT')
    expect(error.code).toBeUndefined()
  } else if (cause) {
    expect(error.type).toBe(cause.type)
    expect(error.code).toBe(cause.code)
    expect(error.helpUrl).toBe(cause.helpUrl)
  }
}

/** @param {Promise<unknown>} promise @returns {Promise<Error|null>} rejects unrelated failures */
const refusalOf = (promise) =>
  promise.then(
    () => null,
    (error) => {
      if (!(error instanceof KafkaJSCommitFailedError)) throw error
      return error
    }
  )

describe('membership ownership identities', () => {
  it('starts valid and renews a distinct numeric identity', () => {
    const ownership = new GroupOwnership()
    expect(ownership.refusal(ownership.token())).toBeNull()
    const old = ownership.token()
    expect(ownership.renew()).toBe(old + 1)
    expectRefusal(ownership.refusal(old), undefined, 'stale-assignment')
    expect(ownership.refusal(ownership.token())).toBeNull()
  })

  it.each([22, 25, 69, 82, 110, 30, 42, 111, 112, 128])(
    'retains code %i without changing the public predicate',
    (code) => {
      const ownership = new GroupOwnership()
      const token = ownership.token()
      const release = ownership.retain(token)
      const cause = brokerError(code)
      ownership.lose(cause, token)
      expectRefusal(ownership.refusal(token), cause)
      expect(isLostMember(ownership.refusal(token))).toBe([22, 25, 82, 110].includes(code))
      ownership.renew()
      ownership.renew()
      expectRefusal(ownership.refusal(token), cause)
      expect(ownership.refusal(ownership.token())).toBeNull()
      release()
      release()
      expect(ownership.states.size).toBe(1)
    }
  )

  it('keeps the first loss when another answer repeats it', () => {
    const ownership = new GroupOwnership()
    const first = brokerError(25)
    ownership.lose(first, ownership.token())
    ownership.lose(brokerError(22), ownership.token())
    expectRefusal(ownership.refusal(ownership.token()), first)
  })

  it('does not turn an old fatal answer into permanent abandonment', () => {
    const ownership = new GroupOwnership()
    const old = ownership.token()
    ownership.renew()
    ownership.lose(brokerError(25), old)
    ownership.abandon(brokerError(82), old)
    expect(ownership.refusal(ownership.token())).toBeNull()
    expect(ownership.renew()).toBe(old + 2)
  })

  it('cannot renew a run abandoned by its current identity', () => {
    const ownership = new GroupOwnership()
    const cause = brokerError(82)
    const token = ownership.token()
    ownership.abandon(cause, token)
    expect(ownership.renew()).toBe(token)
    expectRefusal(ownership.refusal(token), cause)
  })

  it('keeps no idle membership history during one thousand rejoins', () => {
    const ownership = new GroupOwnership()
    for (let i = 0; i < 1000; i++) {
      ownership.lose(brokerError(25), ownership.token())
      ownership.renew()
      expect(ownership.states.size).toBe(1)
    }
    const noLongerRetained = ownership.retain(-1)
    expect(noLongerRetained()).toBeUndefined()
    expect(ownership.states.size).toBe(1)
  })

  it('keeps no synthetic broker code on poll expiry', () => {
    const ownership = new GroupOwnership()
    const cause = pollIntervalExceededError({ elapsedMs: 301, maxPollInterval: 300 })
    ownership.lose(cause, ownership.token())
    const error = ownership.refusal(ownership.token())
    expectRefusal(error, cause, 'poll-interval-exceeded')
    expect(error.code).toBeUndefined()
    expect(isLostMember(error)).toBe(false)
  })

  it('retains a current token only until its last operation releases it', () => {
    const ownership = new GroupOwnership()
    const token = ownership.token()
    const a = ownership.retain(token)
    const b = ownership.retain(token)
    ownership.renew()
    a()
    expect(ownership.states.has(token)).toBe(true)
    b()
    expect(ownership.states.has(token)).toBe(false)
  })
})

describe('handler context isolation', () => {
  it('preserves exact synchronous results and errors', () => {
    expect(handlerContext.current()).toBeUndefined()
    const batch = {}
    const error = new Error('handler failed')
    expect(handlerContext.run(batch, () => handlerContext.current())).toBe(batch)
    expect(() =>
      handlerContext.run(batch, () => {
        throw error
      })
    ).toThrow(error)
    expect(handlerContext.current()).toBeUndefined()
  })

  it('does not mix simultaneous batches or a queue created before them', async () => {
    const schedule = new Schedule()
    const first = schedule.hold({ api: 'first' })
    const second = schedule.hold({ api: 'second' })
    const a = {}
    const b = {}
    const left = handlerContext.run(a, async () => {
      await schedule.run({ api: 'first' }, () => {})
      expect(handlerContext.current()).toBe(a)
    })
    const right = handlerContext.run(b, async () => {
      await schedule.run({ api: 'second' }, () => {})
      expect(handlerContext.current()).toBe(b)
    })
    try {
      await Promise.all([first.arrived(), second.arrived()])
      expect(handlerContext.current()).toBeUndefined()
      second.release()
      await right
      first.release()
      await left
    } finally {
      // Both gates have arrived before this block; each release is made once above.
      await Promise.allSettled([left, right])
    }
  })
})

describe('handler context contents', () => {
  it.each(['eachMessage', 'eachBatch'])(
    'holds only the ownership identity of the batch a %s handler is running',
    async (api) => {
      const stores = []
      const fixture = ownershipFixture({
        [api]: async () => {
          stores.push(handlerContext.current())
        },
      })
      const batch = {
        ...fixture.batch(),
        ownershipKey: `${SCHEDULED_TOPIC}|0`,
        highWatermark: '1',
        offsetLag: () => '0',
        offsetLagLow: () => '0',
        isEmptyDueToFiltering: () => false,
      }
      try {
        await fixture.runner.handleBatch(batch)
        expect(stores).toHaveLength(1)
        const [store] = stores
        expect(store).not.toBe(batch)
        expect(store).toEqual({
          topic: SCHEDULED_TOPIC,
          partition: 0,
          assignmentEpoch: fixture.group.assignmentEpoch,
          acquireClock: fixture.group.acquireClock,
          ownershipKey: `${SCHEDULED_TOPIC}|0`,
          runIdentity: fixture.group.runIdentity,
        })
        // Everything but the run's own identity is a primitive: no batch, message or buffer is kept.
        const { runIdentity, ...rest } = store
        expect(runIdentity).not.toBe(batch)
        expect(Object.values(rest).every((value) => Object(value) !== value)).toBe(true)
      } finally {
        await fixture.close()
      }
    }
  )
})

describe.each([
  ['classic', false],
  ['classic', true],
  ['consumer', false],
  ['consumer', true],
])('commit ownership on %s static %s', (kind, isStatic) => {
  let fixture
  afterEach(async () => {
    if (fixture) await fixture.close()
    fixture = undefined
    jest.useRealTimers()
  })

  it.each([22, 25, 69, 82, 110, ...(kind === 'consumer' ? [30, 42, 111, 112, 128] : [])])(
    'learns code %i in the common sender before any outer handler receives it',
    async (code) => {
      fixture = ownershipFixture({ kind, isStatic })
      const { manager, ownership, coordinator } = fixture
      const cause = brokerError(code)
      coordinator.offsetCommit.mockRejectedValueOnce(cause)
      const error = await refusalOf(
        manager.commitOffsets({
          topics: [
            {
              topic: SCHEDULED_TOPIC,
              partitions: [{ partition: 0, offset: '9' }],
            },
          ],
        })
      )
      expectRefusal(error, cause)
      expect(ownership.refusal(ownership.token()).cause).toBe(cause)
      expect(sentOwnership(error)).toBe(ownership.token())
      const calls = coordinator.offsetCommit.mock.calls.length
      const refused = await refusalOf(
        manager.commitOffsets({
          topics: [
            {
              topic: SCHEDULED_TOPIC,
              partitions: [{ partition: 1, offset: '12' }],
            },
          ],
        })
      )
      expectRefusal(refused, cause)
      expect(coordinator.offsetCommit).toHaveBeenCalledTimes(calls)
      expect(manager.committedOffsets()[SCHEDULED_TOPIC][0]).toBeUndefined()
    }
  )

  it.each(['seek', 'setDefaultOffset'])(
    'learns loss from a held %s before another commit',
    async (method) => {
      fixture = ownershipFixture({ kind, isStatic })
      const { schedule, manager, ownership, coordinator } = fixture
      const held = schedule.hold({ api: 'OffsetCommit' })
      const originalLastCommit = manager.lastCommit
      const operation = manager[method]({ topic: SCHEDULED_TOPIC, partition: 0, offset: '4' })
      await held.arrived()
      const cause = brokerError(69)
      held.fail(cause)
      await expect(operation).resolves.toBe(method === 'seek' ? false : undefined)
      expectRefusal(ownership.refusal(ownership.token()), cause)
      const error = await refusalOf(
        manager.commitOffsets({
          topics: [
            {
              topic: SCHEDULED_TOPIC,
              partitions: [{ partition: 1, offset: '11' }],
            },
          ],
        })
      )
      expectRefusal(error, cause)
      expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
      expect(manager.lastCommit).toBe(originalLastCommit)
      expect(fixture.operations.size()).toBe(0)
    }
  )

  it.each(['accepted', 'rejected'])('fences a transmitted %s commit locally', async (answer) => {
    fixture = ownershipFixture({ kind, isStatic })
    const { schedule, manager, ownership, coordinator } = fixture
    const held = schedule.hold({ api: 'OffsetCommit' })
    const originalLastCommit = manager.lastCommit
    const committing = refusalOf(
      manager.commitOffsets({
        topics: [
          {
            topic: SCHEDULED_TOPIC,
            partitions: [{ partition: 0, offset: '7' }],
          },
        ],
      })
    )
    await held.arrived()
    const cause = brokerError(25)
    ownership.lose(cause, ownership.token())
    if (answer === 'accepted') held.release()
    else held.fail(brokerError(16))
    expectRefusal(await committing, cause)
    expect(manager.committedOffsets()[SCHEDULED_TOPIC][0]).toBeUndefined()
    expect(manager.lastCommit).toBe(originalLastCommit)
    expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
    expect(fixture.operations.size()).toBe(0)
  })

  it('captures before lookup and never transmits after loss during lookup', async () => {
    fixture = ownershipFixture({ kind, isStatic })
    const { manager, ownership, coordinator, schedule } = fixture
    const held = schedule.hold({ api: 'FindCoordinator' })
    manager.getCoordinator = () => schedule.run({ api: 'FindCoordinator' }, () => coordinator)
    const committing = refusalOf(
      manager.commitOffsets({
        topics: [
          {
            topic: SCHEDULED_TOPIC,
            partitions: [{ partition: 0, offset: '7' }],
          },
        ],
      })
    )
    await held.arrived()
    const cause = brokerError(25)
    ownership.lose(cause, ownership.token())
    ownership.renew()
    held.release()
    expectRefusal(await committing, cause)
    expect(coordinator.offsetCommit).not.toHaveBeenCalled()
    expect(manager.lastCommit).toBeDefined()
    expect(manager.committedOffsets()[SCHEDULED_TOPIC][0]).toBeUndefined()
    expect(ownership.refusal(ownership.token())).toBeNull()
  })

  it.each(['entry', 'pool', 'connect', 'authenticate', 'encode', 'pending'])(
    'refuses at the %s transport crossing',
    async (boundary) => {
      fixture = ownershipFixture({ kind, isStatic })
      const { manager, coordinator, ownership } = fixture
      const schedule = new Schedule()
      const held = schedule.hold({ api: boundary })
      const frames = []
      let checks = 0
      coordinator.offsetCommit.mockImplementation(async (payload, { guard }) => {
        for (const site of ['entry', 'pool', 'connect', 'authenticate', 'encode', 'pending']) {
          const error = guard()
          checks++
          if (error) throw error
          if (site === boundary) await schedule.run({ api: site }, () => {})
        }
        const error = guard()
        checks++
        if (error) throw error
        frames.push(payload)
        return {}
      })
      const committing = refusalOf(
        manager.commitOffsets({
          topics: [
            {
              topic: SCHEDULED_TOPIC,
              partitions: [{ partition: 0, offset: '7' }],
            },
          ],
        })
      )
      await held.arrived()
      const cause = brokerError(25)
      ownership.lose(cause, ownership.token())
      held.release()
      expectRefusal(await committing, cause)
      expect(frames).toEqual([])
      expect(checks).toBeGreaterThan(0)
      expect(manager.committedOffsets()[SCHEDULED_TOPIC][0]).toBeUndefined()
      ownership.renew()
      await manager.commitOffsets({
        topics: [
          {
            topic: SCHEDULED_TOPIC,
            partitions: [{ partition: 0, offset: '8' }],
          },
        ],
      })
      expect(frames).toHaveLength(1)
      expect(manager.committedOffsets()[SCHEDULED_TOPIC][0]).toBe('8')
    }
  )

  it('skips all automatic forms without advancing lastCommit after known loss', async () => {
    fixture = ownershipFixture({ kind, isStatic })
    const { manager, ownership, runner, coordinator } = fixture
    manager.autoCommitThreshold = 1
    manager.resolveOffset({ topic: SCHEDULED_TOPIC, partition: 0, offset: '4' })
    const originalLastCommit = manager.lastCommit
    const cause = brokerError(25)
    ownership.lose(cause, ownership.token())
    await runner.autoCommitOffsets()
    await runner.autoCommitOffsetsIfNecessary()
    await manager.commitOffsetsIfNecessary()
    await manager.commitOffsets({}, { auto: true })
    expect(coordinator.offsetCommit).not.toHaveBeenCalled()
    expect(manager.lastCommit).toBe(originalLastCommit)
  })

  it('skips a no-auto-commit seek after membership loss', async () => {
    fixture = ownershipFixture({ kind, isStatic, autoCommit: false })
    const { manager, ownership } = fixture
    ownership.lose(brokerError(25), ownership.token())
    await expect(manager.seek({ topic: SCHEDULED_TOPIC, partition: 0, offset: '4' })).resolves.toBe(
      false
    )
    expect(manager.resolvedOffsetAt(SCHEDULED_TOPIC, 0)).toBeUndefined()
    expect(fixture.operations.size()).toBe(0)
  })

  it('rejects an empty explicit commit while lost without updating lastCommit', async () => {
    fixture = ownershipFixture({ kind, isStatic })
    const { manager, ownership } = fixture
    const originalLastCommit = manager.lastCommit
    const cause = brokerError(25)
    ownership.lose(cause, ownership.token())
    expectRefusal(await refusalOf(manager.commitOffsets({ topics: [] })), cause)
    expect(manager.lastCommit).toBe(originalLastCommit)
  })

  it('keeps detached commits usable after a rejoin and refuses only a later loss', async () => {
    fixture = ownershipFixture({ kind, isStatic })
    const { runner, ownership, coordinator } = fixture
    const offsets = {
      topics: [{ topic: SCHEDULED_TOPIC, partitions: [{ partition: 0, offset: '4' }] }],
    }
    expect(handlerContext.current()).toBeUndefined()
    await runner.commitOffsets(offsets)
    ownership.renew()
    await runner.commitOffsets(offsets)
    const cause = brokerError(25)
    ownership.lose(cause, ownership.token())
    expectRefusal(await refusalOf(runner.commitOffsets(offsets)), cause)
    expect(coordinator.offsetCommit).toHaveBeenCalledTimes(2)
  })

  it('bails with the exact refusal before the commit retrier can wrap it', async () => {
    fixture = ownershipFixture({ kind, isStatic })
    const { runner, group } = fixture
    const refusal = new KafkaJSCommitFailedError('refused', { reason: 'stale-assignment' })
    const commit = jest.spyOn(group, 'commitOffsets').mockRejectedValue(refusal)
    await expect(runner.commitOffsets({ topics: [] })).rejects.toBe(refusal)
    expect(commit).toHaveBeenCalledTimes(1)
  })

  it('does not recapture a replacement identity during retry backoff', async () => {
    jest.useFakeTimers()
    fixture = ownershipFixture({ kind, isStatic })
    const { runner, ownership, group } = fixture
    const commit = jest
      .spyOn(group, 'commitOffsets')
      .mockRejectedValueOnce(brokerError(16))
      .mockResolvedValue()
    const committing = refusalOf(runner.commitOffsets({ topics: [] }))
    await Promise.resolve()
    await Promise.resolve()
    await Promise.resolve()
    const cause = brokerError(25)
    ownership.lose(cause, ownership.token())
    ownership.renew()
    await jest.advanceTimersByTimeAsync(100)
    expectRefusal(await committing, cause)
    expect(commit).toHaveBeenCalledTimes(1)
    await runner.commitOffsets({ topics: [] })
    expect(commit).toHaveBeenCalledTimes(2)
    expect(ownership.states.size).toBe(1)
    jest.useRealTimers()
  })

  it.each(['eachMessage', 'eachBatch'])(
    'keeps the %s handler error when membership is lost',
    async (api) => {
      const error = new Error('handler owns this error')
      let cause
      fixture = ownershipFixture({
        kind,
        isStatic,
        [api]: async () => {
          cause = brokerError(25)
          fixture.ownership.lose(cause, fixture.ownership.token())
          throw error
        },
      })
      const { runner, coordinator, batch } = fixture
      await expect(
        handlerContext.run(batch(), () =>
          api === 'eachMessage'
            ? runner.processEachMessage(batch())
            : runner.processEachBatch(batch())
        )
      ).rejects.toBe(error)
      expect(coordinator.offsetCommit).not.toHaveBeenCalled()
      expectRefusal(fixture.ownership.refusal(fixture.ownership.token()), cause)
    }
  )

  it.each(['eachMessage', 'eachBatch'])(
    'keeps the %s handler error when an otherwise valid commit fails',
    async (api) => {
      const error = new Error('application error')
      fixture = ownershipFixture({
        kind,
        isStatic,
        [api]: async () => {
          throw error
        },
      })
      fixture.manager.resolveOffset({ topic: SCHEDULED_TOPIC, partition: 0, offset: '0' })
      fixture.coordinator.offsetCommit.mockRejectedValue(brokerError(30))
      const batch = fixture.batch()
      await expect(
        handlerContext.run(batch, () =>
          api === 'eachMessage'
            ? fixture.runner.processEachMessage(batch)
            : fixture.runner.processEachBatch(batch)
        )
      ).rejects.toBe(error)
    }
  )

  it('rejects a stale handler public commit while a fresh detached commit succeeds', async () => {
    fixture = ownershipFixture({ kind, isStatic })
    const { runner, group, ownership, batch, coordinator } = fixture
    const old = batch()
    group.assignmentEpoch++
    ownership.renew()
    const offsets = {
      topics: [{ topic: SCHEDULED_TOPIC, partitions: [{ partition: 0, offset: '7' }] }],
    }
    expectRefusal(
      await refusalOf(handlerContext.run(old, () => runner.commitOffsets(offsets))),
      undefined,
      'stale-assignment'
    )
    expect(coordinator.offsetCommit).not.toHaveBeenCalled()
    await runner.commitOffsets(offsets)
    expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
  })

  it('distinguishes revoked-and-reacquired from a retained partition in one cooperative epoch', async () => {
    fixture = ownershipFixture({ kind, isStatic })
    const { runner, group, batch, ownership, coordinator } = fixture
    const revoked = batch(0)
    const retained = batch(1)
    group.acquired.delete(`${SCHEDULED_TOPIC}|0`)
    const at = ++group.acquireClock
    group.acquired.set(`${SCHEDULED_TOPIC}|0`, { acquiredAt: at, changedAt: at })
    ownership.renew()
    expect(runner.isBatchStale(revoked)).toBe(true)
    expect(runner.isBatchStale(retained)).toBe(false)
    const commit = (partition, batch) =>
      handlerContext.run(batch, () =>
        runner.commitOffsets({
          topics: [{ topic: SCHEDULED_TOPIC, partitions: [{ partition, offset: '9' }] }],
        })
      )
    expectRefusal(await refusalOf(commit(0, revoked)), undefined, 'stale-assignment')
    await commit(1, retained)
    expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
  })

  it.each([25, 110, 82, ...(kind === 'consumer' ? [30, 42, 111] : [])])(
    'drops old code %i before verdict installation, discard or abandonment',
    async (code) => {
      fixture = ownershipFixture({ kind, isStatic })
      const { runner, group, ownership } = fixture
      const old = ownership.token()
      ownership.renew()
      const error = stampOwnership(brokerError(code), old)
      const verdict = { kind: 'lost', reason: 'lost', error }
      const membership = jest.spyOn(group, 'onMembershipError')
      const discard = jest.spyOn(group, 'discardUncommittedOffsets')
      const rejoin = jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()
      const abandon = jest.spyOn(runner.rebalanceCoordinator, 'abandon')
      expect(runner.rebalanceCoordinator.classify(error, REBALANCE_PATHS.HEARTBEAT)).toBeNull()
      runner.rebalanceCoordinator.requestRejoin(verdict)
      await runner.handleRebalanceError(verdict)
      await runner.resolveRebalanceVerdict(verdict)
      expect(membership).not.toHaveBeenCalled()
      expect(discard).not.toHaveBeenCalled()
      expect(rejoin).not.toHaveBeenCalled()
      expect(abandon).not.toHaveBeenCalled()
      expect(runner.rebalanceCoordinator.verdict).toBeNull()
      expect(ownership.refusal(ownership.token())).toBeNull()
    }
  )

  it('invalidates at poll-expiry entry while LeaveGroup is still held', async () => {
    fixture = ownershipFixture({ kind, isStatic })
    const { runner, ownership, group } = fixture
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'LeaveGroup' })
    group.leave = () => schedule.run({ api: 'LeaveGroup' }, () => {})
    group.leaveForPollExpiry = group.leave
    const expired = runner.rebalanceCoordinator.onPollExpired({
      elapsedMs: 300001,
      isStatic: false,
    })
    await gate.arrived()
    expect(ownership.refusal(ownership.token())).toMatchObject({ reason: 'poll-interval-exceeded' })
    const commit = refusalOf(runner.commitOffsets({ topics: [] }))
    expect((await commit).reason).toBe('poll-interval-exceeded')
    gate.release()
    await expired
  })
})

describe('obsolete consumer-protocol heartbeat answers', () => {
  it.each([25, 110, 82, 30, 42, 111, 112, 128])(
    'code %i leaves replacement state and target intact',
    (code) => {
      const ownership = new GroupOwnership()
      const protocol = new ConsumerGroupProtocol({
        cluster: {},
        groupId: 'group',
        topics: ['topic'],
        ownership,
        rebalanceTimeout: 1000,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: newLogger(),
      })
      const old = ownership.token()
      ownership.renew()
      protocol.state = 'STABLE'
      protocol.memberEpoch = 8
      protocol.target = { localEpoch: 8, byTopicId: new Map([['id', [0]]]) }
      const target = protocol.target
      const error = stampOwnership(brokerError(code), old)
      expect(protocol.classifyHeartbeatError(error)).toBe(error)
      protocol.onMembershipError(error)
      expect(protocol.state).toBe('STABLE')
      expect(protocol.target).toBe(target)
      expect(protocol.memberEpoch).toBe(8)
      expect(protocol.fatalError).toBeNull()
      expect(ownership.refusal(ownership.token())).toBeNull()
    }
  )

  it('does not replace a request stamp while the error crosses another sender', () => {
    const error = stampOwnership(new Error('request'), 1)
    stampOwnership(error, 2)
    expect(sentOwnership(error)).toBe(1)
    expect(sentOwnership(null)).toBeNull()
    expect(obsoleteOwnership(null, error)).toBe(false)
    expect(obsoleteOwnership(new GroupOwnership(), error)).toBe(true)
    expect(obsoleteOwnership(new GroupOwnership(), new Error('local'))).toBe(false)
  })

  it('validates the path before classifying a current operation', () => {
    expect(() =>
      learnOwnershipLoss(new GroupOwnership(), brokerError(25), 'typo', 'classic')
    ).toThrow('Unknown rebalance path')
  })
})

describe.each([false, true])('public classic commits with static membership %s', (isStatic) => {
  let publicConsumer, cluster, runner, start, finishHandler, handlerAction
  beforeAll(() => prepareRecordBatches())
  afterEach(async () => {
    finishHandler?.()
    try {
      await publicConsumer?.disconnect()
      cluster?.assertClean()
    } finally {
      start?.mockRestore()
      publicConsumer = cluster = runner = start = finishHandler = handlerAction = undefined
    }
  })

  /**
   * Starts a public consumer on the strict wire seam, then holds its first user handler. The
   * original Runner.start runs; the spy supplies the handle needed to deliver a real heartbeat.
   * @param {'eachMessage'|'eachBatch'} api the public handler API
   * @returns {Promise<void>}
   */
  const member = async (api) => {
    cluster = groupScript(createScriptedCluster({ nodes: NODES }), { coordinator: 1 })
    start = jest.spyOn(Runner.prototype, 'start')
    publicConsumer = new Kafka(cluster.kafkaOptions({ requestTimeout: 20000 })).consumer({
      groupId: 'wire-loss-group',
      groupInstanceId: isStatic ? 'wire-instance' : undefined,
      heartbeatInterval: 1000,
      maxWaitTimeInMs: 10,
    })
    let notify
    const entered = new Promise((resolve) => {
      notify = resolve
    })
    const held = new Promise((done) => {
      finishHandler = done
    })
    await publicConsumer.connect()
    await publicConsumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await publicConsumer.run({
      autoCommit: false,
      [api]: async () => {
        notify()
        await held
        await handlerAction?.()
      },
    })
    await within(entered, 5000, 'the public handler')
    runner = start.mock.instances[0]
    expect(runner).toBeInstanceOf(Runner)
    expect(cluster.requests().some(({ api }) => api === 'Fetch')).toBe(true)
  }

  /** @returns {Promise<Error>} the exact broker heartbeat error, learned by the runner */
  const lose = async () => {
    runner.consumerGroup.groupMembership.lastRequest = 0
    cluster.schedule.once({ api: 'Heartbeat' }, { errorCode: 25 })
    let loss
    try {
      await runner.heartbeat()
    } catch (error) {
      loss = error
    }
    expect(loss).toBeInstanceOf(KafkaJSProtocolError)
    expect(loss.code).toBe(25)
    expect(
      runner.consumerGroup.ownership.refusal(runner.consumerGroup.ownership.token()).cause
    ).toBe(loss)
    return loss
  }

  it.each(['eachMessage', 'eachBatch'])(
    'refuses detached public commits after a real loss during %s',
    async (api) => {
      await member(api)
      const cause = await lose()
      const before = cluster.requests().filter(({ api }) => api === 'OffsetCommit').length
      const lastCommit = runner.consumerGroup.offsetManager.lastCommit
      const error = await refusalOf(
        publicConsumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }])
      )
      expectRefusal(error, cause)
      expect(cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toHaveLength(before)
      expect(runner.consumerGroup.offsetManager.lastCommit).toBe(lastCommit)
      cluster.assertClean()
    }
  )

  it.each(['eachMessage', 'eachBatch'])(
    'refuses the original handler after replacement membership is valid on %s',
    async (api) => {
      await member(api)
      let outcome
      handlerAction = async () => {
        outcome = await refusalOf(
          publicConsumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '9' }])
        )
      }
      runner.consumerGroup.assignmentEpoch++
      runner.consumerGroup.ownership.renew()
      finishHandler()
      await until(() => outcome !== undefined, 'the original handler commit settling')
      expectRefusal(outcome, undefined, 'stale-assignment')
      expect(cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
      cluster.assertClean()
    }
  )

  it('learns its own code 69 wire rejection before refusing later public commits', async () => {
    await member('eachMessage')
    const rejoin = jest.spyOn(runner.rebalanceCoordinator, 'requestRejoin')
    try {
      cluster.schedule.once(
        { api: 'OffsetCommit' },
        { topics: [{ topic: TOPIC, partitions: [{ partition: 0, errorCode: 69 }] }] }
      )
      const error = await refusalOf(
        publicConsumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '7' }])
      )
      expectRefusal(error, error.cause)
      expect(error.code).toBe(69)
      expect(isLostMember(error)).toBe(false)
      expect(rejoin).toHaveBeenCalledWith(expect.objectContaining({ kind: 'lost' }))
      const later = await refusalOf(
        publicConsumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '8' }])
      )
      expectRefusal(later, error.cause)
      expect(cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toHaveLength(1)
      cluster.assertClean()
    } finally {
      rejoin.mockRestore()
    }
  })

  it.each(['accepted', 'rejected'])(
    'fences the late %s wire answer without cache publication',
    async (answer) => {
      await member('eachMessage')
      const held = cluster.schedule.hold({ api: 'OffsetCommit' })
      const manager = runner.consumerGroup.offsetManager
      const lastCommit = manager.lastCommit
      const cached = manager.committedOffsets()[TOPIC][0]
      const committing = refusalOf(
        publicConsumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '7' }])
      )
      await within(held.arrived(), 5000, 'the transmitted commit')
      const cause = await lose()
      if (answer === 'accepted') held.release()
      else
        held.release({ topics: [{ topic: TOPIC, partitions: [{ partition: 0, errorCode: 16 }] }] })
      expectRefusal(await within(committing, 5000, 'the fenced wire completion'), cause)
      expect(manager.committedOffsets()[TOPIC][0]).toBe(cached)
      expect(manager.lastCommit).toBe(lastCommit)
      expect(cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toHaveLength(1)
      expect(manager.operations.size()).toBe(0)
      cluster.assertClean()
    }
  )
})

describe('request identities held across heartbeat answers', () => {
  it.each([25, 110, 82, 30, 42, 111])(
    'an obsolete consumer heartbeat code %i changes no replacement state',
    async (code) => {
      const ownership = new GroupOwnership()
      const protocol = new ConsumerGroupProtocol({
        cluster: {},
        groupId: 'group',
        topics: ['topic'],
        ownership,
        rebalanceTimeout: 1000,
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: newLogger(),
      })
      protocol.state = 'ACKNOWLEDGING'
      protocol.memberEpoch = 8
      const schedule = new Schedule()
      const gate = schedule.hold({ api: 'ConsumerGroupHeartbeat' })
      protocol.coordinator = {
        consumerGroupHeartbeat: () => schedule.run({ api: 'ConsumerGroupHeartbeat' }, () => ({})),
      }
      const token = ownership.token()
      const request = protocol.sendHeartbeat({}).then(
        () => null,
        (error) => error
      )
      await gate.arrived()
      ownership.renew()
      const target = { localEpoch: 9, byTopicId: new Map() }
      protocol.target = target
      protocol.memberEpoch = 9
      const reset = jest.spyOn(protocol.sentFields, 'reset')
      const error = brokerError(code)
      gate.fail(error)
      expect(await request).toBe(error)
      expect(sentOwnership(error)).toBe(token)
      expect(reset).not.toHaveBeenCalled()
      expect(protocol.state).toBe('ACKNOWLEDGING')
      expect(protocol.target).toBe(target)
      expect(protocol.memberEpoch).toBe(9)
      expect(protocol.fatalError).toBeNull()
      expect(ownership.refusal(ownership.token())).toBeNull()
    }
  )

  it('drops an obsolete successful consumer heartbeat before installing fields or acknowledging', async () => {
    const ownership = new GroupOwnership()
    const protocol = new ConsumerGroupProtocol({
      cluster: {},
      groupId: 'group',
      topics: ['topic'],
      ownership,
      rebalanceTimeout: 1000,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
    protocol.state = 'ACKNOWLEDGING'
    protocol.memberEpoch = 8
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'ConsumerGroupHeartbeat' })
    protocol.coordinator = {
      consumerGroupHeartbeat: () => schedule.run({ api: 'ConsumerGroupHeartbeat' }, () => ({})),
    }
    const request = protocol.sendHeartbeat({})
    await gate.arrived()
    ownership.renew()
    const target = { localEpoch: 9, byTopicId: new Map() }
    protocol.target = target
    protocol.memberEpoch = 9
    const remember = jest.spyOn(protocol.sentFields, 'remember')
    gate.release({ memberEpoch: 8, heartbeatIntervalMs: 1, assignment: null })
    await request
    expect(remember).not.toHaveBeenCalled()
    expect(protocol.state).toBe('ACKNOWLEDGING')
    expect(protocol.memberEpoch).toBe(9)
    expect(protocol.target).toBe(target)
  })
})

describe('completed join ownership', () => {
  it.each([null, 25, 128])('keeps the pre-release apply boundary sound for %s', async (code) => {
    const fixture = ownershipFixture({ kind: 'consumer' })
    const { group, runner, ownership, manager, schedule, operations } = fixture
    group.partitionsPerSubscribedTopic = new Map([[SCHEDULED_TOPIC, [0, 1]]])
    group.groupMembership.setOwned([{ topic: SCHEDULED_TOPIC, partitions: [0, 1] }])
    group.groupMembership.coordinator = fixture.coordinator
    group.groupMembership.generationId = 1
    group.groupMembership.memberId = 'member-a'
    manager.resolveOffset({ topic: SCHEDULED_TOPIC, partition: 0, offset: '4' })
    const token = ownership.token()
    const gate = releasableOnce(schedule.hold({ api: 'OffsetCommit' }))
    const published = []
    const notify = group.subscriptionState.onAssignmentChange
    group.subscriptionState.onAssignmentChange = () => {
      published.push(ownership.token())
      notify()
    }
    const round = group.beginRebalanceRound()
    const applying = round.wait(() =>
      group.applyRebalanceResult(
        { assignment: { [SCHEDULED_TOPIC]: [1] }, userData: null },
        { rebuildOffsets: false, round }
      )
    )
    const settled = applying.then(
      (result) => result,
      (error) => error
    )
    try {
      await within(gate.arrived(), 2000, 'the real pre-release commit')
      expect(ownership.token()).toBe(token)
      let error
      if (code != null) {
        error = stampOwnership(brokerError(code), token)
        const verdict = runner.rebalanceCoordinator.classify(error, REBALANCE_PATHS.MEMBERSHIP)
        expect(verdict.kind).toBe('lost')
        runner.rebalanceCoordinator.requestRejoin(verdict)
      }
      gate.release()
      const result = await within(settled, 2000, 'the held apply to settle')
      await until(() => operations.size() === 0, 'pre-release operation cleanup')
      if (code == null) {
        expect(result).not.toBeInstanceOf(Error)
        expect(published).toEqual([token + 1])
        expect(ownership.token()).toBe(token + 1)
      } else {
        expect(result).toBe(error)
        expect(published).toEqual([])
        expect(ownership.token()).toBe(token)
        expect(group.assigned()).toEqual([{ topic: SCHEDULED_TOPIC, partitions: [0, 1] }])
        expect(runner.rebalanceCoordinator.takeVerdict().error).toBe(error)
      }
    } finally {
      gate.release()
      round.retire()
      await settled
      group.subscriptionState.onAssignmentChange = notify
      await fixture.close()
    }
  })

  it('renews ownership before publishing the applied assignment', async () => {
    const fixture = ownershipFixture()
    const { runner, group, ownership } = fixture
    const token = ownership.token()
    ownership.lose(brokerError(25), token)
    group.partitionsPerSubscribedTopic = new Map([[SCHEDULED_TOPIC, [0, 1]]])
    group.joinAndSync = async () => ({
      assignment: { [SCHEDULED_TOPIC]: [0, 1] },
      userData: null,
    })
    const published = []
    const checkPublication = () => {
      published.push({
        token: ownership.token(),
        refusal: ownership.refusal(ownership.token()),
      })
    }
    const notify = group.subscriptionState.onAssignmentChange
    group.subscriptionState.onAssignmentChange = () => {
      checkPublication()
      notify()
    }
    const assignment = jest
      .spyOn(group.groupMembership, 'notifyAssignment')
      .mockImplementation(checkPublication)
    const renew = jest.spyOn(ownership, 'renew')
    const stop = jest.spyOn(runner.heartbeatManager, 'ensureStarted').mockImplementation(() => {})
    try {
      await runner.rebalanceCoordinator.runRound(null, 'initial')
      expect(published).toEqual([
        { token: token + 1, refusal: null },
        { token: token + 1, refusal: null },
      ])
      expect(renew).toHaveBeenCalledTimes(1)
      expect(ownership.token()).toBe(token + 1)
      expect(ownership.refusal(ownership.token())).toBeNull()
    } finally {
      group.subscriptionState.onAssignmentChange = notify
      assignment.mockRestore()
      renew.mockRestore()
      stop.mockRestore()
      await fixture.close()
    }
  })
})

describe.each(['classic', 'consumer'])('handler commit forms on %s', (kind) => {
  it('skips an implicit eachBatch commit and rejects its explicit form with the original cause', async () => {
    const cause = brokerError(25)
    const fixture = ownershipFixture({
      kind,
      eachBatch: async ({ commitOffsetsIfNecessary }) => {
        fixture.ownership.lose(cause, fixture.ownership.token())
        await expect(commitOffsetsIfNecessary()).resolves.toBeUndefined()
        expectRefusal(
          await refusalOf(
            commitOffsetsIfNecessary({
              topics: [
                {
                  topic: SCHEDULED_TOPIC,
                  partitions: [{ partition: 0, offset: '4' }],
                },
              ],
            })
          ),
          cause
        )
      },
    })
    try {
      const batch = fixture.batch()
      await handlerContext.run(batch, () => fixture.runner.processEachBatch(batch))
      expect(fixture.coordinator.offsetCommit).not.toHaveBeenCalled()
    } finally {
      await fixture.close()
    }
  })

  it.each(['eachMessage', 'eachBatch'])(
    'preserves the exact heartbeat loss thrown through %s',
    async (api) => {
      const cause = brokerError(25)
      const fixture = ownershipFixture({ kind, [api]: async ({ heartbeat }) => heartbeat() })
      fixture.group.heartbeat.mockRejectedValue(cause)
      try {
        const batch = fixture.batch()
        await expect(
          handlerContext.run(batch, () =>
            api === 'eachMessage'
              ? fixture.runner.processEachMessage(batch)
              : fixture.runner.processEachBatch(batch)
          )
        ).rejects.toBe(cause)
        expect(fixture.coordinator.offsetCommit).not.toHaveBeenCalled()
        expectRefusal(fixture.ownership.refusal(fixture.ownership.token()), cause)
      } finally {
        await fixture.close()
      }
    }
  )
})

describe('a current consumer-protocol loss learned by a commit', () => {
  it.each([22, 69])('code %i fences the current state so its next round really rejoins', (code) => {
    const ownership = new GroupOwnership()
    const protocol = new ConsumerGroupProtocol({
      cluster: {},
      groupId: 'group',
      topics: ['topic'],
      ownership,
      rebalanceTimeout: 1000,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
    protocol.state = 'STABLE'
    protocol.memberEpoch = 8
    protocol.target = { localEpoch: 8, byTopicId: new Map([['id', [0]]]) }
    const error = stampOwnership(stampOffsetCommit(brokerError(code)), ownership.token())
    protocol.onMembershipError(error)
    expect(protocol.state).toBe('FENCED')
    expect(protocol.target).toBeNull()
    expectRefusal(ownership.refusal(ownership.token()), error)
    expect(isLostMember(error)).toBe(code === 22)
  })
})

describe('a lost explicit commit still owes a rejoin', () => {
  it('notifies the run before returning the refusal to a detached caller', async () => {
    const fixture = ownershipFixture()
    const cause = brokerError(69)
    const order = []
    fixture.manager.onCommitError = (error) => {
      expect(error).toBe(cause)
      expect(sentOwnership(error)).toBe(fixture.ownership.token())
      expectRefusal(fixture.ownership.refusal(fixture.ownership.token()), cause)
      order.push('lost')
    }
    fixture.coordinator.offsetCommit.mockRejectedValueOnce(cause)
    try {
      const error = await refusalOf(
        fixture.manager.commitOffsets({
          topics: [
            {
              topic: SCHEDULED_TOPIC,
              partitions: [{ partition: 0, offset: '4' }],
            },
          ],
        })
      )
      order.push('caller')
      expectRefusal(error, cause)
      expect(order).toEqual(['lost', 'caller'])
    } finally {
      await fixture.close()
    }
  })
})

describe('current consumer heartbeat identities', () => {
  it('accepts a current successful answer and completes its acknowledgement', async () => {
    const ownership = new GroupOwnership()
    const protocol = new ConsumerGroupProtocol({
      cluster: {},
      groupId: 'group',
      topics: ['topic'],
      ownership,
      rebalanceTimeout: 1000,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
    protocol.state = 'ACKNOWLEDGING'
    protocol.memberEpoch = 8
    protocol.coordinator = {
      consumerGroupHeartbeat: async () => ({
        memberEpoch: 8,
        heartbeatIntervalMs: 1000,
        assignment: null,
      }),
    }
    await protocol.sendHeartbeat({})
    expect(protocol.state).toBe('STABLE')
    expect(protocol.memberEpoch).toBe(8)
    expect(ownership.refusal(ownership.token())).toBeNull()
  })

  it('learns a current failed answer before moving to fenced state', async () => {
    const ownership = new GroupOwnership()
    const cause = brokerError(25)
    const protocol = new ConsumerGroupProtocol({
      cluster: {},
      groupId: 'group',
      topics: ['topic'],
      ownership,
      rebalanceTimeout: 1000,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
    protocol.state = 'STABLE'
    protocol.memberEpoch = 8
    protocol.coordinator = {
      consumerGroupHeartbeat: async () => {
        throw cause
      },
    }
    await expect(protocol.sendHeartbeat({})).rejects.toBe(cause)
    expect(protocol.state).toBe('FENCED')
    expectRefusal(ownership.refusal(ownership.token()), cause)
    expect(sentOwnership(cause)).toBe(ownership.token())
  })
})

const { consumerProtocolScript } = require('../../../testHelpers/scriptedCluster')
const { MemberAssignment } = require('../assignerProtocol')
const apiKeys = require('../../protocol/requests/apiKeys')
const { TOPIC_ID } = require('./fixtures/fetchHarness')

/**
 * A public consumer whose first handler is held while real transport requests cross a gate.
 * Schedulers wrap completion only; every request and answer uses the strict seam's wire pair.
 */
class TransportMember {
  /** @param {string} kind protocol @param {boolean} isStatic static member @param {string} api handler */
  constructor(kind, isStatic, api) {
    this.kind = kind
    this.isStatic = isStatic
    this.api = api
    this.releases = []
    this.restores = []
    this.jobs = []
    this.cluster = (kind === 'consumer' ? consumerProtocolScript : groupScript)(
      createScriptedCluster({
        nodes: NODES,
        profile: kind === 'consumer' ? 'consumer' : 'sasl',
        advertise: { SaslHandshake: [1, 1], SaslAuthenticate: [2, 2] },
      }),
      { coordinator: 1, heartbeatIntervalMs: 10000, partitions: 2 }
    )
    const { RecordBatch } = require('../../protocol/recordBatch/v0')
    const Record = require('../../protocol/recordBatch/record/v0')
    this.records = RecordBatch({
      firstTimestamp: 1700000000000,
      maxTimestamp: 1700000000000,
      records: [0, 1, 2].map((offset) =>
        Record({ offsetDelta: offset, key: Buffer.from('key'), value: Buffer.from('value') })
      ),
    })
    let fetches = 0
    this.cluster.answer('Fetch', async ({ maxWaitMs }) => {
      const records = fetches++ === 0 ? Buffer.from((await this.records).buffer) : false
      if (!records) await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      return { partitions: [0, 1].map((partition) => ({ partition, errorCode: 0, records })) }
    })
    this.entered = new Promise((resolve) => {
      this.enter = resolve
    })
    this.blocked = new Promise((resolve) => {
      this.finish = resolve
    })
    this.releases.push(this.finish)
  }

  /** @returns {Promise<void>} resolves after the real initial join and first handler entry */
  async open() {
    this.context = jest.spyOn(handlerContext, 'run')
    this.restores.push(() => this.context.mockRestore())
    const spy = jest.spyOn(Runner.prototype, 'start')
    this.restores.push(() => spy.mockRestore())
    const socketFactory = (options) =>
      this.cluster.socketFactory({
        ...options,
        onConnect: () => {
          if (this.connectGate && options.host === 'node1') {
            this.connectGate().then(options.onConnect)
          } else options.onConnect()
        },
      })
    this.consumer = new Kafka(
      this.cluster.kafkaOptions({
        socketFactory,
        sasl: { mechanism: 'plain', username: 'user', password: 'password' },
        requestTimeout: 20000,
        connectionTimeout: 10000,
        retry: { initialRetryTime: 321, factor: 0, retries: 3 },
      })
    ).consumer({
      groupId: 'transport-group',
      groupProtocol: this.kind,
      ...(this.kind === 'classic'
        ? { partitionAssigners: [require('../assigners').cooperativeSticky] }
        : {}),
      groupInstanceId: this.isStatic ? 'transport-instance' : undefined,
      ...(this.kind === 'classic' ? { heartbeatInterval: 10000 } : {}),
      maxWaitTimeInMs: 10,
    })
    await this.consumer.connect()
    await this.consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    this.handlerEntries = 0
    const seen = new Set()
    await this.consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 2,
      [this.api]: async (payload) => {
        const partition = this.api === 'eachBatch' ? payload.batch.partition : payload.partition
        if (seen.has(partition)) return
        seen.add(partition)
        this.handlerEntries++
        this.enter()
        await this.blocked
        await this.handlerAction?.(payload)
      },
    })
    await within(this.entered, 5000, 'the transport handler')
    this.runner = spy.mock.instances[0]
    this.group = this.runner.consumerGroup
    this.manager = this.group.offsetManager
    this.broker = this.manager.coordinator
    this.pauseTimers()
    await until(() => !this.runner.heartbeatManager.inFlight, 'the initial heartbeat settling')
    if (this.kind === 'consumer' && this.group.groupMembership.state === 'ACKNOWLEDGING') {
      await this.group.groupMembership.sendHeartbeat({})
      this.pauseTimers()
    }
    await until(() => this.handlerEntries === 2, 'both partition handlers entering')
    expect(this.frames('Fetch').length).toBeGreaterThan(0)
    expect(this.group.assigned()).toEqual([{ topic: TOPIC, partitions: [0, 1] }])
  }

  /** @returns {void} prevents unsolicited ticks while the test drives protocol operations */
  pauseTimers() {
    this.runner.heartbeatManager.stop()
    this.runner.pollTimer.stop()
  }

  /** @param {string} api request name @returns {object[]} independently decoded requests */
  frames(api) {
    return this.cluster.requests().filter((r) => r.api === api)
  }

  /** @param {Promise<unknown>} job immediately observes a possibly rejected operation @returns {Promise<object>} its outcome */
  observe(job) {
    const observed = job.then(
      (value) => ({ value }),
      (error) => ({ error })
    )
    this.jobs.push(observed)
    return observed
  }

  /** @param {object} selector seam selector @returns {object} a once-releasable gate */
  hold(selector) {
    const gate = this.cluster.schedule.hold(selector)
    const once = releasableOnce(gate)
    this.releases.push(once.release)
    return {
      arrived: once.arrived,
      release: once.release,
      answer: (value) => {
        this.releases.splice(this.releases.indexOf(once.release), 1)
        gate.release(value)
      },
    }
  }

  /** @param {string} name completion boundary @returns {object} gate and a callable completion wrapper */
  completion(name) {
    const schedule = new Schedule()
    const once = releasableOnce(schedule.hold({ api: name }))
    this.releases.push(once.release)
    return { ...once, wait: () => schedule.run({ api: name }, () => {}) }
  }

  /** @param {object} object collaborator @param {string} method member method @param {Function} wrapper replacement @returns {void} */
  wrap(object, method, wrapper) {
    const original = object[method]
    object[method] = wrapper(original.bind(object))
    this.restores.push(() => {
      object[method] = original
    })
  }

  /** @param {string} offset next offset @returns {Promise<void>} a real public explicit commit */
  commit(offset = '7') {
    return this.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset }])
  }

  /**
   * Learns an actual wire heartbeat loss on the other real broker, leaving the gated coordinator
   * request untouched. Only the endpoint is selected here; Runner and protocol classification run.
   * @returns {Promise<Error>} the exact protocol error retained by ownership
   */
  async lose() {
    const protocol = this.group.groupMembership
    const coordinator = protocol.coordinator
    protocol.coordinator = await this.group.cluster.findBroker({ nodeId: 0 })
    protocol.lastRequest = 0
    protocol.lastRequestAt = 0
    this.cluster.schedule.once(
      { api: this.kind === 'consumer' ? 'ConsumerGroupHeartbeat' : 'Heartbeat', node: 0 },
      { errorCode: 25 }
    )
    let cause
    try {
      if (this.kind === 'consumer') await protocol.sendHeartbeat({})
      else await this.runner.heartbeat()
    } catch (error) {
      this.runner.onHeartbeatError(error)
      cause = error
    } finally {
      protocol.coordinator = coordinator
    }
    expect(cause).toBeInstanceOf(KafkaJSProtocolError)
    expect(cause.code).toBe(25)
    expect(this.group.ownership.refusal(this.group.ownership.token()).cause).toBe(cause)
    return cause
  }

  /** @returns {Promise<void>} a completed replacement round with a reader-observed join */
  async rejoin() {
    const before = this.frames(
      this.kind === 'consumer' ? 'ConsumerGroupHeartbeat' : 'JoinGroup'
    ).length
    const token = this.group.ownership.token()
    this.runner.rebalanceCoordinator.takeVerdict()
    await within(
      this.runner.rebalanceCoordinator.runRound('unknown-member-id', 'lost'),
      5000,
      'the replacement join'
    )
    this.pauseTimers()
    expect(this.group.ownership.token()).toBe(token + 1)
    expect(this.group.ownership.refusal(this.group.ownership.token())).toBeNull()
    const requests = this.frames(
      this.kind === 'consumer' ? 'ConsumerGroupHeartbeat' : 'JoinGroup'
    ).slice(before)
    expect(requests.length).toBeGreaterThan(0)
    if (this.kind === 'consumer')
      expect(requests.some(({ fields }) => fields.memberEpoch === 0)).toBe(true)
    expect(this.group.assigned()).toEqual([{ topic: TOPIC, partitions: [0, 1] }])
  }

  /** @returns {Promise<void>} releases all gates, settles operations and disconnects */
  async close() {
    this.connectGate = null
    this.releases.forEach((release) => release())
    await within(Promise.all(this.jobs), 5000, 'transport jobs settling')
    await within(this.consumer?.disconnect(), 5000, 'transport disconnect')
    this.restores.reverse().forEach((restore) => restore())
    this.cluster.assertClean()
  }
}

describe.each([
  ['classic', false, 'eachMessage'],
  ['classic', false, 'eachBatch'],
  ['classic', true, 'eachMessage'],
  ['classic', true, 'eachBatch'],
  ['consumer', false, 'eachMessage'],
  ['consumer', false, 'eachBatch'],
  ['consumer', true, 'eachMessage'],
  ['consumer', true, 'eachBatch'],
])('real ownership transport on %s static %s through %s', (kind, isStatic, api) => {
  let member
  beforeAll(() => prepareRecordBatches())
  beforeEach(async () => {
    member = new TransportMember(kind, isStatic, api)
    await member.open()
  })
  afterEach(async () => {
    await member.close()
  })

  it.each([
    'connect',
    'authentication',
    'encoder',
    'pending',
    'lookup',
    'retry',
    'accepted',
    'rejected',
  ])(
    'fences a commit held at %s and permits fresh work after a completed wire join',
    async (boundary) => {
      const { manager, broker, cluster, group } = member
      let attempts = 0
      member.wrap(
        member.runner,
        'commitRetrier',
        (retry) => (work) =>
          retry((...args) => {
            attempts++
            return work(...args)
          })
      )
      const lastCommit = manager.lastCommit
      let cached = manager.committedOffsets()[TOPIC][0]
      const before = member.frames('OffsetCommit').length
      let gate
      let blocker
      if (boundary === 'connect' || boundary === 'lookup') {
        await broker.disconnect({ drainTimeout: 0 })
        if (boundary === 'connect') {
          gate = member.completion('connect')
          member.connectGate = gate.wait
        } else gate = member.hold({ api: 'FindCoordinator' })
      } else if (boundary === 'authentication') {
        gate = member.hold({ api: 'SaslAuthenticate', node: 1 })
        broker.connectionPool.pool[0].authenticatedAt = null
      } else if (boundary === 'encoder') {
        gate = member.completion('encoder')
        member.wrap(broker, 'lookupRequestWithMax', (lookup) => (...args) => {
          const builder = lookup(...args)
          if (args[0] !== apiKeys.OffsetCommit) return builder
          return (...params) => {
            const built = builder(...params)
            const encode = built.request.encode
            built.request.encode = async (...input) => {
              await gate.wait()
              return encode(...input)
            }
            return built
          }
        })
      } else if (boundary === 'pending') {
        const connection = broker.connectionPool.pool[0]
        connection.requestQueue.maxInFlightRequests = 1
        gate = member.hold({ api: 'Metadata', node: 1 })
        blocker = member.observe(broker.metadata([TOPIC]))
        await within(gate.arrived(), 5000, 'the blocking request')
      } else if (boundary === 'retry') {
        gate = member.completion('retry')
        const native = global.setTimeout
        const timers = []
        const spy = jest
          .spyOn(global, 'setTimeout')
          .mockImplementation((callback, delay, ...args) => {
            if (delay !== 321) return native(callback, delay, ...args)
            const timer = native(() => {}, 20000)
            timers.push(timer)
            gate.wait().then(() => {
              clearTimeout(timer)
              callback(...args)
            })
            return timer
          })
        member.restores.push(() => {
          timers.forEach(clearTimeout)
          spy.mockRestore()
        })
        cluster.schedule.once(
          { api: 'OffsetCommit' },
          {
            topics: [
              {
                ...(kind === 'consumer' ? { topicId: TOPIC_ID } : { topic: TOPIC }),
                partitions: [{ partition: 0, errorCode: 14 }],
              },
            ],
          }
        )
      } else gate = member.hold({ api: 'OffsetCommit' })
      const operation = member.observe(member.commit())
      if (boundary === 'pending') {
        await until(
          () =>
            broker.connectionPool.pool[0].requestQueue.pending.some(
              (r) => r.entry.apiKey === apiKeys.OffsetCommit
            ),
          'the queued commit'
        )
      } else await within(gate.arrived(), 5000, `the ${boundary} completion`)
      const cause = await member.lose()
      const countAtLoss = member.frames('OffsetCommit').length
      const socketsAtLoss = cluster.sockets().filter((s) => !s.destroyed)
      expect(countAtLoss - before).toBe(
        ['accepted', 'rejected', 'retry'].includes(boundary) ? 1 : 0
      )
      if (boundary === 'retry') {
        await member.rejoin()
        cached = manager.committedOffsets()[TOPIC][0]
      }
      if (boundary === 'rejected')
        gate.answer({
          topics: [
            {
              ...(kind === 'consumer' ? { topicId: TOPIC_ID } : { topic: TOPIC }),
              partitions: [{ partition: 0, errorCode: 16 }],
            },
          ],
        })
      else gate.release()
      member.connectGate = null
      expectRefusal((await within(operation, 5000, 'the refused transport commit')).error, cause)
      expect(attempts).toBe(boundary === 'retry' ? 2 : 1)
      if (blocker) expect((await blocker).error).toBeUndefined()
      expect(member.frames('OffsetCommit')).toHaveLength(countAtLoss)
      expect(manager.committedOffsets()[TOPIC][0]).toBe(cached)
      expect(manager.lastCommit).toBe(lastCommit)
      expect(manager.operations.size()).toBe(0)
      for (const connection of broker.connectionPool.pool) {
        await until(
          () => connection.requestQueue.inflight.size === 0,
          'the coordinator requests settling'
        )
        expect(connection.requestQueue.pending).toHaveLength(0)
      }
      expect(socketsAtLoss.every((s) => !s.destroyed)).toBe(true)
      cluster.assertClean()
      if (boundary !== 'retry') await member.rejoin()
      await member.commit('8')
      expect(member.frames('OffsetCommit')).toHaveLength(countAtLoss + 1)
      expect(group.offsetManager.committedOffsets()[TOPIC][0].toString()).toBe('8')
    }
  )

  it('invalidates before a held poll-expiry leave and refuses the old handler after rejoin', async () => {
    const { group, runner } = member
    const leavingApi = kind === 'consumer' ? 'ConsumerGroupHeartbeat' : 'LeaveGroup'
    const gate = kind === 'classic' && isStatic ? null : member.hold({ api: leavingApi, node: 1 })
    const leaving = member.observe(
      runner.rebalanceCoordinator.onPollExpired({ elapsedMs: 300001, isStatic })
    )
    if (gate) await within(gate.arrived(), 5000, 'the poll-expiry leave')
    const refusal = group.ownership.refusal(group.ownership.token())
    expect(refusal).toBeInstanceOf(KafkaJSCommitFailedError)
    expect(refusal.reason).toBe('poll-interval-exceeded')
    const before = member.frames('OffsetCommit').length
    const lastCommit = group.offsetManager.lastCommit
    const cached = group.offsetManager.committedOffsets()[TOPIC][0]
    const sockets = member.cluster.sockets().filter((s) => !s.destroyed)
    expectRefusal(
      (await member.observe(member.commit())).error,
      refusal.cause,
      'poll-interval-exceeded'
    )
    expect(group.offsetManager.lastCommit).toBe(lastCommit)
    expect(group.offsetManager.committedOffsets()[TOPIC][0]).toBe(cached)
    expect(group.offsetManager.operations.size()).toBe(0)
    for (const connection of member.broker.connectionPool.pool) {
      expect(
        [...connection.requestQueue.inflight.values()].filter(
          (request) => request.entry.apiKey === apiKeys.OffsetCommit
        )
      ).toHaveLength(0)
      expect(
        connection.requestQueue.pending.filter(
          (request) => request.entry.apiKey === apiKeys.OffsetCommit
        )
      ).toHaveLength(0)
    }
    if (gate) gate.release()
    expect((await leaving).error).toBeUndefined()
    await member.rejoin()
    await member.commit('8')
    expect(member.frames('OffsetCommit')).toHaveLength(before + 1)
    let old
    member.handlerAction = async () => {
      old = await member.observe(member.commit('9'))
    }
    member.finish()
    await until(() => old !== undefined, 'the obsolete handler refusal')
    expectRefusal(old.error, undefined, 'stale-assignment')
    expect(member.frames('OffsetCommit')).toHaveLength(before + 1)
    expect(sockets.every((s) => !s.destroyed)).toBe(true)
  })

  it('enters context once per handler batch and runs the approved transport guard sites', async () => {
    const { broker, runner } = member
    let checks = 0
    const lookup = jest.spyOn(member.manager, 'getCoordinator')
    member.restores.push(() => lookup.mockRestore())
    member.wrap(
      broker,
      'offsetCommit',
      (send) => (payload, options) =>
        send(payload, {
          guard: () => {
            checks++
            return options.guard()
          },
        })
    )
    const before = member.frames('OffsetCommit').length
    await member.commit()
    expect(checks).toBe(4)
    expect(lookup).toHaveBeenCalledTimes(1)
    expect(checks + lookup.mock.calls.length).toBe(5)
    expect(member.frames('OffsetCommit')).toHaveLength(before + 1)
    expect(runner.inFlightHandlers).toBeGreaterThan(0)
    member.finish()
    await until(() => runner.inFlightHandlers === 0, 'all records completing in both batches')
    expect(member.context.mock.calls).toHaveLength(2)
    expect(member.context.mock.calls.map(([store]) => store.partition).sort()).toEqual([0, 1])
    expect(member.context.mock.calls.every(([store]) => !('messages' in store))).toBe(true)
  })
  it.each(kind === 'consumer' ? [null, 25, 110, 30, 42, 111, 112, 128] : [null, 25, 110, 82])(
    'ignores the old heartbeat answer %s after an actual replacement join',
    async (code) => {
      const { group, runner } = member
      const protocol = group.groupMembership
      const gate = member.hold({
        api: kind === 'consumer' ? 'ConsumerGroupHeartbeat' : 'Heartbeat',
        node: 1,
      })
      protocol.lastRequest = 0
      protocol.lastRequestAt = 0
      const token = group.ownership.token()
      const old = member.observe(
        kind === 'consumer' ? protocol.sendHeartbeat({}) : protocol.heartbeat({ interval: 0 })
      )
      await within(gate.arrived(), 5000, 'the old heartbeat')
      await runner.rebalanceCoordinator.onPollExpired({ elapsedMs: 300001, isStatic })
      await member.rejoin()
      if (kind === 'consumer') {
        await within(protocol.heartbeat({ interval: 0 }), 5000, 'the replacement heartbeat')
        await until(() => protocol.state === 'STABLE', 'the replacement acknowledgement')
        member.pauseTimers()
      }
      const target = protocol.target
      const state = protocol.state
      const fatal = protocol.fatalError
      const assignment = group.assigned()
      const fetchers = runner.fetchManager.getFetchers()
      expect(fetchers.length).toBeGreaterThan(0)
      const starts = jest.spyOn(runner.fetchManager, 'start')
      const stops = jest.spyOn(runner.fetchManager, 'stop')
      const fetcherStops = fetchers.map((fetcher) => jest.spyOn(fetcher, 'stop'))
      const slot = runner.rebalanceCoordinator.verdict
      const discard = jest.spyOn(group, 'discardUncommittedOffsets')
      const abandon = jest.spyOn(group.ownership, 'abandon')
      const round = jest.spyOn(runner.rebalanceCoordinator, 'runRound')
      member.restores.push(() => {
        discard.mockRestore()
        abandon.mockRestore()
        round.mockRestore()
        starts.mockRestore()
        stops.mockRestore()
        fetcherStops.forEach((stop) => stop.mockRestore())
      })
      if (code === null) gate.release()
      else gate.answer({ errorCode: code })
      const outcome = await within(old, 5000, 'the obsolete heartbeat completion')
      if (code !== null) {
        expect(outcome.error).toBeInstanceOf(KafkaJSProtocolError)
        expect(outcome.error.code).toBe(code)
        expect(sentOwnership(outcome.error)).toBe(token)
        runner.onHeartbeatError(outcome.error)
        group.onMembershipError(outcome.error)
        await runner.handleRebalanceError(outcome.error)
      } else expect(outcome.error).toBeUndefined()
      expect(protocol.target).toBe(target)
      expect(protocol.state).toBe(state)
      expect(protocol.fatalError).toBe(fatal)
      expect(group.assigned()).toEqual(assignment)
      // Poll expiry already stopped the manager; an idle fetcher may finish draining while
      // the old heartbeat settles. Its answer must cause no further lifecycle changes.
      const remaining = runner.fetchManager.getFetchers()
      expect(remaining.length).toBeGreaterThan(0)
      remaining.forEach((fetcher) => expect(fetchers).toContain(fetcher))
      expect(starts).not.toHaveBeenCalled()
      expect(stops).not.toHaveBeenCalled()
      fetcherStops.forEach((stop) => expect(stop).not.toHaveBeenCalled())
      expect(runner.rebalanceCoordinator.verdict).toBe(slot)
      expect(discard).not.toHaveBeenCalled()
      expect(abandon).not.toHaveBeenCalled()
      expect(round).not.toHaveBeenCalled()
      await member.commit('8')
      expect(group.offsetManager.committedOffsets()[TOPIC][0].toString()).toBe('8')
    }
  )

  it.each(kind === 'consumer' ? [69, 30] : [69])(
    'learns its own wire commit loss %i before refusing a later operation',
    async (code) => {
      const gate = member.hold({ api: 'OffsetCommit' })
      const operation = member.observe(member.commit())
      await within(gate.arrived(), 5000, 'the commit loss')
      gate.answer({
        topics: [
          {
            ...(kind === 'consumer' ? { topicId: TOPIC_ID } : { topic: TOPIC }),
            partitions: [{ partition: 0, errorCode: code }],
          },
        ],
      })
      const first = (await operation).error
      expectRefusal(first, first.cause)
      expect(first.code).toBe(code)
      expect(isLostMember(first)).toBe(false)
      const count = member.frames('OffsetCommit').length
      expectRefusal((await member.observe(member.commit('8'))).error, first.cause)
      expect(member.frames('OffsetCommit')).toHaveLength(count)
      expect(member.manager.operations.size()).toBe(0)
      if (code === 30) {
        const token = member.group.ownership.token()
        const verdict = member.runner.rebalanceCoordinator.takeVerdict()
        const abandoned = await member.observe(member.runner.resolveRebalanceVerdict(verdict))
        expect(abandoned.error).toBe(first.cause)
        expect(member.group.ownership.renew()).toBe(token)
        expectRefusal(member.group.ownership.refusal(token), first.cause)
      }
    }
  )

  it.each(['seek', 'setDefaultOffset'])(
    'learns a held %s wire loss in the sender before the fetch loop observes it',
    async (method) => {
      const gate = member.hold({ api: 'OffsetCommit' })
      const last = member.manager.lastCommit
      const cached = member.manager.committedOffsets()[TOPIC][0]
      member.manager.autoCommit = true
      const operation = member.observe(
        member.manager[method]({ topic: TOPIC, partition: 0, offset: '4' })
      )
      await within(gate.arrived(), 5000, 'the position commit')
      gate.answer({
        topics: [
          {
            ...(kind === 'consumer' ? { topicId: TOPIC_ID } : { topic: TOPIC }),
            partitions: [{ partition: 0, errorCode: 69 }],
          },
        ],
      })
      const outcome = await operation
      expect(outcome.error).toBeUndefined()
      expect(outcome.value).toBe(method === 'seek' ? false : undefined)
      const loss = member.group.ownership.refusal(member.group.ownership.token())
      expect(loss).toBeInstanceOf(KafkaJSCommitFailedError)
      expect(loss.cause.code).toBe(69)
      const count = member.frames('OffsetCommit').length
      expectRefusal((await member.observe(member.commit('8'))).error, loss.cause)
      expect(member.frames('OffsetCommit')).toHaveLength(count)
      expect(member.manager.operations.size()).toBe(0)
      expect(member.manager.lastCommit).toBe(last)
      expect(member.manager.committedOffsets()[TOPIC][0]).toBe(cached)
    }
  )

  it('refuses reacquired batch work while a retained partition still commits', async () => {
    const { group, runner } = member
    const retained = group.acquired.get(`${TOPIC}|1`)
    const epoch = group.assignmentEpoch
    const apply = async (partitions) => {
      if (kind === 'consumer') {
        member.cluster.schedule.once(
          { api: 'ConsumerGroupHeartbeat', node: 1 },
          {
            memberEpoch: group.generationId + 1,
            heartbeatIntervalMs: 10000,
            assignment: { topicPartitions: [{ topicId: TOPIC_ID, partitions }] },
          }
        )
        await group.groupMembership.sendHeartbeat({})
      } else {
        member.cluster.answer('SyncGroup', () => ({
          memberAssignment: MemberAssignment.encode({
            version: 1,
            assignment: { [TOPIC]: partitions },
          }).toString('hex'),
        }))
      }
      await runner.rebalanceCoordinator.runRound('assignment-changed', 'revoked')
      member.pauseTimers()
      runner.rebalanceCoordinator.takeVerdict()
    }
    await apply([1])
    expect(group.assigned()).toEqual([{ topic: TOPIC, partitions: [1] }])
    await apply([0, 1])
    expect(group.assignmentEpoch).toBe(epoch)
    expect(group.acquired.get(`${TOPIC}|1`)).toBe(retained)
    const before = member.frames('OffsetCommit').length
    const results = new Map()
    member.handlerAction = async (payload) => {
      const partition = api === 'eachBatch' ? payload.batch.partition : payload.partition
      results.set(
        partition,
        await member.observe(
          member.consumer.commitOffsets([{ topic: TOPIC, partition, offset: '8' }])
        )
      )
    }
    member.finish()
    await until(() => results.size === 2, 'both original handler commits settling')
    expectRefusal(results.get(0).error, undefined, 'stale-assignment')
    expect(results.get(1).error).toBeUndefined()
    expect(member.frames('OffsetCommit')).toHaveLength(before + 1)
    expect(
      member
        .frames('OffsetCommit')
        .at(-1)
        .fields.topics[0].partitions.map(({ partition }) => partition)
    ).toEqual([1])
  })
})

describe('real ownership transport on consumer for a heartbeat coordinator lookup', () => {
  let member
  beforeAll(() => prepareRecordBatches())
  beforeEach(async () => {
    member = new TransportMember('consumer', false, 'eachMessage')
    await member.open()
  })
  afterEach(async () => {
    await member.close()
  })

  it('sends no heartbeat for the replaced membership when the lookup succeeds late', async () => {
    const { group, runner } = member
    const protocol = group.groupMembership
    const gate = member.hold({ api: 'FindCoordinator' })
    protocol.coordinator = null
    const token = group.ownership.token()
    const late = member.observe(protocol.sendHeartbeat({}))
    await within(gate.arrived(), 5000, 'the old lookup')
    await protocol.connect()
    await within(
      runner.rebalanceCoordinator.runRound('assignment-changed', 'revoked'),
      5000,
      'the cooperative replacement'
    )
    member.pauseTimers()
    expect(group.ownership.token()).toBe(token + 1)
    const frames = member.frames('ConsumerGroupHeartbeat').length
    const { memberEpoch, state } = protocol
    const backoff = jest.spyOn(protocol.backoff, 'onFailedAttempt')
    gate.release()
    expect((await within(late, 5000, 'the late lookup')).error).toBeUndefined()
    expect(member.frames('ConsumerGroupHeartbeat')).toHaveLength(frames)
    expect(protocol.memberEpoch).toBe(memberEpoch)
    expect(protocol.state).toBe(state)
    expect(backoff).not.toHaveBeenCalled()
    expect(group.ownership.refusal(group.ownership.token())).toBeNull()
    const commits = member.frames('OffsetCommit').length
    expect((await member.observe(member.commit('7'))).error).toBeUndefined()
    expect(member.frames('OffsetCommit')).toHaveLength(commits + 1)
  })
})

describe('a heartbeat coordinator lookup held across a replacement', () => {
  /**
   * A member with no known coordinator whose heartbeat waits on the lookup, which the membership
   * then outlives.
   * @returns {{ protocol: ConsumerGroupProtocol, ownership: GroupOwnership, gate: object,
   *   token: number, heartbeat: Promise<Error|null>, renew: () => void }}
   */
  const heldLookup = () => {
    const ownership = new GroupOwnership()
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'FindCoordinator' })
    const protocol = new ConsumerGroupProtocol({
      cluster: {
        findGroupCoordinator: () => schedule.run({ api: 'FindCoordinator' }, () => null),
      },
      groupId: 'group',
      topics: ['topic'],
      ownership,
      rebalanceTimeout: 1000,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
    protocol.state = 'ACKNOWLEDGING'
    protocol.memberEpoch = 8
    const token = ownership.token()
    const heartbeat = protocol.sendHeartbeat({}).then(
      () => null,
      (error) => error
    )
    return {
      protocol,
      ownership,
      gate,
      token,
      heartbeat,
      renew: () => {
        ownership.renew()
        protocol.memberEpoch = 9
      },
    }
  }

  it('publishes no coordinator and builds no request when the lookup succeeds late', async () => {
    const { protocol, gate, heartbeat, renew } = heldLookup()
    await gate.arrived()
    renew()
    const coordinator = { consumerGroupHeartbeat: jest.fn() }
    const takeDue = jest.spyOn(protocol.backoff, 'takeDue')
    const failed = jest.spyOn(protocol.backoff, 'onFailedAttempt')
    gate.release(coordinator)
    expect(await heartbeat).toBeNull()
    expect(coordinator.consumerGroupHeartbeat).not.toHaveBeenCalled()
    expect(protocol.coordinator).toBeNull()
    expect(protocol.lastRequestAt).toBe(0)
    expect(takeDue).not.toHaveBeenCalled()
    expect(failed).not.toHaveBeenCalled()
    expect(protocol.state).toBe('ACKNOWLEDGING')
    expect(protocol.memberEpoch).toBe(9)
  })

  it('stamps a late lookup failure and records no backoff on the replacement', async () => {
    const { protocol, ownership, gate, token, heartbeat, renew } = heldLookup()
    await gate.arrived()
    renew()
    const failed = jest.spyOn(protocol.backoff, 'onFailedAttempt')
    const lookupError = new Error('lookup failed')
    gate.fail(lookupError)
    expect(await heartbeat).toBe(lookupError)
    expect(failed).not.toHaveBeenCalled()
    expect(protocol.backoff.pending(Date.now())).toBe(false)
    expect(sentOwnership(lookupError)).toBe(token)
    expect(obsoleteOwnership(ownership, lookupError)).toBe(true)
    expect(protocol.coordinator).toBeNull()
    expect(protocol.state).toBe('ACKNOWLEDGING')
  })

  it('still publishes, sends and records a failure for a lookup of the current membership', async () => {
    const { protocol, gate, heartbeat } = heldLookup()
    await gate.arrived()
    const coordinator = { consumerGroupHeartbeat: jest.fn(async () => ({ memberEpoch: 8 })) }
    gate.release(coordinator)
    expect(await heartbeat).toBeNull()
    expect(protocol.coordinator).toBe(coordinator)
    expect(coordinator.consumerGroupHeartbeat).toHaveBeenCalledTimes(1)

    const failing = heldLookup()
    await failing.gate.arrived()
    const failed = jest.spyOn(failing.protocol.backoff, 'onFailedAttempt')
    const lookupError = new Error('lookup failed')
    failing.gate.fail(lookupError)
    expect(await failing.heartbeat).toBe(lookupError)
    expect(failed).toHaveBeenCalledTimes(1)
  })
})

describe.each(['classic', 'consumer'])('commit edges on %s', (kind) => {
  const explicit = (partition, offset) => ({
    topics: [{ topic: SCHEDULED_TOPIC, partitions: [{ partition, offset }] }],
  })

  /** The identity of a handler whose partition moved on: another partition, an older assignment. */
  const staleIdentity = ({ group }) => ({
    topic: SCHEDULED_TOPIC,
    partition: 1,
    assignmentEpoch: group.assignmentEpoch - 1,
    acquireClock: group.acquireClock,
    ownershipKey: `${SCHEDULED_TOPIC}|1`,
  })

  it('refuses an implicit eachBatch commit through the handler guard once its batch is stale', async () => {
    const fixture = ownershipFixture({
      kind,
      eachBatch: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
        resolveOffset('0')
        await expect(commitOffsetsIfNecessary()).resolves.toBeUndefined()
      },
    })
    fixture.manager.autoCommitThreshold = 1
    const originalLastCommit = fixture.manager.lastCommit
    fixture.coordinator.offsetCommit.mockImplementation(async (_payload, { guard }) => {
      fixture.group.acquired.set(`${SCHEDULED_TOPIC}|0`, { acquiredAt: 9, changedAt: 9 })
      const refusal = guard()
      if (refusal) throw refusal
      return {}
    })
    try {
      const batch = fixture.batch()
      await handlerContext.run(batch, () => fixture.runner.processEachBatch(batch))
      expect(fixture.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
      expect(fixture.manager.lastCommit).toBe(originalLastCommit)
      expect(fixture.manager.committedOffsets()[SCHEDULED_TOPIC][0]).toBeUndefined()
      expect(fixture.operations.size()).toBe(0)
    } finally {
      await fixture.close()
    }
  })

  it('sends a due interval commit under a guard that follows the membership', async () => {
    const fixture = ownershipFixture({ kind })
    const cause = brokerError(25)
    const due = jest.spyOn(fixture.group, 'commitOffsetsIfNecessary')
    fixture.manager.autoCommitThreshold = 1
    fixture.manager.resolveOffset({ topic: SCHEDULED_TOPIC, partition: 0, offset: '4' })
    try {
      await fixture.runner.autoCommitOffsetsIfNecessary()
      expect(fixture.stored[0]).toBe('5')
      expect(due).toHaveBeenCalledWith({ guard: expect.any(Function) })
      const [{ guard }] = due.mock.calls[0]
      expect(guard()).toBeNull()
      fixture.ownership.lose(cause, fixture.ownership.token())
      expectRefusal(guard(), cause)
    } finally {
      await fixture.close()
    }
  })

  it('guards an explicit payload that names no topics through the calling batch', async () => {
    const fixture = ownershipFixture({
      kind,
      eachBatch: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
        resolveOffset('3')
        await commitOffsetsIfNecessary({})
        expect(fixture.stored[0]).toBe('4')
        fixture.coordinator.offsetCommit.mockImplementationOnce(async (_payload, { guard }) => {
          fixture.group.acquired.set(`${SCHEDULED_TOPIC}|0`, { acquiredAt: 9, changedAt: 9 })
          const refusal = guard()
          if (refusal) throw refusal
          return {}
        })
        resolveOffset('4')
        expectRefusal(await refusalOf(commitOffsetsIfNecessary({})), undefined, 'stale-assignment')
        expect(fixture.stored[0]).toBe('4')
      },
    })
    try {
      const batch = fixture.batch()
      await handlerContext.run(batch, () => fixture.runner.processEachBatch(batch))
      expect(fixture.coordinator.offsetCommit).toHaveBeenCalledTimes(2)
    } finally {
      await fixture.close()
    }
  })

  const payloads = [
    ['names topics', () => explicit(0, '4')],
    ['names no topics', () => ({})],
  ]

  it.each(payloads)(
    'commits an explicit eachBatch payload that %s once the runner has begun stopping',
    async (_form, payload) => {
      const fixture = ownershipFixture({
        kind,
        eachBatch: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
          resolveOffset('3')
          fixture.runner.running = false
          await commitOffsetsIfNecessary(payload())
        },
      })
      try {
        const batch = fixture.batch()
        await handlerContext.run(batch, () => fixture.runner.processEachBatch(batch))
        expect(fixture.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
        expect(fixture.stored[0]).toBe('4')
      } finally {
        await fixture.close()
      }
    }
  )

  it.each(payloads)(
    'rejects an explicit eachBatch commit that %s with the broker error itself after one attempt',
    async (_form, payload) => {
      const cause = brokerError(12)
      const fixture = ownershipFixture({
        kind,
        eachBatch: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
          resolveOffset('3')
          await expect(commitOffsetsIfNecessary(payload())).rejects.toBe(cause)
        },
      })
      fixture.coordinator.offsetCommit.mockRejectedValue(cause)
      try {
        const batch = fixture.batch()
        await handlerContext.run(batch, () => fixture.runner.processEachBatch(batch))
        expect(fixture.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
      } finally {
        await fixture.close()
      }
    }
  )

  it('keeps a valid batch implicit eachBatch commit when its guard runs in a stale batch context', async () => {
    const fixture = ownershipFixture({
      kind,
      eachBatch: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
        resolveOffset('0')
        await commitOffsetsIfNecessary()
      },
    })
    fixture.manager.autoCommitThreshold = 1
    const stale = staleIdentity(fixture)
    fixture.coordinator.offsetCommit.mockImplementation(async (_payload, { guard }) => {
      const refusal = handlerContext.run(stale, guard)
      if (refusal) throw refusal
      return {}
    })
    try {
      const batch = fixture.batch()
      await handlerContext.run(batch, () => fixture.runner.processEachBatch(batch))
      expect(fixture.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
      expect(fixture.manager.committedOffsets()[SCHEDULED_TOPIC][0]).toBe('1')
    } finally {
      await fixture.close()
    }
  })

  it('keeps a valid batch interval commit when its guard runs in a stale batch context', async () => {
    const fixture = ownershipFixture({ kind })
    const due = jest.spyOn(fixture.group, 'commitOffsetsIfNecessary')
    fixture.manager.autoCommitThreshold = 1
    fixture.manager.resolveOffset({ topic: SCHEDULED_TOPIC, partition: 0, offset: '4' })
    const stale = staleIdentity(fixture)
    try {
      await handlerContext.run(fixture.batch(), () => fixture.runner.autoCommitOffsetsIfNecessary())
      const [{ guard }] = due.mock.calls[0]
      expect(handlerContext.run(stale, guard)).toBeNull()
      expect(handlerContext.run(stale, () => fixture.runner.commitRefusal())).not.toBeNull()
    } finally {
      await fixture.close()
    }
  })

  it.each([
    ['a public commit', 'commitOffsets', 1, (runner) => runner.commitOffsets(explicit(0, '4'))],
    ['an automatic commit', 'commitOffsets', 1, (runner) => runner.autoCommitOffsets()],
    [
      'an interval commit',
      'commitOffsetsIfNecessary',
      0,
      (runner) => runner.autoCommitOffsetsIfNecessary(),
    ],
  ])(
    'keeps %s made outside any handler when its guard runs in a stale batch context',
    async (_name, method, argument, commit) => {
      const fixture = ownershipFixture({ kind })
      const send = jest.spyOn(fixture.group, method).mockResolvedValue()
      try {
        await commit(fixture.runner)
        const { guard } = send.mock.calls[0][argument]
        expect(handlerContext.run(staleIdentity(fixture), guard)).toBeNull()
      } finally {
        await fixture.close()
      }
    }
  )

  it('commits a filtered batch whatever handler store the runner inherited', async () => {
    const fixture = ownershipFixture({ kind, eachMessage: async () => {} })
    const due = jest.spyOn(fixture.group, 'commitOffsetsIfNecessary').mockResolvedValue()
    const batch = {
      ...fixture.batch(),
      ownershipKey: `${SCHEDULED_TOPIC}|0`,
      messages: [],
      highWatermark: '1',
      offsetLag: () => '0',
      offsetLagLow: () => '0',
      isEmpty: () => true,
      isEmptyDueToFiltering: () => true,
    }
    const stale = staleIdentity(fixture)
    try {
      await handlerContext.run(stale, () => fixture.runner.handleBatch(batch))
      expect(due).toHaveBeenCalledTimes(1)
      expect(handlerContext.run(stale, due.mock.calls[0][0].guard)).toBeNull()
    } finally {
      await fixture.close()
    }
  })

  it('releases the retained identity when a commit finds the runner stopped', async () => {
    const fixture = ownershipFixture({ kind })
    fixture.runner.running = false
    try {
      expect(fixture.runner.commitOffsets(explicit(0, '4'))).toBeUndefined()
      expect(fixture.coordinator.offsetCommit).not.toHaveBeenCalled()
      fixture.ownership.renew()
      expect(fixture.ownership.states.size).toBe(1)
    } finally {
      await fixture.close()
    }
  })

  it('never looks up a coordinator for a reset commit that starts after the loss', async () => {
    const fixture = ownershipFixture({ kind })
    const originalLastCommit = fixture.manager.lastCommit
    const lookup = jest.spyOn(fixture.manager, 'getCoordinator')
    fixture.ownership.lose(brokerError(25), fixture.ownership.token())
    try {
      await expect(
        fixture.manager.setDefaultOffset({ topic: SCHEDULED_TOPIC, partition: 0 })
      ).resolves.toBeUndefined()
      expect(lookup).not.toHaveBeenCalled()
      expect(fixture.coordinator.offsetCommit).not.toHaveBeenCalled()
      expect(fixture.manager.lastCommit).toBe(originalLastCommit)
      expect(fixture.operations.size()).toBe(0)
    } finally {
      await fixture.close()
    }
  })

  it('fences local publication when the loss lands while the commit event is emitted', async () => {
    const fixture = ownershipFixture({ kind })
    const cause = brokerError(25)
    const originalLastCommit = fixture.manager.lastCommit
    fixture.manager.instrumentationEmitter.addListener(COMMIT_OFFSETS, () =>
      fixture.ownership.lose(cause, fixture.ownership.token())
    )
    try {
      expectRefusal(await refusalOf(fixture.manager.commitOffsets(explicit(0, '7'))), cause)
      expect(fixture.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
      expect(fixture.manager.committedOffsets()[SCHEDULED_TOPIC][0]).toBeUndefined()
      expect(fixture.manager.lastCommit).toBe(originalLastCommit)
      expect(fixture.operations.size()).toBe(0)
    } finally {
      await fixture.close()
    }
  })

  it('keeps lastCommit when the loss lands right after local publication', async () => {
    const fixture = ownershipFixture({ kind })
    const cause = brokerError(25)
    const originalLastCommit = fixture.manager.lastCommit
    fixture.manager.instrumentationEmitter.addListener(COMMIT_OFFSETS, () =>
      queueMicrotask(() => fixture.ownership.lose(cause, fixture.ownership.token()))
    )
    try {
      expectRefusal(await refusalOf(fixture.manager.commitOffsets(explicit(0, '7'))), cause)
      expect(fixture.manager.lastCommit).toBe(originalLastCommit)
      expect(fixture.operations.size()).toBe(0)
    } finally {
      await fixture.close()
    }
  })
})

describe('fetch rounds of a replaced membership', () => {
  it('rethrows the error of a round that outlived its membership and recovers from a current one', async () => {
    const fixture = ownershipFixture()
    const recover = jest.spyOn(fixture.group, 'recoverFromFetch').mockResolvedValue()
    const old = new Error('old round')
    const current = new Error('current round')
    try {
      fixture.cluster.refreshMetadataIfNecessary = async () => {
        fixture.ownership.renew()
        throw old
      }
      await expect(fixture.group.fetch(0)).rejects.toBe(old)
      expect(recover).not.toHaveBeenCalled()

      fixture.cluster.refreshMetadataIfNecessary = async () => {
        throw current
      }
      await expect(fixture.group.fetch(0)).resolves.toEqual([])
      expect(recover).toHaveBeenCalledWith(
        current,
        expect.objectContaining({ guard: expect.any(Function), closed: false })
      )
    } finally {
      await fixture.close()
    }
  })
})
