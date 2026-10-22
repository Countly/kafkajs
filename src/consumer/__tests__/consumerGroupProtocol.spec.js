const ConsumerGroupProtocol = require('../groupProtocol/consumer')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { events } = require('../instrumentationEvents')
const { MEMBER_STATES } = require('../groupProtocol/memberState')
const { COOPERATIVE } = require('../groupProtocol/interface')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSConnectionError } = require('../../errors')
const { newLogger } = require('testHelpers')

const {
  UNSUBSCRIBED,
  JOINING,
  RECONCILING,
  ACKNOWLEDGING,
  STABLE,
  PREPARE_LEAVING,
  LEAVING,
  FENCED,
  FATAL,
  STALE,
} = MEMBER_STATES

const GROUP_ID = 'consumer-protocol-group'
const TOPIC_ID_A = 'AAAAAAAAAAAAAAAAAAAAAg'
const TOPIC_ID_B = 'AAAAAAAAAAAAAAAAAAAAAw'
const TOPIC_NAMES = { [TOPIC_ID_A]: 'topic-a', [TOPIC_ID_B]: 'topic-b' }

const heartbeatResponse = ({
  memberEpoch = 1,
  heartbeatIntervalMs = 5000,
  assignment = null,
} = {}) => ({
  throttleTime: 0,
  errorCode: 0,
  errorMessage: null,
  memberId: null,
  memberEpoch,
  heartbeatIntervalMs,
  assignment,
})

const assignmentOf = (entries) => ({
  topicPartitions: entries.map(([topicId, partitions]) => ({ topicId, partitions })),
})

/**
 * A heartbeat manager double whose `handle`, `chainId`, `inFlight` and `tickRequested` follow the
 * real class's binding pseudo-code exactly. It is a MODEL of that class, not the class: these
 * cases prove the PROTOCOL drives the manager correctly, and the manager's own spec proves the
 * manager behaves this way. Both halves are required; neither alone is sufficient.
 */
const fakeHeartbeatManager = () => ({
  handle: null,
  chainId: 0,
  inFlight: false,
  tickRequested: false,
  calls: [],
  errors: [],
  protocol: null,

  requestTick() {
    this.calls.push('requestTick')
    if (this.inFlight) {
      this.tickRequested = true
      return
    }
    this.armNow()
  },

  armNow() {
    this.calls.push('armNow')
    this.handle = `T${++this.chainId}`
  },

  ensureStarted() {
    this.calls.push('ensureStarted')
    if (this.handle) return
    this.chainId++
    this.handle = `T${this.chainId}`
  },

  stop() {
    this.calls.push('stop')
    this.handle = null
  },

  async tick() {
    const chainId = this.chainId
    if (!this.handle || chainId !== this.chainId) return

    this.inFlight = true
    try {
      await this.protocol.heartbeat({ interval: this.protocol.heartbeatInterval() })
    } catch (error) {
      this.errors.push(error)
    } finally {
      this.inFlight = false
      const rearm = this.tickRequested && this.handle !== null
      this.tickRequested = false
      if (rearm) this.armNow()
    }
  },
})

const build = ({
  answers = [heartbeatResponse()],
  names = TOPIC_NAMES,
  groupInstanceId = null,
  serverAssignor = null,
  topics = ['topic-a'],
  subscribedTopicRegex = null,
  rackId = '',
  attached = true,
  rebalanceTimeout = 300000,
  metadataMaxAge = 300000,
  retry = {},
  commitBeforeRelease = null,
} = {}) => {
  const sent = []
  const queue = [...answers]
  const refreshedIds = []
  const heartbeats = []

  const coordinator = {
    // The LAST scripted answer repeats, so a case that scripts one response gets a steady state.
    consumerGroupHeartbeat: jest.fn(async (request) => {
      sent.push(request)
      const scripted = queue.length > 1 ? queue.shift() : queue[0]
      const answer = typeof scripted === 'function' ? scripted(request) : scripted
      if (answer instanceof Error) throw answer
      return answer
    }),
  }

  const cluster = {
    findGroupCoordinator: jest.fn(async () => coordinator),
    topicName: (topicId) => names[topicId],
    refreshMetadataForTopicIds: jest.fn(async (ids) => {
      refreshedIds.push([...ids])
      return {
        resolved: ids.filter((topicId) => names[topicId] != null),
        unresolved: ids.filter((topicId) => names[topicId] == null),
      }
    }),
  }

  const instrumentationEmitter = new InstrumentationEventEmitter()
  instrumentationEmitter.addListener(events.HEARTBEAT, ({ payload }) => heartbeats.push(payload))

  // newLogger() returns a REAL logger, not a mock, so every count assertion goes through spies
  // installed here - the same idiom the shipped classic-protocol spec uses.
  const logger = newLogger()
  const logged = {
    error: jest.spyOn(logger, 'error'),
    warn: jest.spyOn(logger, 'warn'),
    debug: jest.spyOn(logger, 'debug'),
    info: jest.spyOn(logger, 'info'),
  }

  const protocol = new ConsumerGroupProtocol({
    cluster,
    groupId: GROUP_ID,
    groupInstanceId,
    rebalanceTimeout,
    rackId,
    serverAssignor,
    topics,
    subscribedTopicRegex,
    metadataMaxAge,
    retry,
    instrumentationEmitter,
    logger,
  })

  const manager = fakeHeartbeatManager()
  manager.protocol = protocol

  const rebalanceCoordinator = { requestRejoin: jest.fn(), emitPartitions: jest.fn() }
  if (attached) {
    protocol.attach({ rebalanceCoordinator, heartbeatManager: manager, commitBeforeRelease })
  }

  return {
    protocol,
    manager,
    coordinator,
    cluster,
    rebalanceCoordinator,
    logger,
    logged,
    sent,
    refreshedIds,
    heartbeats,
  }
}

const context = (overrides = {}) => ({
  topics: ['topic-a'],
  topicsSubscribed: ['topic-a'],
  beforeSync: jest.fn(),
  growSubscription: jest.fn(async () => {}),
  ...overrides,
})

/**
 * Starts one reconciliation step and lets the joining heartbeat it armed go out.
 *
 * The step promise is observed HERE and still returned: a step that a heartbeat rejects would
 * otherwise be an unobserved rejection for the turns this helper spends inside the tick, which is
 * exactly what the suite-wide unhandled-rejection check would catch. The caller still awaits it and
 * still sees the rejection.
 */
const startStep = async (h, ctx = context()) => {
  const pending = h.protocol.rebalance(ctx)
  pending.catch(() => {})
  await h.manager.tick()
  return pending
}

/**
 * Lets an un-awaited metadata refresh and the target re-read it triggers settle. Microtasks rather
 * than a timer: the suite runs on fake timers, and nothing on this path is scheduled on one.
 */
const flushRefresh = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve()
}

// The requests the coordinator double receives are the broker method's payload, so the assertions
// read its field names rather than the wire's.
const partitionsSentOn = (request) =>
  (request.topicPartitions || []).map(({ topicId, partitions }) => [topicId, partitions])

describe('Consumer > groupProtocol > ConsumerGroupProtocol', () => {
  let unhandled

  const trackUnhandled = (reason) => unhandled.push(reason)

  beforeEach(() => {
    unhandled = []
    process.on('unhandledRejection', trackUnhandled)
    jest.useFakeTimers()
  })

  afterEach(async () => {
    // Applies to every case, not only its own: nothing this component fires may reach
    // the process as an unobserved rejection.
    jest.useRealTimers()
    await new Promise((resolve) => setImmediate(resolve))
    process.removeListener('unhandledRejection', trackUnhandled)
    expect(unhandled).toEqual([])
  })

  describe('The join', () => {
    it('sends one full heartbeat at epoch 0 and reports the join once the epoch moves', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 5 })] })

      const result = await await startStep(h)

      expect(h.sent).toHaveLength(1)
      expect(h.sent[0]).toMatchObject({
        groupId: GROUP_ID,
        memberId: h.protocol.memberId,
        memberEpoch: 0,
        rebalanceTimeoutMs: 300000,
      })
      expect(h.sent[0].subscribedTopicNames).toEqual(['topic-a'])
      expect(h.sent[0].topicPartitions).toEqual([])
      expect(result).toEqual({ assignment: {}, userData: null })
      expect(h.protocol.memberEpoch).toEqual(5)
      expect(h.protocol.owned()).toEqual([])
    })
  })

  describe('A join into an empty group', () => {
    it('leaves the state untouched, as the Java client does for a null assignment', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 5, assignment: null })] })

      const result = await await startStep(h)

      expect(h.protocol.state).toEqual(JOINING)
      expect(h.protocol.owned()).toEqual([])
      expect(result.assignment).toEqual({})
    })
  })

  describe('A target arrives', () => {
    it('moves to RECONCILING and asks for one rejoin, and does not ask again for the same target', async () => {
      const h = build({
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) })],
      })

      await await startStep(h)

      expect(h.protocol.state).toEqual(RECONCILING)
      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledWith({
        kind: 'revoked',
        reason: 'assignment-changed',
        error: expect.objectContaining({ type: 'ASSIGNMENT_CHANGED' }),
      })

      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
    })
  })

  describe('The step returns the resolvable subset and the apply acks it', () => {
    it('returns the assignment, and setOwned moves to ACKNOWLEDGING and sends the owned set', async () => {
      const h = build({
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) })],
      })
      const ctx = context()

      const result = await await startStep(h, ctx)
      expect(result).toEqual({ assignment: { 'topic-a': [0, 1] }, userData: null })
      expect(ctx.beforeSync).toHaveBeenCalledTimes(1)

      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0, 1] }])

      expect(h.protocol.state).toEqual(ACKNOWLEDGING)
      expect(h.manager.calls).toContain('requestTick')

      await h.manager.tick()

      expect(partitionsSentOn(h.sent[1])).toEqual([[TOPIC_ID_A, [0, 1]]])
    })
  })

  describe('The ack, on generated', () => {
    it('goes to STABLE when the owned set matches the target', async () => {
      const h = build({
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) })],
      })

      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0, 1] }])
      await h.manager.tick()

      expect(h.protocol.state).toEqual(STABLE)
    })

    it('goes back to RECONCILING and asks for another round when it does not', async () => {
      const h = build({
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) })],
      })

      await await startStep(h)
      h.rebalanceCoordinator.requestRejoin.mockClear()
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()

      expect(h.protocol.state).toEqual(RECONCILING)
      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
    })
  })

  describe('The target changes mid-round', () => {
    it('applies the snapshot the step took, and the next step applies the new target', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          heartbeatResponse({ memberEpoch: 2, assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) }),
        ],
      })

      const first = await await startStep(h)
      expect(first.assignment).toEqual({ 'topic-a': [0] })

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])

      expect(h.protocol.needsReconciliation()).toBe(true)

      const second = await h.protocol.rebalance(context())
      expect(second.assignment).toEqual({ 'topic-a': [0, 1] })
    })
  })

  describe('Unresolved topic ids', () => {
    it('asks for metadata by id once, applies the resolvable subset, and acks the rest', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        answers: [
          heartbeatResponse({
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
      })

      const result = await await startStep(h)

      // Exactly one request, from the reconciliation step. The heartbeat path's own staleness
      // check does not fire here: `lastMetadataRefreshAt` starts at construction time, so the
      // member is not treated as having last refreshed at the epoch.
      expect(h.refreshedIds).toEqual([[TOPIC_ID_B]])
      expect(result.assignment).toEqual({ 'topic-a': [0] })

      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()

      expect(partitionsSentOn(h.sent[1])).toEqual([[TOPIC_ID_A, [0]]])
      expect(h.protocol.needsReconciliation()).toBe(false)
    })

    it('retries the unresolved ids from the heartbeat path at most once per metadata max age', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        metadataMaxAge: 10000,
        answers: [
          heartbeatResponse({
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      expect(h.refreshedIds).toHaveLength(1)

      // Two heartbeats inside one max-age window add nothing; the one after it retries.
      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      jest.advanceTimersByTime(4000)
      await h.manager.tick()
      expect(h.refreshedIds).toHaveLength(1)

      jest.advanceTimersByTime(10000)
      await h.manager.tick()

      expect(h.refreshedIds).toHaveLength(2)
      expect(h.refreshedIds[1]).toEqual([TOPIC_ID_B])
    })

    it('stops asking once the cluster has reported an id unresolved twice', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        metadataMaxAge: 1000,
        answers: [
          heartbeatResponse({
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      expect(h.refreshedIds).toHaveLength(2)

      // Two misses is the whole budget: every later heartbeat still finds the id unresolved and
      // asks for nothing, so a topic this client may never see costs a bounded number of requests.
      for (let i = 0; i < 3; i++) {
        jest.advanceTimersByTime(5000)
        await h.manager.tick()
      }

      expect(h.refreshedIds).toHaveLength(2)
      expect(h.protocol.needsReconciliation()).toBe(false)
    })
  })

  describe('A target naming a topic id this member has never seen', () => {
    const TARGET_A = assignmentOf([[TOPIC_ID_A, [0]]])
    const TARGET_A_AND_B = assignmentOf([
      [TOPIC_ID_A, [0]],
      [TOPIC_ID_B, [0]],
    ])

    /**
     * A member that owns topic-a and has acknowledged it, with every later heartbeat answered by
     * `later` - the coordinator widening the target with an id the client has never resolved. The
     * caller ticks, so it can arm the cluster double first.
     */
    const memberOwningTopicA = async ({ names, metadataMaxAge = 300000, later }) => {
      const h = build({
        names,
        metadataMaxAge,
        answers: [heartbeatResponse({ assignment: TARGET_A }), later],
      })

      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      h.rebalanceCoordinator.requestRejoin.mockClear()

      return h
    }

    it('asks the cluster for that id in the same turn and then reconciles onto the topic', async () => {
      const names = { [TOPIC_ID_A]: 'topic-a' }
      const h = await memberOwningTopicA({
        names,
        later: heartbeatResponse({ memberEpoch: 2, assignment: TARGET_A_AND_B }),
      })

      // What a Metadata response by id does to the cache: the name arrives with the response, a
      // turn after the request, never inside the call.
      const answerRefresh = h.cluster.refreshMetadataForTopicIds.getMockImplementation()
      h.cluster.refreshMetadataForTopicIds.mockImplementation(async (ids) => {
        await Promise.resolve()
        names[TOPIC_ID_B] = 'topic-b'
        return answerRefresh(ids)
      })

      await h.manager.tick()

      // The same turn as the response that named the id: no second heartbeat, and no wait for the
      // metadata max age, which is the shipped default 300000 here.
      expect(h.cluster.refreshMetadataForTopicIds).toHaveBeenCalledWith([TOPIC_ID_B])
      expect(h.sent).toHaveLength(2)

      await flushRefresh()

      expect(h.refreshedIds).toEqual([[TOPIC_ID_B]])

      expect(h.protocol.state).toEqual(RECONCILING)
      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
      expect((await h.protocol.rebalance(context())).assignment).toEqual({
        'topic-a': [0],
        'topic-b': [0],
      })
    })

    it('asks once now, then once per metadata max age, and starts the budget over on a new target', async () => {
      let answer = heartbeatResponse({ memberEpoch: 2, assignment: TARGET_A_AND_B })
      const h = await memberOwningTopicA({
        names: { [TOPIC_ID_A]: 'topic-a' },
        metadataMaxAge: 10000,
        later: () => answer,
      })

      await h.manager.tick()
      expect(h.refreshedIds).toEqual([[TOPIC_ID_B]])

      // Inside the window every heartbeat carries the same target, and nothing is asked again.
      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      expect(h.refreshedIds).toHaveLength(1)

      jest.advanceTimersByTime(6000)
      await h.manager.tick()
      expect(h.refreshedIds).toHaveLength(2)

      // Two misses is the whole budget for one target, whatever the immediate ask spent of it.
      for (let i = 0; i < 3; i++) {
        jest.advanceTimersByTime(11000)
        await h.manager.tick()
      }
      expect(h.refreshedIds).toHaveLength(2)

      // A new target naming the same id is the coordinator asserting it exists, so it is asked for
      // again, at once.
      answer = heartbeatResponse({
        memberEpoch: 3,
        assignment: assignmentOf([
          [TOPIC_ID_A, [0]],
          [TOPIC_ID_B, [0, 1]],
        ]),
      })
      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(h.refreshedIds).toHaveLength(3)
      expect(h.refreshedIds[2]).toEqual([TOPIC_ID_B])
    })

    it('keeps a failure in the re-read after a refresh off the process', async () => {
      const names = { [TOPIC_ID_A]: 'topic-a' }
      const h = await memberOwningTopicA({
        names,
        later: heartbeatResponse({ memberEpoch: 2, assignment: TARGET_A_AND_B }),
      })

      const answerRefresh = h.cluster.refreshMetadataForTopicIds.getMockImplementation()
      h.cluster.refreshMetadataForTopicIds.mockImplementation(async (ids) => {
        await Promise.resolve()
        names[TOPIC_ID_B] = 'topic-b'
        return answerRefresh(ids)
      })
      h.rebalanceCoordinator.requestRejoin.mockImplementation(() => {
        throw new Error('the round loop is gone')
      })

      await h.manager.tick()
      await flushRefresh()

      // The suite-wide unhandled-rejection check is the other half of this assertion: the re-read
      // runs with no caller, so what it raises would otherwise reach the process.
      expect(h.logged.debug).toHaveBeenCalledWith(
        'Could not re-read the target after refreshing its topic ids',
        expect.objectContaining({ groupId: GROUP_ID, error: 'the round loop is gone' })
      )
    })

    it('drops a refresh that lands after the target was cleared', async () => {
      const names = { [TOPIC_ID_A]: 'topic-a' }
      const h = await memberOwningTopicA({
        names,
        later: heartbeatResponse({ memberEpoch: 2, assignment: TARGET_A_AND_B }),
      })

      const releases = []
      h.cluster.refreshMetadataForTopicIds.mockImplementation(async (ids) => {
        await new Promise((resolve) => releases.push(resolve))
        names[TOPIC_ID_B] = 'topic-b'
        return { resolved: ids, unresolved: [] }
      })

      await h.manager.tick()
      expect(h.cluster.refreshMetadataForTopicIds).toHaveBeenCalledTimes(1)

      h.protocol.onMembershipError(createErrorFromCode(25))
      expect(h.protocol.state).toEqual(FENCED)

      releases[0]()
      await flushRefresh()

      // The target is gone, so there is nothing to re-read: reading it anyway would move a fenced
      // member and would reach the suite's unhandled-rejection check.
      expect(h.protocol.state).toEqual(FENCED)
      expect(h.protocol.target).toBeNull()
      expect(h.rebalanceCoordinator.requestRejoin).not.toHaveBeenCalled()
    })

    it('drops a refresh that lands after a newer target superseded the one it was resolving', async () => {
      const names = { [TOPIC_ID_A]: 'topic-a' }
      let answer = heartbeatResponse({ memberEpoch: 2, assignment: TARGET_A_AND_B })
      const h = await memberOwningTopicA({ names, later: () => answer })

      const releases = []
      h.cluster.refreshMetadataForTopicIds.mockImplementation(async (ids) => {
        await new Promise((resolve) => releases.push(resolve))
        names[TOPIC_ID_B] = 'topic-b'
        return { resolved: ids, unresolved: [] }
      })

      await h.manager.tick()
      expect(releases).toHaveLength(1)

      answer = heartbeatResponse({
        memberEpoch: 3,
        assignment: assignmentOf([
          [TOPIC_ID_A, [0]],
          [TOPIC_ID_B, [0, 1]],
        ]),
      })
      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      h.rebalanceCoordinator.requestRejoin.mockClear()

      releases[0]()
      await flushRefresh()

      // The stale refresh resolved a name the current target also wants, so re-reading it would
      // ask for a round under an epoch that is no longer this member's.
      expect(h.rebalanceCoordinator.requestRejoin).not.toHaveBeenCalled()
      expect(h.protocol.state).toEqual(STABLE)
    })
  })

  describe('Fencing on the heartbeat path', () => {
    it('goes to FENCED, keeps the epoch and the member id, resets the sent fields and rejoins', async () => {
      const fenced = createErrorFromCode(110)
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 7 }),
          fenced,
          heartbeatResponse({ memberEpoch: 9 }),
        ],
      })

      await await startStep(h)
      const memberId = h.protocol.memberId

      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(h.protocol.state).toEqual(FENCED)
      expect(h.protocol.memberEpoch).toEqual(7)
      expect(h.protocol.memberId).toEqual(memberId)
      expect(h.manager.errors[0].code).toEqual(110)

      const rejoined = await await startStep(h, context())

      expect(h.protocol.memberEpoch).toEqual(9)
      expect(rejoined.assignment).toEqual({})
    })
  })

  describe('Fencing learned on the commit or the fetch path', () => {
    it('reaches the state machine through onMembershipError, idempotently', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 4 })] })
      await await startStep(h)

      h.protocol.onMembershipError(createErrorFromCode(25))
      expect(h.protocol.state).toEqual(FENCED)

      h.protocol.onMembershipError(createErrorFromCode(25))
      expect(h.protocol.state).toEqual(FENCED)
      expect(h.protocol.memberEpoch).toEqual(4)
    })

    it('is a no-op for an error that is neither fencing nor fatal', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 4 })] })
      await await startStep(h)

      h.protocol.onMembershipError(createErrorFromCode(27))

      expect(h.protocol.state).toEqual(JOINING)
    })
  })

  describe('The fatal codes', () => {
    test.each([[81], [111], [112], [42], [30], [82]])(
      'code %i moves to FATAL and rethrows',
      async (code) => {
        const h = build({ answers: [createErrorFromCode(code)] })

        await expect(startStep(h).then((pending) => pending)).rejects.toThrow()

        expect(h.protocol.state).toEqual(FATAL)
      }
    )

    it('prefixes the invalid-regex message the way the Java client does', async () => {
      const h = build({ answers: [createErrorFromCode(128)] })

      await expect(startStep(h).then((pending) => pending)).rejects.toThrow(
        'Invalid RE2J SubscriptionPattern provided in the call to subscribe.'
      )
      expect(h.protocol.state).toEqual(FATAL)
    })

    // A fatal error can reach `onMembershipError` from the commit or the fetch path while
    // the member is out of the group. Recording it without entering FATAL dropped it - the member
    // rejoined and hit the same error again, and the stale `fatalError` outlived the rejoin.
    it('enters FATAL from FENCED, so the round does not rejoin into the same error', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 4 })] })
      await await startStep(h)

      h.protocol.onMembershipError(createErrorFromCode(25))
      expect(h.protocol.state).toEqual(FENCED)

      h.protocol.onMembershipError(createErrorFromCode(112))

      expect(h.protocol.state).toEqual(FATAL)
      await expect(h.protocol.rebalance(context())).rejects.toMatchObject({ code: 112 })
    })

    it('enters FATAL from STALE too', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      h.manager.stop()
      await h.protocol.leaveForPollExpiry()
      expect(h.protocol.state).toEqual(STALE)

      h.protocol.onMembershipError(createErrorFromCode(112))

      expect(h.protocol.state).toEqual(FATAL)
      await expect(h.protocol.rebalance(context())).rejects.toMatchObject({ code: 112 })
    })

    it('refuses to rejoin once fatal', async () => {
      const h = build({ answers: [createErrorFromCode(112)] })
      await expect(startStep(h).then((pending) => pending)).rejects.toThrow()

      await expect(h.protocol.rebalance(context())).rejects.toThrow()
      expect(h.sent).toHaveLength(1)
    })
  })

  describe('The cluster does not support the protocol', () => {
    it('is fatal, and says what to set instead', async () => {
      const h = build({ answers: [createErrorFromCode(35)] })

      await expect(startStep(h).then((pending) => pending)).rejects.toThrow(
        "The cluster does not support the new CONSUMER group protocol. Set groupProtocol: 'classic' on the consumer until the cluster is upgraded."
      )
      expect(h.protocol.state).toEqual(FATAL)
    })
  })

  describe('Coordinator errors', () => {
    it('drops the cached coordinator and re-finds it without a backoff on 15 and 16', async () => {
      for (const code of [15, 16]) {
        const h = build({
          answers: [
            heartbeatResponse({ memberEpoch: 3 }),
            createErrorFromCode(code),
            heartbeatResponse({ memberEpoch: 4 }),
          ],
        })
        await await startStep(h)
        h.cluster.findGroupCoordinator.mockClear()
        h.manager.calls.length = 0

        jest.advanceTimersByTime(5000)
        await h.manager.tick()

        expect(h.manager.calls).toContain('requestTick')
        expect(h.protocol.heartbeatInterval()).toEqual(5000)

        await h.manager.tick()
        expect(h.cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
        expect(h.protocol.memberEpoch).toEqual(4)
      }
    })

    it('re-finds it on the next tick from a state that owes no immediate heartbeat', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          createErrorFromCode(16),
          heartbeatResponse({ memberEpoch: 4 }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()
      expect(h.protocol.state).toEqual(STABLE)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      h.cluster.findGroupCoordinator.mockClear()

      // No time passes: STABLE owes no immediate heartbeat, so only the due retry the coordinator
      // move marked gets this request past the interval gate.
      await h.manager.tick()

      expect(h.cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
      expect(h.protocol.memberEpoch).toEqual(4)
    })

    it('backs off exponentially on 14, and surfaces the remaining delay as the heartbeat interval', async () => {
      jest.spyOn(Math, 'random').mockReturnValue(0.5)
      const h = build({
        answers: [heartbeatResponse({ memberEpoch: 3 }), createErrorFromCode(14)],
        retry: { initialRetryTime: 300, maxRetryTime: 30000, multiplier: 2, factor: 0.2 },
      })
      await await startStep(h)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      expect(h.protocol.heartbeatInterval()).toEqual(300)
      expect(h.protocol.state).toEqual(JOINING)

      jest.advanceTimersByTime(300)
      await h.manager.tick()
      expect(h.protocol.heartbeatInterval()).toEqual(600)

      Math.random.mockRestore()
    })

    it.each([
      [0.5, [300, 600, 1000, 1000, 1000, 1000]],
      [1, [360, 720, 1000, 1000, 1000, 1000]],
    ])(
      'caps the heartbeat backoff at 1 s after the jitter, as the Java client caps it at retry.backoff.max.ms (random %s)',
      async (random, expected) => {
        jest.spyOn(Math, 'random').mockReturnValue(random)
        const h = build({
          answers: [heartbeatResponse({ memberEpoch: 3 }), createErrorFromCode(14)],
          retry: { initialRetryTime: 300, maxRetryTime: 30000, multiplier: 2, factor: 0.2 },
        })
        await await startStep(h)

        jest.advanceTimersByTime(5000)
        const intervals = []
        for (let i = 0; i < 6; i++) {
          await h.manager.tick()
          intervals.push(h.protocol.heartbeatInterval())
          jest.advanceTimersByTime(intervals[i])
        }

        expect(intervals).toEqual(expected)
        Math.random.mockRestore()
      }
    )

    it('keeps the backoff inside its jitter band and under the cap', async () => {
      for (const [random, expected] of [
        [0, 240],
        [1, 360],
      ]) {
        jest.spyOn(Math, 'random').mockReturnValue(random)
        const h = build({
          answers: [heartbeatResponse({ memberEpoch: 3 }), createErrorFromCode(14)],
          retry: { initialRetryTime: 300, maxRetryTime: 30000, multiplier: 2, factor: 0.2 },
        })
        await await startStep(h)
        jest.advanceTimersByTime(5000)
        await h.manager.tick()

        expect(h.protocol.heartbeatInterval()).toEqual(expected)
        Math.random.mockRestore()
      }
    })
  })

  describe('A topic this member may not read', () => {
    it('logs once per distinct message, keeps the membership and keeps heartbeating', async () => {
      const h = build({
        answers: [heartbeatResponse({ memberEpoch: 3 }), createErrorFromCode(29)],
      })
      await await startStep(h)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(h.protocol.state).toEqual(JOINING)
      expect(h.protocol.memberEpoch).toEqual(3)
      expect(h.logged.error).toHaveBeenCalledTimes(1)
      expect(h.sent).toHaveLength(3)
    })

    it('logs again when the set of unauthorized topics changes', async () => {
      const unauthorized = (message) => {
        const error = createErrorFromCode(29)
        error.message = message
        return error
      }
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3 }),
          unauthorized('Not authorized to access topics: [orders]'),
          unauthorized('Not authorized to access topics: [orders]'),
          unauthorized('Not authorized to access topics: [orders, payments]'),
        ],
      })
      await await startStep(h)

      for (let i = 0; i < 3; i++) {
        jest.advanceTimersByTime(60000)
        await h.manager.tick()
      }
      expect(h.sent).toHaveLength(4)

      // Two distinct messages, three failures: the repeat is swallowed, the change is reported.
      expect(h.logged.error).toHaveBeenCalledTimes(2)
      expect(h.logged.error.mock.calls[1][1]).toMatchObject({
        error: 'Not authorized to access topics: [orders, payments]',
      })
      expect(h.protocol.state).toEqual(JOINING)
    })
  })

  describe('Leaving', () => {
    it('commits, reports the partitions as revoked and sends epoch -1', async () => {
      const commitBeforeRelease = jest.fn(async () => {})
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
        ],
        commitBeforeRelease,
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      h.manager.stop()

      await h.protocol.leave()

      expect(commitBeforeRelease).toHaveBeenCalledWith([{ topic: 'topic-a', partitions: [0] }])
      expect(h.rebalanceCoordinator.emitPartitions).toHaveBeenCalledWith(
        events.PARTITIONS_REVOKED,
        [{ topic: 'topic-a', partitions: [0] }]
      )
      expect(h.sent[h.sent.length - 1].memberEpoch).toEqual(-1)
      expect(h.protocol.state).toEqual(UNSUBSCRIBED)
      expect(h.manager.handle).toBeNull()
    })

    it('sends epoch -2 for a static member', async () => {
      const h = build({
        groupInstanceId: 'instance-1',
        answers: [heartbeatResponse({ memberEpoch: 3 })],
      })
      await await startStep(h)
      h.manager.stop()

      await h.protocol.leave()

      expect(h.sent[h.sent.length - 1].memberEpoch).toEqual(-2)
    })

    it('reports the partitions as lost and commits nothing when the epoch is not positive', async () => {
      const commitBeforeRelease = jest.fn(async () => {})
      const h = build({ commitBeforeRelease })

      // In the group but without an epoch yet: joining, which is the one state where a leave has
      // partitions to release and no epoch to commit them under.
      const joining = h.protocol.rebalance(context())
      joining.catch(() => {})
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])

      await h.protocol.leave()

      expect(commitBeforeRelease).not.toHaveBeenCalled()
      expect(h.rebalanceCoordinator.emitPartitions).toHaveBeenCalledWith(events.PARTITIONS_LOST, [
        { topic: 'topic-a', partitions: [0] },
      ])

      // The leave abandoned the join, so the round it belonged to is told the consumer is going
      // away rather than being handed an assignment for a member that has left.
      h.protocol.onStopRequested()
      await expect(joining).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
    })
  })

  describe('A poll-interval overrun', () => {
    it('sends the leave epoch now, keeps the offsets alone, and goes STALE until the next join', async () => {
      const commitBeforeRelease = jest.fn(async () => {})
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          heartbeatResponse({ memberEpoch: 8 }),
        ],
        commitBeforeRelease,
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      h.manager.stop()

      await h.protocol.leaveForPollExpiry()

      expect(h.sent[h.sent.length - 1].memberEpoch).toEqual(-1)
      expect(commitBeforeRelease).not.toHaveBeenCalled()
      expect(h.rebalanceCoordinator.emitPartitions).not.toHaveBeenCalled()
      expect(h.protocol.state).toEqual(STALE)
      expect(h.manager.handle).toBeNull()

      await h.protocol.heartbeat({ interval: 0 })
      const sentWhileStale = h.sent.length

      h.protocol.resetGeneration({ clearMemberId: true })
      h.protocol.setOwned([])
      const rejoined = await await startStep(h)

      expect(h.sent).toHaveLength(sentWhileStale + 1)
      expect(h.protocol.memberEpoch).toEqual(8)
      expect(rejoined.assignment).toEqual({})
    })
  })

  describe('The heartbeat interval', () => {
    it('is a bounded default before the first response and the server value after it', async () => {
      const h = build({ answers: [heartbeatResponse({ heartbeatIntervalMs: 500 })] })

      expect(h.protocol.heartbeatInterval()).toEqual(1000)

      await await startStep(h)

      expect(h.protocol.heartbeatInterval()).toEqual(500)
    })
  })

  describe('The generation alias', () => {
    it('reports the member epoch, refuses to be set, and stamps what a heartbeat was sent under', async () => {
      const h = build({
        answers: [heartbeatResponse({ memberEpoch: 6 }), createErrorFromCode(110)],
      })
      await await startStep(h)

      expect(h.protocol.generationId).toEqual(6)
      h.protocol.generationId = 99
      expect(h.protocol.generationId).toEqual(6)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      const { sentGeneration } = require('../rebalanceErrorContract')
      expect(sentGeneration(h.manager.errors[0])).toEqual(6)
    })
  })

  describe('ResetGeneration', () => {
    it('zeroes the epoch and clears the target, and keeps the member id whatever it is told', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 6, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
        ],
      })
      await await startStep(h)
      const memberId = h.protocol.memberId

      h.protocol.resetGeneration({ clearMemberId: true })

      expect(h.protocol.memberId).toEqual(memberId)
      expect(h.protocol.memberEpoch).toEqual(0)
      expect(h.protocol.needsReconciliation()).toBe(false)
      expect(h.protocol.owned()).toEqual([])
    })
  })

  describe('What the round reports', () => {
    it('emits one HEARTBEAT per request, under the epoch the request carried', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 6 })] })

      await await startStep(h)

      expect(h.heartbeats).toEqual([
        {
          groupId: GROUP_ID,
          memberId: h.protocol.memberId,
          groupGenerationId: 0,
          groupInstanceId: null,
        },
      ])
    })

    it('answers the round with the values a KIP-848 join reports', async () => {
      const h = build({ serverAssignor: 'uniform' })

      expect(h.protocol.kind).toEqual('consumer')
      expect(h.protocol.isLeader()).toBe(false)
      expect(h.protocol.leaderId).toBeNull()
      expect(h.protocol.members).toBeNull()
      expect(h.protocol.rebalanceProtocol()).toEqual(COOPERATIVE)
      expect(h.protocol.groupProtocol).toEqual('uniform')
      expect(build().protocol.groupProtocol).toEqual('')
    })

    it('reports the difference between what it owns and what it was assigned', async () => {
      const h = build()
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [2, 0, 1] }])

      expect(h.protocol.diffAssignment([{ topic: 'topic-a', partitions: [1, 4, 3] }])).toEqual({
        added: [{ topic: 'topic-a', partitions: [3, 4] }],
        revoked: [{ topic: 'topic-a', partitions: [0, 2] }],
      })

      h.protocol.narrowOwned([{ topic: 'topic-a', partitions: [1] }])
      expect(h.protocol.owned()).toEqual([{ topic: 'topic-a', partitions: [1] }])
    })

    it('answers the assigner hooks without an assigner to answer for', async () => {
      const h = build()

      expect(h.protocol.warnIfStaticMembershipInactive()).toBeUndefined()
      expect(h.protocol.notifyAssignment()).toBeUndefined()
      expect(h.protocol.clearAssignerState()).toBeUndefined()
    })
  })

  describe('The join arms an unarmed chain', () => {
    it('sends exactly one heartbeat before anything else started the manager', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 2 })] })
      expect(h.manager.handle).toBeNull()

      const pending = h.protocol.rebalance(context())
      expect(h.manager.handle).not.toBeNull()
      const armedChain = h.manager.chainId

      await h.manager.tick()
      await pending

      h.manager.ensureStarted()
      expect(h.manager.chainId).toEqual(armedChain)
      expect(h.sent).toHaveLength(1)
    })
  })

  describe('Many requestTick calls during one in-flight heartbeat', () => {
    it('leave exactly one armed chain, and stopping afterwards leaves none', async () => {
      const gate = { resolve: null }
      const held = new Promise((resolve) => {
        gate.resolve = resolve
      })
      const h = build({
        answers: [
          async () => {
            await held
            return heartbeatResponse({ memberEpoch: 2 })
          },
        ],
      })

      const pending = h.protocol.rebalance(context())
      const tick = h.manager.tick()
      await Promise.resolve()

      for (let i = 0; i < 10; i++) h.manager.requestTick()
      expect(h.manager.tickRequested).toBe(true)

      gate.resolve()
      await tick
      await pending

      expect(h.manager.handle).not.toBeNull()
      expect(h.manager.chainId).toEqual(2)

      h.manager.stop()
      expect(h.manager.handle).toBeNull()
    })
  })

  describe('The user heartbeat stays interval-gated', () => {
    it('sends one request for a thousand calls inside one interval', async () => {
      const h = build({
        answers: [heartbeatResponse({ heartbeatIntervalMs: 5000, memberEpoch: 2 })],
      })
      await await startStep(h)
      const afterJoin = h.sent.length

      await Promise.all(
        Array.from({ length: 1000 }, () => h.protocol.heartbeat({ interval: 3000 }))
      )

      expect(h.sent).toHaveLength(afterJoin)

      jest.advanceTimersByTime(5000)
      await h.protocol.heartbeat({ interval: 3000 })

      expect(h.sent).toHaveLength(afterJoin + 1)
    })

    it('still sends one request per interval after a backoff has been armed and taken', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3 }),
          new KafkaJSConnectionError('no route'),
          createErrorFromCode(29),
        ],
      })
      await await startStep(h)

      // One failure that arms a backoff ...
      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      expect(h.manager.errors[0].message).toEqual('no route')

      // ... then a code designed to recur forever without touching the backoff. The elapsed
      // backoff owes exactly ONE request; every call after it is the interval gate's again.
      jest.advanceTimersByTime(1000)
      const before = h.sent.length

      for (let i = 0; i < 4; i++) {
        await h.protocol.heartbeat({ interval: 5000 }).catch(() => {})
      }

      expect(h.sent).toHaveLength(before + 1)

      jest.advanceTimersByTime(5000)
      await h.protocol.heartbeat({ interval: 5000 }).catch(() => {})

      expect(h.sent).toHaveLength(before + 2)
    })
  })

  describe('A stop racing a poll-expiry leave', () => {
    const gated = () => {
      const gate = { resolve: null }
      const held = new Promise((resolve) => {
        gate.resolve = resolve
      })
      return {
        gate,
        answer: async () => {
          await held
          return heartbeatResponse({ memberEpoch: 4 })
        },
      }
    }

    it('joins the leave already on the wire instead of walking the machine a second time', async () => {
      const { gate, answer } = gated()
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          answer,
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      h.manager.stop()

      const expiry = h.protocol.leaveForPollExpiry()
      expect(h.protocol.state).toEqual(LEAVING)

      // What the runner does next when the user disconnects mid-overrun. A second walk would raise
      // an illegal transition out of LEAVING, which the shutdown path swallows - a clean-looking
      // stop over a discarded invariant violation.
      const stopping = h.protocol.leave()

      gate.resolve()
      await expect(expiry).resolves.toBeUndefined()
      await expect(stopping).resolves.toBeUndefined()

      expect(h.sent.filter(({ memberEpoch }) => memberEpoch < 0)).toHaveLength(1)
      expect(h.protocol.state).toEqual(STALE)
    })

    it('joins it from the poll-expiry side too', async () => {
      const { gate, answer } = gated()
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 }), answer] })
      await await startStep(h)
      h.manager.stop()

      // `leave()` is still committing what it owns at this point, which is the other half of the
      // guard: PREPARE_LEAVING is as much "a leave is under way" as LEAVING is.
      const stopping = h.protocol.leave()
      expect(h.protocol.state).toEqual(PREPARE_LEAVING)

      const expiry = h.protocol.leaveForPollExpiry()

      gate.resolve()
      await expect(stopping).resolves.toBeUndefined()
      await expect(expiry).resolves.toBeUndefined()

      expect(h.sent.filter(({ memberEpoch }) => memberEpoch < 0)).toHaveLength(1)
      expect(h.protocol.state).toEqual(UNSUBSCRIBED)
    })
  })

  describe('Leaving from every state', () => {
    it('sends nothing and throws nothing from the states that are not in the group', async () => {
      for (const { drive, endsIn } of [
        { drive: async (h) => h.protocol.resetGeneration(), endsIn: UNSUBSCRIBED },
        {
          drive: async (h) => {
            await await startStep(h)
            h.protocol.onMembershipError(createErrorFromCode(110))
            // The runner stops the manager before it leaves; without this the shared assertion
            // below would be asserting that nobody armed a manager rather than that the leave
            // rearmed none.
            h.manager.stop()
          },
          // A fenced member is walked out of the group rather than left where it is: it has
          // nothing to send and nothing to release.
          endsIn: UNSUBSCRIBED,
        },
        {
          drive: async (h) => {
            await await startStep(h)
            h.manager.stop()
            await h.protocol.leaveForPollExpiry()
          },
          endsIn: STALE,
        },
      ]) {
        const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })
        await drive(h)
        const before = h.sent.length

        await expect(h.protocol.leave()).resolves.toBeUndefined()

        expect(h.sent).toHaveLength(before)
        expect(h.manager.handle).toBeNull()
        // The leave sends nothing from these states and never walks the machine into
        // PREPARE_LEAVING from a state the table does not allow it from.
        expect(h.protocol.state).toEqual(endsIn)
      }
    })

    it('returns the in-flight leave to a second caller', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })
      await await startStep(h)
      h.manager.stop()

      const first = h.protocol.leave()
      const second = h.protocol.leave()

      await Promise.all([first, second])
      expect(h.sent.filter(({ memberEpoch }) => memberEpoch < 0)).toHaveLength(1)
    })
  })

  describe('A fencing at a live epoch is not mistaken for stale news', () => {
    it('keeps the epoch until the rejoin consumes it, and clears the target', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 7, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          createErrorFromCode(110),
          heartbeatResponse({ memberEpoch: 11 }),
        ],
      })
      await await startStep(h)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(h.protocol.memberEpoch).toEqual(7)
      expect(h.protocol.needsReconciliation()).toBe(false)

      const pending = h.protocol.rebalance(context())
      expect(h.protocol.memberEpoch).toEqual(0)
      await h.manager.tick()
      const result = await pending

      expect(h.protocol.memberEpoch).toEqual(11)
      expect(result.assignment).toEqual({})
    })
  })

  describe('A fencing raised on another path recovers the same way', () => {
    it('rejoins after onMembershipError, from both the commit and the fetch shapes', async () => {
      for (const code of [25, 110]) {
        const h = build({
          answers: [heartbeatResponse({ memberEpoch: 5 }), heartbeatResponse({ memberEpoch: 6 })],
        })
        await await startStep(h)

        h.protocol.onMembershipError(createErrorFromCode(code))
        expect(h.protocol.state).toEqual(FENCED)

        await await startStep(h)
        expect(h.protocol.memberEpoch).toEqual(6)
      }
    })
  })

  describe('Forgetting the owned partitions outside a reconciliation', () => {
    it('assigns and nothing else, from FENCED and from STALE', async () => {
      for (const drive of [
        async (h) => {
          await await startStep(h)
          h.protocol.onMembershipError(createErrorFromCode(110))
        },
        async (h) => {
          await await startStep(h)
          h.manager.stop()
          await h.protocol.leaveForPollExpiry()
        },
      ]) {
        const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })
        await drive(h)
        const state = h.protocol.state
        h.manager.calls.length = 0

        expect(() => h.protocol.setOwned([])).not.toThrow()

        expect(h.protocol.state).toEqual(state)
        expect(h.manager.calls).not.toContain('requestTick')
      }
    })
  })

  describe('Reconciliation is driven from live state', () => {
    it('reports that another step is owed after an ack that did not converge', async () => {
      const h = build({
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) })],
      })
      await await startStep(h)

      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()

      expect(h.protocol.state).toEqual(RECONCILING)
      expect(h.protocol.needsReconciliation()).toBe(true)

      const next = await h.protocol.rebalance(context())
      expect(next.assignment).toEqual({ 'topic-a': [0, 1] })
    })
  })

  describe('The join deferred is a latch for nobody', () => {
    it('re-awaits the pending attempt rather than reporting an empty join', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 4 })] })

      const first = h.protocol.rebalance(context())
      const second = h.protocol.rebalance(context())
      await h.manager.tick()

      expect((await first).assignment).toEqual({})
      expect((await second).assignment).toEqual({})
      expect(h.sent).toHaveLength(1)
    })

    it('re-arms after the deadline, re-sends a joining heartbeat and still joins', async () => {
      const h = build({
        answers: [createErrorFromCode(14), heartbeatResponse({ memberEpoch: 4 })],
        rebalanceTimeout: 10000,
      })

      const timedOut = h.protocol.rebalance(context())
      await h.manager.tick()
      jest.advanceTimersByTime(10000)

      await expect(timedOut).rejects.toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
      expect(h.protocol.state).toEqual(JOINING)

      const retried = h.protocol.rebalance(context())
      await h.manager.tick()

      expect(await retried).toEqual({ assignment: {}, userData: null })
      expect(h.protocol.memberEpoch).toEqual(4)
    })

    it('rejects and clears on a stop request, so a later success is not swallowed', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 4 })] })

      const stopped = h.protocol.rebalance(context())
      h.protocol.onStopRequested()

      await expect(stopped).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })

      const retried = h.protocol.rebalance(context())
      await h.manager.tick()
      expect(await retried).toEqual({ assignment: {}, userData: null })
    })
  })

  describe('The member id setter is inert', () => {
    it('ignores every assignment, and says so at debug', async () => {
      const h = build()
      const memberId = h.protocol.memberId
      h.logged.debug.mockClear()

      h.protocol.memberId = null
      h.protocol.memberId = 'someone-else'

      expect(h.protocol.memberId).toEqual(memberId)
      expect(h.logged.debug).toHaveBeenCalledTimes(2)
      expect(h.logged.debug.mock.calls[0][0]).toMatch(/member id/i)
      expect(h.logged.debug.mock.calls[1][0]).toMatch(/member id/i)
    })

    it('ignores a generation assignment the same way, and says so at debug', async () => {
      const h = build()
      const epoch = h.protocol.memberEpoch
      h.logged.debug.mockClear()

      h.protocol.generationId = 99

      expect(h.protocol.generationId).toEqual(epoch)
      expect(h.logged.debug).toHaveBeenCalledTimes(1)
      expect(h.logged.debug.mock.calls[0][0]).toMatch(/generation/i)
    })
  })

  describe('A fatal membership error is fatal to the predicate too', () => {
    test.each([[81], [111], [112], [128]])('code %i answers the fatal predicate', (code) => {
      const { isFatalMembership } = require('../../errors')
      expect(isFatalMembership(createErrorFromCode(code))).toBe(true)
    })

    it('marks the unsupported-protocol error as fatal membership as well', async () => {
      const { isFatalMembership } = require('../../errors')
      const h = build({ answers: [createErrorFromCode(35)] })

      await expect(startStep(h).then((pending) => pending)).rejects.toThrow()

      expect(isFatalMembership(h.manager.errors[0] || h.protocol.fatalError)).toBe(true)
    })

    it('treats a cluster that does not advertise the API at all as fatal, not as a transport failure', async () => {
      const { isFatalMembership, KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
      const apiKeys = require('../../protocol/requests/apiKeys')
      // What version negotiation raises before a request is framed: a non-retriable error with no
      // protocol code. Read as transport, it would back off and retry for the life of the process.
      const unsupported = new KafkaJSServerDoesNotSupportApiKey(
        'The Kafka server does not support the requested API version',
        { apiKey: apiKeys.ConsumerGroupHeartbeat, apiName: 'ConsumerGroupHeartbeat' }
      )
      const h = build({ answers: [unsupported] })

      await expect(startStep(h).then((pending) => pending)).rejects.toThrow(
        /does not support the new CONSUMER group protocol/
      )

      expect(h.protocol.state).toEqual(FATAL)
      expect(isFatalMembership(h.protocol.fatalError)).toBe(true)
      // The coordinator is kept and no backoff was taken: this is not a connection problem.
      expect(h.protocol.coordinator).not.toBeNull()
    })

    it('still treats an unsupported OTHER api as a transport failure', async () => {
      const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
      const apiKeys = require('../../protocol/requests/apiKeys')
      const unsupported = new KafkaJSServerDoesNotSupportApiKey('nope', {
        apiKey: apiKeys.OffsetFetch,
        apiName: 'OffsetFetch',
      })
      const h = build({ answers: [unsupported] })

      // The join is NOT failed here: a transport failure is retried, so the step stays pending -
      // which is why the step is driven inline: awaiting `startStep` would unwrap the pending step
      // it returns and wait for a join that is never going to happen.
      const pending = h.protocol.rebalance(context())
      pending.catch(() => {})
      await h.manager.tick()

      expect(h.manager.errors[0].message).toEqual('nope')
      expect(h.protocol.state).toEqual(JOINING)
      expect(h.protocol.coordinator).toBeNull()
    })
  })

  describe('A fenced member rejoins', () => {
    it('takes the join path on the state, although the epoch is still the fenced one', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 7, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          createErrorFromCode(110),
          heartbeatResponse({ memberEpoch: 12, assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      expect(h.protocol.state).toEqual(FENCED)
      expect(h.protocol.memberEpoch).toEqual(7)

      h.protocol.setOwned([])
      const result = await await startStep(h)

      // The negative case, made visible: an epoch-only gate would have skipped the join and
      // reported an empty assignment for a member that is still fenced.
      expect(h.sent[h.sent.length - 1].memberEpoch).toEqual(0)
      expect(result.assignment).toEqual({ 'topic-a': [0, 1] })
      expect(h.protocol.state).toEqual(RECONCILING)
    })
  })

  describe('The join and the ack are not swallowed by the interval gate', () => {
    it('sends both immediately although the interval is far longer than the test', async () => {
      const h = build({
        answers: [
          heartbeatResponse({
            memberEpoch: 3,
            heartbeatIntervalMs: 3600000,
            assignment: assignmentOf([[TOPIC_ID_A, [0]]]),
          }),
        ],
      })

      await await startStep(h)
      expect(h.sent).toHaveLength(1)

      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()

      expect(h.sent).toHaveLength(2)
      expect(h.protocol.state).toEqual(STABLE)

      await Promise.all(
        Array.from({ length: 1000 }, () => h.protocol.heartbeat({ interval: 3000 }))
      )
      expect(h.sent).toHaveLength(2)
    })
  })

  describe('The immediate send is bounded', () => {
    it('sends one heartbeat for a thousand calls while acknowledging', async () => {
      const h = build({
        answers: [
          heartbeatResponse({
            memberEpoch: 3,
            heartbeatIntervalMs: 3600000,
            assignment: assignmentOf([[TOPIC_ID_A, [0]]]),
          }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])

      await Promise.all(Array.from({ length: 1000 }, () => h.protocol.heartbeat({ interval: 0 })))

      expect(h.sent).toHaveLength(2)
    })

    it('does not fire while a backoff is armed', async () => {
      jest.spyOn(Math, 'random').mockReturnValue(0.5)
      const h = build({
        answers: [heartbeatResponse({ memberEpoch: 3 }), createErrorFromCode(14)],
        retry: { initialRetryTime: 300, maxRetryTime: 30000, multiplier: 2, factor: 0.2 },
      })
      await await startStep(h)
      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      const afterBackoff = h.sent.length

      await h.protocol.heartbeat({ interval: 0 })

      expect(h.sent).toHaveLength(afterBackoff)
      Math.random.mockRestore()
    })
  })

  describe('A topic id that never resolves', () => {
    it('serves what it can, keeps heartbeating and schedules no round for the rest', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        answers: [
          heartbeatResponse({
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
        metadataMaxAge: 1000,
      })

      const result = await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()

      expect(result.assignment).toEqual({ 'topic-a': [0] })
      expect(h.protocol.needsReconciliation()).toBe(false)
      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)

      for (let i = 0; i < 5; i++) {
        jest.advanceTimersByTime(5000)
        await h.manager.tick()
      }

      expect(h.protocol.needsReconciliation()).toBe(false)
      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
      expect(h.sent.length).toBeGreaterThan(2)
    })

    it('picks the topic up once it resolves', async () => {
      const names = { [TOPIC_ID_A]: 'topic-a' }
      const h = build({
        names,
        answers: [
          heartbeatResponse({
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
        metadataMaxAge: 1000,
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()
      h.rebalanceCoordinator.requestRejoin.mockClear()

      names[TOPIC_ID_B] = 'topic-b'
      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(h.protocol.needsReconciliation()).toBe(true)
      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
      expect((await h.protocol.rebalance(context())).assignment).toEqual({
        'topic-a': [0],
        'topic-b': [0],
      })
    })
  })

  describe('Acking an unchanged subset', () => {
    it('adopts the bumped local epoch and re-sends the same partitions', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        answers: [
          heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          heartbeatResponse({
            memberEpoch: 2,
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()
      expect(h.protocol.state).toEqual(STABLE)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      const result = await h.protocol.rebalance(context())
      expect(result.assignment).toEqual({ 'topic-a': [0] })

      // The step returned the same subset the member already owns, so the reconciliation completes
      // inside the step: the target's local epoch is adopted and the member stays where it is.
      // Asserting ACKNOWLEDGING here would be asserting a round that never settles.
      expect(h.protocol.state).toEqual(STABLE)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      expect(partitionsSentOn(h.sent[h.sent.length - 1])).toEqual([[TOPIC_ID_A, [0]]])
    })

    it('acknowledges what the apply kept, not what the step returned', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a', [TOPIC_ID_B]: 'topic-b' },
        answers: [
          heartbeatResponse({
            assignment: assignmentOf([
              [TOPIC_ID_A, [0, 1]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
      })

      const result = await await startStep(h)
      expect(result.assignment).toEqual({ 'topic-a': [0, 1], 'topic-b': [0] })

      // The apply legitimately keeps less than it was given: an unsubscribed topic is stripped
      // before setOwned, and one partition of the other topic is not applied either.
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])

      await h.manager.tick()

      expect(partitionsSentOn(h.sent[h.sent.length - 1])).toEqual([[TOPIC_ID_A, [0]]])
      expect(h.protocol.owned()).toEqual([{ topic: 'topic-a', partitions: [0] }])
    })

    it('acknowledges an empty set when the apply kept nothing', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0]]]) })],
      })
      await await startStep(h)

      h.protocol.setOwned([])

      await h.manager.tick()

      expect(partitionsSentOn(h.sent[h.sent.length - 1])).toEqual([])
    })

    it('adopts the epoch without transitioning when it is not reconciling', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        answers: [
          heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          heartbeatResponse({
            memberEpoch: 2,
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      await h.manager.tick()
      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      const state = h.protocol.state

      await h.protocol.rebalance(context())

      expect(h.protocol.state).toEqual(state)
    })
  })

  describe('A stop landing inside the join', () => {
    it('settles the step, reports the stop, and leaves nothing armed', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 4 })] })

      const pending = h.protocol.rebalance(context())
      h.protocol.onStopRequested()
      h.manager.stop()

      await expect(pending).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
      expect(h.manager.handle).toBeNull()
    })
  })

  describe('A superseded chain does not send', () => {
    it('rejects the callback whose chain was replaced', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 2 })] })

      const pending = h.protocol.rebalance(context())
      h.manager.stop()
      h.manager.ensureStarted()

      const stale = h.manager.chainId - 1
      expect(stale).not.toEqual(h.manager.chainId)

      await h.manager.tick()
      await pending

      expect(h.sent).toHaveLength(1)
    })
  })

  describe('A stop between a tick request and its settlement', () => {
    it('arms nothing on the stopped manager', async () => {
      const gate = { resolve: null }
      const held = new Promise((resolve) => {
        gate.resolve = resolve
      })
      const h = build({
        answers: [
          async () => {
            await held
            return heartbeatResponse({ memberEpoch: 2 })
          },
        ],
      })

      const pending = h.protocol.rebalance(context())
      const tick = h.manager.tick()
      await Promise.resolve()

      h.manager.requestTick()
      h.manager.stop()

      gate.resolve()
      await tick
      await pending

      expect(h.manager.handle).toBeNull()
    })
  })

  describe('A fencing while joining', () => {
    it('rejects the step with the fencing, and the retried step joins', async () => {
      const h = build({
        answers: [createErrorFromCode(110), heartbeatResponse({ memberEpoch: 3 })],
      })

      const fenced = h.protocol.rebalance(context())
      fenced.catch(() => {})
      await h.manager.tick()

      await expect(fenced).rejects.toMatchObject({ code: 110 })
      expect(h.protocol.state).toEqual(FENCED)

      const retried = await await startStep(h)

      expect(retried).toEqual({ assignment: {}, userData: null })
      expect(h.protocol.memberEpoch).toEqual(3)
    })

    it('treats a second, redundant recovery as a no-op', async () => {
      const h = build({
        answers: [createErrorFromCode(25), heartbeatResponse({ memberEpoch: 3 })],
      })
      const fenced = h.protocol.rebalance(context())
      fenced.catch(() => {})
      await h.manager.tick()
      await expect(fenced).rejects.toMatchObject({ code: 25 })
      await await startStep(h)

      h.protocol.setOwned([])
      const redundant = await await startStep(h)

      expect(redundant).toEqual({ assignment: {}, userData: null })
      expect(h.protocol.memberEpoch).toEqual(3)
    })
  })

  describe('the ack consumes a tick request raised inside an in-flight heartbeat', () => {
    it('leaves one chain, and the ack carries the owned set', async () => {
      const gate = { resolve: null }
      const held = new Promise((resolve) => {
        gate.resolve = resolve
      })
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 2, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          async () => {
            await held
            return heartbeatResponse({ memberEpoch: 3 })
          },
        ],
      })
      await await startStep(h)

      jest.advanceTimersByTime(5000)
      const inFlight = h.manager.tick()
      await Promise.resolve()

      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      expect(h.manager.tickRequested).toBe(true)

      gate.resolve()
      await inFlight

      expect(h.manager.chainId).toEqual(2)
      await h.manager.tick()
      expect(partitionsSentOn(h.sent[h.sent.length - 1])).toEqual([[TOPIC_ID_A, [0]]])
    })
  })

  describe('the leave never touches the heartbeat manager', () => {
    it('sends the leave itself on a stopped manager', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })
      await await startStep(h)
      h.manager.stop()
      h.manager.calls.length = 0

      await h.protocol.leave()

      expect(h.manager.calls).toEqual([])
      expect(h.manager.handle).toBeNull()
      expect(h.sent[h.sent.length - 1].memberEpoch).toEqual(-1)
    })

    it('completes the leave without ever looking for a coordinator', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })
      await await startStep(h)
      h.manager.stop()
      h.protocol.coordinator = null
      // A never-settling lookup, which is what the cluster's retrier looks like from here: it
      // retries for tens of seconds. A rejecting double would reject instantly and would pass
      // even if the leave awaited the lookup, so it proves nothing about the wait.
      h.cluster.findGroupCoordinator.mockImplementation(() => new Promise(() => {}))
      h.cluster.findGroupCoordinator.mockClear()
      const before = h.sent.length

      await expect(h.protocol.leave()).resolves.toBeUndefined()

      expect(h.cluster.findGroupCoordinator).not.toHaveBeenCalled()
      expect(h.sent).toHaveLength(before)
      expect(h.protocol.state).toEqual(UNSUBSCRIBED)
    })

    it('leaves the same way when the coordinator lookup would fail outright', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })
      await await startStep(h)
      h.manager.stop()
      h.protocol.coordinator = null
      h.cluster.findGroupCoordinator.mockRejectedValue(new KafkaJSConnectionError('nope'))

      await expect(h.protocol.leave()).resolves.toBeUndefined()

      expect(h.protocol.state).toEqual(UNSUBSCRIBED)
    })

    it('leaves anyway when the leave heartbeat itself fails', async () => {
      const h = build({
        answers: [heartbeatResponse({ memberEpoch: 3 }), createErrorFromCode(15)],
      })
      await await startStep(h)
      h.manager.stop()

      h.manager.calls.length = 0

      await expect(h.protocol.leave()).resolves.toBeUndefined()

      // The failure is swallowed rather than classified: a leaving member must not be moved into a
      // state it cannot complete the leave out of, and must not re-arm the stopped manager.
      expect(h.protocol.state).toEqual(UNSUBSCRIBED)
      expect(h.manager.calls).toEqual([])
      expect(h.manager.handle).toBeNull()
    })

    it('rejoins cleanly after a leave, rather than waiting on the abandoned join', async () => {
      const h = build({
        answers: [heartbeatResponse({ memberEpoch: 3 }), heartbeatResponse({ memberEpoch: 8 })],
      })
      await await startStep(h)
      h.manager.stop()
      await h.protocol.leave()
      h.manager.calls.length = 0

      // A stale pending deferred here would swallow the joining heartbeat: no immediate send, no
      // requestTick, and this round would hang until the PREVIOUS attempt's deadline.
      const rejoined = await await startStep(h)

      expect(h.manager.calls).toContain('requestTick')
      expect(h.protocol.memberEpoch).toEqual(8)
      expect(rejoined.assignment).toEqual({})
    })
  })

  describe('The fields one heartbeat carries', () => {
    it('sends the rack, the assignor and the pattern on the join, and none of them again', async () => {
      const h = build({
        rackId: 'rack-1',
        serverAssignor: 'uniform',
        subscribedTopicRegex: 'orders-.*',
        topics: [],
        answers: [heartbeatResponse({ memberEpoch: 2 })],
      })

      await await startStep(h)

      expect(h.sent[0]).toMatchObject({
        rackId: 'rack-1',
        serverAssignor: 'uniform',
        subscribedTopicRegex: 'orders-.*',
      })
      // A pattern subscription still says "no names" with an empty list, never with a null.
      expect(h.sent[0].subscribedTopicNames).toEqual([])

      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(h.sent[1].rackId).toBeUndefined()
      expect(h.sent[1].serverAssignor).toBeUndefined()
      expect(h.sent[1].subscribedTopicRegex).toBeUndefined()
      expect(h.sent[1].subscribedTopicNames).toBeUndefined()
    })

    it('names the static instance on every heartbeat', async () => {
      const h = build({
        groupInstanceId: 'instance-1',
        answers: [heartbeatResponse({ memberEpoch: 2 })],
      })

      await await startStep(h)
      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(h.sent.every(({ instanceId }) => instanceId === 'instance-1')).toBe(true)
    })
  })

  describe('The step acks a target the apply has already converged on', () => {
    it('acknowledges from RECONCILING without applying anything', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) }),
          heartbeatResponse({ memberEpoch: 2, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0, 1] }])
      await h.manager.tick()

      // The coordinator took a partition back, so the member is reconciling again.
      expect(h.protocol.state).toEqual(RECONCILING)

      // What the apply does first: it narrows before it widens, which here leaves the member
      // owning exactly what the coordinator wants.
      h.protocol.narrowOwned([{ topic: 'topic-a', partitions: [0] }])

      const result = await h.protocol.rebalance(context())

      expect(result).toEqual({ assignment: { 'topic-a': [0] }, userData: null })
      expect(h.protocol.state).toEqual(ACKNOWLEDGING)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      expect(partitionsSentOn(h.sent[h.sent.length - 1])).toEqual([[TOPIC_ID_A, [0]]])
    })
  })

  describe('An acknowledgement that failed still counts as generated', () => {
    it('goes back to RECONCILING and asks for another round', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0, 1]]]) }),
          createErrorFromCode(14),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      h.rebalanceCoordinator.requestRejoin.mockClear()

      await h.manager.tick()

      expect(h.manager.errors[0].code).toEqual(14)
      expect(h.protocol.state).toEqual(RECONCILING)
      expect(h.rebalanceCoordinator.requestRejoin).toHaveBeenCalledTimes(1)
    })

    it('sends the retry once the backoff elapses, and reports the join', async () => {
      jest.spyOn(Math, 'random').mockReturnValue(0.5)
      const h = build({
        answers: [createErrorFromCode(14), heartbeatResponse({ memberEpoch: 4 })],
        retry: { initialRetryTime: 300, maxRetryTime: 30000, multiplier: 2, factor: 0.2 },
      })

      const pending = h.protocol.rebalance(context())
      pending.catch(() => {})
      await h.manager.tick()
      expect(h.protocol.heartbeatInterval()).toEqual(300)

      jest.advanceTimersByTime(300)
      await h.manager.tick()

      expect(await pending).toEqual({ assignment: {}, userData: null })
      expect(h.protocol.heartbeatInterval()).toEqual(5000)
      Math.random.mockRestore()
    })
  })

  describe('The coordinator cannot be found', () => {
    it('backs off and raises the lookup failure rather than sending', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 2 })] })
      h.cluster.findGroupCoordinator.mockRejectedValue(new KafkaJSConnectionError('no route'))

      const pending = h.protocol.rebalance(context())
      pending.catch(() => {})
      await h.manager.tick()

      expect(h.manager.errors[0].message).toEqual('no route')
      expect(h.sent).toHaveLength(0)
      expect(h.protocol.heartbeatInterval()).toBeLessThan(5000)
    })
  })

  describe('A metadata refresh that fails', () => {
    it('is logged and swallowed, and the step still returns what it could resolve', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        answers: [
          heartbeatResponse({
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
        ],
      })
      h.cluster.refreshMetadataForTopicIds.mockRejectedValue(new KafkaJSConnectionError('no route'))

      const result = await await startStep(h)

      expect(result.assignment).toEqual({ 'topic-a': [0] })
      expect(h.logged.debug).toHaveBeenCalledWith(
        'Could not refresh metadata for the assigned topic ids',
        expect.objectContaining({ topicIds: [TOPIC_ID_B] })
      )
    })
  })

  describe('A group the coordinator does not know', () => {
    it('is fatal while the member believes it is in the group', async () => {
      const h = build({ answers: [createErrorFromCode(69)] })

      await expect(startStep(h).then((pending) => pending)).rejects.toMatchObject({ code: 69 })

      expect(h.protocol.state).toEqual(FATAL)
    })

    it('is ignored once the member has already left', async () => {
      const h = build({
        answers: [heartbeatResponse({ memberEpoch: 3 }), createErrorFromCode(69)],
      })
      await await startStep(h)
      h.manager.stop()
      await h.protocol.leave()
      expect(h.protocol.state).toEqual(UNSUBSCRIBED)

      await expect(h.protocol.sendHeartbeat({})).rejects.toMatchObject({ code: 69 })

      expect(h.protocol.state).toEqual(UNSUBSCRIBED)
    })
  })

  describe('An answer the table does not name', () => {
    it('is fatal rather than retried forever', async () => {
      const h = build({ answers: [createErrorFromCode(1)] })

      await expect(startStep(h).then((pending) => pending)).rejects.toThrow()

      expect(h.protocol.state).toEqual(FATAL)
    })

    it('reaches the state machine from another path too', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })
      await await startStep(h)

      h.protocol.onMembershipError(createErrorFromCode(112))

      expect(h.protocol.state).toEqual(FATAL)
    })
  })

  describe('A defect raised while handling a response', () => {
    it('is not mistaken for a transport failure', async () => {
      const { KafkaJSInvariantViolation } = require('../../errors')
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
        ],
      })
      jest.spyOn(h.protocol, 'evaluateTarget').mockImplementation(() => {
        throw new KafkaJSInvariantViolation('illegal member state transition')
      })

      const pending = h.protocol.rebalance(context())
      pending.catch(() => {})
      await h.manager.tick()

      expect(h.manager.errors[0].name).toEqual('KafkaJSInvariantViolation')
      // A bug that recurs on every heartbeat must not drop the coordinator and back off as though
      // the network were at fault.
      expect(h.protocol.coordinator).not.toBeNull()
      expect(h.protocol.heartbeatInterval()).toEqual(5000)
    })
  })

  describe('A response carrying no epoch', () => {
    it('is ignored, so no target is installed under it', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 0, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          heartbeatResponse({ memberEpoch: 5 }),
        ],
      })

      const pending = h.protocol.rebalance(context())
      pending.catch(() => {})
      await h.manager.tick()

      // Epoch 0 is what this member SENDS to join. Taken as a membership it would install a target
      // and move to RECONCILING, from where the next round's step-0 gate attempts an illegal rejoin.
      expect(h.protocol.state).toEqual(JOINING)
      expect(h.protocol.needsReconciliation()).toBe(false)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()

      expect(await pending).toEqual({ assignment: {}, userData: null })
      expect(h.protocol.memberEpoch).toEqual(5)
    })
  })

  describe('A commit that fails on the way out', () => {
    it('is logged and the member leaves anyway', async () => {
      const commitBeforeRelease = jest.fn(async () => {
        throw new Error('coordinator unreachable')
      })
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
        ],
        commitBeforeRelease,
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      h.manager.stop()

      await expect(h.protocol.leave()).resolves.toBeUndefined()

      expect(h.logged.warn).toHaveBeenCalledWith(
        'Commit before leaving the group failed; leaving anyway',
        expect.objectContaining({ error: 'coordinator unreachable' })
      )
      expect(h.protocol.state).toEqual(UNSUBSCRIBED)
    })
  })

  describe('A poll overrun landing inside the join', () => {
    it('abandons the attempt without failing it, and the member parks in STALE', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })

      const joining = h.protocol.rebalance(context())
      await h.protocol.leaveForPollExpiry()

      expect(h.protocol.state).toEqual(STALE)
      // Nothing was sent: the join never reached a coordinator, so neither can the leave.
      expect(h.sent).toHaveLength(0)
      await expect(joining).resolves.toEqual({ assignment: {}, userData: null })
    })
  })

  describe('Before the runner has attached anything', () => {
    it('joins, reconciles and asks nobody for a tick or a round', async () => {
      const h = build({
        attached: false,
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0]]]) })],
      })

      const pending = h.protocol.rebalance(context())
      // Nothing was armed, so the step is driven by hand - which is the window the guards exist
      // for: the membership is constructed before the runner builds either collaborator.
      await h.protocol.sendHeartbeat({ sendAllFields: true })

      expect(await pending).toEqual({ assignment: { 'topic-a': [0] }, userData: null })
      expect(h.manager.calls).toEqual([])
      expect(h.rebalanceCoordinator.requestRejoin).not.toHaveBeenCalled()
      expect(h.protocol.state).toEqual(RECONCILING)
    })
  })

  describe('A round that supplies no way to widen the subscription', () => {
    it('reconciles the assignment as it is', async () => {
      const h = build({
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0]]]) })],
      })

      const result = await await startStep(h, context({ growSubscription: undefined }))

      expect(result.assignment).toEqual({ 'topic-a': [0] })
    })
  })

  describe('A join the coordinator answers with an empty assignment', () => {
    it('is a converged member, not a reconciling one', async () => {
      const h = build({
        answers: [heartbeatResponse({ memberEpoch: 4, assignment: assignmentOf([]) })],
      })

      const result = await await startStep(h)

      expect(h.protocol.state).toEqual(STABLE)
      expect(result.assignment).toEqual({})
      expect(h.rebalanceCoordinator.requestRejoin).not.toHaveBeenCalled()
    })
  })

  describe('A poll overrun reported twice', () => {
    it('does nothing the second time', async () => {
      const h = build({ answers: [heartbeatResponse({ memberEpoch: 3 })] })
      await await startStep(h)
      h.manager.stop()
      await h.protocol.leaveForPollExpiry()
      const before = h.sent.length

      await expect(h.protocol.leaveForPollExpiry()).resolves.toBeUndefined()

      expect(h.sent).toHaveLength(before)
      expect(h.protocol.state).toEqual(STALE)
    })
  })

  describe('A topic flushed from metadata between the step and the apply', () => {
    it('still names it, so the acknowledgement carries the partitions that were applied', async () => {
      const names = { [TOPIC_ID_A]: 'topic-a' }
      const h = build({
        names,
        answers: [heartbeatResponse({ assignment: assignmentOf([[TOPIC_ID_A, [0]]]) })],
      })
      await await startStep(h)

      // What a metadata refresh that no longer mentions the topic looks like from here.
      delete names[TOPIC_ID_A]
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])

      await h.manager.tick()

      expect(partitionsSentOn(h.sent[h.sent.length - 1])).toEqual([[TOPIC_ID_A, [0]]])
    })
  })

  describe('A target that stops mentioning a topic id', () => {
    it('forgets what it knew about it rather than keeping a cache that only grows', async () => {
      const h = build({
        names: { [TOPIC_ID_A]: 'topic-a' },
        answers: [
          heartbeatResponse({
            assignment: assignmentOf([
              [TOPIC_ID_A, [0]],
              [TOPIC_ID_B, [0]],
            ]),
          }),
          heartbeatResponse({ memberEpoch: 2, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
        ],
      })
      await await startStep(h)
      expect(h.protocol.unresolvedTopicIds.has(TOPIC_ID_B)).toBe(true)

      jest.advanceTimersByTime(5000)
      await h.manager.tick()
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])

      expect(h.protocol.unresolvedTopicIds.size).toEqual(0)
      expect([...h.protocol.assignedTopicNames.keys()]).toEqual([TOPIC_ID_A])
    })
  })

  describe('a poll overrun while acknowledging', () => {
    it('leaves from ACKNOWLEDGING and comes back through a join', async () => {
      const h = build({
        answers: [
          heartbeatResponse({ memberEpoch: 3, assignment: assignmentOf([[TOPIC_ID_A, [0]]]) }),
          heartbeatResponse({ memberEpoch: 9 }),
        ],
      })
      await await startStep(h)
      h.protocol.setOwned([{ topic: 'topic-a', partitions: [0] }])
      expect(h.protocol.state).toEqual(ACKNOWLEDGING)
      h.manager.stop()

      await h.protocol.leaveForPollExpiry()
      expect(h.protocol.state).toEqual(STALE)

      h.protocol.resetGeneration({ clearMemberId: true })
      h.protocol.setOwned([])
      await await startStep(h)

      expect(h.protocol.memberEpoch).toEqual(9)
    })
  })
})
