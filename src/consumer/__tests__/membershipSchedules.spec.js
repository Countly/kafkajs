const ConsumerGroupProtocol = require('../groupProtocol/consumer')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { events } = require('../instrumentationEvents')
const { MEMBER_STATES } = require('../groupProtocol/memberState')
const { COOPERATIVE } = require('../groupProtocol/interface')
const { newLogger } = require('testHelpers')

const { JOINING } = MEMBER_STATES

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

const { createCoordinator, lifecycleCells } = require('../../../testHelpers/lifecycle')
const { stampGeneration } = require('../rebalanceErrorContract')

describe('Membership schedules in lifecycle cells', () => {
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  test('a late accepted consumer join counts once after its deadline and resets for another attempt', async () => {
    expect(lifecycleCells()).toContainEqual({
      event: 'poll-expiry',
      boundary: 'in-heartbeat',
      protocol: 'consumer',
      membership: 'dynamic',
    })
    jest.useFakeTimers()
    const { protocol } = build({ rebalanceTimeout: 10 })
    protocol.transitionTo(JOINING)
    const attempt = protocol.armFirstMembership()
    const failure = attempt.promise.catch((error) => error)
    await jest.advanceTimersByTimeAsync(10)
    expect(await failure).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
    protocol.onSuccess(heartbeatResponse({ memberEpoch: 1 }))
    protocol.onSuccess(heartbeatResponse({ memberEpoch: 1 }))
    expect(protocol.joinCount).toBe(1)
    const second = protocol.armFirstMembership()
    protocol.onSuccess(heartbeatResponse({ memberEpoch: 2 }))
    await second.promise
    expect(protocol.joinCount).toBe(2)
    protocol.resetGeneration()
    protocol.onSuccess(heartbeatResponse({ memberEpoch: 3 }))
    expect(protocol.joinCount).toBe(3)
    protocol.clearFirstMembership()
    expect(jest.getTimerCount()).toBe(0)
  })

  test.each(['classic', 'consumer'])(
    '%s cooperative revocation clears an already stale LOST',
    async (kind) => {
      expect(lifecycleCells()).toContainEqual({
        event: 'rejoin',
        boundary: 'pre-release-commit',
        protocol: kind === 'classic' ? 'classic-cooperative' : 'consumer',
        membership: 'dynamic',
      })
      const { coordinator, consumerGroup } = createCoordinator({ mode: COOPERATIVE })
      consumerGroup.kind = kind
      consumerGroup.joinCount = 2
      consumerGroup.generationId = 2
      const old = Object.assign(new Error('old loss'), { code: 22 })
      coordinator.verdict = { kind: 'lost', reason: 'old', error: stampGeneration(old, 1) }
      // The consumer protocol compares its membership identity before generation. This cell
      // supplies the completed-attempt classification directly, without a fabricated wire answer.
      const stale = jest
        .spyOn(coordinator, 'isStaleGenerationVerdict')
        .mockImplementation((verdict) => verdict.error === old)
      const round = jest.spyOn(coordinator, 'runRound').mockResolvedValue()
      coordinator.requestRejoin({
        kind: 'revoked',
        reason: 'assignment',
        error: new Error('current'),
      })
      expect(coordinator.verdict).toBeNull()
      expect(round).toHaveBeenCalledTimes(1)
      stale.mockRestore()
    }
  )

  test.each(['classic', 'consumer'])(
    '%s ignores a queued LOST after a completed replacement round',
    (kind) => {
      const { coordinator, consumerGroup } = createCoordinator({ mode: COOPERATIVE })
      consumerGroup.kind = kind
      consumerGroup.joinCount = 3
      coordinator.inFlight = Promise.resolve()
      coordinator.runRound('fenced join', 'lost')
      consumerGroup.joinCount = 4
      expect(coordinator.takePendingKind()).toBeNull()
      expect(coordinator.takeRejoinRequested()).toBeNull()
      coordinator.inFlight = null
      coordinator.runRound = jest.fn()
    }
  )
})

{
  const { Kafka } = require('../../../index')
  const Runner = require('../runner')
  const {
    createScriptedCluster,
    groupScript,
    consumerProtocolScript,
  } = require('../../../testHelpers/scriptedCluster')
  const { cooperativeSticky } = require('../assigners')
  const { TOPIC, prepareRecordBatches } = require('./fixtures/fetchHarness')
  const { stampOwnership } = require('../rebalanceErrorContract')
  const { pump } = require('./fixtures/lifecycleCase')

  /**
   * Runs a schedule against a public cooperative member and real broker responses.
   * @param {'classic'|'consumer'} kind membership protocol
   * @param {(member: object) => Promise<void>} work schedule and assertions
   * @returns {Promise<void>}
   */
  const withWireMember = async (kind, work) => {
    jest.useFakeTimers({ doNotFake: ['setImmediate'] })
    const baseline = jest.getTimerCount()
    const scripted = (kind === 'consumer' ? consumerProtocolScript : groupScript)(
      createScriptedCluster({
        nodes: [{ id: 0, host: 'member', port: 9092 }],
        profile: kind === 'consumer' ? 'consumer' : 'default',
      }),
      { batches: 0, heartbeatIntervalMs: 10000 }
    )
    const options =
      kind === 'consumer'
        ? { groupProtocol: 'consumer' }
        : { heartbeatInterval: 10000, partitionAssigners: [cooperativeSticky] }
    const consumer = new Kafka(
      scripted.kafkaOptions({ retry: { retries: 1, initialRetryTime: 1 } })
    ).consumer({ groupId: 'schedule-member', maxWaitTimeInMs: 20, ...options })
    const joins = []
    const removeJoin = consumer.on(consumer.events.GROUP_JOIN, (event) => joins.push(event))
    let runner
    const start = Runner.prototype.start
    const capture = jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
      runner = this
      return start.apply(this, args)
    })
    const gates = []
    const hold = (selector) => {
      const gate = scripted.schedule.hold(selector)
      gates.push(gate)
      return gate
    }
    const finish = async (promise, label) => {
      let done = false
      let failure
      const job = promise.then(
        () => {
          done = true
        },
        (error) => {
          done = true
          failure = error
        }
      )
      await pump(() => done, label)
      await job
      if (failure) throw failure
    }
    try {
      await prepareRecordBatches()
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await finish(consumer.run({ autoCommit: false, eachMessage: async () => {} }), 'public join')
      await pump(
        () => !runner.heartbeatManager.inFlight && !runner.rebalanceCoordinator.isRebalancing(),
        'stable member'
      )
      expect(
        scripted
          .requests()
          .some(({ api }) => api === (kind === 'consumer' ? 'ConsumerGroupHeartbeat' : 'SyncGroup'))
      ).toBe(true)
      await work({
        scripted,
        consumer,
        joins,
        runner,
        group: runner.consumerGroup,
        coordinator: runner.rebalanceCoordinator,
        hold,
        finish,
      })
      scripted.assertClean()
    } finally {
      for (const gate of gates) if (gate.settle) gate.release()
      try {
        await finish(consumer.disconnect(), 'member disconnect')
      } finally {
        removeJoin()
        capture.mockRestore()
      }
      expect(jest.getTimerCount()).toBe(baseline)
    }
  }

  describe('Membership schedules over decoded broker responses', () => {
    afterEach(() => {
      jest.useRealTimers()
      jest.restoreAllMocks()
    })

    test('late accepted consumer join counts after the actual answer crosses its deadline', async () => {
      await withWireMember('consumer', async ({ scripted, group, runner, hold, finish }) => {
        runner.heartbeatManager.stop()
        const protocol = new ConsumerGroupProtocol({
          cluster: group.cluster,
          groupId: 'late-member',
          rebalanceTimeout: 10,
          topics: [TOPIC],
          instrumentationEmitter: new InstrumentationEventEmitter(),
          logger: newLogger(),
        })
        protocol.transitionTo(JOINING)
        const deferred = protocol.armFirstMembership()
        const failed = deferred.promise.catch((error) => error)
        const gate = hold({
          api: 'ConsumerGroupHeartbeat',
          match: ({ fields }) => fields.groupId === 'late-member',
        })
        const answer = protocol.sendHeartbeat({ sendAllFields: true })
        await pump(() => gate.call != null, 'late membership request')
        await jest.advanceTimersByTimeAsync(10)
        expect(await failed).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
        expect(protocol.joinCount).toBe(0)
        gate.release()
        await finish(answer, 'late membership answer')
        expect(protocol.joinCount).toBe(1)
        await finish(protocol.sendHeartbeat({}), 'duplicate accepted membership')
        expect(protocol.joinCount).toBe(1)
        protocol.clearFirstMembership()
        expect(
          scripted
            .requests()
            .filter(
              ({ api, fields }) =>
                api === 'ConsumerGroupHeartbeat' && fields.groupId === 'late-member'
            )
        ).toHaveLength(2)
      })
    })

    test.each(['classic', 'consumer'])(
      '%s clears stored obsolete LOST before a real cooperative round',
      async (kind) => {
        await withWireMember(kind, async ({ runner, group, coordinator, finish }) => {
          const oldToken = group.ownership.token()
          const old = stampOwnership(new Error('obsolete membership'), oldToken)
          const first = group.joinCount
          if (kind === 'consumer')
            await finish(group.leaveForPollExpiry(), 'leave previous membership')
          else group.resetGenerationLocally()
          await finish(coordinator.runRound('replacement', 'revoked'), 'replacement membership')
          expect(group.joinCount).toBeGreaterThan(first)
          expect(group.ownership.token()).not.toBe(oldToken)
          coordinator.verdict = { kind: 'lost', reason: 'old', error: old }
          coordinator.requestRejoin({
            kind: 'revoked',
            reason: 'assignment',
            error: new Error('current assignment'),
          })
          expect(coordinator.verdict).toBeNull()
          await finish(coordinator.idle(), 'cooperative round')
          expect(runner.stopRequested).toBe(false)
          expect(coordinator.verdict).toBeNull()
        })
      }
    )

    test.each(['classic', 'consumer'])(
      '%s drops queued LOST only after the held replacement really joins',
      async (kind) => {
        await withWireMember(kind, async ({ group, coordinator, hold, finish, joins }) => {
          const first = group.joinCount
          const joinedBefore = joins.length
          if (kind === 'consumer')
            await finish(group.leaveForPollExpiry(), 'leave previous membership')
          else group.resetGenerationLocally()
          const gate = hold({ api: kind === 'consumer' ? 'ConsumerGroupHeartbeat' : 'JoinGroup' })
          const round = coordinator.runRound('replacement', 'revoked')
          await pump(() => gate.call != null, 'replacement request')
          expect(coordinator.runRound('old loss', 'lost')).toBe(round)
          expect(coordinator.pendingKind).toBe('lost')
          gate.release()
          await finish(round, 'accepted replacement')
          expect(group.joinCount).toBe(first + 1)
          expect(joins).toHaveLength(joinedBefore + 1)
          expect(coordinator.takePendingKind()).toBeNull()
          expect(coordinator.takeRejoinRequested()).toBeNull()
        })
      }
    )
  })
}
