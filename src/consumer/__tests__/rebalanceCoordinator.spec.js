const RebalanceCoordinator = require('../rebalanceCoordinator')
const Batch = require('../batch')
const PollTimer = require('../pollTimer')
const ConsumerGroup = require('../consumerGroup')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { events } = require('../instrumentationEvents')
const { COOPERATIVE, EAGER } = require('../groupProtocol/interface')
const { pollIntervalExceededError } = require('../rebalanceErrorContract')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger, waitFor } = require('testHelpers')

const ILLEGAL_GENERATION = 22
const UNKNOWN_MEMBER_ID = 25
const COORDINATOR_LOAD_IN_PROGRESS = 14
const COORDINATOR_NOT_AVAILABLE = 15
const NOT_COORDINATOR_FOR_GROUP = 16
const REBALANCE_IN_PROGRESS = 27
const FENCED_INSTANCE_ID = 82

const deferred = () => {
  let resolve, reject
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

const payloadsOf = (emitted, type) =>
  emitted.filter((event) => event.type === type).map(({ payload }) => payload)

const build = ({ mode = EAGER, assignment = [], deltas = [] } = {}) => {
  const calls = []

  const consumerGroup = {
    groupId: 'coordinator-group',
    memberId: 'member-1',
    generationId: 3,
    leaderId: 'member-1',
    groupInstanceId: null,
    groupProtocol: 'RoundRobinAssigner',
    groupMembership: {},
    isLeader: () => true,
    rebalanceProtocol: () => mode,
    assigned: jest.fn(() => assignment),
    joinAndSync: jest.fn(async () => {
      calls.push('joinAndSync')
      return { assignment: {} }
    }),
    // A QUEUE, not a constant: a non-empty `revoked` schedules a follow-up round, so a constant
    // delta chains rounds until the round guard rejects. `deltas` is shifted one per call and
    // falls back to an empty delta, which is what makes a follow-up round converge.
    applyRebalanceResult: jest.fn(async () => {
      calls.push('applyRebalanceResult')
      return { assignment, added: [], revoked: [], ...(deltas.shift() || {}) }
    }),
    discardUncommittedOffsets: jest.fn(() => calls.push('discard')),
    leave: jest.fn(async () => calls.push('leave')),
    resetGenerationLocally: jest.fn(() => calls.push('resetGenerationLocally')),
    clearStaleAssignmentPending: jest.fn(),
  }

  const fetchManager = {
    start: jest.fn(async () => {}),
    stop: jest.fn(async () => calls.push('fetchManager.stop')),
    getFetchers: jest.fn(() => []),
  }

  const heartbeatManager = {
    ensureStarted: jest.fn(() => calls.push('ensureStarted')),
    stop: jest.fn(() => calls.push('heartbeatManager.stop')),
  }

  const runner = { stopRequested: false, onCrash: jest.fn() }
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const emitted = []
  for (const name of [
    events.REBALANCING,
    events.GROUP_JOIN,
    events.PARTITIONS_ASSIGNED,
    events.PARTITIONS_REVOKED,
    events.PARTITIONS_LOST,
    events.POLL_INTERVAL_EXCEEDED,
  ]) {
    instrumentationEmitter.addListener(name, ({ type, payload }) => {
      calls.push(type)
      emitted.push({ type, payload })
    })
  }

  const coordinator = new RebalanceCoordinator({
    runner,
    consumerGroup,
    fetchManager,
    heartbeatManager,
    pollTimer: new PollTimer({ pollDeadline: 8000 }),
    instrumentationEmitter,
    logger: newLogger(),
    groupId: 'coordinator-group',
  })

  return {
    coordinator,
    consumerGroup,
    fetchManager,
    heartbeatManager,
    runner,
    instrumentationEmitter,
    calls,
    emitted,
  }
}

/**
 * Cases 5, 6, 7, 8, 12, 13 and 18 need a REAL `Runner` around the coordinator, so they assert
 * through `runner.rebalanceCoordinator` - a real `Runner` builds its own, and it is a different
 * object from `build()`'s.
 */
const realRunners = []
const createRealRunner = (options = {}) => {
  const Runner = require('../runner')
  const built = build(options)

  Object.assign(built.consumerGroup, {
    attach: jest.fn(),
    getNodeIds: () => [1],
    fetch: jest.fn(async () => []),
    heartbeat: jest.fn(async () => {}),
    hasSeekOffset: () => false,
    isPaused: () => false,
    commitOffsets: jest.fn(async () => built.calls.push('commitOffsets')),
    commitOffsetsIfNecessary: jest.fn(async () => {}),
    uncommittedOffsets: jest.fn(),
    clearAssignerState: jest.fn(),
    connect: jest.fn(async () => {}),
    isAssigned: jest.fn(() => true),
    resolveOffset: jest.fn(),
  })

  const runner = new Runner({
    logger: newLogger(),
    consumerGroup: built.consumerGroup,
    instrumentationEmitter: built.instrumentationEmitter,
    concurrency: 1,
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    eachBatch: async () => {},
    onCrash: jest.fn(),
  })

  // Replacing a collaborator field on an instance is stubbing, not a production accessor added
  // for a test. BOTH references matter: the Runner constructor passed its own collaborators into
  // the RebalanceCoordinator it built, so replacing only the runner's leaves the coordinator's
  // pointing at the real ones.
  // Kept before the swap below: the manager the Runner itself built is the only one carrying the
  // Runner's own `onPollExpired` closure, which is what the poll-deadline case has to drive.
  const realHeartbeatManager = runner.heartbeatManager
  realHeartbeatManager.stop()
  runner.fetchManager = built.fetchManager
  runner.rebalanceCoordinator.fetchManager = built.fetchManager
  runner.heartbeatManager = built.heartbeatManager
  runner.rebalanceCoordinator.heartbeatManager = built.heartbeatManager

  realRunners.push(runner)
  return { ...built, runner, realHeartbeatManager }
}

/** A real `ConsumerGroup`, for the one case whose subject is `recoverFromFetch` itself. */
const createRealGroup = ({ mode = COOPERATIVE } = {}) => {
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    refreshMetadataIfNecessary: async () => {},
    committedOffsets: () => ({}),
    findTopicPartitionMetadata: () => [],
  }

  const group = new ConsumerGroup({
    retry: { retries: 0 },
    cluster,
    groupId: 'real-group',
    topics: ['test-topic'],
    topicConfigurations: { 'test-topic': {} },
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
    assigners: [],
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 1048576,
    maxWaitTimeInMs: 100,
    autoCommit: false,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    isolationLevel: 0,
    rackId: '',
    checkCrcs: true,
    rebalanceProtocol: mode,
    metadataMaxAge: 300000,
    pendingTopicResets: new Map(),
  })

  return { group, cluster }
}

describe('Consumer > RebalanceCoordinator', () => {
  let unhandled

  const trackUnhandled = (reason) => unhandled.push(reason)

  beforeEach(() => {
    unhandled = []
    process.on('unhandledRejection', trackUnhandled)
  })

  afterEach(async () => {
    // Case 11 applies to every case, not only its own: nothing this component fires may reach the
    // process as an unobserved rejection.
    await new Promise((resolve) => setImmediate(resolve))
    process.removeListener('unhandledRejection', trackUnhandled)
    expect(unhandled).toEqual([])

    while (realRunners.length) {
      realRunners.pop().heartbeatManager.stop()
    }
  })

  describe('case 1: single-flight', () => {
    it('returns the running chain, starts no second one, and remembers the reason as a string', async () => {
      const { coordinator, consumerGroup } = build()
      const gate = deferred()
      consumerGroup.joinAndSync.mockImplementation(async () => {
        await gate.promise
        return { assignment: {} }
      })

      const first = coordinator.runRound('a', 'revoked')
      await Promise.resolve()

      const second = coordinator.runRound('b', 'revoked')

      expect(second).toBe(first)
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
      expect(coordinator.rejoinRequested).toEqual('b')
      expect(typeof coordinator.rejoinRequested).toEqual('string')

      gate.resolve()
      await first
    })
  })

  describe('case 2: merge precedence', () => {
    const verdict = (kind, reason) => ({ kind, reason, error: new Error(reason) })

    it('upgrades REVOKED to LOST, never downgrades, and collapses same kinds to the first', () => {
      const { coordinator } = build()
      const revoked = verdict('revoked', 'rebalance-in-progress')
      const lost = verdict('lost', 'lost')

      expect(coordinator.mergeVerdict(null, revoked)).toBe(revoked)
      expect(coordinator.mergeVerdict(revoked, lost)).toBe(lost)
      expect(coordinator.mergeVerdict(lost, revoked)).toBe(lost)
      expect(coordinator.mergeVerdict(revoked, verdict('revoked', 'coordinator-changed'))).toBe(
        revoked
      )
      expect(coordinator.mergeVerdict(lost, verdict('lost', 'poll-interval-exceeded'))).toBe(lost)
    })

    it('hands the merged verdict out once and leaves the slot null', () => {
      const { coordinator } = build()

      coordinator.requestRejoin(verdict('revoked', 'rebalance-in-progress'))
      coordinator.requestRejoin(verdict('lost', 'lost'))

      expect(coordinator.takeVerdict()).toMatchObject({ kind: 'lost', reason: 'lost' })
      expect(coordinator.takeVerdict()).toBeNull()
    })

    it('keeps the first mid-chain reason and still runs exactly one follow-up round', async () => {
      const { coordinator, consumerGroup, emitted } = build()
      const gate = deferred()
      consumerGroup.joinAndSync.mockImplementation(async () => {
        await gate.promise
        return { assignment: {} }
      })

      const chain = coordinator.runRound('first', 'revoked')
      await Promise.resolve()

      coordinator.runRound('second', 'revoked')
      coordinator.runRound('third', 'revoked')
      expect(coordinator.rejoinRequested).toEqual('second')

      gate.resolve()
      await chain

      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(2)
      expect(payloadsOf(emitted, events.REBALANCING).map(({ reason }) => reason)).toEqual([
        'first',
        'second',
      ])
      expect(coordinator.rejoinRequested).toBeNull()
    })
  })

  describe('case 3: follow-up rounds', () => {
    it('runs a second round for a cooperative revocation, with reason partitions-revoked', async () => {
      const { coordinator, consumerGroup, emitted } = build({
        mode: COOPERATIVE,
        deltas: [{ revoked: [{ topic: 't', partitions: [0] }] }],
      })

      await coordinator.runRound('rebalance-in-progress', 'revoked')

      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(2)
      expect(payloadsOf(emitted, events.REBALANCING).map(({ reason }) => reason)).toEqual([
        'rebalance-in-progress',
        'partitions-revoked',
      ])
    })

    it('runs once when nothing was revoked and nothing was requested', async () => {
      const { coordinator, consumerGroup, emitted } = build({ mode: COOPERATIVE })

      await coordinator.runRound('rebalance-in-progress', 'revoked')

      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
      expect(payloadsOf(emitted, events.REBALANCING)).toHaveLength(1)
      expect(coordinator.rejoinRequested).toBeNull()
    })

    it('drains rejoinRequested at the bottom of the loop, not in the finally', async () => {
      const { coordinator, consumerGroup } = build()
      const gate = deferred()
      consumerGroup.joinAndSync.mockImplementationOnce(async () => {
        await gate.promise
        return { assignment: {} }
      })

      const chain = coordinator.runRound('first', 'revoked')
      await Promise.resolve()
      coordinator.runRound('mid-chain', 'revoked')
      gate.resolve()
      await chain

      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(2)
      expect(coordinator.rejoinRequested).toBeNull()
    })
  })

  describe('case 4: the round guard', () => {
    // The guard is `++rounds > 20`, so twenty rounds run and the twenty-first is refused.
    it('lets a chain of twenty rounds through', async () => {
      const revoked = [{ topic: 't', partitions: [0] }]
      const { coordinator, consumerGroup } = build({
        mode: COOPERATIVE,
        deltas: Array.from({ length: 19 }, () => ({ revoked })),
      })

      await coordinator.runRound('rebalance-in-progress', 'revoked')

      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(20)
      expect(coordinator.rounds).toEqual(0)
    })

    it('refuses a chain that does not converge', async () => {
      const revoked = [{ topic: 't', partitions: [0] }]
      const { coordinator, consumerGroup } = build({
        mode: COOPERATIVE,
        deltas: Array.from({ length: 25 }, () => ({ revoked })),
      })

      await expect(coordinator.runRound('rebalance-in-progress', 'revoked')).rejects.toThrow(
        'did not converge in 20 rounds'
      )
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(20)
      expect(coordinator.rounds).toEqual(0)
      expect(coordinator.isRebalancing()).toBe(false)
    })

    it('counts rounds, not the attempts a join retries internally', async () => {
      const revoked = [{ topic: 't', partitions: [0] }]
      let attempts = 0
      const { coordinator, consumerGroup } = build({
        mode: COOPERATIVE,
        deltas: Array.from({ length: 19 }, () => ({ revoked })),
      })
      consumerGroup.joinAndSync.mockImplementation(async () => {
        attempts += 3
        return { assignment: {} }
      })

      await coordinator.runRound('rebalance-in-progress', 'revoked')

      expect(attempts).toEqual(60)
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(20)
    })
  })

  describe('case 5: stop() waits for the chain to go idle', () => {
    it('resolves idle() immediately when no chain is running', async () => {
      const { coordinator } = build()
      await expect(coordinator.idle()).resolves.toBeUndefined()
    })

    it('awaits idle() before stopping the fetch manager and before LeaveGroup', async () => {
      const { runner, consumerGroup, calls } = createRealRunner({ mode: COOPERATIVE })
      const gate = deferred()
      consumerGroup.joinAndSync.mockImplementation(async () => {
        calls.push('joinAndSync')
        await gate.promise
        return { assignment: {} }
      })

      runner.running = true
      runner.rebalanceCoordinator.runRound('rebalance-in-progress', 'revoked')
      await Promise.resolve()

      const stopping = runner.stop()
      gate.resolve()
      await stopping

      expect(calls.indexOf('applyRebalanceResult')).toBeLessThan(calls.indexOf('fetchManager.stop'))
      expect(calls.indexOf('fetchManager.stop')).toBeLessThan(calls.indexOf('leave'))
    })

    it('joins when only `running` is false, and refuses once stopRequested is set', async () => {
      const { coordinator, consumerGroup, runner } = build()

      await coordinator.runRound('reason', 'revoked')
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)

      runner.stopRequested = true
      await coordinator.runRound('reason', 'revoked')

      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
    })
  })

  describe('case 6: the verdict is consumed once after start() settles normally', () => {
    const driveOneVerdict = async (built, error) => {
      const { runner, consumerGroup, fetchManager, calls } = built
      consumerGroup.joinAndSync.mockImplementation(async () => {
        calls.push('joinAndSync')
        runner.running = false
        return { assignment: {} }
      })
      fetchManager.start.mockImplementation(async () => {
        runner.rebalanceCoordinator.requestRejoin(
          runner.rebalanceCoordinator.classify(error, 'fetch')
        )
      })

      runner.running = true
      runner.scheduleFetchManager()
      await waitFor(() => consumerGroup.joinAndSync.mock.calls.length > 0, { delay: 5 })
    }

    it('discards once for a LOST verdict, commits nothing, and joins once', async () => {
      const built = createRealRunner()
      await driveOneVerdict(built, createErrorFromCode(ILLEGAL_GENERATION))

      expect(payloadsOf(built.emitted, events.REBALANCING)).toHaveLength(1)
      expect(built.consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
      expect(built.consumerGroup.discardUncommittedOffsets).toHaveBeenCalledTimes(1)
      expect(built.consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(built.runner.rebalanceCoordinator.takeVerdict()).toBeNull()
    })

    it('commits once for a REVOKED verdict and discards nothing', async () => {
      const built = createRealRunner()
      await driveOneVerdict(built, createErrorFromCode(REBALANCE_IN_PROGRESS))

      expect(payloadsOf(built.emitted, events.REBALANCING)).toHaveLength(1)
      expect(built.consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
      expect(built.consumerGroup.commitOffsets).toHaveBeenCalledTimes(1)
      expect(built.consumerGroup.discardUncommittedOffsets).not.toHaveBeenCalled()
    })

    it('is the single site: heartbeat() itself neither discards nor commits', async () => {
      const { runner, consumerGroup } = createRealRunner()
      consumerGroup.heartbeat.mockRejectedValue(createErrorFromCode(ILLEGAL_GENERATION))

      await expect(runner.heartbeat()).rejects.toThrow()

      expect(consumerGroup.discardUncommittedOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
    })
  })

  describe('case 7: the verdict is merged and cleared when start() settles by throw', () => {
    it('produces one REBALANCING and one join for an error that both threw and sat in the slot', async () => {
      const { runner, consumerGroup, fetchManager, calls, emitted } = createRealRunner()
      const error = createErrorFromCode(REBALANCE_IN_PROGRESS)

      consumerGroup.joinAndSync.mockImplementation(async () => {
        calls.push('joinAndSync')
        runner.running = false
        return { assignment: {} }
      })
      fetchManager.start.mockImplementation(async () => {
        runner.rebalanceCoordinator.requestRejoin(
          runner.rebalanceCoordinator.classify(error, 'fetch')
        )
        throw error
      })

      runner.running = true
      runner.scheduleFetchManager()
      await waitFor(() => consumerGroup.joinAndSync.mock.calls.length > 0, { delay: 5 })

      expect(payloadsOf(emitted, events.REBALANCING)).toHaveLength(1)
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
      expect(runner.rebalanceCoordinator.takeVerdict()).toBeNull()
    })
  })

  describe('case 8: a cooperative revocation resolves the heartbeat callback', () => {
    it('resolves, stops no fetcher, and runs a round in the background', async () => {
      const { runner, consumerGroup, fetchManager } = createRealRunner({ mode: COOPERATIVE })
      consumerGroup.heartbeat.mockRejectedValue(createErrorFromCode(REBALANCE_IN_PROGRESS))

      await expect(runner.heartbeat()).resolves.toBeUndefined()
      await waitFor(() => consumerGroup.joinAndSync.mock.calls.length > 0, { delay: 5 })

      expect(fetchManager.stop).not.toHaveBeenCalled()
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
    })

    it('rethrows under eager and takes the abort path', async () => {
      const { runner, consumerGroup, fetchManager } = createRealRunner()
      consumerGroup.heartbeat.mockRejectedValue(createErrorFromCode(REBALANCE_IN_PROGRESS))

      await expect(runner.heartbeat()).rejects.toThrow()

      expect(fetchManager.stop).toHaveBeenCalledTimes(1)
      expect(runner.rebalanceCoordinator.takeVerdict()).toMatchObject({ kind: 'revoked' })
    })
  })

  describe('case 9: poll expiry, dynamic member', () => {
    it('warns, emits, stops heartbeats, leaves, resets locally, then requests a LOST rejoin', async () => {
      const { coordinator, consumerGroup, calls, emitted } = build()
      jest.spyOn(coordinator.logger, 'warn').mockImplementation(() => calls.push('warn'))

      await coordinator.onPollExpired({ elapsedMs: 9000, isStatic: false })

      expect(calls).toEqual([
        'warn',
        events.POLL_INTERVAL_EXCEEDED,
        'heartbeatManager.stop',
        'leave',
        'resetGenerationLocally',
        'fetchManager.stop',
      ])
      expect(payloadsOf(emitted, events.POLL_INTERVAL_EXCEEDED)).toEqual([
        {
          groupId: 'coordinator-group',
          memberId: 'member-1',
          generationId: 3,
          maxPollInterval: 8000,
          elapsedMs: 9000,
          isStatic: false,
        },
      ])
      expect(coordinator.takeVerdict()).toMatchObject({
        kind: 'lost',
        reason: 'poll-interval-exceeded',
      })
      expect(consumerGroup.leave).toHaveBeenCalledTimes(1)
    })

    it('returns without awaiting the fetch manager, and swallows a rejecting stop', async () => {
      const { coordinator, fetchManager, consumerGroup } = build()
      fetchManager.stop.mockImplementation(() => new Promise(() => {}))

      await expect(
        coordinator.onPollExpired({ elapsedMs: 9000, isStatic: false })
      ).resolves.toBeUndefined()

      fetchManager.stop.mockRejectedValue(new Error('stop failed'))
      consumerGroup.leave.mockRejectedValue(new Error('leave failed'))

      await expect(
        coordinator.onPollExpired({ elapsedMs: 9000, isStatic: false })
      ).resolves.toBeUndefined()
    })
  })

  describe('case 10: poll expiry, static member', () => {
    it('resets locally without a LeaveGroup, and reports isStatic', async () => {
      const { coordinator, consumerGroup, calls, emitted } = build()
      jest.spyOn(coordinator.logger, 'warn').mockImplementation(() => calls.push('warn'))

      await coordinator.onPollExpired({ elapsedMs: 12000, isStatic: true })

      expect(consumerGroup.leave).not.toHaveBeenCalled()
      expect(calls).toEqual([
        'warn',
        events.POLL_INTERVAL_EXCEEDED,
        'heartbeatManager.stop',
        'resetGenerationLocally',
        'fetchManager.stop',
      ])
      expect(payloadsOf(emitted, events.POLL_INTERVAL_EXCEEDED)[0].isStatic).toBe(true)
    })

    it('sends no LeaveGroup and stops no manager once stopRequested is set', async () => {
      const { coordinator, consumerGroup, heartbeatManager, runner, emitted } = build()
      runner.stopRequested = true

      await coordinator.onPollExpired({ elapsedMs: 12000, isStatic: false })

      expect(payloadsOf(emitted, events.POLL_INTERVAL_EXCEEDED)).toHaveLength(1)
      expect(heartbeatManager.stop).not.toHaveBeenCalled()
      expect(consumerGroup.leave).not.toHaveBeenCalled()
      expect(consumerGroup.resetGenerationLocally).not.toHaveBeenCalled()
    })
  })

  describe('case 11: nothing is raced as a bare rejected promise', () => {
    it('keeps a rejecting fetch-manager stop, a rejecting chain and a rejecting leave caught', async () => {
      const { coordinator, consumerGroup, fetchManager, runner } = build({ mode: COOPERATIVE })
      fetchManager.stop.mockRejectedValue(new Error('stop failed'))
      consumerGroup.leave.mockRejectedValue(new Error('leave failed'))
      consumerGroup.joinAndSync.mockRejectedValue(new Error('join failed'))

      coordinator.requestRejoin({
        kind: 'revoked',
        reason: 'stale-assignment',
        error: new Error('stale'),
      })
      await coordinator.idle().catch(() => {})
      await coordinator.onPollExpired({ elapsedMs: 9000, isStatic: false })
      await new Promise((resolve) => setImmediate(resolve))

      expect(runner.onCrash).toHaveBeenCalledTimes(1)
      expect(unhandled).toEqual([])
    })
  })

  describe('case 12: the first join of a run', () => {
    it('runs with running already true and emits GROUP_JOIN without REBALANCING', async () => {
      const { runner, consumerGroup, emitted, calls } = createRealRunner()
      let runningDuringJoin = null
      consumerGroup.joinAndSync.mockImplementation(async () => {
        calls.push('joinAndSync')
        runningDuringJoin = runner.running
        return { assignment: {} }
      })
      runner.scheduleFetchManager = jest.fn()

      await runner.start()

      expect(runningDuringJoin).toBe(true)
      expect(payloadsOf(emitted, events.GROUP_JOIN)).toHaveLength(1)
      expect(payloadsOf(emitted, events.REBALANCING)).toHaveLength(0)
      expect(calls.filter((name) => name === 'ensureStarted')).toHaveLength(2)

      await runner.rebalanceCoordinator.runRound('rebalance-in-progress', 'revoked')
      expect(payloadsOf(emitted, events.REBALANCING)).toHaveLength(1)
      expect(payloadsOf(emitted, events.GROUP_JOIN)).toHaveLength(2)
    })

    it('reverts running to false and crashes when the initial round throws', async () => {
      const { runner, consumerGroup } = createRealRunner()
      const boom = new Error('join failed')
      consumerGroup.joinAndSync.mockRejectedValue(boom)
      runner.scheduleFetchManager = jest.fn()

      await runner.start()

      expect(runner.running).toBe(false)
      expect(runner.onCrash).toHaveBeenCalledWith(boom)
      expect(runner.scheduleFetchManager).not.toHaveBeenCalled()
    })
  })

  describe('case 13: a fenced member abandons instead of joining', () => {
    it('discards, then emits REBALANCING for the abandoned round, then rethrows', async () => {
      const { runner, consumerGroup, calls, emitted } = createRealRunner()
      const fenced = createErrorFromCode(FENCED_INSTANCE_ID)

      await expect(runner.handleRebalanceError(fenced)).rejects.toBe(fenced)

      expect(consumerGroup.discardUncommittedOffsets).toHaveBeenCalledTimes(1)
      expect(calls.indexOf('discard')).toBeLessThan(calls.indexOf(events.REBALANCING))
      expect(payloadsOf(emitted, events.REBALANCING)).toEqual([
        { groupId: 'coordinator-group', memberId: 'member-1', reason: 'lost' },
      ])
      expect(consumerGroup.joinAndSync).not.toHaveBeenCalled()
      expect(payloadsOf(emitted, events.GROUP_JOIN)).toHaveLength(0)
      expect(runner.rebalanceCoordinator.rounds).toEqual(0)
    })
  })

  describe('case 14: PARTITIONS_LOST precedes the JoinGroup', () => {
    const assignment = [{ topic: 't', partitions: [0, 1] }]

    it('emits the previous assignment before joining, exactly once per converging chain', async () => {
      const { coordinator, calls, emitted } = build({ assignment })

      await coordinator.runRound('lost', 'lost')

      expect(calls.indexOf(events.PARTITIONS_LOST)).toBeLessThan(calls.indexOf('joinAndSync'))
      expect(payloadsOf(emitted, events.PARTITIONS_LOST)).toEqual([
        {
          groupId: 'coordinator-group',
          memberId: 'member-1',
          generationId: 3,
          partitions: assignment,
        },
      ])
    })

    it('emits nothing when the previous assignment is empty, and never for a revoked round', async () => {
      const empty = build()
      await empty.coordinator.runRound('lost', 'lost')
      expect(payloadsOf(empty.emitted, events.PARTITIONS_LOST)).toHaveLength(0)

      const revoked = build({ assignment })
      await revoked.coordinator.runRound('rebalance-in-progress', 'revoked')
      expect(payloadsOf(revoked.emitted, events.PARTITIONS_LOST)).toHaveLength(0)
    })

    it('carries a mid-chain LOST into the follow-up round, with its own kind and reason', async () => {
      const { coordinator, consumerGroup, emitted } = build({ mode: COOPERATIVE, assignment })
      const gate = deferred()
      consumerGroup.joinAndSync.mockImplementationOnce(async () => {
        await gate.promise
        return { assignment: {} }
      })

      const chain = coordinator.runRound('rebalance-in-progress', 'revoked')
      await Promise.resolve()
      coordinator.runRound('lost', 'lost')
      gate.resolve()
      await chain

      expect(payloadsOf(emitted, events.REBALANCING).map(({ reason }) => reason)).toEqual([
        'rebalance-in-progress',
        'lost',
      ])
      expect(payloadsOf(emitted, events.PARTITIONS_LOST)).toHaveLength(1)
    })
  })

  describe('case 15: a cooperative round leaves the fetch manager alone', () => {
    it('emits in Java order and touches no fetcher', async () => {
      const assignment = [{ topic: 't', partitions: [0] }]
      const { coordinator, fetchManager, calls, emitted } = build({
        mode: COOPERATIVE,
        assignment,
        deltas: [
          {
            revoked: [{ topic: 't', partitions: [1] }],
            added: [{ topic: 't', partitions: [0] }],
          },
        ],
      })
      const reset = jest.spyOn(coordinator.pollTimer, 'reset')

      await coordinator.runRound('rebalance-in-progress', 'revoked')

      expect(calls.slice(0, 6)).toEqual([
        events.REBALANCING,
        'joinAndSync',
        'applyRebalanceResult',
        events.PARTITIONS_REVOKED,
        events.PARTITIONS_ASSIGNED,
        events.GROUP_JOIN,
      ])
      expect(reset).toHaveBeenCalled()
      // The fetcher-identity half of this claim lives in `fetchManagerStop.spec.js`: this suite's
      // `getFetchers` is a fake returning [], so an identity check here would pass vacuously.
      expect(fetchManager.start).not.toHaveBeenCalled()
      expect(fetchManager.stop).not.toHaveBeenCalled()
      expect(payloadsOf(emitted, events.GROUP_JOIN)[0]).toMatchObject({
        rebalanceProtocol: COOPERATIVE,
        memberAssignment: { t: [0] },
      })
    })

    it('reports the whole assignment on every eager round', async () => {
      const assignment = [{ topic: 't', partitions: [0, 1] }]
      const { coordinator, emitted } = build({
        assignment,
        deltas: [{ revoked: assignment, added: assignment }],
      })

      await coordinator.runRound('rebalance-in-progress', 'revoked')

      expect(payloadsOf(emitted, events.PARTITIONS_REVOKED)[0].partitions).toEqual(assignment)
      expect(payloadsOf(emitted, events.PARTITIONS_ASSIGNED)[0].partitions).toEqual(assignment)
      expect(payloadsOf(emitted, events.GROUP_JOIN)[0].rebalanceProtocol).toEqual(EAGER)
    })
  })

  describe('case 16: recoverFromFetch requests a rejoin without awaiting one', () => {
    it('returns while the round is still joining', async () => {
      const { group, cluster } = createRealGroup()
      const built = build({ mode: COOPERATIVE })
      const coordinator = new RebalanceCoordinator({
        runner: built.runner,
        consumerGroup: group,
        fetchManager: built.fetchManager,
        heartbeatManager: built.heartbeatManager,
        pollTimer: new PollTimer({ pollDeadline: 8000 }),
        instrumentationEmitter: group.instrumentationEmitter,
        logger: newLogger(),
        groupId: 'real-group',
      })
      group.attach({ rebalanceCoordinator: coordinator, heartbeatManager: null })

      // The negative control: a join that never settles must not hold recoverFromFetch open.
      const joinAndSync = jest
        .spyOn(group, 'joinAndSync')
        .mockImplementation(() => new Promise(() => {}))

      await expect(
        group.recoverFromFetch(createErrorFromCode(6)) // NOT_LEADER_FOR_PARTITION
      ).resolves.toBeUndefined()

      expect(cluster.refreshMetadata).toHaveBeenCalled()
      expect(joinAndSync).toHaveBeenCalledTimes(1)
      expect(coordinator.isRebalancing()).toBe(true)
    })

    it('takes the abort path under eager instead of a background round', async () => {
      const { group } = createRealGroup({ mode: EAGER })
      const built = build()
      const coordinator = new RebalanceCoordinator({
        runner: built.runner,
        consumerGroup: group,
        fetchManager: built.fetchManager,
        heartbeatManager: built.heartbeatManager,
        pollTimer: new PollTimer({ pollDeadline: 8000 }),
        instrumentationEmitter: group.instrumentationEmitter,
        logger: newLogger(),
        groupId: 'real-group',
      })
      group.attach({ rebalanceCoordinator: coordinator, heartbeatManager: null })
      const joinAndSync = jest.spyOn(group, 'joinAndSync')

      await group.recoverFromFetch(createErrorFromCode(6))

      expect(joinAndSync).not.toHaveBeenCalled()
      expect(built.fetchManager.stop).toHaveBeenCalledTimes(1)
      expect(coordinator.takeVerdict()).toMatchObject({
        kind: 'revoked',
        reason: 'stale-assignment',
      })
    })

    it('refreshes metadata and returns when no coordinator is attached yet', async () => {
      const { group, cluster } = createRealGroup()
      const joinAndSync = jest.spyOn(group, 'joinAndSync')

      await expect(group.recoverFromFetch(createErrorFromCode(6))).resolves.toBeUndefined()

      expect(cluster.refreshMetadata).toHaveBeenCalled()
      expect(joinAndSync).not.toHaveBeenCalled()
    })
  })

  describe('case 17: a REVOKED arriving during a round raises no verdict', () => {
    it('is dropped, and ten of them still produce one round', async () => {
      const { coordinator, consumerGroup, fetchManager } = build({ mode: COOPERATIVE })
      const gate = deferred()
      consumerGroup.joinAndSync.mockImplementation(async () => {
        await gate.promise
        return { assignment: {} }
      })

      const chain = coordinator.runRound('rebalance-in-progress', 'revoked')
      await Promise.resolve()
      expect(coordinator.isRebalancing()).toBe(true)

      for (let i = 0; i < 10; i++) {
        coordinator.requestRejoin({
          kind: 'revoked',
          reason: 'rebalance-in-progress',
          error: createErrorFromCode(REBALANCE_IN_PROGRESS),
        })
      }

      expect(coordinator.verdict).toBeNull()
      expect(coordinator.rejoinRequested).toBeNull()
      expect(fetchManager.stop).not.toHaveBeenCalled()

      gate.resolve()
      await chain

      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
      expect(coordinator.isRebalancing()).toBe(false)
    })
  })

  describe('case 18: the heartbeat manager is re-armed after a round', () => {
    it('re-arms after a successful round, and not at all once stopRequested is set', async () => {
      const { coordinator, heartbeatManager, calls, runner } = build()

      await coordinator.runRound('rebalance-in-progress', 'revoked')

      expect(calls.indexOf(events.GROUP_JOIN)).toBeLessThan(calls.indexOf('ensureStarted'))
      expect(heartbeatManager.ensureStarted).toHaveBeenCalledTimes(1)

      runner.stopRequested = true
      await coordinator.runRound('rebalance-in-progress', 'revoked')
      expect(heartbeatManager.ensureStarted).toHaveBeenCalledTimes(1)
    })

    it('never re-arms a manager that stop() cancelled while the round was joining', async () => {
      const { coordinator, consumerGroup, heartbeatManager, runner } = build()
      const gate = deferred()
      consumerGroup.joinAndSync.mockImplementation(async () => {
        await gate.promise
        return { assignment: {} }
      })

      const chain = coordinator.runRound('rebalance-in-progress', 'revoked')
      await Promise.resolve()
      runner.stopRequested = true
      gate.resolve()
      await chain

      expect(heartbeatManager.ensureStarted).not.toHaveBeenCalled()
    })

    it('recovers from a poll expiry end to end: stop, rejoin, re-arm', async () => {
      const { runner, consumerGroup, fetchManager, calls } = createRealRunner()
      let releaseStart
      fetchManager.start.mockImplementation(
        () => new Promise((resolve) => (releaseStart = resolve))
      )
      fetchManager.stop.mockImplementation(async () => {
        calls.push('fetchManager.stop')
        releaseStart()
      })
      consumerGroup.joinAndSync.mockImplementation(async () => {
        calls.push('joinAndSync')
        runner.running = false
        return { assignment: {} }
      })

      runner.running = true
      runner.scheduleFetchManager()
      await runner.rebalanceCoordinator.onPollExpired({ elapsedMs: 9000, isStatic: false })
      await waitFor(() => calls.includes('ensureStarted'), { delay: 5 })

      expect(calls.indexOf('heartbeatManager.stop')).toBeLessThan(calls.indexOf('joinAndSync'))
      expect(calls.indexOf('joinAndSync')).toBeLessThan(calls.indexOf('ensureStarted'))
    })
  })

  describe('a batch that outlived the assignment it was fetched under', () => {
    const batchFor = (partition, assignmentEpoch = null) => {
      const batch = new Batch('t', 0, {
        partition,
        highWatermark: '5',
        messages: [{ offset: '4', key: null }],
      })
      batch.assignmentEpoch = assignmentEpoch
      return batch
    }

    const runnerHandling = (handled) => {
      const built = createRealRunner()
      built.runner.eachBatch = async ({ batch }) => handled.push(batch.partition)
      built.runner.running = true
      return built
    }

    it('is not delivered, resolved or committed when its partition is gone', async () => {
      const handled = []
      const { runner, consumerGroup } = runnerHandling(handled)
      consumerGroup.isAssigned.mockImplementation((topic, partition) => partition === 0)

      await runner.handleBatch(batchFor(1))

      expect(handled).toEqual([])
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(consumerGroup.resolveOffset).not.toHaveBeenCalled()
    })

    it('is not delivered, resolved or committed when it carries the previous epoch', async () => {
      const handled = []
      const { runner, consumerGroup } = runnerHandling(handled)
      consumerGroup.assignmentEpoch = 4

      // Still assigned - only the epoch says this batch belongs to a replaced assignment.
      await runner.handleBatch(batchFor(0, 3))

      expect(consumerGroup.isAssigned).not.toHaveBeenCalled()
      expect(handled).toEqual([])
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled()
      expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled()
      expect(consumerGroup.resolveOffset).not.toHaveBeenCalled()
    })

    it('goes through on the current epoch and an assigned partition', async () => {
      const handled = []
      const { runner, consumerGroup } = runnerHandling(handled)
      consumerGroup.assignmentEpoch = 4

      await runner.handleBatch(batchFor(0, 4))

      expect(handled).toEqual([0])
      expect(consumerGroup.commitOffsets).toHaveBeenCalled()
    })
  })

  describe('the round waits for in-flight handlers before it replaces the offsets', () => {
    const batchFor = (partition, assignmentEpoch = null) => {
      const batch = new Batch('t', 0, {
        partition,
        highWatermark: '5',
        messages: [{ offset: '4', key: null }],
      })
      batch.assignmentEpoch = assignmentEpoch
      return batch
    }

    /** Fails loudly instead of hanging when a wait never settles. */
    const settlesWithin = (promise, ms = 2000) =>
      Promise.race([
        promise,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error(`did not settle within ${ms}ms`)), ms)
        ),
      ])

    it('holds the round until the handler returns, and commits what it resolved', async () => {
      const { runner, consumerGroup, calls } = createRealRunner()
      const gate = deferred()
      runner.running = true
      runner.eachBatch = async () => {
        calls.push('handler-start')
        await gate.promise
        consumerGroup.resolveOffset({ topic: 't', partition: 0, offset: '4' })
        calls.push('handler-end')
      }

      const handling = runner.handleBatch(batchFor(0))
      await Promise.resolve()
      expect(calls).toContain('handler-start')

      const rebalancing = runner.handleRebalanceError(createErrorFromCode(REBALANCE_IN_PROGRESS))
      await new Promise((resolve) => setImmediate(resolve))

      // The offsets must still be the ones the handler is resolving into.
      expect(consumerGroup.applyRebalanceResult).not.toHaveBeenCalled()
      expect(consumerGroup.joinAndSync).not.toHaveBeenCalled()

      gate.resolve()
      await settlesWithin(handling)
      await settlesWithin(rebalancing)

      expect(calls.indexOf('handler-end')).toBeLessThan(calls.indexOf('commitOffsets'))
      expect(calls.indexOf('commitOffsets')).toBeLessThan(calls.indexOf('applyRebalanceResult'))
      expect(consumerGroup.resolveOffset).toHaveBeenCalledWith({
        topic: 't',
        partition: 0,
        offset: '4',
      })
    })

    it('drops a batch the worker picks up during the stop-the-world window', async () => {
      const { runner, consumerGroup, calls } = createRealRunner()
      const gate = deferred()
      runner.running = true
      runner.eachBatch = async ({ batch }) => {
        calls.push(`handled-${batch.partition}`)
        await gate.promise
      }

      const first = runner.handleBatch(batchFor(0))
      await Promise.resolve()

      const rebalancing = runner.handleRebalanceError(createErrorFromCode(REBALANCE_IN_PROGRESS))
      await new Promise((resolve) => setImmediate(resolve))

      // The queue hands the worker the next batch the instant the current one returns, so a
      // batch arriving inside this window must not be delivered either.
      const queued = runner.handleBatch(batchFor(1))

      gate.resolve()
      await settlesWithin(first)
      await settlesWithin(queued)
      await settlesWithin(rebalancing)

      expect(calls).toContain('handled-0')
      expect(calls).not.toContain('handled-1')
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
    })

    it('does not deadlock when the handler that threw is the one that raised the verdict', async () => {
      const { runner, consumerGroup } = createRealRunner()
      const boom = createErrorFromCode(REBALANCE_IN_PROGRESS)
      runner.running = true
      runner.eachBatch = async () => {
        throw boom
      }

      // The callback path: the handler rethrows, so it has already returned by the time the
      // rebalance is handled on its own stack.
      await expect(runner.handleBatch(batchFor(0))).rejects.toBe(boom)
      await settlesWithin(runner.handleRebalanceError(boom))

      expect(runner.inFlightHandlers).toEqual(0)
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
    })

    it('lets a concurrent stop() complete while the wait is pending', async () => {
      const { runner, consumerGroup } = createRealRunner()
      const gate = deferred()
      runner.running = true
      runner.eachBatch = async () => gate.promise

      const handling = runner.handleBatch(batchFor(0))
      await Promise.resolve()

      const rebalancing = runner.handleRebalanceError(createErrorFromCode(REBALANCE_IN_PROGRESS))
      const stopping = runner.stop()

      gate.resolve()
      await settlesWithin(handling)
      await settlesWithin(rebalancing)
      await settlesWithin(stopping)

      expect(runner.stopRequested).toBe(true)
      expect(runner.inFlightHandlers).toEqual(0)
      // The round is a no-op once stop() has been asked for, so nothing rejoined behind it.
      expect(consumerGroup.joinAndSync).not.toHaveBeenCalled()
      expect(consumerGroup.leave).toHaveBeenCalled()
    })
  })

  describe('the assignment epoch', () => {
    const syncResult = { assignment: {}, userData: null }

    it('moves whenever the apply replaces the offsets', async () => {
      const { group } = createRealGroup()
      expect(group.assignmentEpoch).toEqual(0)

      await group.applyRebalanceResult(syncResult)
      expect(group.assignmentEpoch).toEqual(1)

      await group.applyRebalanceResult(syncResult)
      expect(group.assignmentEpoch).toEqual(2)
    })

    it('does not move when the apply is told it was incremental', async () => {
      const { group } = createRealGroup()
      await group.applyRebalanceResult(syncResult)
      const offsetManager = group.offsetManager

      await group.applyRebalanceResult(syncResult, { rebuildOffsets: false })

      expect(group.assignmentEpoch).toEqual(1)
      expect(group.offsetManager).toBe(offsetManager)
    })
  })

  describe('the poll deadline is not armed yet', () => {
    it('a freshly constructed runner never schedules a poll-expiry tick', async () => {
      jest.useFakeTimers()

      try {
        // The manager the Runner built, carrying the Runner's own onPollExpired closure - not the
        // recording double, which would prove nothing about the wiring.
        const { runner, realHeartbeatManager } = createRealRunner()
        const onPollExpired = jest.spyOn(runner.rebalanceCoordinator, 'onPollExpired')

        realHeartbeatManager.ensureStarted()
        // Far past the configured deadline, with no batch ever handed off.
        await jest.advanceTimersByTimeAsync(runner.pollTimer.deadline() * 2)

        expect(runner.pollTimer.deadline()).toEqual(300000)
        expect(runner.pollTimer.expired()).toBe(true)
        expect(onPollExpired).not.toHaveBeenCalled()

        realHeartbeatManager.stop()
      } finally {
        jest.useRealTimers()
      }
    })
  })

  describe('a stop() during the initial join is not undone by start()', () => {
    it('arms no heartbeat timer and sends no heartbeat after LeaveGroup', async () => {
      jest.useFakeTimers()

      try {
        const { runner, consumerGroup, realHeartbeatManager } = createRealRunner()
        // The manager the Runner built, so `start()`'s own re-arm is what is under test.
        runner.heartbeatManager = realHeartbeatManager
        runner.rebalanceCoordinator.heartbeatManager = realHeartbeatManager
        runner.scheduleFetchManager = jest.fn()

        const gate = deferred()
        consumerGroup.joinAndSync.mockImplementation(async () => {
          await gate.promise
          return { assignment: {} }
        })

        const starting = runner.start()
        await Promise.resolve()

        // A shutdown signal while the first JoinGroup is still in flight - the documented
        // `consumer.run({...})`-without-await shape.
        const stopping = runner.stop()
        gate.resolve()
        await starting
        await stopping

        expect(consumerGroup.leave).toHaveBeenCalledTimes(1)
        expect(realHeartbeatManager.isRunning()).toBe(false)
        expect(jest.getTimerCount()).toEqual(0)

        consumerGroup.heartbeat.mockClear()
        await jest.advanceTimersByTimeAsync(30000)
        expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
      } finally {
        jest.useRealTimers()
      }
    })
  })

  describe('a crash out of start() leaves no timer behind', () => {
    it('cancels the chain the initial round armed, even when onCrash does not stop the runner', async () => {
      jest.useFakeTimers()

      try {
        const { runner, consumerGroup, realHeartbeatManager } = createRealRunner({
          mode: COOPERATIVE,
        })
        // The manager the Runner built, so the crash path's own cleanup is what is under test.
        runner.heartbeatManager = realHeartbeatManager
        runner.rebalanceCoordinator.heartbeatManager = realHeartbeatManager
        runner.scheduleFetchManager = jest.fn()

        // The first round succeeds and arms the manager; the follow-up round it schedules throws.
        consumerGroup.applyRebalanceResult.mockResolvedValueOnce({
          assignment: [],
          added: [],
          revoked: [{ topic: 't', partitions: [0] }],
        })
        consumerGroup.joinAndSync
          .mockImplementationOnce(async () => ({ assignment: {} }))
          .mockRejectedValue(new Error('second round failed'))

        await runner.start()

        expect(runner.onCrash).toHaveBeenCalledTimes(1)
        expect(runner.running).toBe(false)
        expect(realHeartbeatManager.isRunning()).toBe(false)
        expect(jest.getTimerCount()).toEqual(0)

        consumerGroup.heartbeat.mockClear()
        await jest.advanceTimersByTimeAsync(30000)
        expect(consumerGroup.heartbeat).not.toHaveBeenCalled()
      } finally {
        jest.useRealTimers()
      }
    })
  })

  describe('a verdict is routed by its own kind and reason', () => {
    const staleAssignmentVerdict = () => {
      const error = new Error('has 2 partitions for topic t, unknown partitions: 1')
      error.name = 'KafkaJSStaleTopicMetadataAssignment'
      return { kind: 'revoked', reason: 'stale-assignment', error }
    }

    it('rejoins on an eager stale-assignment verdict instead of crashing the fetch loop', async () => {
      const { runner, consumerGroup, fetchManager, emitted, calls } = createRealRunner()
      const verdict = staleAssignmentVerdict()
      let attempts = 0

      consumerGroup.joinAndSync.mockImplementation(async () => {
        calls.push('joinAndSync')
        runner.running = false
        return { assignment: {} }
      })
      fetchManager.start.mockImplementation(async () => {
        attempts++
        runner.rebalanceCoordinator.requestRejoin(verdict)
      })

      runner.running = true
      runner.scheduleFetchManager()
      await waitFor(() => consumerGroup.joinAndSync.mock.calls.length > 0, { delay: 5 })

      expect(payloadsOf(emitted, events.REBALANCING)).toEqual([
        {
          groupId: 'coordinator-group',
          memberId: 'member-1',
          reason: 'stale-assignment',
        },
      ])
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
      // A verdict costs no retrier attempt, and a stale assignment is not a lost membership.
      expect(attempts).toEqual(1)
      expect(consumerGroup.discardUncommittedOffsets).not.toHaveBeenCalled()
      expect(runner.onCrash).not.toHaveBeenCalled()
    })

    it('classifies from the error when there is no verdict to route', async () => {
      const { runner, consumerGroup, emitted } = createRealRunner()

      await runner.handleRebalanceError(createErrorFromCode(REBALANCE_IN_PROGRESS))

      expect(payloadsOf(emitted, events.REBALANCING)[0].reason).toEqual('rebalance-in-progress')
      expect(consumerGroup.joinAndSync).toHaveBeenCalledTimes(1)
    })

    it('still rethrows something that is not a rebalance at all', async () => {
      const { runner, consumerGroup } = createRealRunner()
      const boom = new Error('not a rebalance')

      await expect(runner.handleRebalanceError(boom)).rejects.toBe(boom)
      expect(consumerGroup.joinAndSync).not.toHaveBeenCalled()
    })
  })

  describe('case 19: every rebalance error yields a reason', () => {
    it.each([
      ['ILLEGAL_GENERATION', createErrorFromCode(ILLEGAL_GENERATION), 'lost', 'lost'],
      ['UNKNOWN_MEMBER_ID', createErrorFromCode(UNKNOWN_MEMBER_ID), 'lost', 'lost'],
      ['FENCED_INSTANCE_ID', createErrorFromCode(FENCED_INSTANCE_ID), 'lost', 'lost'],
      [
        'poll interval exceeded',
        pollIntervalExceededError({ elapsedMs: 1, maxPollInterval: 0 }),
        'lost',
        'poll-interval-exceeded',
      ],
      [
        'REBALANCE_IN_PROGRESS',
        createErrorFromCode(REBALANCE_IN_PROGRESS),
        'revoked',
        'rebalance-in-progress',
      ],
      [
        'a bare REBALANCE_IN_PROGRESS type',
        { type: 'REBALANCE_IN_PROGRESS' },
        'revoked',
        'rebalance-in-progress',
      ],
      [
        'COORDINATOR_LOAD_IN_PROGRESS',
        createErrorFromCode(COORDINATOR_LOAD_IN_PROGRESS),
        'revoked',
        'coordinator-changed',
      ],
      [
        'NOT_COORDINATOR_FOR_GROUP',
        createErrorFromCode(NOT_COORDINATOR_FOR_GROUP),
        'revoked',
        'coordinator-changed',
      ],
      [
        'a bare NOT_COORDINATOR_FOR_GROUP type',
        { type: 'NOT_COORDINATOR_FOR_GROUP' },
        'revoked',
        'coordinator-changed',
      ],
    ])('classifies %s and emits a REBALANCING for it', async (_name, error, kind, reason) => {
      const { coordinator, emitted } = build()

      const verdict = coordinator.classify(error, 'heartbeat')
      expect(verdict).toMatchObject({ kind, reason })

      await coordinator.runRound(verdict.reason, verdict.kind)
      expect(payloadsOf(emitted, events.REBALANCING)[0].reason).toEqual(reason)
    })

    it('raises no verdict for a coordinator that must simply be re-discovered', () => {
      const { coordinator } = build()
      expect(
        coordinator.classify(createErrorFromCode(COORDINATOR_NOT_AVAILABLE), 'heartbeat')
      ).toBeNull()
      expect(coordinator.classify(new Error('unrelated'), 'fetch')).toBeNull()
    })

    it('is a no-op once stopRequested is set, and clears inFlight on a failed round', async () => {
      const { coordinator, fetchManager, runner, consumerGroup } = build()
      runner.stopRequested = true

      coordinator.requestRejoin({
        kind: 'lost',
        reason: 'lost',
        error: createErrorFromCode(ILLEGAL_GENERATION),
      })

      expect(fetchManager.stop).not.toHaveBeenCalled()
      expect(coordinator.takeVerdict()).toBeNull()

      runner.stopRequested = false
      consumerGroup.joinAndSync.mockRejectedValue(new Error('join failed'))
      await expect(coordinator.runRound('lost', 'lost')).rejects.toThrow('join failed')
      expect(coordinator.isRebalancing()).toBe(false)
      expect(consumerGroup.clearStaleAssignmentPending).toHaveBeenCalled()
    })
  })
})
