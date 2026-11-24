const { createLogger, LEVELS } = require('../../loggers')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const Runner = require('../runner')
const { MemberAssignment } = require('../assignerProtocol')
const {
  KafkaJSError,
  KafkaJSCommitFailedError,
  KafkaJSConnectionError,
  KafkaJSTopicRecreatedError,
} = require('../../errors')
const {
  fenced,
  groupAuthorization,
  protocolError,
  target,
  buildConsumer,
  advanceUntil,
  deferred,
  settled,
  start,
  teardown,
  forget,
  beats,
  inCauseChain,
  recordingLogger,
} = require('./fixtures/scriptedConsumer')

const TOPIC_A_ID = 'aAaAaAaAaAaAaAaAaAaAaA'
const UNKNOWN_ID = 'cCcCcCcCcCcCcCcCcCcCcC'
const HANDLERS_IDLE = 'handlers-idle'
const REBALANCE_IN_PROGRESS = 27
const NOT_COORDINATOR = 16

/** The disconnect owed by a run `beginJoin` built, which the fixture's teardown does not know. */
let closeJoin = null

beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  const close = closeJoin
  closeJoin = null
  await close?.()
  await teardown()
  jest.useRealTimers()
  jest.restoreAllMocks()
})

/** @returns {KafkaJSTopicRecreatedError} the error a recreation stop reports */
const recreated = () =>
  new KafkaJSTopicRecreatedError('Topic topic-a was recreated', {
    topic: 'topic-a',
    previousTopicId: TOPIC_A_ID,
    topicId: 'zZzZzZzZzZzZzZzZzZzZzZ',
    groupId: 'loss-driver-group',
  })

/** @returns {KafkaJSError} a retriable error */
const transient = () => new KafkaJSError('A transient failure', { retriable: true })

/** The run's coordinator, runner, group and membership. */
const parts = (run) => {
  const rc = run.coordinators[0]
  return {
    rc,
    runner: rc.runner,
    group: rc.consumerGroup,
    membership: rc.consumerGroup.groupMembership,
  }
}

/** @returns {Array<object>} the leave heartbeats the member sent */
const leaves = (run) => run.log.filter(({ kind, epoch }) => kind === 'heartbeat' && epoch < 0)

/** @returns {number} every heartbeat sent */
const heartbeats = (run) => run.log.filter(({ kind }) => kind === 'heartbeat').length

/** @returns {number} the Fetch, heartbeat and OffsetCommit requests sent */
const requests = (run) =>
  run.log.filter(({ kind }) => ['fetch', 'heartbeat', 'commit'].includes(kind)).length

/** A logger recording each line's message and the text of the error it logs. */
const errorLogger = () => {
  const lines = []
  const logger = createLogger({
    logCreator:
      () =>
      ({ log }) =>
        lines.push({ message: log.message, error: log.error }),
  })
  return { logger, lines }
}

/** Counts the consumer's DISCONNECT events. */
const countDisconnects = (consumer) => {
  const events = jest.fn()
  consumer.on(consumer.events.DISCONNECT, events)
  return events
}

/**
 * Starts a run whose eachMessage parks on the fixture's `held`, and waits until it has entered.
 * `run.handlerEnded()` tells whether it has returned; `run.handlerHeartbeat()` calls its heartbeat.
 */
const startHeld = async (options = {}) => {
  let payload = null
  let ended = false
  const run = await start({
    ...options,
    eachMessage: async (received, held) => {
      payload = received
      await held.promise
      ended = true
    },
  })
  await advanceUntil(() => payload !== null)
  run.handlerEnded = () => ended
  run.handlerHeartbeat = () => payload.heartbeat()
  return run
}

/**
 * Holds the next positive-epoch heartbeat until the test answers it.
 * @returns {{ sent: () => boolean, answer: (reply: Promise<object>) => void }}
 */
const holdNextBeat = (run) => {
  const held = deferred()
  const answer = run.state.answer
  let sent = false
  run.state.answer = (req) => {
    if (req.memberEpoch <= 0 || sent) return answer(req)
    sent = true
    return held.promise
  }
  return { sent: () => sent, answer: (reply) => held.resolve(reply) }
}

/**
 * Holds the answer to the first leave heartbeat until `gate` opens, then answers it with
 * `reply(answer)`.
 * @returns {() => boolean} whether that leave was written
 */
const holdLeaveAnswer = (run, gate, reply) => {
  const beat = run.coordinator.consumerGroupHeartbeat.getMockImplementation()
  let written = false
  run.coordinator.consumerGroupHeartbeat.mockImplementation(async (request) => {
    const answer = await beat(request)
    if (request.memberEpoch >= 0 || written) return answer
    written = true
    await gate.promise
    return reply(answer)
  })
  return () => written
}

/**
 * Starts a run whose first joining heartbeat is held, and waits until it has been sent.
 * @param {object} [options] consumer options
 */
const beginJoin = async (options = {}) => {
  const coordinators = []
  const runRound = RebalanceCoordinator.prototype.runRound
  jest.spyOn(RebalanceCoordinator.prototype, 'runRound').mockImplementation(function (...args) {
    if (!coordinators.includes(this)) coordinators.push(this)
    return runRound.apply(this, args)
  })
  const built = buildConsumer({ maxPollInterval: 4000, rebalanceTimeout: 3000, ...options })
  const first = deferred()
  built.state.answer = (req) =>
    req.memberEpoch === 0 ? first.promise : target(req.memberEpoch, null)
  await built.consumer.subscribe({ topic: 'topic-a' })
  const started = settled(built.consumer.run({ eachMessage: async () => {} }))
  await advanceUntil(() => coordinators.length > 0 && heartbeats(built) > 0)
  closeJoin = async () => {
    const end = settled(built.consumer.disconnect({ timeout: 0 }))
    await jest.advanceTimersByTimeAsync(10)
    expect(end.done).toBe(true)
  }
  return { ...built, coordinators, started }
}

/** An eager classic assigner, named as the coordinator double's JoinGroup answer names it. */
const eager = () => ({
  name: 'eager',
  version: 0,
  protocol: () => ({ name: 'eager', metadata: Buffer.alloc(0) }),
  assign: async () => [],
})

/**
 * Starts a run of topic-a/0 over the scripted cluster under the classic protocol, as a follower
 * that joins with JoinGroup and SyncGroup, or under manual assignment. Waits until `run()` settled,
 * or, when `joined` holds the first JoinGroup answer, until that JoinGroup was sent. A handler
 * receives the payload and `held`, which the run's teardown resolves.
 * @param {'classic'|'manual'} kind
 * @param {object} [options]
 * @returns {Promise<object>} the run, its runner and its group
 */
const startOther = async (
  kind,
  { eachMessage, eachBatch, hook, joined, config, autoCommitThreshold } = {}
) => {
  const runs = jest.spyOn(Runner.prototype, 'start')
  const built = buildConsumer({ groupProtocol: 'classic', partitionAssigners: [eager], ...config })
  const { consumer, coordinator, cluster, state } = built
  Object.assign(coordinator, {
    joinGroup: jest.fn(async () => {
      await joined?.promise
      return {
        generationId: 1,
        leaderId: 'another-member',
        memberId: 'member-1',
        members: [],
        groupProtocol: 'eager',
      }
    }),
    syncGroup: jest.fn(async () => ({
      memberAssignment: MemberAssignment.encode({ version: 0, assignment: { 'topic-a': [0] } }),
    })),
    heartbeat: jest.fn(async () => {}),
    leaveGroup: jest.fn(async () => {}),
  })
  cluster.stageMetadata = jest.fn(async ({ topics }) => ({
    topicMetadata: topics.map((topic) => ({
      topic,
      partitionMetadata: [{ partitionId: 0 }, { partitionId: 1 }],
    })),
  }))
  if (eachMessage || eachBatch) state.queue['topic-a|0'] = [40]
  if (kind === 'manual') {
    const assigned = settled(consumer.assign([{ topic: 'topic-a', partitions: [0] }]))
    await advanceUntil(() => assigned.done, 1000)
    expect(assigned).toMatchObject({ done: true, value: undefined })
  } else {
    await consumer.subscribe({ topic: 'topic-a' })
  }
  const held = deferred()
  const started = settled(
    consumer.run({
      eachMessage: eachMessage && ((payload) => eachMessage(payload, held)),
      eachBatch: eachBatch && ((payload) => eachBatch(payload, held)),
      ...(hook && { rebalanceListener: hook }),
      ...(autoCommitThreshold && { autoCommitThreshold }),
    })
  )
  closeJoin = async () => {
    held.resolve()
    const end = settled(consumer.disconnect({ timeout: 0 }))
    await jest.advanceTimersByTimeAsync(10)
    expect(end.done).toBe(true)
  }
  if (joined) {
    await advanceUntil(() => coordinator.joinGroup.mock.calls.length > 0)
    expect(started.done).toBe(false)
  } else {
    await advanceUntil(() => started.done)
    expect(started).toMatchObject({ done: true, value: undefined })
  }
  expect(runs).toHaveBeenCalledTimes(1)
  const runner = runs.mock.contexts[0]
  return { ...built, held, started, runner, group: runner.consumerGroup }
}

describe('Consumer > a run stopped for good with an error', () => {
  it('after stopRun no Fetch request is sent and an answered Fetch delivers nothing', async () => {
    const delivered = []
    const run = await start({ eachMessage: async ({ message }) => delivered.push(message.offset) })
    await advanceUntil(() => delivered.length === 1)
    const broker = await run.cluster.findBroker()
    const fetch = broker.fetch.getMockImplementation()
    const gate = deferred()
    const before = broker.fetch.mock.calls.length
    broker.fetch.mockImplementationOnce(async (request) => {
      await gate.promise
      return fetch(request)
    })
    await advanceUntil(() => broker.fetch.mock.calls.length > before)
    run.state.queue['topic-a|0'] = [40, 41]
    const error = recreated()

    parts(run).group.stopRun(error)
    const sent = broker.fetch.mock.calls.length
    gate.resolve()
    await advanceUntil(() => run.crashes.length > 0)
    await jest.advanceTimersByTimeAsync(5000)

    expect(delivered).toEqual(['40'])
    expect(broker.fetch.mock.calls.length).toBe(sent)
    expect(run.crashes).toHaveLength(1)
    expect(run.crashes[0].error).toBe(error)
  }, 30000)

  it('after stopRun a saved eachBatch resolveOffset resolves nothing', async () => {
    let resolveOffset = null
    const run = await start({
      eachBatch: async (payload, held) => {
        resolveOffset = payload.resolveOffset
        await held.promise
      },
    })
    await advanceUntil(() => resolveOffset !== null)
    const { group } = parts(run)
    const manager = group.offsetManager
    const commits = run.coordinator.offsetCommit.mock.calls.length
    resolveOffset('40')
    expect(manager.resolvedOffsets['topic-a'][0]).toBe('41')

    group.stopRun(recreated())
    resolveOffset('45')
    await jest.advanceTimersByTimeAsync(1000)

    expect(manager.resolvedOffsets['topic-a'][0]).toBe('41')
    expect(run.coordinator.offsetCommit).toHaveBeenCalledTimes(commits)
  }, 30000)

  it('after stopRun no OffsetCommit is written', async () => {
    const gate = deferred()
    let entered = false
    const run = await start({
      autoCommitThreshold: 1,
      eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
        entered = true
        resolveOffset(batch.messages[0].offset)
        await gate.promise
        await commitOffsetsIfNecessary().catch(() => {})
      },
    })
    await advanceUntil(() => entered)
    const commits = run.coordinator.offsetCommit.mock.calls.length

    parts(run).group.stopRun(recreated())
    await jest.advanceTimersByTimeAsync(6000)
    gate.resolve()
    await advanceUntil(() => run.crashes.length > 0)
    await jest.advanceTimersByTimeAsync(10000)

    expect(run.coordinator.offsetCommit).toHaveBeenCalledTimes(commits)
    expect(run.crashes).toHaveLength(1)
  }, 30000)

  it('an automatic commit made due by its threshold is written without stopRun (control)', async () => {
    const gate = deferred()
    let entered = false
    let committed = false
    const run = await start({
      autoCommitThreshold: 1,
      eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
        entered = true
        resolveOffset(batch.messages[0].offset)
        await gate.promise
        await commitOffsetsIfNecessary()
        committed = true
      },
    })
    await advanceUntil(() => entered)
    const commits = run.coordinator.offsetCommit.mock.calls.length

    gate.resolve()
    await advanceUntil(() => committed, 1000)

    expect(run.coordinator.offsetCommit.mock.calls.length).toBeGreaterThan(commits)
    const [{ topics }] = run.coordinator.offsetCommit.mock.calls[commits]
    expect(String(topics[0].partitions[0].offset)).toBe('41')
    expect(run.crashes).toEqual([])
  }, 30000)

  it('a public commitOffsets around stopRun is refused with the error of its moment', async () => {
    const run = await startHeld()
    const offsets = [{ topic: 'topic-a', partition: 0, offset: '41' }]
    const commits = run.coordinator.offsetCommit.mock.calls.length
    const lookup = deferred()
    const lookups = run.cluster.findGroupCoordinator.mock.calls.length
    run.cluster.findGroupCoordinator.mockImplementationOnce(async () => {
      await lookup.promise
      return run.coordinator
    })
    run.coordinator.isConnected = () => false
    const inFlight = settled(run.consumer.commitOffsets(offsets))
    await advanceUntil(() => run.cluster.findGroupCoordinator.mock.calls.length > lookups, 1000)
    const error = recreated()

    parts(run).group.stopRun(error)
    run.coordinator.isConnected = () => true
    lookup.resolve()
    await advanceUntil(() => inFlight.done, 1000)
    const attached = settled(run.consumer.commitOffsets(offsets))
    await advanceUntil(() => attached.done, 1000)
    run.held.resolve()
    await advanceUntil(() => run.crashes.length > 0)
    await jest.advanceTimersByTimeAsync(1000)
    const after = settled((async () => run.consumer.commitOffsets(offsets))())
    await advanceUntil(() => after.done, 1000)

    expect(inFlight.done).toBe(true)
    expect(inFlight.error).toBeInstanceOf(KafkaJSCommitFailedError)
    expect(inFlight.error.cause).toBe(error)
    expect(attached.error.type).toBe('CONSUMER_STOPPING')
    expect(after.error.message).toBe(
      'Consumer group was not initialized, consumer#run must be called first'
    )
    expect(run.coordinator.offsetCommit).toHaveBeenCalledTimes(commits)
    expect(run.crashes).toHaveLength(1)
    expect(run.crashes[0].error).toBe(error)
  }, 30000)

  describe('a commit overtaken by stopRun emits no COMMIT_OFFSETS', () => {
    const offsets = [{ topic: 'topic-a', partition: 0, offset: '41' }]

    it('when stopRun lands after its answer was taken and before it is recorded', async () => {
      const run = await startHeld()
      const { group } = parts(run)
      const manager = group.offsetManager
      const send = manager.sendOffsetCommit.bind(manager)
      const error = recreated()
      jest.spyOn(manager, 'sendOffsetCommit').mockImplementationOnce(async (...args) => {
        const result = await send(...args)
        group.stopRun(error)
        return result
      })
      const events = jest.fn()
      run.consumer.on(run.consumer.events.COMMIT_OFFSETS, events)
      const commits = run.coordinator.offsetCommit.mock.calls.length

      const inFlight = settled(run.consumer.commitOffsets(offsets))
      await advanceUntil(() => inFlight.done, 1000)

      expect(inFlight.done).toBe(true)
      expect(inCauseChain(inFlight.error, error)).toBe(true)
      expect(run.coordinator.offsetCommit).toHaveBeenCalledTimes(commits + 1)
      expect(events).not.toHaveBeenCalled()
    }, 30000)

    it('when its answer arrives after stopRun (control)', async () => {
      const run = await startHeld()
      const { group } = parts(run)
      const commit = run.coordinator.offsetCommit.getMockImplementation()
      const gate = deferred()
      let written = false
      run.coordinator.offsetCommit.mockImplementationOnce(async (request) => {
        written = true
        await gate.promise
        return commit(request)
      })
      const events = jest.fn()
      run.consumer.on(run.consumer.events.COMMIT_OFFSETS, events)
      const commits = run.coordinator.offsetCommit.mock.calls.length
      const inFlight = settled(run.consumer.commitOffsets(offsets))
      await advanceUntil(() => written, 1000)
      const error = recreated()

      group.stopRun(error)
      gate.resolve()
      await advanceUntil(() => inFlight.done, 1000)

      expect(inFlight.done).toBe(true)
      expect(inCauseChain(inFlight.error, error)).toBe(true)
      expect(run.coordinator.offsetCommit).toHaveBeenCalledTimes(commits + 1)
      expect(events).not.toHaveBeenCalled()
    }, 30000)
  })

  it('after stopRun no heartbeat is sent, and a handler heartbeat is refused', async () => {
    const run = await startHeld()
    const error = recreated()

    parts(run).group.stopRun(error)
    const sent = heartbeats(run)
    await expect(run.handlerHeartbeat()).rejects.toBe(error)
    await jest.advanceTimersByTimeAsync(10000)

    expect(heartbeats(run)).toBe(sent)
    expect(run.crashes).toEqual([])
  }, 30000)

  it.each([
    ['a success naming a new target', () => Promise.resolve(target(5, { 'topic-a': [0, 1] }))],
    ['a fencing error', () => Promise.reject(fenced())],
  ])(
    'after stopRun a heartbeat in flight answered with %s changes nothing',
    async (_, reply) => {
      const run = await startHeld()
      const { group, membership } = parts(run)
      const beat = holdNextBeat(run)
      await advanceUntil(beat.sent)
      const before = {
        epoch: membership.memberEpoch,
        state: membership.state,
        target: membership.target,
      }
      const events = jest.fn()
      run.consumer.on(run.consumer.events.HEARTBEAT, events)

      group.stopRun(recreated())
      beat.answer(reply())
      await jest.advanceTimersByTimeAsync(5000)

      expect(membership.memberEpoch).toBe(before.epoch)
      expect(membership.state).toBe(before.state)
      expect(membership.target).toBe(before.target)
      expect(events).not.toHaveBeenCalled()
      expect(run.crashes).toEqual([])
    },
    30000
  )

  it('stopRun during the initial join reports its own error', async () => {
    const run = await beginJoin()
    const error = recreated()

    run.coordinators[0].consumerGroup.stopRun(error)
    await advanceUntil(() => run.started.done && run.crashes.length > 0)
    const sent = heartbeats(run)
    await jest.advanceTimersByTimeAsync(10000)

    expect(run.started).toMatchObject({ done: true, value: undefined })
    expect(run.crashes).toHaveLength(1)
    expect(run.crashes[0].error).toBe(error)
    expect(run.crashes[0].restart).toBe(false)
    expect(heartbeats(run)).toBe(sent)
    expect(run.log.filter(({ kind }) => kind === 'fetch')).toEqual([])
  }, 30000)

  it('stopRun with a retriable error during startup keeps it and restarts by default', async () => {
    const run = await beginJoin({ retry: { initialRetryTime: 300 } })
    const error = transient()

    run.coordinators[0].consumerGroup.stopRun(error)
    await advanceUntil(() => run.crashes.length > 0)

    expect(run.crashes).toHaveLength(1)
    expect(run.crashes[0].error).toBe(error)
    expect(run.crashes[0].restart).toBe(true)
  }, 30000)

  it('a pending fatal heartbeat answer wins over stopRun', async () => {
    const answered = groupAuthorization()
    const lost = jest.fn()
    const run = await startHeld({ fault: () => answered, hook: { onPartitionsLost: lost } })
    const { group, runner } = parts(run)
    run.arm()
    await advanceUntil(() => runner.fatal !== null)
    expect(runner.fatal).toMatchObject({ claimed: false })

    group.stopRun(recreated())
    await advanceUntil(() => run.crashes.length > 0, 2000)
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes).toHaveLength(1)
    expect(inCauseChain(run.crashes[0].error, answered)).toBe(true)
    expect(run.crashes[0].restart).toBe(false)
    expect(lost).toHaveBeenCalledTimes(1)
  }, 30000)

  it('a fatal heartbeat answer after stopRun does not replace the stop error', async () => {
    const run = await startHeld()
    const beat = holdNextBeat(run)
    await advanceUntil(beat.sent)
    const error = recreated()

    parts(run).group.stopRun(error)
    beat.answer(Promise.reject(groupAuthorization()))
    await jest.advanceTimersByTimeAsync(1000)
    run.held.resolve()
    await advanceUntil(() => run.crashes.length > 0)
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes).toHaveLength(1)
    expect(run.crashes[0].error).toBe(error)
  }, 30000)

  it('a pending fatal answer that wins leaves the run not terminated', async () => {
    const run = await startHeld()
    const { group, runner } = parts(run)
    run.arm()
    await advanceUntil(() => runner.fatal !== null)

    group.stopRun(recreated())
    await advanceUntil(() => run.crashes.length > 0, 2000)

    expect(runner.terminated).toBe(false)
    expect(run.handlerEnded()).toBe(false)
    expect(run.crashes).toHaveLength(1)
  }, 30000)

  it('stopRun waits for a running handler before CRASH and before a restart', async () => {
    const run = await startHeld({ retry: { initialRetryTime: 300 } })
    const { group } = parts(run)
    const clear = jest.spyOn(group, 'clearAssignerState')
    const error = transient()

    group.stopRun(error)
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes).toEqual([])
    expect(clear).not.toHaveBeenCalled()
    expect(run.cluster.connect).toHaveBeenCalledTimes(1)

    run.held.resolve()
    await advanceUntil(() => run.crashes.length > 0)

    expect(run.handlerEnded()).toBe(true)
    expect(clear).toHaveBeenCalledTimes(1)
    expect(run.crashes).toHaveLength(1)
    expect(run.crashes[0].error).toBe(error)
    expect(run.crashes[0].restart).toBe(true)

    await advanceUntil(() => run.cluster.connect.mock.calls.length > 1, 5000)
    expect(run.cluster.connect).toHaveBeenCalledTimes(2)
  }, 30000)

  describe("a bounded disconnect ends a recreation stop's handler drain at its deadline", () => {
    it('when the deadline passes while the drain waits for the handler', async () => {
      const run = await startHeld()
      const { group, runner } = parts(run)
      const clear = jest.spyOn(group, 'clearAssignerState')

      group.stopRun(recreated())
      await jest.advanceTimersByTimeAsync(100)
      expect(runner.stopScope).not.toBeNull()
      const closing = settled(run.consumer.disconnect({ timeout: 1000 }))
      await jest.advanceTimersByTimeAsync(900)
      expect(closing.done).toBe(false)
      await advanceUntil(() => closing.done, 1000)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.handlerEnded()).toBe(false)
      expect(clear).toHaveBeenCalledTimes(1)
      expect(runner.listenerCount(HANDLERS_IDLE)).toBe(0)
      forget()
      run.held.resolve()
    }, 30000)

    it('when the deadline passed before the crash cleanup reached the stop', async () => {
      let consumer = null
      let closing = null
      const logger = createLogger({
        logCreator:
          () =>
          ({ log }) => {
            if (!closing && String(log.message).startsWith('Crash: '))
              closing = settled(consumer.disconnect({ timeout: 0 }))
          },
      })
      const run = await startHeld({ logger })
      consumer = run.consumer
      const { group, runner } = parts(run)
      const clear = jest.spyOn(group, 'clearAssignerState')

      group.stopRun(recreated())
      await advanceUntil(() => closing?.done, 1000)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.handlerEnded()).toBe(false)
      expect(clear).toHaveBeenCalledTimes(1)
      expect(runner.listenerCount(HANDLERS_IDLE)).toBe(0)
      forget()
      run.held.resolve()
    }, 30000)
  })

  describe('stopRun during an ordinary stop takes it over and drains', () => {
    it('a stop waiting for a running handler still waits for it before CRASH', async () => {
      const run = await startHeld()
      const { group } = parts(run)
      const clear = jest.spyOn(group, 'clearAssignerState')
      const stopping = settled(run.consumer.stop())
      await jest.advanceTimersByTimeAsync(100)
      expect(stopping.done).toBe(false)
      const error = recreated()

      group.stopRun(error)
      await jest.advanceTimersByTimeAsync(5000)

      expect(stopping.done).toBe(false)
      expect(run.crashes).toEqual([])
      expect(run.handlerEnded()).toBe(false)
      expect(clear).not.toHaveBeenCalled()

      run.held.resolve()
      await advanceUntil(() => stopping.done && run.crashes.length > 0)

      expect(stopping).toMatchObject({ done: true, value: undefined })
      expect(clear).toHaveBeenCalledTimes(1)
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
    }, 30000)

    it('a bounded disconnect keeps its deadline, and its drain waits for the handler until then', async () => {
      const run = await startHeld()
      const { group, runner } = parts(run)
      const clear = jest.spyOn(group, 'clearAssignerState')
      const disconnected = countDisconnects(run.consumer)
      const transport = run.cluster.disconnect.mock.calls.length
      const closing = settled(run.consumer.disconnect({ timeout: 2000 }))
      await jest.advanceTimersByTimeAsync(100)
      expect(closing.done).toBe(false)
      const error = recreated()

      group.stopRun(error)
      await jest.advanceTimersByTimeAsync(1899)

      expect(closing.done).toBe(false)
      expect(run.crashes).toEqual([])
      expect(clear).not.toHaveBeenCalled()

      await advanceUntil(() => closing.done, 100)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.handlerEnded()).toBe(false)
      expect(clear).toHaveBeenCalledTimes(1)
      expect(runner.listenerCount(HANDLERS_IDLE)).toBe(0)

      await advanceUntil(() => run.crashes.length > 0, 100)

      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(disconnected).toHaveBeenCalledTimes(1)
      expect(run.cluster.disconnect.mock.calls.slice(transport)).toEqual([[{ drainTimeout: 0 }]])
      forget()
      run.held.resolve()
    }, 30000)

    it('a classic stop that stopRun overtakes after its final commit clears the assigners once and does not leave', async () => {
      const run = await startOther('classic')
      const { runner, group } = run
      const clear = jest.spyOn(group, 'clearAssignerState')
      const commitFinal = runner.commitFinalOffsetsContained.bind(runner)
      const error = recreated()
      jest.spyOn(runner, 'commitFinalOffsetsContained').mockImplementationOnce(async (scope) => {
        await commitFinal(scope)
        group.stopRun(error)
      })

      const stopping = settled(run.consumer.stop())
      await advanceUntil(() => stopping.done && run.crashes.length > 0)

      expect(runner.commitFinalOffsetsContained).toHaveBeenCalledTimes(1)
      expect(stopping).toMatchObject({ done: true, value: undefined })
      expect(clear).toHaveBeenCalledTimes(1)
      expect(run.coordinator.leaveGroup).not.toHaveBeenCalled()
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
    }, 30000)

    it('a static classic member whose assigner calls stopRun while it is cleared keeps its member id and is cleared once', async () => {
      const run = await startOther('classic', { config: { groupInstanceId: 'instance-1' } })
      const { group } = run
      const error = recreated()
      const cleared = jest.fn((assignment) => {
        if (assignment === null) group.stopRun(error)
      })
      group.groupMembership.assigners[0].onAssignment = cleared
      const forgotten = jest.spyOn(group, 'forgetMemberId')

      const stopping = settled(run.consumer.stop())
      await advanceUntil(() => stopping.done && run.crashes.length > 0)

      expect(stopping).toMatchObject({ done: true, value: undefined })
      expect(cleared).toHaveBeenCalledTimes(1)
      expect(forgotten).not.toHaveBeenCalled()
      expect(group.memberId).toBe('member-1')
      expect(run.coordinator.leaveGroup).not.toHaveBeenCalled()
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
    }, 30000)
  })

  describe('a crash cleanup and a public close that overlaps it disconnect once', () => {
    it('a close asked while the crash cleanup waits for the drain owns the disconnect', async () => {
      const run = await startHeld()
      const disconnected = countDisconnects(run.consumer)
      const transport = run.cluster.disconnect.mock.calls.length
      const error = recreated()

      parts(run).group.stopRun(error)
      await jest.advanceTimersByTimeAsync(100)
      const closing = settled(run.consumer.disconnect())
      await jest.advanceTimersByTimeAsync(1000)

      expect(closing.done).toBe(false)
      expect(run.crashes).toEqual([])

      run.held.resolve()
      await advanceUntil(() => closing.done && run.crashes.length > 0, 1000)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(disconnected).toHaveBeenCalledTimes(1)
      expect(run.cluster.disconnect.mock.calls.slice(transport)).toEqual([[]])
      forget()
    }, 30000)

    it('a close asked while the crash cleanup disconnects only bounds that disconnect and leaves DISCONNECT to it', async () => {
      const run = await start()
      const disconnected = countDisconnects(run.consumer)
      const transport = run.cluster.disconnect.mock.calls.length
      const drained = deferred()
      run.cluster.disconnect.mockImplementation(async (options) => {
        if (options?.drainTimeout === 0) drained.resolve()
        await drained.promise
      })
      const error = recreated()

      parts(run).group.stopRun(error)
      await advanceUntil(() => run.cluster.disconnect.mock.calls.length > transport, 1000)
      const closing = settled(run.consumer.disconnect({ timeout: 1000 }))
      await jest.advanceTimersByTimeAsync(999)

      expect(closing.done).toBe(false)
      expect(disconnected).not.toHaveBeenCalled()

      await advanceUntil(() => closing.done && run.crashes.length > 0, 100)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(disconnected).toHaveBeenCalledTimes(1)
      expect(run.cluster.disconnect.mock.calls.slice(transport)).toEqual([
        [{ preserveSetupState: true }],
        [{ drainTimeout: 1000 }],
        [{ drainTimeout: 0 }],
      ])
      forget()
    }, 30000)

    it('a close asked while the crash cleanup disconnects emits DISCONNECT itself when that disconnect fails', async () => {
      const run = await start()
      const disconnected = countDisconnects(run.consumer)
      const transport = run.cluster.disconnect.mock.calls.length
      let failDrain = null
      const draining = new Promise((_, reject) => {
        failDrain = reject
      })
      run.cluster.disconnect.mockImplementation(async (options) => {
        if (options?.preserveSetupState) await draining
      })
      const error = recreated()

      parts(run).group.stopRun(error)
      await advanceUntil(() => run.cluster.disconnect.mock.calls.length > transport, 1000)
      const closing = settled(run.consumer.disconnect({ timeout: 1000 }))
      await jest.advanceTimersByTimeAsync(10)

      expect(closing.done).toBe(false)
      expect(run.crashes).toEqual([])
      expect(run.cluster.disconnect.mock.calls.slice(transport)).toEqual([
        [{ preserveSetupState: true }],
        [{ drainTimeout: 1000 }],
      ])

      failDrain(new Error('the drain failed'))
      await advanceUntil(() => closing.done && run.crashes.length > 0, 100)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(disconnected).toHaveBeenCalledTimes(1)
      forget()
    }, 30000)
  })

  describe('a failing drain stage still lets the crash cleanup uninstall the run', () => {
    const offsets = [{ topic: 'topic-a', partition: 0, offset: '41' }]
    const notInitialized = 'Consumer group was not initialized, consumer#run must be called first'

    const drainLines = (lines) =>
      lines.filter(({ message }) => message === 'Error while draining the consumer group')

    it('a classic assigner that throws while its state is cleared', async () => {
      const { logger, lines } = errorLogger()
      const run = await startOther('classic', { config: { logger } })
      const { assigners } = run.group.groupMembership
      const failing = jest.fn((assignment) => {
        if (assignment === null) throw new Error('the assigner failed')
      })
      const next = jest.fn()
      assigners[0].onAssignment = failing
      assigners.push({ name: 'next', onAssignment: next })
      const disconnects = run.cluster.disconnect.mock.calls.length
      const error = recreated()

      run.group.stopRun(error)
      await advanceUntil(() => run.crashes.length > 0)
      const after = settled((async () => run.consumer.commitOffsets(offsets))())
      await advanceUntil(() => after.done, 1000)

      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(failing).toHaveBeenCalledWith(null, { generationId: -1, groupProtocol: null })
      expect(next).toHaveBeenCalledWith(null, { generationId: -1, groupProtocol: null })
      expect(run.cluster.disconnect).toHaveBeenCalledTimes(disconnects + 1)
      expect(after.error.message).toBe(notInitialized)
      expect(drainLines(lines)).toEqual([
        { message: 'Error while draining the consumer group', error: 'the assigner failed' },
      ])
    }, 30000)

    it('a classic assigner that throws undefined is logged as the first failure, not a later one', async () => {
      const { logger, lines } = errorLogger()
      const run = await startOther('classic', { config: { logger } })
      const { assigners } = run.group.groupMembership
      const nothing = undefined
      const failing = jest.fn((assignment) => {
        if (assignment === null) throw nothing
      })
      const later = jest.fn((assignment) => {
        if (assignment === null) throw new Error('a later failure')
      })
      const next = jest.fn()
      assigners[0].onAssignment = failing
      assigners.push({ name: 'later', onAssignment: later }, { name: 'next', onAssignment: next })
      const disconnects = run.cluster.disconnect.mock.calls.length
      const error = recreated()

      run.group.stopRun(error)
      await advanceUntil(() => run.crashes.length > 0)
      const after = settled((async () => run.consumer.commitOffsets(offsets))())
      await advanceUntil(() => after.done, 1000)

      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(later).toHaveBeenCalledWith(null, { generationId: -1, groupProtocol: null })
      expect(next).toHaveBeenCalledWith(null, { generationId: -1, groupProtocol: null })
      expect(drainLines(lines)).toEqual([
        { message: 'Error while draining the consumer group', error: 'undefined' },
      ])
      expect(run.cluster.disconnect).toHaveBeenCalledTimes(disconnects + 1)
      expect(after.error.message).toBe(notInitialized)
    }, 30000)

    it('a rebalance chain that rejects', async () => {
      const run = await start()
      const { rc } = parts(run)
      jest.spyOn(rc, 'idle').mockRejectedValueOnce(new Error('the round failed'))
      const disconnects = run.cluster.disconnect.mock.calls.length
      const error = recreated()

      parts(run).group.stopRun(error)
      await advanceUntil(() => run.crashes.length > 0)
      const after = settled((async () => run.consumer.commitOffsets(offsets))())
      await advanceUntil(() => after.done, 1000)

      expect(rc.idle).toHaveBeenCalledTimes(1)
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(run.cluster.disconnect).toHaveBeenCalledTimes(disconnects + 1)
      expect(after.error.message).toBe(notInitialized)
    }, 30000)
  })

  it("the crash report's failure is logged, not unhandled", async () => {
    const { logger, lines } = recordingLogger()
    const run = await start({ logger })
    const { group, runner } = parts(run)
    const unhandled = jest.fn()
    process.on('unhandledRejection', unhandled)
    runner.onCrash = jest.fn(() => Promise.reject(new Error('the crash handler failed')))
    try {
      group.stopRun(recreated())
      await jest.advanceTimersByTimeAsync(100)
    } finally {
      process.off('unhandledRejection', unhandled)
    }

    expect(runner.onCrash).toHaveBeenCalledTimes(1)
    expect(lines.filter((line) => line === 'Crash handler failed')).toHaveLength(1)
    expect(unhandled).not.toHaveBeenCalled()
  }, 30000)

  describe('stopRun during a pending or in-flight rebalance starts and applies nothing', () => {
    it('a lost verdict resolved after it changes no membership', async () => {
      const run = await start()
      const { group, runner } = parts(run)
      const notified = jest.spyOn(group, 'onMembershipError')
      const discarded = jest.spyOn(group, 'discardUncommittedOffsets')
      group.stopRun(recreated())

      await runner.resolveRebalanceVerdict({ kind: 'lost', reason: 'lost', error: fenced() })

      expect(notified).not.toHaveBeenCalled()
      expect(discarded).not.toHaveBeenCalled()
    }, 30000)

    it('a revoked verdict waiting in the pre-rebalance commit changes no membership when it resumes', async () => {
      const run = await start()
      const { group, runner } = parts(run)
      const gate = deferred()
      let committing = false
      jest.spyOn(runner, 'autoCommitOffsets').mockImplementationOnce(async () => {
        committing = true
        await gate.promise
      })
      const notified = jest.spyOn(group, 'onMembershipError')
      const discarded = jest.spyOn(group, 'discardUncommittedOffsets')
      const rounds = RebalanceCoordinator.prototype.runRound.mock.calls.length
      const verdict = settled(
        runner.resolveRebalanceVerdict({
          kind: 'revoked',
          reason: 'revoked',
          error: protocolError(REBALANCE_IN_PROGRESS),
        })
      )
      await advanceUntil(() => committing, 1000)
      expect(verdict.done).toBe(false)

      group.stopRun(recreated())
      gate.resolve()
      await advanceUntil(() => verdict.done, 1000)

      expect(verdict).toMatchObject({ done: true, value: undefined })
      expect(notified).not.toHaveBeenCalled()
      expect(discarded).not.toHaveBeenCalled()
      expect(RebalanceCoordinator.prototype.runRound.mock.calls.length).toBe(rounds)
    }, 30000)

    it('a KIP-848 round resolving its target adopts and acknowledges nothing', async () => {
      const run = await start()
      const { group, membership } = parts(run)
      const gate = deferred()
      const resolve = membership.resolveAssignmentTopicNames
      let resolving = false
      jest
        .spyOn(membership, 'resolveAssignmentTopicNames')
        .mockImplementationOnce(async (...args) => {
          const resolved = await resolve.apply(membership, args)
          resolving = true
          await gate.promise
          return resolved
        })
      const answer = run.state.answer
      let moved = false
      run.state.answer = (req) => {
        if (req.memberEpoch <= 0 || moved) return answer(req)
        moved = true
        return target(2, { 'topic-a': [0, 1] })
      }
      await advanceUntil(() => resolving, 5000)
      jest.spyOn(membership, 'matchesOwned').mockReturnValue(true)
      const adopted = jest.spyOn(membership, 'adoptLocalAssignment')
      const renewed = jest.spyOn(group.ownership, 'renew')
      const error = recreated()

      group.stopRun(error)
      const sent = heartbeats(run)
      gate.resolve()
      await advanceUntil(() => run.crashes.length > 0)
      await jest.advanceTimersByTimeAsync(2000)

      expect(adopted).not.toHaveBeenCalled()
      expect(renewed).not.toHaveBeenCalled()
      expect(heartbeats(run)).toBe(sent)
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
    }, 30000)

    it('a lost round held in its listener preparation forgets no ownership', async () => {
      const run = await start({ hook: { onPartitionsLost: jest.fn() }, fault: fenced })
      const { rc, group } = parts(run)
      const gate = deferred()
      const prepare = rc.prepareListenerRound
      let prepared = false
      jest.spyOn(rc, 'prepareListenerRound').mockImplementationOnce(async (...args) => {
        await prepare.apply(rc, args)
        prepared = true
        await gate.promise
      })
      const forgotten = jest.spyOn(group, 'forgetOwnedPartitions')
      run.arm()
      await advanceUntil(() => prepared, 5000)
      const error = recreated()

      group.stopRun(error)
      gate.resolve()
      await advanceUntil(() => run.crashes.length > 0)
      await jest.advanceTimersByTimeAsync(2000)

      expect(forgotten).not.toHaveBeenCalled()
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
    }, 30000)
  })

  it('a recoverable KIP-848 lost round still completes (control)', async () => {
    const run = await start({ fault: fenced })
    const { membership } = parts(run)
    run.arm()
    await advanceUntil(() => run.faultAt() !== null)
    await advanceUntil(() => beats(run, false) === 2, 10000)
    await jest.advanceTimersByTimeAsync(2000)

    expect(beats(run, false)).toBe(2)
    expect(membership.memberEpoch).toBe(2)
    expect(membership.target).not.toBeNull()
    expect(run.crashes).toEqual([])
  }, 30000)

  it('a poll-expiry leave does not run after stopRun', async () => {
    const run = await start()
    const { rc, group, runner } = parts(run)
    const gate = deferred()
    let stopping = false
    jest.spyOn(runner.heartbeatManager, 'stop').mockImplementationOnce(async () => {
      stopping = true
      await gate.promise
    })
    const left = jest.spyOn(group, 'leaveForPollExpiry')
    const expiry = settled(rc.onPollExpired({ elapsedMs: 5000, isStatic: false }))
    await advanceUntil(() => stopping, 1000)

    group.stopRun(recreated())
    gate.resolve()
    await advanceUntil(() => expiry.done, 1000)
    await jest.advanceTimersByTimeAsync(1000)

    expect(expiry.done).toBe(true)
    expect(left).not.toHaveBeenCalled()
    expect(leaves(run)).toEqual([])
  }, 30000)

  it('a poll-expiry leave without stopRun still leaves and rejoins (control)', async () => {
    const run = await start()
    const { rc, group, runner, membership } = parts(run)
    const gate = deferred()
    const stop = runner.heartbeatManager.stop.bind(runner.heartbeatManager)
    let stopping = false
    jest.spyOn(runner.heartbeatManager, 'stop').mockImplementationOnce(async () => {
      stopping = true
      await gate.promise
      return stop()
    })
    const left = jest.spyOn(group, 'leaveForPollExpiry')
    const joins = beats(run, false)
    const expiry = settled(rc.onPollExpired({ elapsedMs: 5000, isStatic: false }))
    await advanceUntil(() => stopping, 1000)

    gate.resolve()
    await advanceUntil(() => expiry.done && beats(run, false) > joins, 10000)
    await jest.advanceTimersByTimeAsync(1000)

    expect(expiry).toMatchObject({ done: true, value: undefined })
    expect(left).toHaveBeenCalledTimes(1)
    expect(leaves(run)).toHaveLength(1)
    expect(beats(run, false)).toBe(joins + 1)
    expect(membership.memberEpoch).toBe(2)
    expect(run.crashes).toEqual([])
  }, 30000)

  describe('a poll-expiry leave overtaken by stopRun', () => {
    it('is not written when it reaches its write after stopRun', async () => {
      const run = await start()
      const { rc, group } = parts(run)
      const beat = run.coordinator.consumerGroupHeartbeat.getMockImplementation()
      const gate = deferred()
      let reached = false
      run.coordinator.consumerGroupHeartbeat.mockImplementation(async (request) => {
        if (request.memberEpoch < 0 && !reached) {
          reached = true
          await gate.promise
          const refused = request.guard?.()
          if (refused) throw refused
        }
        return beat(request)
      })
      const reset = jest.spyOn(group, 'resetGenerationLocally')
      const expiry = settled(rc.onPollExpired({ elapsedMs: 5000, isStatic: false }))
      await advanceUntil(() => reached, 1000)

      group.stopRun(recreated())
      gate.resolve()
      await advanceUntil(() => expiry.done, 1000)
      await jest.advanceTimersByTimeAsync(1000)

      expect(expiry).toMatchObject({ done: true, value: undefined })
      expect(leaves(run)).toEqual([])
      expect(reset).not.toHaveBeenCalled()
      expect(parts(run).membership.state).toBe('LEAVING')
    }, 30000)

    it.each([
      ['takes no effect from an answer that arrives after stopRun', (answer) => answer],
      [
        'takes no effect from a failure that answers it after stopRun',
        () => Promise.reject(fenced()),
      ],
    ])(
      '%s',
      async (_, reply) => {
        const run = await start()
        const { rc, group, membership } = parts(run)
        const gate = deferred()
        const written = holdLeaveAnswer(run, gate, reply)
        const reset = jest.spyOn(group, 'resetGenerationLocally')
        const expiry = settled(rc.onPollExpired({ elapsedMs: 5000, isStatic: false }))
        await advanceUntil(written, 1000)
        const resets = jest.spyOn(membership.sentFields, 'reset')
        const events = jest.fn()
        run.consumer.on(run.consumer.events.HEARTBEAT, events)

        group.stopRun(recreated())
        gate.resolve()
        await advanceUntil(() => expiry.done, 1000)
        await jest.advanceTimersByTimeAsync(1000)

        expect(expiry).toMatchObject({ done: true, value: undefined })
        expect(leaves(run)).toHaveLength(1)
        expect(events).not.toHaveBeenCalled()
        expect(reset).not.toHaveBeenCalled()
        expect(resets).not.toHaveBeenCalled()
        expect(membership.state).toBe('LEAVING')
      },
      30000
    )

    it('completes nothing when stopRun comes from its diagnostic that no coordinator is known', async () => {
      const noCoordinator = 'Leaving without a heartbeat: no coordinator is known'
      const sink = { onNoCoordinator: null }
      const logger = createLogger({
        level: LEVELS.DEBUG,
        logCreator:
          () =>
          ({ log }) => {
            if (log.message === noCoordinator) sink.onNoCoordinator?.()
          },
      })
      const run = await start({ logger })
      const { rc, group, runner, membership } = parts(run)
      const error = recreated()
      let resets = null
      sink.onNoCoordinator = () => {
        sink.onNoCoordinator = null
        group.stopRun(error)
        resets = jest.spyOn(membership.sentFields, 'reset')
      }
      const stop = runner.heartbeatManager.stop.bind(runner.heartbeatManager)
      jest.spyOn(runner.heartbeatManager, 'stop').mockImplementationOnce(async () => {
        await stop()
        membership.coordinator = null
      })
      const reset = jest.spyOn(group, 'resetGenerationLocally')
      const rejoin = jest.spyOn(rc, 'requestRejoin')
      const expiry = settled(rc.onPollExpired({ elapsedMs: 5000, isStatic: false }))
      await advanceUntil(() => expiry.done && run.crashes.length > 0, 1000)

      expect(expiry).toMatchObject({ done: true, value: undefined })
      expect(resets).not.toBeNull()
      expect(resets).not.toHaveBeenCalled()
      expect(leaves(run)).toEqual([])
      expect(membership.state).toBe('LEAVING')
      expect(reset).not.toHaveBeenCalled()
      expect(rejoin).not.toHaveBeenCalled()
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
    }, 30000)
  })

  describe('a fatal answer reserved during a poll-expiry leave, without stopRun', () => {
    it('lets a leave answered after it complete nothing', async () => {
      const run = await start()
      const { rc, group, runner, membership } = parts(run)
      const gate = deferred()
      const written = holdLeaveAnswer(run, gate, (answer) => answer)
      const reset = jest.spyOn(group, 'resetGenerationLocally')
      const rejoin = jest.spyOn(rc, 'requestRejoin')
      const expiry = settled(rc.onPollExpired({ elapsedMs: 5000, isStatic: false }))
      await advanceUntil(written, 1000)

      runner.reserveFatal(groupAuthorization())
      expect(runner.fatal).toMatchObject({ claimed: false })
      gate.resolve()
      await advanceUntil(() => expiry.done, 1000)

      expect(expiry).toMatchObject({ done: true, value: undefined })
      expect(leaves(run)).toHaveLength(1)
      expect(reset).not.toHaveBeenCalled()
      expect(rejoin).not.toHaveBeenCalled()
      expect(membership.state).toBe('LEAVING')
    }, 30000)

    it('requests no rejoin when it is reserved during the generation reset', async () => {
      const run = await start()
      const { rc, group, runner } = parts(run)
      const resetLocally = group.resetGenerationLocally.bind(group)
      jest.spyOn(group, 'resetGenerationLocally').mockImplementationOnce(() => {
        resetLocally()
        runner.reserveFatal(groupAuthorization())
      })
      const rejoin = jest.spyOn(rc, 'requestRejoin')
      const expiry = settled(rc.onPollExpired({ elapsedMs: 5000, isStatic: false }))
      await advanceUntil(() => expiry.done, 1000)

      expect(expiry).toMatchObject({ done: true, value: undefined })
      expect(leaves(run)).toHaveLength(1)
      expect(group.resetGenerationLocally).toHaveBeenCalledTimes(1)
      expect(runner.fatal).toMatchObject({ claimed: false })
      expect(rejoin).not.toHaveBeenCalled()
    }, 30000)
  })

  it('stopRun sends no LeaveGroup and does not call the close callback', async () => {
    const hook = { onPartitionsRevoked: jest.fn(), onPartitionsLost: jest.fn() }
    const run = await start({ hook })

    parts(run).group.stopRun(recreated())
    await advanceUntil(() => run.crashes.length > 0)
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes).toHaveLength(1)
    expect(leaves(run)).toEqual([])
    expect(hook.onPartitionsRevoked).not.toHaveBeenCalled()
    expect(hook.onPartitionsLost).not.toHaveBeenCalled()
  }, 30000)

  it('a target evaluation after stopRun does not turn the membership fatal', async () => {
    const run = await startHeld()
    const { group, membership } = parts(run)
    const refresh = deferred()
    run.cluster.refreshMetadataForTopicIds.mockImplementationOnce(() => refresh.promise)
    const reply = target(2, { 'topic-a': [0] })
    reply.assignment.topicPartitions.push({ topicId: UNKNOWN_ID, partitions: [0] })
    const answer = run.state.answer
    let moved = false
    run.state.answer = (req) => {
      if (req.memberEpoch <= 0 || moved) return answer(req)
      moved = true
      return reply
    }
    await advanceUntil(() => run.cluster.refreshMetadataForTopicIds.mock.calls.length > 0, 5000)

    group.stopRun(recreated())
    refresh.resolve(Promise.reject(protocolError(17)))
    await jest.advanceTimersByTimeAsync(1000)

    expect(run.cluster.refreshMetadataForTopicIds).toHaveBeenCalledTimes(1)
    expect(membership.state).not.toBe('FATAL')
    expect(membership.fatalError).toBeNull()
  }, 30000)

  it('stopRun twice keeps the first error', async () => {
    const run = await start()
    const { group } = parts(run)
    const first = recreated()

    group.stopRun(first)
    group.stopRun(transient())
    await advanceUntil(() => run.crashes.length > 0)
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes).toHaveLength(1)
    expect(run.crashes[0].error).toBe(first)
  }, 30000)

  it('stopRun after a forced close does nothing', async () => {
    const run = await start()
    const { group } = parts(run)
    const closing = settled(run.consumer.disconnect({ timeout: 0 }))
    await advanceUntil(() => closing.done, 1000)
    expect(closing).toMatchObject({ done: true, value: undefined })

    group.stopRun(recreated())
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes).toEqual([])
    forget()
  }, 30000)

  it('a retriable error passed to stopRun is not retried in place', async () => {
    const run = await start()
    const error = transient()
    const at = Date.now()
    const sent = requests(run)
    const rounds = RebalanceCoordinator.prototype.runRound.mock.calls.length

    parts(run).group.stopRun(error)
    await advanceUntil(() => run.crashes.length > 0, 1000)
    const crashedAt = Date.now()
    await jest.advanceTimersByTimeAsync(5000)

    expect(run.crashes).toHaveLength(1)
    expect(run.crashes[0].error).toBe(error)
    expect(crashedAt - at).toBeLessThan(300)
    expect(requests(run)).toBe(sent)
    expect(RebalanceCoordinator.prototype.runRound.mock.calls.length).toBe(rounds)
    expect(run.cluster.connect).toHaveBeenCalledTimes(1)
  }, 30000)

  describe.each([
    ['the classic protocol', 'classic'],
    ['manual assignment', 'manual'],
  ])('under %s', (_, kind) => {
    it('after stopRun no Fetch request is sent and an answered Fetch delivers nothing', async () => {
      const delivered = []
      const run = await startOther(kind, {
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      await advanceUntil(() => delivered.length === 1)
      const broker = await run.cluster.findBroker()
      const fetch = broker.fetch.getMockImplementation()
      const gate = deferred()
      const before = broker.fetch.mock.calls.length
      broker.fetch.mockImplementationOnce(async (request) => {
        await gate.promise
        return fetch(request)
      })
      await advanceUntil(() => broker.fetch.mock.calls.length > before)
      run.state.queue['topic-a|0'] = [40, 41]
      const error = recreated()

      run.group.stopRun(error)
      const sent = broker.fetch.mock.calls.length
      gate.resolve()
      await advanceUntil(() => run.crashes.length > 0)
      await jest.advanceTimersByTimeAsync(5000)

      expect(delivered).toEqual(['40'])
      expect(broker.fetch.mock.calls.length).toBe(sent)
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
    }, 30000)

    it('after stopRun no OffsetCommit is written', async () => {
      const gate = deferred()
      let entered = false
      const run = await startOther(kind, {
        autoCommitThreshold: 1,
        eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
          entered = true
          resolveOffset(batch.messages[0].offset)
          await gate.promise
          await commitOffsetsIfNecessary().catch(() => {})
        },
      })
      await advanceUntil(() => entered)
      const commits = run.coordinator.offsetCommit.mock.calls.length
      const error = recreated()

      run.group.stopRun(error)
      await jest.advanceTimersByTimeAsync(6000)
      gate.resolve()
      await advanceUntil(() => run.crashes.length > 0)
      await jest.advanceTimersByTimeAsync(10000)

      expect(run.coordinator.offsetCommit).toHaveBeenCalledTimes(commits)
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
    }, 30000)

    it('an automatic commit made due by its threshold is written without stopRun (control)', async () => {
      const gate = deferred()
      let entered = false
      let committed = false
      const run = await startOther(kind, {
        autoCommitThreshold: 1,
        eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary }) => {
          entered = true
          resolveOffset(batch.messages[0].offset)
          await gate.promise
          await commitOffsetsIfNecessary()
          committed = true
        },
      })
      await advanceUntil(() => entered)
      const commits = run.coordinator.offsetCommit.mock.calls.length

      gate.resolve()
      await advanceUntil(() => committed, 1000)

      expect(run.coordinator.offsetCommit.mock.calls.length).toBeGreaterThan(commits)
      const [{ topics }] = run.coordinator.offsetCommit.mock.calls[commits]
      expect(String(topics[0].partitions[0].offset)).toBe('41')
      expect(run.crashes).toEqual([])
    }, 30000)
  })

  describe('a classic member', () => {
    /** Starts a classic run whose eachMessage parks on `held`, and waits until it has entered. */
    const startHeldClassic = async (options = {}) => {
      let entered = false
      const run = await startOther('classic', {
        ...options,
        eachMessage: async (_, held) => {
          entered = true
          await held.promise
        },
      })
      await advanceUntil(() => entered)
      expect(entered).toBe(true)
      return run
    }

    it.each([
      ['a success', () => Promise.resolve()],
      ['a coordinator error', () => Promise.reject(protocolError(NOT_COORDINATOR))],
    ])(
      'after stopRun a heartbeat in flight answered with %s changes nothing',
      async (_, reply) => {
        const run = await startHeldClassic()
        const { coordinator, group } = run
        const membership = group.groupMembership
        const gate = deferred()
        const sent = coordinator.heartbeat.mock.calls.length
        coordinator.heartbeat.mockImplementationOnce(async () => {
          await gate.promise
          return reply()
        })
        await advanceUntil(() => coordinator.heartbeat.mock.calls.length > sent)
        expect(coordinator.heartbeat).toHaveBeenCalledTimes(sent + 1)
        const lastRequest = membership.lastRequest
        expect(membership.coordinatorUnknown).toBe(false)
        const events = jest.fn()
        run.consumer.on(run.consumer.events.HEARTBEAT, events)

        group.stopRun(recreated())
        gate.resolve()
        await jest.advanceTimersByTimeAsync(10000)

        expect(events).not.toHaveBeenCalled()
        expect(membership.lastRequest).toBe(lastRequest)
        expect(membership.coordinatorUnknown).toBe(false)
        expect(coordinator.heartbeat).toHaveBeenCalledTimes(sent + 1)
        expect(run.crashes).toEqual([])
      },
      30000
    )

    it('a heartbeat refused at its write with the stop error does not mark the coordinator unknown', async () => {
      const run = await startHeldClassic()
      const { coordinator, group } = run
      const membership = group.groupMembership
      const gate = deferred()
      const sent = coordinator.heartbeat.mock.calls.length
      coordinator.heartbeat.mockImplementationOnce(async ({ guard }) => {
        await gate.promise
        const refused = guard?.()
        if (refused) throw refused
      })
      await advanceUntil(() => coordinator.heartbeat.mock.calls.length > sent)
      expect(coordinator.heartbeat).toHaveBeenCalledTimes(sent + 1)
      expect(membership.coordinatorUnknown).toBe(false)
      const events = jest.fn()
      run.consumer.on(run.consumer.events.HEARTBEAT, events)

      group.stopRun(new KafkaJSConnectionError('The coordinator connection was closed'))
      gate.resolve()
      await jest.advanceTimersByTimeAsync(10000)

      expect(membership.coordinatorUnknown).toBe(false)
      expect(events).not.toHaveBeenCalled()
      expect(coordinator.heartbeat).toHaveBeenCalledTimes(sent + 1)
      expect(run.crashes).toEqual([])
    }, 30000)

    describe('a poll-expiry LeaveGroup overtaken by stopRun', () => {
      it('is not written when its coordinator wait ends after stopRun', async () => {
        const run = await startOther('classic')
        const { runner, group, coordinator } = run
        const membership = group.groupMembership
        const stop = runner.heartbeatManager.stop.bind(runner.heartbeatManager)
        const live = membership.liveCoordinator.bind(membership)
        const gate = deferred()
        let armed = false
        let looking = false
        jest.spyOn(runner.heartbeatManager, 'stop').mockImplementationOnce(async () => {
          await stop()
          armed = true
        })
        jest.spyOn(membership, 'liveCoordinator').mockImplementation(async (operation) => {
          if (armed && !looking) {
            looking = true
            await gate.promise
          }
          return live(operation)
        })
        const reset = jest.spyOn(group, 'resetGenerationLocally')
        const clear = jest.spyOn(membership, 'clearAssignerState')
        const expiry = settled(
          runner.rebalanceCoordinator.onPollExpired({ elapsedMs: 5000, isStatic: false })
        )
        await advanceUntil(() => looking, 1000)

        group.stopRun(recreated())
        gate.resolve()
        await advanceUntil(() => expiry.done, 1000)
        await jest.advanceTimersByTimeAsync(1000)

        expect(expiry).toMatchObject({ done: true, value: undefined })
        expect(coordinator.leaveGroup).not.toHaveBeenCalled()
        expect(reset).not.toHaveBeenCalled()
        expect(membership.memberId).toBe('member-1')
        expect(run.crashes).toHaveLength(1)
        expect(clear).toHaveBeenCalledTimes(1)
      }, 30000)

      it.each([
        ['resets no generation when its answer arrives after stopRun', () => Promise.resolve()],
        [
          'keeps its member and assigners when a failure answers it after stopRun',
          () => Promise.reject(protocolError(NOT_COORDINATOR)),
        ],
      ])(
        '%s',
        async (_, reply) => {
          const run = await startOther('classic')
          const { runner, group, coordinator } = run
          const membership = group.groupMembership
          const gate = deferred()
          let written = false
          coordinator.leaveGroup.mockImplementationOnce(async () => {
            written = true
            await gate.promise
            return reply()
          })
          const reset = jest.spyOn(group, 'resetGenerationLocally')
          const clear = jest.spyOn(membership, 'clearAssignerState')
          const expiry = settled(
            runner.rebalanceCoordinator.onPollExpired({ elapsedMs: 5000, isStatic: false })
          )
          await advanceUntil(() => written, 1000)

          group.stopRun(recreated())
          gate.resolve()
          await advanceUntil(() => expiry.done, 1000)
          await jest.advanceTimersByTimeAsync(1000)

          expect(expiry).toMatchObject({ done: true, value: undefined })
          expect(coordinator.leaveGroup).toHaveBeenCalledTimes(1)
          expect(reset).not.toHaveBeenCalled()
          expect(membership.memberId).toBe('member-1')
          expect(run.crashes).toHaveLength(1)
          expect(clear).toHaveBeenCalledTimes(1)
        },
        30000
      )
    })

    it('an ordinary stop whose assigner throws undefined while it is cleared settles and logs it', async () => {
      const { logger, lines } = errorLogger()
      const run = await startOther('classic', { config: { logger } })
      const nothing = undefined
      run.group.groupMembership.assigners[0].onAssignment = (assignment) => {
        if (assignment === null) throw nothing
      }

      const stopping = settled(run.consumer.stop())
      await advanceUntil(() => stopping.done, 2000)

      expect(stopping).toMatchObject({ done: true, value: undefined })
      expect(
        lines.filter(({ message }) => message === 'Error while stopping the consumer group')
      ).toEqual([{ message: 'Error while stopping the consumer group', error: 'undefined' }])
    }, 30000)

    it('a bounded close whose deadline passes after its accepted final commit clears the assigners before the first JoinGroup of the next run', async () => {
      let retained = null
      const retaining = {
        ...eager(),
        protocol: () => ({ name: 'eager', metadata: Buffer.from(JSON.stringify(retained)) }),
        onAssignment: (assignment, { generationId }) => {
          retained = assignment && { partitions: assignment.partitions, generationId }
        },
      }
      const delivered = []
      const run = await startOther('classic', {
        config: { partitionAssigners: [() => retaining] },
        autoCommitThreshold: 2,
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      const { runner, coordinator } = run
      const manager = run.group.offsetManager
      await advanceUntil(() => manager.selectUncommitted().length > 0)

      expect(delivered).toEqual(['40'])
      expect(coordinator.offsetCommit).not.toHaveBeenCalled()
      expect(manager.selectUncommitted()).toMatchObject([
        { topic: 'topic-a', partitions: [{ partition: '0', offset: '41' }] },
      ])
      expect(retained).toMatchObject({ generationId: 1 })
      const commitFinal = runner.commitFinalOffsetsContained.bind(runner)
      let written = null
      jest.spyOn(runner, 'commitFinalOffsetsContained').mockImplementationOnce(async (scope) => {
        await commitFinal(scope)
        written = coordinator.offsetCommit.mock.calls.length
        await new Promise((resolve) => setTimeout(resolve, 600))
      })
      const commits = coordinator.offsetCommit.mock.calls.length

      const closing = settled(run.consumer.disconnect({ timeout: 500 }))
      await advanceUntil(() => closing.done, 2000)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(runner.commitFinalOffsetsContained).toHaveBeenCalledTimes(1)
      expect(written).toBe(commits + 1)
      const [{ topics }] = coordinator.offsetCommit.mock.calls[commits]
      expect(String(topics[0].partitions[0].offset)).toBe('41')
      expect(coordinator.leaveGroup).not.toHaveBeenCalled()
      expect(retained).toMatchObject({ generationId: 1 })

      const joins = coordinator.joinGroup.mock.calls.length
      const again = settled(run.consumer.run({ eachMessage: async () => {} }))
      await advanceUntil(() => again.done && coordinator.joinGroup.mock.calls.length > joins, 2000)

      expect(again).toMatchObject({ done: true, value: undefined })
      expect(coordinator.joinGroup.mock.calls[joins][0].groupProtocols).toEqual([
        { name: 'eager', metadata: Buffer.from('null') },
      ])
    }, 30000)

    it('a stop called by the assigner while the next run clears it is honoured: that run sends no JoinGroup', async () => {
      let retained = null
      let interrupt = false
      let stopping = null
      const retaining = {
        ...eager(),
        protocol: () => ({ name: 'eager', metadata: Buffer.from(JSON.stringify(retained)) }),
        onAssignment: (assignment, { generationId }) => {
          retained = assignment && { partitions: assignment.partitions, generationId }
          if (assignment === null && interrupt) {
            interrupt = false
            stopping = settled(run.consumer.stop())
          }
        },
      }
      const delivered = []
      const run = await startOther('classic', {
        config: { partitionAssigners: [() => retaining] },
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      await advanceUntil(() => delivered.length === 1)
      const { runner, coordinator } = run
      const commitFinal = runner.commitFinalOffsetsContained.bind(runner)
      jest.spyOn(runner, 'commitFinalOffsetsContained').mockImplementationOnce(async (scope) => {
        await commitFinal(scope)
        await new Promise((resolve) => setTimeout(resolve, 600))
      })

      const closing = settled(run.consumer.disconnect({ timeout: 500 }))
      await advanceUntil(() => closing.done, 2000)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(coordinator.leaveGroup).not.toHaveBeenCalled()
      expect(retained).toMatchObject({ generationId: 1 })

      const joins = coordinator.joinGroup.mock.calls.length
      interrupt = true
      const again = settled(run.consumer.run({ eachMessage: async () => {} }))
      await advanceUntil(() => again.done && stopping?.done === true, 2000)
      await jest.advanceTimersByTimeAsync(5000)

      expect(stopping).toMatchObject({ done: true, value: undefined })
      expect(again).toMatchObject({ done: true, value: undefined })
      expect(coordinator.joinGroup).toHaveBeenCalledTimes(joins)
      expect(retained).toBeNull()
    }, 30000)

    it('stopRun during the initial join reports its own error and a later JoinGroup answer syncs nothing', async () => {
      const joined = deferred()
      const run = await startOther('classic', { joined })
      const error = recreated()

      run.group.stopRun(error)
      joined.resolve()
      await advanceUntil(() => run.started.done && run.crashes.length > 0)
      await jest.advanceTimersByTimeAsync(10000)

      expect(run.started).toMatchObject({ done: true, value: undefined })
      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(run.crashes[0].restart).toBe(false)
      expect(run.coordinator.syncGroup).not.toHaveBeenCalled()
      expect(run.coordinator.heartbeat).not.toHaveBeenCalled()
      expect(run.log.filter(({ kind }) => kind === 'fetch')).toEqual([])
    }, 30000)

    it('stopRun sends no LeaveGroup and does not call the close callback', async () => {
      const hook = { onPartitionsRevoked: jest.fn(), onPartitionsLost: jest.fn() }
      const run = await startOther('classic', { hook })
      expect(hook.onPartitionsRevoked.mock.calls).toEqual([
        [{ groupId: 'loss-driver-group', generationId: null, memberId: null, partitions: [] }],
      ])
      expect(hook.onPartitionsLost).not.toHaveBeenCalled()
      expect(run.group.assigned()).toEqual([{ topic: 'topic-a', partitions: [0] }])
      hook.onPartitionsRevoked.mockClear()

      const error = recreated()
      run.group.stopRun(error)
      await advanceUntil(() => run.crashes.length > 0)
      await jest.advanceTimersByTimeAsync(5000)

      expect(run.crashes).toHaveLength(1)
      expect(run.crashes[0].error).toBe(error)
      expect(run.coordinator.leaveGroup).not.toHaveBeenCalled()
      expect(hook.onPartitionsRevoked).not.toHaveBeenCalled()
      expect(hook.onPartitionsLost).not.toHaveBeenCalled()
    }, 30000)
  })
})
