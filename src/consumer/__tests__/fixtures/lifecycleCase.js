const { Kafka } = require('../../../../index')
const Runner = require('../../runner')
const RequestQueue = require('../../../network/requestQueue')
const Cluster = require('../../../cluster')
const Connection = require('../../../network/connection')
const ConnectionPool = require('../../../network/connectionPool')
const { Schedule } = require('../../../../testHelpers/schedule')
const {
  createScriptedCluster,
  groupScript,
  consumerProtocolScript,
} = require('../../../../testHelpers/scriptedCluster')
const { cooperativeSticky } = require('../../assigners')
const { MemberAssignment } = require('../../assignerProtocol')
const { TOPIC, prepareRecordBatches } = require('./fetchHarness')
const { KafkaJSCommitFailedError, KafkaJSConnectionClosedError } = require('../../../errors')

/**
 * Advances fake time with real socket-delivery turns, bounded by a deterministic step budget.
 * @param {() => boolean} predicate
 * @param {string} label
 * @returns {Promise<void>}
 * @throws {Error} if the requested boundary never arrives
 */
const pump = async (predicate, label) => {
  for (let step = 0; step < 4000; step++) {
    if (predicate()) return
    await new Promise(setImmediate)
    await jest.advanceTimersByTimeAsync(1)
  }
  throw new Error(`Lifecycle boundary did not settle: ${label}`)
}

/**
 * Internal calls can settle with the stopping error or the closed connection on forced close.
 * Public commits must instead report CONSUMER_STOPPING before held application work is released.
 * @param {Error} error how the held call settled
 * @returns {boolean} true when the error is one of the forced close's two retirements
 */
const isForcedRetirement = (error) =>
  error.type === 'CONSUMER_STOPPING' || error instanceof KafkaJSConnectionClosedError

/**
 * Drives one reviewed lifecycle cell through public factories and the real wire stack. Spies
 * hold existing boundaries; they add no production export or alternate membership machine.
 * @param {{ event: string, boundary: string, protocol: string, membership: string }} cell
 * @returns {Promise<void>}
 */
const runLifecycleCell = async (cell) => {
  const schedule = new Schedule()
  const held = schedule.hold({ boundary: cell.boundary })
  const release = () => {
    if (held.settle) held.release()
  }
  const brokerAnswerTimers = new Set()
  const scripted = (cell.protocol === 'consumer' ? consumerProtocolScript : groupScript)(
    createScriptedCluster({
      nodes: [{ id: 0, host: 'node', port: 9092 }],
      profile: cell.protocol === 'consumer' ? 'consumer' : 'default',
    }),
    { batches: 1, brokerAnswerTimers }
  )
  const errors = []
  const unhandled = (error) => errors.push(error)
  const baseline = {
    timers: jest.getTimerCount(),
    unhandled: process.listenerCount('unhandledRejection'),
  }
  process.on('unhandledRejection', unhandled)
  let runner
  let fetchRounds
  let consumer
  let enabled = false
  let used = false
  let operation
  let starting
  let initialGate
  const spies = []
  const cleanup = []
  const pending = []
  const settled = (promise) => {
    const state = { done: false, error: null }
    state.promise = promise.then(
      (value) => {
        state.done = true
        return value
      },
      (error) => {
        state.done = true
        state.error = error
      }
    )
    pending.push(state)
    return state
  }
  const at = async () => {
    if (!enabled || used) return
    used = true
    await schedule.run({ boundary: cell.boundary }, () => undefined)
  }
  const wrap = (target, method, before, select = () => true) => {
    const original = target[method]
    spies.push(
      jest.spyOn(target, method).mockImplementation(async function (...args) {
        if (before && select(...args)) await at()
        const result = await original.apply(this, args)
        if (!before && select(...args)) await at()
        return result
      })
    )
  }
  const capture = Runner.prototype.start
  spies.push(
    jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
      runner = this
      return capture.apply(this, args)
    })
  )
  const writes = []
  const mark = Cluster.prototype.markOffsetAsCommitted
  spies.push(
    jest.spyOn(Cluster.prototype, 'markOffsetAsCommitted').mockImplementation(function (value) {
      writes.push(value)
      return mark.call(this, value)
    })
  )
  const queues = new Set()
  const push = RequestQueue.prototype.push
  spies.push(
    jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (...args) {
      queues.add(this)
      return push.apply(this, args)
    })
  )
  const running = []
  const losses = []
  const eventRemovers = []
  const listeners = () =>
    [...queues].reduce(
      (count, queue) =>
        count + queue.eventNames().reduce((sum, event) => sum + queue.listenerCount(event), 0),
      0
    ) +
    (runner
      ? runner.instrumentationEmitter.emitter
          .eventNames()
          .reduce(
            (count, event) => count + runner.instrumentationEmitter.emitter.listenerCount(event),
            0
          )
      : 0)
  const socketBaseline = () => scripted.sockets().filter((socket) => !socket.destroyed).length
  const beforeSockets = socketBaseline()
  const sessions = cell.boundary.startsWith('session:')
  if (sessions) {
    let fetches = 0
    scripted.answer('Fetch', async ({ maxWaitMs, topics }) => {
      fetches += 1
      if (fetches > 1)
        await new Promise((resolve) => {
          const timer = setTimeout(() => {
            brokerAnswerTimers.delete(timer)
            resolve()
          }, maxWaitMs)
          brokerAnswerTimers.add(timer)
        })
      return {
        sessionId: 7,
        includeTopic: topics.length > 0,
        partitions: (topics[0]?.partitions ?? []).map(({ partition }) => ({
          partition,
          errorCode: 0,
          records: fetches <= 1,
        })),
      }
    })
  }
  const stage = cell.boundary.startsWith('hook:') ? cell.boundary.slice('hook:'.length) : null
  const hooks = stage
    ? Object.fromEntries(
        ['assigned', 'revoked', 'lost'].map((name) => [
          `onPartitions${name[0].toUpperCase()}${name.slice(1)}`,
          async () => {
            if (name === stage) await at()
          },
        ])
      )
    : null
  let failed = false
  try {
    await prepareRecordBatches()
    const options =
      cell.protocol === 'consumer'
        ? { groupProtocol: 'consumer' }
        : {
            heartbeatInterval: 100,
            partitionAssigners:
              cell.protocol === 'classic-cooperative' ? [cooperativeSticky] : undefined,
          }
    consumer = new Kafka(
      scripted.kafkaOptions({
        maxInFlightRequests: 1,
        requestTimeout: 20000,
        retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
      })
    ).consumer({
      groupId: 'lifecycle',
      maxInFlightRequests: 1,
      groupInstanceId: cell.membership === 'static' ? 'static-instance' : null,
      maxWaitTimeInMs: 20,
      fetchSessions: sessions,
      ...options,
      ...(cell.event === 'crash' && ['in-join', 'in-sync'].includes(cell.boundary)
        ? { retry: { retries: 0, initialRetryTime: 1, restartOnFailure: async () => true } }
        : {}),
    })
    eventRemovers.push(
      consumer.on(consumer.events.PARTITIONS_LOST, () => {
        losses.push({ from: scripted.requests().length, to: null })
      })
    )
    eventRemovers.push(
      consumer.on(consumer.events.GROUP_JOIN, () => {
        for (const loss of losses) if (loss.to === null) loss.to = scripted.requests().length
        running.push({
          timers: jest.getTimerCount(),
          sockets: socketBaseline(),
          listeners: listeners(),
        })
      })
    )
    if (cell.boundary === 'before-handler') wrap(Runner.prototype, 'handleBatch', true)
    if (cell.boundary === 'in-join' || cell.boundary === 'in-sync') {
      const gate = scripted.schedule.hold({
        api: cell.boundary === 'in-join' ? 'JoinGroup' : 'SyncGroup',
      })
      cleanup.push(() => {
        if (gate.settle) gate.release()
      })
      initialGate = gate
      gate.arrived().then(() => at())
      enabled = true
    }
    if (cell.boundary === 'session:initial') {
      const gate = scripted.schedule.hold({ api: 'Fetch' })
      cleanup.push(() => {
        if (gate.settle) gate.release()
      })
      gate.arrived().then(() => at())
    }
    if (
      ['before-handler', 'in-handler', 'hook:assigned', 'session:initial'].includes(cell.boundary)
    )
      enabled = true
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    starting = settled(
      consumer.run({
        autoCommit: cell.boundary === 'pre-release-commit',
        eachBatch: async () => {
          if (cell.boundary === 'in-handler') await at()
        },
        ...(hooks ? { rebalanceListener: hooks } : {}),
      })
    )
    await pump(() => runner != null, 'runner capture')
    fetchRounds = jest.spyOn(runner, 'fetch')
    if (
      ![
        'in-join',
        'in-sync',
        'before-handler',
        'in-handler',
        'hook:assigned',
        'session:initial',
      ].includes(cell.boundary)
    ) {
      await pump(
        () => starting.done && runner.consumerGroup.offsetManager != null,
        'first assignment'
      )
      if (starting.error) throw starting.error
      enabled = true
      const group = runner.consumerGroup
      if (cell.boundary === 'session:inflight') {
        const gate = scripted.schedule.hold({
          api: 'Fetch',
          match: ({ fields }) => fields.sessionId === 7,
        })
        cleanup.push(() => {
          if (gate.settle) gate.release()
        })
        gate.arrived().then(() => at())
      }
      if (stage === 'revoked' && cell.protocol === 'classic-cooperative')
        scripted.schedule.once(
          { api: 'SyncGroup' },
          {
            memberAssignment: MemberAssignment.encode({ version: 1, assignment: {} }).toString(
              'hex'
            ),
          }
        )
      if (stage === 'revoked' && cell.protocol === 'consumer')
        scripted.schedule.once({ api: 'ConsumerGroupHeartbeat' }, ({ fields }) => ({
          memberId: fields.memberId,
          memberEpoch: fields.memberEpoch + 1,
          heartbeatIntervalMs: 100,
          assignment: { topicPartitions: [] },
        }))
      if (stage === 'revoked' || stage === 'lost') {
        operation = settled(
          runner.handleRebalanceError({
            kind: stage,
            reason: stage === 'lost' ? 'lost' : 'assignment',
            error: new Error(stage),
          })
        )
      }
      if (cell.boundary === 'commit:coordinator-lookup') {
        group.offsetManager.coordinator = { isConnected: () => false }
        wrap(Cluster.prototype, 'findGroupCoordinator', true)
      }
      if (cell.boundary === 'commit:pool-connect')
        wrap(ConnectionPool.prototype, 'getConnection', true)
      if (cell.boundary === 'commit:auth-encode') {
        const original = Connection.prototype.send
        spies.push(
          jest.spyOn(Connection.prototype, 'send').mockImplementation(function (protocol) {
            if (protocol.request.apiName === 'OffsetCommit') {
              const encode = protocol.request.encode
              protocol = {
                ...protocol,
                request: {
                  ...protocol.request,
                  encode: async (...args) => {
                    await at()
                    return encode(...args)
                  },
                },
              }
            }
            return original.call(this, protocol)
          })
        )
      }
      if (
        [
          'commit:answer-held',
          'in-heartbeat',
          'in-leave',
          'poll-expiry:leave',
          'pre-release-commit',
        ].includes(cell.boundary) &&
        !(
          cell.boundary === 'poll-expiry:leave' &&
          cell.membership === 'static' &&
          cell.protocol !== 'consumer'
        )
      ) {
        const api =
          cell.boundary === 'in-heartbeat'
            ? cell.protocol === 'consumer'
              ? 'ConsumerGroupHeartbeat'
              : 'Heartbeat'
            : ['in-leave', 'poll-expiry:leave'].includes(cell.boundary)
              ? cell.protocol === 'consumer'
                ? 'ConsumerGroupHeartbeat'
                : 'LeaveGroup'
              : 'OffsetCommit'
        const gate = scripted.schedule.hold({
          api,
          ...(api === 'ConsumerGroupHeartbeat' &&
          ['in-leave', 'poll-expiry:leave'].includes(cell.boundary)
            ? { match: (record) => record.fields.memberEpoch < 0 }
            : {}),
        })
        cleanup.push(() => {
          if (gate.settle) gate.release()
        })
        gate.arrived().then(() => at())
      }
      if (
        cell.boundary === 'poll-expiry:leave' &&
        cell.membership === 'static' &&
        cell.protocol !== 'consumer'
      )
        wrap(group, 'resetGenerationLocally', true)
      if (cell.boundary === 'commit:retry-backoff') {
        scripted.schedule.once(
          { api: 'OffsetCommit' },
          { topics: [{ topic: TOPIC, partitions: [{ partition: 0, errorCode: 14 }] }] }
        )
        const old = runner.commitRetrier
        runner.commitRetrier = (work) =>
          old(async (...args) => {
            if (args[1] > 0) await at()
            return work(...args)
          })
      }
      if (cell.boundary === 'commit:pending-queue') {
        const blocker = scripted.schedule.hold({
          api: cell.protocol === 'consumer' ? 'ConsumerGroupHeartbeat' : 'Heartbeat',
        })
        cleanup.push(() => {
          if (blocker.settle) blocker.release()
        })
        settled(group.heartbeat({ interval: 0 }))
        await pump(() => blocker.call != null, 'occupying the non-fetch socket')
        operation = settled(consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }]))
        await pump(
          () =>
            Object.values(group.cluster.brokerPool.brokers).some((broker) =>
              broker.connectionPool.pool.some(
                (connection) => connection.requestQueue.pending.length > 0
              )
            ),
          'actual pending queue'
        )
        settled(at())
      } else if (cell.boundary.startsWith('commit:'))
        operation = settled(consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }]))
      else if (cell.boundary === 'in-heartbeat')
        operation = settled(group.heartbeat({ interval: 0 }))
      else if (cell.boundary === 'in-leave') operation = settled(group.leave())
      else if (cell.boundary === 'poll-expiry:leave')
        operation = settled(
          runner.rebalanceCoordinator.onPollExpired({
            elapsedMs: 1001,
            isStatic: cell.membership === 'static',
          })
        )
      else if (cell.boundary === 'pre-release-commit') {
        group.offsetManager.resolveOffset({ topic: TOPIC, partition: 0, offset: '0' })
        operation = settled(
          runner.handleRebalanceError({
            kind: 'revoked',
            reason: 'assignment',
            error: new Error('assignment'),
          })
        )
      }
    }
    await pump(() => held.call != null, cell.boundary)
    const group = runner.consumerGroup
    const joinsBefore = running.length
    const manager = group.offsetManager
    const lastCommit = manager?.lastCommit
    const token = group.ownership.token()
    const writeCount = writes.length
    const framesBefore = scripted
      .requests()
      .filter((record) => record.api === 'OffsetCommit').length
    const terminal = ['stop', 'disconnect', 'force'].includes(cell.event)
    const learnedLoss = cell.event === 'poll-expiry'
    let event
    if (cell.event === 'stop') event = settled(consumer.stop())
    if (cell.event === 'disconnect') event = settled(consumer.disconnect())
    if (cell.event === 'force') event = settled(consumer.disconnect({ timeout: 0 }))
    if (cell.event === 'crash' && initialGate) {
      initialGate.fail(new KafkaJSConnectionClosedError('join transport closed'))
      event = starting
    } else if (cell.event === 'crash') {
      event = settled(
        runner.onCrash(Object.assign(new Error('fetch failure'), { retriable: true, retryTime: 1 }))
      )
    }
    if (cell.event === 'rejoin')
      event = settled(
        runner.handleRebalanceError({
          kind: 'revoked',
          reason: 'assignment',
          error: new Error('assignment'),
        })
      )
    if (cell.event === 'poll-expiry')
      event =
        cell.boundary === 'poll-expiry:leave'
          ? operation
          : settled(
              runner.rebalanceCoordinator.onPollExpired({
                elapsedMs: 1001,
                isStatic: cell.membership === 'static',
              })
            )
    if (learnedLoss) {
      expect(group.ownership.refusal(token)).toBeInstanceOf(KafkaJSCommitFailedError)
      if (stage === 'lost') {
        expect(group.ownership.refusal(token).reason).toBe('membership-lost')
      } else {
        expect(group.ownership.refusal(token).reason).toBe('poll-interval-exceeded')
        expect(group.ownership.refusal(token).cause.type).toBe('POLL_INTERVAL_EXCEEDED')
      }
    }
    if (cell.event === 'force' && cell.boundary.startsWith('commit:')) {
      await pump(() => event.done && operation.done, 'forced public commit before gate release')
      expect(event.error).toBeNull()
      expect(operation.error).toMatchObject({ type: 'CONSUMER_STOPPING' })
      expect(manager.lastCommit).toBe(lastCommit)
      expect(writes.slice(writeCount)).toEqual([])
      expect(scripted.requests().filter((r) => r.api === 'OffsetCommit')).toHaveLength(framesBefore)
    }
    release()
    cleanup.forEach((fn) => fn())
    await pump(() => event.done, 'event completion')
    if (event.error) throw event.error
    if (terminal) {
      expect(runner.stopRequested).toBe(true)
      expect(runner.running).toBe(false)
      expect(runner.heartbeatManager.isRunning()).toBe(false)
    }
    if (operation) {
      await pump(() => operation.done, 'held operation completion')
      if (cell.event === 'force' && cell.boundary.startsWith('commit:')) {
        expect(operation.error).toMatchObject({ type: 'CONSUMER_STOPPING' })
        expect(manager.lastCommit).toBe(lastCommit)
        expect(writes.slice(writeCount)).toEqual([])
        expect(scripted.requests().filter((r) => r.api === 'OffsetCommit')).toHaveLength(
          framesBefore
        )
      } else if (
        operation.error &&
        !(operation.error instanceof KafkaJSCommitFailedError) &&
        !(cell.event === 'force' && isForcedRetirement(operation.error))
      )
        throw operation.error
      if (learnedLoss && cell.boundary.startsWith('commit:')) {
        expect(operation.error).toBeInstanceOf(KafkaJSCommitFailedError)
        expect(operation.error.reason).toBe('poll-interval-exceeded')
        expect(manager.lastCommit).toBe(lastCommit)
        expect(writes.slice(writeCount)).toEqual([])
      }
    }
    if (
      learnedLoss &&
      cell.boundary.startsWith('commit:') &&
      cell.boundary !== 'commit:answer-held'
    )
      expect(scripted.requests().filter((record) => record.api === 'OffsetCommit')).toHaveLength(
        framesBefore
      )
    if (!terminal) await pump(() => running.length > joinsBefore, 'completed rejoin when owed')
    const stopping = settled(consumer.disconnect())
    await pump(() => stopping.done, 'terminal disconnect')
    if (stopping.error) throw stopping.error
    expect(group.operations.size()).toBe(0)
    expect(group.obligations.size()).toBe(0)
    expect(socketBaseline()).toBe(beforeSockets)
    expect(errors).toEqual([])
    if (!(stage === 'assigned' && terminal)) expect(running.length).toBeGreaterThan(0)
    for (const loss of losses)
      expect(
        scripted
          .requests()
          .slice(loss.from, loss.to ?? scripted.requests().length)
          .filter((record) => record.api === 'OffsetCommit')
      ).toHaveLength(0)
    scripted.assertClean()
  } catch (error) {
    failed = true
    throw error
  } finally {
    const cleanupErrors = []
    const attempt = async (work) => {
      try {
        await work()
      } catch (error) {
        cleanupErrors.push(error)
      }
    }
    await attempt(release)
    for (const work of cleanup) await attempt(work)
    await attempt(async () => {
      if (!consumer) return
      const closing = settled(consumer.disconnect())
      await pump(() => closing.done, 'finally disconnect')
      if (closing.error !== null) throw closing.error
    })
    for (const remove of eventRemovers) await attempt(remove)
    for (const spy of spies.reverse()) await attempt(() => spy.mockRestore())
    await attempt(() => process.removeListener('unhandledRejection', unhandled))
    if (!failed) {
      expect(cleanupErrors).toEqual([])
      expect(listeners()).toBe(0)
      expect(process.listenerCount('unhandledRejection')).toBe(baseline.unhandled)
      if (cell.event === 'force') {
        expect(jest.getTimerCount()).toBe(baseline.timers + brokerAnswerTimers.size)
        await pump(() => brokerAnswerTimers.size === 0, 'held broker answers')
        const fetched = fetchRounds.mock.calls.length
        for (let step = 0; step < 20; step++) {
          await new Promise(setImmediate)
          await jest.advanceTimersByTimeAsync(1)
        }
        expect(fetchRounds.mock.calls.length).toBe(fetched)
      }
      expect(jest.getTimerCount()).toBe(baseline.timers)
    }
  }
}

module.exports = { runLifecycleCell, pump }
