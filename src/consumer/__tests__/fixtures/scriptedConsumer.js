const { createLogger } = require('../../../loggers')
const createConsumer = require('../../index')
const Runner = require('../../runner')
const RebalanceCoordinator = require('../../rebalanceCoordinator')
const { MemberAssignment } = require('../../assignerProtocol')
const OffsetOperations = require('../../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { KafkaJSProtocolError } = require('../../../errors')
const { createErrorFromCode } = require('../../../protocol/error')
const { newLogger } = require('testHelpers')

const GROUP_ID = 'loss-driver-group'
const FENCED_MEMBER_EPOCH = 110
const GROUP_AUTHORIZATION_FAILED = 30
const IDS = { 'topic-a': 'aAaAaAaAaAaAaAaAaAaAaA', 'topic-b': 'bBbBbBbBbBbBbBbBbBbBbB' }
const NAMES = Object.fromEntries(Object.entries(IDS).map(([name, id]) => [id, name]))

const fenced = () => new KafkaJSProtocolError(createErrorFromCode(FENCED_MEMBER_EPOCH))
const groupAuthorization = () =>
  new KafkaJSProtocolError(createErrorFromCode(GROUP_AUTHORIZATION_FAILED))

/** A ConsumerGroupHeartbeat response; `byTopic` null means "no change". */
const target = (memberEpoch, byTopic) => ({
  throttleTime: 0,
  errorCode: 0,
  errorMessage: null,
  memberId: 'member-1',
  memberEpoch,
  heartbeatIntervalMs: 500,
  assignment:
    byTopic === null
      ? null
      : {
          topicPartitions: Object.entries(byTopic).map(([topic, partitions]) => ({
            topicId: IDS[topic],
            partitions,
          })),
        },
})

/**
 * A whole consumer-protocol consumer over a scripted coordinator and broker. `state.answer`
 * answers every heartbeat; `state.queue['topic|partition']` holds the offsets served, one record per
 * Fetch; `state.partial` names topics whose metadata knows partition 0 only until a refresh, and
 * `state.metadataDelay` delays the next refresh once. `log` orders heartbeats, fetches and events.
 * @param {object} [options] consumer options replacing the defaults
 * @returns {object}
 */
const buildConsumer = (options = {}) => {
  const committed = {}
  const state = { answer: null, queue: {}, partial: new Set(), metadataDelay: 0 }
  const log = []
  const crashes = []
  const coordinator = {
    isConnected: () => true,
    nodeId: 0,
    negotiatedVersion: jest.fn(() => null),
    consumerGroupHeartbeat: jest.fn(async (request) => {
      const entry = { kind: 'heartbeat', at: Date.now(), epoch: request.memberEpoch }
      log.push(entry)
      try {
        return state.answer(request)
      } catch (error) {
        entry.error = error.code
        throw error
      }
    }),
    offsetCommit: jest.fn(async ({ topics }) => {
      for (const { topic, partitions } of topics) {
        for (const { partition, offset } of partitions) {
          committed[`${topic}|${partition}`] = String(offset)
          log.push({ kind: 'commit', tp: `${topic}|${partition}`, offset: String(offset) })
        }
      }
    }),
    offsetFetch: jest.fn(async ({ topics }) => ({
      responses: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          offset: committed[`${topic}|${partition}`] ?? '40',
          committedLeaderEpoch: -1,
          metadata: null,
          errorCode: 0,
        })),
      })),
    })),
  }
  const record = (offset) => ({
    offset: String(offset),
    key: Buffer.from('k'),
    value: Buffer.from(`v${offset}`),
    headers: {},
    timestamp: '0',
    attributes: 0,
    isControlRecord: false,
    batchContext: {
      partitionLeaderEpoch: 0,
      firstOffset: String(offset),
      firstTimestamp: '0',
      maxTimestamp: '0',
      magicByte: 2,
      producerId: '-1',
      producerEpoch: -1,
      firstSequence: -1,
      isTransactional: false,
      isControlBatch: false,
    },
  })
  const broker = {
    fetch: jest.fn(async ({ topics }) => {
      let served = false
      log.push({ kind: 'fetch', at: Date.now() })
      const responses = topics.map(({ topic, partitions }) => ({
        topicName: topic,
        partitions: partitions.map(({ partition, fetchOffset }) => {
          const messages = (state.queue[`${topic}|${partition}`] || [])
            .filter((offset) => offset >= Number(fetchOffset))
            .slice(0, state.batchSize ?? 1)
            .map((offset) => {
              const message = record(offset)
              if (state.controlOffset === offset) message.isControlRecord = true
              return message
            })
          served = served || messages.length > 0
          return {
            partition,
            errorCode: 0,
            highWatermark: '100',
            lastStableOffset: '100',
            logStartOffset: '0',
            abortedTransactions: [],
            preferredReadReplica: -1,
            messages,
          }
        }),
      }))
      await new Promise((resolve) => setTimeout(resolve, served ? 5 : 50))
      return { throttleTime: 0, errorCode: 0, sessionId: 0, responses }
    }),
  }
  const cluster = {
    instrumentationEmitter: new InstrumentationEventEmitter(),
    connect: jest.fn(async () => {
      log.push({ kind: 'connect', at: Date.now() })
    }),
    disconnect: jest.fn(async () => {
      log.push({ kind: 'disconnect', at: Date.now() })
      await state.disconnecting
    }),
    isConnected: () => true,
    removeBroker: jest.fn(),
    onTopicRecreated: jest.fn(() => () => {}),
    addMultipleTargetTopics: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {
      const delay = state.metadataDelay
      state.metadataDelay = 0
      if (delay) await new Promise((resolve) => setTimeout(resolve, delay))
      state.partial.clear()
    }),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadataForTopicIds: jest.fn(async () => {}),
    useConsumerMetadataScope: jest.fn(),
    releaseConsumerMetadataScope: jest.fn(),
    resetTargetTopics: jest.fn(async (_names, { publish } = {}) => publish?.()),
    committedOffsets: () => ({}),
    offsetOperations: () => new OffsetOperations(),
    defaultOffset: () => -2,
    topicId: (name) => IDS[name],
    topicName: (id) => NAMES[id],
    leaderEpoch: () => -1,
    getNodeIds: () => [0],
    findTopicPartitionMetadata: (topic) =>
      (state.partial.has(topic) ? [0] : [0, 1]).map((partitionId) => ({ partitionId, leader: 0 })),
    findBroker: jest.fn(async () => broker),
    findGroupCoordinator: jest.fn(async () => coordinator),
    fetchTopicsOffset: jest.fn(async (requests) =>
      requests.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, offset: '0' })),
      }))
    ),
    retry: {},
  }
  const consumer = createConsumer({
    cluster,
    groupId: GROUP_ID,
    logger: newLogger(),
    instrumentationEmitter: cluster.instrumentationEmitter,
    groupProtocol: 'consumer',
    fetchSessions: false,
    maxWaitTimeInMs: 50,
    retry: { initialRetryTime: 300, restartOnFailure: async () => false },
    ...options,
  })
  for (const key of ['PARTITIONS_LOST', 'PARTITIONS_REVOKED', 'PARTITIONS_ASSIGNED', 'STOP']) {
    consumer.on(consumer.events[key], ({ payload }) =>
      log.push({ kind: key, at: Date.now(), partitions: payload?.partitions })
    )
  }
  consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
  return { consumer, cluster, state, log, crashes, coordinator }
}

/** Advances fake time in 10 ms steps until `done()` holds, for at most `budget` ms. */
const advanceUntil = async (done, budget = 5000) => {
  for (let spent = 0; !done() && spent < budget; spent += 10) {
    await jest.advanceTimersByTimeAsync(10)
  }
}

/** @returns {{ promise: Promise<void>, resolve: () => void }} */
const deferred = () => {
  let resolve
  const promise = new Promise((r) => {
    resolve = r
  })
  return { promise, resolve }
}
/** Records how `p` settles, without awaiting it. */
const settled = (p) => {
  const s = { done: false }
  p.then(
    (value) => Object.assign(s, { done: true, value }),
    (error) => Object.assign(s, { done: true, error })
  )
  return s
}
/** The disconnect owed by the run `start` built last, cleared by `teardown` or `forget`. */
let cleanup = null

/** Releases and disconnects the run `start` built last, if it still owes that. */
const teardown = async () => {
  const pending = cleanup
  cleanup = null
  await pending?.()
}

/** Drops the owed disconnect of a run the test already closed itself. */
const forget = () => {
  cleanup = null
}
/**
 * Builds an established consumer-protocol member over the scripted cluster. The first epoch-zero
 * heartbeat joins with topic-a/0 (or throws `initial()`), the next positive-epoch heartbeat after
 * `arm()` throws `fault()`, and later joins are answered `onZero` or epoch 2. A handler receives
 * the payload and `held`, which the run's teardown resolves.
 * @param {object} [options]
 * @returns {Promise<object>} the run, its trace and controls
 */
async function start({
  retry,
  logger,
  hook,
  eachMessage,
  eachBatch,
  fault = groupAuthorization,
  initial,
  onZero,
  autoCommitThreshold,
  partitions = [0],
  records,
  run,
  prepare,
} = {}) {
  const coordinators = []
  const runRound = RebalanceCoordinator.prototype.runRound
  jest.spyOn(RebalanceCoordinator.prototype, 'runRound').mockImplementation(function (...args) {
    if (!coordinators.includes(this)) coordinators.push(this)
    return runRound.apply(this, args)
  })
  const built = buildConsumer({
    maxPollInterval: 4000,
    rebalanceTimeout: 3000,
    ...(retry && { retry }),
    ...(logger && { logger }),
  })
  if (prepare) await prepare(built)
  const { consumer, state, log, crashes, cluster, coordinator } = built
  const held = deferred()
  let first = true
  let armed = false
  let faultAt = null
  state.answer = (req) => {
    if (req.memberEpoch === 0 && first) {
      first = false
      if (initial) throw initial()
      return target(1, { 'topic-a': partitions })
    }
    if (req.memberEpoch > 0 && armed) {
      armed = false
      faultAt = Date.now()
      throw fault()
    }
    if (req.memberEpoch === 0) return onZero?.(req) ?? target(2, { 'topic-a': partitions })
    return target(req.memberEpoch, null)
  }
  if (records) Object.assign(state.queue, records)
  else if (eachMessage || eachBatch) state.queue['topic-a|0'] = [40]
  await consumer.subscribe({ topic: 'topic-a' })
  const started = settled(
    consumer.run({
      eachMessage: eachMessage && ((payload) => eachMessage(payload, held)),
      eachBatch: eachBatch && ((payload) => eachBatch(payload, held)),
      ...(hook && { rebalanceListener: hook }),
      ...(autoCommitThreshold && { autoCommitThreshold }),
      ...run,
    })
  )
  await advanceUntil(() => started.done)
  expect(started).toMatchObject({ done: true, value: undefined })
  cleanup = async () => {
    held.resolve()
    const end = settled(consumer.disconnect({ timeout: 0 }))
    await jest.advanceTimersByTimeAsync(10)
    expect(end.done).toBe(true)
  }
  return {
    consumer,
    state,
    log,
    crashes,
    cluster,
    held,
    coordinators,
    coordinator,
    runner: coordinators[0]?.runner,
    group: coordinators[0]?.consumerGroup,
    arm: () => {
      armed = true
    },
    faultAt: () => faultAt,
  }
}

/** An eager classic assigner, named as the coordinator double's JoinGroup answer names it. */
const eager = () => ({
  name: 'eager',
  version: 0,
  protocol: () => ({ name: 'eager', metadata: Buffer.alloc(0) }),
  assign: async () => [],
})

/**
 * Starts a run of topic-a over the scripted cluster under the classic protocol, as a follower
 * that joins with JoinGroup and SyncGroup, or under manual assignment. Waits until `run()` settled,
 * or, when `joined` holds the first JoinGroup answer, until that JoinGroup was sent. A handler
 * receives the payload and `held`, which the run's teardown resolves. With `prepare` or a parked
 * `run()` the caller owns the wait: `until` names the point to wait for.
 * @param {'classic'|'manual'} kind
 * @param {object} [options]
 * @param {(payload: object, held: object) => Promise<void>} [options.eachMessage] the `eachMessage`
 *   handler; `held` resolves in the run's teardown
 * @param {(payload: object, held: object) => Promise<void>} [options.eachBatch] the `eachBatch`
 *   handler, with the same `held`
 * @param {object} [options.hook] the `rebalanceListener` of the run
 * @param {{ promise: Promise<void> }} [options.joined] holds the first JoinGroup answer: the
 *   start waits until that JoinGroup was sent, and `run()` is still pending then
 * @param {object} [options.config] consumer options replacing the fixture's defaults
 * @param {number} [options.autoCommitThreshold] the run's `autoCommitThreshold`
 * @param {Record<string, number[]>} [options.records] the offsets served by `topic|partition`;
 *   `topic-a|0` serves 40 when a handler is given and this is omitted
 * @param {number[]} [options.partitions=[0]] the partitions of topic-a SyncGroup assigns
 * @param {object} [options.run] further `run()` options
 * @param {boolean} [options.trace] records DISCONNECT, CRASH (with `restart` and the fake-clock
 *   `at`) and the settlement of `run()` in `log`, so one trace orders them with STOP and the
 *   transport disconnect; every listener is attached before `run()` starts
 * @param {(built: object) => void} [options.prepare] runs on the built consumer before `run()`,
 *   to park a step of the start (for instance `built.state.disconnecting`)
 * @param {(built: object) => boolean} [options.until] waits, instead of for `run()` to settle,
 *   until this holds; `run()` may then still be pending
 * @returns {Promise<object>} the run, its runner and its group
 */
const startClassic = async (
  kind,
  {
    eachMessage,
    eachBatch,
    hook,
    joined,
    config,
    autoCommitThreshold,
    records,
    partitions = [0],
    run: runOptions,
    trace,
    prepare,
    until,
  } = {}
) => {
  const runs = jest.spyOn(Runner.prototype, 'start')
  const built = buildConsumer({ groupProtocol: 'classic', partitionAssigners: [eager], ...config })
  const { consumer, coordinator, cluster, state, log } = built
  if (trace) {
    consumer.on(consumer.events.DISCONNECT, () => log.push({ kind: 'DISCONNECT' }))
    consumer.on(consumer.events.CRASH, ({ payload }) =>
      log.push({ kind: 'CRASH', restart: payload.restart, at: Date.now() })
    )
  }
  if (prepare) await prepare(built)
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
      memberAssignment: MemberAssignment.encode({
        version: 0,
        assignment: { 'topic-a': partitions },
      }),
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
  if (records) Object.assign(state.queue, records)
  else if (eachMessage || eachBatch) state.queue['topic-a|0'] = [40]
  if (kind === 'manual') {
    const assigned = settled(consumer.assign([{ topic: 'topic-a', partitions: [0] }]))
    await advanceUntil(() => assigned.done, 1000)
    expect(assigned).toMatchObject({ done: true, value: undefined })
  } else {
    await consumer.subscribe({ topic: 'topic-a' })
  }
  const held = deferred()
  const running = consumer.run({
    eachMessage: eachMessage && ((payload) => eachMessage(payload, held)),
    eachBatch: eachBatch && ((payload) => eachBatch(payload, held)),
    ...(hook && { rebalanceListener: hook }),
    ...(autoCommitThreshold && { autoCommitThreshold }),
    ...runOptions,
  })
  const runSettled = () => log.push({ kind: 'RUN' })
  if (trace) running.then(runSettled, runSettled)
  const started = settled(running)
  cleanup = async () => {
    held.resolve()
    const end = settled(consumer.disconnect({ timeout: 0 }))
    await jest.advanceTimersByTimeAsync(10)
    expect(end.done).toBe(true)
  }
  if (until) {
    await advanceUntil(() => until(built))
    expect(until(built)).toBe(true)
  } else if (joined) {
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

const protocolError = (code) => new KafkaJSProtocolError(createErrorFromCode(code))

/** Whether `error` or one of its causes is `target`. */
const inCauseChain = (error, target) => {
  for (; error; error = error.cause) if (error === target) return true
  return false
}

/** A logger recording every line's message, so a test can look for a diagnostic. */
const recordingLogger = () => {
  const lines = []
  const logger = createLogger({
    logCreator:
      () =>
      ({ log }) =>
        lines.push(log.message),
  })
  return { logger, lines }
}

/** @returns {number} heartbeats sent with a positive (or zero) member epoch */
const beats = (run, positive) =>
  run.log.filter(
    (entry) => entry.kind === 'heartbeat' && (positive ? entry.epoch > 0 : entry.epoch === 0)
  ).length

module.exports = {
  GROUP_AUTHORIZATION_FAILED,
  fenced,
  groupAuthorization,
  protocolError,
  target,
  buildConsumer,
  advanceUntil,
  deferred,
  settled,
  start,
  eager,
  startClassic,
  teardown,
  forget,
  beats,
  inCauseChain,
  recordingLogger,
}
