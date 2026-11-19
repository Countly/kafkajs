const { createLogger } = require('../../loggers')
const createConsumer = require('../index')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const OffsetOperations = require('../offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
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
            .slice(0, 1)
            .map(record)
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

const deferred = () => {
  let resolve
  const promise = new Promise((r) => {
    resolve = r
  })
  return { promise, resolve }
}
const settled = (p) => {
  const s = { done: false }
  p.then(
    (value) => Object.assign(s, { done: true, value }),
    (error) => Object.assign(s, { done: true, error })
  )
  return s
}
let cleanup
beforeEach(() => jest.useFakeTimers())
afterEach(async () => {
  await cleanup?.()
  cleanup = null
  jest.useRealTimers()
  jest.restoreAllMocks()
})
async function start({
  active = true,
  retry,
  logger,
  hook,
  eachMessage,
  fault = groupAuthorization,
  onZero,
} = {}) {
  const coordinators = []
  const runRound = RebalanceCoordinator.prototype.runRound
  jest.spyOn(RebalanceCoordinator.prototype, 'runRound').mockImplementation(function (...args) {
    if (!coordinators.includes(this)) coordinators.push(this)
    return runRound.apply(this, args)
  })
  const { consumer, state, log, crashes, cluster, coordinator } = buildConsumer({
    maxPollInterval: 4000,
    rebalanceTimeout: 3000,
    ...(retry && { retry }),
    ...(logger && { logger }),
  })
  const held = deferred()
  let first = true
  let armed = false
  let faultAt = null
  state.answer = (req) => {
    if (req.memberEpoch === 0 && first) {
      first = false
      return target(1, { 'topic-a': [0] })
    }
    if (req.memberEpoch > 0 && armed) {
      armed = false
      faultAt = Date.now()
      throw fault()
    }
    if (req.memberEpoch === 0) return onZero?.(req) ?? target(2, { 'topic-a': [0] })
    return target(req.memberEpoch, null)
  }
  if (eachMessage) state.queue['topic-a|0'] = [40]
  await consumer.subscribe({ topic: 'topic-a' })
  const started = settled(
    consumer.run({
      eachMessage: eachMessage && ((payload) => eachMessage(payload, held)),
      ...(hook && { rebalanceListener: hook }),
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
    arm: () => {
      armed = true
    },
    faultAt: () => faultAt,
  }
}

const beats = (run, positive) =>
  run.log.filter(
    (entry) => entry.kind === 'heartbeat' && (positive ? entry.epoch > 0 : entry.epoch === 0)
  ).length

describe('Consumer > crash supervision', () => {
  describe('a log sink that throws on the membership-lost line', () => {
    const LOST_LINE = 'Membership lost, discarding uncommitted offsets and re-joining'

    /** Fences an established member, lets recovery run and returns the observed trace. */
    const fenceWith = async (logger) => {
      const delivered = []
      const run = await start({
        logger,
        fault: fenced,
        eachMessage: async ({ message }) => delivered.push(message.offset),
      })
      await advanceUntil(() => delivered.length === 1)
      run.arm()
      await advanceUntil(() => run.crashes.length > 0 || beats(run, false) > 1, 60000)
      run.state.queue['topic-a|0'] = [40, 41]
      await advanceUntil(() => delivered.length > 1)
      return {
        crashes: run.crashes.length,
        lost: run.log.filter((entry) => entry.kind === 'PARTITIONS_LOST').map((e) => e.partitions),
        zeroJoins: beats(run, false),
        delivered,
        commits: run.log.filter((entry) => entry.kind === 'commit').map((e) => [e.tp, e.offset]),
      }
    }

    it('recovers with a second epoch-zero join and resumed delivery, as with a working sink', async () => {
      const invoked = jest.fn()
      const throwing = createLogger({
        logCreator:
          () =>
          ({ log }) => {
            if (log.message !== LOST_LINE) return
            invoked()
            throw new Error('logging failed')
          },
      })
      const failing = await fenceWith(throwing)
      await cleanup()
      cleanup = null
      const working = await fenceWith(createLogger({ logCreator: () => () => {} }))

      expect(invoked).toHaveBeenCalledTimes(1)
      expect(failing.crashes).toBe(0)
      expect(failing.zeroJoins).toBe(2)
      expect(failing.delivered).toEqual(['40', '41'])
      expect(failing).toEqual(working)
    }, 120000)
  })
})
