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
  initial,
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
      if (initial) throw initial()
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
      jest.restoreAllMocks()
      const working = await fenceWith(createLogger({ logCreator: () => () => {} }))

      expect(invoked).toHaveBeenCalledTimes(1)
      expect(failing.crashes).toBe(0)
      expect(failing.zeroJoins).toBe(2)
      expect(failing.delivered).toEqual(['40', '41'])
      expect(failing).toEqual(working)
    }, 120000)
  })
  describe('a restartOnFailure policy failing with an unusual value', () => {
    const unreadable = () => {
      throw Object.defineProperty({}, 'message', {
        get() {
          throw new Error('unreadable')
        },
      })
    }
    const policies = [
      [
        'throws null synchronously',
        () => {
          throw null // eslint-disable-line no-throw-literal
        },
      ],
      ['rejects with null', () => Promise.reject(null)], // eslint-disable-line prefer-promise-reject-errors
      ['throws an object with an unreadable message', unreadable],
    ]

    it.each(policies)(
      'an ordinary handler crash whose policy %s: one CRASH with the original error, restart true, one delayed restart',
      async (_, failing) => {
        const policy = jest.fn(failing)
        const { logger, lines } = recordingLogger()
        const failure = Object.assign(new Error('handler failed'), { retriable: false })
        let thrown = false
        const run = await start({
          logger,
          retry: { initialRetryTime: 300, restartOnFailure: policy },
          eachMessage: async () => {
            if (thrown) return
            thrown = true
            throw failure
          },
        })
        await advanceUntil(() => run.crashes.length > 0, 10000)
        const connects = run.cluster.connect.mock.calls.length
        const joins = beats(run, false)
        await jest.advanceTimersByTimeAsync(5000)

        expect(policy).toHaveBeenCalledTimes(1)
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].restart).toBe(true)
        let cause = run.crashes[0].error
        while (cause !== failure && cause?.cause) cause = cause.cause
        expect(cause).toBe(failure)
        expect(run.cluster.connect.mock.calls.length).toBe(connects + 1)
        expect(beats(run, false)).toBe(joins + 1)
        expect(lines).not.toContain('Crash handler failed')
      },
      30000
    )

    it.each(policies)(
      'a fatal heartbeat at startup whose policy %s: one CRASH with restart false and no restart',
      async (_, failing) => {
        const policy = jest.fn(failing)
        const { logger, lines } = recordingLogger()
        const run = await start({
          logger,
          retry: { initialRetryTime: 300, restartOnFailure: policy },
          initial: groupAuthorization,
        })
        await advanceUntil(() => run.crashes.length > 0, 10000)
        const connects = run.cluster.connect.mock.calls.length
        await jest.advanceTimersByTimeAsync(5000)

        expect(policy).toHaveBeenCalledTimes(1)
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0]).toMatchObject({ restart: false })
        expect(run.crashes[0].error.code).toBe(GROUP_AUTHORIZATION_FAILED)
        expect(run.cluster.connect.mock.calls.length).toBe(connects)
        expect(beats(run, false)).toBe(1)
        expect(lines).not.toContain('Crash handler failed')
      },
      30000
    )
  })
  describe('a REBALANCING subscriber failing during a fatal membership loss', () => {
    /** Runs the fatal code-30 schedule with two REBALANCING subscribers, the first given. */
    const fatalWith = async (first) => {
      const policy = jest.fn(async () => true)
      const { logger, lines } = recordingLogger()
      const run = await start({
        logger,
        retry: { initialRetryTime: 300, restartOnFailure: policy },
      })
      const order = []
      run.consumer.on(run.consumer.events.REBALANCING, () => {
        order.push('first')
        return first()
      })
      run.consumer.on(run.consumer.events.REBALANCING, () => order.push('second'))
      run.arm()
      await advanceUntil(() => run.crashes.length > 0, 20000)
      await jest.advanceTimersByTimeAsync(5000)
      return {
        order,
        policy: policy.mock.calls.map(([error]) => error.code),
        crashes: run.crashes.map(({ error, restart }) => [error.code, restart]),
        diagnostics: lines.filter((line) => line.startsWith('Failed to execute listener')),
        trace: run.log.map(({ kind, epoch, partitions }) => [kind, epoch, partitions]),
      }
    }

    it.each([
      [
        'throws synchronously',
        () => {
          throw new Error('subscriber threw')
        },
      ],
      ['rejects', async () => Promise.reject(new Error('subscriber threw'))],
    ])(
      'a first subscriber that %s: the second runs, one diagnostic, and the run crashes as with a working subscriber',
      async (_, failing) => {
        const failed = await fatalWith(failing)
        await cleanup()
        cleanup = null
        jest.restoreAllMocks()
        const working = await fatalWith(() => {})

        expect(failed.order).toEqual(['first', 'second'])
        expect(failed.diagnostics).toEqual(['Failed to execute listener: subscriber threw'])
        expect(failed.policy).toEqual([GROUP_AUTHORIZATION_FAILED])
        expect(failed.crashes).toEqual([[GROUP_AUTHORIZATION_FAILED, false]])
        expect(failed.trace).toEqual(working.trace)
        expect(working.diagnostics).toEqual([])
      },
      60000
    )
  })
  describe('a current consumer-group heartbeat answered with a fatal code the error table does not list', () => {
    /**
     * Answers an established member's next positive-epoch heartbeat with `make()` and waits for
     * the crash, then 60 s more. Returns the trace after the fault with times dropped.
     */
    const fatalAnswer = async (make, { policy = jest.fn(async () => true) } = {}) => {
      let answered = null
      const run = await start({
        retry: { initialRetryTime: 300, restartOnFailure: policy },
        fault: () => (answered = make()),
      })
      run.arm()
      await advanceUntil(() => run.crashes.length > 0, 20000)
      const crashedAt = run.log.length
      await jest.advanceTimersByTimeAsync(60000)
      const fault = run.faultAt()
      return {
        answered,
        policy,
        crashes: run.crashes,
        after: run.log
          .filter((entry) => entry.at === undefined || entry.at >= fault)
          .map(({ kind, epoch }) => [kind, epoch]),
        sendsAfterCrash: run.log.slice(crashedAt).filter(({ kind }) => kind !== 'STOP'),
        connects: run.cluster.connect.mock.calls.length,
      }
    }

    it.each([
      ['GROUP_ID_NOT_FOUND (69)', 69],
      ['an unrecognised wire code (1042)', 1042],
      ['a code with a generic retriable flag (7)', 7],
    ])(
      '%s ends the run like GROUP_AUTHORIZATION_FAILED: one policy call, one CRASH with restart false, nothing sent after it',
      async (_, code) => {
        const observed = await fatalAnswer(() => protocolError(code))
        await cleanup()
        cleanup = null
        jest.restoreAllMocks()
        const control = await fatalAnswer(() => protocolError(GROUP_AUTHORIZATION_FAILED))

        expect(observed.policy).toHaveBeenCalledTimes(1)
        expect(inCauseChain(observed.policy.mock.calls[0][0], observed.answered)).toBe(true)
        expect(observed.crashes).toHaveLength(1)
        expect(observed.crashes[0].restart).toBe(false)
        expect(inCauseChain(observed.crashes[0].error, observed.answered)).toBe(true)
        expect(observed.sendsAfterCrash).toEqual([])
        expect(observed.connects).toBe(1)
        expect(observed.after).toEqual(control.after)
      },
      120000
    )

    it('a policy clearing the public code and type of the answer still cannot restart the run', async () => {
      const policy = jest.fn(async (error) => {
        for (let e = error; e; e = e.cause) {
          e.code = undefined
          e.type = undefined
        }
        return true
      })
      const observed = await fatalAnswer(() => protocolError(69), { policy })

      expect(policy).toHaveBeenCalledTimes(1)
      expect(observed.crashes).toHaveLength(1)
      expect(observed.crashes[0].restart).toBe(false)
      expect(observed.connects).toBe(1)
    }, 120000)

    it.each([
      ['GROUP_ID_NOT_FOUND (69)', 69],
      ['an unrecognised wire code (1042)', 1042],
      ['REBALANCE_IN_PROGRESS (27)', 27],
      ['a code with a generic retriable flag (7)', 7],
    ])(
      '%s answering the first join ends the run after one join request with one CRASH and restart false',
      async (_, code) => {
        const policy = jest.fn(async () => true)
        let answered = null
        const run = await start({
          retry: { initialRetryTime: 300, restartOnFailure: policy },
          initial: () => (answered = protocolError(code)),
          eachMessage: async () => {},
        })
        await advanceUntil(() => run.crashes.length > 0, 20000)
        await jest.advanceTimersByTimeAsync(60000)

        expect(beats(run, false)).toBe(1)
        expect(policy).toHaveBeenCalledTimes(1)
        expect(inCauseChain(policy.mock.calls[0][0], answered)).toBe(true)
        expect(run.crashes).toHaveLength(1)
        expect(run.crashes[0].restart).toBe(false)
        expect(inCauseChain(run.crashes[0].error, answered)).toBe(true)
        expect(run.log.filter(({ kind }) => kind === 'fetch')).toEqual([])
        expect(run.cluster.connect).toHaveBeenCalledTimes(1)
      },
      120000
    )

    it('GROUP_ID_NOT_FOUND answering the leave heartbeat completes the disconnect without a CRASH', async () => {
      const run = await start({})
      const answer = run.state.answer
      run.state.answer = (req) =>
        req.memberEpoch < 0 ? Promise.reject(protocolError(69)) : answer(req)
      const closing = settled(run.consumer.disconnect())
      await advanceUntil(() => closing.done, 10000)

      expect(closing).toMatchObject({ done: true, value: undefined })
      expect(run.log.some(({ kind, epoch }) => kind === 'heartbeat' && epoch < 0)).toBe(true)
      expect(run.crashes).toEqual([])
      cleanup = null
    })
  })
})
