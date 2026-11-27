const GroupOwnership = require('../groupOwnership')
const OffsetManager = require('../offsetManager')
const Runner = require('../runner')
const Generation = require('../../producer/generation')
const TransactionOffsets = require('../../producer/state/transactionOffsets')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { KafkaJSProtocolError, KafkaJSCommitFailedError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { isSuperseded } = require('../handlerContext')
const {
  REBALANCE_PATHS,
  isStaleMembershipVerdict,
  learnOwnershipLoss,
  obsoleteOwnership,
  stampGeneration,
  stampOwnership,
} = require('../rebalanceErrorContract')
const {
  events: { COMMIT_OFFSETS },
} = require('../instrumentationEvents')
const { newLogger } = require('testHelpers')
const {
  NODE_ID,
  TOPIC,
  createFetchGroup,
  fetchResponse,
  prepareRecordBatches,
} = require('./fixtures/fetchHarness')

const FAKE_TIME = { now: 0, doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] }

/** @returns {Error} the refusal a withhold is given */
const refusal = () => new Error('assignment refused')

/** @param {number} code @returns {Error} a decoded broker error */
const brokerError = (code) => new KafkaJSProtocolError(createErrorFromCode(code))

/** @returns {Promise<void>} one event-loop turn, without a timer */
const turn = () => new Promise(setImmediate)

/**
 * @param {() => boolean} condition what the schedule waits for
 * @returns {Promise<void>} once it holds, after at most a few hundred turns
 * @throws {Error} when it never holds
 */
const until = async (condition) => {
  for (let turns = 0; turns < 500 && !condition(); turns++) await turn()
  if (!condition()) throw new Error('the schedule never reached its hold point')
}

/** @returns {{promise: Promise<*>, release: Function}} a step held open from outside */
const gate = () => {
  const held = {}
  held.promise = new Promise((resolve) => {
    held.release = resolve
  })
  return held
}

/** @returns {Promise<void>} lets fake time, microtasks and real immediates run */
const drain = async () => {
  for (let n = 0; n < 6; n++) {
    await turn()
    await jest.advanceTimersByTimeAsync(0)
  }
}

/**
 * Holds every Fetch answer, success or failure, until released: a reply that arrives late.
 * @param {object} broker the harness broker
 * @returns {{release: () => void, failNext: (error: Error) => void, pending: () => number}}
 */
const holdFetches = (broker) => {
  const real = broker.fetch.bind(broker)
  const waiting = []
  let failure = null
  jest.spyOn(broker, 'fetch').mockImplementation(async (fields) => {
    const failing = failure
    failure = null
    const response = failing ? null : await real(fields)
    await new Promise((resolve) => waiting.push(resolve))
    if (failing) throw failing
    return response
  })
  return {
    release: () => waiting.splice(0).forEach((resolve) => resolve()),
    failNext: (error) => {
      failure = error
    },
    pending: () => waiting.length,
  }
}

/**
 * @param {number} sessionId the session id the broker answers with
 * @param {(number|object)[]} [partitions] the rows of the reply
 * @returns {Buffer} one scripted Fetch reply
 */
const reply = (sessionId, partitions = []) =>
  fetchResponse(
    partitions.map((entry) =>
      typeof entry === 'number'
        ? { partition: entry, errorCode: 0, records: true }
        : { errorCode: 0, records: true, ...entry }
    ),
    { sessionId, includeTopic: partitions.length > 0 }
  )

/** @param {object} frame a decoded Fetch frame @returns {object} its session fields and rows */
const shape = (frame) => ({
  sessionId: frame.sessionId,
  sessionEpoch: frame.sessionEpoch,
  topics: frame.topics.flatMap(({ partitions }) => partitions.map(({ partition }) => partition)),
  forgotten: frame.forgottenTopics.flatMap(({ partitions }) => partitions),
})

/**
 * A Fetch group with sessions on, every partition committed at offset 5. An offset manager the
 * group builds at an eager install or a withhold evicts its cells from the shared cache and reads
 * them from the coordinator, so the coordinator answers OffsetFetch as a broker holding offset 5
 * for every partition would.
 * @param {object} [options] `createFetchGroup` options
 * @returns {Promise<object>} the harness
 */
const opened = async (options = {}) =>
  answeringOffsetFetch(await createFetchGroup({ fetchSessions: true, position: '5', ...options }))

/**
 * @param {object} h a harness whose coordinator is given the OffsetFetch answer
 * @returns {Promise<object>} the harness
 */
const answeringOffsetFetch = async (h) => {
  const coordinator = await h.cluster.findGroupCoordinator()
  coordinator.offsetFetch = jest.fn(async ({ topics }) => ({
    responses: topics.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition }) => ({
        partition,
        offset: '5',
        metadata: null,
        errorCode: 0,
      })),
    })),
  }))
  return h
}

/**
 * Applies an assignment through the group's own install path.
 * @param {object} h the harness
 * @param {number[]} partitions the assigned partitions of the topic
 * @param {boolean} [rebuildOffsets=false] eager rebuild instead of a cooperative update
 * @returns {Promise<object>} the apply result
 */
const install = (h, partitions, rebuildOffsets = false) => {
  h.group.partitionsPerSubscribedTopic = h.group.generatePartitionsPerSubscribedTopic()
  return h.group.applyRebalanceResult({ assignment: { [TOPIC]: partitions } }, { rebuildOffsets })
}

/** A usable install after a withhold. */
const heal = (h, partitions) => install(h, partitions, true)

/** Counts the live registrations made on a fence from now on. */
const trackRegistrations = (fence) => {
  const register = fence.register.bind(fence)
  const tracker = { active: 0 }
  fence.register = (cancel) => {
    tracker.active += 1
    const unregister = register(cancel)
    let done = false
    return () => {
      if (!done) tracker.active -= 1
      done = true
      unregister()
    }
  }
  return tracker
}

let h
beforeAll(() => prepareRecordBatches())
afterEach(async () => {
  jest.clearAllTimers()
  jest.useRealTimers()
  if (h) await h.broker.disconnect()
  h = null
})

describe('assignment ownership states', () => {
  test('a withhold keeps the current token and refuses it as assignment-refused with the refusal as cause', () => {
    const ownership = new GroupOwnership()
    const token = ownership.token()
    const cause = refusal()
    ownership.withhold(cause)
    expect(ownership.token()).toBe(token)
    expect(ownership.withheld(token)).toBe(true)
    expect(ownership.lost(token)).toBeNull()
    const refused = ownership.refusal(token)
    expect(refused).toBeInstanceOf(KafkaJSCommitFailedError)
    expect(refused.reason).toBe('assignment-refused')
    expect(refused.cause).toBe(cause)
  })

  test('a loss of a withheld token wins: lost returns the loss and the refusal says membership-lost', () => {
    const ownership = new GroupOwnership()
    const token = ownership.token()
    ownership.withhold(refusal())
    const loss = brokerError(25)
    ownership.lose(loss, token)
    expect(ownership.lost(token)).toBe(loss)
    expect(ownership.withheld(token)).toBe(false)
    expect(ownership.refusal(token)).toMatchObject({ reason: 'membership-lost', cause: loss })
  })

  test('a poll expiry of a withheld token still reports poll-interval-exceeded', () => {
    const ownership = new GroupOwnership()
    const token = ownership.token()
    ownership.withhold(refusal())
    const expiry = Object.assign(new Error('poll expired'), { type: 'POLL_INTERVAL_EXCEEDED' })
    ownership.lose(expiry, token)
    expect(ownership.refusal(token).reason).toBe('poll-interval-exceeded')
  })

  test('an abandoned run is lost for every token, withheld or not', () => {
    const ownership = new GroupOwnership()
    const token = ownership.token()
    ownership.withhold(refusal())
    const fatal = brokerError(82)
    ownership.abandon(fatal, token)
    expect(ownership.lost(token)).toBe(fatal)
    expect(ownership.lost(token + 1)).toBe(fatal)
    expect(ownership.refusal(token).reason).toBe('membership-lost')
  })

  test('a renew after a withhold starts a fresh token; a retained withheld token stays assignment-refused', () => {
    const ownership = new GroupOwnership()
    const retained = ownership.token()
    const release = ownership.retain(retained)
    ownership.withhold(refusal())
    const fresh = ownership.renew()
    expect(fresh).not.toBe(retained)
    expect(ownership.withheld(fresh)).toBe(false)
    expect(ownership.refusal(fresh)).toBeNull()
    expect(ownership.refusal(retained).reason).toBe('assignment-refused')
    release()
    expect(ownership.withheld(retained)).toBe(false)
    expect(ownership.refusal(retained).reason).toBe('stale-assignment')
  })

  test('a retained withheld token is lost once the run is abandoned under a later token', () => {
    const ownership = new GroupOwnership()
    const withheld = ownership.token()
    const release = ownership.retain(withheld)
    ownership.withhold(refusal())
    const fatal = brokerError(82)
    ownership.abandon(fatal, ownership.renew())
    expect(ownership.withheld(withheld)).toBe(false)
    expect(ownership.lost(withheld)).toBe(fatal)
    expect(ownership.refusal(withheld).reason).toBe('membership-lost')
    release()
  })

  test('a withhold of a lost token keeps it lost', () => {
    const ownership = new GroupOwnership()
    const token = ownership.token()
    const loss = brokerError(22)
    ownership.lose(loss, token)
    ownership.withhold(refusal())
    expect(ownership.withheld(token)).toBe(false)
    expect(ownership.refusal(token)).toMatchObject({ reason: 'membership-lost', cause: loss })
  })

  test.each([
    [27, false],
    [82, true],
  ])(
    'a heartbeat answer %i stamped before a withhold is current and classified as before',
    (code, loses) => {
      const ownership = new GroupOwnership({
        isStale: (cause) => isStaleMembershipVerdict({ kind: 'lost', error: cause }, 'classic', 7),
      })
      const token = ownership.token()
      const answer = stampOwnership(stampGeneration(brokerError(code), 6), token)
      ownership.withhold(refusal())
      expect(obsoleteOwnership(ownership, answer)).toBe(false)
      expect(learnOwnershipLoss(ownership, answer, REBALANCE_PATHS.HEARTBEAT, 'classic')).toBe(
        loses
      )
      expect(ownership.refusal(token).reason).toBe(loses ? 'membership-lost' : 'assignment-refused')
    }
  )
})

/**
 * A runner over a group stand-in that carries only methods the real group has: rounds and commits
 * are observed rather than run.
 * @returns {object} runner, group, round spy and cleanup
 */
const buildRunner = () => {
  const ownership = new GroupOwnership()
  const group = {
    groupId: 'group',
    memberId: 'member',
    kind: 'classic',
    groupInstanceId: null,
    ownership,
    groupMembership: {},
    rebalanceTimeout: 60000,
    rebalanceHooks: { retire: jest.fn() },
    forgetMemberId() {
      this.memberId = null
    },
    attach() {},
    getNodeIds: () => [],
    rebalanceProtocol: () => 'eager',
    commitOffsets: jest.fn(async () => {}),
    onMembershipError: jest.fn(),
    discardUncommittedOffsets: jest.fn(),
    heartbeatInterval: () => 1000,
    invokeHook: jest.fn(async () => {}),
    hookIdentity: () => ({}),
    takeListenerFailure: () => null,
  }
  const runner = new Runner({
    consumerGroup: group,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
    concurrency: 1,
    heartbeatInterval: 1000,
    maxPollInterval: 300000,
    onCrash: jest.fn(),
    retry: { retries: 3, initialRetryTime: 1000 },
  })
  runner.running = true
  const round = jest.spyOn(runner.rebalanceCoordinator, 'runRound').mockResolvedValue()
  return {
    runner,
    group,
    round,
    close: () => {
      runner.heartbeatManager.shutdown()
      runner.pollTimer.stop()
      expect(jest.getTimerCount()).toBe(0)
    },
  }
}

describe('loss readers tell a withheld assignment from a lost one', () => {
  let fixture
  beforeEach(() => jest.useFakeTimers(FAKE_TIME))
  afterEach(() => {
    fixture?.close()
    fixture = undefined
  })

  test('a revoked verdict with a withheld token stays a revoked round (withhold driven directly)', async () => {
    fixture = buildRunner()
    fixture.group.ownership.withhold(refusal())
    await fixture.runner.resolveRebalanceVerdict({
      kind: 'revoked',
      reason: 'rebalance-in-progress',
      error: new Error('round'),
    })
    expect(fixture.round).toHaveBeenCalledWith('rebalance-in-progress', 'revoked')
    expect(fixture.group.discardUncommittedOffsets).not.toHaveBeenCalled()
  })

  test('no loss is owed for a withheld token, and one is once it is lost (withhold driven directly)', () => {
    fixture = buildRunner()
    const { ownership } = fixture.group
    ownership.withhold(refusal())
    expect(fixture.runner.rebalanceCoordinator.lossOwed()).toBe(false)
    ownership.lose(brokerError(25), ownership.token())
    expect(fixture.runner.rebalanceCoordinator.lossOwed()).toBe(true)
  })

  test.each([
    [
      'onPartitionsRevoked',
      'withheld through the primitive',
      (ownership) => ownership.withhold(refusal()),
    ],
    ['onPartitionsLost', 'lost', (ownership) => ownership.lose(brokerError(25), 0)],
  ])('the close callback is %s for a token that is %s', async (hook, _, mark) => {
    fixture = buildRunner()
    mark(fixture.group.ownership)
    await fixture.runner.invokeCloseHook({ previous: [{ topic: TOPIC, partitions: [0] }] })
    expect(fixture.group.invokeHook).toHaveBeenCalledTimes(1)
    expect(fixture.group.invokeHook.mock.calls[0][1]).toBe(hook)
  })

  test('an eager preparation reports no loss for a withheld token (withhold driven directly)', async () => {
    fixture = buildRunner()
    jest.spyOn(fixture.runner, 'quiesceRun').mockResolvedValue()
    fixture.group.ownership.withhold(refusal())
    const round = { roundDeadlineAt: Infinity, wait: (work) => work(), guard: () => null }
    await expect(fixture.runner.prepareEagerRound(round)).resolves.toBe(false)
  })

  test('an automatic commit is not attempted while withheld, and the refusal says why (withhold driven directly)', () => {
    fixture = buildRunner()
    fixture.group.ownership.withhold(refusal())
    expect(fixture.runner.autoCommitOffsets(null)).toBeUndefined()
    expect(fixture.group.commitOffsets).not.toHaveBeenCalled()
    expect(fixture.runner.commitRefusal().reason).toBe('assignment-refused')
  })
})

describe('an install after a withhold', () => {
  test('a consumer-protocol install is not refused by a withheld token (withhold driven directly)', async () => {
    h = await opened({ partitions: [0], groupProtocol: 'consumer' })
    const token = h.group.ownership.token()
    h.group.withholdAssignment(refusal())
    await install(h, [0], true)
    expect(h.group.ownership.token()).toBe(token + 1)
    expect(h.group.assigned()).toEqual([{ topic: TOPIC, partitions: [0] }])
  })
})

describe('the fetch authority', () => {
  test('an ordinary install, cooperative or eager, keeps the one open authority', async () => {
    h = await opened({ partitions: [0, 1] })
    const authority = h.group.authority
    expect(authority).toBeInstanceOf(Generation)
    await install(h, [0, 1])
    await install(h, [0])
    await install(h, [0, 1], true)
    expect(h.group.authority).toBe(authority)
    expect(authority.closed).toBe(false)
  })

  test('a withhold closes the authority with its refusal; only the next usable install opens another, kept after that (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    const first = h.group.authority
    const cause = refusal()
    h.group.withholdAssignment(cause)
    expect(first.closed).toBe(true)
    expect(first.error).toBe(cause)
    expect(h.group.authority).toBe(first)
    await heal(h, [0])
    const second = h.group.authority
    expect(second).not.toBe(first)
    expect(second.closed).toBe(false)
    expect(first.closed).toBe(true)
    await install(h, [0])
    expect(h.group.authority).toBe(second)
  })

  test('the authority closes with its run and a new run opens a new one', async () => {
    h = await opened({ partitions: [0] })
    const authority = h.group.authority
    h.group.onStopRequested()
    expect(authority.closed).toBe(true)
    h.group.startIdleWait()
    expect(h.group.authority).not.toBe(authority)
    expect(h.group.authority.closed).toBe(false)
  })

  test('repeated withhold and heal cycles leave one authority registration on the run (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    const tracker = trackRegistrations(h.group.idleWait.fence)
    for (let cycle = 0; cycle < 3; cycle++) {
      h.group.withholdAssignment(refusal())
      await heal(h, [0])
    }
    expect(tracker.active).toBe(1)
  })
})

const STRANDED = 'a Fetch sent under one install, then a cooperative install and a withhold, %s'

describe.each([
  ['without a heal before the reply', false],
  ['with a heal before the reply', true],
])(STRANDED, (_, healFirst) => {
  test('publishes nothing from its reply and resets only its own pending session (withhold driven directly)', async () => {
    h = await opened({ partitions: [0, 1], knownNodes: [0, 1] })
    await install(h, [0, 1])
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)

    const held = holdFetches(h.broker)
    h.setResponse(
      reply(42, [
        { partition: 0, records: false, preferredReadReplica: 1 },
        { partition: 1, errorCode: 1, records: false },
      ])
    )
    const pending = h.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)
    const captured = h.group.authority
    await install(h, [0, 1])
    expect(h.group.authority).toBe(captured)
    h.group.withholdAssignment(refusal())
    expect(captured.closed).toBe(true)
    if (healFirst) await heal(h, [0, 1])
    held.release()
    expect(await pending).toEqual([])

    expect(h.group.preferredReadReplicasPerTopicPartition[TOPIC]?.[0]).toBeUndefined()
    expect(h.requests().map(({ apiKey }) => apiKey)).not.toContain(apiKeys.ListOffsets)
    expect(h.offsetCommit).not.toHaveBeenCalled()

    h.broker.fetch.mockRestore()
    if (!healFirst) await heal(h, [0, 1])
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    const fullRequestsShuffledByTheBroker = (frame) => ({
      ...frame,
      topics: [...frame.topics].sort((a, b) => a - b),
    })
    expect(h.fetchFrames().map(shape).slice(-3).map(fullRequestsShuffledByTheBroker)).toEqual([
      { sessionId: 0, sessionEpoch: 0, topics: [0, 1], forgotten: [] },
      { sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 42, sessionEpoch: 0, topics: [0, 1], forgotten: [] },
    ])
    expect(h.fetchRequestOffsets()).toEqual(['5', '5'])
    expect(h.findBrokerCalls.at(-1).nodeId).toBe(NODE_ID)
  })
})

describe('a Fetch that crosses a withhold', () => {
  test('a withhold while the reply rows are processed drops the rest and owes the session no reset (withhold driven directly)', async () => {
    h = await opened({ partitions: [0, 1], knownNodes: [0, 1] })
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    const hint = gate()
    h.cluster.applyLeaderHint = jest.fn(() => hint.promise)
    h.setResponse(
      reply(42, [
        {
          partition: 0,
          errorCode: 6,
          records: false,
          currentLeader: { leaderId: 1, leaderEpoch: 9 },
        },
        1,
      ])
    )
    const pending = h.group.fetch(NODE_ID)
    await until(() => h.cluster.applyLeaderHint.mock.calls.length === 1)
    h.group.withholdAssignment(refusal())
    hint.release(true)
    expect(await pending).toEqual([])

    await heal(h, [0, 1])
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    expect(h.fetchFrames().map(shape).slice(-2)).toEqual([
      { sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 42, sessionEpoch: 2, topics: [], forgotten: [] },
    ])
  })

  test("a withhold leaves another node's session untouched (withhold driven directly)", async () => {
    h = await opened({ partitions: [0, 1], partitionLeaders: { 0: 0, 1: 1 }, knownNodes: [0, 1] })
    h.setResponse(reply(11, [0]))
    await h.group.fetch(0)
    h.setResponse(reply(12, [1]))
    await h.group.fetch(1)

    const held = holdFetches(h.broker)
    h.setResponse(reply(11, [0]))
    const pending = h.group.fetch(0)
    await until(() => held.pending() === 1)
    h.group.withholdAssignment(refusal())
    held.release()
    expect(await pending).toEqual([])
    h.broker.fetch.mockRestore()

    await heal(h, [0, 1])
    h.setResponse(reply(12, [1]))
    await h.group.fetch(1)
    h.setResponse(reply(11, [0]))
    await h.group.fetch(0)
    expect(h.fetchFrames().map(shape).slice(-2)).toEqual([
      { sessionId: 12, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 11, sessionEpoch: 0, topics: [0], forgotten: [] },
    ])
  })

  test('a failed answer of a crossing Fetch resets its session and recovers nothing (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    h.setResponse(reply(42, [0]))
    await h.group.fetch(NODE_ID)
    const held = holdFetches(h.broker)
    held.failNext(Object.assign(new Error('not leader'), { type: 'NOT_LEADER_FOR_PARTITION' }))
    const pending = h.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)
    h.group.withholdAssignment(refusal())
    await heal(h, [0])
    held.release()
    expect(await pending).toEqual([])
    expect(h.cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(h.requestRejoin).not.toHaveBeenCalled()

    h.broker.fetch.mockRestore()
    h.setResponse(reply(42, [0]))
    await h.group.fetch(NODE_ID)
    expect(shape(h.fetchFrames().at(-1))).toEqual({
      sessionId: 42,
      sessionEpoch: 0,
      topics: [0],
      forgotten: [],
    })
  })

  test('a refresh the reply started is bound to the authority, and a withhold during it returns no batch (withhold driven directly)', async () => {
    h = await opened({ partitions: [0, 1] })
    const refresh = gate()
    h.cluster.refreshMetadata = jest.fn(() => refresh.promise)
    const authority = h.group.authority
    h.setResponse(reply(42, [{ partition: 0, errorCode: 6, records: false }, 1]))
    const pending = h.group.fetch(NODE_ID)
    await until(() => h.cluster.refreshMetadata.mock.calls.length === 1)
    expect(h.cluster.refreshMetadata).toHaveBeenCalledWith(
      expect.objectContaining({ fence: authority })
    )
    h.group.withholdAssignment(refusal())
    refresh.release()
    expect(await pending).toEqual([])
  })

  const crossings = [
    [
      'the metadata wait before preparation',
      (group, held, reached) => {
        group.cluster.refreshMetadataIfNecessary = () => {
          reached()
          return held.promise
        }
      },
    ],
    [
      'the seek wait',
      (group, held, reached) => {
        const seek = group.seekOffsets.bind(group)
        group.seekOffsets = async (...args) => {
          reached()
          await held.promise
          return seek(...args)
        }
      },
    ],
    [
      'the validation wait',
      (group, held, reached) => {
        const validate = group.validatePositions.bind(group)
        group.validatePositions = async (...args) => {
          reached()
          await held.promise
          return validate(...args)
        }
      },
    ],
    [
      'the broker lookup',
      (group, held, reached) => {
        const find = group.cluster.findBroker
        group.cluster.findBroker = async (args) => {
          reached()
          await held.promise
          return find(args)
        }
      },
    ],
  ]

  test.each(crossings)(
    'a Fetch crossing %s returns nothing and writes no frame, also after a heal (withhold driven directly)',
    async (_, hold) => {
      h = await opened({ partitions: [0, 1] })
      const held = gate()
      let entered = false
      hold(h.group, held, () => {
        entered = true
      })
      const frames = h.fetchFrames().length
      const pending = h.group.fetch(NODE_ID)
      await until(() => entered)
      h.group.withholdAssignment(refusal())
      await heal(h, [0, 1])
      held.release()
      expect(await pending).toEqual([])
      expect(h.fetchFrames()).toHaveLength(frames)
    }
  )

  test('a request refused by its guard before the write is not written and resets its session (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    h.setResponse(reply(42, [0]))
    await h.group.fetch(NODE_ID)
    const frames = h.fetchFrames().length
    const held = gate()
    let entered = false
    const real = h.broker.fetch.bind(h.broker)
    jest.spyOn(h.broker, 'fetch').mockImplementation(async (fields) => {
      entered = true
      await held.promise
      return real(fields)
    })
    const pending = h.group.fetch(NODE_ID)
    await until(() => entered)
    h.group.withholdAssignment(refusal())
    await heal(h, [0])
    held.release()
    expect(await pending).toEqual([])
    expect(h.fetchFrames()).toHaveLength(frames)

    h.broker.fetch.mockRestore()
    h.setResponse(reply(42, [0]))
    await h.group.fetch(NODE_ID)
    expect(shape(h.fetchFrames().at(-1))).toEqual({
      sessionId: 42,
      sessionEpoch: 0,
      topics: [0],
      forgotten: [],
    })
  })
})

describe('fetching while withheld', () => {
  test('a withheld assignment parks every Fetch for its wait time with no preparation and no spin (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    jest.useFakeTimers(FAKE_TIME)
    h.group.maxWaitTime = 1000
    const prepare = jest.spyOn(h.group, 'prepareFetch')
    const frames = h.fetchFrames().length
    h.group.withholdAssignment(refusal())
    let rounds = 0
    const loop = { running: true }
    const worker = (async () => {
      while (loop.running) {
        rounds += 1
        await h.group.fetch(NODE_ID)
        await turn()
      }
    })()
    for (let step = 0; step < 30; step++) {
      await jest.advanceTimersByTimeAsync(1000)
      for (let n = 0; n < 20; n++) await turn()
    }
    expect(rounds).toBeGreaterThanOrEqual(30)
    expect(rounds).toBeLessThanOrEqual(31)
    expect(prepare).not.toHaveBeenCalled()
    expect(h.fetchFrames()).toHaveLength(frames)
    loop.running = false
    h.group.onStopRequested()
    await drain()
    await worker
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a withhold ends an idle Fetch at once and the next Fetch parks without preparing (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    jest.useFakeTimers(FAKE_TIME)
    h.group.maxWaitTime = 5000
    h.group.pause([{ topic: TOPIC }])
    const settledAt = []
    const idle = h.group.fetch(NODE_ID).then(() => settledAt.push(Date.now()))
    await drain()
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(100)
    h.group.withholdAssignment(refusal())
    await drain()
    await idle
    expect(settledAt).toEqual([100])

    const prepare = jest.spyOn(h.group, 'prepareFetch')
    let parked = false
    const next = h.group.fetch(NODE_ID).then(() => {
      parked = true
    })
    await drain()
    expect(parked).toBe(false)
    expect(jest.getTimerCount()).toBe(1)
    expect(prepare).not.toHaveBeenCalled()
    h.group.onStopRequested()
    await drain()
    await next
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a heal wakes a parked Fetch and resumes with fresh acquisitions and the seek made while withheld (withhold driven directly)', async () => {
    h = await opened({ partitions: [0, 1] })
    h.group.maxWaitTime = 60000
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    const before = h.group.acquired.get(`${TOPIC}|0`)
    h.group.seek({ topic: TOPIC, partition: 0, offset: '7' })
    h.group.withholdAssignment(refusal())
    expect(before.retired).toBe(true)
    expect(h.group.acquired.size).toBe(0)
    h.group.seek({ topic: TOPIC, partition: 0, offset: '9' })

    let woke = false
    const parked = h.group.fetch(NODE_ID).then((batches) => {
      woke = true
      return batches
    })
    for (let n = 0; n < 5; n++) await turn()
    expect(woke).toBe(false)
    await heal(h, [0, 1])
    expect(await parked).toEqual([])
    expect(h.group.acquired.get(`${TOPIC}|0`)).not.toBe(before)

    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    const last = h.fetchFrames().at(-1)
    expect(shape(last)).toEqual({ sessionId: 42, sessionEpoch: 1, topics: [0], forgotten: [] })
    expect(last.topics[0].partitions[0].fetchOffset).toBe('9')
  })

  test('a healed Fetch never joins the leaderless refresh its closed authority started (withhold driven directly)', async () => {
    h = await answeringOffsetFetch(
      await createFetchGroup({
        partitions: [0],
        partitionLeaders: { 0: -1 },
        retry: { initialRetryTime: 0, retries: 0 },
      })
    )
    const options = []
    const releases = []
    h.cluster.refreshMetadata.mockImplementation((given) => {
      options.push(given)
      return new Promise((resolve) => releases.push(resolve))
    })
    const first = h.group.authority
    const round = h.group.fetch(NODE_ID)
    await until(() => options.length === 1)
    expect(options[0]).toMatchObject({ fence: first })
    h.group.withholdAssignment(refusal())
    expect(await round).toEqual([])

    await heal(h, [0])
    const second = h.group.authority
    expect(second).not.toBe(first)
    const next = h.group.fetch(NODE_ID)
    await until(() => options.length === 2)
    expect(options[1]).toMatchObject({ fence: second })
    releases.splice(0).forEach((resolve) => resolve())
    h.group.onStopRequested()
    expect(await next).toEqual([])
  })
})

describe('seeks across a withhold', () => {
  test('a queued seek is dropped by the withhold and the next acquisition starts from the committed offset (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    h.setResponse(reply(42, [0]))
    await h.group.fetch(NODE_ID)
    h.group.seek({ topic: TOPIC, partition: 0, offset: '7' })
    h.group.withholdAssignment(refusal())
    expect(h.group.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
    await heal(h, [0])
    h.setResponse(reply(42, [0]))
    await h.group.fetch(NODE_ID)
    expect(shape(h.fetchFrames().at(-1))).toEqual({
      sessionId: 42,
      sessionEpoch: 1,
      topics: [],
      forgotten: [],
    })
  })

  test('a seek whose commit waits for its coordinator when withheld is never written (withhold driven directly)', async () => {
    h = await opened({ partitions: [0], autoCommit: true })
    const discovery = gate()
    h.offsetManager.coordinator = { isConnected: () => false }
    h.cluster.findGroupCoordinator = jest.fn(() => discovery.promise)
    h.group.seek({ topic: TOPIC, partition: 0, offset: '9' })
    const pending = h.group.fetch(NODE_ID)
    await until(() => h.cluster.findGroupCoordinator.mock.calls.length === 1)
    h.group.withholdAssignment(refusal())
    expect(await pending).toEqual([])
    discovery.release(h.broker)
    for (let n = 0; n < 10; n++) await turn()
    expect(h.offsetCommit).not.toHaveBeenCalled()
  })

  test('a seek whose commit was written before the withhold takes no local effect after a heal (withhold driven directly)', async () => {
    h = await opened({ partitions: [0], autoCommit: true })
    const write = gate()
    h.offsetCommit.mockImplementation(() => write.promise)
    h.group.seek({ topic: TOPIC, partition: 0, offset: '9' })
    const pending = h.group.fetch(NODE_ID)
    await until(() => h.offsetCommit.mock.calls.length === 1)
    h.group.withholdAssignment(refusal())
    expect(await pending).toEqual([])
    await heal(h, [0])
    write.release({ topics: [] })
    for (let n = 0; n < 10; n++) await turn()

    h.setResponse(reply(42, [0]))
    await h.group.fetch(NODE_ID)
    expect(h.fetchRequestOffsets()).toEqual(['5'])
    expect(h.group.offsetManager.committedOffsets()[TOPIC][0]).toBe('5')
  })
})

/**
 * A real offset manager and ownership over one held OffsetCommit per call. A new manager evicts
 * its assigned cells from the shared committed cache, so the cell of `t[0]` starts empty and only
 * a recorded commit fills it.
 * @returns {object} manager, ownership, held answers, committed cache, events and losses
 */
const commitSetup = () => {
  const ownership = new GroupOwnership()
  const held = []
  const coordinator = {
    isConnected: () => true,
    offsetCommit: jest.fn(() => new Promise((resolve, reject) => held.push({ resolve, reject }))),
  }
  const committed = {}
  const emitter = new InstrumentationEventEmitter()
  const events = []
  emitter.addListener(COMMIT_OFFSETS, (event) => events.push(event))
  const lost = []
  const manager = new OffsetManager({
    cluster: { committedOffsets: () => committed, topicId: () => undefined },
    coordinator,
    memberAssignment: { t: [0] },
    autoCommit: true,
    topicConfigurations: { t: {} },
    instrumentationEmitter: emitter,
    groupId: 'g',
    generationId: 7,
    memberId: 'm',
    ownership,
    onCommitError: (error) => lost.push(error),
    logger: newLogger(),
  })
  return { manager, ownership, held, coordinator, events, lost }
}

const OFFSETS = { topics: [{ topic: 't', partitions: [{ partition: 0, offset: '1' }] }] }

/**
 * Lets a step run in the gap between the answer and the commit event.
 * @param {object} c the commit setup
 * @param {() => void} step what lands in the gap
 * @returns {void}
 */
const inPublicationGap = (c, step) => {
  const send = c.manager.sendOffsetCommit.bind(c.manager)
  jest.spyOn(c.manager, 'sendOffsetCommit').mockImplementation(async (...args) => {
    const result = await send(...args)
    step()
    return result
  })
}

describe('commits across a withhold', () => {
  test('an explicit commit while withheld is refused before any write (withhold driven directly)', async () => {
    const c = commitSetup()
    c.ownership.withhold(refusal())
    await expect(c.manager.commitOffsets(OFFSETS)).rejects.toMatchObject({
      reason: 'assignment-refused',
    })
    expect(c.coordinator.offsetCommit).not.toHaveBeenCalled()
  })

  test('an automatic commit while withheld resolves and writes nothing (withhold driven directly)', async () => {
    const c = commitSetup()
    c.ownership.withhold(refusal())
    await expect(c.manager.commitOffsets(OFFSETS, { auto: true })).resolves.toBeUndefined()
    expect(c.coordinator.offsetCommit).not.toHaveBeenCalled()
  })

  test('a commit whose coordinator lookup is pending at the withhold is never written (withhold driven directly)', async () => {
    const c = commitSetup()
    const discovery = gate()
    c.manager.coordinator = { isConnected: () => false }
    c.manager.findCoordinator = jest.fn(() => discovery.promise)
    const sending = c.manager.commitOffsets(OFFSETS).catch((error) => error)
    await until(() => c.manager.findCoordinator.mock.calls.length === 1)
    c.ownership.withhold(refusal())
    discovery.release(c.coordinator)
    expect(await sending).toMatchObject({ reason: 'assignment-refused' })
    expect(c.coordinator.offsetCommit).not.toHaveBeenCalled()
  })

  test('a commit written before the withhold and accepted after it is refused, reported to no listener and not cached (withhold driven directly)', async () => {
    const c = commitSetup()
    const sending = c.manager.commitOffsets(OFFSETS).catch((error) => error)
    await until(() => c.held.length === 1)
    c.ownership.withhold(refusal())
    c.held[0].resolve({ topics: [] })
    expect(await sending).toMatchObject({ reason: 'assignment-refused' })
    expect(c.events).toEqual([])
    expect(c.manager.committedOffsets().t[0]).toBeUndefined()
  })

  test('a loss answered to a commit written before the withhold is learned and the caller still gets assignment-refused (withhold driven directly)', async () => {
    const c = commitSetup()
    const cause = refusal()
    const sending = c.manager.commitOffsets(OFFSETS).catch((error) => error)
    await until(() => c.held.length === 1)
    c.ownership.withhold(cause)
    const loss = brokerError(25)
    c.held[0].reject(loss)
    const error = await sending
    expect(error).toBeInstanceOf(KafkaJSCommitFailedError)
    expect(error.reason).toBe('assignment-refused')
    expect(error.cause).toBe(cause)
    expect(c.ownership.lost(c.ownership.token())).toBe(loss)
    expect(c.lost).toEqual([loss])
  })

  test('a withhold after the answer and before the commit event suppresses the event and the cache write (withhold driven directly)', async () => {
    const c = commitSetup()
    inPublicationGap(c, () => c.ownership.withhold(refusal()))
    const sending = c.manager.commitOffsets(OFFSETS).catch((error) => error)
    await until(() => c.held.length === 1)
    c.held[0].resolve({ topics: [] })
    expect(await sending).toMatchObject({ reason: 'assignment-refused' })
    expect(c.events).toEqual([])
    expect(c.manager.committedOffsets().t[0]).toBeUndefined()
  })
})

describe('what a withhold leaves to a running handler', () => {
  test('uncommittedOffsets() read after a withhold is an empty set (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    h.setResponse(reply(42, [0]))
    await h.group.fetch(NODE_ID)
    const old = h.group.offsetManager
    h.group.resolveOffset({ topic: TOPIC, partition: 0, offset: '5' })
    expect(h.group.uncommittedOffsets().topics).toHaveLength(1)
    h.group.withholdAssignment(refusal())
    expect(h.group.offsetManager).not.toBe(old)
    expect(h.group.uncommittedOffsets()).toEqual({ topics: [] })
  })

  test('a batch fetched before a withhold is superseded for delivery and as a transaction input (withhold driven directly)', async () => {
    h = await opened({ partitions: [0] })
    h.setResponse(reply(42, [0]))
    const [batch] = await h.group.fetch(NODE_ID)
    const offsets = new TransactionOffsets({ error: jest.fn() })
    offsets.enlist(batch.acquisition, batch.acquireClock)
    expect(offsets.supersededInput()).toBeNull()
    h.group.withholdAssignment(refusal())
    expect(isSuperseded(batch)).toBe(true)
    expect(h.group.isAssigned(TOPIC, 0)).toBe(false)
    expect(offsets.supersededInput()).toEqual({
      acquisition: batch.acquisition,
      acquireClock: batch.acquireClock,
    })
    offsets.release()
  })
})

describe('ordinary installs, losses and commits stay as they were', () => {
  test('a cooperative install while a Fetch is on the wire keeps its reply and continues its session', async () => {
    h = await opened({ partitions: [0, 1] })
    await install(h, [0, 1])
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    const held = holdFetches(h.broker)
    h.setResponse(reply(42, [0, 1]))
    const pending = h.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)
    await install(h, [0, 1])
    held.release()
    expect((await pending).map(({ partition }) => partition)).toEqual([0, 1])
    h.broker.fetch.mockRestore()
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    expect(h.fetchFrames().map(shape).slice(-2)).toEqual([
      { sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 42, sessionEpoch: 2, topics: [], forgotten: [] },
    ])
  })

  test('an eager install while a Fetch is on the wire continues its session', async () => {
    h = await opened({ partitions: [0, 1] })
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    const held = holdFetches(h.broker)
    h.setResponse(reply(42, [0, 1]))
    const pending = h.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)
    await install(h, [0, 1], true)
    held.release()
    await pending
    h.broker.fetch.mockRestore()
    h.setResponse(reply(42, [0, 1]))
    await h.group.fetch(NODE_ID)
    expect(h.fetchFrames().map(shape).slice(-2)).toEqual([
      { sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 42, sessionEpoch: 2, topics: [], forgotten: [] },
    ])
  })

  test('a cooperative install that keeps a partition keeps its transaction input standing', async () => {
    h = await opened({ partitions: [0, 1] })
    await install(h, [0, 1])
    h.setResponse(reply(42, [0, 1]))
    const [batch] = await h.group.fetch(NODE_ID)
    const offsets = new TransactionOffsets({ error: jest.fn() })
    offsets.enlist(batch.acquisition, batch.acquireClock)
    await install(h, [0, 1])
    expect(offsets.supersededInput()).toBeNull()
    expect(isSuperseded(batch)).toBe(false)
    offsets.release()
  })

  test('a loss refuses its token as membership-lost and a renew makes it stale', () => {
    const ownership = new GroupOwnership()
    const token = ownership.token()
    const loss = brokerError(22)
    ownership.lose(loss, token)
    expect(ownership.refusal(token)).toMatchObject({ reason: 'membership-lost', cause: loss })
    const fresh = ownership.renew()
    expect(ownership.refusal(fresh)).toBeNull()
    expect(ownership.refusal(token).reason).toBe('stale-assignment')
  })

  test('an accepted commit is reported once and cached', async () => {
    const c = commitSetup()
    const sending = c.manager.commitOffsets(OFFSETS)
    await until(() => c.held.length === 1)
    c.held[0].resolve({ topics: [] })
    await expect(sending).resolves.toBeUndefined()
    expect(c.events).toHaveLength(1)
    expect(c.manager.committedOffsets().t[0]).toBe('1')
  })

  test('a loss learned after the answer and before the commit event still reports the commit and caches nothing', async () => {
    const c = commitSetup()
    inPublicationGap(c, () => c.ownership.lose(brokerError(25), c.ownership.token()))
    const sending = c.manager.commitOffsets(OFFSETS).catch((error) => error)
    await until(() => c.held.length === 1)
    c.held[0].resolve({ topics: [] })
    expect(await sending).toMatchObject({ reason: 'membership-lost' })
    expect(c.events).toHaveLength(1)
    expect(c.manager.committedOffsets().t[0]).toBeUndefined()
  })
})
