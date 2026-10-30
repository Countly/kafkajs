const OffsetManager = require('../index')
const ConsumerGroup = require('../../consumerGroup')
const createAdmin = require('../../../admin')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { KafkaJSProtocolError } = require('../../../errors')
const { createErrorFromCode } = require('../../../protocol/error')
const { requests } = require('../../../protocol/requests')
const { newLogger } = require('testHelpers')

const UNSTABLE_OFFSET_COMMIT = 88
const STALE_MEMBER_EPOCH = 113
const COORDINATOR_LOAD_IN_PROGRESS = 14
const REBALANCE_IN_PROGRESS = 27
const TOPIC = 'topic-a'
const GROUP_ID = 'group-a'

const protocolError = (code) => new KafkaJSProtocolError(createErrorFromCode(code))

/**
 * An offset manager over a fake coordinator whose `offsetFetch` is the given mock.
 * @param {{ membershipProtocol?: 'classic'|'consumer', offsetFetch?: jest.Mock,
 *   isStopping?: () => boolean, isRebalancePending?: () => boolean, markPolled?: () => void,
 *   staleEpochTimeout?: number, logger?: object }} [options]
 */
const build = ({
  membershipProtocol = 'classic',
  offsetFetch,
  isStopping,
  isRebalancePending,
  markPolled,
  staleEpochTimeout = 1000,
  logger,
} = {}) => {
  const coordinator = {
    isConnected: () => true,
    offsetFetch: offsetFetch || jest.fn(async () => ({ responses: [] })),
  }
  const cluster = {
    committedOffsets: () => ({}),
    findGroupCoordinator: jest.fn(async () => coordinator),
    fetchTopicsOffset: jest.fn(async () => []),
    topicId: () => undefined,
  }

  const manager = new OffsetManager({
    cluster,
    coordinator,
    memberAssignment: { [TOPIC]: [0, 1] },
    autoCommit: true,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { [TOPIC]: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: GROUP_ID,
    generationId: 3,
    memberId: 'member-a',
    membershipProtocol,
    staleEpochTimeout,
    isStopping,
    isRebalancePending,
    markPolled,
    logger: logger || newLogger(),
  })

  return { manager, coordinator }
}

/** An OffsetFetch that answers each call from the script, then succeeds. */
const scripted = (...errors) =>
  jest.fn(async () => {
    const error = errors.shift()
    if (error) {
      throw error
    }
    return { responses: [] }
  })

/** The request bytes a payload becomes at OffsetFetch `version`. */
const encode = async (version, payload) => {
  const { request } = requests.OffsetFetch.protocol({ version })(payload)
  return (await request.encode()).buffer
}

describe('Consumer > OffsetManager > stable offsets', () => {
  afterEach(() => {
    jest.useRealTimers()
  })

  test.each(['classic', 'consumer'])(
    'every %s OffsetFetch asks for stable offsets',
    async (protocol) => {
      const { manager, coordinator } = build({ membershipProtocol: protocol })

      await manager.resolveOffsets()

      expect(coordinator.offsetFetch).toHaveBeenCalledTimes(1)
      expect(coordinator.offsetFetch).toHaveBeenCalledWith(
        expect.objectContaining({ groupId: GROUP_ID, requireStable: true })
      )
    }
  )

  test.each([7, 8, 9])('RequireStable is true on the wire at OffsetFetch v%d', async (version) => {
    const { manager, coordinator } = build({ membershipProtocol: 'consumer' })
    await manager.resolveOffsets()
    const [payload] = coordinator.offsetFetch.mock.calls[0]

    const sent = await encode(version, payload)

    expect(sent).not.toEqual(await encode(version, { ...payload, requireStable: false }))
  })

  test("the admin client's OffsetFetch does not ask for stable offsets", async () => {
    const coordinator = { offsetFetch: jest.fn(async () => ({ responses: [] })) }
    const admin = createAdmin({
      logger: newLogger(),
      retry: { retries: 0 },
      cluster: { findGroupCoordinator: async () => coordinator },
    })

    await admin.fetchOffsets({ groupId: GROUP_ID, topics: [] })

    const [payload] = coordinator.offsetFetch.mock.calls[0]
    expect(payload).toEqual({ groupId: GROUP_ID, topics: [] })
    expect(await encode(9, payload)).toEqual(await encode(9, { ...payload, requireStable: false }))
  })

  test.each(['classic', 'consumer'])(
    'UNSTABLE_OFFSET_COMMIT is re-sent until the transaction resolves (%s), with no member epoch needed',
    async (protocol) => {
      jest.useFakeTimers()
      const offsetFetch = scripted(
        protocolError(UNSTABLE_OFFSET_COMMIT),
        protocolError(UNSTABLE_OFFSET_COMMIT)
      )
      const { manager, coordinator } = build({ membershipProtocol: protocol, offsetFetch })
      manager.membership = () => ({ generationId: 0, memberId: 'member-a', coordinator })

      const resolving = manager.resolveOffsets()
      await jest.advanceTimersByTimeAsync(300)
      await resolving

      expect(offsetFetch).toHaveBeenCalledTimes(3)
      expect(offsetFetch.mock.calls.every(([{ requireStable }]) => requireStable)).toBe(true)
    }
  )

  test('waits 100 ms between attempts and says why at debug level', async () => {
    jest.useFakeTimers()
    const logger = { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
    const offsetFetch = scripted(protocolError(UNSTABLE_OFFSET_COMMIT))
    const { manager } = build({ offsetFetch, logger })

    const resolving = manager.resolveOffsets()
    await jest.advanceTimersByTimeAsync(99)
    expect(offsetFetch).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    await resolving

    expect(offsetFetch).toHaveBeenCalledTimes(2)
    expect(logger.debug).toHaveBeenCalledWith(
      'OffsetFetch answered while a transactional offset commit is open, re-sending',
      expect.objectContaining({ groupId: GROUP_ID })
    )
  })

  test.each(['classic', 'consumer'])(
    'an UNSTABLE_OFFSET_COMMIT that never clears is thrown as the last error at the deadline (%s)',
    async (protocol) => {
      jest.useFakeTimers()
      let last
      const offsetFetch = jest.fn(async () => {
        last = protocolError(UNSTABLE_OFFSET_COMMIT)
        throw last
      })
      const { manager } = build({ membershipProtocol: protocol, offsetFetch })

      const resolving = manager.resolveOffsets().catch((e) => e)
      await jest.advanceTimersByTimeAsync(2000)
      const caught = await resolving

      expect(caught).toBe(last)
      expect(caught).toBeInstanceOf(KafkaJSProtocolError)
      expect(caught).toMatchObject({ type: 'UNSTABLE_OFFSET_COMMIT', retriable: true })
      // Sent at 0, 100, 300, 700 and 1 500 ms; the deadline is 1 000 ms.
      expect(offsetFetch).toHaveBeenCalledTimes(5)
    }
  )

  test('a stop abandons the retry at once, before and after the sleep', async () => {
    jest.useFakeTimers()
    let stopping = false
    const offsetFetch = jest.fn(async () => {
      throw protocolError(UNSTABLE_OFFSET_COMMIT)
    })
    const { manager } = build({ offsetFetch, isStopping: () => stopping })

    const resolving = manager.resolveOffsets().catch((e) => e)
    await jest.advanceTimersByTimeAsync(50)
    stopping = true
    await jest.advanceTimersByTimeAsync(50)
    const caught = await resolving

    expect(caught.type).toEqual('UNSTABLE_OFFSET_COMMIT')
    expect(offsetFetch).toHaveBeenCalledTimes(1)

    const stopped = build({
      offsetFetch: scripted(protocolError(UNSTABLE_OFFSET_COMMIT)),
      isStopping: () => true,
    })
    const rejected = stopped.manager.resolveOffsets().catch((e) => e)
    await jest.advanceTimersByTimeAsync(0)
    expect(jest.getTimerCount()).toBe(0)
    expect(await rejected).toMatchObject({ type: 'UNSTABLE_OFFSET_COMMIT' })
    expect(stopped.coordinator.offsetFetch).toHaveBeenCalledTimes(1)
  })

  test.each([
    ['UNSTABLE_OFFSET_COMMIT', 'classic', UNSTABLE_OFFSET_COMMIT],
    ['UNSTABLE_OFFSET_COMMIT', 'consumer', UNSTABLE_OFFSET_COMMIT],
    ['STALE_MEMBER_EPOCH', 'consumer', STALE_MEMBER_EPOCH],
  ])(
    'a pending rebalance ends a %s wait (%s) within one interval, with the last error',
    async (_, protocol, code) => {
      jest.useFakeTimers()
      let pending = false
      let last
      let settled
      const offsetFetch = jest.fn(async () => {
        last = protocolError(code)
        throw last
      })
      const { manager } = build({
        membershipProtocol: protocol,
        offsetFetch,
        isRebalancePending: () => pending,
      })

      manager.resolveOffsets().catch((e) => (settled = e))
      await jest.advanceTimersByTimeAsync(50)
      pending = true
      await jest.advanceTimersByTimeAsync(50)

      expect(settled).toBe(last)
      expect(offsetFetch).toHaveBeenCalledTimes(1)

      const yielded = build({
        membershipProtocol: protocol,
        offsetFetch: scripted(protocolError(code)),
        isRebalancePending: () => true,
      })
      const caught = yielded.manager.resolveOffsets().catch((e) => e)
      await jest.advanceTimersByTimeAsync(0)
      expect(jest.getTimerCount()).toBe(0)
      expect(await caught).toMatchObject({ code })
      expect(yielded.coordinator.offsetFetch).toHaveBeenCalledTimes(1)
    }
  )

  test('with no rebalance pending the wait goes on', async () => {
    jest.useFakeTimers()
    const isRebalancePending = jest.fn(() => false)
    let settled = false
    const offsetFetch = jest.fn(async () => {
      throw protocolError(UNSTABLE_OFFSET_COMMIT)
    })
    const { manager } = build({ offsetFetch, isRebalancePending })

    const resolving = manager.resolveOffsets().catch((e) => e)
    resolving.then(() => (settled = true))
    await jest.advanceTimersByTimeAsync(500)

    expect(settled).toBe(false)
    expect(offsetFetch.mock.calls.length).toBeGreaterThan(1)
    expect(isRebalancePending).toHaveBeenCalled()

    await jest.advanceTimersByTimeAsync(2000)
    expect((await resolving).type).toEqual('UNSTABLE_OFFSET_COMMIT')
  })

  test.each(['classic', 'consumer'])(
    'each UNSTABLE_OFFSET_COMMIT re-send counts as a poll (%s)',
    async (protocol) => {
      jest.useFakeTimers()
      const markPolled = jest.fn()
      const offsetFetch = scripted(
        protocolError(UNSTABLE_OFFSET_COMMIT),
        protocolError(UNSTABLE_OFFSET_COMMIT)
      )
      const { manager } = build({ membershipProtocol: protocol, offsetFetch, markPolled })

      const resolving = manager.resolveOffsets()
      await jest.advanceTimersByTimeAsync(100)
      expect(markPolled).toHaveBeenCalledTimes(1)
      expect(offsetFetch).toHaveBeenCalledTimes(2)
      await jest.advanceTimersByTimeAsync(1000)
      await resolving

      expect(offsetFetch).toHaveBeenCalledTimes(3)
      expect(markPolled).toHaveBeenCalledTimes(2)
    }
  )

  test('a STALE_MEMBER_EPOCH re-send does not count as a poll', async () => {
    jest.useFakeTimers()
    const markPolled = jest.fn()
    const offsetFetch = scripted(
      protocolError(STALE_MEMBER_EPOCH),
      protocolError(STALE_MEMBER_EPOCH)
    )
    const { manager } = build({ membershipProtocol: 'consumer', offsetFetch, markPolled })

    const resolving = manager.resolveOffsets()
    await jest.advanceTimersByTimeAsync(1000)
    await resolving

    expect(offsetFetch).toHaveBeenCalledTimes(3)
    expect(markPolled).not.toHaveBeenCalled()
  })

  test.each([
    ['UNSTABLE_OFFSET_COMMIT', 'classic', UNSTABLE_OFFSET_COMMIT, [100, 200, 400, 800, 1000, 1000]],
    ['STALE_MEMBER_EPOCH', 'consumer', STALE_MEMBER_EPOCH, [100, 100, 100, 100, 100, 100]],
  ])('the %s re-send interval (%s)', async (_, protocol, code, expected) => {
    jest.useFakeTimers()
    const sentAt = []
    const offsetFetch = jest.fn(async () => {
      sentAt.push(performance.now())
      throw protocolError(code)
    })
    const { manager } = build({
      membershipProtocol: protocol,
      offsetFetch,
      staleEpochTimeout: 10000,
    })

    const resolving = manager.resolveOffsets().catch((e) => e)
    await jest.advanceTimersByTimeAsync(5000)

    const gaps = sentAt.slice(1, expected.length + 1).map((at, i) => at - sentAt[i])
    expect(gaps).toEqual(expected)
    await jest.advanceTimersByTimeAsync(10000)
    await resolving
  })

  test('a wall-clock jump does not end the wait: the deadline is monotonic', async () => {
    jest.useFakeTimers()
    const offsetFetch = scripted(
      protocolError(UNSTABLE_OFFSET_COMMIT),
      protocolError(UNSTABLE_OFFSET_COMMIT)
    )
    const { manager } = build({ offsetFetch })

    const resolving = manager.resolveOffsets()
    await jest.advanceTimersByTimeAsync(0)
    expect(offsetFetch).toHaveBeenCalledTimes(1)
    jest.setSystemTime(Date.now() + 3600000)
    await jest.advanceTimersByTimeAsync(300)

    await expect(resolving).resolves.toBe(true)
    expect(offsetFetch).toHaveBeenCalledTimes(3)
  })

  test.each([
    ['another retriable error', COORDINATOR_LOAD_IN_PROGRESS],
    ['a stale member epoch under the classic protocol', STALE_MEMBER_EPOCH],
  ])('%s is still thrown at once', async (_, code) => {
    const offsetFetch = scripted(protocolError(code))
    const { manager } = build({ offsetFetch })

    await expect(manager.resolveOffsets()).rejects.toMatchObject({ code })
    expect(offsetFetch).toHaveBeenCalledTimes(1)
  })
})

describe('Consumer > ConsumerGroup > the stable-offset wait wiring', () => {
  /** A real group with no rebalance coordinator attached yet, as before `Runner` builds one. */
  const createGroup = async () => {
    const group = new ConsumerGroup({
      retry: { retries: 0 },
      cluster: {
        committedOffsets: () => ({}),
        findTopicPartitionMetadata: () => [{ partitionId: 0 }],
      },
      groupId: GROUP_ID,
      topics: [TOPIC],
      topicConfigurations: { [TOPIC]: {} },
      logger: newLogger(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      assigners: [],
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      heartbeatInterval: 3000,
      autoCommit: false,
    })
    group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()
    await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: true })
    return group
  }

  test('reads the pending verdict and the poll timer at call time, and tolerates neither', async () => {
    const group = await createGroup()
    const { offsetManager } = group

    expect(offsetManager.isRebalancePending()).toBe(false)
    expect(() => offsetManager.markPolled()).not.toThrow()

    const pollTimer = { poll: jest.fn() }
    group.rebalanceCoordinator = { verdict: null, pollTimer, runner: { stopRequested: false } }
    expect(offsetManager.isRebalancePending()).toBe(false)

    group.rebalanceCoordinator.verdict = { kind: 'lost' }
    expect(offsetManager.isRebalancePending()).toBe(true)

    group.rebalanceCoordinator.verdict = {
      kind: 'revoked',
      reason: 'rebalance-in-progress',
      error: protocolError(REBALANCE_IN_PROGRESS),
    }
    expect(offsetManager.isRebalancePending()).toBe(true)
    expect(offsetManager.isStopping()).toBe(false)
    expect(group.isStopping()).toBe(false)

    offsetManager.markPolled()
    expect(pollTimer.poll).toHaveBeenCalledTimes(1)
  })
})

describe('Consumer > OffsetManager > a fetch round preparation', () => {
  const Generation = require('../../../producer/generation')

  afterEach(() => {
    jest.useRealTimers()
  })

  const preparation = () => {
    const fence = new Generation()
    const state = { refusal: null }
    return { fence, state, guard: () => state.refusal, close: (e) => fence.close(e) }
  }

  test('forwards the guard to OffsetFetch and answers unguarded callers as before', async () => {
    const { manager, coordinator } = build()
    const prepared = preparation()
    await manager.resolveOffsets(prepared)
    expect(coordinator.offsetFetch).toHaveBeenLastCalledWith(
      expect.objectContaining({ guard: prepared.guard })
    )
    await manager.resolveOffsets()
    expect(coordinator.offsetFetch.mock.calls[1][0]).not.toHaveProperty('guard')
  })

  test('a refusal after the reply publishes no resolution and ends the ticket', async () => {
    const prepared = preparation()
    const { manager } = build({
      offsetFetch: jest.fn(async () => {
        prepared.state.refusal = new Error('replaced')
        return { responses: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '9' }] }] }
      }),
    })
    const before = { ...manager.committedOffsets()[TOPIC] }
    await expect(manager.resolveOffsets(prepared)).rejects.toThrow('replaced')
    expect(manager.committedOffsets()[TOPIC]).toEqual(before)
  })

  test('a refusal during the UNSTABLE re-send delay stops before another request and clears the timer', async () => {
    jest.useFakeTimers()
    const prepared = preparation()
    const { manager, coordinator } = build({
      offsetFetch: scripted(protocolError(UNSTABLE_OFFSET_COMMIT)),
    })
    const pending = manager.fetchCommittedOffsets(
      [{ topic: TOPIC, partitions: [{ partition: 0 }] }],
      prepared
    )
    const rejected = pending.catch((error) => error)
    await jest.advanceTimersByTimeAsync(0)
    expect(jest.getTimerCount()).toBe(1)
    prepared.close(new Error('stopping'))
    expect((await rejected).message).toBe('stopping')
    expect(jest.getTimerCount()).toBe(0)
    expect(coordinator.offsetFetch).toHaveBeenCalledTimes(1)
  })

  const held = () => {
    const gate = {}
    gate.promise = new Promise((resolve, reject) => {
      gate.release = resolve
      gate.fail = reject
    })
    return gate
  }

  /** A manager whose coordinator is disconnected and whose rediscovery is held open. */
  const rediscovering = () => {
    const { manager, coordinator } = build()
    const discovery = held()
    const stale = { isConnected: () => false }
    manager.coordinator = stale
    manager.cluster.findGroupCoordinator = jest.fn(() => discovery.promise)
    return { manager, coordinator, discovery, stale }
  }

  test.each([
    ['resolving offsets', (manager, prepared) => manager.resolveOffsets(prepared), 'offsetFetch'],
    [
      'committing a seek',
      (manager, prepared) => manager.seek({ topic: TOPIC, partition: 0, offset: '5' }, prepared),
      'offsetCommit',
    ],
  ])(
    'coordinator discovery while %s is bound to the round and publishes nothing after it',
    async (_, run, request) => {
      const prepared = preparation()
      const { manager, coordinator, discovery, stale } = rediscovering()
      coordinator.offsetCommit = jest.fn(async () => ({}))
      const pending = run(manager, prepared).catch((error) => error)
      await Promise.resolve()
      expect(manager.cluster.findGroupCoordinator).toHaveBeenCalledWith(
        expect.objectContaining({ groupId: GROUP_ID, fence: prepared.fence })
      )
      const stopping = new Error('stopping')
      prepared.state.refusal = stopping
      prepared.close(stopping)
      discovery.release(coordinator)
      expect(await pending).toBe(stopping)
      expect(manager.coordinator).toBe(stale)
      expect(coordinator[request]).not.toHaveBeenCalled()
    }
  )

  test('the metadata refresh a refused commit id starts is bound to the round', async () => {
    const prepared = preparation()
    const { manager, coordinator } = build()
    coordinator.offsetCommit = jest.fn(async () => {
      throw protocolError(100)
    })
    manager.cluster.refreshMetadata = jest.fn(async () => {})
    await expect(
      manager.seek({ topic: TOPIC, partition: 0, offset: '5' }, prepared)
    ).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_ID' })
    expect(manager.cluster.refreshMetadata).toHaveBeenCalledWith({ fence: prepared.fence })
  })

  test('an unguarded commit keeps the unfenced discovery and refresh calls', async () => {
    const { manager, coordinator } = build()
    coordinator.offsetCommit = jest.fn(async () => {
      throw protocolError(100)
    })
    manager.coordinator = { isConnected: () => false }
    manager.cluster.findGroupCoordinator = jest.fn(async () => coordinator)
    manager.cluster.refreshMetadata = jest.fn(async () => {})
    await expect(manager.seek({ topic: TOPIC, partition: 0, offset: '5' })).rejects.toBeDefined()
    expect(manager.cluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: GROUP_ID })
    expect(manager.cluster.refreshMetadata).toHaveBeenCalledWith()
  })

  test('a refused seek never reaches the commit', async () => {
    const prepared = preparation()
    prepared.state.refusal = new Error('replaced')
    const { manager } = build()
    const commit = jest.spyOn(manager, 'sendOffsetCommit')
    await expect(
      manager.seek({ topic: TOPIC, partition: 0, offset: '5' }, prepared)
    ).rejects.toThrow('replaced')
    expect(commit).not.toHaveBeenCalled()
  })

  test('a refusal that arrives with the commit reply throws and leaves the cell as it was', async () => {
    const prepared = preparation()
    const { manager } = build()
    jest.spyOn(manager, 'sendOffsetCommit').mockImplementation(async () => {
      prepared.state.refusal = new Error('replaced')
    })
    const before = { ...manager.committedOffsets()[TOPIC] }
    expect(await manager.seek({ topic: TOPIC, partition: 0, offset: '5' }, prepared)).toBe(false)
    expect(manager.committedOffsets()[TOPIC]).toEqual(before)
  })
})

describe('late commit rejection', () => {
  it('publishes no ownership loss or commit error once the caller run closed', async () => {
    const Generation = require('../../../producer/generation')
    const GroupOwnership = require('../../groupOwnership')
    const { manager, coordinator } = build()
    manager.ownership = new GroupOwnership()
    manager.onCommitError = jest.fn()
    const lose = jest.spyOn(manager.ownership, 'lose')
    const fence = new Generation()
    let rejectCommit
    coordinator.offsetCommit = jest.fn(
      () => new Promise((resolve, reject) => (rejectCommit = reject))
    )
    const pending = manager
      .sendOffsetCommit({ groupGenerationId: 3, topics: [] }, { fence, guard: fence.guard })
      .catch((e) => e)
    for (let n = 0; n < 5; n++) await Promise.resolve()
    expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
    fence.close(new Error('run stopped'))
    rejectCommit(protocolError(25))
    await pending
    expect(lose).not.toHaveBeenCalled()
    expect(manager.onCommitError).not.toHaveBeenCalled()
  })
})
