const fs = require('fs')
const Broker = require('../../../broker')
const ConnectionPool = require('../../../network/connectionPool')
const Cluster = require('../../../cluster')
const { createErrorFromCode } = require('../../../protocol/error')
const { ShareMembership, SharePollView } = require('../shareMembership')
const { newLogger } = require('testHelpers')
const { createScriptedCluster } = require('../../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../../testHelpers/schedule')
const A = '00112233-4455-6677-8899-aabbccddeeff'
const B = '10112233-4455-6677-8899-aabbccddeeff'

/** Selects the one leave frame (member epoch -1) a test holds on the wire. */
const LEAVE = { api: 'ShareGroupHeartbeat', match: ({ fields }) => fields.memberEpoch === -1 }

/** @returns {Promise<void>} Drains the bounded metadata continuations without a clock sleep. */
const settled = async () => {
  for (let i = 0; i < 12; i++) await Promise.resolve()
}

/**
 * @param {object} [options] Membership overrides.
 * @returns {Promise<object>} A membership over a real Broker on scripted sockets, with the
 *   scripted answer in `response`, the wire schedule in `schedule` and the heartbeat frames the
 *   cluster read in `frames()`.
 */
const fixture = async (options = {}) => {
  const schedule = new Schedule()
  const wire = createScriptedCluster({
    nodes: [{ id: 0, host: 'node0', port: 9092 }],
    profile: 'share',
    schedule,
  })
  const response = {
    errorCode: 0,
    errorMessage: null,
    memberId: null,
    memberEpoch: 1,
    heartbeatIntervalMs: 1000,
    assignment: null,
    throttleTime: 0,
  }
  wire.answer('ShareGroupHeartbeat', () => ({ ...response }))
  const pool = new ConnectionPool({
    host: 'node0',
    port: 9092,
    clientId: 'test-client',
    socketFactory: wire.socketFactory,
    logger: newLogger(),
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    connectionsMaxIdle: 0,
  })
  const broker = new Broker({ connectionPool: pool, logger: newLogger() })
  try {
    await broker.connect()
  } catch (error) {
    await pool.destroy({ drainTimeout: 0 })
    throw error
  }
  const names = new Map([[A, '__proto__']])
  const cluster = {
    findGroupCoordinator: jest.fn(async () => broker),
    topicName: (id) => names.get(id),
    refreshMetadataForTopicIds: jest.fn(async () => ({
      resolved: [],
      unresolved: [B],
    })),
  }
  const onAssignment = jest.fn()
  const onInvalidate = jest.fn()
  const onFatal = jest.fn()
  const member = new ShareMembership({
    cluster,
    groupId: 'group',
    topics: ['__proto__'],
    rackId: 'rack',
    logger: newLogger(),
    retry: { initialRetryTime: 0, factor: 0 },
    onAssignment,
    onInvalidate,
    onFatal,
    ...options,
  })
  return {
    wire,
    schedule,
    pool,
    broker,
    response,
    names,
    cluster,
    member,
    onAssignment,
    onInvalidate,
    onFatal,
    frames: () =>
      wire
        .requests()
        .filter((r) => r.api === 'ShareGroupHeartbeat')
        .map((r) => r.fields),
  }
}

describe('share membership', () => {
  let f
  afterEach(async () => {
    if (f) {
      f.member.stop()
      await f.pool.destroy({ drainTimeout: 0 })
      f.wire.assertClean()
      f = null
    }
    jest.restoreAllMocks()
  })
  test('ordinary membership imports the single shared backoff implementation', () => {
    const source = fs.readFileSync(require.resolve('../../groupProtocol/consumer'), 'utf8')
    expect(source).toContain("require('./heartbeatBackoff')")
    expect(source).not.toContain('class HeartbeatBackoff')
  })
  test('mints fresh authority when an epoch-zero join is accepted', async () => {
    f = await fixture()
    const before = f.member.token
    await f.member.heartbeat()
    expect(f.member.token).not.toBe(before)
    expect(f.onInvalidate).toHaveBeenCalledWith(before, 'joined')
    const accepted = f.member.token
    await f.member.heartbeat()
    expect(f.member.token).toBe(accepted)
  })
  test('leave never looks up a replacement coordinator for a closing membership', async () => {
    f = await fixture()
    await f.member.leave()
    expect(f.cluster.findGroupCoordinator).not.toHaveBeenCalled()
    expect(f.frames()).toEqual([])
  })
  test('a join settlement callback cannot resurrect authority after stopping it', async () => {
    f = await fixture({
      onInvalidate: (token, reason) => {
        if (reason === 'joined') f.member.stop()
      },
    })
    await f.member.heartbeat()
    expect(f.member.closed).toBe(true)
    expect(f.member.memberEpoch).toBe(0)
    expect(f.member.snapshot().assignment).toEqual([])
  })
  test('fences ordinary work before lazy final waits and sends leave only afterwards', async () => {
    f = await fixture()
    await f.member.heartbeat()
    const before = f.member.token
    let release
    const beforeLeave = jest.fn(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    const leaving = f.member.leave({ beforeLeave })
    expect(f.member.token).not.toBe(before)
    expect(beforeLeave).toHaveBeenCalledTimes(1)
    await f.member.heartbeat()
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(0)
    release()
    await leaving
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(1)
  })
  test('joins with topics and rack then sends null deltas and exact leave fields', async () => {
    f = await fixture()
    const id = f.member.memberId
    expect(id).toMatch(/^[A-Za-z0-9_][A-Za-z0-9_-]{21}$/)
    expect(f.member.heartbeatInterval()).toBe(1000)
    await f.member.heartbeat()
    f.response.heartbeatIntervalMs = 2700
    await f.member.heartbeat()
    expect(f.member.heartbeatInterval()).toBe(2700)
    await f.member.leave()
    expect(f.frames()).toEqual([
      {
        groupId: 'group',
        memberId: id,
        memberEpoch: 0,
        rackId: 'rack',
        subscribedTopicNames: ['__proto__'],
      },
      {
        groupId: 'group',
        memberId: id,
        memberEpoch: 1,
        rackId: null,
        subscribedTopicNames: null,
      },
      {
        groupId: 'group',
        memberId: id,
        memberEpoch: -1,
        rackId: null,
        subscribedTopicNames: null,
      },
    ])
    expect(f.member.snapshot().state).toBe('UNSUBSCRIBED')
  })
  test('repeats every epoch-zero payload and sends changed names once', async () => {
    f = await fixture({ rackId: '' })
    f.response.memberEpoch = 0
    await f.member.heartbeat()
    await f.member.heartbeat()
    expect(f.frames().map((x) => [x.memberEpoch, x.rackId, x.subscribedTopicNames])).toEqual([
      [0, null, ['__proto__']],
      [0, null, ['__proto__']],
    ])
    f.response.memberEpoch = 1
    await f.member.heartbeat()
    f.member.updateSubscription(['second'])
    await f.member.heartbeat()
    await f.member.heartbeat()
    expect(f.frames()[3].subscribedTopicNames).toEqual(['second'])
    expect(f.frames()[4].subscribedTopicNames).toBeNull()
  })
  test.each([14, 15, 16])(
    're-finds coordinator after code %s and repeats both fields',
    async (code) => {
      f = await fixture()
      await f.member.heartbeat()
      const token = f.member.token
      f.response.errorCode = code
      await expect(f.member.heartbeat()).rejects.toMatchObject({ code })
      expect(f.member.token).toBe(token)
      f.response.errorCode = 0
      await f.member.heartbeat()
      expect(f.cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
      expect(f.frames()[2]).toMatchObject({
        rackId: 'rack',
        subscribedTopicNames: ['__proto__'],
        memberEpoch: 1,
      })
    }
  )
  test('resets delta memory after an unanswered transport attempt', async () => {
    f = await fixture()
    await f.member.heartbeat()
    const original = f.broker.shareGroupHeartbeat.bind(f.broker)
    jest.spyOn(f.broker, 'shareGroupHeartbeat').mockRejectedValueOnce(new Error('answer lost'))
    await expect(f.member.heartbeat()).rejects.toThrow('answer lost')
    f.broker.shareGroupHeartbeat.mockImplementation(original)
    await f.member.heartbeat()
    expect(f.frames()[1]).toMatchObject({
      rackId: 'rack',
      subscribedTopicNames: ['__proto__'],
    })
  })
  test.each([25, 110])(
    'fences code %s synchronously and rejoins with the same member ID',
    async (code) => {
      f = await fixture()
      await f.member.heartbeat()
      const old = f.member.snapshot()
      f.response.errorCode = code
      await expect(f.member.heartbeat()).rejects.toMatchObject({ code })
      expect(f.member.token).not.toBe(old.token)
      expect(f.onInvalidate).toHaveBeenCalledWith(old.token, 'fenced')
      expect(f.member.snapshot()).toMatchObject({
        memberId: old.memberId,
        memberEpoch: 0,
        state: 'JOINING',
      })
      f.response.errorCode = 0
      await f.member.heartbeat()
      expect(f.frames()[2]).toMatchObject({
        memberId: old.memberId,
        memberEpoch: 0,
        rackId: 'rack',
        subscribedTopicNames: ['__proto__'],
      })
    }
  )
  test.each([35, 30, 29, 81, 42])(
    'makes code %s terminal with no later heartbeat',
    async (code) => {
      f = await fixture()
      f.response.errorCode = code
      await expect(f.member.heartbeat()).rejects.toMatchObject({
        retriable: false,
      })
      expect(f.member.state).toBe('FATAL')
      expect(f.onFatal).toHaveBeenCalledTimes(1)
      await f.member.heartbeat()
      expect(f.frames()).toHaveLength(1)
    }
  )
  test('names the refused API when the broker answers code 35 at runtime', async () => {
    f = await fixture()
    f.response.errorCode = 35
    await expect(f.member.heartbeat()).rejects.toMatchObject({
      reason: 'refused-by-broker',
      apiKey: 76,
      apiName: 'ShareGroupHeartbeat',
    })
    expect(f.onFatal.mock.calls[0][0]).toMatchObject({
      reason: 'refused-by-broker',
      apiKey: 76,
      apiName: 'ShareGroupHeartbeat',
    })
  })
  test.each([
    ['before the join is accepted', false],
    ['for an established member', true],
  ])('makes code 69 fatal %s and sends nothing afterwards', async (phase, joined) => {
    f = await fixture()
    if (joined) await f.member.heartbeat()
    const sent = f.frames().length
    const before = f.member.token
    f.response.errorCode = 69
    await expect(f.member.heartbeat()).rejects.toMatchObject({ code: 69, retriable: false })
    expect(f.member.state).toBe('FATAL')
    expect(f.onFatal).toHaveBeenCalledTimes(1)
    expect(f.onFatal.mock.calls[0][0]).toMatchObject({ code: 69, retriable: false })
    expect(f.onInvalidate).toHaveBeenCalledWith(before, 'fatal')
    await f.member.heartbeat()
    expect(f.frames()).toHaveLength(sent + 1)
  })
  test('completes a leave answered with code 69 without a failure, a fatal verdict or a retry', async () => {
    const logger = { error: jest.fn() }
    f = await fixture({ logger })
    await f.member.heartbeat()
    f.response.errorCode = 69
    await f.member.leave()
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(1)
    expect(logger.error).not.toHaveBeenCalled()
    expect(f.onFatal).not.toHaveBeenCalled()
    expect(f.member.state).toBe('UNSUBSCRIBED')
    await f.member.heartbeat()
    expect(f.frames()).toHaveLength(2)
  })
  test('completes a poll-expiry leave answered with code 69 and rejoins with the same member ID', async () => {
    f = await fixture()
    await f.member.heartbeat()
    const id = f.member.memberId
    f.response.errorCode = 69
    await f.member.leave({ stale: true })
    expect(f.member.state).toBe('STALE')
    expect(f.onFatal).not.toHaveBeenCalled()
    f.response.errorCode = 0
    f.member.rejoinAfterPoll(new SharePollView({ maxPollInterval: 1000 }), [])
    await f.member.heartbeat()
    expect(f.frames().map((x) => [x.memberId, x.memberEpoch])).toEqual([
      [id, 0],
      [id, -1],
      [id, 0],
    ])
    expect(f.member.state).toBe('STABLE')
  })
  test('ignores code 69 once the membership is unsubscribed and never rejoins', async () => {
    f = await fixture()
    await f.member.heartbeat()
    await f.member.leave()
    const { token, state } = f.member.snapshot()
    expect(state).toBe('UNSUBSCRIBED')
    expect(f.member.onMembershipError(createErrorFromCode(69), token)).toBeNull()
    await f.member.heartbeat()
    expect(f.member.snapshot()).toMatchObject({ state: 'UNSUBSCRIBED', token })
    expect(f.onFatal).not.toHaveBeenCalled()
    expect(f.frames()).toHaveLength(2)
  })
  test('ignores a code 69 verdict that arrives while the leave is pending and still settles the leave', async () => {
    f = await fixture()
    await f.member.heartbeat()
    const gate = f.schedule.hold(LEAVE)
    const leaving = f.member.leave()
    await gate.arrived()
    const { token, state } = f.member.snapshot()
    expect(state).toBe('LEAVING')
    expect(f.member.onMembershipError(createErrorFromCode(69), token)).toBeNull()
    expect(f.member.state).toBe('LEAVING')
    gate.release({ ...f.response })
    await leaving
    expect(f.member.state).toBe('UNSUBSCRIBED')
    expect(f.onFatal).not.toHaveBeenCalled()
    expect(f.frames()).toHaveLength(2)
  })
  test('discards a late code 69 answer from a superseded membership', async () => {
    f = await fixture()
    await f.member.heartbeat()
    const gate = f.schedule.hold({ api: 'ShareGroupHeartbeat' })
    const late = f.member.heartbeat()
    await gate.arrived()
    expect(f.member.onMembershipError(createErrorFromCode(25), f.member.token)).toMatchObject({
      code: 25,
    })
    const replacement = f.member.snapshot()
    gate.release({ ...f.response, errorCode: 69 })
    await expect(late).resolves.toBeUndefined()
    expect(f.member.snapshot()).toMatchObject({
      state: 'JOINING',
      memberEpoch: 0,
      token: replacement.token,
    })
    expect(f.onFatal).not.toHaveBeenCalled()
    await f.member.heartbeat()
    expect(f.member.state).toBe('STABLE')
  })
  test('a late code 69 answer to a retired run leave cannot touch its replacement run', async () => {
    f = await fixture()
    await f.member.heartbeat()
    const onFatal = jest.fn()
    const replacement = new ShareMembership({
      cluster: f.cluster,
      groupId: 'group',
      topics: ['__proto__'],
      logger: newLogger(),
      retry: { initialRetryTime: 0, factor: 0 },
      onFatal,
    })
    await replacement.heartbeat()
    const { token } = replacement.snapshot()
    const gate = f.schedule.hold(LEAVE)
    const leaving = f.member.leave()
    await gate.arrived()
    gate.release({ ...f.response, errorCode: 69 })
    await leaving
    expect(f.onFatal).not.toHaveBeenCalled()
    expect(onFatal).not.toHaveBeenCalled()
    expect(replacement.snapshot()).toMatchObject({ state: 'STABLE', token })
    await replacement.heartbeat()
    const own = f.frames().filter((x) => x.memberId === replacement.memberId)
    expect(own.map((x) => x.memberEpoch)).toEqual([0, 1])
    replacement.stop()
  })
  test('does not act on the reply to a leave whose closing guard refused in flight', async () => {
    const logger = { error: jest.fn() }
    f = await fixture({ logger })
    await f.member.heartbeat()
    const gate = f.schedule.hold(LEAVE)
    let expired = false
    const leaving = f.member.leave({
      guard: () => (expired ? new Error('closing episode expired') : null),
    })
    await gate.arrived()
    expired = true
    gate.release({ ...f.response, errorCode: 16 })
    await leaving
    expect(logger.error).not.toHaveBeenCalled()
    expect(f.member.state).toBe('UNSUBSCRIBED')
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(1)
  })
  test('sends no leave frame when the closing guard refuses before the lazy finals', async () => {
    f = await fixture()
    await f.member.heartbeat()
    const beforeLeave = jest.fn(async () => {})
    await f.member.leave({ beforeLeave, guard: () => new Error('closing episode expired') })
    expect(beforeLeave).not.toHaveBeenCalled()
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(0)
    expect(f.member.state).toBe('UNSUBSCRIBED')
  })
  test('sends no leave frame when the closing guard refuses while the lazy finals run', async () => {
    f = await fixture()
    await f.member.heartbeat()
    let expired = false
    const beforeLeave = jest.fn(async () => {
      expired = true
    })
    await f.member.leave({
      beforeLeave,
      guard: () => (expired ? new Error('closing episode expired') : null),
    })
    expect(beforeLeave).toHaveBeenCalledTimes(1)
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(0)
    expect(f.member.state).toBe('UNSUBSCRIBED')
  })
  test('runs a full lifecycle with the default inert observers', async () => {
    f = await fixture({ onAssignment: undefined, onInvalidate: undefined, onFatal: undefined })
    f.response.assignment = { topicPartitions: [{ topicId: A, partitions: [1] }] }
    await f.member.heartbeat()
    expect(f.member.snapshot().assignment).toEqual([
      { topicId: A, topic: '__proto__', partitions: [1] },
    ])
    f.response.errorCode = 69
    await expect(f.member.heartbeat()).rejects.toMatchObject({ code: 69 })
    expect(f.member.state).toBe('FATAL')
    expect(f.member.snapshot().assignment).toEqual([])
  })
  test('a leave refused inside a fence notification does not latch the later leave', async () => {
    const logger = { error: jest.fn() }
    f = await fixture({
      logger,
      onInvalidate: (token, reason) => {
        if (reason === 'fenced') f.member.leave()
      },
    })
    await f.member.heartbeat()
    f.response.errorCode = 25
    await expect(f.member.heartbeat()).rejects.toMatchObject({ code: 25 })
    expect(logger.error).toHaveBeenCalledTimes(1)
    expect(logger.error).toHaveBeenCalledWith('Share membership observer failed', {
      error: expect.stringContaining('FENCED -> PREPARE_LEAVING'),
    })
    f.response.errorCode = 0
    await f.member.heartbeat()
    await f.member.leave()
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(1)
  })
  test('applies known IDs immediately, preserves null, deduplicates equal and removes empty', async () => {
    f = await fixture()
    const assigned = {
      topicPartitions: [
        { topicId: A, partitions: [2, 0] },
        { topicId: B, partitions: [1] },
      ],
    }
    f.response.assignment = assigned
    await f.member.heartbeat()
    expect(f.member.snapshot().assignment).toEqual([
      { topicId: A, topic: '__proto__', partitions: [0, 2] },
    ])
    expect(f.member.snapshot().unresolved).toEqual([B])
    expect(f.cluster.refreshMetadataForTopicIds).toHaveBeenCalledTimes(1)
    f.response.assignment = null
    await f.member.heartbeat()
    expect(f.onAssignment).toHaveBeenCalledTimes(1)
    f.response.assignment = assigned
    await f.member.heartbeat()
    expect(f.onAssignment).toHaveBeenCalledTimes(1)
    const snapshot = f.member.snapshot()
    snapshot.assignment[0].partitions.push(99)
    expect(f.member.snapshot().assignment[0].partitions).toEqual([0, 2])
    f.response.assignment = { topicPartitions: [] }
    await f.member.heartbeat()
    expect(f.member.snapshot().assignment).toEqual([])
    expect(f.onAssignment).toHaveBeenCalledTimes(2)
  })
  test('ignores lower epochs including their interval and assignment', async () => {
    f = await fixture()
    f.response.memberEpoch = 4
    await f.member.heartbeat()
    f.response.memberEpoch = 3
    f.response.heartbeatIntervalMs = 9999
    f.response.assignment = {
      topicPartitions: [{ topicId: A, partitions: [7] }],
    }
    await f.member.heartbeat()
    expect(f.member.snapshot()).toMatchObject({
      memberEpoch: 4,
      assignment: [],
    })
    expect(f.member.heartbeatInterval()).toBe(1000)
  })
  test('keeps retrying unresolved IDs under its own elapsed backoff without blocking heartbeats', async () => {
    f = await fixture({
      retry: { initialRetryTime: 100, multiplier: 2, factor: 0 },
    })
    let now = 0
    jest.spyOn(performance, 'now').mockImplementation(() => now)
    f.response.assignment = {
      topicPartitions: [{ topicId: B, partitions: [1] }],
    }
    await f.member.heartbeat()
    await settled()
    f.response.assignment = null
    for (let i = 0; i < 5; i++) {
      now += 1000
      await f.member.heartbeat()
      await settled()
    }
    expect(f.cluster.refreshMetadataForTopicIds).toHaveBeenCalledTimes(6)
    expect(f.frames()).toHaveLength(6)
    f.names.set(B, 'late')
    now += 1000
    await f.member.heartbeat()
    await settled()
    expect(f.member.snapshot().assignment).toEqual([{ topicId: B, topic: 'late', partitions: [1] }])
    expect(f.member.snapshot().unresolved).toEqual([])
  })
  test('permits only one metadata flight and respects its next retry deadline', async () => {
    f = await fixture({ retry: { initialRetryTime: 100, multiplier: 2, factor: 0 } })
    let now = 0
    jest.spyOn(performance, 'now').mockImplementation(() => now)
    let release
    f.cluster.refreshMetadataForTopicIds.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    f.response.assignment = { topicPartitions: [{ topicId: B, partitions: [1] }] }
    await f.member.heartbeat()
    await f.member.heartbeat()
    f.response.assignment = null
    expect(f.cluster.refreshMetadataForTopicIds).toHaveBeenCalledTimes(1)
    release({ resolved: [], unresolved: [B] })
    await settled()
    now = 99
    await f.member.heartbeat()
    expect(f.cluster.refreshMetadataForTopicIds).toHaveBeenCalledTimes(1)
    now = 100
    await f.member.heartbeat()
    await settled()
    expect(f.cluster.refreshMetadataForTopicIds).toHaveBeenCalledTimes(2)
  })
  test('retires a held old UUID resolution while the same name acquires a new UUID', async () => {
    f = await fixture()
    let release
    let captured
    f.cluster.refreshMetadataForTopicIds.mockImplementationOnce((ids, { isCurrent }) => {
      captured = isCurrent
      return new Promise((resolve) => {
        release = resolve
      })
    })
    f.response.assignment = {
      topicPartitions: [{ topicId: B, partitions: [1] }],
    }
    await f.member.heartbeat()
    expect(captured()).toBe(true)
    f.response.assignment = {
      topicPartitions: [{ topicId: A, partitions: [1] }],
    }
    await f.member.heartbeat()
    expect(captured()).toBe(false)
    f.names.set(B, '__proto__')
    release({ resolved: [B], unresolved: [] })
    await settled()
    expect(f.member.snapshot().assignment.map((x) => x.topicId)).toEqual([A])
    expect(f.member.snapshot().unresolved).toEqual([])
  })
  test('coalesces concurrent heartbeats and rejects a captured write after stop', async () => {
    f = await fixture()
    const original = f.broker.shareGroupHeartbeat.bind(f.broker)
    let release
    let entered
    const arrival = new Promise((resolve) => {
      entered = resolve
    })
    jest.spyOn(f.broker, 'shareGroupHeartbeat').mockImplementationOnce(async (payload, options) => {
      entered()
      await new Promise((resolve) => {
        release = resolve
      })
      return original(payload, options)
    })
    const one = f.member.heartbeat()
    expect(f.member.heartbeat()).toBe(one)
    await arrival
    f.member.stop()
    release()
    await one
    expect(f.frames()).toEqual([])
  })
  test('discards old fetch membership errors even when an epoch number repeats', async () => {
    f = await fixture()
    await f.member.heartbeat()
    const old = f.member.token
    f.member.onMembershipError(createErrorFromCode(25), old)
    const replacement = f.member.token
    expect(f.member.onMembershipError(createErrorFromCode(81), old)).toBeNull()
    expect(f.member.state).toBe('JOINING')
    expect(f.member.token).toBe(replacement)
    expect(f.onFatal).not.toHaveBeenCalled()
  })
  test('late coordinator lookup cannot send or install after terminal stop', async () => {
    f = await fixture()
    let release
    f.cluster.findGroupCoordinator.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    const operation = f.member.heartbeat()
    f.member.stop()
    release(f.broker)
    await operation
    expect(f.frames()).toEqual([])
    expect(f.member.coordinator).toBeNull()
  })
  test('latches leave before an assignment observer re-enters it', async () => {
    f = await fixture({
      onAssignment: () => {
        if (f && f.member.snapshot().assignment.length === 0) f.member.leave()
      },
    })
    f.response.assignment = { topicPartitions: [{ topicId: A, partitions: [1] }] }
    await f.member.heartbeat()
    await f.member.leave()
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(1)
  })
  test('leave is once-only even when assignment observers throw', async () => {
    f = await fixture({
      onAssignment: () => {
        throw new Error('observer')
      },
    })
    f.response.assignment = {
      topicPartitions: [{ topicId: A, partitions: [1] }],
    }
    await f.member.heartbeat()
    const first = f.member.leave()
    expect(f.member.leave()).toBe(first)
    await first
    expect(f.frames().filter((x) => x.memberEpoch === -1)).toHaveLength(1)
  })
  test('records a protocol refusal of the captured leave without retrying it', async () => {
    const logger = { error: jest.fn() }
    f = await fixture({ logger })
    await f.member.heartbeat()
    f.response.errorCode = 16
    await f.member.leave()
    expect(f.frames().filter((frame) => frame.memberEpoch === -1)).toHaveLength(1)
    expect(logger.error).toHaveBeenCalledWith('Share leave failed', {
      error: createErrorFromCode(16).message,
    })
    expect(f.member.state).toBe('UNSUBSCRIBED')
  })
})

describe('nested topic metadata currentness', () => {
  /** @param {object} broker Injected metadata peer. @returns {object} The real Cluster helpers over it. */
  const clusterFor = (broker) => {
    const cluster = Object.create(Cluster.prototype)
    cluster.logger = newLogger()
    cluster.metadataCache = {
      updateTopicsById: jest.fn(),
      topicName: () => null,
    }
    cluster.brokerPool = { withBroker: (work) => work({ broker }) }
    return cluster
  }
  test.each([11, 13])('never installs a retired answer on Metadata version %s', async (version) => {
    let release
    let current = true
    const broker = {
      negotiatedVersion: () => version,
      metadata: jest.fn(
        () =>
          new Promise((resolve) => {
            release = resolve
          })
      ),
    }
    const cluster = clusterFor(broker)
    const operation = cluster.refreshMetadataForTopicIds([B], {
      isCurrent: () => current,
    })
    current = false
    release({ topicMetadata: [] })
    await expect(operation).resolves.toEqual({
      resolved: [],
      unresolved: [],
    })
    expect(cluster.metadataCache.updateTopicsById).not.toHaveBeenCalled()
    expect(broker.metadata).toHaveBeenCalledTimes(1)
  })
  test('starts no per-ID followup when a retired bulk request fails', async () => {
    let reject
    let current = true
    const broker = {
      metadata: jest.fn(
        () =>
          new Promise((resolve, fail) => {
            reject = fail
          })
      ),
    }
    const cluster = clusterFor(broker)
    const operation = cluster.fetchMetadataByTopicIds(broker, [A, B], {
      isCurrent: () => current,
    })
    current = false
    reject(createErrorFromCode(3))
    await expect(operation).resolves.toBe('failed')
    expect(broker.metadata).toHaveBeenCalledTimes(1)
    expect(cluster.metadataCache.updateTopicsById).not.toHaveBeenCalled()
  })
  test('refuses a held per-ID install and every later followup after target retirement', async () => {
    let release
    let current = true
    const broker = {
      metadata: jest
        .fn()
        .mockRejectedValueOnce(createErrorFromCode(3))
        .mockImplementationOnce(
          () =>
            new Promise((resolve) => {
              release = resolve
            })
        ),
    }
    const cluster = clusterFor(broker)
    const operation = cluster.fetchMetadataByTopicIds(broker, [A, B], { isCurrent: () => current })
    await settled()
    expect(broker.metadata).toHaveBeenCalledTimes(2)
    current = false
    release({ topicMetadata: [] })
    await expect(operation).resolves.toBe('failed')
    expect(cluster.metadataCache.updateTopicsById).not.toHaveBeenCalled()
    expect(broker.metadata).toHaveBeenCalledTimes(2)
  })
  test('installs current bulk metadata and preserves ordinary unguarded fallback', async () => {
    const broker = {
      negotiatedVersion: () => 13,
      metadata: jest.fn(async () => ({ topicMetadata: [] })),
    }
    const cluster = clusterFor(broker)
    await cluster.refreshMetadataForTopicIds([A], { isCurrent: () => true })
    expect(cluster.metadataCache.updateTopicsById).toHaveBeenCalledTimes(1)
    broker.negotiatedVersion = () => 11
    cluster.refreshMetadata = jest.fn(async () => {})
    await cluster.refreshMetadataForTopicIds([A])
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })
})
