const HeartbeatManager = require('../../heartbeatManager')
const HeartbeatBackoff = require('../../groupProtocol/heartbeatBackoff')
const { ShareHandlerCall } = require('../shareHandlerCall')
const { ShareMembership, SharePollView } = require('../shareMembership')
const { newLogger } = require('testHelpers')

/** @returns {Promise<void>} Bounded microtask drain. */
const settled = async () => {
  for (let i = 0; i < 12; i++) await Promise.resolve()
}

/**
 * @param {object} [options] HeartbeatManager overrides.
 * @returns {object} A membership, poll view and the real HeartbeatManager over fake collaborators;
 *   only timers and the broker's answers are scripted.
 */
const fixture = (options = {}) => {
  const response = {
    errorCode: 0,
    memberEpoch: 1,
    heartbeatIntervalMs: 1000,
    assignment: null,
  }
  const broker = {
    shareGroupHeartbeat: jest.fn(async () => ({ ...response })),
  }
  const cluster = {
    findGroupCoordinator: jest.fn(async () => broker),
    topicName: () => null,
    refreshMetadataForTopicIds: jest.fn(async () => ({
      resolved: [],
      unresolved: [],
    })),
  }
  const member = new ShareMembership({
    cluster,
    groupId: 'group',
    topics: ['topic'],
    logger: newLogger(),
    retry: { initialRetryTime: 100, multiplier: 2, factor: 0 },
  })
  const poll = new SharePollView({ maxPollInterval: 2000 })
  const onPollExpired = jest.fn(async () => {})
  const manager = new HeartbeatManager({
    groupMembership: member,
    pollTimer: poll,
    heartbeatInterval: 1000,
    logger: newLogger(),
    onError: jest.fn(),
    onPollExpired,
    ...options,
  })
  return { member, broker, response, poll, manager, onPollExpired }
}

describe('share heartbeat scheduling', () => {
  let f
  beforeEach(() => {
    jest.useFakeTimers({ now: 0 })
  })
  afterEach(() => {
    if (f) {
      f.manager.shutdown()
      f.poll.stop()
      f.member.stop()
    }
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })
  test('uses one completion-rearmed chain and queues no tick behind a slow answer', async () => {
    f = fixture()
    let release
    f.broker.shareGroupHeartbeat.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    f.manager.ensureStarted()
    f.manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(1000)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(5000)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    release({ ...f.response, heartbeatIntervalMs: 1500 })
    await settled()
    await jest.advanceTimersByTimeAsync(1499)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(2)
  })
  test('starts the chain at the initial second and then follows the broker interval', async () => {
    f = fixture()
    f.response.heartbeatIntervalMs = 250
    expect(f.member.heartbeatInterval()).toBe(1000)
    f.manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(999)
    expect(f.broker.shareGroupHeartbeat).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(1)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    expect(f.member.heartbeatInterval()).toBe(250)
    await jest.advanceTimersByTimeAsync(249)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(1)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(2)
  })
  test('grows retry delay and resets it on success using the shared backoff', async () => {
    f = fixture()
    f.broker.shareGroupHeartbeat
      .mockRejectedValueOnce(new Error('first'))
      .mockRejectedValueOnce(new Error('second'))
    await expect(f.member.heartbeat()).rejects.toThrow('first')
    expect(f.member.heartbeatInterval()).toBe(100)
    await jest.advanceTimersByTimeAsync(100)
    await expect(f.member.heartbeat()).rejects.toThrow('second')
    expect(f.member.heartbeatInterval()).toBe(200)
    await jest.advanceTimersByTimeAsync(200)
    await f.member.heartbeat()
    expect(f.member.heartbeatInterval()).toBe(1000)
    const backoff = new HeartbeatBackoff({
      initialRetryTime: 100,
      maxRetryTime: 9000,
      factor: 0,
    })
    for (let i = 0; i < 10; i++) backoff.onFailedAttempt(0)
    expect(backoff.remaining(0)).toBe(1000)
  })
  test('a failed heartbeat sends nothing before its backoff deadline and exactly one at it', async () => {
    f = fixture()
    f.broker.shareGroupHeartbeat.mockRejectedValueOnce(new Error('first'))
    await expect(f.member.heartbeat()).rejects.toThrow('first')
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    await f.member.heartbeat()
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    await jest.advanceTimersByTimeAsync(99)
    await f.member.heartbeat()
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    expect(f.member.heartbeatInterval()).toBe(1)
    await jest.advanceTimersByTimeAsync(1)
    await f.member.heartbeat()
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(2)
    expect(f.member.heartbeatInterval()).toBe(1000)
  })
  test('terminal shutdown cannot arm through either entrypoint or a held completion', async () => {
    f = fixture()
    let release
    f.broker.shareGroupHeartbeat.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    f.manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(1000)
    f.manager.shutdown()
    f.member.stop()
    release(f.response)
    await settled()
    f.manager.ensureStarted()
    f.manager.requestTick()
    await jest.advanceTimersByTimeAsync(10000)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })
  test('an idle empty assignment never expires over three poll intervals', async () => {
    f = fixture()
    f.manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(6000)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(6)
    expect(f.onPollExpired).not.toHaveBeenCalled()
    expect(f.poll.elapsed()).toBe(0)
  })
  test('tracks handler identities without a separate call object and keeps independent start times', async () => {
    f = fixture()
    const frame = { records: [], authority: {} }
    const old = new ShareHandlerCall(frame)
    const current = new ShareHandlerCall(frame)
    expect(f.poll.begin(old)).toBe(old)
    await jest.advanceTimersByTimeAsync(1000)
    expect(f.poll.begin(current)).toBe(current)
    await jest.advanceTimersByTimeAsync(1001)
    expect(f.poll.captureExpired()).toEqual([old])
    f.poll.finish(old)
    f.poll.finish(old)
    expect(f.poll.elapsed()).toBe(1001)
    expect(f.poll.drained([current])).toBe(false)
    expect(f.poll.expired()).toBe(false)
    f.poll.finish(current)
    expect(f.poll.elapsed()).toBe(0)
    expect(f.poll.drained([current])).toBe(true)
  })
  test('progress at another node cannot reset a held call deadline', async () => {
    f = fixture()
    const held = f.poll.begin({ nodeId: 1 })
    await jest.advanceTimersByTimeAsync(1000)
    for (let i = 0; i < 4; i++) {
      const fast = f.poll.begin({ nodeId: 2 })
      await jest.advanceTimersByTimeAsync(300)
      f.poll.finish(fast)
    }
    f.poll.begin({ nodeId: 2 })
    expect(f.poll.elapsed()).toBe(2200)
    expect(f.poll.expired()).toBe(true)
    expect(f.poll.captureExpired()).toEqual([held])
    f.poll.finish(held)
    expect(f.poll.expired()).toBe(false)
  })
  test('captures simultaneous overruns and waits for each identity before rejoin', async () => {
    f = fixture()
    await f.member.heartbeat()
    const one = f.poll.begin({ nodeId: 1 })
    const two = f.poll.begin({ nodeId: 2 })
    let captured
    f.manager.onPollExpired = async () => {
      captured = f.poll.captureExpired()
      f.manager.stop()
      await f.member.leave({ stale: true })
    }
    f.manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(3000)
    expect(captured).toEqual([one, two])
    expect(
      f.broker.shareGroupHeartbeat.mock.calls.filter(([p]) => p.memberEpoch === -1)
    ).toHaveLength(1)
    expect(f.member.state).toBe('STALE')
    f.poll.finish(one)
    f.member.rejoinAfterPoll(f.poll, captured)
    expect(f.member.state).toBe('STALE')
    expect(f.poll.drained(captured)).toBe(false)
    await jest.advanceTimersByTimeAsync(3000)
    expect(f.member.state).toBe('STALE')
    f.poll.finish(two)
    expect(f.poll.drained(captured)).toBe(true)
    f.member.rejoinAfterPoll(f.poll, captured)
    f.manager.requestTick()
    await jest.advanceTimersByTimeAsync(1)
    expect(f.member.state).toBe('STABLE')
  })
  test('stop during a stale leave cannot be undone by the leave completion', async () => {
    f = fixture()
    await f.member.heartbeat()
    let release
    f.broker.shareGroupHeartbeat.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    const leaving = f.member.leave({ stale: true })
    await settled()
    f.manager.shutdown()
    f.poll.stop()
    f.member.stop()
    release(f.response)
    await leaving
    f.member.rejoinAfterPoll(f.poll, [])
    await f.member.heartbeat()
    expect(f.member.closed).toBe(true)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(2)
  })
  test('a stale leave answered with code 69 still lets the poll recovery rejoin', async () => {
    f = fixture()
    await f.member.heartbeat()
    f.broker.shareGroupHeartbeat.mockResolvedValueOnce({ ...f.response, errorCode: 69 })
    await f.member.leave({ stale: true })
    expect(f.member.state).toBe('STALE')
    f.member.rejoinAfterPoll(f.poll, [])
    f.manager.requestTick()
    await jest.advanceTimersByTimeAsync(1)
    expect(f.member.state).toBe('STABLE')
    expect(f.broker.shareGroupHeartbeat.mock.calls.map(([p]) => p.memberEpoch)).toEqual([0, -1, 0])
  })
  test('old returns cannot reset another run and exact boundary is not expired', async () => {
    f = fixture()
    const old = f.poll.begin({ nodeId: 1 })
    await jest.advanceTimersByTimeAsync(2000)
    expect(f.poll.expired()).toBe(false)
    f.poll.stop()
    const next = new SharePollView({ maxPollInterval: 2000 })
    next.begin({ nodeId: 1 })
    await jest.advanceTimersByTimeAsync(2001)
    next.finish(old)
    expect(next.expired()).toBe(true)
    expect(next.calls.size).toBe(1)
    next.stop()
  })
  test('a stopped poll view admits no call, reports no age and keeps its deadline', () => {
    f = fixture()
    const call = f.poll.begin({ nodeId: 1 })
    f.poll.stop()
    expect(f.poll.begin({ nodeId: 1 })).toBeNull()
    expect(f.poll.elapsed()).toBe(0)
    expect(f.poll.expired()).toBe(false)
    expect(f.poll.drained([call])).toBe(true)
    expect(f.poll.deadline()).toBe(2000)
  })
  test('throwing error observers cannot strand the chain', async () => {
    f = fixture({
      onError: () => {
        throw new Error('observer')
      },
    })
    f.broker.shareGroupHeartbeat.mockRejectedValueOnce(new Error('transport'))
    f.manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(1100)
    expect(f.broker.shareGroupHeartbeat).toHaveBeenCalledTimes(2)
  })
})
