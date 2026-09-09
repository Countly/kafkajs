const HeartbeatManager = require('../heartbeatManager')
const PollTimer = require('../pollTimer')
const { newLogger } = require('testHelpers')

/**
 * A membership double shaped like the two real implementations: a single-flight `heartbeat` and
 * the `heartbeatInterval()` accessor makes the manager read per schedule.
 */
const build = ({ serverInterval = 500, heartbeat, ...overrides } = {}) => {
  const heartbeatFn = heartbeat || jest.fn(async () => {})
  const groupMembership = {
    heartbeat: heartbeatFn,
    heartbeatInterval: jest.fn(() => serverInterval),
  }
  const pollTimer = new PollTimer({ pollDeadline: 1000000 })
  const manager = new HeartbeatManager({
    groupMembership,
    pollTimer,
    heartbeatInterval: 3000,
    logger: newLogger(),
    onError: jest.fn(),
    onPollExpired: jest.fn(async () => {}),
    ...overrides,
  })

  return { manager, groupMembership, heartbeat: heartbeatFn, pollTimer }
}

const fields = (manager) => ({
  armed: manager.handle !== null,
  chainId: manager.chainId,
  inFlight: manager.inFlight,
  disabled: manager.disabled,
  tickRequested: manager.tickRequested,
})

describe('Consumer > HeartbeatManager > requestTick', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  // The join arms a chain that was never armed, and it is the SAME chain ensureStarted() would
  // have armed later, so nothing is doubled.
  it('arms an unarmed chain and sends one heartbeat before ensureStarted() runs', async () => {
    const { manager, heartbeat } = build()

    expect(fields(manager)).toEqual({
      armed: false,
      chainId: 0,
      inFlight: false,
      disabled: false,
      tickRequested: false,
    })

    manager.requestTick()

    expect(fields(manager)).toEqual({
      armed: true,
      chainId: 1,
      inFlight: false,
      disabled: false,
      tickRequested: false,
    })
    expect(jest.getTimerCount()).toEqual(1)

    await jest.advanceTimersByTimeAsync(0)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    manager.ensureStarted()
    manager.ensureStarted()
    expect(jest.getTimerCount()).toEqual(1)
    expect(manager.chainId).toEqual(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  // An ack while no tick is in flight supersedes the armed chain instead of adding one.
  it('supersedes an armed chain rather than doubling it', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()
    expect(fields(manager).chainId).toEqual(1)

    manager.requestTick()
    expect(fields(manager)).toEqual({
      armed: true,
      chainId: 2,
      inFlight: false,
      disabled: false,
      tickRequested: false,
    })
    expect(jest.getTimerCount()).toEqual(1)

    await jest.advanceTimersByTimeAsync(0)
    expect(heartbeat).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toEqual(1)

    manager.stop()
  })

  // An ack DURING an in-flight heartbeat is consumed by that tick's finally, through armNow(), so
  // the tail reschedule's guard fails and only one chain survives.
  it('defers to the in-flight tick and leaves exactly one chain', async () => {
    // One resolver per call: the deferred tick sends a SECOND heartbeat, and releasing them in
    // turn is what makes the state after each one observable.
    const releases = []
    const heartbeat = jest.fn(() => new Promise((resolve) => releases.push(resolve)))
    const { manager } = build({ heartbeat })

    manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(500)
    expect(manager.inFlight).toBe(true)

    for (let i = 0; i < 10; i++) {
      manager.requestTick()
    }

    expect(fields(manager)).toEqual({
      armed: true,
      chainId: 1,
      inFlight: true,
      disabled: false,
      tickRequested: true,
    })

    releases[0]()
    await jest.advanceTimersByTimeAsync(0)

    // Ten requests were collapsed into one re-arm: one bumped chain, one extra heartbeat, and
    // that heartbeat is still in flight, so there is no timer to count yet.
    expect(manager.tickRequested).toBe(false)
    expect(manager.chainId).toEqual(2)
    expect(heartbeat).toHaveBeenCalledTimes(2)
    expect(manager.inFlight).toBe(true)

    releases[1]()
    await jest.advanceTimersByTimeAsync(0)

    // The re-armed chain rescheduled exactly once, under the same id.
    expect(jest.getTimerCount()).toEqual(1)
    expect(manager.chainId).toEqual(2)
    expect(heartbeat).toHaveBeenCalledTimes(2)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  // stop() BETWEEN the requestTick and the tick settling arms nothing.
  it('does not resurrect a stopped manager when the deferred tick settles', async () => {
    let release
    const heartbeat = jest.fn(() => new Promise((resolve) => (release = resolve)))
    const { manager } = build({ heartbeat })

    manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(500)
    manager.requestTick()
    expect(manager.tickRequested).toBe(true)

    manager.stop()
    expect(manager.handle).toBeNull()

    release()
    await jest.advanceTimersByTimeAsync(0)

    expect(manager.tickRequested).toBe(false)
    expect(manager.handle).toBeNull()
    expect(jest.getTimerCount()).toEqual(0)
    expect(heartbeat).toHaveBeenCalledTimes(1)
  })

  // The manager-side half of "stop during the join": a superseded or stopped chain's callback
  // returns without sending and without rescheduling.
  it('rejects a superseded chain: requestTick, stop, ensureStarted leaves one live chain', async () => {
    const { manager, heartbeat } = build()

    manager.requestTick()
    manager.stop()
    manager.ensureStarted()

    expect(jest.getTimerCount()).toEqual(1)
    await jest.advanceTimersByTimeAsync(500)

    expect(heartbeat).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toEqual(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  // The poll-expiry recovery round re-arms through requestTick on a stopped manager.
  it('arms a fresh chain on a stopped manager, which is how the recovery round rejoins', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()
    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)

    manager.requestTick()
    expect(manager.chainId).toEqual(2)

    await jest.advanceTimersByTimeAsync(0)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    manager.stop()
  })

  // The other half of the pair above: `Runner#stop()` latches the manager, and a reconciliation
  // still settling afterwards must not bring the chain back.
  it('arms nothing after shutdown(), however it is asked', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()
    manager.shutdown()

    expect(manager.stopped).toBe(true)
    expect(jest.getTimerCount()).toEqual(0)

    manager.requestTick()
    manager.ensureStarted()
    manager.requestTick()

    expect(fields(manager)).toEqual({
      armed: false,
      chainId: 1,
      inFlight: false,
      disabled: false,
      tickRequested: false,
    })
    expect(jest.getTimerCount()).toEqual(0)

    await jest.advanceTimersByTimeAsync(5000)
    expect(heartbeat).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('shuts down a manager that was never started, and stays shut', () => {
    const { manager } = build()
    manager.shutdown()
    manager.requestTick()

    expect(jest.getTimerCount()).toEqual(0)
  })

  it('does nothing while disabled', () => {
    const { manager } = build()
    manager.disable()
    manager.requestTick()

    expect(fields(manager)).toEqual({
      armed: false,
      chainId: 0,
      inFlight: false,
      disabled: true,
      tickRequested: false,
    })
    expect(jest.getTimerCount()).toEqual(0)
  })
})

describe('Consumer > HeartbeatManager > the membership owns the interval', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  // A server interval of 500 with a configured 3000 gives a 500 ms cadence, and the {interval}
  // the tick passes is the server's too - not the constructor's.
  it('schedules and heartbeats at the membership interval, not the configured one', async () => {
    const { manager, heartbeat, groupMembership } = build({ serverInterval: 500 })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(500)
    expect(heartbeat).toHaveBeenCalledTimes(1)
    expect(heartbeat).toHaveBeenCalledWith({ interval: 500 })

    await jest.advanceTimersByTimeAsync(500)
    expect(heartbeat).toHaveBeenCalledTimes(2)
    expect(groupMembership.heartbeatInterval.mock.calls.length).toBeGreaterThanOrEqual(3)

    manager.stop()
  })

  it('follows a membership whose interval changes between schedules', async () => {
    let serverInterval = 500
    // Flipped from inside the heartbeat, because the reschedule that must see the new value runs
    // in the same tick: a flip made between two advances would arrive one timer too late, after a
    // delay that schedule() had already fixed.
    const heartbeat = jest.fn(async () => {
      serverInterval = 5000
    })
    const { manager } = build({ heartbeat })
    manager.groupMembership.heartbeatInterval = () => serverInterval

    manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(500)
    expect(heartbeat).toHaveBeenCalledTimes(1)
    expect(heartbeat).toHaveBeenNthCalledWith(1, { interval: 500 })

    await jest.advanceTimersByTimeAsync(500)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    await jest.advanceTimersByTimeAsync(4500)
    expect(heartbeat).toHaveBeenCalledTimes(2)
    expect(heartbeat).toHaveBeenLastCalledWith({ interval: 5000 })

    manager.stop()
  })

  // Classic stays byte-identical, including for a membership double that has no accessor.
  it('falls back to the configured interval when the membership offers none', async () => {
    const heartbeat = jest.fn(async () => {})
    const manager = new HeartbeatManager({
      groupMembership: { heartbeat },
      pollTimer: new PollTimer({ pollDeadline: 1000000 }),
      heartbeatInterval: 3000,
      logger: newLogger(),
      onError: jest.fn(),
      onPollExpired: jest.fn(async () => {}),
    })

    manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(2999)
    expect(heartbeat).not.toHaveBeenCalled()

    await jest.advanceTimersByTimeAsync(1)
    expect(heartbeat).toHaveBeenCalledWith({ interval: 3000 })

    manager.stop()
  })

  it('falls back when the membership answers with something that is not a usable delay', async () => {
    const heartbeat = jest.fn(async () => {})
    const { manager } = build({ heartbeat })
    manager.groupMembership.heartbeatInterval = () => NaN

    manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(3000)

    expect(heartbeat).toHaveBeenCalledWith({ interval: 3000 })
    manager.stop()
  })
})
