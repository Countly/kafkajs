const HeartbeatManager = require('../heartbeatManager')
const PollTimer = require('../pollTimer')
const { newLogger } = require('testHelpers')

const build = (overrides = {}) => {
  const heartbeat = jest.fn(async () => {})
  const onError = jest.fn()
  const onPollExpired = jest.fn(async () => {})
  const pollTimer = new PollTimer({ pollDeadline: 1000000 })
  const manager = new HeartbeatManager({
    groupMembership: { heartbeat },
    pollTimer,
    heartbeatInterval: 100,
    logger: newLogger(),
    onError,
    onPollExpired,
    ...overrides,
  })
  return { manager, heartbeat, onError, onPollExpired, pollTimer }
}

describe('Consumer > HeartbeatManager', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  it('sends at the interval and reschedules from the previous tick completing', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    await jest.advanceTimersByTimeAsync(100)
    expect(heartbeat).toHaveBeenCalledTimes(2)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('is idempotent and arms no second timer', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()
    manager.ensureStarted()
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)
    expect(heartbeat).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toEqual(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('never sends twice to catch up after a slow request', async () => {
    let release
    const heartbeat = jest.fn(() => new Promise((resolve) => (release = resolve)))
    const { manager } = build({ groupMembership: { heartbeat } })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)
    await jest.advanceTimersByTimeAsync(1000)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    release()
    await jest.advanceTimersByTimeAsync(100)
    expect(heartbeat).toHaveBeenCalledTimes(2)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('reschedules while disabled and evaluates nothing', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()
    manager.disable()

    await jest.advanceTimersByTimeAsync(500)
    expect(heartbeat).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toEqual(1)

    manager.enable()
    await jest.advanceTimersByTimeAsync(100)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('leaves no timer after stop, and a stopped manager never resurrects itself', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)
    manager.stop()

    expect(jest.getTimerCount()).toEqual(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(heartbeat).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('a tick that reaches a stopped manager sends nothing and re-arms nothing', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()
    manager.stop()

    await manager.tick()

    expect(heartbeat).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('reports an error through onError, and discards one that lands after stop', async () => {
    const boom = new Error('heartbeat failed')
    let release
    const heartbeat = jest.fn(() => new Promise((_, reject) => (release = () => reject(boom))))
    const { manager, onError } = build({ groupMembership: { heartbeat } })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)
    manager.stop()
    release()
    await Promise.resolve()
    await Promise.resolve()

    expect(onError).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('reports an error through onError while running', async () => {
    const boom = new Error('heartbeat failed')
    const heartbeat = jest.fn(async () => {
      throw boom
    })
    const { manager, onError } = build({ groupMembership: { heartbeat } })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)

    expect(onError).toHaveBeenCalledWith(boom)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('does not unref its handle', async () => {
    const { manager } = build()
    const spy = jest.spyOn(global, 'setTimeout')
    manager.ensureStarted()

    const handle = spy.mock.results[0].value
    // A Timeout that has been unref'd reports hasRef() === false; an unref'd heartbeat would let
    // the process exit while this member is still in the group.
    expect(typeof handle.hasRef === 'function' ? handle.hasRef() : true).toBe(true)

    manager.stop()
    spy.mockRestore()
    expect(jest.getTimerCount()).toEqual(0)
  })
})
