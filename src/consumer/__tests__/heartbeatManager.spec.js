const HeartbeatManager = require('../heartbeatManager')
const PollTimer = require('../pollTimer')
const ClassicGroupProtocol = require('../groupProtocol/classic')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createErrorFromCode } = require('../../protocol/error')
const { events } = require('../instrumentationEvents')
const { newLogger } = require('testHelpers')

const ILLEGAL_GENERATION = 22

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

  it('re-arms instead of sending when a tick lands while a heartbeat is in flight', async () => {
    let release
    const heartbeat = jest.fn(() => new Promise((resolve) => (release = resolve)))
    const { manager } = build({ groupMembership: { heartbeat } })
    manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(100)
    expect(jest.getTimerCount()).toEqual(0)

    await manager.tick()

    expect(heartbeat).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toEqual(1)

    release()
    manager.stop()
    await jest.advanceTimersByTimeAsync(0)
    expect(jest.getTimerCount()).toEqual(0)
  })

  it.each([
    ['a message', 'handler broke', 'heartbeat failed', 'handler broke', 'heartbeat failed'],
    ['no message', '', '', undefined, undefined],
  ])(
    'logs and keeps the chain when the error handler throws with %s',
    async (_, handlerMessage, heartbeatMessage, loggedError, loggedCause) => {
      const heartbeatError = new Error(heartbeatMessage)
      const handlerError = new Error(handlerMessage)
      const logged = jest.fn()
      const logger = { namespace: () => ({ error: logged, debug: jest.fn() }) }
      const heartbeat = jest.fn(async () => {
        throw heartbeatError
      })
      const onError = jest.fn(() => {
        throw handlerError
      })
      const { manager } = build({ groupMembership: { heartbeat }, logger, onError })
      manager.ensureStarted()

      await jest.advanceTimersByTimeAsync(100)

      expect(logged).toHaveBeenCalledWith('Heartbeat error handler failed', {
        error: loggedError ?? handlerError,
        cause: loggedCause ?? heartbeatError,
      })
      expect(jest.getTimerCount()).toEqual(1)
      manager.stop()
    }
  )

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

describe('Consumer > HeartbeatManager > the JoinGroup window', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  const buildProtocol = ({ joinGroup }) => {
    const coordinator = { joinGroup, negotiatedVersion: () => 5 }
    return new ClassicGroupProtocol({
      cluster: { findGroupCoordinator: async () => coordinator },
      groupId: 'window-group',
      groupInstanceId: null,
      sessionTimeout: 30000,
      rebalanceTimeout: 300000,
      assigners: [],
      mode: 'eager',
      rackId: '',
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
    })
  }

  const joinGroupResponse = {
    generationId: 1,
    leaderId: 'm',
    memberId: 'm',
    members: [],
    groupProtocol: 'x',
  }

  it('is disabled for the JoinGroup request and enabled again on the response', async () => {
    const seen = []
    const { manager } = build()
    const protocol = buildProtocol({
      joinGroup: async () => {
        seen.push(manager.disabled)
        return joinGroupResponse
      },
    })
    protocol.attach({ rebalanceCoordinator: {}, heartbeatManager: manager })

    expect(manager.disabled).toBe(false)
    await protocol.join({ topicsSubscribed: ['t'] })

    expect(seen).toEqual([true])
    expect(manager.disabled).toBe(false)
  })

  it('re-enables when the JoinGroup throws', async () => {
    // `ClassicGroupProtocol.join()` has no retrier of its own - the retrier stays on
    // `ConsumerGroup#joinAndSync` - so the `finally` is the whole mechanism, and re-enabling
    // between the retrier's own attempts follows from it rather than being a separate path.
    const { manager } = build()
    const protocol = buildProtocol({
      joinGroup: async () => {
        throw new Error('coordinator unavailable')
      },
    })
    protocol.attach({ rebalanceCoordinator: {}, heartbeatManager: manager })

    await expect(protocol.join({ topicsSubscribed: ['t'] })).rejects.toThrow(
      'coordinator unavailable'
    )
    expect(manager.disabled).toBe(false)
  })

  it('joins without a manager attached', async () => {
    const protocol = buildProtocol({ joinGroup: async () => joinGroupResponse })

    await protocol.join({ topicsSubscribed: ['t'] })

    expect(protocol.generationId).toEqual(1)
  })

  it('sends no heartbeat inside the window, so a stale generation cannot be raised mid-join', async () => {
    const heartbeat = jest.fn(async () => {
      throw createErrorFromCode(ILLEGAL_GENERATION)
    })
    const { manager, onError } = build({ groupMembership: { heartbeat } })
    const protocol = buildProtocol({
      joinGroup: async () => {
        await jest.advanceTimersByTimeAsync(500)
        return joinGroupResponse
      },
    })
    protocol.attach({ rebalanceCoordinator: {}, heartbeatManager: manager })
    manager.ensureStarted()

    await protocol.join({ topicsSubscribed: ['t'] })

    expect(heartbeat).not.toHaveBeenCalled()
    expect(onError).not.toHaveBeenCalled()

    // Outside the window the very same error IS classified, so the case pins the window and not
    // a manager that never reports.
    await jest.advanceTimersByTimeAsync(100)
    expect(onError).toHaveBeenCalledTimes(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('discards a heartbeat result that lands inside the window, and classifies it outside', async () => {
    const boom = createErrorFromCode(ILLEGAL_GENERATION)
    let reject
    let sends = 0
    const heartbeat = jest.fn(() =>
      ++sends === 1 ? new Promise((_, rej) => (reject = () => rej(boom))) : Promise.reject(boom)
    )
    const { manager, onError } = build({ groupMembership: { heartbeat } })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    // The window opens while that heartbeat is still awaiting, so its answer belongs to the
    // generation this member's own JoinGroup is about to replace.
    manager.disable()
    reject()
    await jest.advanceTimersByTimeAsync(0)

    expect(onError).not.toHaveBeenCalled()
    // The chain survives the discard: only the classification is suppressed.
    expect(jest.getTimerCount()).toEqual(1)

    manager.enable()
    await jest.advanceTimersByTimeAsync(100)

    expect(onError).toHaveBeenCalledTimes(1)
    expect(onError).toHaveBeenCalledWith(boom)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('heartbeats during the leader assign window and during SyncGroup', async () => {
    const { manager, heartbeat } = build()
    manager.ensureStarted()
    manager.disable()
    await jest.advanceTimersByTimeAsync(300)
    expect(heartbeat).not.toHaveBeenCalled()

    // The bracket closes on the JoinGroup RESPONSE, so everything after it - refreshMetadata,
    // assign, generatePartitionsPerSubscribedTopic and the SyncGroup request - heartbeats.
    manager.enable()
    await jest.advanceTimersByTimeAsync(300)
    expect(heartbeat.mock.calls.length).toBeGreaterThanOrEqual(2)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })
})

describe('Consumer > HeartbeatManager > poll expiry', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  it('reports expiry once and evaluates nothing further that tick', async () => {
    const pollTimer = new PollTimer({ pollDeadline: 50 })
    const { manager, heartbeat, onPollExpired } = build({ pollTimer })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)

    expect(onPollExpired).toHaveBeenCalledTimes(1)
    expect(onPollExpired.mock.calls[0][0].elapsedMs).toBeGreaterThan(50)
    expect(heartbeat).not.toHaveBeenCalled()
    // The sequence the coordinator runs stops this manager itself, so no tick was rescheduled.
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('leaves no timer behind when the sequence stops the manager, as the coordinator does', async () => {
    const pollTimer = new PollTimer({ pollDeadline: 50 })
    const { manager, onPollExpired } = build({ pollTimer })
    onPollExpired.mockImplementation(async () => manager.stop())
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)

    expect(onPollExpired).toHaveBeenCalledTimes(1)
    expect(manager.isRunning()).toBe(false)
    expect(jest.getTimerCount()).toEqual(0)

    // And the round that recovers re-arms it through the same idempotent entry.
    pollTimer.reset()
    manager.ensureStarted()
    expect(jest.getTimerCount()).toEqual(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('does not report expiry while a rebalance round is in flight, and heartbeats instead', async () => {
    const pollTimer = new PollTimer({ pollDeadline: 50 })
    let rebalancing = true
    const { manager, heartbeat, onPollExpired } = build({
      pollTimer,
      isRebalancing: () => rebalancing,
    })
    manager.ensureStarted()

    // The JoinGroup of a slow round blocks for longer than the poll deadline, which is the same
    // number as the rebalance timeout - that time is the coordinator's, not the handler's.
    await jest.advanceTimersByTimeAsync(300)

    expect(onPollExpired).not.toHaveBeenCalled()
    expect(heartbeat.mock.calls.length).toBeGreaterThan(1)

    // The same deadline, once the round is over, is a real overrun.
    rebalancing = false
    await jest.advanceTimersByTimeAsync(100)

    expect(onPollExpired).toHaveBeenCalledTimes(1)
    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('does not report expiry while disabled', async () => {
    const pollTimer = new PollTimer({ pollDeadline: 50 })
    const { manager, onPollExpired } = build({ pollTimer })
    manager.ensureStarted()
    manager.disable()

    await jest.advanceTimersByTimeAsync(500)

    expect(onPollExpired).not.toHaveBeenCalled()
    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('keeps the chain alive when the sequence throws, instead of losing the session', async () => {
    const pollTimer = new PollTimer({ pollDeadline: 50 })
    const onPollExpired = jest.fn(async () => {
      throw new Error('sequence failed')
    })
    const { manager } = build({ pollTimer, onPollExpired })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)
    expect(onPollExpired).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toEqual(1)

    await jest.advanceTimersByTimeAsync(100)
    expect(onPollExpired).toHaveBeenCalledTimes(2)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('leaves a chain the sequence itself re-armed alone', async () => {
    const pollTimer = new PollTimer({ pollDeadline: 50 })
    const { manager } = build({ pollTimer })
    // The whole recovery collapsed into the callback: stop, reset, re-arm - which is what the
    // coordinator's sequence plus the round that follows it do between two ticks.
    manager.onPollExpired = async () => {
      manager.stop()
      pollTimer.reset()
      manager.ensureStarted()
    }
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)

    expect(manager.isRunning()).toBe(true)
    expect(jest.getTimerCount()).toEqual(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('never leaves a fired timer in place when the sequence returns without stopping it', async () => {
    const pollTimer = new PollTimer({ pollDeadline: 50 })
    // The sequence has a path that returns early - while the runner is already stopping - without
    // stopping this manager. A `handle` left pointing at a fired timeout would report an armed
    // chain that does not exist, and `ensureStarted()` would then refuse to arm a real one.
    const { manager, onPollExpired } = build({ pollTimer })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)

    expect(onPollExpired).toHaveBeenCalledTimes(1)
    expect(manager.isRunning()).toBe(false)
    expect(jest.getTimerCount()).toEqual(0)

    pollTimer.reset()
    manager.ensureStarted()
    expect(manager.isRunning()).toBe(true)
    expect(jest.getTimerCount()).toEqual(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('does not reschedule a superseded chain when the sequence throws', async () => {
    const pollTimer = new PollTimer({ pollDeadline: 50 })
    const { manager } = build({ pollTimer })
    // What the coordinator's sequence does before it fails: stop, then let the recovery round
    // re-arm. The failing tick belongs to the chain that is already gone.
    manager.onPollExpired = async () => {
      manager.stop()
      pollTimer.reset()
      manager.ensureStarted()
      throw new Error()
    }
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)

    expect(jest.getTimerCount()).toEqual(1)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })

  it('arms one chain when a stop and a re-arm land inside an in-flight heartbeat', async () => {
    let release
    let sends = 0
    const heartbeat = jest.fn(() =>
      ++sends === 1 ? new Promise((resolve) => (release = resolve)) : Promise.resolve()
    )
    const { manager } = build({ groupMembership: { heartbeat } })
    manager.ensureStarted()

    await jest.advanceTimersByTimeAsync(100)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    // What the poll-expiry sequence does: stop while a heartbeat is still in flight, then re-arm
    // from the round that recovers. The settling tick must not schedule a second chain on top.
    manager.stop()
    manager.ensureStarted()
    expect(jest.getTimerCount()).toEqual(1)

    release()
    await Promise.resolve()
    await Promise.resolve()
    expect(jest.getTimerCount()).toEqual(1)

    await jest.advanceTimersByTimeAsync(300)
    expect(heartbeat).toHaveBeenCalledTimes(4)

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })
})

describe('Consumer > HeartbeatManager > the HEARTBEAT event', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  it('is emitted for a timer-driven send, not only for the handler callback', async () => {
    // The emit lives beside the request inside the group protocol, so the fixture is a real
    // protocol with a stubbed coordinator: nothing else can prove the timer's send is emitted.
    const heartbeat = jest.fn(async () => {})
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const emitted = []
    instrumentationEmitter.addListener(events.HEARTBEAT, ({ payload }) => emitted.push(payload))

    const protocol = new ClassicGroupProtocol({
      cluster: {
        findGroupCoordinator: async () => ({
          joinGroup: async () => ({
            generationId: 7,
            leaderId: 'm',
            memberId: 'm',
            members: [],
            groupProtocol: 'x',
          }),
          heartbeat,
          negotiatedVersion: () => 5,
          isConnected: () => true,
        }),
      },
      groupId: 'heartbeat-group',
      groupInstanceId: null,
      sessionTimeout: 30000,
      rebalanceTimeout: 300000,
      assigners: [],
      mode: 'eager',
      rackId: '',
      instrumentationEmitter,
      logger: newLogger(),
    })

    const { manager } = build({ groupMembership: protocol })
    protocol.attach({ rebalanceCoordinator: {}, heartbeatManager: manager })
    await protocol.join({ topicsSubscribed: ['t'] })

    manager.ensureStarted()
    await jest.advanceTimersByTimeAsync(100)

    expect(heartbeat).toHaveBeenCalledTimes(1)
    expect(emitted).toEqual([
      {
        groupId: 'heartbeat-group',
        memberId: 'm',
        groupGenerationId: 7,
        groupInstanceId: null,
      },
    ])

    manager.stop()
    expect(jest.getTimerCount()).toEqual(0)
  })
})
