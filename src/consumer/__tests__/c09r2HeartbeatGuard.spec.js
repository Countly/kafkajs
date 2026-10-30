const ClassicGroupProtocol = require('../groupProtocol/classic')
const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const HeartbeatManager = require('../heartbeatManager')
const PollTimer = require('../pollTimer')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')

/** @returns {{promise: Promise<any>, resolve: Function}} a manually settled promise */
const gate = () => {
  const held = {}
  held.promise = new Promise((resolve) => (held.resolve = resolve))
  return held
}

const flush = () => new Promise((resolve) => setImmediate(resolve))

/**
 * A classic member whose cached coordinator is disconnected, so a heartbeat looks it up again.
 * @returns {object} the protocol, the held lookup and the two brokers
 */
const rig = async () => {
  const lookup = gate()
  const current = { heartbeat: jest.fn(async () => {}), isConnected: () => true }
  const old = { heartbeat: jest.fn(async () => {}), isConnected: () => true }
  const cluster = {
    findGroupCoordinator: jest.fn().mockResolvedValueOnce(old).mockReturnValue(lookup.promise),
  }
  const protocol = new ClassicGroupProtocol({
    cluster,
    groupId: 'g',
    groupInstanceId: null,
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    assigners: [],
    instrumentationEmitter: new InstrumentationEventEmitter(),
    logger: newLogger(),
  })
  await protocol.connect()
  Object.assign(protocol, { memberId: 'm', generationId: 1 })
  old.isConnected = () => false
  return { protocol, lookup, current, old }
}

describe('a classic heartbeat awaiting its coordinator lookup', () => {
  it('sends nothing once the caller refuses after the lookup', async () => {
    const { protocol, lookup, current, old } = await rig()
    let refusal = null
    const beating = protocol.heartbeat({ interval: 0, guard: () => refusal }).catch((e) => e)
    await flush()
    refusal = new Error('the run was retired')
    lookup.resolve(current)
    expect(await beating).toBe(refusal)
    expect(current.heartbeat).not.toHaveBeenCalled()
    expect(old.heartbeat).not.toHaveBeenCalled()
  })

  it('sends one heartbeat when the guard stays clear', async () => {
    const { protocol, lookup, current } = await rig()
    const beating = protocol.heartbeat({ interval: 0, guard: () => null })
    await flush()
    lookup.resolve(current)
    await beating
    expect(current.heartbeat).toHaveBeenCalledTimes(1)
  })

  it('the guard reaches the transport request', async () => {
    const { protocol, lookup, current } = await rig()
    const guard = () => null
    const beating = protocol.heartbeat({ interval: 0, guard })
    await flush()
    lookup.resolve(current)
    await beating
    expect(current.heartbeat.mock.calls[0][0].guard).toBe(guard)
  })
})

describe('the heartbeat guard is forwarded from the runner', () => {
  it('hands the group a guard that refuses once the run was retired', async () => {
    const heartbeat = jest.fn(async () => {})
    const group = {
      groupId: 'g',
      memberId: 'm',
      kind: 'classic',
      attach: () => {},
      getNodeIds: () => [],
      heartbeat,
      heartbeatInterval: () => 3000,
      onStopRequested: jest.fn(),
      rebalanceProtocol: () => 'eager',
    }
    const runner = new Runner({
      logger: newLogger(),
      consumerGroup: group,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      concurrency: 1,
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
      eachBatch: async () => {},
      onCrash: () => {},
    })
    await runner.heartbeat()
    const { guard } = heartbeat.mock.calls[0][0]
    expect(guard()).toBeNull()
    runner.retire(new Error('closed'))
    expect(guard()).toBe(runner.retirement)
    runner.heartbeatManager.shutdown()
    runner.pollTimer.stop()
  })

  it('the consumer group forwards the guard to its membership', async () => {
    const group = Object.create(ConsumerGroup.prototype)
    const heartbeat = jest.fn(async () => {})
    group.groupMembership = { heartbeat }
    const guard = () => null
    await group.heartbeat({ interval: 5, guard })
    expect(heartbeat).toHaveBeenCalledWith({ interval: 5, guard })
  })
})

describe('the timer chain hands its owner guard to every heartbeat', () => {
  it('passes the guard it was built with', async () => {
    jest.useFakeTimers()
    try {
      const heartbeat = jest.fn(async () => {})
      const guard = () => null
      const manager = new HeartbeatManager({
        groupMembership: { heartbeat },
        pollTimer: new PollTimer({ pollDeadline: 300000 }),
        heartbeatInterval: 100,
        logger: newLogger(),
        onError: jest.fn(),
        onPollExpired: jest.fn(),
        guard,
      })
      manager.ensureStarted()
      await jest.advanceTimersByTimeAsync(100)
      expect(heartbeat).toHaveBeenCalledWith({ interval: 100, guard })
      manager.shutdown()
    } finally {
      jest.useRealTimers()
    }
  })
})
