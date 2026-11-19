const Cluster = require('../../cluster')
const createConsumer = require('../index')
const Runner = require('../runner')
const OffsetOperations = require('../offsetManager/offsetOperations')
const handlerContext = require('../handlerContext')
const { newLogger } = require('testHelpers')
const { CloseRig, GROUP, TOPIC, gate, pause, until } = require('./fixtures/closeRig')

jest.setTimeout(20000)

/** @returns {Promise<void>} resolves after the pending microtasks and one macrotask turn */
const flush = () => new Promise((resolve) => setImmediate(resolve))

/**
 * A cluster whose connect and disconnect log into one ordered list and track whether it is
 * connected, with just what a consumer without a broker needs.
 * @returns {{ cluster: object, log: string[], state: { connected: boolean } }}
 */
const scriptedCluster = () => {
  const log = []
  const state = { connected: false }
  const operations = new OffsetOperations()
  const cluster = {
    connect: jest.fn(async () => {
      log.push('connect')
      state.connected = true
    }),
    disconnect: jest.fn(async (options) => {
      log.push(options ? `disconnect(${options.drainTimeout})` : 'disconnect')
      state.connected = false
    }),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    resetTargetTopics: jest.fn(async () => true),
    removeBroker: jest.fn(),
    isConnected: () => state.connected,
    addMultipleTargetTopics: jest.fn(async () => {}),
    useConsumerMetadataScope: jest.fn(),
    releaseConsumerMetadataScope: jest.fn(),
    onTopicRecreated: jest.fn(() => jest.fn()),
    offsetOperations: () => operations,
    retry: {},
  }
  return { cluster, log, state }
}

const build = (cluster) =>
  createConsumer({ cluster, groupId: 'g', logger: newLogger(), partitionAssigners: [] })

describe('a close that is still running when a replacement connects', () => {
  afterEach(() => jest.restoreAllMocks())

  /**
   * A running consumer whose first runner stop is held, so the close waits inside its stop.
   * @returns {Promise<{ consumer: object, cluster: object, log: string[], state: object, stopped: object }>}
   */
  const withHeldStop = async () => {
    const stopped = gate()
    jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
    jest
      .spyOn(Runner.prototype, 'stop')
      // Like the real stop, it unwinds once a forced close retires its scope.
      .mockImplementationOnce((_options, operation) =>
        operation
          .wait(() => stopped.promise)
          .catch((error) => {
            if (!operation.guard()) throw error
          })
      )
      .mockResolvedValue()
    const { cluster, log, state } = scriptedCluster()
    const consumer = build(cluster)
    await consumer.connect()
    await consumer.subscribe({ topic: 't' })
    await consumer.run({ eachMessage: async () => {} })
    return { consumer, cluster, log, state, stopped }
  }

  it.each([
    ['a plain close', undefined, 'disconnect'],
    ['a forced close', { timeout: 30 }, 'disconnect(0)'],
  ])('%s never disconnects the connection that replaced it', async (_name, options, teardown) => {
    const { consumer, cluster, log, state, stopped } = await withHeldStop()
    const closing = consumer.disconnect(options)
    await flush()
    const replacing = consumer.connect()
    if (options) {
      await Promise.all([closing, replacing])
      expect(log).toEqual(['connect', teardown, 'connect'])
      stopped.resolve()
      await flush()
    } else {
      let replaced = false
      replacing.then(() => (replaced = true))
      await pause(60)
      expect(replaced).toBe(false)
      expect(log).toEqual(['connect'])
      stopped.resolve()
      await Promise.all([closing, replacing])
    }

    expect(cluster.connect).toHaveBeenCalledTimes(2)
    expect(log).toEqual(['connect', teardown, 'connect'])
    expect(state.connected).toBe(true)
    await consumer.disconnect()
    expect(state.connected).toBe(false)
  })

  it('a replacement run waits for the close and then starts a run of its own', async () => {
    const { consumer, log, state, stopped } = await withHeldStop()
    const closing = consumer.disconnect()
    await flush()
    const running = consumer.run({ eachMessage: async () => {} })
    await pause(30)
    expect(Runner.prototype.start).toHaveBeenCalledTimes(1)

    stopped.resolve()
    await Promise.all([closing, running])

    expect(Runner.prototype.start).toHaveBeenCalledTimes(2)
    expect(log).toEqual(['connect', 'disconnect'])
    expect(state.connected).toBe(false)
    await consumer.disconnect()
  })

  it('a failed close does not fail the replacement, which still connects', async () => {
    const { consumer, state, stopped } = await withHeldStop()
    const closing = consumer.disconnect().catch((error) => error)
    await flush()
    const replacing = consumer.connect()
    stopped.reject(new Error('stop failed'))

    expect((await closing).message).toBe('stop failed')
    await replacing
    expect(state.connected).toBe(true)
    await consumer.disconnect()
  })

  it('a disconnect asked after the replacement connect still ends it', async () => {
    const { consumer, cluster, state, stopped } = await withHeldStop()
    const first = consumer.disconnect()
    await flush()
    const replacing = consumer.connect().catch((error) => error)
    const second = consumer.disconnect()
    stopped.resolve()
    await Promise.all([first, second])

    expect((await replacing).type).toBe('CONSUMER_STOPPING')
    expect(cluster.connect).toHaveBeenCalledTimes(1)
    expect(state.connected).toBe(false)
  })

  it('with no close in progress, a connect does not wait (control)', async () => {
    const { cluster, log } = scriptedCluster()
    const consumer = build(cluster)
    await consumer.connect()
    await consumer.connect()
    expect(log).toEqual(['connect', 'connect'])
    await consumer.disconnect()
    expect(log).toEqual(['connect', 'connect', 'disconnect'])
  })

  it('a handler of the run being closed does not wait for its own close', async () => {
    const stopped = gate()
    jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
    jest.spyOn(Runner.prototype, 'stop').mockImplementation(() => stopped.promise)
    const { cluster } = scriptedCluster()
    const consumer = build(cluster)
    await consumer.connect()
    await consumer.subscribe({ topic: 't' })
    await consumer.run({ eachMessage: async () => {} })
    const closing = consumer.disconnect()
    await flush()
    const inHandler = handlerContext.run(
      { topic: 't', partition: 0, assignmentEpoch: 1, acquireClock: 1, ownershipKey: 'k' },
      () => consumer.connect()
    )
    const outcome = await Promise.race([inHandler, pause(200).then(() => 'waited')])
    expect(outcome).toBeUndefined()
    stopped.resolve()
    await closing
    await inHandler
  })
})

describe.each([true, false])(
  'a real consumer over a scripted cluster, with enforcement %p',
  (enforce) => {
    /** @type {CloseRig} */
    let rig
    afterEach(async () => {
      await rig.dispose()
    })

    test('a close held at its leave does not disconnect the connection that replaced it', async () => {
      rig = await CloseRig.open({ enforce })
      const lookups = rig.lookup()
      const delivered = []
      const { consumer } = await rig.started({ delivered: 3 })
      const cluster = lookups.cluster
      expect(cluster.groupEntries.has(GROUP)).toBe(true)
      const leave = rig.hold('LeaveGroup')

      const closing = consumer.disconnect()
      await until(() => leave.hits === 1, 'the held leave')
      let replaced = false
      const replacing = (async () => {
        await consumer.connect()
        replaced = true
        await consumer.run({
          autoCommit: false,
          eachMessage: async ({ message }) => {
            delivered.push(message.value.toString())
          },
        })
      })()
      await pause(80)
      expect(replaced).toBe(false)
      expect(cluster.isConnected()).toBe(true)

      leave.release()
      await Promise.all([closing, replacing])

      expect(replaced).toBe(true)
      await until(() => delivered.length >= 1, 'the replacement run delivers')
      expect(cluster.isConnected()).toBe(true)
      expect(cluster.groupEntries.has(GROUP)).toBe(true)
    })

    test('a forced close held at its leave resolves at its deadline, then the replacement connects and runs', async () => {
      rig = await CloseRig.open({ enforce })
      const lookups = rig.lookup()
      const delivered = []
      const { consumer } = await rig.started({ delivered: 3 })
      const cluster = lookups.cluster
      const leave = rig.hold('LeaveGroup')

      const closing = consumer.disconnect({ timeout: 200 })
      await until(() => leave.hits === 1, 'the held leave')
      const replacing = (async () => {
        await consumer.connect()
        await consumer.run({
          autoCommit: false,
          eachMessage: async ({ message }) => {
            delivered.push(message.value.toString())
          },
        })
      })()
      await Promise.all([closing, replacing])
      leave.release()
      await until(() => delivered.length >= 1, 'the replacement run delivers')
      await pause(80)

      expect(cluster.isConnected()).toBe(true)
      expect(cluster.groupEntries.has(GROUP)).toBe(true)
    })

    test('a close with no replacement still disconnects the cluster and clears its group entry (control)', async () => {
      rig = await CloseRig.open({ enforce })
      const lookups = rig.lookup()
      const { consumer } = await rig.started({ delivered: 3 })
      const cluster = lookups.cluster
      expect(cluster.isConnected()).toBe(true)

      await consumer.disconnect()

      expect(cluster.isConnected()).toBe(false)
      expect(cluster.groupEntries.has(GROUP)).toBe(false)
    })
  }
)

describe.each(['classic', 'consumer'])(
  'a call queued behind a close, then a second close, over the %s protocol',
  (protocol) => {
    /** @type {CloseRig} */
    let rig
    afterEach(async () => {
      jest.restoreAllMocks()
      await rig.dispose()
    })

    const natural = { kind: 'a natural', secondOptions: undefined }
    const zeroTimeout = { kind: 'a zero-timeout', secondOptions: { timeout: 0 } }
    it.each([
      { call: 'run', ...natural, arrangedFirst: false },
      { call: 'run', ...zeroTimeout, arrangedFirst: false },
      { call: 'connect', ...natural, arrangedFirst: false },
      { call: 'connect', ...zeroTimeout, arrangedFirst: false },
      { call: 'run', ...natural, arrangedFirst: true },
      { call: 'connect', ...natural, arrangedFirst: true },
    ])(
      'a queued $call rejects and leaves no membership or socket after $kind second close (second close arranged first: $arrangedFirst)',
      async ({ call, secondOptions, arrangedFirst }) => {
        rig = await CloseRig.open({ protocol })
        const consumer = rig.consumer()
        const events = []
        for (const name of ['CONNECT', 'DISCONNECT', 'GROUP_JOIN'])
          consumer.on(consumer.events[name], () => events.push(name))
        await consumer.connect()
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true })

        const teardown = gate()
        let teardowns = 0
        const original = Cluster.prototype.disconnect
        jest.spyOn(Cluster.prototype, 'disconnect').mockImplementation(async function (...args) {
          if (++teardowns === 1) await teardown.promise
          return original.apply(this, args)
        })

        const first = consumer.disconnect()
        await until(() => teardowns === 1, 'the held teardown')
        const closeAgain = () => first.then(() => consumer.disconnect(secondOptions))
        const second = arrangedFirst ? closeAgain() : null
        const queued = (
          call === 'run' ? consumer.run({ eachMessage: async () => {} }) : consumer.connect()
        ).then(
          () => null,
          (error) => error
        )
        const closed = second ?? closeAgain()
        teardown.resolve()

        expect((await queued)?.type).toBe('CONSUMER_STOPPING')
        await closed
        await pause(150)

        expect(events).toEqual(['CONNECT', 'DISCONNECT', 'DISCONNECT'])
        const membership = rig.cluster
          .requests()
          .filter(({ api }) => ['JoinGroup', 'SyncGroup', 'ConsumerGroupHeartbeat'].includes(api))
        expect(membership).toEqual([])
        expect(rig.cluster.sockets().filter((socket) => !socket.destroyed)).toEqual([])
      }
    )
  }
)
