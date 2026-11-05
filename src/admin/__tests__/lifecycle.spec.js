const createAdmin = require('..')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('../../errors')
const { currentDeadline, checked, waitOf } = require('../deadline')
const { createResourceProbe } = require('../../../testHelpers/resourceProbe')

const logger = { namespace: () => logger, warn: jest.fn(), debug() {}, info() {}, error() {} }

/** @returns {object} a deferred answer whose rejection is observed by its caller */
const held = () => {
  let resolve, reject
  const promise = new Promise((yes, no) => {
    resolve = yes
    reject = no
  })
  return { promise, resolve, reject }
}

/** @param {Promise<any>} promise caller result @returns {object} observed terminal state */
const observe = (promise) => {
  const result = { state: 'pending' }
  promise.then(
    (value) => Object.assign(result, { state: 'fulfilled', value }),
    (error) => Object.assign(result, { state: 'rejected', error })
  )
  return result
}

/** @returns {Promise<void>} drains local continuations without advancing a deadline */
const flush = async () => {
  for (let n = 0; n < 50; n++) await Promise.resolve()
}

/** @param {() => boolean} ready observed boundary @returns {Promise<void>} bounded fake-clock pump */
const until = async (ready) => {
  for (let n = 0; n < 200 && !ready(); n++) {
    await jest.advanceTimersByTimeAsync(1)
    await new Promise((resolve) => setImmediate(resolve))
  }
  expect(ready()).toBe(true)
}

/** @param {object} [overrides] cluster seams @returns {object} admin and observable transport */
const fixture = (overrides = {}) => {
  const cluster = {
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    metadata: jest.fn(async () => ({ topicMetadata: [{ topic: 'current' }] })),
    ...overrides,
  }
  return { cluster, admin: createAdmin({ logger, cluster, retry: { retries: 0 } }) }
}

describe('admin lifecycle', () => {
  beforeEach(() => jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => {
    expect(jest.getTimerCount()).toBe(0)
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test('a completed close refuses an operation without a lookup until explicit connect', async () => {
    const { admin, cluster } = fixture()
    await admin.disconnect()
    await expect(admin.listTopics()).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    expect(cluster.metadata).not.toHaveBeenCalled()
    await admin.connect()
    await expect(admin.listTopics()).resolves.toEqual(['current'])
    await admin.disconnect()
    expect(cluster.disconnect).toHaveBeenCalledTimes(2)
  })

  test('first use without connect retains implicit connection behavior', async () => {
    const { admin } = fixture()
    await expect(admin.listTopics()).resolves.toEqual(['current'])
    await admin.disconnect()
  })

  test.each([-1, 0.5, NaN, Infinity, 2147483648, '10', true, null, () => {}])(
    'refuses invalid object timeout %p before changing admission',
    async (timeout) => {
      const { admin, cluster } = fixture()
      let result
      expect(() => {
        result = admin.disconnect({ timeout })
      }).not.toThrow()
      await expect(result).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(cluster.disconnect).not.toHaveBeenCalled()
      await expect(admin.listTopics()).resolves.toEqual(['current'])
      await admin.disconnect()
    }
  )

  test.each(['SIGTERM', 7, true, null, () => {}, {}, { type: 'event' }])(
    'a legacy argument %p drains as the no-argument call',
    async (argument) => {
      const { admin, cluster } = fixture()
      await admin.disconnect(argument)
      expect(cluster.disconnect).toHaveBeenCalledExactlyOnceWith()
    }
  )

  test.each([0, 2147483647])(
    'accepts endpoint timeout %p and clears its timer',
    async (timeout) => {
      const { admin, cluster } = fixture()
      await admin.disconnect({ timeout })
      expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    }
  )

  test('natural close drains admitted work and refuses new calls', async () => {
    const answer = held()
    const { admin, cluster } = fixture({ metadata: jest.fn(() => answer.promise) })
    const call = observe(admin.listTopics())
    await flush()
    const close = observe(admin.disconnect())
    await flush()
    expect(close.state).toBe('pending')
    await expect(admin.listTopics()).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    expect(cluster.metadata).toHaveBeenCalledTimes(1)
    expect(cluster.disconnect).not.toHaveBeenCalled()
    answer.resolve({ topicMetadata: [] })
    await flush()
    expect(call.state).toBe('fulfilled')
    expect(close.state).toBe('fulfilled')
    expect(cluster.disconnect).toHaveBeenCalledExactlyOnceWith()
  })

  test('the first concurrent close owns the deadline and cleanup Promise', async () => {
    const answer = held()
    const { admin, cluster } = fixture({ metadata: jest.fn(() => answer.promise) })
    const call = observe(admin.listTopics())
    await flush()
    const first = admin.disconnect({ timeout: 10 })
    expect(admin.disconnect({ timeout: 0 })).toBe(first)
    await expect(admin.disconnect({ timeout: -1 })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    const close = observe(first)
    await jest.advanceTimersByTimeAsync(9)
    expect(close.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)
    expect(close.state).toBe('fulfilled')
    expect(call.error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(cluster.disconnect).toHaveBeenCalledExactlyOnceWith({ drainTimeout: 0 })
    await admin.disconnect({ timeout: 0 })
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    answer.resolve({ topicMetadata: [] })
    await flush()
  })

  test('a natural first close cannot be shortened by a second close', async () => {
    const answer = held()
    const { admin } = fixture({ metadata: () => answer.promise })
    observe(admin.listTopics())
    await flush()
    const first = admin.disconnect()
    expect(admin.disconnect({ timeout: 0 })).toBe(first)
    const close = observe(first)
    await jest.advanceTimersByTimeAsync(100)
    expect(close.state).toBe('pending')
    answer.resolve({ topicMetadata: [] })
    await first
  })

  test('zero closes before queued admission can start wire work', async () => {
    const { admin, cluster } = fixture()
    const call = observe(admin.listTopics())
    await admin.disconnect({ timeout: 0 })
    await flush()
    expect(call.error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(cluster.metadata).not.toHaveBeenCalled()
  })

  test('a held invocation cannot send or publish after close and reconnect', async () => {
    const answer = held()
    const old = jest.fn()
    const metadata = jest
      .fn(async () => {
        const round = currentDeadline()
        checked(await waitOf(() => answer.promise))
        round.check()
        old()
        return { topicMetadata: [{ topic: 'obsolete' }] }
      })
      .mockImplementationOnce(async () => {
        const round = currentDeadline()
        checked(await waitOf(() => answer.promise))
        round.check()
        old()
        return { topicMetadata: [{ topic: 'obsolete' }] }
      })
      .mockResolvedValueOnce({ topicMetadata: [{ topic: 'replacement' }] })
    const { admin, cluster } = fixture({ metadata })
    const call = observe(admin.listTopics())
    await flush()
    await admin.disconnect({ timeout: 0 })
    await admin.connect()
    await expect(admin.listTopics()).resolves.toEqual(['replacement'])
    answer.resolve()
    await flush()
    expect(old).not.toHaveBeenCalled()
    expect(call.error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    await admin.disconnect()
  })

  test.each(['ApiVersions', 'SaslHandshake', 'SaslAuthenticate'])(
    'a close retires held %s setup before explicit reconnect',
    async (api) => {
      const Broker = require('../../broker')
      const Pool = require('../../network/connectionPool')
      const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
      const { Schedule } = require('../../../testHelpers/schedule')
      const schedule = new Schedule()
      const wire = createScriptedCluster({
        nodes: [{ id: 0, host: 'node', port: 9092 }],
        schedule,
        profile: 'sasl',
      })
      const pool = new Pool({
        host: 'node',
        port: 9092,
        logger,
        socketFactory: wire.socketFactory,
        connectionTimeout: 1000,
        requestTimeout: 5000,
        sasl: { mechanism: 'plain', username: 'u', password: 'p' },
      })
      const broker = new Broker({ connectionPool: pool, logger, authenticationTimeout: 1000 })
      const gate = schedule.hold({ api })
      const { admin } = fixture({
        connect: ({ fence }) => broker.connect({ guard: fence.guard }),
        disconnect: (options) => broker.disconnect(options),
      })
      const old = observe(admin.connect())
      try {
        await until(() => Boolean(gate.call))
        expect(gate.call?.api).toBe(api)
        const finalClose = admin.disconnect({ timeout: 0 })
        await jest.advanceTimersByTimeAsync(1)
        await finalClose
        const reopening = observe(admin.connect())
        await until(() => reopening.state !== 'pending')
        expect(reopening.state).toBe('fulfilled')
        const socket = pool.pool[0].socket
        const frames = wire.requests().length
        gate.release()
        await jest.advanceTimersByTimeAsync(100)
        expect(wire.requests()).toHaveLength(frames)
        expect(pool.pool[0].socket).toBe(socket)
        expect(socket.destroyed).toBe(false)
        expect(old.state).toBe('rejected')
      } finally {
        if (gate.settle) gate.release()
        const finalClose = admin.disconnect({ timeout: 0 })
        await jest.advanceTimersByTimeAsync(1)
        await finalClose
        await jest.advanceTimersByTimeAsync(1)
        expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
        wire.assertClean()
      }
    },
    10000
  )

  test.each(['encoding', 'queue', 'answer', 'decoding'])(
    'a close retires held %s work on the real connection and preserves its replacement',
    async (stage) => {
      const Broker = require('../../broker')
      const Pool = require('../../network/connectionPool')
      const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
      const { Schedule } = require('../../../testHelpers/schedule')
      const schedule = new Schedule()
      const wire = createScriptedCluster({
        nodes: [{ id: 0, host: 'node', port: 9092 }],
        schedule,
        advertise: { Metadata: [7, 7] },
      })
      const pool = new Pool({
        host: 'node',
        port: 9092,
        logger,
        socketFactory: wire.socketFactory,
        connectionTimeout: 1000,
        requestTimeout: 5000,
      })
      const broker = new Broker({ connectionPool: pool, logger })
      const gate = held()
      let reached = false
      let heldAnswer
      const request = require('../../protocol/requests/metadata/v7/request')({ topics: [] })
      const originalEncode = request.encode
      request.encode = async () => {
        if (stage === 'encoding') {
          reached = true
          await gate.promise
        }
        return originalEncode()
      }
      const originalResponse = require('../../protocol/requests/metadata/v7/response')
      const response = {
        ...originalResponse,
        decode: async (...args) => {
          if (stage === 'decoding') {
            reached = true
            await gate.promise
          }
          return originalResponse.decode(...args)
        },
      }
      const { admin } = fixture({
        connect: ({ fence }) => broker.connect({ guard: fence.guard }),
        disconnect: (options) => broker.disconnect(options),
        metadata: () => pool.pool[0].send({ request, response }),
      })
      const connected = observe(admin.connect())
      await until(() => connected.state !== 'pending')
      expect(connected.state).toBe('fulfilled')
      if (stage === 'queue') pool.pool[0].requestQueue.maybeThrottle(100)
      if (stage === 'answer') heldAnswer = schedule.hold({ api: 'Metadata' })
      const old = observe(admin.listTopics())
      try {
        if (stage === 'queue') await until(() => pool.pool[0].requestQueue.pending.length === 1)
        else if (stage === 'answer') await until(() => Boolean(heldAnswer.call))
        else await until(() => reached)
        if (stage === 'queue') expect(pool.pool[0].requestQueue.pending).toHaveLength(1)
        else if (stage === 'answer') expect(heldAnswer.call.api).toBe('Metadata')
        else expect(reached).toBe(true)
        const closing = admin.disconnect({ timeout: 10 })
        await jest.advanceTimersByTimeAsync(10)
        await closing
        const reopening = observe(admin.connect())
        await until(() => reopening.state !== 'pending')
        expect(reopening.state).toBe('fulfilled')
        const socket = pool.pool[0].socket
        const frames = wire.requests().length
        gate.resolve()
        if (heldAnswer?.settle) heldAnswer.release()
        await jest.advanceTimersByTimeAsync(100)
        expect(wire.requests()).toHaveLength(frames)
        expect(pool.pool[0].socket).toBe(socket)
        expect(socket.destroyed).toBe(false)
        expect(old.error).toBeInstanceOf(KafkaJSNonRetriableError)
      } finally {
        gate.resolve()
        if (heldAnswer?.settle) heldAnswer.release()
        const finalClose = admin.disconnect({ timeout: 0 })
        await jest.advanceTimersByTimeAsync(1)
        await finalClose
        await jest.advanceTimersByTimeAsync(1)
        const probe = createResourceProbe({
          sockets: () => ({
            created: wire.sockets().length,
            destroyed: wire.sockets().filter((socket) => socket.destroyed).length,
          }),
        })
        expect((await probe.snapshot()).sockets.open).toBe(0)
        wire.assertClean()
      }
    },
    3000
  )

  test('a borrowed consumer crash cannot disconnect the admin transport or restart itself', async () => {
    const createConsumer = require('../../consumer')
    const OffsetOperations = require('../../consumer/offsetManager/offsetOperations')
    const { disposeConsumer } = require('../../consumer/disposal')
    const failure = new KafkaJSNonRetriableError('temporary setup failed')
    const cluster = {
      connect: jest.fn(async () => {
        throw failure
      }),
      disconnect: jest.fn(async () => {}),
      refreshMetadataIfNecessary: jest.fn(async () => {}),
      removeBroker: jest.fn(),
      isConnected: () => true,
      addMultipleTargetTopics: jest.fn(async () => {}),
      onTopicRecreated: () => () => {},
      offsetOperations: () => new OffsetOperations(),
      retry: {},
    }
    const consumer = createConsumer({
      cluster,
      groupId: 'temporary',
      logger,
      ownsCluster: false,
      retry: { retries: 0, restartOnFailure: async () => true },
    })
    const crashed = held()
    consumer.on(consumer.events.CRASH, ({ payload }) => crashed.resolve(payload))
    await consumer.subscribe({ topic: 'topic' })
    await consumer.run({ eachMessage: async () => {} })
    expect(await crashed.promise).toMatchObject({ error: failure, restart: false })
    expect(cluster.disconnect).not.toHaveBeenCalled()
    await disposeConsumer(consumer)
  })

  test('an external callback re-enters its original invocation scope', async () => {
    const { sharedSetup } = require('../deadline')
    const ready = held()
    const answer = held()
    let original
    const { admin } = fixture({
      metadata: async () => {
        original = currentDeadline()
        ready.resolve()
        return original.wait(() => answer.promise)
      },
    })
    const call = observe(admin.listTopics())
    await ready.promise
    const seen = await sharedSetup(() => original.wait(() => currentDeadline()))
    expect(seen).toBe(original)
    await admin.disconnect({ timeout: 0 })
    answer.resolve({ topicMetadata: [] })
    await flush()
    expect(call.state).toBe('rejected')
  })

  test('withheld timer delivery still fences the action at the actual deadline', async () => {
    const answer = held()
    const now = jest.spyOn(performance, 'now').mockReturnValue(0)
    let round
    const publish = jest.fn()
    const { admin, cluster } = fixture({
      metadata: async () => {
        round = currentDeadline()
        await answer.promise
        checked(null)
        publish()
        return { topicMetadata: [] }
      },
    })
    const call = observe(admin.listTopics())
    await flush()
    const close = observe(admin.disconnect({ timeout: 10 }))
    await flush()
    now.mockReturnValue(11)
    expect(round.guard()).toBeInstanceOf(KafkaJSNonRetriableError)
    answer.resolve()
    await flush()
    expect(publish).not.toHaveBeenCalled()
    expect(call.state).toBe('rejected')
    expect(close.state).toBe('fulfilled')
    expect(cluster.disconnect).toHaveBeenCalledExactlyOnceWith({ drainTimeout: 0 })
  })

  test('explicit reconnect joins cleanup before replacing the captured setup', async () => {
    const cleanup = held()
    const setup = held()
    const { admin, cluster } = fixture({
      disconnect: jest.fn(() => cleanup.promise),
      connect: jest.fn(() => setup.promise),
    })
    const old = observe(admin.connect())
    await flush()
    const close = admin.disconnect({ timeout: 0 })
    const reopen = observe(admin.connect())
    await flush()
    expect(old.state).toBe('rejected')
    expect(cluster.connect).toHaveBeenCalledTimes(1)
    cleanup.resolve()
    await close
    await flush()
    expect(cluster.connect).toHaveBeenCalledTimes(2)
    const [first, second] = cluster.connect.mock.calls.map(([options]) => options.fence)
    expect(first).not.toBe(second)
    expect(first.guard()).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(second.guard()).toBeNull()
    setup.resolve()
    await flush()
    expect(reopen.state).toBe('fulfilled')
    await admin.disconnect()
  })

  test('a rejected-call handler may close and a connect listener may close', async () => {
    const failure = new Error('original failure')
    const one = fixture({
      metadata: async () => {
        throw failure
      },
    })
    await one.admin.listTopics().catch(async (error) => {
      expect(error).toBe(failure)
      await one.admin.disconnect()
    })
    const two = fixture()
    const closed = held()
    two.admin.on(two.admin.events.CONNECT, () =>
      two.admin.disconnect().then(closed.resolve, closed.reject)
    )
    await two.admin.connect()
    await closed.promise
    expect(two.cluster.disconnect).toHaveBeenCalledTimes(1)
  })

  test('a failed cleanup still completes once and explicit connect can resume', async () => {
    const { admin, cluster } = fixture({
      disconnect: jest.fn(async () => {
        throw new Error('cleanup')
      }),
    })
    await admin.disconnect({ timeout: 0 })
    await admin.disconnect()
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    await admin.connect()
    await expect(admin.listTopics()).resolves.toEqual(['current'])
    await expect(admin.disconnect()).rejects.toThrow('cleanup')
  })

  test.each([false, true])(
    'closing cancels real pending queue work with timeout enforcement %p',
    async (enforceRequestTimeout) => {
      const Queue = require('../../network/requestQueue')
      const queue = new Queue({
        logger,
        requestTimeout: 1000,
        enforceRequestTimeout,
        isConnected: () => true,
      })
      queue.maybeThrottle(100)
      const write = jest.fn()
      const metadata = () =>
        new Promise((resolve, reject) =>
          queue.push({
            entry: {
              correlationId: 1,
              apiName: 'Metadata',
              apiKey: 3,
              apiVersion: 7,
              resolve,
              reject,
            },
            expectResponse: true,
            sendRequest: write,
            guard: currentDeadline().guard,
          })
        )
      const { admin } = fixture({
        metadata,
        disconnect: async () => {
          queue.destroy()
        },
      })
      const call = observe(admin.listTopics())
      await flush()
      expect(queue.pending).toHaveLength(1)
      await admin.disconnect({ timeout: 0 })
      await jest.advanceTimersByTimeAsync(200)
      expect(write).not.toHaveBeenCalled()
      expect(call.state).toBe('rejected')
      expect(queue.pending).toHaveLength(0)
      expect(queue.inflight.size).toBe(0)
      expect(queue.throttleCheckTimeoutId).toBeNull()
    }
  )
})

describe('admin real resource closure', () => {
  test('natural and forced close return active timers to the measured baseline', async () => {
    const probe = createResourceProbe({ activeResources: true })
    const baseline = await probe.snapshot()
    for (const options of [undefined, { timeout: 0 }, { timeout: 10 }]) {
      const { admin } = fixture()
      await admin.listTopics()
      await admin.disconnect(options)
    }
    await new Promise((resolve) => setImmediate(resolve))
    const after = await probe.snapshot()
    expect(after.activeResources.Timeout || 0).toBe(baseline.activeResources.Timeout || 0)
    expect(after.activeResources.TCPSocketWrap || 0).toBe(
      baseline.activeResources.TCPSocketWrap || 0
    )
  })
})
