const { requests, lookup } = require('../../protocol/requests')
const Cluster = require('../../cluster')
const ConnectionPool = require('../connectionPool')
const Broker = require('../../broker')
const BrokerPool = require('../../cluster/brokerPool')
const { failureSourceOf, attachFailureSource } = require('../../broker/failureSource')
const { isConnectionFailure } = require('../connectionFailure')
const {
  KafkaJSConnectionClosedError,
  KafkaJSConnectionError,
  KafkaJSInvalidFrameError,
  KafkaJSProtocolError,
  KafkaJSRequestTimeoutError,
} = require('../../errors')
const { createLogger, LEVELS } = require('../../loggers')
const { connectionOpts, newLogger } = require('testHelpers')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]

const settle = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )

const within = (promise, ms, what) => {
  let timer
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not happen within ${ms} ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

const closedError = () => new KafkaJSConnectionClosedError('closed', { host: 'h', port: 1 })

describe('isConnectionFailure', () => {
  it.each([
    ['a connection error', new KafkaJSConnectionError('refused'), true],
    ['an invalid frame', new KafkaJSInvalidFrameError('bad', { reason: 'negative' }), true],
    ['a connection closed under a request', closedError(), false],
    ['a request timeout', new KafkaJSRequestTimeoutError('timed out'), false],
    ['a plain error', new Error('boom'), false],
    ['an object named like a connection error', { name: 'KafkaJSConnectionError' }, true],
  ])('is %s', (_name, error, expected) => {
    expect(isConnectionFailure(error)).toBe(expected)
  })
})

describe('the connection failures the callers act on', () => {
  const actedOn = [
    ['a connection error', () => new KafkaJSConnectionError('refused')],
    ['an invalid frame', () => new KafkaJSInvalidFrameError('bad', { reason: 'too-large' })],
  ]
  const notActedOn = [['a connection closed under a request', closedError]]
  const retry = { retries: 2, initialRetryTime: 1, maxRetryTime: 2 }

  const newCluster = () =>
    new Cluster({
      ...connectionOpts(),
      logger: newLogger(),
      retry,
      metadataRecoveryStrategy: 'none',
    })

  describe('BrokerPool#connect rotates the seed broker', () => {
    const connectWith = async (error) => {
      const pool = newCluster().brokerPool
      pool.brokers = {}
      pool.seedBroker = {
        isConnected: () => false,
        connect: jest.fn().mockRejectedValueOnce(error),
        versions: {},
      }
      pool.createSeedBroker = jest.fn(async () => {
        pool.seedBroker = {
          isConnected: () => false,
          connect: jest.fn(async () => {}),
          versions: {},
        }
      })
      await pool.connect().catch(() => {})
      return pool
    }

    it.each(actedOn)('after %s', async (_name, build) => {
      const pool = await connectWith(build())

      expect(pool.createSeedBroker).toHaveBeenCalledTimes(1)
    })

    it.each(notActedOn)(
      'and does not after %s, which the retrier retries on the same seed',
      async (_name, build) => {
        const seed = {
          isConnected: () => false,
          connect: jest.fn().mockRejectedValueOnce(build()).mockResolvedValue(),
          versions: {},
        }
        const pool = newCluster().brokerPool
        pool.brokers = {}
        pool.seedBroker = seed
        pool.createSeedBroker = jest.fn()

        await pool.connect()

        expect(pool.createSeedBroker).not.toHaveBeenCalled()
        expect(seed.connect).toHaveBeenCalledTimes(2)
      }
    )
  })

  describe('BrokerPool#connectBroker disconnects the broker and gives up at once', () => {
    const connectWith = async (error) => {
      const pool = newCluster().brokerPool
      const broker = {
        isConnected: () => false,
        connect: jest.fn().mockRejectedValue(error),
        disconnect: jest.fn(async () => {}),
      }
      const outcome = await settle(pool.connectBroker(broker))
      return { broker, outcome }
    }

    it.each(actedOn)('after %s', async (_name, build) => {
      const error = build()
      const { broker, outcome } = await connectWith(error)

      expect(outcome.error).toBe(error)
      expect(broker.connect).toHaveBeenCalledTimes(1)
      expect(broker.disconnect).toHaveBeenCalledTimes(1)
    })

    it.each(notActedOn)(
      'and retries after %s, leaving the broker as it is',
      async (_name, build) => {
        const { broker, outcome } = await connectWith(build())

        expect(outcome.error).toBeInstanceOf(Error)
        expect(broker.connect.mock.calls.length).toBeGreaterThan(1)
        expect(broker.disconnect).not.toHaveBeenCalled()
      }
    )
  })

  describe('Cluster#findBroker refreshes metadata', () => {
    const find = async (error) => {
      const cluster = newCluster()
      cluster.brokerPool.findBroker = jest.fn().mockRejectedValue(error)
      cluster.refreshMetadataBestEffort = jest.fn(async () => {})
      await settle(cluster.findBroker({ nodeId: '1' }))
      return cluster
    }

    it.each(actedOn)('after %s', async (_name, build) => {
      const error = build()
      const cluster = await find(error)

      expect(cluster.refreshMetadataBestEffort).toHaveBeenCalledTimes(1)
    })

    it.each(notActedOn)('and does not after %s', async (_name, build) => {
      const cluster = await find(build())

      expect(cluster.refreshMetadataBestEffort).not.toHaveBeenCalled()
    })
  })
})

describe('Connection and Broker lifecycle on the scripted cluster', () => {
  const closers = []

  const poolOn = (cluster, options = {}) => {
    const pool = new ConnectionPool(
      connectionOpts({
        host: 'node0',
        port: 9092,
        socketFactory: cluster.socketFactory,
        requestTimeout: 5000,
        maxInFlightRequests: 1,
        ...options,
      })
    )
    closers.push(() => pool.destroy({ drainTimeout: 0 }).catch(() => {}))
    return pool
  }

  const open = (cluster, options = {}) => {
    const broker = new Broker({ connectionPool: poolOn(cluster, options), logger: newLogger() })
    closers.push(() => broker.disconnect().catch(() => {}))
    return broker
  }

  /** A broker over a fake pool whose `send` fails as the test says. */
  const brokerOver = (send) => {
    const pool = {
      connectionTimeout: 1000,
      host: 'h',
      port: 1,
      sasl: null,
      generation: 1,
      isConnected: () => true,
      isAuthenticated: () => true,
      getConnectionByRequest: async () => ({ isConnected: () => true }),
      destroy: jest.fn(() => new Promise(() => {})),
      send: jest.fn(async () => send(pool)),
    }
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })
    broker.versions = { 3: { minVersion: 0, maxVersion: 13 } }
    broker.lookupRequest = lookup(broker.versions)
    broker.versionsGeneration = 1
    return { broker, pool }
  }

  afterEach(async () => {
    jest.useRealTimers()
    await Promise.all(closers.splice(0).map((close) => close()))
  })

  describe('disconnect({ drainTimeout })', () => {
    it('rejects what is still pending at the bound with a retriable close and destroys the socket', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const stalled = cluster.schedule.hold({ api: 'Metadata' })
      const pending = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the request')
      const connection = await broker.connectionPool.getConnection(0)
      const started = Date.now()

      await within(broker.disconnect({ drainTimeout: 40 }), 2000, 'the bounded close')

      expect(Date.now() - started).toBeGreaterThanOrEqual(35)
      const { error } = await pending
      expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(error.retriable).toBe(true)
      expect(cluster.sockets()[0].destroyed).toBe(true)
      expect(connection.socket).toBeNull()
      expect(connection.isConnected()).toBe(false)
      expect(connection.requestQueue.listenerCount('requestQueueEmpty')).toBe(0)
    })

    it('closes at once with a bound of 0, whatever is in flight', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const stalled = cluster.schedule.hold({ api: 'Metadata' })
      const pending = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the request')

      await within(broker.disconnect({ drainTimeout: 0 }), 1000, 'the close')

      expect((await pending).error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(cluster.sockets()[0].destroyed).toBe(true)
    })

    it('waits for a request that answers within an unbounded drain, which then completes', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const stalled = cluster.schedule.hold({ api: 'Metadata' })
      const pending = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the request')

      let closed = false
      const closing = broker.disconnect().then(() => {
        closed = true
      })
      await new Promise((resolve) => setTimeout(resolve, 100))
      expect(closed).toBe(false)
      stalled.release()
      await within(closing, 2000, 'the close')

      expect((await pending).value).toBeDefined()
      expect(cluster.sockets()[0].destroyed).toBe(true)
      cluster.assertClean()
    })

    it('leaves no timer behind when the drain ends before the bound', async () => {
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()

      await broker.disconnect({ drainTimeout: 60000 })

      expect(jest.getTimerCount()).toBe(0)
    })

    it('bounds a plain disconnect() by the drain timeout the pool was built with', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster, { drainTimeout: 30 })
      await broker.connect()
      const stalled = cluster.schedule.hold({ api: 'Metadata' })
      const pending = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the request')

      await within(broker.disconnect(), 2000, 'the pool-bounded close')

      expect((await pending).error).toBeInstanceOf(KafkaJSConnectionClosedError)
    })

    it('stays reusable: the next connect opens a new socket', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      await broker.disconnect({ drainTimeout: 0 })

      await broker.connect()

      expect(cluster.sockets()).toHaveLength(2)
      expect(broker.isConnected()).toBe(true)
      cluster.assertClean()
    })
  })

  describe('a retired broker', () => {
    it('opens nothing again: connect, send and getConnection fail with a retriable connection error', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const pool = broker.connectionPool
      const held = await pool.getConnection(0)

      await broker.retire({ drainTimeout: 0 })

      for (const attempt of [
        () => broker.connect(),
        () => broker.metadata([]),
        () => pool.getConnection(0),
        () => pool.send({ request: { apiKey: 3 } }, held),
      ]) {
        const { error } = await settle(attempt())
        expect(error).toBeInstanceOf(KafkaJSConnectionError)
        expect(error.name).toBe('KafkaJSConnectionError')
        expect(error.retriable).toBe(true)
        expect(error.message).toContain('replaced')
      }
      expect(held.isConnected()).toBe(false)
      expect(cluster.sockets()).toHaveLength(1)
    })

    it('holds a retired pool at once, so the first operation reached is refused whichever it is', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()

      await broker.retire({ drainTimeout: 0 })

      expect(broker.retired).toBe(true)
      expect(broker.connectionPool.retired).toBe(true)
      const { error } = await settle(broker.metadata([]))
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.message).toContain('replaced')
      expect(cluster.sockets()).toHaveLength(1)
    })

    it('drains its connections for at most the bound, in the background', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const stalled = cluster.schedule.hold({ api: 'Metadata' })
      const pending = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the request')

      await within(broker.retire({ drainTimeout: 30 }), 2000, 'the bounded retirement')

      expect((await pending).error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(cluster.sockets()[0].destroyed).toBe(true)
    })

    it('can be disconnected again without error', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      await broker.retire({ drainTimeout: 0 })

      await expect(broker.disconnect()).resolves.toBeUndefined()
    })
  })

  describe('replacing the connection pool of a broker', () => {
    const heldOnOldPool = async (cluster, broker) => {
      const stalled = cluster.schedule.hold({ api: 'Metadata' })
      const pending = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the request on the old pool')
      return { pending }
    }

    it('numbers a pool from the generation it is given', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const pool = poolOn(cluster, { generation: 7 })
      expect(pool.generation).toBe(7)

      await pool.getConnection(0)
      expect(pool.generation).toBe(8)

      pool.continueGenerationFrom(3)
      expect(pool.generation).toBe(8)
      pool.continueGenerationFrom(20)
      expect(pool.generation).toBe(21)
      await pool.destroy({ drainTimeout: 0 })
    })

    it('numbers the replacement on from the pool it replaces, so a generation never repeats', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const old = broker.connectionPool
      const replacement = poolOn(cluster)

      broker.replaceConnectionPool(replacement)
      await broker.connect()

      expect(broker.connectionPool).toBe(replacement)
      expect(replacement.generation).toBeGreaterThan(old.generation)
      await old.destroy({ drainTimeout: 0 })
    })

    it('numbers the replacement on from the replaced pool as it is when the swap happens', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const old = broker.connectionPool
      const replacement = poolOn(cluster, { generation: old.generation })
      await old.destroy({ drainTimeout: 0 })
      await broker.connect()
      await broker.connect()
      expect(old.generation).toBeGreaterThan(replacement.generation)

      broker.replaceConnectionPool(replacement)

      expect(replacement.generation).toBeGreaterThanOrEqual(old.generation)
      await old.destroy({ drainTimeout: 0 })
    })

    it('leaves a healthy replacement connected when a connection of the replaced pool closes under a request', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const old = broker.connectionPool
      const { pending } = await heldOnOldPool(cluster, broker)
      const replacement = poolOn(cluster)
      broker.replaceConnectionPool(replacement)
      await broker.connect()
      expect(replacement.isConnected()).toBe(true)

      await old.destroy({ drainTimeout: 0 })
      const { error } = await within(pending, 2000, 'the failed request')
      await new Promise(setImmediate)

      expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(broker.connectionPool).toBe(replacement)
      expect(replacement.isConnected()).toBe(true)
      expect(cluster.sockets()[1].destroyed).toBe(false)
      expect(broker.isConnected()).toBe(true)
      expect(replacement.generation).toBeGreaterThan(old.generation)
    })

    it('does the same when the replacement has advanced further, which was never in doubt', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const old = broker.connectionPool
      const { pending } = await heldOnOldPool(cluster, broker)
      const replacement = poolOn(cluster)
      broker.replaceConnectionPool(replacement)
      await broker.connect()
      await broker.disconnect({ drainTimeout: 0 })
      await broker.connect()

      await old.destroy({ drainTimeout: 0 })
      await within(pending, 2000, 'the failed request')
      await new Promise(setImmediate)

      expect(replacement.isConnected()).toBe(true)
      expect(cluster.sockets()[2].destroyed).toBe(false)
    })

    it('does not close a different pool that reports the same generation', async () => {
      let other
      const { broker, pool } = brokerOver(() => {
        other = { ...pool, destroy: jest.fn(() => new Promise(() => {})) }
        broker.connectionPool = other
        throw closedError()
      })

      await settle(broker.metadata([]))

      expect(other.generation).toBe(pool.generation)
      expect(other.destroy).not.toHaveBeenCalled()
      expect(pool.destroy).not.toHaveBeenCalled()
    })
  })

  describe('a request is bound to the pool it started on', () => {
    const commit = {
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
    }

    /** The apis written on each socket, oldest socket first. */
    const writtenBySocket = (cluster) => {
      const lists = cluster.sockets().map(() => [])
      for (const { generation, api } of cluster.requests()) {
        lists[generation - 1].push(api)
      }
      return lists
    }

    /** Where the swap lands, and how the request ends; every row ends by its settlement. */
    const rows = [
      [
        'before the request starts',
        async ({ broker, replacement }) => {
          broker.replaceConnectionPool(replacement)
          await broker.connect()
          return settle(broker.metadata([]))
        },
        {
          outcome: 'result',
          through: 'replacement',
          written: [['ApiVersions'], ['ApiVersions', 'Metadata']],
          destroyed: [false, false],
          replacementConnected: true,
        },
      ],
      [
        'during the first acquisition, with the replacement connected',
        async ({ broker, replacement }) => {
          const pending = settle(broker.metadata([]))
          broker.replaceConnectionPool(replacement)
          await broker.connect()
          return pending
        },
        {
          outcome: 'refused',
          generation: 1,
          written: [['ApiVersions'], ['ApiVersions']],
          destroyed: [false, false],
          replacementConnected: true,
        },
      ],
      [
        'during the first acquisition, with the replacement not connected',
        async ({ broker, replacement }) => {
          const pending = settle(broker.metadata([]))
          broker.replaceConnectionPool(replacement)
          return pending
        },
        {
          outcome: 'refused',
          generation: 1,
          written: [['ApiVersions']],
          destroyed: [false],
          replacementConnected: false,
        },
      ],
      [
        'during the version renegotiation',
        async ({ cluster, broker, old, replacement }) => {
          await old.getConnection(1)
          const stalled = cluster.schedule.hold({ api: 'ApiVersions' })
          const pending = settle(broker.metadata([]))
          await within(stalled.arrived(), 2000, 'the renegotiation')
          broker.replaceConnectionPool(replacement)
          const negotiating = settle(broker.connect())
          stalled.release()
          await negotiating
          return pending
        },
        {
          outcome: 'refused',
          generation: 2,
          written: [['ApiVersions', 'ApiVersions'], [], ['ApiVersions']],
          destroyed: [false, false, false],
          replacementConnected: true,
        },
      ],
      [
        'during the reacquisition of a connection that closed',
        async ({ broker, old, replacement }) => {
          const acquire = old.getConnectionByRequest.bind(old)
          let calls = 0
          old.getConnectionByRequest = async (request) => {
            const connection = await acquire(request)
            if (++calls === 1) {
              await connection.disconnect({ drainTimeout: 0 })
            } else {
              broker.replaceConnectionPool(replacement)
              await broker.connect()
            }
            return connection
          }
          return settle(broker.metadata([]))
        },
        {
          outcome: 'refused',
          generation: 1,
          written: [['ApiVersions'], [], ['ApiVersions']],
          destroyed: [true, false, false],
          replacementConnected: true,
        },
      ],
      [
        'after the send, while the response is held',
        async ({ cluster, broker, old, replacement }) => {
          const stalled = cluster.schedule.hold({ api: 'Metadata' })
          const pending = settle(broker.metadata([]))
          await within(stalled.arrived(), 2000, 'the request')
          broker.replaceConnectionPool(replacement)
          await broker.connect()
          await old.destroy({ drainTimeout: 0 })
          return pending
        },
        {
          outcome: 'closed under it',
          generation: 1,
          through: 'old',
          written: [['ApiVersions', 'Metadata'], ['ApiVersions']],
          destroyed: [true, false],
          replacementConnected: true,
        },
      ],
      [
        'with no swap',
        ({ broker }) => settle(broker.metadata([])),
        {
          outcome: 'result',
          through: 'old',
          written: [['ApiVersions', 'Metadata']],
          destroyed: [false],
          replacementConnected: false,
        },
      ],
      [
        'inside the pre-write guard, after the last check',
        async ({ cluster, broker, old, replacement }) => {
          const stalled = cluster.schedule.hold({ api: 'OffsetCommit' })
          let calls = 0
          const guard = () => {
            if (++calls === 2) {
              broker.replaceConnectionPool(replacement)
            }
            return null
          }
          const pending = settle(broker.offsetCommit(commit, { guard }))
          await within(stalled.arrived(), 2000, 'the request')
          await broker.connect()
          await old.destroy({ drainTimeout: 0 })
          return pending
        },
        {
          outcome: 'closed under it',
          api: 'OffsetCommit',
          generation: 1,
          through: 'old',
          written: [['ApiVersions', 'OffsetCommit'], ['ApiVersions']],
          destroyed: [true, false],
          replacementConnected: true,
        },
      ],
    ]

    it.each(rows)('%s', async (_name, act, expected) => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const old = broker.connectionPool
      const replacement = poolOn(cluster)
      const sends = (pool) => {
        const apis = []
        const send = pool.send.bind(pool)
        pool.send = (protocolRequest, ...rest) => {
          apis.push(protocolRequest.request.apiName)
          return send(protocolRequest, ...rest)
        }
        return apis
      }
      const sentOn = { old: sends(old), replacement: sends(replacement) }

      const outcome = await within(
        act({ cluster, broker, old, replacement }),
        3000,
        'the request to settle'
      )
      await new Promise(setImmediate)

      if (expected.outcome === 'result') {
        expect(outcome.error).toBeUndefined()
        expect(outcome.value).toBeDefined()
      } else {
        expect(outcome.error).toBeInstanceOf(KafkaJSConnectionClosedError)
        expect(outcome.error.retriable).toBe(true)
        expect(failureSourceOf(outcome.error)).toEqual({ broker, generation: expected.generation })
        if (expected.replacementConnected) {
          expect(replacement.generation).toBeGreaterThan(expected.generation)
        }
        if (expected.outcome === 'refused') {
          expect(outcome.error.message).toContain('replaced')
        }
      }
      expect(writtenBySocket(cluster)).toEqual(expected.written)
      expect(cluster.sockets().map((socket) => socket.destroyed)).toEqual(expected.destroyed)
      expect(replacement.isConnected()).toBe(expected.replacementConnected)
      const api = expected.api ?? 'Metadata'
      const through = ['old', 'replacement'].filter((pool) => sentOn[pool].includes(api))
      expect(through).toEqual(expected.through ? [expected.through] : [])
    })
  })

  describe('a broker retired while its connection pool is being rebuilt', () => {
    const illegalState = () =>
      new KafkaJSProtocolError({
        type: 'ILLEGAL_SASL_STATE',
        code: 34,
        message: 'illegal state',
        retriable: false,
      })

    const until = async (condition, what) => {
      const started = Date.now()
      while (!condition()) {
        if (Date.now() - started > 2000) {
          throw new Error(`${what} did not happen`)
        }
        await new Promise((resolve) => setTimeout(resolve, 2))
      }
    }

    /** Starts a connect whose first attempt fails and whose pool rebuild waits for the test. */
    const rebuilding = async (cluster) => {
      const brokerPool = new BrokerPool({
        logger: newLogger(),
        connectionPoolBuilder: {},
        retry: { retries: 3, initialRetryTime: 1, maxRetryTime: 2 },
      })
      const broker = open(cluster)
      const connect = broker.connect.bind(broker)
      broker.connect = jest.fn().mockRejectedValueOnce(illegalState()).mockImplementation(connect)
      let finish
      brokerPool.buildConnectionPool = jest.fn(
        () =>
          new Promise((resolve) => {
            finish = resolve
          })
      )
      const connecting = settle(brokerPool.connectBroker(broker))
      await until(() => brokerPool.buildConnectionPool.mock.calls.length === 1, 'the rebuild')
      return { broker, connecting, release: (pool) => finish(pool) }
    }

    it('refuses the late replacement: no socket is opened, the broker stays retired and the connect is refused', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const { broker, connecting, release } = await rebuilding(cluster)
      const original = broker.connectionPool
      await broker.retire({ drainTimeout: 0 })
      const replacement = poolOn(cluster)

      release(replacement)
      const { error } = await within(connecting, 3000, 'the connect')

      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error.message).toContain('replaced')
      expect(cluster.sockets()).toHaveLength(0)
      expect(replacement.retired).toBe(true)
      expect(broker.retired).toBe(true)
      expect(broker.connectionPool).toBe(original)
      expect(broker.isConnected()).toBe(false)
      expect(replacement.isConnected()).toBe(false)
    })

    it('connects on the replacement when the broker was not retired', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const { broker, connecting, release } = await rebuilding(cluster)
      const replacement = poolOn(cluster)

      release(replacement)
      const outcome = await within(connecting, 3000, 'the connect')

      expect(outcome.error).toBeUndefined()
      expect(broker.connectionPool).toBe(replacement)
      expect(cluster.sockets()).toHaveLength(1)
      expect(broker.isConnected()).toBe(true)
    })

    it('refuses a replacement offered to a retired broker, closing it unopened', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      await broker.retire({ drainTimeout: 0 })
      const original = broker.connectionPool
      const replacement = poolOn(cluster)

      expect(broker.replaceConnectionPool(replacement)).toBe(false)

      expect(broker.connectionPool).toBe(original)
      expect(replacement.retired).toBe(true)
      await expect(replacement.getConnection(0)).rejects.toBeInstanceOf(KafkaJSConnectionError)
      expect(cluster.sockets()).toHaveLength(1)
    })
  })

  describe('the pre-write guard of an offset commit', () => {
    const payload = {
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
    }
    const committed = (cluster) => cluster.requests().filter(({ api }) => api === 'OffsetCommit')
    const refusal = new Error('the membership was lost')

    it.each([
      [1, 'at entry'],
      [2, 'after the connection was acquired and connected'],
      [3, 'after the request was authenticated and encoded'],
      [4, 'at the instant of transmission'],
    ])('check %i, %s, refuses this one request and nothing else', async (check) => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      let calls = 0
      const guard = () => (++calls === check ? refusal : null)
      await broker.connect()

      const { error } = await settle(broker.offsetCommit(payload, { guard }))

      expect(error).toBe(refusal)
      expect(calls).toBe(check)
      expect(committed(cluster)).toEqual([])

      await broker.offsetCommit(payload)
      expect(committed(cluster)).toHaveLength(1)
      expect(cluster.sockets()).toHaveLength(1)
      cluster.assertClean()
    })

    it('refuses a request that waits behind another when the guard turns, taking no in-flight slot', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const connection = await broker.connectionPool.getConnection(0)
      const stalled = cluster.schedule.hold({ api: 'Metadata' })
      const first = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the first request')
      let refuse = false
      const waiting = settle(
        broker.offsetCommit(payload, { guard: () => (refuse ? refusal : null) })
      )
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect(connection.requestQueue.pending).toHaveLength(1)

      refuse = true
      stalled.release()

      expect((await within(waiting, 2000, 'the refusal')).error).toBe(refusal)
      expect((await first).value).toBeDefined()
      expect(committed(cluster)).toEqual([])
      expect(connection.requestQueue.inflight.size).toBe(0)
      expect(connection.isConnected()).toBe(true)
      expect(cluster.sockets()).toHaveLength(1)
      cluster.assertClean()
    })

    it('treats a guard that throws at transmission as a refusal with what it threw', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      let calls = 0
      const guard = () => {
        if (++calls === 4) {
          throw refusal
        }
        return null
      }

      const { error } = await settle(broker.offsetCommit(payload, { guard }))

      expect(error).toBe(refusal)
      expect(committed(cluster)).toEqual([])
      expect(broker.connectionPool.isConnected()).toBe(true)
    })
  })

  describe('the failure source', () => {
    it('notes the broker and the pool generation a failed request was sent on, out of sight', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster, { requestTimeout: 50 })
      await broker.connect()
      const stalled = cluster.schedule.hold({ api: 'Metadata' })
      const timedOut = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the stalled request')
      const collateral = settle(broker.metadata([]))

      const { error } = await within(collateral, 3000, 'the collateral failure')
      await timedOut

      expect(failureSourceOf(error)).toEqual({ broker, generation: 1 })
      expect(broker.connectionPool.generation).toBe(1)
      expect(Object.keys(error)).not.toContain('failureSource')
      expect(Object.getOwnPropertySymbols(error)).toHaveLength(1)
      expect(
        Object.getOwnPropertyDescriptor(error, Object.getOwnPropertySymbols(error)[0])
      ).toMatchObject({
        enumerable: false,
        writable: false,
      })
      expect(JSON.parse(JSON.stringify(error))).not.toHaveProperty('broker.connectionPool')
    })

    it('keeps the first note and ignores anything that is not an object', () => {
      const error = new Error('x')
      attachFailureSource(error, { broker: 'first', generation: 1 })
      attachFailureSource(error, { broker: 'second', generation: 2 })
      attachFailureSource('text', { broker: 'x', generation: 3 })
      attachFailureSource(null, { broker: 'x', generation: 3 })

      expect(failureSourceOf(error)).toEqual({ broker: 'first', generation: 1 })
      expect(failureSourceOf('text')).toBeUndefined()
      expect(failureSourceOf(undefined)).toBeUndefined()
    })
  })

  describe('closing the sibling connection after a connection closed under a request', () => {
    it('closes it in the background, without holding the failing request', async () => {
      const { broker, pool } = brokerOver(() => {
        throw closedError()
      })

      const { error } = await within(settle(broker.metadata([])), 1000, 'the failure')

      expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(pool.destroy).toHaveBeenCalledTimes(1)
    })

    it('logs a failure to close it and still fails the request with its own error', async () => {
      const entries = []
      const logger = createLogger({
        level: LEVELS.ERROR,
        logCreator: () => (entry) => entries.push(entry),
      })
      const { pool } = brokerOver(() => {
        throw closedError()
      })
      const broker = new Broker({ connectionPool: pool, logger })
      broker.versions = { 3: { minVersion: 0, maxVersion: 13 } }
      broker.lookupRequest = lookup(broker.versions)
      broker.versionsGeneration = 1
      pool.destroy = jest.fn(() => Promise.reject(new Error('close failed')))

      const { error } = await settle(broker.metadata([]))
      await new Promise(setImmediate)

      expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(entries.map(({ log }) => log.message)).toContain(
        'Failed to close the sibling connection'
      )
    })

    it('compares against the generation of the connection the request was sent on, after any reconnect it caused', async () => {
      const { broker, pool } = brokerOver(() => {
        throw closedError()
      })
      broker.versionsGeneration = 0
      pool.generation = 1
      broker.connect = jest.fn(async () => {
        pool.generation = 2
        broker.versionsGeneration = 2
      })

      await settle(broker.metadata([]))

      expect(broker.connect).toHaveBeenCalledTimes(1)
      expect(pool.destroy).toHaveBeenCalledTimes(1)
    })

    it('does nothing when a connection of the pool was established since the request was sent', async () => {
      const { broker, pool } = brokerOver((p) => {
        p.generation = 2
        throw closedError()
      })

      await settle(broker.metadata([]))

      expect(pool.destroy).not.toHaveBeenCalled()
    })

    it.each([
      ['a request timeout', () => new KafkaJSRequestTimeoutError('timed out')],
      ['an invalid frame', () => new KafkaJSInvalidFrameError('bad', { reason: 'negative' })],
      ['a connection error', () => new KafkaJSConnectionError('Not connected')],
      ['null', () => null],
      ['undefined', () => undefined],
    ])('does nothing for %s, and rejects with what was thrown', async (_name, build) => {
      const thrown = build()
      const { broker, pool } = brokerOver(() => {
        throw thrown
      })

      expect(await settle(broker.metadata([]))).toStrictEqual({ error: thrown })

      expect(pool.destroy).not.toHaveBeenCalled()
      expect(requests.Metadata).toBeDefined()
    })
  })
})
