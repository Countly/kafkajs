const Connection = require('../connection')
const ConnectionPool = require('../connectionPool')
const Broker = require('../../broker')
const BrokerPool = require('../../cluster/brokerPool')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const InstrumentationEmitter = require('../../instrumentation/emitter')
const events = require('../instrumentationEvents')
const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSConnectionClosedError, KafkaJSConnectionError } = require('../../errors')
const { isConnectionFailure } = require('../connectionFailure')
const { createFakeSocketFactory, respondWithFrame } = require('../../../testHelpers/fakeSocket')

const tick = () => new Promise(setImmediate)
const settle = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )
const protocol = () => ({
  request: {
    apiKey: apiKeys.ApiVersions,
    apiVersion: 0,
    apiName: 'ApiVersions',
    encode: async () => new Encoder(),
  },
  response: { decode: async (payload) => payload, parse: async (payload) => payload },
})
const options = (extra) => ({
  host: 'broker',
  port: 9092,
  clientId: 'client',
  connectionTimeout: 1000,
  requestTimeout: 30000,
  maxInFlightRequests: 1,
  enforceRequestTimeout: false,
  ...extra,
})
const values = [
  ['an Error', () => new Error('callback failed')],
  ['null', () => null],
  ['undefined', () => undefined],
  ['zero', () => 0],
  ['an empty string', () => ''],
  ['a Symbol', () => Symbol('failure')],
  [
    'a throwing message getter',
    (observe) =>
      Object.defineProperty({}, 'message', {
        get() {
          observe()
          throw new Error('getter failed')
        },
      }),
  ],
  [
    'a throwing message coercion',
    (observe) => ({
      message: {
        toString() {
          observe()
          throw new Error('coercion failed')
        },
      },
    }),
  ],
  [
    'a throwing primitive conversion',
    (observe) => ({
      [Symbol.toPrimitive]() {
        observe()
        throw new Error('conversion failed')
      },
    }),
  ],
  [
    'a throwing Proxy',
    (observe) =>
      new Proxy(
        {},
        {
          get() {
            observe()
            throw new Error('get failed')
          },
        }
      ),
  ],
]
const boundaries = [
  'queue-size fulfilment',
  'completion fulfilment',
  'pending diagnostic fulfilment',
  'socket factory',
  'timeoutRequest',
  'reportObserverFailure',
  'retireSibling',
  'sibling drain diagnostic',
  'refusal',
  'BrokerPool connect diagnostic',
  'BrokerPool connect rejection',
  'BrokerPool connectBroker rejection',
  'isConnectionFailure',
  'Cluster findBroker',
]

describe('Network > arbitrary thrown values', () => {
  it.each(
    boundaries.flatMap((boundary) => values.map(([label, build]) => [boundary, label, build]))
  )('%s contains %s and preserves the original value', async (boundary, label, build) => {
    let observeRead = () => {}
    const thrown = build(() => observeRead())
    const diagnostics = []
    const describedAt = []
    let diagnosticThrow = false
    const logger = createLogger({
      level: LEVELS.DEBUG,
      logCreator:
        () =>
        ({ log }) => {
          diagnostics.push(log)
          if (diagnosticThrow) throw thrown
        },
    })
    const fake = createFakeSocketFactory()
    const emitter = new InstrumentationEmitter()
    const connection = new Connection(
      options({ logger, socketFactory: fake.factory, instrumentationEmitter: emitter })
    )
    let pool
    const completed = []
    const replies = []
    const watch = (promise, index) => {
      completed[index] = 0
      return settle(promise).then((outcome) => {
        completed[index] += 1
        replies[index] = outcome
        return outcome
      })
    }
    const closed = () => {
      expect(connection.isConnected()).toBe(false)
      expect(connection.socket).toBeFalsy()
      expect(connection.requestQueue.inflight.size).toBe(0)
      expect(connection.requestQueue.pending).toHaveLength(0)
      if (fake.state.socket) expect(fake.state.socket.destroyed).toBe(true)
    }
    if (boundary.endsWith('fulfilment') || boundary === 'socket factory') {
      observeRead = () =>
        describedAt.push({
          attempt: connection.connectAttempt,
          connected: connection.isConnected(),
          inflight: connection.requestQueue.inflight.size,
          pending: connection.requestQueue.pending.length,
        })
    }
    const waitFor = async (predicate) => {
      for (let i = 0; i < 100 && !predicate(); i++) await tick()
      expect(predicate()).toBe(true)
    }
    try {
      if (boundary === 'Cluster findBroker') {
        const Cluster = require('../../cluster')
        const cluster = Object.create(Cluster.prototype)
        cluster.brokerPool = {
          findBroker: async () => {
            throw thrown
          },
        }
        cluster.refreshMetadataBestEffort = jest.fn()
        expect((await watch(cluster.findBroker({ nodeId: '0' }), 0)).error).toBe(thrown)
        expect(cluster.refreshMetadataBestEffort).not.toHaveBeenCalled()
        expect(completed).toEqual([1])
        closed()
        return
      }
      if (boundary === 'isConnectionFailure') {
        expect(() => isConnectionFailure(thrown)).not.toThrow()
        expect(isConnectionFailure(thrown)).toBe(false)
        expect(isConnectionFailure(new KafkaJSConnectionError('failed'))).toBe(true)
        expect(isConnectionFailure(new KafkaJSConnectionClosedError('closed'))).toBe(false)
        return
      }
      if (boundary === 'socket factory') {
        connection.socketFactory = () => {
          throw thrown
        }
        const { error } = await watch(connection.connect(), 0)
        closed()
        expect(error.name).toBe('KafkaJSConnectionError')
        expect(error.retriable).toBe(true)
        expect(error.cause).toBe(thrown)
        if (label === 'an Error') expect(error.message).toBe('Failed to connect: callback failed')
        expect(completed).toEqual([1])
        return
      }
      if (boundary.startsWith('BrokerPool connect')) {
        if (boundary.endsWith('diagnostic')) {
          const { Kafka, logLevel } = require('../../..')
          let armed = true
          const kafka = new Kafka({
            clientId: 'client',
            brokers: ['broker:9092'],
            socketFactory: fake.factory,
            enforceRequestTimeout: false,
            retry: { retries: 0 },
            logLevel: logLevel.DEBUG,
            logCreator:
              () =>
              ({ log }) => {
                if (armed && log.message === 'Connecting') throw thrown
              },
          })
          const admin = kafka.admin()
          const close = jest.spyOn(Connection.prototype, 'closeGeneration')
          const { error } = await watch(admin.connect(), 0)
          const closes = close.mock.calls.length
          close.mockRestore()
          expect(closes).toBe(1)
          armed = false
          await admin.disconnect()
          expect(fake.state.socket).toBeFalsy()
          expect(label === 'an Error' ? error : error.cause).toBe(thrown)
          if (label === 'an Error') expect(error.message).toBe('callback failed')
        } else {
          pool = new BrokerPool({ logger, connectionPoolBuilder: {}, retry: { retries: 0 } })
          pool.seedBroker = {
            isConnected: () => false,
            connect: async () => {
              throw thrown
            },
            disconnect: async () => {},
          }
          diagnosticThrow = true
          const { error } = await watch(
            boundary.includes('connectBroker')
              ? pool.connectBroker(pool.seedBroker)
              : pool.connect(),
            0
          )
          diagnosticThrow = false
          expect(label === 'an Error' ? error : error.cause).toBe(thrown)
        }
        expect(completed).toEqual([1])
        closed()
        return
      }
      if (boundary === 'retireSibling') {
        pool = new ConnectionPool(options({ logger, socketFactory: fake.factory, drainTimeout: 5 }))
        const broker = new Broker({ connectionPool: pool, logger })
        const sibling = await pool.getConnection(1)
        const reply = watch(sibling.send(protocol()), 0)
        await waitFor(() => sibling.requestQueue.inflight.size === 1)
        const destroy = pool.destroy.bind(pool)
        pool.destroy = async () => {
          await destroy({ drainTimeout: 0 })
          throw thrown
        }
        diagnosticThrow = true
        expect(() =>
          broker.retireSibling(new KafkaJSConnectionClosedError('closed'), pool.generation)
        ).not.toThrow()
        await tick()
        diagnosticThrow = false
        const entry = diagnostics.find(
          (log) => log.message === 'Failed to close the sibling connection'
        )
        expect(entry).toBeDefined()
        expect(entry.cause).toBe(thrown)
        if (label === 'an Error') expect(entry.error).toBe('callback failed')
        expect((await reply).error.name).toBe('KafkaJSConnectionClosedError')
        expect(fake.state.socket.destroyed).toBe(true)
        expect(completed).toEqual([1])
        expect(() => broker.retireSibling(thrown, pool.generation)).not.toThrow()
        return
      }
      if (boundary === 'sibling drain diagnostic') {
        let armed = false
        const drainLogger = createLogger({
          level: LEVELS.DEBUG,
          logCreator:
            () =>
            ({ log }) => {
              if (log.message === 'Failed to drain connection') diagnostics.push(log)
              if (armed && log.message === 'Waiting for pending requests') throw thrown
            },
        })
        pool = new ConnectionPool(
          options({ logger: drainLogger, socketFactory: fake.factory, drainTimeout: 5 })
        )
        const broker = new Broker({ connectionPool: pool, logger: drainLogger })
        const sibling = await pool.getConnection(1)
        const reply = watch(sibling.send(protocol()), 0)
        await waitFor(() => sibling.requestQueue.inflight.size === 1)
        armed = true
        broker.retireSibling(new KafkaJSConnectionClosedError('closed'), pool.generation)
        await new Promise((resolve) => setTimeout(resolve, 25))
        expect(fake.state.socket.destroyed).toBe(true)
        expect(sibling.requestQueue.inflight.size).toBe(0)
        const { error } = await reply
        expect(error.name).toBe('KafkaJSConnectionClosedError')
        const entry = diagnostics.find((log) => log.message === 'Failed to drain connection')
        expect(entry.cause).toBe(thrown)
        if (label === 'an Error') expect(entry.error).toBe('callback failed')
        expect(completed).toEqual([1])
        armed = false
        return
      }
      await connection.connect()
      if (boundary === 'refusal') {
        let checks = 0
        const reply = await watch(
          connection.send({
            ...protocol(),
            guard: () => {
              if (++checks === 1) return null
              throw thrown
            },
          }),
          0
        )
        expect(label === 'an Error' ? reply.error : reply.error.cause).toBe(thrown)
        expect(fake.state.writes).toHaveLength(0)
        expect(connection.isConnected()).toBe(true)
        await connection.disconnect({ drainTimeout: 0 })
        closed()
        expect(completed).toEqual([1])
        return
      }
      const requests = [
        watch(connection.send(protocol()), 0),
        watch(connection.send(protocol()), 1),
        watch(connection.send(protocol()), 2),
      ]
      await waitFor(() => connection.requestQueue.pending.length === 2)
      const entries = [
        ...connection.requestQueue.inflight.values(),
        ...connection.requestQueue.pending,
      ]
      const calls = entries.map((entry) => ({
        resolve: jest.spyOn(entry.entry, 'resolve'),
        reject: jest.spyOn(entry.entry, 'reject'),
      }))
      if (boundary.endsWith('fulfilment')) {
        if (boundary === 'pending diagnostic fulfilment') {
          const realDebug = connection.requestQueue.logger.debug
          connection.requestQueue.logger = {
            ...connection.requestQueue.logger,
            debug: (...args) => {
              if (args[0] === 'Consumed pending request') throw thrown
              return realDebug(...args)
            },
          }
        } else
          emitter.addListener(
            boundary === 'queue-size fulfilment'
              ? events.NETWORK_REQUEST_QUEUE_SIZE
              : events.NETWORK_REQUEST,
            () => {
              throw thrown
            }
          )
        expect(() =>
          respondWithFrame(fake.state, entries[0].correlationId, 0, Buffer.from('valid'))
        ).not.toThrow()
        closed()
        await Promise.all(requests)
        expect(replies[0].value).toEqual(Buffer.from('valid'))
        for (const { error } of replies.slice(1)) {
          expect(error.name).toBe('KafkaJSInvalidFrameError')
          expect(error.cause).toBe(thrown)
          if (label === 'an Error')
            expect(error.message).toBe('Unreadable response frame: callback failed')
        }
      } else if (boundary === 'timeoutRequest') {
        emitter.addListener(events.NETWORK_REQUEST_TIMEOUT, () => {
          closed()
          throw thrown
        })
        diagnosticThrow = true
        expect(() => entries[0].timeoutRequest()).not.toThrow()
        diagnosticThrow = false
        closed()
        await Promise.all(requests)
        expect(replies[0].error.name).toBe('KafkaJSRequestTimeoutError')
        const entry = diagnostics.find((log) => log.message === 'A request timeout listener failed')
        expect(entry).toBeDefined()
        expect(entry.cause).toBe(thrown)
        if (label === 'an Error') expect(entry.error).toBe('callback failed')
      } else {
        emitter.addListener(events.NETWORK_REQUEST_QUEUE_SIZE, () => {
          closed()
          throw thrown
        })
        diagnosticThrow = true
        expect(() => connection.failGeneration(new Error('generation failed'))).not.toThrow()
        diagnosticThrow = false
        closed()
        await Promise.all(requests)
        const entry = diagnostics.find((log) => log.message === 'A queue observer failed')
        expect(entry).toBeDefined()
        expect(entry.cause).toBe(thrown)
        if (label === 'an Error') expect(entry.error).toBe('callback failed')
      }
      expect(completed).toEqual([1, 1, 1])
      connection.failGeneration(new Error('late failure'))
      connection.requestQueue.settleAll(new Error('late settlement'))
      for (const call of calls)
        expect(call.resolve.mock.calls.length + call.reject.mock.calls.length).toBe(1)
    } finally {
      const attempt = connection.connectAttempt
      diagnosticThrow = false
      if (pool) await pool.destroy?.({ drainTimeout: 0 }).catch(() => {})
      await connection.disconnect({ drainTimeout: 0 })
      for (const { attempt: describedAttempt, ...state } of describedAt) {
        expect(describedAttempt).toBeGreaterThanOrEqual(attempt)
        expect(state).toEqual({ connected: false, inflight: 0, pending: 0 })
      }
    }
  })
})

describe('Connection error causes', () => {
  it.each(values)('keeps %s by identity in the public cause option', (_label, build) => {
    const thrown = build(() => {})
    const error = new KafkaJSConnectionError('connection failed', {
      broker: 'broker:9092',
      code: 'ECONNRESET',
      cause: thrown,
    })
    expect(error.cause).toBe(thrown)
    expect(error.message).toBe('connection failed')
    expect(error.name).toBe('KafkaJSConnectionError')
    expect(error.retriable).toBe(true)
    expect(error.code).toBe('ECONNRESET')
    expect(error.broker).toBe('broker:9092')
  })
})

describe('Connection failure retry causes', () => {
  it.each(values)('retains %s when retries are exhausted', (_label, build) => {
    const { KafkaJSNumberOfRetriesExceeded } = require('../../errors')
    const thrown = build(() => {})
    const connectionError = new KafkaJSConnectionError('connection failed', { cause: thrown })
    const error = new KafkaJSNumberOfRetriesExceeded(connectionError, {
      retryCount: 0,
      retryTime: 1,
    })
    expect(error.cause).toBe(connectionError)
    expect(error.cause.cause).toBe(thrown)
    expect(error.message).toBe('connection failed')
    expect(error.retriable).toBe(false)
  })
})

describe('Contained failure descriptions', () => {
  it('describes a function with a throwing message getter', () => {
    const describeThrown = require('../describeThrown')
    const thrown = Object.defineProperty(() => {}, 'message', {
      get() {
        throw new Error('unreadable')
      },
    })
    expect(describeThrown(thrown)).toBe('<unreadable thrown value>')
  })
  it('retains an Error whose category getter throws', () => {
    const cause = Object.defineProperty(new Error('callback failed'), 'category', {
      get() {
        throw new Error('unreadable')
      },
    })
    const error = new KafkaJSConnectionError('connection failed', { cause })
    expect(error.cause).toBe(cause)
    expect(error.message).toBe('connection failed')
    expect(error.retriable).toBe(true)
    expect(error.category).toBe('retriable')
  })
})

describe('Contained broker cleanup', () => {
  it('contains a diagnostic while recovering from illegal SASL state', async () => {
    const { KafkaJSProtocolError } = require('../../errors')
    const logger = createLogger({
      level: LEVELS.DEBUG,
      logCreator: () => () => {
        throw new Error('diagnostic failed')
      },
    })
    const pool = new BrokerPool({
      logger,
      connectionPoolBuilder: {},
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1 },
    })
    const illegal = new KafkaJSProtocolError({
      type: 'ILLEGAL_SASL_STATE',
      code: 34,
      message: 'illegal state',
      retriable: false,
    })
    const broker = {
      isConnected: () => false,
      connect: jest.fn().mockRejectedValueOnce(illegal).mockResolvedValue(),
      disconnect: jest.fn(async () => {}),
      connectionPool: { host: 'broker', port: 9092, rack: 'rack' },
      replaceConnectionPool: jest.fn(),
    }
    const replacement = {}
    pool.buildConnectionPool = jest.fn(async () => replacement)
    const caught = []
    const retrier = pool.retrier
    pool.retrier = (fn) =>
      retrier(async (...args) => {
        try {
          return await fn(...args)
        } catch (error) {
          caught.push(error)
          throw error
        }
      })
    await pool.connectBroker(broker)
    expect(caught[0].cause).toBe(illegal)
    expect(caught[0].message).toBe('illegal state')
    expect(caught[0].name).toBe('KafkaJSProtocolError')
    expect(caught[0].retriable).toBe(true)
    expect(broker.connect).toHaveBeenCalledTimes(2)
    expect(broker.disconnect).toHaveBeenCalledTimes(1)
    expect(broker.replaceConnectionPool).toHaveBeenCalledWith(replacement)
  })
  it('contains a hostile failure-source annotation and lookup', () => {
    const { attachFailureSource, failureSourceOf } = require('../../broker/failureSource')
    const fail = () => {
      throw new Error('unreadable')
    }
    const thrown = new Proxy({}, { get: fail, has: fail, defineProperty: fail })
    expect(() => attachFailureSource(thrown, { broker: {}, generation: 1 })).not.toThrow()
    expect(failureSourceOf(thrown)).toBeUndefined()
  })
})

describe('Protocol failure descriptions', () => {
  it('contains a native Error message getter during illegal SASL recovery', async () => {
    const logger = createLogger({ level: LEVELS.DEBUG, logCreator: () => () => {} })
    const pool = new BrokerPool({
      logger,
      connectionPoolBuilder: {},
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1 },
    })
    const thrown = Object.defineProperty(
      Object.assign(new Error('unreadable'), { type: 'ILLEGAL_SASL_STATE', code: 34 }),
      'message',
      {
        get() {
          throw new Error('message getter failed')
        },
      }
    )
    const broker = {
      isConnected: () => false,
      connect: jest.fn().mockRejectedValueOnce(thrown).mockResolvedValue(),
      disconnect: jest.fn(async () => {}),
      connectionPool: { host: 'broker', port: 9092 },
      replaceConnectionPool: jest.fn(),
    }
    pool.buildConnectionPool = jest.fn(async () => ({}))
    const caught = []
    const retrier = pool.retrier
    pool.retrier = (fn) =>
      retrier(async (...args) => {
        try {
          return await fn(...args)
        } catch (error) {
          caught.push(error)
          throw error
        }
      })
    await pool.connectBroker(broker)
    expect(caught[0].cause === thrown).toBe(true)
    expect(caught[0].message).toBe('<unreadable thrown value>')
    expect(caught[0].name).toBe('KafkaJSProtocolError')
    expect(caught[0].code).toBe(34)
    expect(caught[0].retriable).toBe(true)
    expect(broker.connect).toHaveBeenCalledTimes(2)
  })
})

describe('Contained Error classification', () => {
  it('closes and settles when a thrown Error has a throwing prototype trap', async () => {
    const fake = createFakeSocketFactory()
    const emitter = new InstrumentationEmitter()
    const logger = createLogger({ level: LEVELS.DEBUG, logCreator: () => () => {} })
    const connection = new Connection(
      options({ logger, socketFactory: fake.factory, instrumentationEmitter: emitter })
    )
    const thrown = Object.setPrototypeOf(
      new Error('prototype failure'),
      new Proxy(
        {},
        {
          getPrototypeOf() {
            throw new Error('prototype lookup failed')
          },
        }
      )
    )
    const replies = []
    try {
      await connection.connect()
      replies.push(settle(connection.send(protocol())), settle(connection.send(protocol())))
      for (let i = 0; i < 100 && connection.requestQueue.pending.length !== 1; i++) await tick()
      expect(connection.requestQueue.pending).toHaveLength(1)
      emitter.addListener(events.NETWORK_REQUEST, () => {
        throw thrown
      })
      const id = [...connection.requestQueue.inflight.keys()][0]
      expect(() => respondWithFrame(fake.state, id, 0, Buffer.from('valid'))).not.toThrow()
      expect(fake.state.socket.destroyed).toBe(true)
      const [first, second] = await Promise.all(replies)
      expect(first.value).toEqual(Buffer.from('valid'))
      expect(second.error.cause === thrown).toBe(true)
      expect(connection.requestQueue.inflight.size).toBe(0)
      expect(connection.requestQueue.pending).toHaveLength(0)
    } finally {
      await connection.disconnect({ drainTimeout: 0 })
      await Promise.all(replies)
    }
  })
})
