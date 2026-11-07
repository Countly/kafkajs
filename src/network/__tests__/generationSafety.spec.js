const { EventEmitter } = require('events')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const Connection = require('../connection')
const ConnectionPool = require('../connectionPool')
const Broker = require('../../broker')
const { Kafka, logLevel } = require('../../../index')
const {
  KafkaJSConnectionClosedError,
  KafkaJSInvalidFrameError,
  KafkaJSNonRetriableError,
  KafkaJSRequestTimeoutError,
} = require('../../errors')
const { failureSourceOf } = require('../../broker/failureSource')
const { connectionOpts, newLogger } = require('testHelpers')
const { createFakeSocketFactory } = require('../../../testHelpers/fakeSocket')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')

const settle = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )
const payload = {
  groupId: 'g',
  groupGenerationId: 1,
  memberId: 'm',
  topics: [{ topic: 't', partitions: [{ partition: 0, offset: '1' }] }],
}
const noResponseProtocol = {
  request: {
    apiKey: apiKeys.ApiVersions,
    apiVersion: 0,
    apiName: 'ApiVersions',
    encode: async () => new Encoder(),
    expectResponse: () => false,
  },
  response: { decode: async (p) => p, parse: async (p) => p },
}

describe('Network > generation failure boundaries', () => {
  const connections = []
  const brokers = []
  afterEach(async () => {
    await Promise.all(brokers.splice(0).map((b) => b.disconnect({ drainTimeout: 0 })))
    await Promise.all(connections.splice(0).map((c) => c.disconnect({ drainTimeout: 0 })))
  })

  const stalledBroker = async () => {
    const cluster = createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
    const pool = new ConnectionPool(
      connectionOpts({
        host: 'node0',
        port: 9092,
        socketFactory: cluster.socketFactory,
        enforceRequestTimeout: false,
      })
    )
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })
    brokers.push(broker)
    await broker.connect()
    const held = cluster.schedule.hold({ api: 'Metadata' })
    const pending = settle(broker.metadata([]))
    await held.arrived()
    return { cluster, broker, connection: pool.pool[0], pending }
  }

  it('queue empty observers see a fully closed generation after a timeout', async () => {
    const { connection, cluster, pending } = await stalledBroker()
    const seen = []
    connection.requestQueue.once('requestQueueEmpty', () => {
      seen.push({ connected: connection.isConnected(), destroyed: cluster.sockets()[0].destroyed })
    })
    const request = [...connection.requestQueue.inflight.values()][0]
    request.timeoutRequest()
    expect(seen).toEqual([{ connected: false, destroyed: true }])
    expect((await pending).error).toBeInstanceOf(KafkaJSRequestTimeoutError)
  })

  it('a throwing queue empty observer cannot prevent timeout settlement or close', async () => {
    const { connection, cluster } = await stalledBroker()
    connection.requestQueue.once('requestQueueEmpty', () => {
      throw new Error('queue empty observer failed')
    })
    const request = [...connection.requestQueue.inflight.values()][0]
    expect(() => request.timeoutRequest()).not.toThrow()
    expect(cluster.sockets()[0].destroyed).toBe(true)
  })

  it('retirement during connection establishment prevents a late pool send from reconnecting', async () => {
    const fake = createFakeSocketFactory()
    let retired = false
    const socketFactory = (options) =>
      fake.factory({
        ...options,
        onConnect: () => {
          options.onConnect()
          if (!retired) {
            retired = true
            pool.retire()
            pool.destroy({ drainTimeout: 0 }).catch(() => {})
          }
        },
      })
    const pool = new ConnectionPool(
      connectionOpts({ host: 'node0', port: 9092, socketFactory, enforceRequestTimeout: false })
    )
    connections.push(...pool.pool)
    const outcome = await settle(pool.send(noResponseProtocol))
    expect({
      error: outcome.error?.name,
      sockets: fake.state.sockets.length,
      writes: fake.state.writes.length,
    }).toEqual({ error: 'KafkaJSConnectionError', sockets: 1, writes: 0 })
  })

  it('failGeneration settles an outstanding connect attempt', async () => {
    const socket = new EventEmitter()
    socket.destroyed = false
    socket.destroy = () => {
      socket.destroyed = true
    }
    socket.write = () => true
    const connection = new Connection(
      connectionOpts({
        host: 'node0',
        port: 9092,
        socketFactory: () => socket,
        connectionTimeout: 30,
        enforceRequestTimeout: false,
      })
    )
    connections.push(connection)
    let outcome
    const connecting = settle(connection.connect()).then((result) => {
      outcome = result
    })
    const failure = new KafkaJSInvalidFrameError('bad frame', { reason: 'negative', size: -1 })
    connection.failGeneration(failure)
    await new Promise((resolve) => setTimeout(resolve, 60))
    expect(outcome).toEqual({ error: failure })
    await connecting
  })

  it('checks the commit guard before looking up a request on a never connected broker', async () => {
    const pool = new ConnectionPool(connectionOpts())
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })
    brokers.push(broker)
    const refusal = new Error('membership lost')
    const guard = jest.fn(() => refusal)
    const outcome = await settle(broker.offsetCommit(payload, { guard }))
    expect(outcome.error).toBe(refusal)
    expect(guard).toHaveBeenCalledTimes(1)
  })

  it('a socket end closes before its queue observer can run or throw', async () => {
    const { connection, cluster } = await stalledBroker()
    const socket = cluster.sockets()[0]
    const onEnd = socket.listeners('end')[0]
    connection.requestQueue.instrumentationEmitter = null
    const seen = []
    connection.requestQueue.once('requestQueueEmpty', () => {
      seen.push({ connected: connection.isConnected(), destroyed: socket.destroyed })
      throw new Error('end observer failed')
    })
    const outcome = await settle(Promise.resolve().then(onEnd))
    expect(outcome.error).toBeUndefined()
    expect(seen).toEqual([{ connected: false, destroyed: true }])
    expect(socket.destroyed).toBe(true)
  })

  it('a close-only socket destruction fails its requests and reconnects the next request', async () => {
    const { broker, connection, cluster, pending } = await stalledBroker()
    const socket = cluster.sockets()[0]
    const generation = connection.generation
    const onEnd = jest.fn()
    const onError = jest.fn()
    socket.on('end', onEnd)
    socket.on('error', onError)
    const fail = jest.spyOn(connection, 'failGeneration')

    socket.destroy()
    await new Promise(setImmediate)

    expect(onEnd).not.toHaveBeenCalled()
    expect(onError).not.toHaveBeenCalled()
    expect(connection.isConnected()).toBe(false)
    expect(connection.socket).toBeNull()
    expect(fail).toHaveBeenCalledTimes(1)
    const { error } = await pending
    expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
    expect(error).toMatchObject({ message: 'Closed connection', retriable: true })
    expect(connection.requestQueue.inflight.size).toBe(0)

    await expect(broker.metadata([])).resolves.toMatchObject({ brokers: expect.any(Array) })
    expect(connection.generation).toBeGreaterThan(generation)
    expect(connection.socket).not.toBe(socket)
    expect(connection.isConnected()).toBe(true)
    socket.emit('close', false)
    expect(connection.isConnected()).toBe(true)
    expect(fail).toHaveBeenCalledTimes(1)
    cluster.assertClean()
  })

  it.each(['end', 'ECONNRESET'])(
    'a close after %s does not fail the generation twice',
    async (event) => {
      const { connection, cluster, pending } = await stalledBroker()
      const socket = cluster.sockets()[0]
      const fail = jest.spyOn(connection, 'failGeneration')
      if (event === 'end') socket.emit('end')
      else socket.destroy(Object.assign(new Error('reset'), { code: 'ECONNRESET' }))
      await new Promise(setImmediate)

      expect(connection.isConnected()).toBe(false)
      expect(fail).toHaveBeenCalledTimes(1)
      expect((await pending).error).toMatchObject({
        name: event === 'end' ? 'KafkaJSConnectionClosedError' : 'KafkaJSConnectionError',
        retriable: true,
      })
    }
  )

  it('an intentional disconnect ignores its socket close without another queue event', async () => {
    const { connection, cluster, pending } = await stalledBroker()
    const socket = cluster.sockets()[0]
    const fail = jest.spyOn(connection, 'failGeneration')
    const queueState = jest.spyOn(connection.requestQueue, 'emitQueueState')

    await connection.disconnect({ drainTimeout: 0 })
    await new Promise(setImmediate)

    expect(socket.destroyed).toBe(true)
    expect(connection.isConnected()).toBe(false)
    expect((await pending).error).toBeInstanceOf(KafkaJSConnectionClosedError)
    expect(fail).not.toHaveBeenCalled()
    expect(queueState).toHaveBeenCalledTimes(1)
  })

  it('a frozen guard error remains the request rejection', async () => {
    const { broker } = await stalledBroker()
    const refusal = Object.freeze(new Error('membership lost'))
    const outcome = await settle(broker.offsetCommit(payload, { guard: () => refusal }))
    expect(outcome.error).toBe(refusal)
  })

  it('failure source names the generation established by the final pool reconnect', async () => {
    const { broker, connection, cluster, pending } = await stalledBroker()
    connection.failGeneration(
      new KafkaJSConnectionClosedError('closed', { host: 'node0', port: 9092 })
    )
    await pending
    await broker.connect()
    await broker.connectionPool.getConnection(1)
    const connect = broker.connect.bind(broker)
    jest.spyOn(broker, 'connect').mockImplementation(async () => {
      await connect()
      await connection.disconnect({ drainTimeout: 0 })
    })
    const held = cluster.schedule.hold({ api: 'Metadata' })
    const sending = settle(broker.metadata([]))
    await held.arrived()
    const actual = broker.connectionPool.generation
    connection.failGeneration(
      new KafkaJSConnectionClosedError('closed after final reconnect', {
        host: 'node0',
        port: 9092,
      })
    )
    const { error } = await sending
    expect(failureSourceOf(error).generation).toBe(actual)
  })

  it('diagnostic logging cannot stop the generation close', async () => {
    const { connection, cluster } = await stalledBroker()
    const log = connection.logDebug.bind(connection)
    connection.logDebug = (message, ...args) => {
      if (message === 'Generation failed') throw new Error('diagnostic failed')
      return log(message, ...args)
    }
    const error = new KafkaJSInvalidFrameError('bad frame', { reason: 'negative', size: -1 })
    expect(() => connection.failGeneration(error)).not.toThrow()
    expect(cluster.sockets()[0].destroyed).toBe(true)
  })

  it('warns when the effective default consumer maxBytes exceeds the configured frame bound', () => {
    const entries = []
    new Kafka({
      brokers: ['localhost:9092'],
      maxResponseSize: 2 * 1024 * 1024,
      logCreator: () => (entry) => entries.push(entry),
    }).consumer({ groupId: 'g' })
    const warnings = entries.filter(({ level }) => level === logLevel.WARN)
    expect(warnings).toHaveLength(1)
    expect(warnings[0].log.message).toContain('maxBytes (10485760)')
  })
  it('settles the last timeout before releasing a standalone waiter despite a throwing observer', async () => {
    const RequestQueue = require('../requestQueue')
    const queue = new RequestQueue({
      requestTimeout: 100,
      enforceRequestTimeout: false,
      logger: newLogger(),
    })
    const order = []
    queue.once('requestQueueEmpty', () => {
      order.push('observer')
      throw new Error('observer failed')
    })
    const reject = jest.fn(() => order.push('rejected'))
    queue.push({ entry: { correlationId: 1, reject }, expectResponse: true, sendRequest: () => {} })
    const waiting = queue.waitForPendingRequests().then(() => order.push('waiter'))
    expect(() => queue.inflight.get(1).timeoutRequest()).not.toThrow()
    await waiting
    expect(reject).toHaveBeenCalledWith(expect.any(KafkaJSRequestTimeoutError))
    expect(order).toEqual(['rejected', 'observer', 'waiter'])
    expect(queue.inflight.size).toBe(0)
    queue.destroy()
  })

  it.each(['end', 'error', 'timeout'])(
    'a socket %s closes all state before observers and preserves its error class',
    async (event) => {
      const { connection, cluster, pending } = await stalledBroker()
      const socket = cluster.sockets()[0]
      const seen = []
      connection.requestQueue.once('requestQueueEmpty', () => {
        seen.push({
          connected: connection.isConnected(),
          destroyed: socket.destroyed,
          queued: connection.requestQueue.inflight.size,
        })
        throw new Error('observer failed')
      })
      connection.logError = () => {
        throw new Error('diagnostic failed')
      }
      const firing = await settle(
        Promise.resolve().then(() => socket.listeners(event)[0](new Error('socket failed')))
      )
      expect(firing.error).toBeUndefined()
      expect(seen).toEqual([{ connected: false, destroyed: true, queued: 0 }])
      expect((await pending).error.name).toBe(
        event === 'end' ? 'KafkaJSConnectionClosedError' : 'KafkaJSConnectionError'
      )
      for (const name of ['data', 'end', 'timeout']) expect(socket.listenerCount(name)).toBe(0)
    }
  )

  it.each(['end', 'error', 'timeout'])(
    'a connecting socket %s settles its attempt and clears the deadline before diagnostics',
    async (event) => {
      jest.useFakeTimers()
      try {
        const socket = new EventEmitter()
        socket.destroy = () => {
          socket.destroyed = true
        }
        const connection = new Connection(
          connectionOpts({ socketFactory: () => socket, enforceRequestTimeout: false })
        )
        connections.push(connection)
        connection.logError = () => {
          throw new Error('diagnostic failed')
        }
        const connecting = settle(connection.connect())
        expect(jest.getTimerCount()).toBe(1)
        const firing = await settle(
          Promise.resolve().then(() => socket.listeners(event)[0](new Error('socket failed')))
        )
        expect(firing.error).toBeUndefined()
        expect(jest.getTimerCount()).toBe(0)
        expect((await connecting).error.name).toBe('KafkaJSConnectionError')
        expect(connection.socket).toBeNull()
        expect(socket.destroyed).toBe(true)
      } finally {
        jest.useRealTimers()
      }
    }
  )

  it('the connection deadline synchronously closes and settles a still connecting attempt', async () => {
    jest.useFakeTimers()
    try {
      const socket = new EventEmitter()
      socket.destroy = () => {
        socket.destroyed = true
      }
      const connection = new Connection(
        connectionOpts({
          socketFactory: () => socket,
          connectionTimeout: 30,
          enforceRequestTimeout: false,
        })
      )
      connections.push(connection)
      const connecting = settle(connection.connect())
      expect(() => jest.advanceTimersByTime(30)).not.toThrow()
      expect(socket.destroyed).toBe(true)
      expect((await connecting).error.message).toBe('Connection timeout')
      expect((await connecting).error).toMatchObject({ phase: 'TCP/TLS', timeout: 30 })
      expect(connection.socket).toBeNull()
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      jest.useRealTimers()
    }
  })

  it('an orderly disconnect supersedes a connecting attempt with a plain connection error', async () => {
    const socket = new EventEmitter()
    socket.destroy = () => {
      socket.destroyed = true
    }
    const connection = new Connection(
      connectionOpts({ socketFactory: () => socket, enforceRequestTimeout: false })
    )
    connections.push(connection)
    const connecting = settle(connection.connect())
    await connection.disconnect({ drainTimeout: 0 })
    expect((await connecting).error.name).toBe('KafkaJSConnectionError')
    expect(socket.destroyed).toBe(true)
  })

  it('a broker retired while its drain is pending refuses the connected fast path', async () => {
    const { broker, connection, cluster, pending } = await stalledBroker()
    const retiring = broker.retire({ drainTimeout: 100 })
    expect(connection.isConnected()).toBe(true)
    const { error } = await settle(broker.connect())
    expect(error).toMatchObject({ name: 'KafkaJSConnectionError', retriable: true })
    expect(error.message).toContain('replaced')
    await retiring
    await pending
    expect(cluster.sockets()).toHaveLength(1)
  })

  it('retirement while broker connect waits for its lock refuses acquisition without opening a socket', async () => {
    const cluster = createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
    const pool = new ConnectionPool(
      connectionOpts({ host: 'node0', port: 9092, socketFactory: cluster.socketFactory })
    )
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })
    brokers.push(broker)
    await broker.lock.acquire()
    const connecting = settle(broker.connect())
    pool.retire()
    await broker.lock.release()
    expect((await connecting).error.message).toContain('replaced')
    expect(cluster.sockets()).toHaveLength(0)
    await broker.lock.acquire()
    await broker.lock.release()
  })

  it.each([
    ['returns', 1],
    ['throws', 1],
    ['returns', 4],
    ['throws', 4],
  ])(
    'a guard that %s a frozen error at checkpoint %i keeps that rejection and a usable connection',
    async (mode, checkpoint) => {
      const { broker, cluster } = await stalledBroker()
      const refusal = Object.freeze(new Error('membership lost'))
      let calls = 0
      const guard = () => {
        if (++calls !== checkpoint) return null
        if (mode === 'throws') throw refusal
        return refusal
      }
      const { error } = await settle(broker.offsetCommit(payload, { guard }))
      expect(error).toBe(refusal)
      expect(calls).toBe(checkpoint)
      expect(cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
      await broker.offsetCommit(payload)
      expect(cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toHaveLength(1)
      expect(broker.connectionPool.isConnected()).toBe(true)
    }
  )

  it.each([
    ['null', null],
    ['undefined', undefined],
  ])(
    'a guard that throws %s at transmission refuses the commit with an error it caused, writing nothing',
    async (_what, thrown) => {
      const { broker, cluster } = await stalledBroker()
      let calls = 0
      const guard = () => {
        if (++calls === 4) throw thrown
        return null
      }

      const { error } = await settle(broker.offsetCommit(payload, { guard }))

      expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(error.cause).toBe(thrown)
      expect(cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
    }
  )

  it('an ApiVersions failure captures the generation after direct pool acquisition', async () => {
    const cluster = createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
    const pool = new ConnectionPool(
      connectionOpts({
        host: 'node0',
        port: 9092,
        socketFactory: cluster.socketFactory,
        enforceRequestTimeout: false,
      })
    )
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })
    brokers.push(broker)
    const held = cluster.schedule.hold({ api: 'ApiVersions' })
    const sending = settle(broker.apiVersions())
    await held.arrived()
    const generation = pool.generation
    pool.pool[0].failGeneration(
      new KafkaJSConnectionClosedError('closed', { host: 'node0', port: 9092 })
    )
    expect(failureSourceOf((await sending).error)).toEqual({ broker, generation })
  })

  it.each([
    ['omitted', {}],
    ['undefined', { maxBytes: undefined }],
  ])('warns once for the effective fetch default when maxBytes is %s', (_name, options) => {
    const entries = []
    new Kafka({
      brokers: ['localhost:9092'],
      maxResponseSize: 2 * 1024 * 1024,
      logCreator: () => (entry) => entries.push(entry),
    }).consumer({ groupId: 'g', ...options })
    const warnings = entries.filter(({ level }) => level === logLevel.WARN)
    expect(warnings).toHaveLength(1)
    expect(warnings[0].log.message).toContain('maxBytes (10485760)')
  })

  it('does not warn when the effective fetch default equals the response bound', () => {
    const entries = []
    new Kafka({
      brokers: ['localhost:9092'],
      maxResponseSize: 10485760,
      logCreator: () => (entry) => entries.push(entry),
    }).consumer({ groupId: 'g' })
    expect(entries.filter(({ level }) => level === logLevel.WARN)).toEqual([])
  })

  it('unguarded dispatch preserves protocol identity and shares the queue timeout callback', async () => {
    const fake = createFakeSocketFactory()
    const pool = new ConnectionPool(
      connectionOpts({ socketFactory: fake.factory, enforceRequestTimeout: false })
    )
    connections.push(...pool.pool)
    const connection = pool.pool[0]
    const send = jest.spyOn(connection, 'send')
    await pool.send(noResponseProtocol, connection)
    expect(send.mock.calls[0][0]).toBe(noResponseProtocol)
    const queue = connection.requestQueue
    const request = { entry: { correlationId: 1 }, expectResponse: true, sendRequest: () => {} }
    expect(queue.createSocketRequest(request).afterTimeout).toBe(
      queue.createSocketRequest(request).afterTimeout
    )
  })

  it('ordinary dispatch has no allocating absent-options default or unconditional guard wrapper', () => {
    const fs = require('fs')
    const brokerSource = fs.readFileSync(require.resolve('../../broker'), 'utf8')
    const poolSource = fs.readFileSync(require.resolve('../connectionPool'), 'utf8')
    const queueSource = fs.readFileSync(require.resolve('../requestQueue'), 'utf8')
    const dispatch = brokerSource.slice(
      brokerSource.indexOf('  async [PRIVATE.SEND_REQUEST]('),
      brokerSource.indexOf('\n  /**', brokerSource.indexOf('  async [PRIVATE.SEND_REQUEST]('))
    )
    const poolSend = poolSource.slice(
      poolSource.indexOf('  async send('),
      poolSource.indexOf('\n  getConnectionByRequest(')
    )
    const createRequest = queueSource.slice(
      queueSource.indexOf('  createSocketRequest('),
      queueSource.indexOf('\n  /**', queueSource.indexOf('  createSocketRequest('))
    )
    expect(dispatch).not.toMatch(/=\s*\{\}/)
    expect(dispatch).toContain('guard ? { guard } : null')
    expect(poolSend).not.toMatch(/=\s*\{\}/)
    expect(createRequest).toContain('afterTimeout: this.afterTimeout')
    expect(createRequest).not.toMatch(/afterTimeout:\s*\(/)
    expect(queueSource).toContain('if (this.listenerCount(REQUEST_QUEUE_EMPTY) === 0)')
  })
  it.each([1, 2, 3, 4])(
    'checks commit checkpoint %i at its actual lookup, acquisition, encoding and transmission boundary',
    async (checkpoint) => {
      const cluster = createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
      const pool = new ConnectionPool(
        connectionOpts({
          host: 'node0',
          port: 9092,
          socketFactory: cluster.socketFactory,
          enforceRequestTimeout: false,
        })
      )
      const broker = new Broker({ connectionPool: pool, logger: newLogger() })
      brokers.push(broker)
      await broker.connect()
      const connection = pool.pool[0]
      const acquire = jest.spyOn(pool, 'getConnectionByRequest')
      const send = jest.spyOn(connection, 'send')
      const push = jest.spyOn(connection.requestQueue, 'push')
      const write = jest.spyOn(cluster.sockets()[0], 'write')
      let encoded = false
      const originalLookup = broker.lookupRequestWithMax.bind(broker)
      const lookup = jest.spyOn(broker, 'lookupRequestWithMax').mockImplementation((...args) => {
        const build = originalLookup(...args)
        return (payload) => {
          const protocol = build(payload)
          const encode = protocol.request.encode
          protocol.request.encode = async (...args) => {
            const result = await encode.apply(protocol.request, args)
            encoded = true
            return result
          }
          return protocol
        }
      })
      const refusal = new Error('membership lost')
      let calls = 0
      const guard = () => {
        calls += 1
        expect(lookup).toHaveBeenCalledTimes(calls === 1 ? 0 : 1)
        expect(acquire).toHaveBeenCalledTimes(calls === 1 ? 0 : 1)
        expect(send).toHaveBeenCalledTimes(calls < 3 ? 0 : 1)
        expect(encoded).toBe(calls >= 3)
        expect(push).toHaveBeenCalledTimes(calls < 4 ? 0 : 1)
        expect(write).not.toHaveBeenCalled()
        return calls === checkpoint ? refusal : null
      }
      expect((await settle(broker.offsetCommit(payload, { guard }))).error).toBe(refusal)
      expect(calls).toBe(checkpoint)
      expect(write).not.toHaveBeenCalled()
      expect(connection.requestQueue.inflight.size).toBe(0)
      expect(connection.isConnected()).toBe(true)
      cluster.assertClean()
    }
  )
  it('keeps the first private failure source when the same error is annotated twice', () => {
    const { attachFailureSource } = require('../../broker/failureSource')
    const error = new Error('connection failed')
    const first = { broker: {}, generation: 1 }
    attachFailureSource(error, first)
    attachFailureSource(error, { broker: {}, generation: 2 })
    expect(failureSourceOf(error)).toBe(first)
    expect(Object.keys(error)).toEqual([])
  })
  it.each([
    ['an Error', new Error('factory failed')],
    ['null', null],
    ['undefined', undefined],
  ])(
    'a socket factory that throws %s clears the connect deadline and framing state with a plain connection error',
    async (_what, thrown) => {
      jest.useFakeTimers()
      try {
        const connection = new Connection(
          connectionOpts({
            socketFactory: () => {
              throw thrown
            },
            enforceRequestTimeout: false,
          })
        )
        connections.push(connection)
        const reset = jest.spyOn(connection.frames, 'reset')
        const { error } = await settle(connection.connect())
        expect(error.name).toBe('KafkaJSConnectionError')
        expect(error.message).toContain(`Failed to connect: ${thrown?.message}`)
        expect(error.cause).toBe(thrown)
        expect(reset).toHaveBeenCalledTimes(1)
        expect(connection.socket).toBeNull()
        expect(connection.isConnected()).toBe(false)
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        jest.useRealTimers()
      }
    }
  )
  it('notifies empty observers after an explicit generation failure settled and closed the socket', async () => {
    const { connection, cluster, pending } = await stalledBroker()
    const seen = []
    connection.requestQueue.once('requestQueueEmpty', () => {
      seen.push({
        connected: connection.isConnected(),
        destroyed: cluster.sockets()[0].destroyed,
        inflight: connection.requestQueue.inflight.size,
      })
      throw new Error('observer failed')
    })
    const failure = new KafkaJSInvalidFrameError('bad frame', { reason: 'negative', size: -1 })
    expect(() => connection.failGeneration(failure)).not.toThrow()
    expect(seen).toEqual([{ connected: false, destroyed: true, inflight: 0 }])
    expect((await pending).error).toBe(failure)
  })
})
