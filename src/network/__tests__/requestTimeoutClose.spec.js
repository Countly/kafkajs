const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const ConnectionPool = require('../connectionPool')
const Broker = require('../../broker')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { NETWORK_REQUEST_TIMEOUT, NETWORK_REQUEST_QUEUE_SIZE } = require('../instrumentationEvents')
const { createLogger, LEVELS } = require('../../loggers')
const {
  KafkaJSConnectionClosedError,
  KafkaJSConnectionError,
  KafkaJSInvalidFrameError,
  KafkaJSRequestTimeoutError,
} = require('../../errors')
const { connectionOpts, newLogger } = require('testHelpers')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { TOPIC, TOPIC_ID } = require('../../consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]

/** What a promise settled with, so a rejection is a value the test can look at. */
const settle = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )

/** Waits for a promise, failing with `what` after `ms` instead of leaving the test to time out. */
const within = (promise, ms, what) => {
  let timer
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not happen within ${ms} ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

const until = async (predicate, what) => {
  for (let turn = 0; turn < 400 && !predicate(); turn++) {
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  if (!predicate()) {
    throw new Error(`${what} did not happen`)
  }
}

const metadataRequests = (cluster) => cluster.requests().filter(({ api }) => api === 'Metadata')

const fetchOf = (broker, maxWaitTime = 5000) =>
  broker.fetch({
    maxWaitTime,
    topics: [{ topic: TOPIC, partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1048576 }] }],
    topicIds: new Map([[TOPIC, TOPIC_ID]]),
  })

const THROWN = [
  ['an Error', new Error('observer failed')],
  ['null', null],
  ['undefined', undefined],
]

describe('Network > a request timeout fails its generation', () => {
  const closers = []

  /**
   * A real broker over a real pool and connections, on the scripted cluster, with a request
   * timeout short enough to wait for.
   */
  const open = (cluster, options = {}) => {
    const connectionPool = new ConnectionPool(
      connectionOpts({
        host: 'node0',
        port: 9092,
        socketFactory: cluster.socketFactory,
        requestTimeout: 50,
        maxInFlightRequests: 1,
        ...options,
      })
    )
    const broker = new Broker({ connectionPool, logger: newLogger() })
    closers.push(() => broker.disconnect().catch(() => {}))
    return broker
  }

  afterEach(async () => {
    await Promise.all(closers.splice(0).map((close) => close()))
  })

  describe('with other requests on the connection', () => {
    let cluster
    let broker
    let stalled
    let timedOut
    let collateral

    beforeEach(async () => {
      cluster = createScriptedCluster({ nodes: NODES })
      broker = open(cluster)
      await broker.connect()
      stalled = cluster.schedule.hold({ api: 'Metadata' })
      timedOut = settle(broker.metadata([]))
      await within(stalled.arrived(), 2000, 'the stalled request')
      collateral = settle(broker.metadata([]))
    })

    it('rejects the stalled request with its own timeout error and every other with a retriable close', async () => {
      const first = await within(timedOut, 3000, 'the timeout')
      const second = await within(collateral, 3000, 'the collateral failure')

      expect(first.error).toBeInstanceOf(KafkaJSRequestTimeoutError)
      expect(first.error).not.toBeInstanceOf(KafkaJSConnectionError)
      expect(second.error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(second.error.retriable).toBe(true)
      expect(second.error).not.toBeInstanceOf(KafkaJSInvalidFrameError)
    })

    it('never writes a queued request to the socket that timed out', async () => {
      await within(Promise.all([timedOut, collateral]), 3000, 'the failure')

      expect(metadataRequests(cluster)).toHaveLength(1)
    })

    it('closes the connection and ignores a late reply', async () => {
      await within(Promise.all([timedOut, collateral]), 3000, 'the failure')
      const [socket] = cluster.sockets()

      expect(socket.destroyed).toBe(true)
      expect(socket.listenerCount('data')).toBe(0)
      expect(broker.connectionPool.isConnected()).toBe(false)
      expect(() => stalled.release()).not.toThrow()
      cluster.assertClean()
    })

    it('reconnects the next request on a new socket and negotiates again', async () => {
      await within(Promise.all([timedOut, collateral]), 3000, 'the failure')

      await expect(broker.metadata([])).resolves.toMatchObject({ brokers: expect.any(Array) })

      const apiVersions = cluster.requests().filter(({ api }) => api === 'ApiVersions')
      expect(apiVersions.map(({ generation }) => generation)).toEqual([1, 2])
      expect(cluster.sockets()).toHaveLength(2)
      cluster.assertClean()
    })
  })

  it('does not hold the failing request behind a healthy long Fetch on the other connection', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const broker = open(cluster)
    await broker.connect()
    const longFetch = cluster.schedule.hold({ api: 'Fetch' })
    const fetching = settle(fetchOf(broker))
    await within(longFetch.arrived(), 2000, 'the Fetch')
    const stalled = cluster.schedule.hold({ api: 'Metadata' })
    const timedOut = settle(broker.metadata([]))
    await within(stalled.arrived(), 2000, 'the stalled request')
    const collateral = settle(broker.metadata([]))

    const outcome = await within(collateral, 3000, 'the collateral failure')
    await within(timedOut, 3000, 'the timeout')

    expect(outcome.error).toBeInstanceOf(KafkaJSConnectionClosedError)
    const [metadataSocket, fetchSocket] = cluster.sockets()
    expect(metadataSocket.destroyed).toBe(true)
    expect(fetchSocket.destroyed).toBe(false)

    longFetch.release()
    expect((await within(fetching, 3000, 'the Fetch')).value).toBeDefined()
    await until(() => fetchSocket.destroyed, 'the sibling connection closing after its drain')
    cluster.assertClean()
  })

  it('honours the longer allowance of a Fetch: a Fetch held past the request timeout is not failed', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const broker = open(cluster)
    await broker.connect()
    const longFetch = cluster.schedule.hold({ api: 'Fetch' })
    const fetching = settle(fetchOf(broker))
    await within(longFetch.arrived(), 2000, 'the Fetch')

    await new Promise((resolve) => setTimeout(resolve, 300))

    expect(cluster.sockets().every((socket) => !socket.destroyed)).toBe(true)
    longFetch.release()
    expect((await within(fetching, 3000, 'the Fetch')).value).toBeDefined()
    cluster.assertClean()
  })

  it('never closes when the timeout is not enforced', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const broker = open(cluster, { enforceRequestTimeout: false, requestTimeout: 30 })
    await broker.connect()
    const stalled = cluster.schedule.hold({ api: 'Metadata' })
    const pending = settle(broker.metadata([]))
    await within(stalled.arrived(), 2000, 'the request')

    await new Promise((resolve) => setTimeout(resolve, 200))

    expect(cluster.sockets().every((socket) => !socket.destroyed)).toBe(true)
    stalled.release()
    expect((await within(pending, 3000, 'the answer')).value).toBeDefined()
    cluster.assertClean()
  })

  describe('while the broker negotiates versions', () => {
    it('rejects connect() with the retriable timeout, releases the lock and connects afresh next time', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster, { connectionTimeout: 50 })
      const stalled = cluster.schedule.hold({ api: 'ApiVersions' })
      const connecting = settle(broker.connect())
      await within(stalled.arrived(), 2000, 'the ApiVersions request')

      const outcome = await within(connecting, 3000, 'the timeout')

      expect(outcome.error).toBeInstanceOf(KafkaJSRequestTimeoutError)
      expect(outcome.error.retriable).toBe(true)
      const [first] = cluster.sockets()
      expect(first.destroyed).toBe(true)
      expect(first.listenerCount('data')).toBe(0)
      expect(broker.connectionPool.isConnected()).toBe(false)

      await within(broker.connect(), 3000, 'the next connect')

      expect(broker.isConnected()).toBe(true)
      const apiVersions = cluster.requests().filter(({ api }) => api === 'ApiVersions')
      expect(apiVersions.map(({ generation }) => generation)).toEqual([1, 2])
      cluster.assertClean()
    })
  })

  describe('observers', () => {
    const openObserved = async () => {
      const entries = []
      const logger = createLogger({
        level: LEVELS.ERROR,
        logCreator: () => (entry) => entries.push(entry),
      })
      const cluster = createScriptedCluster({ nodes: NODES })
      const instrumentationEmitter = new InstrumentationEventEmitter()
      const broker = open(cluster, { instrumentationEmitter, logger })
      await broker.connect()
      const connection = await broker.connectionPool.getConnection(0)
      return { cluster, broker, connection, instrumentationEmitter, entries }
    }

    it.each(THROWN)(
      'run only after the generation failed, and a listener that sends or throws %s changes nothing',
      async (_what, thrown) => {
        const { cluster, broker, connection, instrumentationEmitter, entries } =
          await openObserved()
        const seen = []
        let resent
        instrumentationEmitter.addListener(NETWORK_REQUEST_TIMEOUT, () => {
          seen.push({
            connected: connection.isConnected(),
            destroyed: cluster.sockets()[0].destroyed,
            inflight: connection.requestQueue.inflight.size,
            pending: connection.requestQueue.pending.length,
          })
          resent = settle(broker.metadata([]))
        })
        instrumentationEmitter.addListener(NETWORK_REQUEST_TIMEOUT, () => {
          throw thrown
        })
        instrumentationEmitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, () => {
          seen.push({ connected: connection.isConnected(), event: 'queue size' })
        })
        const stalled = cluster.schedule.hold({ api: 'Metadata' })
        const timedOut = settle(broker.metadata([]))
        await within(stalled.arrived(), 2000, 'the stalled request')

        expect((await within(timedOut, 3000, 'the timeout')).error).toBeInstanceOf(
          KafkaJSRequestTimeoutError
        )
        expect(
          (await within(resent, 3000, 'the request sent from the listener')).value
        ).toBeDefined()

        expect(seen.map(({ event }) => event ?? 'request timeout').slice(0, 2)).toEqual([
          'queue size',
          'request timeout',
        ])
        expect(seen.find(({ event }) => !event)).toEqual({
          connected: false,
          destroyed: true,
          inflight: 0,
          pending: 0,
        })
        expect(seen.every(({ connected }) => connected === false)).toBe(true)
        expect(seen.some(({ event }) => event === 'queue size')).toBe(true)
        expect(entries.map(({ log }) => log.message)).toContain('A request timeout listener failed')
        expect(metadataRequests(cluster).map(({ generation }) => generation)).toEqual([1, 2])
        cluster.assertClean()
      }
    )

    it.each(THROWN)(
      'contains a queue observer that throws %s, and still empties the queue',
      async (_what, thrown) => {
        const { cluster, broker, connection, instrumentationEmitter, entries } =
          await openObserved()
        instrumentationEmitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, () => {
          throw thrown
        })
        const stalled = cluster.schedule.hold({ api: 'Metadata' })
        const timedOut = settle(broker.metadata([]))
        await within(stalled.arrived(), 2000, 'the stalled request')
        const waiting = connection.requestQueue.waitForPendingRequests()

        await within(timedOut, 3000, 'the timeout')
        await within(waiting, 3000, 'the queue emptying')

        expect(entries.map(({ log }) => log.message)).toContain('A queue observer failed')
        expect(connection.requestQueue.inflight.size).toBe(0)
      }
    )
  })

  describe('Connection#failGeneration', () => {
    const closed = (connection) =>
      new KafkaJSConnectionClosedError('Closed connection', { host: 'node0', port: 9092 })

    it('is synchronous and idempotent', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const connection = await broker.connectionPool.getConnection(0)
      const destroy = jest.spyOn(connection.requestQueue, 'destroy')
      const emitQueueState = jest.spyOn(connection.requestQueue, 'emitQueueState')

      connection.failGeneration(closed(connection))

      expect(connection.isConnected()).toBe(false)
      expect(connection.socket).toBeNull()
      expect(cluster.sockets()[0].destroyed).toBe(true)
      expect(destroy).toHaveBeenCalledTimes(1)
      expect(emitQueueState).toHaveBeenCalledTimes(1)

      connection.failGeneration(closed(connection))

      expect(destroy).toHaveBeenCalledTimes(1)
      expect(emitQueueState).toHaveBeenCalledTimes(1)
    })

    it('leaves a newer socket in place when it ends the socket of an older generation', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const connection = await broker.connectionPool.getConnection(0)
      const current = connection.socket
      const older = { destroyed: false, destroy: jest.fn() }

      connection.closeGeneration(closed(connection), older)

      expect(older.destroy).toHaveBeenCalledTimes(1)
      expect(connection.socket).toBe(current)
    })

    it('rejects a request still encoding when it lands, writing it to no socket, not even a later one', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const broker = open(cluster)
      await broker.connect()
      const connection = await broker.connectionPool.getConnection(0)
      let finishEncoding
      const encoding = new Promise((resolve) => {
        finishEncoding = resolve
      })
      const real = requests.ApiVersions.protocol({ version: 4 })()
      const slow = {
        ...real,
        request: {
          ...real.request,
          encode: async () => {
            await encoding
            return new Encoder()
          },
        },
      }
      const id = connection.correlationId
      const sending = settle(connection.send(slow))

      connection.failGeneration(closed(connection))
      await broker.metadata([])
      finishEncoding()
      const outcome = await within(sending, 3000, 'the encoding request')

      expect(outcome.error).toBeInstanceOf(KafkaJSConnectionError)
      expect(outcome.error.retriable).toBe(true)
      expect(cluster.requests().map(({ correlationId }) => correlationId)).not.toContain(id)
    })
  })
})

describe('Network > the error contracts', () => {
  it('keeps the timeout and the invalid frame apart: neither is the other, the frame is a connection error', () => {
    const timeout = new KafkaJSRequestTimeoutError('timed out')
    const frame = new KafkaJSInvalidFrameError('bad frame', { reason: 'negative', size: -1 })
    const closed = new KafkaJSConnectionClosedError('closed', { host: 'h', port: 1 })

    expect(timeout).not.toBeInstanceOf(KafkaJSInvalidFrameError)
    expect(timeout).not.toBeInstanceOf(KafkaJSConnectionError)
    expect(frame).toBeInstanceOf(KafkaJSConnectionError)
    expect(frame).not.toBeInstanceOf(KafkaJSConnectionClosedError)
    expect(closed).not.toBeInstanceOf(KafkaJSInvalidFrameError)
    expect([timeout, frame, closed].map(({ retriable }) => retriable)).toEqual([true, true, true])
    expect(apiKeys.Metadata).toBe(3)
  })
})
