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

      expect(outcome.error).toBeInstanceOf(KafkaJSConnectionError)
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

describe('Failure removal across delayed reactions', () => {
  const {
    BOUND,
    NODES,
    until,
    outcome,
    createBoundHarness,
    createRemovalGroup,
  } = require('./fixtures/obsoleteConnections')
  const { KafkaJSConnectionClosedError, KafkaJSRequestTimeoutError } = require('../../errors')
  const { failureSourceOf } = require('../../broker/failureSource')
  const createProducer = require('../../producer')
  const createConsumer = require('../../consumer')
  const Runner = require('../../consumer/runner')
  const ClassicGroupProtocol = require('../../consumer/groupProtocol/classic')
  const InstrumentationEventEmitter = require('../../instrumentation/emitter')
  const { newLogger } = require('testHelpers')
  const { WireOrder } = require('../../../testHelpers/wireOrder')
  const apiKeys = require('../../protocol/requests/apiKeys')
  const TOPIC = 'test-topic'
  const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
  const RETRY = { retries: 5, initialRetryTime: 10, maxRetryTime: 20, factor: 0, multiplier: 1 }

  /** Scripts every routing field explicitly; no endpoint comes from a pair's defaults. */
  const metadata = (leader, moved) => ({
    brokers: NODES.map((n) => ({
      nodeId: n.id,
      host: moved && n.id === leader ? moved.host : n.host,
      port: moved && n.id === leader ? moved.port : n.port,
      rack: null,
    })),
    controllerId: 0,
    topics: [
      {
        errorCode: 0,
        name: TOPIC,
        topicId: TOPIC_ID,
        isInternal: false,
        partitions: [
          {
            errorCode: 0,
            partitionIndex: 0,
            leaderId: leader,
            leaderEpoch: 1,
            replicas: [leader],
            isr: [leader],
            offlineReplicas: [],
          },
        ],
        topicAuthorizedOperations: 0,
      },
    ],
  })

  /**
   * Supplies literal Produce v11/v12 response fields to the strict fixture builder.
   * @returns {object} an accepted partition response with no endpoint hint
   */
  const produced = () => ({
    responses: [
      {
        topic: TOPIC,
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            baseOffset: '0',
            logAppendTime: '-1',
            logStartOffset: '0',
            recordErrors: [],
            errorMessage: null,
            leader: { leaderId: -1, leaderEpoch: -1 },
          },
        ],
      },
    ],
    throttleTime: 0,
    nodeEndpoints: [],
  })

  it.each(['replacement', 'reconnect'])(
    'a delayed consumer removal preserves the %s and its new socket',
    async (kind) => {
      const h = await createBoundHarness({ enforceRequestTimeout: true })
      const group = createRemovalGroup(h.cluster)
      try {
        const generation = h.old.connectionPool.generation
        const { first, second } = await h.overload()
        await jest.advanceTimersByTimeAsync(BOUND * 2)
        expect((await first).error).toBeInstanceOf(KafkaJSRequestTimeoutError)
        const error = (await second).error
        expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
        expect(failureSourceOf(error)).toMatchObject({ broker: h.old, generation })
        if (kind === 'replacement') await h.replace('hint')
        const current = h.cluster.brokerPool.brokers[1]
        await current.connect()
        const socket = current.connectionPool.pool[0].socket
        const currentGeneration = current.connectionPool.generation
        if (kind === 'reconnect') expect(currentGeneration).toBeGreaterThan(generation)
        await group.recoverFromFetch(error)
        expect(h.cluster.brokerPool.brokers[1]).toBe(current)
        expect(current.connectionPool.pool[0].socket).toBe(socket)
        expect(socket.destroyed).toBe(false)
        await expect(current.metadata([])).resolves.toBeDefined()
        expect(h.socket.destroyed).toBe(true)
        expect(h.connection.socket).not.toBe(h.socket)
      } finally {
        group.obligations.clear()
        await h.close()
      }
    }
  )

  it('a failure from a replaced seed pool cannot remove the reused seed Broker', async () => {
    const h = await createBoundHarness({ target: 0, enforceRequestTimeout: true })
    const oldPool = h.old.connectionPool
    try {
      const { first, second } = await h.overload()
      await jest.advanceTimersByTimeAsync(BOUND * 2)
      expect((await first).error).toBeInstanceOf(KafkaJSRequestTimeoutError)
      const error = (await second).error
      expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      const source = failureSourceOf(error)
      expect(source.broker).toBe(h.old)
      await h.replace('hint', NODES[2])
      const current = h.cluster.brokerPool.brokers[0]
      expect(current).toBe(h.old)
      expect(current.connectionPool).not.toBe(oldPool)
      await current.connect()
      const socket = current.connectionPool.pool[0].socket
      h.cluster.removeBroker({ host: error.host, port: error.port, source })
      expect(h.cluster.brokerPool.seedBroker).toBe(current)
      expect(h.cluster.brokerPool.brokers[0]).toBe(current)
      expect(socket.destroyed).toBe(false)
      await expect(current.metadata([])).resolves.toBeDefined()
    } finally {
      await h.close()
    }
  })

  it('a pre-write refusal preserves a replacement that has never connected', async () => {
    const h = await createBoundHarness()
    const broker = h.old
    const oldPool = broker.connectionPool
    try {
      expect(oldPool.generation).toBe(1)
      const replacement = await h.cluster.connectionPoolBuilder.build(NODES[2])
      const writes = h.socket.writes.length
      const sockets = h.seam.sockets().length
      const refusing = outcome(broker.metadata([]))
      expect(broker.replaceConnectionPool(replacement)).toBe(true)
      const error = (await refusing).error
      expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      const source = failureSourceOf(error)
      expect(source).toEqual({ broker, generation: 1 })
      expect(replacement.generation).toBe(source.generation)
      expect(replacement.generationBase).toBe(source.generation)
      expect(replacement.pool.every((connection) => !connection.socket)).toBe(true)
      h.cluster.removeBroker({ host: error.host, port: error.port, source })
      expect(h.cluster.brokerPool.brokers[1] === broker).toBe(true)
      expect(broker.retired).toBe(false)
      expect(replacement.retired).toBe(false)
      expect(h.seam.sockets()).toHaveLength(sockets)
      expect(h.socket.writes).toHaveLength(writes)
      await broker.connect()
      expect(replacement.generation).toBeGreaterThan(source.generation)
      await expect(broker.metadata([])).resolves.toBeDefined()
    } finally {
      oldPool.retire()
      await oldPool.destroy({ drainTimeout: 0 })
      await h.close()
    }
  })

  it('consumer crash removal forwards the same private source before normal terminal cleanup', async () => {
    const h = await createBoundHarness({ enforceRequestTimeout: true })
    let consumer
    const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
    jest.spyOn(Runner.prototype, 'stop').mockResolvedValue()
    try {
      const { second } = await h.overload()
      await jest.advanceTimersByTimeAsync(BOUND * 2)
      const error = (await second).error
      expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      const remove = jest.spyOn(h.cluster, 'removeBroker')
      consumer = createConsumer({
        cluster: h.cluster,
        logger: newLogger(),
        groupId: 'crash-removal',
        instrumentationEmitter: new InstrumentationEventEmitter(),
        retry: { retries: 0, restartOnFailure: async () => false },
      })
      await consumer.run({ eachMessage: async () => {}, autoCommit: false })
      await start.mock.instances[0].onCrash(error)
      expect(remove).toHaveBeenCalledWith({
        host: error.host,
        port: error.port,
        source: failureSourceOf(error),
      })
    } finally {
      jest.restoreAllMocks()
      await h.close(consumer ? [consumer] : [])
    }
  })

  it.each([
    ['Heartbeat', (protocol) => protocol.heartbeat({ interval: 0 })],
    ['LeaveGroup', (protocol) => protocol.leave()],
  ])(
    'a classic %s after the coordinator node is removed finds the coordinator again and reaches the replacement Broker',
    async (api, send) => {
      const h = await createBoundHarness()
      h.seam.answer('FindCoordinator', ({ coordinatorKeys }) => ({
        coordinators: coordinatorKeys.map((key) => ({
          key,
          nodeId: 1,
          host: 'old',
          port: 9093,
          errorCode: 0,
          errorMessage: null,
        })),
      }))
      const find = jest.spyOn(h.cluster, 'findGroupCoordinator')
      const protocol = new ClassicGroupProtocol({
        cluster: h.cluster,
        groupId: 'coordinator-refind',
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        assigners: [],
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: newLogger(),
      })
      try {
        await protocol.connect()
        Object.assign(protocol, { memberId: 'member-1', generationId: 3 })
        await send(protocol)
        expect(protocol.coordinator).toBe(h.old)
        expect(find).toHaveBeenCalledTimes(1)
        // A leave forgets the member id; the second send needs it again.
        protocol.memberId = 'member-1'

        h.cluster.removeBroker({
          host: 'old',
          port: 9093,
          source: { broker: h.old, generation: h.old.connectionPool.generation },
        })
        expect(h.old.retired).toBe(true)

        await expect(send(protocol)).resolves.toBeUndefined()
        expect(find).toHaveBeenCalledTimes(2)
        expect(protocol.coordinator).toBe(h.cluster.brokerPool.brokers[1])
        expect(protocol.coordinator).not.toBe(h.old)
        expect(protocol.coordinator.isConnected()).toBe(true)
        expect(h.seam.requests().filter((request) => request.api === api)).toHaveLength(2)
      } finally {
        jest.restoreAllMocks()
        await h.close()
      }
    }
  )

  describe.each(['tv1', 'tv2'])('producer profile %s', (profile) => {
    it.each([
      [false, 'replacement'],
      [false, 'reconnect'],
      [true, 'replacement'],
      [true, 'reconnect'],
    ])(
      'idempotent=%s delayed removal preserves %s, settles and never writes to the old socket',
      async (idempotent, kind) => {
        const h = await createBoundHarness({
          enforceRequestTimeout: true,
          profile,
          maxInFlightRequests: 5,
        })
        const reactions = []
        const original = h.cluster.removeBroker.bind(h.cluster)
        const remove = jest
          .spyOn(h.cluster, 'removeBroker')
          .mockImplementation((destination) => reactions.push(destination))
        const producer = createProducer({
          cluster: h.cluster,
          logger: newLogger(),
          idempotent,
          retry: RETRY,
          linger: 0,
          instrumentationEmitter: new InstrumentationEventEmitter(),
        })
        let gate
        let refresh
        let releaseRecovery = () => {}
        try {
          h.seam.answer('Metadata', () => metadata(1)).answer('Produce', produced)
          await h.cluster.refreshMetadata()
          await producer.connect()
          await h.cluster.addTargetTopic(TOPIC)
          const refreshMetadata = h.cluster.refreshMetadata.bind(h.cluster)
          const recovery = new Promise((resolve) => {
            releaseRecovery = resolve
          })
          refresh = jest.spyOn(h.cluster, 'refreshMetadata').mockImplementation(async (...args) => {
            await recovery
            return refreshMetadata(...args)
          })
          const generation = h.old.connectionPool.generation
          const poison = h.hold({ api: 'Metadata', node: 1 })

          const timed = outcome(h.old.metadata([]))
          await poison.arrived()
          gate = h.hold({ api: 'Produce', node: 1 })

          const sending = outcome(
            producer.send({
              topic: TOPIC,
              acks: -1,
              timeout: 1000,
              messages: [{ partition: 0, key: 'key', value: 'first' }],
            })
          )
          await gate.arrived()
          const writes = h.socket.writes.length
          await jest.advanceTimersByTimeAsync(BOUND * 2)
          expect((await timed).error).toBeInstanceOf(KafkaJSRequestTimeoutError)
          await until(() => reactions.length > 0)
          expect(reactions[0].source).toMatchObject({ broker: h.old, generation })
          if (kind === 'replacement') {
            h.seam.answer('Metadata', () => metadata(1, NODES[2]))
            await h.replace('hint')
          }
          const current = h.cluster.brokerPool.brokers[1]
          await current.connect()
          const socket = current.connectionPool.pool[0].socket
          const nextGeneration = current.connectionPool.generation
          if (kind === 'reconnect') expect(nextGeneration).toBeGreaterThan(generation)
          for (const destination of reactions.splice(0)) original(destination)
          releaseRecovery()
          expect(h.cluster.brokerPool.brokers[1]).toBe(current)
          expect(socket.destroyed).toBe(false)
          gate.release()
          for (let attempt = 0; attempt < 20; attempt++) {
            await jest.advanceTimersByTimeAsync(20)
            await new Promise(setImmediate)
          }
          expect((await sending).error).toBeUndefined()
          expect(h.socket.writes).toHaveLength(writes)
          expect(current.connectionPool.pool[0].socket).toBe(socket)
          expect(h.cluster.brokerPool.brokers[1]).toBe(current)
          await expect(current.metadata([])).resolves.toBeDefined()
          expect(
            h.seam.requests().filter((r) => r.api === 'Produce').length
          ).toBeGreaterThanOrEqual(2)
        } finally {
          releaseRecovery()
          gate?.release()
          refresh?.mockRestore()
          remove.mockRestore()
          await h.close([producer])
        }
      }
    )

    it('idempotent batches retry before concurrent new work in per-partition sequence order', async () => {
      const h = await createBoundHarness({
        enforceRequestTimeout: true,
        profile,
        maxInFlightRequests: 5,
      })
      const producer = createProducer({
        cluster: h.cluster,
        logger: newLogger(),
        idempotent: true,
        retry: RETRY,
        linger: 0,
        batchSize: 1,
        instrumentationEmitter: new InstrumentationEventEmitter(),
      })
      try {
        h.seam.answer('Metadata', () => metadata(1)).answer('Produce', produced)
        await h.cluster.refreshMetadata()
        await producer.connect()
        await h.cluster.addTargetTopic(TOPIC)
        const poison = h.hold({ api: 'Metadata', node: 1 })

        const timed = outcome(h.old.metadata([]))
        await poison.arrived()
        const firstGate = h.hold({ api: 'Produce', node: 1 })

        const first = outcome(
          producer.send({
            topic: TOPIC,
            acks: -1,
            timeout: 1000,
            messages: [{ partition: 0, key: 'a', value: 'A', timestamp: '1' }],
          })
        )
        await firstGate.arrived()

        const second = outcome(
          producer.send({
            topic: TOPIC,
            acks: -1,
            timeout: 1000,
            messages: [{ partition: 0, key: 'b', value: 'B', timestamp: '2' }],
          })
        )
        for (let turn = 0; turn < 4; turn++) await new Promise(setImmediate)
        expect(h.seam.requests().filter((request) => request.api === 'Produce')).toHaveLength(1)
        const oldWrites = h.socket.writes.length
        await jest.advanceTimersByTimeAsync(BOUND * 2)
        expect((await timed).error).toBeInstanceOf(KafkaJSRequestTimeoutError)
        const third = outcome(
          producer.send({
            topic: TOPIC,
            acks: -1,
            timeout: 1000,
            messages: [{ partition: 0, key: 'c', value: 'C', timestamp: '3' }],
          })
        )
        firstGate.release()
        for (let turn = 0; turn < 30; turn++) {
          await jest.advanceTimersByTimeAsync(20)
          await new Promise(setImmediate)
        }
        for (const result of await Promise.all([first, second, third]))
          expect(result.error).toBeUndefined()
        const actual = h.seam.requests().filter((r) => r.apiKey === apiKeys.Produce)
        expect(actual).toHaveLength(4)
        expect(new Set(actual.map((r) => r.generation)).size).toBeGreaterThanOrEqual(2)
        expect(h.socket.writes).toHaveLength(oldWrites)
        // The wire-order oracle reads request-reader records, including pid/epoch/sequence/payload identity.
        const order = WireOrder.of(h.seam)
        expect(order.produced().map(({ sequence }) => sequence)).toEqual([0, 0, 1, 2])
        order.assertSequencesInOrder()
        order.assertNoRewrite()
        order.assertNoStaleGeneration()
        order.assertTransmitted(
          [0, 1, 2].map((sequence) => ({ topic: TOPIC, partition: 0, sequence }))
        )
      } finally {
        await h.close([producer])
      }
    })
  })
})
