const Connection = require('../connection')
const SocketRequest = require('../requestQueue/socketRequest')
const TelemetryMetrics = require('../../telemetry/metrics')
const Encoder = require('../../protocol/encoder')
const {
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
  newLogger,
} = require('testHelpers')

/** @returns {Promise<void>} one event-loop turn; no timer sleep or real broker */
const turn = () => new Promise((resolve) => setImmediate(resolve))

/**
 * @param {object|null} telemetrySink trusted private numeric sink
 * @param {object|null} telemetrySlot pre-issued slot
 * @returns {Promise<object>} connected fake-socket Connection with captured writes
 */
const connected = async (telemetrySink = null, telemetrySlot = null) => {
  const fake = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: fake.factory,
    connectionTimeout: 1000,
    requestTimeout: 1000,
    telemetrySink,
    telemetrySlot,
  })
  await connection.connect()
  return { ...fake, connection }
}

/**
 * @param {object} result exact parse result @param {Function} [encode] optional held encoder
 * @returns {object} minimal fake-frame request using the real Connection path
 */
const protocol = (result, encode = async () => new Encoder()) => ({
  request: { apiKey: 3, apiName: 'Metadata', apiVersion: 0, encode },
  response: { decode: async () => ({}), parse: async () => result },
})

/** @param {object} fake owned fake connection @returns {void} literal correlated response */
const respond = (fake) =>
  respondWithFrame(
    fake.state,
    readRequestHeader(fake.state.writes.at(-1)).correlationId,
    0,
    Buffer.from([0])
  )

describe('normal transport metric observation', () => {
  test('physical connects count once while logical no-op calls add nothing', async () => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    const s = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
    metrics.select(s)
    const fake = await connected(metrics)
    try {
      await fake.connection.connect()
      await fake.connection.connect()
      expect(fake.state.sockets).toHaveLength(1)
      expect(metrics.snapshot(s).points.map((p) => p.value)).toEqual([1n])
    } finally {
      await fake.connection.disconnect({ drainTimeout: 0 })
    }
  })

  test('two sockets for the same node contribute two physical establishments', async () => {
    const metrics = new TelemetryMetrics({ kind: 'consumer' })
    const a = await connected(metrics)
    const b = await connected(metrics)
    try {
      const s = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
      metrics.select(s)
      expect(metrics.snapshot(s).points.map((p) => p.value)).toEqual([2n])
    } finally {
      await a.connection.disconnect({ drainTimeout: 0 })
      await b.connection.disconnect({ drainTimeout: 0 })
    }
  })

  test('synchronous socket callback captures attempt identity before dialing', async () => {
    const { EventEmitter } = require('events')
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    const socket = new EventEmitter()
    Object.assign(socket, {
      write: () => true,
      end: () => {},
      unref: () => {},
      destroy: () => {
        socket.destroyed = true
      },
    })
    const connection = new Connection({
      host: 'node',
      port: 9092,
      logger: newLogger(),
      connectionTimeout: 1000,
      requestTimeout: 1000,
      telemetrySink: metrics,
      socketFactory: ({ onConnect }) => {
        onConnect()
        onConnect()
        return socket
      },
    })
    try {
      await connection.connect()
      const s = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
      metrics.select(s)
      expect(metrics.snapshot(s).points[0].value).toBe(1n)
    } finally {
      await connection.disconnect({ drainTimeout: 0 })
    }
  })

  test('request captures its slot before a held encoder and endpoint replacement', async () => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    const s = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
    metrics.select(s)
    const old = metrics.bindNode({ nodeId: 1, endpoint: 'same', endpointRevision: 1 })
    const fake = await connected(metrics, old)
    let release
    const gate = new Promise((resolve) => {
      release = resolve
    })
    const value = { exact: true }
    const sent = fake.connection.send(
      protocol(value, async () => {
        await gate
        return new Encoder()
      })
    )
    try {
      const current = metrics.bindNode({ nodeId: 2, endpoint: 'same', endpointRevision: 2 })
      fake.connection.setTelemetryObserver(metrics, current)
      release()
      await turn()
      respond(fake)
      expect(await sent).toBe(value)
      const stale = metrics.snapshot(s)
      expect(stale.points.filter((p) => p.type === 'gauge')).toEqual([])
      metrics.abandon(stale)
      const positive = fake.connection.send(protocol(value))
      await turn()
      respond(fake)
      expect(await positive).toBe(value)
      const sample = metrics.snapshot(s)
      expect(sample.points.filter((p) => p.type === 'gauge').map((p) => p.attributes)).toEqual([
        [{ key: 'node_id', value: '2' }],
        [{ key: 'node_id', value: '2' }],
      ])
    } finally {
      release()
      await sent.catch(() => {})
      await fake.connection.disconnect({ drainTimeout: 0 })
    }
  })

  test('collector failure preserves success and rejection identity with one invocation', async () => {
    const secret = {
      get message() {
        throw new Error('must not inspect')
      },
    }
    const sink = {
      observeConnection: () => {
        throw secret
      },
      observeRequest: jest.fn(() => {
        throw secret
      }),
    }
    const fake = await connected(sink, {})
    const value = { value: true }
    const error = new Error('original')
    try {
      const success = fake.connection.send(protocol(value))
      await turn()
      respond(fake)
      expect(await success).toBe(value)
      const request = protocol(null)
      request.response.parse = jest.fn(async () => {
        throw error
      })
      const failure = fake.connection.send({ ...request, logResponseError: false }).catch((e) => e)
      await turn()
      respond(fake)
      expect(await failure).toBe(error)
      expect(request.response.parse).toHaveBeenCalledTimes(1)
      expect(sink.observeRequest).toHaveBeenCalledTimes(2)
    } finally {
      await fake.connection.disconnect({ drainTimeout: 0 })
    }
  })

  test('null sink excludes dedicated telemetry traffic and leaves REQUEST payload unchanged', async () => {
    const fake = await connected()
    const events = []
    fake.connection.requestQueue.instrumentationEmitter = {
      emit: (name, payload) => {
        if (name === 'network.request') events.push(payload)
      },
    }
    const value = {}
    try {
      const sent = fake.connection.send(protocol(value))
      await turn()
      respond(fake)
      expect(await sent).toBe(value)
      expect(events).toHaveLength(1)
      expect(Object.keys(events[0]).sort()).toEqual(
        [
          'apiKey',
          'apiName',
          'apiVersion',
          'broker',
          'clientId',
          'correlationId',
          'createdAt',
          'duration',
          'pendingDuration',
          'sentAt',
          'size',
        ].sort()
      )
    } finally {
      await fake.connection.disconnect({ drainTimeout: 0 })
    }
  })

  test('existing completion RTT excludes pending time and rejects do not observe', () => {
    let time = 0
    const now = jest.spyOn(Date, 'now').mockImplementation(() => time)
    const sink = { observeRequest: jest.fn() }
    const value = {}
    const entry = { correlationId: 1, resolve: jest.fn(), reject: jest.fn() }
    const request = new SocketRequest({
      requestTimeout: 1000,
      broker: 'node',
      clientId: 'client',
      entry,
      expectResponse: true,
      send: () => {},
      timeout: () => {},
      telemetrySink: sink,
      telemetrySlot: value,
    })
    try {
      time = 100
      request.send()
      time = 107
      request.completed({ size: 1, payload: Buffer.from([0]) })
      expect(request.pendingDuration).toBe(100)
      expect(sink.observeRequest).toHaveBeenCalledWith(value, 7)
      const rejected = new SocketRequest({
        entry,
        expectResponse: true,
        telemetrySink: sink,
        telemetrySlot: value,
      })
      const failure = new Error('original')
      rejected.rejected(failure)
      expect(entry.reject).toHaveBeenCalledWith(failure)
      expect(sink.observeRequest).toHaveBeenCalledTimes(1)
    } finally {
      now.mockRestore()
    }
  })

  test('no-response completion and explicit timeout do not fabricate response RTT', () => {
    const sink = { observeRequest: jest.fn() }
    const entry = { correlationId: 1, apiName: 'Produce', resolve: jest.fn(), reject: jest.fn() }
    const options = {
      entry,
      send: () => {},
      timeout: () => {},
      requestTimeout: 1,
      telemetrySink: sink,
      telemetrySlot: {},
    }
    const noResponse = new SocketRequest({ ...options, expectResponse: false })
    noResponse.send()
    noResponse.completed({ size: 0, payload: null })
    const timedOut = new SocketRequest({ ...options, expectResponse: true })
    timedOut.send()
    timedOut.timeoutRequest()
    expect(sink.observeRequest).not.toHaveBeenCalled()
    expect(entry.reject).toHaveBeenCalledTimes(1)
  })
})
