const ClientTelemetry = require('../client')
const TelemetryMetrics = require('../metrics')
const ConnectionPool = require('../../network/connectionPool')
const Encoder = require('../../protocol/encoder')
const {
  newLogger,
  createFakeSocketFactory,
  readRequestHeader,
  respondWithFrame,
} = require('testHelpers')
const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const subscription = (fields = {}) => ({
  clientInstanceId: ID,
  subscriptionId: -7,
  acceptedCompressionTypes: [0],
  pushIntervalMs: 1000,
  telemetryMaxBytes: 1048576,
  deltaTemporality: true,
  requestedMetrics: ['*'],
  ...fields,
})
const failure = (code, extra = {}) =>
  Object.assign(new Error('private diagnostic'), { code, clientSideThrottleTime: 0, ...extra })
/** @param {object} options deterministic controls @returns {object} real collector and finite transport */
const setup = (options = {}) => {
  const clock = {
    now: () => jest.now(),
    random: () => options.random ?? 0.5,
    setTimeout,
    clearTimeout,
  }
  const metrics = new TelemetryMetrics({
    kind: 'producer',
    clock: { wallMs: () => 1000, monotonicNs: () => BigInt(jest.now()) * 1000000n },
  })
  const events = []
  const gets = []
  const pushes = []
  const handles = []
  let candidates = options.candidates ?? [
    {
      nodeId: 1,
      host: 'a',
      port: 9092,
      endpointRevision: 1n,
      versions: { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } },
    },
  ]
  const transport = {
    candidates: () => candidates,
    open: jest.fn(async (candidate, { guard, onAllocated }) => {
      const handle = {
        retired: false,
        isConnected: () => !handle.retired,
        get: jest.fn(async (payload, authority) => {
          const error = authority.guard()
          if (error) throw error
          events.push({ api: 71, ...payload })
          const answer = gets.shift()
          return typeof answer === 'function'
            ? answer(payload, authority)
            : (answer ?? subscription())
        }),
        push: jest.fn(async (payload, authority) => {
          const error = authority.guard()
          if (error) throw error
          authority.onWrite()
          events.push({ api: 72, ...payload })
          const answer = pushes.shift()
          return typeof answer === 'function'
            ? answer(payload, authority)
            : (answer ?? { errorCode: 0, clientSideThrottleTime: 0 })
        }),
        retire: jest.fn(async () => {
          handle.retired = true
        }),
      }
      handles.push(handle)
      onAllocated(handle)
      const refusal = guard()
      if (refusal) throw refusal
      return handle
    }),
  }
  const client = new ClientTelemetry({
    kind: 'producer',
    metrics,
    transport,
    clock,
    logger: options.logger,
    config: { enableMetricsPush: true, ...options.config },
  })
  return {
    client,
    metrics,
    transport,
    events,
    gets,
    pushes,
    handles,
    setCandidates: (value) => {
      candidates = value
    },
  }
}
beforeEach(() => {
  jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick'] })
})
afterEach(() => {
  jest.useRealTimers()
})

describe('both negotiated telemetry APIs and runtime handshake', () => {
  it.each([
    {},
    { 71: { minVersion: 0, maxVersion: 0 } },
    { 72: { minVersion: 0, maxVersion: 0 } },
    { 71: { minVersion: 1, maxVersion: 1 }, 72: { minVersion: 0, maxVersion: 0 } },
  ])('leaves incompatible advertisements dormant: %j', async (versions) => {
    const h = setup({
      candidates: [{ nodeId: 1, host: 'a', port: 1, endpointRevision: 1n, versions }],
    })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.client.state).toBe('DORMANT')
    expect(h.transport.open).not.toHaveBeenCalled()
    expect(h.events).toEqual([])
    await expect(h.client.clientInstanceId()).rejects.toMatchObject({ reason: 'unsupported' })
    h.client.suspend()
    expect(jest.getTimerCount()).toBe(0)
  })
  it('reconsiders only a newly supplied ready advertisement', async () => {
    const h = setup({ candidates: [] })
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    h.setCandidates([
      {
        host: 'b',
        port: 2,
        nodeId: 2,
        endpointRevision: 2n,
        versions: { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } },
      },
    ])
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.events).toEqual([{ api: 71, clientInstanceId: null }])
    expect(h.client.uuid).toBe(ID)
    h.client.suspend()
  })
  it('distinguishes runtime refusal from a successful empty resource selection', async () => {
    const h = setup()
    h.gets.push(() => Promise.reject(failure(35)))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.client.state).toBe('DORMANT')
    expect(h.transport.open).toHaveBeenCalledTimes(1)
    h.setCandidates([
      {
        host: 'a',
        port: 9092,
        nodeId: 1,
        endpointRevision: 2n,
        versions: { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } },
      },
    ])
    h.gets.push(subscription({ requestedMetrics: [] }))
    h.client.resume()
    await jest.advanceTimersByTimeAsync(0)
    expect(h.client.state).toBe('POLL')
    await expect(h.client.clientInstanceId()).resolves.toBe(ID)
    expect(h.events.filter((e) => e.api === 72)).toEqual([])
    h.client.suspend()
  })
  it('preserves foreground Connection invocation and result/error identities under a throwing telemetry logger', async () => {
    const value = {}
    const sentinel = new Error('foreground')
    const h = setup({
      logger: {
        warn: () => {
          throw Error('logger')
        },
      },
    })
    const fake = createFakeSocketFactory()
    const pool = new ConnectionPool({
      host: 'a',
      port: 9092,
      logger: newLogger(),
      clientId: 'foreground',
      socketFactory: fake.factory,
      connectionTimeout: 1000,
      requestTimeout: 5000,
      enforceRequestTimeout: false,
      maxInFlightRequests: 2,
      connectionsMaxIdle: 0,
      drainTimeout: 0,
    })
    const connection = await pool.getConnection(0)
    const encode = jest.fn(async () => new Encoder())
    const success = jest.fn(() => value)
    const reject = jest.fn(() => {
      throw sentinel
    })
    const invoke = (parse) =>
      connection
        .send({
          request: { apiKey: 3, apiVersion: 0, apiName: 'Metadata', encode },
          response: { decode: async (raw) => raw, parse },
        })
        .catch((error) => error)
    const a = invoke(success)
    const b = invoke(reject)
    try {
      await new Promise((resolve) => setImmediate(resolve))
      expect(fake.state.writes).toHaveLength(2)
      h.gets.push(() => Promise.reject(failure(42)))
      h.client.resume()
      await jest.advanceTimersByTimeAsync(0)
      expect(h.client.state).toBe('STOPPED')
      expect(connection.isConnected()).toBe(true)
      for (const frame of fake.state.writes)
        respondWithFrame(fake.state, readRequestHeader(frame).correlationId, 0, Buffer.alloc(0))
      expect(await a).toBe(value)
      expect(await b).toBe(sentinel)
      expect(encode).toHaveBeenCalledTimes(2)
      expect(success).toHaveBeenCalledTimes(1)
      expect(reject).toHaveBeenCalledTimes(1)
      expect(connection.requestQueue.isIdle()).toBe(true)
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      h.client.suspend()
      await pool.destroy({ drainTimeout: 0 })
      await Promise.all([a, b])
    }
  })
})
