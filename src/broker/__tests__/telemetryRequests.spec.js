const Broker = require('../index')
const ConnectionPool = require('../../network/connectionPool')
const Encoder = require('../../protocol/encoder')
const Long = require('../../utils/long')
const { runWithDeadline, currentDeadline } = require('../../admin/deadline')
const ClientTelemetry = require('../../telemetry/client')
const TelemetryMetrics = require('../../telemetry/metrics')
const { lookup } = require('../../protocol/requests')
const {
  newLogger,
  createFakeSocketFactory,
  readRequestHeader,
  respondWithFrame,
} = require('testHelpers')
const getPair = require('../../../testHelpers/wire/getTelemetrySubscriptionsV0')
const pushPair = require('../../../testHelpers/wire/pushTelemetryV0')
const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
/** @returns {Promise<void>} finite event-loop turn, no real broker */
const turn = () => new Promise((resolve) => setImmediate(resolve))
/** @returns {Promise<object>} real Connection/queue behind the actual Broker methods */
const setup = async (options = {}) => {
  const fake = createFakeSocketFactory()
  const pool = new ConnectionPool({
    host: 'a',
    port: 9092,
    logger: newLogger(),
    clientId: 'client',
    socketFactory: fake.factory,
    connectionTimeout: 1000,
    requestTimeout: 5000,
    enforceRequestTimeout: true,
    maxInFlightRequests: 1,
    drainTimeout: 0,
    connectionsMaxIdle: 0,
    ...options,
  })
  const connection = await pool.getConnection(0)
  const broker = new Broker({ connectionPool: pool, logger: newLogger() })
  broker.versions = { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } }
  broker.versionsGeneration = pool.generation
  broker.lookupRequest = lookup(broker.versions)
  return { fake, pool, broker, connection }
}
describe('dedicated broker telemetry requests', () => {
  it.each([
    [71, 'getTelemetrySubscriptions', getPair, { clientInstanceId: null }],
    [
      72,
      'pushTelemetry',
      pushPair,
      {
        clientInstanceId: ID,
        subscriptionId: -7,
        terminating: false,
        compressionType: 0,
        metrics: Buffer.alloc(0),
      },
    ],
  ])(
    'writes API%i through the real queue and independent pair',
    async (key, method, pair, payload) => {
      const h = await setup()
      const onWrite = jest.fn()
      try {
        const pending = h.broker[method](payload, { guard: () => null, onWrite })
        await turn()
        expect(h.fake.state.writes).toHaveLength(1)
        const frame = h.fake.state.writes[0]
        const header = readRequestHeader(frame)
        expect(header).toMatchObject({
          apiKey: key,
          apiVersion: 0,
          correlationId: 0,
          clientId: 'client',
        })
        const fields = pair.readRequest(frame.subarray(header.byteLength))
        expect(fields.clientInstanceId).toBe(payload.clientInstanceId)
        if (key === 72)
          expect(fields).toMatchObject({
            subscriptionId: -7,
            terminating: false,
            compressionType: 0,
            metrics: '',
          })
        expect(onWrite).toHaveBeenCalledTimes(1)
        respondWithFrame(h.fake.state, header.correlationId, 1, pair.buildResponse())
        await expect(pending).resolves.toMatchObject({ errorCode: 0 })
        expect(h.fake.state.sockets).toHaveLength(1)
        expect(h.pool.pool[1].isConnected()).toBe(false)
      } finally {
        await h.pool.destroy({ drainTimeout: 0 })
      }
    }
  )
  it('writes the codec byte and body of a compressed push for the independent reader', async () => {
    const h = await setup()
    try {
      const pending = h.broker.pushTelemetry(
        {
          clientInstanceId: ID,
          subscriptionId: 3,
          terminating: false,
          compressionType: 1,
          metrics: Buffer.from([1, 2, 3]),
        },
        { guard: () => null }
      )
      await turn()
      expect(h.fake.state.writes).toHaveLength(1)
      const frame = h.fake.state.writes[0]
      const header = readRequestHeader(frame)
      expect(header).toMatchObject({ apiKey: 72, apiVersion: 0, correlationId: 0 })
      expect(pushPair.readRequest(frame.subarray(header.byteLength))).toEqual({
        clientInstanceId: ID,
        subscriptionId: 3,
        terminating: false,
        compressionType: 1,
        metrics: '010203',
      })
      respondWithFrame(h.fake.state, header.correlationId, 1, pushPair.buildResponse())
      await expect(pending).resolves.toMatchObject({ errorCode: 0 })
      expect(h.fake.state.sockets).toHaveLength(1)
    } finally {
      await h.pool.destroy({ drainTimeout: 0 })
    }
  })
  it('refuses before write and before transmission commit when authority expires', async () => {
    const h = await setup()
    const sentinel = new Error('revoked')
    const onWrite = jest.fn()
    try {
      await expect(h.broker.pushTelemetry({}, { guard: () => sentinel, onWrite })).rejects.toBe(
        sentinel
      )
      expect(h.fake.state.writes).toEqual([])
      expect(onWrite).not.toHaveBeenCalled()
    } finally {
      await h.pool.destroy({ drainTimeout: 0 })
    }
  })
  it('checks authority again when a queued push becomes eligible', async () => {
    const h = await setup()
    let refused = false
    const onWrite = jest.fn()
    const sentinel = new Error('revoked')
    try {
      const first = h.broker.getTelemetrySubscriptions(
        { clientInstanceId: null },
        { guard: () => null }
      )
      await turn()
      const second = h.broker.pushTelemetry(
        {
          clientInstanceId: ID,
          subscriptionId: 0,
          terminating: false,
          compressionType: 0,
          metrics: Buffer.alloc(0),
        },
        { guard: () => (refused ? sentinel : null), onWrite }
      )
      const observed = second.catch((e) => e)
      await turn()
      expect(h.fake.state.writes).toHaveLength(1)
      refused = true
      respondWithFrame(
        h.fake.state,
        readRequestHeader(h.fake.state.writes[0]).correlationId,
        1,
        getPair.buildResponse()
      )
      await first
      expect(await observed).toBe(sentinel)
      expect(h.fake.state.writes).toHaveLength(1)
      expect(onWrite).not.toHaveBeenCalled()
    } finally {
      await h.pool.destroy({ drainTimeout: 0 })
    }
  })
  it('exposes numeric quota before parse and retires no foreground pool', async () => {
    const h = await setup()
    try {
      const pending = h.broker.getTelemetrySubscriptions(
        { clientInstanceId: null },
        { guard: () => null }
      )
      const observed = pending.catch((e) => e)
      await turn()
      respondWithFrame(
        h.fake.state,
        0,
        1,
        getPair.buildResponse({ errorCode: 89, throttleTime: 7 })
      )
      expect(await observed).toMatchObject({ code: 89, clientSideThrottleTime: 7 })
      expect(h.pool.retired).toBe(false)
    } finally {
      await h.pool.destroy({ drainTimeout: 0 })
    }
  })
})

it('never begins authentication for a terminating push', async () => {
  const h = await setup()
  const authenticate = jest.fn(async () => {})
  h.connection.sasl = { mechanism: 'PLAIN', authenticationProvider: () => ({ authenticate }) }
  h.connection.versions = {
    17: { minVersion: 1, maxVersion: 1 },
    36: { minVersion: 0, maxVersion: 0 },
  }
  const onWrite = jest.fn()
  const observed = h.broker
    .pushTelemetry(
      {
        clientInstanceId: ID,
        subscriptionId: 1,
        terminating: true,
        compressionType: 0,
        metrics: Buffer.alloc(0),
      },
      { guard: () => null, onWrite }
    )
    .catch((error) => error)
  try {
    await turn()
    const keys = h.fake.state.writes.map((frame) => readRequestHeader(frame).apiKey)
    expect(keys.filter((key) => key === 17 || key === 36)).toEqual([])
    expect(await observed).toBeInstanceOf(Error)
    expect(authenticate).not.toHaveBeenCalled()
    expect(onWrite).not.toHaveBeenCalled()
  } finally {
    await h.pool.destroy({ drainTimeout: 0 })
    await observed
  }
})

it('writes a final on an authenticated socket and commits exactly once', async () => {
  const h = await setup()
  const authenticate = jest.fn(async () => {})
  h.connection.sasl = { mechanism: 'PLAIN', authenticationProvider: () => ({ authenticate }) }
  h.connection.authenticatedAt = process.hrtime()
  const onWrite = jest.fn()
  const pending = h.broker.pushTelemetry(
    {
      clientInstanceId: ID,
      subscriptionId: 1,
      terminating: true,
      compressionType: 0,
      metrics: Buffer.alloc(0),
    },
    { guard: () => null, onWrite }
  )
  try {
    await turn()
    expect(h.fake.state.writes.map((frame) => readRequestHeader(frame).apiKey)).toEqual([72])
    const frame = h.fake.state.writes[0]
    const header = readRequestHeader(frame)
    expect(pushPair.readRequest(frame.subarray(header.byteLength)).terminating).toBe(true)
    expect(onWrite).toHaveBeenCalledTimes(1)
    respondWithFrame(h.fake.state, header.correlationId, 1, pushPair.buildResponse())
    await expect(pending).resolves.toMatchObject({ errorCode: 0 })
    expect(authenticate).not.toHaveBeenCalled()
    expect(onWrite).toHaveBeenCalledTimes(1)
    expect(h.fake.state.sockets).toHaveLength(1)
  } finally {
    await h.pool.destroy({ drainTimeout: 0 })
  }
})

it('abandons a queued final if authentication becomes due before transmission', async () => {
  const h = await setup()
  const authenticate = jest.fn(async () => {})
  h.connection.sasl = { mechanism: 'PLAIN', authenticationProvider: () => ({ authenticate }) }
  h.connection.authenticatedAt = process.hrtime()
  const first = h.broker.getTelemetrySubscriptions(
    { clientInstanceId: null },
    { guard: () => null }
  )
  const firstObserved = first.catch((error) => error)
  const onWrite = jest.fn()
  let final
  try {
    await turn()
    final = h.broker
      .pushTelemetry(
        {
          clientInstanceId: ID,
          subscriptionId: 1,
          terminating: true,
          compressionType: 0,
          metrics: Buffer.alloc(0),
        },
        { guard: () => null, onWrite }
      )
      .catch((error) => error)
    await turn()
    expect(h.fake.state.writes).toHaveLength(1)
    expect(h.connection.requestQueue.pending.length).toBe(1)
    h.connection.authenticatedAt = null
    respondWithFrame(h.fake.state, 0, 1, getPair.buildResponse())
    expect(await firstObserved).toMatchObject({ errorCode: 0 })
    expect(await final).toBeInstanceOf(Error)
    expect(h.fake.state.writes.map((frame) => readRequestHeader(frame).apiKey)).toEqual([71])
    expect(authenticate).not.toHaveBeenCalled()
    expect(onWrite).not.toHaveBeenCalled()
  } finally {
    await h.pool.destroy({ drainTimeout: 0 })
    await Promise.all([firstObserved, final])
  }
})

it.each([false, true])(
  'enforces established-only preparation with held encoder=%s',
  async (held) => {
    const h = await setup()
    h.connection.sasl = { mechanism: 'PLAIN' }
    if (held) h.connection.authenticatedAt = process.hrtime()
    let release
    const gate = new Promise((resolve) => {
      release = resolve
    })
    const { requests } = require('../../protocol/requests')
    const protocol = h.broker.lookupRequest(
      72,
      requests.PushTelemetry
    )({
      clientInstanceId: ID,
      subscriptionId: 1,
      terminating: true,
      compressionType: 0,
      metrics: Buffer.alloc(0),
    })
    const encode = protocol.request.encode
    protocol.request.encode = async () => {
      await gate
      return encode()
    }
    const onWrite = jest.fn()
    const observed = h.connection
      .send({ ...protocol, establishedOnly: true, onWrite })
      .catch((error) => error)
    try {
      await turn()
      h.connection.authenticatedAt = null
      release()
      expect(await observed).toBeInstanceOf(Error)
      expect(h.fake.state.writes).toEqual([])
      expect(onWrite).not.toHaveBeenCalled()
    } finally {
      release()
      await h.pool.destroy({ drainTimeout: 0 })
      await observed
    }
  }
)

it.each([false, true])(
  'keeps foreground SASL renewal shared when caller A expires=%s and caller B remains live',
  async (expire) => {
    const h = await setup({ enforceRequestTimeout: false, maxInFlightRequests: 2 })
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick'] })
    let release
    const held = new Promise((resolve) => {
      release = resolve
    })
    const authenticate = jest.fn(() => held)
    h.connection.sasl = { mechanism: 'PLAIN', authenticationProvider: () => ({ authenticate }) }
    h.connection.versions = {
      17: { minVersion: 1, maxVersion: 1 },
      36: { minVersion: 0, maxVersion: 0 },
    }
    h.connection.authenticatedAt = process.hrtime()
    h.connection.sessionLifetime = Long.fromValue(1)
    const resultA = {}
    const resultB = {}
    const parseA = jest.fn(() => resultA)
    const parseB = jest.fn(() => resultB)
    const invoke = (name, budget, parse) =>
      runWithDeadline(name, budget, () =>
        h.connection.send({
          request: {
            apiKey: 3,
            apiVersion: 0,
            apiName: 'Metadata',
            encode: async () => new Encoder(),
          },
          response: { decode: async (raw) => raw, parse },
          guard: currentDeadline().guard,
        })
      ).catch((error) => error)
    const a = invoke('A', 100, parseA)
    let b
    try {
      await turn()
      const header = readRequestHeader(h.fake.state.writes[0])
      expect(header.apiKey).toBe(17)
      respondWithFrame(
        h.fake.state,
        header.correlationId,
        0,
        Buffer.from([0, 0, 0, 0, 0, 1, 0, 5, 80, 76, 65, 73, 78])
      )
      await turn()
      expect(authenticate).toHaveBeenCalledTimes(1)
      b = invoke('B', 1000, parseB)
      await turn()
      const episode = h.connection.authEpisode
      if (expire) {
        await jest.advanceTimersByTimeAsync(100)
        expect(await a).toMatchObject({ name: 'KafkaJSTimeout' })
      }
      release()
      await turn()
      const requests = h.fake.state.writes.filter((frame) => readRequestHeader(frame).apiKey === 3)
      expect(requests).toHaveLength(expire ? 1 : 2)
      for (const frame of requests)
        respondWithFrame(h.fake.state, readRequestHeader(frame).correlationId, 0, Buffer.alloc(0))
      expect(await b).toBe(resultB)
      expect(parseB).toHaveBeenCalledTimes(1)
      if (expire) expect(parseA).not.toHaveBeenCalled()
      else {
        expect(await a).toBe(resultA)
        expect(parseA).toHaveBeenCalledTimes(1)
      }
      expect(episode).not.toBeNull()
      expect(authenticate).toHaveBeenCalledTimes(1)
      expect(h.connection.authenticatedAt).not.toBeNull()
      expect(h.connection.authenticationOwner).toBeNull()
      expect(h.connection.telemetrySink).toBeNull()
      expect(h.fake.state.sockets).toHaveLength(1)
      expect(h.connection.requestQueue.isIdle()).toBe(true)
    } finally {
      release()
      await h.pool.destroy({ drainTimeout: 0 })
      await Promise.all([a, b])
      jest.useRealTimers()
    }
  }
)

it.each(
  ['throttle', 'response'].flatMap((dispatch) =>
    ['refusal', 'throw', 'valid'].map((outcome) => [dispatch, outcome])
  )
)(
  'settles a queued final on %s dispatch with a %s commit without escaping the callback',
  async (dispatch, outcome) => {
    const h = await setup({ enforceRequestTimeout: false })
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick'] })
    const sentinel = new Error('commit refused')
    let expired = false
    const guard = () => (expired ? sentinel : null)
    const onWrite = jest.fn(() => {
      if (outcome === 'throw') throw sentinel
      if (outcome === 'refusal') {
        expired = true
        const refusal = guard()
        if (refusal) throw refusal
      }
    })
    let first
    let final
    try {
      if (dispatch === 'throttle') h.connection.requestQueue.maybeThrottle(100)
      else {
        first = h.broker
          .getTelemetrySubscriptions({ clientInstanceId: null }, { guard: () => null })
          .catch((error) => error)
        await turn()
      }
      final = h.broker
        .pushTelemetry(
          {
            clientInstanceId: ID,
            subscriptionId: 1,
            terminating: true,
            compressionType: 0,
            metrics: Buffer.alloc(0),
          },
          { guard, onWrite }
        )
        .catch((error) => error)
      await turn()
      expect(h.connection.requestQueue.pending).toHaveLength(1)
      if (dispatch === 'throttle') await jest.advanceTimersByTimeAsync(100)
      else {
        respondWithFrame(h.fake.state, 0, 1, getPair.buildResponse({ throttleTime: 0 }))
        expect(await first).toMatchObject({ errorCode: 0 })
        await turn()
      }
      expect(onWrite).toHaveBeenCalledTimes(1)
      const frames = h.fake.state.writes.filter((frame) => readRequestHeader(frame).apiKey === 72)
      expect(frames).toHaveLength(outcome === 'valid' ? 1 : 0)
      if (outcome === 'valid') {
        const header = readRequestHeader(frames[0])
        expect(pushPair.readRequest(frames[0].subarray(header.byteLength)).terminating).toBe(true)
        respondWithFrame(h.fake.state, header.correlationId, 1, pushPair.buildResponse())
        expect(await final).toMatchObject({ errorCode: 0 })
      } else expect(await final).toBe(sentinel)
      expect(h.connection.requestQueue.isIdle()).toBe(true)
      expect(h.connection.isConnected()).toBe(true)
    } finally {
      await h.pool.destroy({ drainTimeout: 0 })
      await Promise.all([first, final])
      jest.useRealTimers()
    }
  }
)

it.each(['entry', 'authentication-yield', 'deadline-yield', 'valid'])(
  'fences private protocol encoding at %s',
  async (stage) => {
    const h = await setup({
      enforceRequestTimeout: false,
      authenticationOwner: { run: (work) => work() },
    })
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick'] })
    const sentinel = new Error('private request revoked')
    let revoked = stage === 'entry'
    const encode = jest.fn(async () => new Encoder())
    const result = {}
    const pending = h.connection
      .send({
        request: { apiKey: 3, apiVersion: 0, apiName: 'Metadata', encode },
        response: { decode: async (raw) => raw, parse: async () => result },
        guard: () => (revoked || jest.now() >= 100 ? sentinel : null),
      })
      .catch((error) => error)
    if (stage === 'authentication-yield') revoked = true
    if (stage === 'deadline-yield') jest.setSystemTime(100)
    try {
      await turn()
      if (stage === 'valid') {
        expect(encode).toHaveBeenCalledTimes(1)
        expect(h.fake.state.writes).toHaveLength(1)
        respondWithFrame(h.fake.state, 0, 0, Buffer.alloc(0))
        expect(await pending).toBe(result)
      } else {
        expect(await pending).toBe(sentinel)
        expect(encode).not.toHaveBeenCalled()
        expect(h.fake.state.writes).toEqual([])
      }
      expect(h.connection.requestQueue.isIdle()).toBe(true)
    } finally {
      await h.pool.destroy({ drainTimeout: 0 })
      await pending
      jest.useRealTimers()
    }
  }
)

it.each(['suspend', 'close'])(
  'publishes omission diagnostics after physical write before logger %s can revoke authority',
  async (revocation) => {
    const h = await setup({ enforceRequestTimeout: false })
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick'] })
    const metrics = new TelemetryMetrics({
      kind: 'producer',
      clock: { wallMs: () => 1000, monotonicNs: () => BigInt(jest.now()) * 1000000n },
    })
    metrics.register({ name: 'custom.total', type: 'sum', value: 10n })
    let closing
    const writesAtWarning = []
    const logger = {
      warn: jest.fn(() => {
        writesAtWarning.push(h.fake.state.writes.map((frame) => readRequestHeader(frame).apiKey))
        if (revocation === 'suspend') client.suspend()
        else closing = client.close({ zero: true })
        throw new Error('hostile diagnostic')
      }),
    }
    const client = new ClientTelemetry({
      kind: 'producer',
      metrics,
      logger,
      clock: { now: () => jest.now(), random: () => 0.5, setTimeout, clearTimeout },
      transport: { candidates: () => [], open: () => {} },
    })
    client.connected = true
    client.handle = {
      push: (payload, options) => h.broker.pushTelemetry(payload, options),
      isConnected: () => h.connection.isConnected(),
      retire: () => h.pool.destroy({ drainTimeout: 0 }),
    }
    client.install({
      clientInstanceId: ID,
      subscriptionId: 1,
      acceptedCompressionTypes: [0],
      pushIntervalMs: 1000,
      telemetryMaxBytes: 100,
      deltaTemporality: true,
      requestedMetrics: ['*'],
    })
    const subscription = client.subscription
    client.payload.buildPayload = async (snapshot) => ({
      metrics: Buffer.from([1]),
      compressionType: 0,
      includedIdentities: snapshot.points
        .filter((point) => point.name === 'custom.total')
        .map((point) => point.identity),
      omittedIdentities: snapshot.points
        .filter((point) => point.name !== 'custom.total')
        .map((point) => point.identity),
    })
    const written = jest.spyOn(metrics, 'written')
    try {
      await jest.advanceTimersByTimeAsync(1000)
      await turn()
      await closing
      expect(logger.warn).toHaveBeenCalledTimes(1)
      expect(writesAtWarning).toEqual([[72]])
      expect(h.fake.state.writes).toHaveLength(1)
      const frame = h.fake.state.writes[0]
      const header = readRequestHeader(frame)
      expect(pushPair.readRequest(frame.subarray(header.byteLength))).toMatchObject({
        terminating: false,
        metrics: '01',
      })
      expect(written).toHaveBeenCalledTimes(1)
      expect(client.omittedPoints).toBe(1n)
      metrics.select(subscription)
      const next = metrics.snapshot(subscription)
      expect(next.points.find((point) => point.name === 'custom.total').value).toBe(0n)
      metrics.abandon(next)
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      client.suspend()
      await closing
      await h.pool.destroy({ drainTimeout: 0 })
      jest.useRealTimers()
    }
  }
)

it.each([true, false])(
  'writes and commits an omitted-gauge overflow before collector logger suspension=%s',
  async (suspend) => {
    const h = await setup({ enforceRequestTimeout: false })
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate', 'nextTick'] })
    let release
    const held = new Promise((resolve) => {
      release = resolve
    })
    let snapshot
    const writesAtWarning = []
    const currentAtWarning = []
    const logger = {
      warn: jest.fn(() => {
        writesAtWarning.push(h.fake.state.writes.map((frame) => readRequestHeader(frame).apiKey))
        currentAtWarning.push(metrics.isCurrent(snapshot))
        if (suspend) client.suspend()
        throw new Error('hostile collector diagnostic')
      }),
    }
    const metrics = new TelemetryMetrics({
      kind: 'producer',
      logger,
      clock: { wallMs: () => 1000, monotonicNs: () => BigInt(jest.now()) * 1000000n },
    })
    const omissionLogger = { warn: jest.fn() }
    const client = new ClientTelemetry({
      kind: 'producer',
      metrics,
      logger: omissionLogger,
      clock: { now: () => jest.now(), random: () => 0.5, setTimeout, clearTimeout },
      transport: { candidates: () => [], open: () => {} },
    })
    client.connected = true
    client.handle = {
      push: (payload, options) => h.broker.pushTelemetry(payload, options),
      isConnected: () => h.connection.isConnected(),
      retire: () => h.pool.destroy({ drainTimeout: 0 }),
    }
    const sumName = 'org.apache.kafka.producer.connection.creation.total'
    const averageName = 'org.apache.kafka.producer.node.request.latency.avg'
    client.install({
      clientInstanceId: ID,
      subscriptionId: 1,
      acceptedCompressionTypes: [0],
      pushIntervalMs: 1000,
      telemetryMaxBytes: 160,
      deltaTemporality: true,
      requestedMetrics: [sumName, averageName],
    })
    client.clearTimer()
    const subscription = client.subscription
    const ref = metrics.bindNode({ nodeId: 1, endpoint: 'a:9092', endpointRevision: 1 })
    metrics.observeConnection({})
    metrics.observeRequest(ref, 1)
    const buildPayload = client.payload.buildPayload.bind(client.payload)
    let fitted
    client.payload.buildPayload = async (sample, options) => {
      snapshot = sample
      await held
      fitted = await buildPayload(sample, options)
      return fitted
    }
    h.connection.requestQueue.maybeThrottle(100)
    const pending = client.run('push')
    try {
      await turn()
      expect(snapshot.points.map((point) => [point.name, point.value])).toEqual([
        [sumName, 1n],
        [averageName, 1],
      ])
      metrics.observeRequest(ref, Number.MAX_SAFE_INTEGER)
      expect(logger.warn).not.toHaveBeenCalled()
      release()
      await turn()
      expect(fitted.includedIdentities).toEqual([snapshot.points[0].identity])
      expect(fitted.omittedIdentities).toEqual([snapshot.points[1].identity])
      expect(h.connection.requestQueue.pending).toHaveLength(1)
      expect(h.fake.state.writes).toEqual([])
      expect(metrics.isCurrent(snapshot)).toBe(true)

      await jest.advanceTimersByTimeAsync(100)
      await turn()
      expect(logger.warn).toHaveBeenCalledTimes(1)
      expect(writesAtWarning).toEqual([[72]])
      expect(currentAtWarning).toEqual([false])
      expect(h.fake.state.writes).toHaveLength(1)
      const frame = h.fake.state.writes[0]
      const header = readRequestHeader(frame)
      const push = pushPair.readRequest(frame.subarray(header.byteLength))
      expect(push).toEqual({
        clientInstanceId: ID,
        subscriptionId: 1,
        terminating: false,
        compressionType: 0,
        metrics: fitted.metrics.toString('hex'),
      })
      const bytes = Buffer.from(push.metrics, 'hex')
      expect(bytes.includes(Buffer.from(sumName))).toBe(true)
      expect(bytes.includes(Buffer.from(averageName))).toBe(false)
      if (!suspend)
        respondWithFrame(h.fake.state, header.correlationId, 1, pushPair.buildResponse())
      await pending
      expect(client.connected).toBe(!suspend)
      expect(omissionLogger.warn).toHaveBeenCalledTimes(1)
      expect(client.omittedPoints).toBe(1n)
      const next = metrics.snapshot(subscription)
      expect(next.points.map((point) => [point.name, point.value])).toEqual([[sumName, 0n]])
      metrics.abandon(next)
    } finally {
      release()
      client.suspend()
      await h.pool.destroy({ drainTimeout: 0 })
      await pending
      // Fake socket destruction defers its close event with queueMicrotask.
      jest.runAllTicks()
      const timers = jest.getTimerCount()
      jest.useRealTimers()
      expect(timers).toBe(0)
    }
  }
)
