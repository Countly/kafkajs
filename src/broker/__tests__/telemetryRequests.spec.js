const Broker = require('../index')
const ConnectionPool = require('../../network/connectionPool')
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
const setup = async () => {
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
  it.each([
    { acceptedCompressionTypes: Array(129).fill(0) },
    { requestedMetrics: Array(257).fill('') },
    { requestedMetrics: ['é'.repeat(129)] },
    { requestedMetrics: Array(65).fill('x'.repeat(256)) },
  ])('refuses bounded metadata before array decoding: %j', (fields) => {
    const broker = Object.create(Broker.prototype)
    expect(() => broker.validateTelemetryMetadata(getPair.buildResponse(fields))).toThrow()
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
