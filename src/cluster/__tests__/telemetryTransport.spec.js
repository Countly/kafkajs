const Cluster = require('../index')
const { runWithDeadline, currentDeadline } = require('../../admin/deadline')
const BrokerPool = require('../brokerPool')
const builderFactory = require('../connectionPoolBuilder')
const {
  newLogger,
  createFakeSocketFactory,
  readRequestHeader,
  respondWithFrame,
} = require('testHelpers')
const apiPair = require('../../../testHelpers/wire/apiVersionsV4')
const getPair = require('../../../testHelpers/wire/getTelemetrySubscriptionsV0')
/** @returns {Promise<void>} finite event-loop turn */
const turn = () => new Promise((resolve) => setImmediate(resolve))
/** @returns {object} explicit known-endpoint builder with a fake physical socket */
const setup = (sasl) => {
  const fake = createFakeSocketFactory()
  const brokers = jest.fn(() => {
    throw Error('discovery forbidden')
  })
  const builder = builderFactory({
    brokers,
    sasl,
    socketFactory: fake.factory,
    logger: newLogger(),
    clientId: 'client',
    connectionTimeout: 1000,
    requestTimeout: 90000,
    enforceRequestTimeout: false,
    maxResponseSize: 1000000,
  })
  const pools = []
  const buildTelemetry = builder.buildTelemetry
  builder.buildTelemetry = (...args) => {
    const pool = buildTelemetry(...args)
    pools.push(pool)
    return pool
  }
  const candidate = {
    nodeId: 1,
    host: 'a',
    port: 9092,
    endpointRevision: 1n,
    versions: { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } },
  }
  const source = { telemetryCandidates: () => [candidate] }
  const clock = { now: () => 0 }
  const transport = Cluster.prototype.createTelemetryTransport.call(
    { connectionPoolBuilder: builder, brokerPool: source, rootLogger: newLogger() },
    clock
  )
  return { fake, builder, brokers, candidate, source, transport, pools, clock }
}
describe('dedicated telemetry transport', () => {
  it('uses explicit candidates and private timeout queue frame and observer policies', async () => {
    const h = setup()
    const normal = await h.builder.build({ host: 'a', port: 9092 })
    const pool = h.builder.buildTelemetry(h.candidate, {
      guard: () => null,
      remaining: () => 30000,
    })
    expect(h.brokers).not.toHaveBeenCalled()
    expect(pool).not.toBe(normal)
    expect(pool.authenticationBlackout).not.toBe(normal.authenticationBlackout)
    expect(pool.setupStates).not.toBe(normal.setupStates)
    expect(pool.pool[0].requestTimeout).toBe(5000)
    expect(pool.pool[0].requestQueue.enforceRequestTimeout).toBe(true)
    expect(pool.pool[0].frames.maxBodySize).toBe(65536)
    expect(pool.pool[0].telemetrySink).toBeNull()
    expect(pool.pool[1].isConnected()).toBe(false)
    await pool.destroy({ drainTimeout: 0 })
    await normal.destroy({ drainTimeout: 0 })
  })
  it.each([true, false])('renegotiates the actual dedicated socket, paired=%s', async (paired) => {
    const h = setup()
    let allocated
    const pending = h.transport.open(h.candidate, {
      guard: () => null,
      deadline: 30000,
      onAllocated: (handle) => {
        allocated = handle
      },
    })
    const observed = pending.catch((e) => e)
    try {
      await turn()
      await turn()
      const frame = h.fake.state.writes[0]
      const header = readRequestHeader(frame)
      expect(header.apiKey).toBe(18)
      respondWithFrame(
        h.fake.state,
        header.correlationId,
        0,
        apiPair.buildResponse({
          apiKeys: [
            { apiKey: 18, minVersion: 0, maxVersion: 4 },
            { apiKey: 71, minVersion: 0, maxVersion: 0 },
            ...(paired ? [{ apiKey: 72, minVersion: 0, maxVersion: 0 }] : []),
          ],
        })
      )
      const result = await observed
      if (paired) {
        expect(result).toBe(allocated)
        const response = result.get({ clientInstanceId: null }, { guard: () => null })
        await turn()
        const header = readRequestHeader(h.fake.state.writes.at(-1))
        expect(header.apiKey).toBe(71)
        respondWithFrame(h.fake.state, header.correlationId, 1, getPair.buildResponse())
        await expect(response).resolves.toMatchObject({ errorCode: 0 })
      } else {
        expect(result).toMatchObject({ code: 35 })
        expect(h.fake.state.writes.map((f) => readRequestHeader(f).apiKey)).toEqual([18])
      }
      expect(h.fake.state.sockets).toHaveLength(1)
      expect(h.brokers).not.toHaveBeenCalled()
    } finally {
      if (allocated) await allocated.retire({ drainTimeout: 0 })
    }
  })
  it('copies only ready current foreground advertisements and issues endpoint revisions', () => {
    const pool = Object.create(BrokerPool.prototype)
    pool.telemetryEndpointRevision = 0n
    pool.telemetryAdvertisements = new WeakMap()
    const broker = {
      nodeId: 1,
      retired: false,
      isConnected: () => true,
      connectionPool: { host: 'a', port: 9092, generation: 1 },
      versionsGeneration: 1,
      versions: { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } },
    }
    pool.seedBroker = broker
    pool.brokers = { 1: broker }
    const a = pool.telemetryCandidates()
    expect(a).toHaveLength(1)
    expect(Object.isFrozen(a[0].versions[71])).toBe(true)
    expect(pool.telemetryCandidates()[0].endpointRevision).toBe(a[0].endpointRevision)
    broker.connectionPool = { host: 'a', port: 9092, generation: 1 }
    const b = pool.telemetryCandidates()
    expect(b[0].endpointRevision > a[0].endpointRevision).toBe(true)
    broker.versionsGeneration = 0
    expect(pool.telemetryCandidates()).toEqual([])
  })
})

describe('captured transport retirement', () => {
  it('waits for old retirement and checks old authority before another dial', async () => {
    const h = setup()
    let allocated
    const first = h.transport.open(h.candidate, {
      guard: () => null,
      deadline: 30000,
      onAllocated: (handle) => {
        allocated = handle
      },
    })
    await turn()
    await turn()
    respondWithFrame(
      h.fake.state,
      0,
      0,
      apiPair.buildResponse({
        apiKeys: [
          { apiKey: 18, minVersion: 0, maxVersion: 4 },
          { apiKey: 71, minVersion: 0, maxVersion: 0 },
          { apiKey: 72, minVersion: 0, maxVersion: 0 },
        ],
      })
    )
    await first
    let release
    let revoked = false
    const gate = new Promise((resolve) => {
      release = resolve
    })
    const retire = allocated.retire
    allocated.retire = async (options) => {
      await gate
      return retire(options)
    }
    const sentinel = new Error('closed lifecycle')
    const second = h.transport
      .open(h.candidate, {
        guard: () => (revoked ? sentinel : null),
        deadline: 30000,
        onAllocated: () => {
          throw Error('must not allocate')
        },
      })
      .catch((e) => e)
    await turn()
    expect(h.fake.state.sockets).toHaveLength(1)
    revoked = true
    release()
    expect(await second).toBe(sentinel)
    expect(h.fake.state.sockets).toHaveLength(1)
    expect(h.fake.state.sockets[0].destroyed).toBe(true)
  })
})

/** @param {object} h real transport harness @returns {Promise<void>} reply only to live setup writes */
const replySetup = async (h) => {
  await turn()
  await turn()
  const socket = h.fake.state.socket
  if (!socket || socket.destroyed) return
  const frame = socket.writes.at(-1)
  if (!frame) return
  const header = readRequestHeader(frame)
  if (header.apiKey === 18) {
    respondWithFrame(
      h.fake.state,
      header.correlationId,
      0,
      apiPair.buildResponse({
        apiKeys: [
          { apiKey: 18, minVersion: 0, maxVersion: 4 },
          { apiKey: 17, minVersion: 0, maxVersion: 1 },
          { apiKey: 36, minVersion: 0, maxVersion: 0 },
          { apiKey: 71, minVersion: 0, maxVersion: 0 },
          { apiKey: 72, minVersion: 0, maxVersion: 0 },
        ],
      })
    )
    await turn()
  }
  const handshake = socket.writes.at(-1)
  if (readRequestHeader(handshake).apiKey === 17) {
    const mechanism = h.pools.at(-1).pool[0].sasl.mechanism.toUpperCase()
    const name = Buffer.from(mechanism)
    const body = Buffer.alloc(8 + name.length)
    body.writeInt32BE(1, 2)
    body.writeInt16BE(name.length, 6)
    name.copy(body, 8)
    respondWithFrame(h.fake.state, readRequestHeader(handshake).correlationId, 0, body)
    await turn()
  }
}
/** @param {object} h harness @param {Function} guard original authority @returns {object} observed open and captured handles */
const startOpen = (h, guard = () => null) => {
  const handles = []
  const observed = h.transport
    .open(h.candidate, {
      guard,
      deadline: h.clock.now() + 30000,
      onAllocated: (handle) => handles.push(handle),
    })
    .catch((error) => error)
  return { observed, handles }
}
/** @param {object} h harness @param {number} key response API @returns {void} independently framed success */
const replySuccess = (h, key) => {
  const header = readRequestHeader(h.fake.state.writes.at(-1))
  expect(header.apiKey).toBe(key)
  const body =
    key === 36 ? Buffer.from([0, 0, 255, 255, 0, 0, 0, 0]) : Buffer.from([0, 0, 0, 0, 0, 0, 0])
  respondWithFrame(h.fake.state, header.correlationId, key === 72 ? 1 : 0, body)
}

describe('authentication ownership and authority', () => {
  it('keeps a held built-in OAuth supplier in the one client shell', async () => {
    const releases = []
    const oauthBearerProvider = jest.fn(() => new Promise((resolve) => releases.push(resolve)))
    const h = setup({ mechanism: 'OAUTHBEARER', oauthBearerProvider })
    const first = startOpen(h)
    let second
    try {
      await replySetup(h)
      expect(oauthBearerProvider).toHaveBeenCalledTimes(1)
      await first.handles[0].retire({ drainTimeout: 0 })
      await first.observed
      second = startOpen(h)
      await replySetup(h)
      expect(oauthBearerProvider).toHaveBeenCalledTimes(1)
      expect(await second.observed).toBeInstanceOf(Error)
      expect(h.fake.state.sockets).toHaveLength(1)
      for (let index = 0; index < 4; index++) {
        const refused = startOpen(h)
        expect(await refused.observed).toBeInstanceOf(Error)
        expect(refused.handles).toEqual([])
        expect(h.fake.state.sockets).toHaveLength(1)
        expect(oauthBearerProvider).toHaveBeenCalledTimes(1)
      }
    } finally {
      for (const handle of [...first.handles, ...(second?.handles || [])])
        await handle.retire({ drainTimeout: 0 })
      for (const release of releases) release({ value: 'token' })
      await Promise.all([first.observed, second?.observed])
      await turn()
    }
    const next = startOpen(h)
    try {
      await replySetup(h)
      expect(oauthBearerProvider).toHaveBeenCalledTimes(2)
      releases.at(-1)({ value: 'current-token' })
      await turn()
      replySuccess(h, 36)
      expect(await next.observed).toBe(next.handles[0])
      expect(
        h.fake.state.sockets[0].writes.map((frame) => readRequestHeader(frame).apiKey)
      ).toEqual([18, 17])
    } finally {
      for (const handle of next.handles) await handle.retire({ drainTimeout: 0 })
      releases.at(-1)({ value: 'cleanup' })
      await next.observed
    }
  })

  it('a held supplier prevents another telemetry supplier after retirement', async () => {
    const releases = []
    const authenticate = jest.fn(() =>
      authenticate.mock.calls.length === 1
        ? new Promise((resolve) => releases.push(resolve))
        : Promise.resolve()
    )
    const h = setup({ mechanism: 'PLAIN', authenticationProvider: () => ({ authenticate }) })
    const first = startOpen(h)
    let next
    try {
      await replySetup(h)
      expect(authenticate).toHaveBeenCalledTimes(1)
      await first.handles[0].retire({ drainTimeout: 0 })
      await first.observed
      for (let attempt = 0; attempt < 100; attempt++) {
        const refused = startOpen(h)
        expect(await refused.observed).toBeInstanceOf(Error)
        expect(refused.handles).toEqual([])
      }
      expect(authenticate).toHaveBeenCalledTimes(1)
      expect(h.fake.state.sockets).toHaveLength(1)
      expect(h.pools).toHaveLength(1)
      releases[0]()
      await turn()
      expect(h.pools[0].pool[0].authenticatedAt).toBeNull()
      expect(h.pools).toHaveLength(1)
      next = startOpen(h)
      await replySetup(h)
      expect(authenticate).toHaveBeenCalledTimes(2)
      expect(await next.observed).toBe(next.handles[0])
      expect(h.pools).toHaveLength(2)
      expect(h.fake.state.sockets).toHaveLength(2)
    } finally {
      for (const handle of [...first.handles, ...(next?.handles || [])])
        await handle.retire({ drainTimeout: 0 })
      for (const release of releases) release()
      await Promise.all([first.observed, next?.observed])
    }
  })

  it('uses live request authority for custom-provider reauthentication', async () => {
    const authenticate = jest.fn(async () => {})
    const h = setup({ mechanism: 'PLAIN', authenticationProvider: () => ({ authenticate }) })
    let opening = true
    const expired = new Error('opening request ended')
    const first = startOpen(h, () => (opening ? null : expired))
    let push
    try {
      await replySetup(h)
      expect(await first.observed).toBe(first.handles[0])
      expect(authenticate).toHaveBeenCalledTimes(1)
      opening = false
      h.clock.now = () => 31000
      h.pools[0].pool[0].authenticatedAt = null
      push = first.handles[0]
        .push(
          {
            clientInstanceId: '01020304-0506-0708-090a-0b0c0d0e0f10',
            subscriptionId: 1,
            terminating: false,
            compressionType: 0,
            metrics: Buffer.alloc(0),
          },
          { guard: () => null }
        )
        .catch((error) => error)
      await replySetup(h)
      expect(authenticate).toHaveBeenCalledTimes(2)
      replySuccess(h, 72)
      expect(await push).toMatchObject({ errorCode: 0 })
      expect(h.fake.state.sockets).toHaveLength(1)
    } finally {
      for (const handle of first.handles) await handle.retire({ drainTimeout: 0 })
      await Promise.all([first.observed, push])
    }
  })
})

it.each(['handshake', 'supplier', 'encode', 'decode', 'parse', 'publication'])(
  'retains renewal authority through %s',
  async (stage) => {
    let release
    const gate = new Promise((resolve) => {
      release = resolve
    })
    let calls = 0
    let reached = null
    const h = setup({
      mechanism: 'PLAIN',
      authenticationProvider: ({ saslAuthenticate }) => ({
        authenticate: async () => {
          calls++
          if (calls === 1) return
          if (stage === 'supplier') {
            reached = stage
            await gate
          }
          await saslAuthenticate({
            request: {
              encode: async () => {
                if (stage === 'encode') {
                  reached = stage
                  await gate
                }
                return Buffer.from([1])
              },
            },
            response: {
              decode: async (raw) => {
                if (stage === 'decode') {
                  reached = stage
                  await gate
                }
                return raw
              },
              parse: async (raw) => {
                if (stage === 'parse') {
                  reached = stage
                  await gate
                }
                return raw
              },
            },
          })
          if (stage === 'publication') {
            reached = stage
            await gate
          }
        },
      }),
    })
    const first = startOpen(h)
    let push
    let revoked = false
    const sentinel = new Error('renewal ended')
    try {
      await replySetup(h)
      expect(await first.observed).toBe(first.handles[0])
      const connection = h.pools[0].pool[0]
      connection.authenticatedAt = null
      const writesBefore = h.fake.state.writes.length
      push = first.handles[0]
        .push(
          {
            clientInstanceId: '01020304-0506-0708-090a-0b0c0d0e0f10',
            subscriptionId: 1,
            terminating: false,
            compressionType: 0,
            metrics: Buffer.alloc(0),
          },
          { guard: () => (revoked ? sentinel : null) }
        )
        .catch((error) => error)
      await turn()
      expect(readRequestHeader(h.fake.state.writes.at(-1)).apiKey).toBe(17)
      if (stage === 'handshake') {
        revoked = true
        await replySetup(h)
      } else {
        await replySetup(h)
        if (['decode', 'parse', 'publication'].includes(stage)) {
          replySuccess(h, 36)
          await turn()
        }
        expect(reached).toBe(stage)
        revoked = true
        release()
      }
      expect(await push).toBe(sentinel)
      expect(connection.authenticatedAt).toBeNull()
      const keys = h.fake.state.writes
        .slice(writesBefore)
        .map((frame) => readRequestHeader(frame).apiKey)
      expect(keys).toEqual(['decode', 'parse', 'publication'].includes(stage) ? [17, 36] : [17])
    } finally {
      release()
      for (const handle of first.handles) await handle.retire({ drainTimeout: 0 })
      await Promise.all([first.observed, push])
    }
  }
)

it.each([false, true])('preserves scoped renewal authority with revocation=%s', async (revoke) => {
  let release
  const gate = new Promise((resolve) => {
    release = resolve
  })
  const authenticate = jest.fn(async () => {
    if (authenticate.mock.calls.length === 2) await gate
  })
  const h = setup({ mechanism: 'PLAIN', authenticationProvider: () => ({ authenticate }) })
  const first = startOpen(h)
  const sentinel = new Error('renewal request ended')
  let revoked = false
  let pending
  try {
    await replySetup(h)
    expect(await first.observed).toBe(first.handles[0])
    const connection = h.pools[0].pool[0]
    connection.authenticatedAt = null
    const { lookup, requests } = require('../../protocol/requests')
    const protocol = lookup({ 72: { minVersion: 0, maxVersion: 0 } })(72, requests.PushTelemetry)({
      clientInstanceId: '01020304-0506-0708-090a-0b0c0d0e0f10',
      subscriptionId: 1,
      terminating: false,
      compressionType: 0,
      metrics: Buffer.alloc(0),
    })
    pending = runWithDeadline('renewal', 5000, () =>
      connection.send({ ...protocol, guard: () => (revoked ? sentinel : null) })
    ).catch((error) => error)
    await replySetup(h)
    expect(authenticate).toHaveBeenCalledTimes(2)
    revoked = revoke
    release()
    await turn()
    if (revoke) {
      expect(await pending).toBe(sentinel)
      expect(connection.authenticatedAt).toBeNull()
      expect(h.fake.state.writes.map((frame) => readRequestHeader(frame).apiKey)).toEqual([
        18, 17, 17,
      ])
    } else {
      replySuccess(h, 72)
      expect(await pending).toMatchObject({ errorCode: 0 })
      expect(connection.authenticatedAt).not.toBeNull()
      expect(h.fake.state.writes.map((frame) => readRequestHeader(frame).apiKey)).toEqual([
        18, 17, 17, 72,
      ])
    }
    expect(h.fake.state.sockets).toHaveLength(1)
    expect(connection.preparing).toBe(0)
  } finally {
    release()
    for (const handle of first.handles) await handle.retire({ drainTimeout: 0 })
    await Promise.all([first.observed, pending])
  }
})

it('keeps private notifications open and requests outside the calling operation', async () => {
  const contexts = []
  const authenticate = jest.fn(async () => contexts.push(currentDeadline()))
  const h = setup({ mechanism: 'PLAIN', authenticationProvider: () => ({ authenticate }) })
  const handles = []
  let opening
  let get
  let push
  let notified
  try {
    h.transport.onCandidatesChanged(() => {
      contexts.push(currentDeadline())
      notified = turn().then(() => contexts.push(currentDeadline()))
    })
    await runWithDeadline('origin', 5000, () => {
      h.source.telemetryAdvertisementListener()
      opening = h.transport
        .open(h.candidate, {
          guard: () => null,
          deadline: 30000,
          onAllocated: (handle) => {
            contexts.push(currentDeadline())
            handles.push(handle)
          },
        })
        .catch((error) => error)
    })
    await replySetup(h)
    expect(await opening).toBe(handles[0])
    await notified
    expect(contexts).toEqual([undefined, undefined, undefined, undefined])
    await runWithDeadline('origin', 5000, () => {
      get = handles[0].get({ clientInstanceId: null }, { guard: () => null }).catch((e) => e)
    })
    await turn()
    const header = readRequestHeader(h.fake.state.writes.at(-1))
    expect(header.apiKey).toBe(71)
    respondWithFrame(
      h.fake.state,
      header.correlationId,
      1,
      getPair.buildResponse({ throttleTime: 0 })
    )
    expect(await get).toMatchObject({ errorCode: 0 })
    h.pools[0].pool[0].authenticatedAt = null
    await runWithDeadline('origin', 5000, () => {
      push = handles[0]
        .push(
          {
            clientInstanceId: '01020304-0506-0708-090a-0b0c0d0e0f10',
            subscriptionId: 1,
            terminating: false,
            compressionType: 0,
            metrics: Buffer.alloc(0),
          },
          { guard: () => null }
        )
        .catch((e) => e)
    })
    await replySetup(h)
    replySuccess(h, 72)
    expect(await push).toMatchObject({ errorCode: 0 })
    expect(authenticate).toHaveBeenCalledTimes(2)
    expect(contexts.every((context) => context === undefined)).toBe(true)
    expect(h.brokers).not.toHaveBeenCalled()
    expect(h.fake.state.sockets).toHaveLength(1)
  } finally {
    for (const handle of handles) await handle.retire({ drainTimeout: 0 })
    await Promise.all([opening, get, push, notified])
  }
})
