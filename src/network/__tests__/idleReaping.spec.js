const Connection = require('../connection')
const Encoder = require('../../protocol/encoder')
const { newLogger, createFakeSocketFactory } = require('testHelpers')

/** A manually released encoder gate; no work starts before the caller awaits it.
 * @returns {{promise: Promise<void>, release: () => void}} */
const gate = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}
/** Flushes pending preparation and queue reactions.
 * @returns {Promise<void>} */
const settle = () => new Promise(setImmediate)
/** Builds a real protocol envelope with controlled encoding and response expectation.
 * @param {object} [options] fixture controls
 * @returns {object} Connection.send protocol */
const protocol = ({ response = false, hold = null, error = null } = {}) => ({
  request: {
    apiKey: 18,
    apiVersion: 0,
    apiName: 'ApiVersions',
    expectResponse: () => response,
    encode: async () => {
      if (hold) await hold.promise
      if (error) throw error
      return new Encoder()
    },
  },
  response: { decode: async () => ({}), parse: async (value) => value },
})

const connections = []
/** Opens a fake transport and notifies readiness when the implementation supports it.
 * The optional notification permits the old runtime to reach the behavioral red.
 * @param {object} [options] Connection options
 * @returns {Promise<{connection: Connection, state: object}>} */
const open = async (options = {}) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: 'localhost',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'idle-check',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    connectionsMaxIdle: 100,
    ...options,
  })
  connections.push(connection)
  await connection.connect()
  if (connection.markReady) connection.markReady()
  return { connection, state }
}

beforeEach(() => {
  jest.useFakeTimers({ doNotFake: ['setImmediate', 'queueMicrotask', 'hrtime'] })
  jest.setSystemTime(10000)
})
afterEach(async () => {
  for (const connection of connections.splice(0)) await connection.disconnect({ drainTimeout: 0 })
  expect(jest.getTimerCount()).toBe(0)
  jest.useRealTimers()
})

describe('Connection idle reaping', () => {
  test('closes exactly at the idle limit and clears the socket', async () => {
    const { connection, state } = await open()
    await jest.advanceTimersByTimeAsync(99)
    expect(state.socket.destroyed).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    expect(state.socket.destroyed).toBe(true)
    expect(connection.socket).toBe(null)
  })
  test('zero disables the idle timer', async () => {
    const { connection, state } = await open({ connectionsMaxIdle: 0 })
    expect(jest.getTimerCount()).toBe(0)
    expect(connection.idleTimer).toBe(null)
    await jest.advanceTimersByTimeAsync(1000000)
    expect(state.socket.destroyed).toBe(false)
    expect(connection.idleTimer).toBe(null)
  })
  test('the only idle timer is unreferenced and readiness is idempotent', async () => {
    const { connection } = await open()
    expect(connection.idleTimer.hasRef()).toBe(false)
    const timer = connection.idleTimer
    connection.markReady()
    expect(connection.idleTimer).toBe(timer)
    expect(jest.getTimerCount()).toBe(1)
  })
  test('an in-flight response protects a long poll until its answer', async () => {
    const { connection, state } = await open()
    const sent = connection.send(protocol({ response: true }))
    await settle()
    await jest.advanceTimersByTimeAsync(301)
    expect(state.socket.destroyed).toBe(false)
    const id = [...connection.requestQueue.inflight.keys()][0]
    connection.requestQueue.fulfillRequest({ correlationId: id, payload: Buffer.alloc(0), size: 0 })
    await sent
    await jest.advanceTimersByTimeAsync(99)
    expect(state.socket.destroyed).toBe(false)
    await jest.advanceTimersByTimeAsync(100)
    expect(state.socket.destroyed).toBe(true)
  })
  test('a throttled pending request and its late acks zero transmission postpone reaping', async () => {
    const { connection, state } = await open()
    connection.requestQueue.maybeThrottle(150)
    const sent = connection.send(protocol())
    await settle()
    expect(connection.requestQueue.pending).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(149)
    expect(state.socket.destroyed).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    await sent
    expect(connection.requestQueue.lastActivityAt).toBe(10150)
    await jest.advanceTimersByTimeAsync(99)
    expect(state.socket.destroyed).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    expect(state.socket.destroyed).toBe(true)
  })
  test('an encoder held across the deadline protects the socket', async () => {
    const { connection, state } = await open()
    const hold = gate()
    const sent = connection.send(protocol({ hold }))
    await settle()
    await jest.advanceTimersByTimeAsync(301)
    expect(state.socket.destroyed).toBe(false)
    expect(connection.preparing).toBe(1)
    hold.release()
    await sent
    expect(connection.preparing).toBe(0)
    await jest.advanceTimersByTimeAsync(200)
    expect(state.socket.destroyed).toBe(true)
  })
  test('an encoding failure releases preparation and leaves reaping enabled', async () => {
    const { connection, state } = await open()
    const error = new Error('encoder refused')
    await expect(connection.send(protocol({ error }))).rejects.toBe(error)
    expect(connection.preparing).toBe(0)
    await jest.advanceTimersByTimeAsync(100)
    expect(state.socket.destroyed).toBe(true)
  })
  test('a stale timer callback cannot close or clear the replacement timer', async () => {
    const spy = jest.spyOn(global, 'setTimeout')
    const { connection, state } = await open()
    const old = spy.mock.calls.find(([, ms]) => ms === 100)[0]
    await connection.disconnect()
    await connection.connect()
    connection.markReady()
    const timer = connection.idleTimer
    old()
    expect(connection.idleTimer).toBe(timer)
    expect(state.socket.destroyed).toBe(false)
    spy.mockRestore()
  })
  test('an obsolete encoder cannot decrement the replacement preparation count', async () => {
    const { connection, state } = await open()
    const oldGate = gate()
    const old = connection.send(protocol({ hold: oldGate })).catch((error) => error)
    await settle()
    await connection.disconnect({ drainTimeout: 0 })
    await connection.connect()
    connection.markReady()
    const newGate = gate()
    const current = connection.send(protocol({ hold: newGate }))
    await settle()
    oldGate.release()
    expect(await old).toBeInstanceOf(Error)
    expect(connection.preparing).toBe(1)
    await jest.advanceTimersByTimeAsync(100)
    expect(state.socket.destroyed).toBe(false)
    newGate.release()
    await current
    expect(connection.preparing).toBe(0)
  })
  test('recent acks zero work rearms only the remaining idle period', async () => {
    const { connection, state } = await open()
    await jest.advanceTimersByTimeAsync(40)
    await connection.send(protocol())
    await jest.advanceTimersByTimeAsync(99)
    expect(state.socket.destroyed).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    expect(state.socket.destroyed).toBe(true)
  })
  test('teardown clears the timer even when the socket fails', async () => {
    const { connection } = await open()
    connection.failGeneration(new Error('socket failed'))
    expect(connection.idleTimer).toBe(null)
    expect(connection.socket).toBe(null)
    expect(jest.getTimerCount()).toBe(0)
  })
  test('a diagnostic exception cannot prevent idle teardown', async () => {
    const { connection, state } = await open()
    connection.logDebug = () => {
      throw new Error('logger failed')
    }
    await jest.advanceTimersByTimeAsync(100)
    expect(state.socket.destroyed).toBe(true)
  })
})

const Broker = require('../../broker')
const ConnectionPool = require('../connectionPool')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')

describe.each(['default', 'sasl'])('Idle reconnect through %s', (profile) => {
  test('the next heartbeat authenticates fresh and renegotiates versions', async () => {
    const node = { id: 0, host: 'localhost', port: 9092 }
    const cluster = createScriptedCluster({ nodes: [node], profile })
    const pool = new ConnectionPool({
      ...node,
      logger: newLogger(),
      socketFactory: cluster.socketFactory,
      connectionTimeout: 1000,
      requestTimeout: 1000,
      enforceRequestTimeout: false,
      connectionsMaxIdle: 100,
      ...(profile === 'sasl' && {
        sasl: { mechanism: 'plain', username: 'user', password: 'secret' },
      }),
    })
    const broker = new Broker({ connectionPool: pool, logger: newLogger(), nodeId: 0 })
    try {
      await broker.connect()
      const first = await pool.getConnection()
      connections.push(first)
      const oldSocket = first.socket
      const before = cluster.requests().filter(({ api }) => api === 'ApiVersions').length
      await jest.advanceTimersByTimeAsync(100)
      expect(oldSocket.destroyed).toBe(true)
      expect(first.authenticatedAt).toBe(null)
      await broker.heartbeat({ groupId: 'idle-group', groupGenerationId: 1, memberId: 'member' })
      expect(cluster.requests().filter(({ api }) => api === 'ApiVersions')).toHaveLength(before + 1)
      expect(first.socket).not.toBe(oldSocket)
      const heartbeat = cluster.requests().find(({ api }) => api === 'Heartbeat')
      expect(heartbeat.generation).toBeGreaterThan(1)
      if (profile === 'sasl') {
        expect(first.authenticatedAt).not.toBe(null)
        expect(cluster.requests().filter(({ api }) => api === 'SaslAuthenticate')).toHaveLength(2)
      }
      expect(cluster.sockets().filter((socket) => !socket.destroyed)).toHaveLength(1)
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
      cluster.assertClean()
    }
  })
})

test('an external reauthentication protects a socket while its token provider is pending', async () => {
  const node = { id: 0, host: 'localhost', port: 9092 }
  const cluster = createScriptedCluster({ nodes: [node], profile: 'sasl' })
  cluster.answer('SaslHandshake', () => ({ enabledMechanisms: ['OAUTHBEARER'] }))
  cluster.answer('SaslAuthenticate', () => ({ sessionLifetimeMs: '1000' }))
  const hold = gate()
  let calls = 0
  const provider = async () => {
    if (++calls === 2) await hold.promise
    return { value: 'fixture-token' }
  }
  const pool = new ConnectionPool({
    ...node,
    logger: newLogger(),
    socketFactory: cluster.socketFactory,
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    connectionsMaxIdle: 100,
    sasl: { mechanism: 'oauthbearer', oauthBearerProvider: provider },
  })
  const broker = new Broker({ connectionPool: pool, logger: newLogger(), nodeId: 0 })
  let authenticating
  try {
    await broker.connect()
    const connection = await pool.getConnection()
    connections.push(connection)
    await settle()
    expect(connection.sessionLifetime.toString()).toBe('1000')
    expect(connection.isAuthenticated()).toBe(false)
    authenticating = connection.authenticate()
    authenticating.catch(() => {})
    for (let turn = 0; turn < 20; turn++) {
      if (calls >= 2) break
      await settle()
    }
    expect(calls).toBe(2)
    await jest.advanceTimersByTimeAsync(301)
    expect(connection.socket.destroyed).toBe(false)
    expect(connection.preparing).toBeGreaterThan(0)
    hold.release()
    await authenticating
    expect(connection.preparing).toBe(0)
  } finally {
    hold.release()
    if (authenticating) await authenticating.catch(() => {})
    await broker.disconnect({ drainTimeout: 0 })
    cluster.assertClean()
  }
})

test('a pending push records activity before transmission', async () => {
  const { connection } = await open()
  await jest.advanceTimersByTimeAsync(40)
  connection.requestQueue.maybeThrottle(100)
  const sent = connection.send(protocol())
  await settle()
  expect(connection.requestQueue.pending).toHaveLength(1)
  expect(connection.requestQueue.lastActivityAt).toBe(10040)
  await jest.advanceTimersByTimeAsync(100)
  await sent
})

test('a pool send reconnects a connection reaped after acquisition', async () => {
  const { factory, state } = createFakeSocketFactory()
  const pool = new ConnectionPool({
    host: 'localhost',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    connectionsMaxIdle: 100,
  })
  try {
    const acquired = await pool.getConnection()
    connections.push(acquired)
    acquired.markReady()
    await jest.advanceTimersByTimeAsync(100)
    expect(acquired.socket).toBe(null)
    await pool.send(protocol(), acquired)
    expect(state.sockets).toHaveLength(2)
    expect(state.writes).toHaveLength(1)
    expect(state.sockets[0].destroyed).toBe(true)
    expect(state.sockets[1].destroyed).toBe(false)
  } finally {
    await pool.destroy({ drainTimeout: 0 })
  }
})

test('a directly opened Fetch connection becomes idle eligible automatically', async () => {
  const { factory, state } = createFakeSocketFactory()
  const pool = new ConnectionPool({
    host: 'localhost',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    connectionsMaxIdle: 100,
  })
  try {
    const fetch = await pool.getConnection(1)
    connections.push(fetch)
    await jest.advanceTimersByTimeAsync(99)
    expect(state.socket.destroyed).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    expect(state.socket.destroyed).toBe(true)
    expect(fetch.socket).toBe(null)
  } finally {
    await pool.destroy({ drainTimeout: 0 })
  }
})

test('a primary held before ApiVersions ready stays ineligible until setup finishes', async () => {
  const { Schedule } = require('../../../testHelpers/schedule')
  const schedule = new Schedule()
  const cluster = createScriptedCluster({
    nodes: [{ id: 0, host: 'localhost', port: 9092 }],
    schedule,
  })
  const held = schedule.hold({ api: 'ApiVersions' })
  const pool = new ConnectionPool({
    host: 'localhost',
    port: 9092,
    logger: newLogger(),
    socketFactory: cluster.socketFactory,
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    connectionsMaxIdle: 100,
  })
  const broker = new Broker({ connectionPool: pool, logger: newLogger(), nodeId: 0 })
  const connected = broker.connect()
  connected.catch(() => {})
  try {
    for (let turn = 0; turn < 20 && !held.call; turn++) await settle()
    await jest.advanceTimersByTimeAsync(301)
    expect(held.call.api).toBe('ApiVersions')
    const primary = pool.pool[0]
    connections.push(primary)
    expect(primary.socket.destroyed).toBe(false)
    expect(primary.idleTimer).toBe(null)
    held.release(cluster.apiVersionsAnswer())
    await jest.advanceTimersByTimeAsync(0)
    await connected
    const socket = primary.socket
    expect(primary.idleTimer).not.toBe(null)
    await jest.advanceTimersByTimeAsync(99)
    expect(socket.destroyed).toBe(false)
    await jest.advanceTimersByTimeAsync(1)
    expect(socket.destroyed).toBe(true)
  } finally {
    await broker.disconnect({ drainTimeout: 0 })
    if (held.settle) held.release(cluster.apiVersionsAnswer())
    await connected.catch(() => {})
    cluster.assertClean()
  }
})
