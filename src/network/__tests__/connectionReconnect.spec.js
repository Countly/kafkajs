const { EventEmitter } = require('events')
const Connection = require('../connection')
const ConnectionPool = require('../connectionPool')
const Encoder = require('../../protocol/encoder')
const { CONNECTION_STATUS } = require('../connectionStatus')
const { KafkaJSConnectionError } = require('../../errors')
const { newLogger, respondWithFrame, readRequestHeader } = require('testHelpers')

/**
 * A minimal fake socket factory that records every socket it creates. `mode: 'async'` (the
 * default) defers `onConnect` to the next tick, matching a real socket; `mode: 'sync'` calls it
 * before `createSocket` returns, matching a `socketFactory` that hands back an already-connected
 * or pooled socket — a documented extension point, not an edge case.
 * @param {'async'|'sync'} [mode='async']
 * @returns {{ factory: Function, sockets: EventEmitter[] }}
 */
const createTrackingSocketFactory = (mode = 'async') => {
  const sockets = []
  const factory = ({ onConnect }) => {
    const socket = new EventEmitter()
    socket.write = () => true
    socket.end = () => {}
    socket.unref = () => {}
    sockets.push(socket)
    if (mode === 'sync') {
      onConnect()
    } else {
      setImmediate(onConnect)
    }
    return socket
  }
  return { factory, sockets }
}

const connectionOptsWith = (socketFactory) => ({
  host: '127.0.0.1',
  port: 9092,
  logger: newLogger(),
  socketFactory,
  clientId: 'test-client',
  connectionTimeout: 200,
  requestTimeout: 200,
  enforceRequestTimeout: false,
})

describe('Connection > reconnect safety', () => {
  test('two overlapping connect() calls both settle and only one socket is created', async () => {
    const { factory, sockets } = createTrackingSocketFactory()
    const connection = new Connection(connectionOptsWith(factory))

    const [first, second] = await Promise.all([connection.connect(), connection.connect()])

    expect(first).toBe(true)
    expect(second).toBe(true)
    expect(sockets).toHaveLength(1)
    expect(connection.isConnected()).toBe(true)

    await connection.disconnect()
  })

  test('a late end from the socket a reconnect already replaced does not tear down the new one', async () => {
    const { factory, sockets } = createTrackingSocketFactory()
    const connection = new Connection(connectionOptsWith(factory))

    await connection.connect()
    const staleSocket = sockets[0]

    await connection.disconnect()
    await connection.connect()

    expect(sockets).toHaveLength(2)
    expect(connection.isConnected()).toBe(true)

    // The remote FIN for the socket just disconnected is delivered asynchronously, and can
    // arrive after the reconnect above already created a new one.
    staleSocket.emit('end')

    expect(connection.isConnected()).toBe(true)

    await connection.disconnect()
  })

  test('a synchronous socketFactory (onConnect called before createSocket returns) can reconnect', async () => {
    const { factory, sockets } = createTrackingSocketFactory('sync')
    const connection = new Connection(connectionOptsWith(factory))

    await connection.connect()
    await connection.disconnect()
    await connection.connect()

    expect(sockets).toHaveLength(2)
    expect(connection.isConnected()).toBe(true)

    await connection.disconnect()
  })

  test('generation increments once per successful connect, not per connect() call', async () => {
    const { factory } = createTrackingSocketFactory()
    const connection = new Connection(connectionOptsWith(factory))

    expect(connection.generation).toEqual(0)

    await connection.connect()
    await connection.connect()
    expect(connection.generation).toEqual(1)

    await connection.disconnect()
    await connection.connect()
    expect(connection.generation).toEqual(2)

    await connection.disconnect()
  })

  test('overlapping disconnect() calls do not tear down or downgrade a socket a later connect() already installed', async () => {
    const { factory, sockets } = createTrackingSocketFactory()
    const connection = new Connection(connectionOptsWith(factory))

    await connection.connect() // socket #1

    // The first disconnect() blocks in its drain step; the second's own drain resolves
    // immediately, so it finishes (and tears down socket #1) while the first is still pending —
    // mirroring a broker FIN's onEnd->disconnect() racing an application-level disconnect().
    let releaseFirstDrain
    const firstDrain = new Promise((resolve) => {
      releaseFirstDrain = resolve
    })
    let drainCalls = 0
    connection.requestQueue.waitForPendingRequests = () => {
      drainCalls += 1
      return drainCalls === 1 ? firstDrain : Promise.resolve()
    }

    const firstDisconnect = connection.disconnect() // captures socket #1, blocks in the drain
    await new Promise(setImmediate) // let it reach and start awaiting the drain

    await connection.disconnect() // captures socket #1 too; its own drain is already resolved

    await connection.connect() // socket #2 — reconnected while the first disconnect() is stuck
    const liveSocket = sockets[1]
    expect(liveSocket.listenerCount('end')).toBeGreaterThan(0)

    releaseFirstDrain()
    await firstDisconnect // its stale continuation must act on socket #1, not socket #2

    expect(connection.connectionStatus).toEqual(CONNECTION_STATUS.CONNECTED)
    expect(connection.isConnected()).toBe(true)
    expect(liveSocket.listenerCount('data')).toBeGreaterThan(0)
    expect(liveSocket.listenerCount('end')).toBeGreaterThan(0)
    expect(liveSocket.listenerCount('timeout')).toBeGreaterThan(0)

    await connection.disconnect()
  })

  test('disconnect() rejects an in-flight connect() attempt with a retriable KafkaJSConnectionError, not KafkaJSConnectionClosedError (which src/cluster/ branches do not match by name)', async () => {
    const pendingSockets = []
    const neverConnectingFactory = () => {
      const socket = new EventEmitter()
      socket.write = () => true
      socket.end = () => {}
      socket.unref = () => {}
      pendingSockets.push(socket)
      // onConnect is deliberately never called: the attempt stays in flight until disconnect()
      // aborts it.
      return socket
    }
    const connection = new Connection(connectionOptsWith(neverConnectingFactory))

    const connectPromise = connection.connect()
    await connection.disconnect()

    await expect(connectPromise).rejects.toBeInstanceOf(KafkaJSConnectionError)
    await expect(connectPromise).rejects.toMatchObject({
      name: 'KafkaJSConnectionError',
      retriable: true,
    })
  })

  test('connect() issued while a disconnect() is mid-flight rejects instead of resolving on a connection that ends up disconnected', async () => {
    const { factory } = createTrackingSocketFactory()
    const connection = new Connection(connectionOptsWith(factory))

    await connection.connect()

    let releaseDrain
    const pendingDrain = new Promise((resolve) => {
      releaseDrain = resolve
    })
    connection.requestQueue.waitForPendingRequests = () => pendingDrain

    const disconnectPromise = connection.disconnect()
    expect(connection.connectionStatus).toEqual(CONNECTION_STATUS.DISCONNECTING)

    await expect(connection.connect()).rejects.toBeInstanceOf(KafkaJSConnectionError)

    releaseDrain()
    await disconnectPromise
  })

  test('a send() call issued during the disconnect() drain window rejects instead of hanging forever', async () => {
    const { factory } = createTrackingSocketFactory()
    const connection = new Connection(connectionOptsWith(factory))
    await connection.connect()

    // Blocks the drain step so the send() below lands squarely in the window where
    // connectionStatus is DISCONNECTING but the queue has not been destroyed yet — the exact
    // interleaving that used to let a request slip past both failIfNotConnected() checks, get
    // written to the socket, and then have its listeners stripped out from under it with no
    // response path, no close path and no timeout timer, so it never settled.
    let releaseDrain
    const pendingDrain = new Promise((resolve) => {
      releaseDrain = resolve
    })
    connection.requestQueue.waitForPendingRequests = () => pendingDrain

    const disconnectPromise = connection.disconnect()
    expect(connection.connectionStatus).toEqual(CONNECTION_STATUS.DISCONNECTING)

    const request = {
      apiKey: 3,
      apiName: 'Metadata',
      apiVersion: 1,
      expectResponse: () => true,
      encode: async () => new Encoder().writeInt8(0x2a),
    }
    const response = { decode: async (raw) => raw, parse: async (data) => data }

    // Races the send() against a window far shorter than requestTimeout (200ms, set in
    // connectionOptsWith), so a pass here can only mean the rejection was immediate — not the
    // natural per-request timeout arriving well over three requestTimeouts later.
    const outcome = await Promise.race([
      connection.send({ request, response }).then(
        () => ({ settled: 'resolved' }),
        (error) => ({ settled: 'rejected', error })
      ),
      new Promise((resolve) => setTimeout(() => resolve({ settled: 'timed out' }), 50)),
    ])

    expect(outcome.settled).toEqual('rejected')
    expect(outcome.error).toBeInstanceOf(KafkaJSConnectionError)
    expect(outcome.error.message).toEqual('Not connected')

    releaseDrain()
    await disconnectPromise
  })

  test('a response that arrives while disconnect() drains pending requests still settles that request, so the drain finishes', async () => {
    const { factory, sockets } = createTrackingSocketFactory()
    const connection = new Connection(connectionOptsWith(factory))
    await connection.connect()
    const writes = []
    sockets[0].write = (buffer) => {
      writes.push(buffer)
      return true
    }

    const request = {
      apiKey: 3,
      apiName: 'Metadata',
      apiVersion: 1,
      expectResponse: () => true,
      encode: async () => new Encoder().writeInt8(0x2a),
    }
    const response = { decode: async (raw) => raw, parse: async (data) => data }
    const inFlight = connection.send({ request, response })
    await new Promise((resolve) => setImmediate(resolve))
    expect(writes).toHaveLength(1)

    // disconnect() must keep listening for the reply to the request it is waiting on: dropping it
    // as stale leaves the drain waiting for a request that can never complete.
    const disconnectPromise = connection.disconnect()
    const { correlationId } = readRequestHeader(writes[0])
    respondWithFrame({ socket: sockets[0] }, correlationId, 0, Buffer.from([0x01]))

    await expect(inFlight).resolves.toBeDefined()
    const outcome = await Promise.race([
      disconnectPromise.then(() => 'disconnected'),
      new Promise((resolve) => setTimeout(() => resolve('timed out'), 500)),
    ])
    expect(outcome).toEqual('disconnected')
    expect(connection.connectionStatus).toEqual(CONNECTION_STATUS.DISCONNECTED)
  })
})

describe('ConnectionPool > send', () => {
  test('reconnects a caller-supplied connection that died since the caller resolved it', async () => {
    const { factory } = createTrackingSocketFactory()
    const pool = new ConnectionPool(connectionOptsWith(factory))
    const connection = await pool.getConnection(0)
    const send = jest.spyOn(connection, 'send').mockResolvedValue('response')

    // The window `Broker[SEND_REQUEST]` opens: the connection is resolved, then an ApiVersions
    // round trip is awaited, and the socket can die before the request is dispatched.
    await connection.disconnect()
    expect(connection.isConnected()).toBe(false)

    await expect(pool.send({ request: { apiKey: 1 } }, connection)).resolves.toEqual('response')
    expect(connection.isConnected()).toBe(true)
    expect(send).toHaveBeenCalledTimes(1)

    await pool.destroy()
  })
})

describe('ConnectionPool > generation', () => {
  test('is the sum of both pooled connections generations, and changes when either is replaced', async () => {
    const { factory } = createTrackingSocketFactory()
    const pool = new ConnectionPool(connectionOptsWith(factory))

    expect(pool.generation).toEqual(0)

    await pool.getConnection(0)
    expect(pool.generation).toEqual(1)

    await pool.getConnection(1)
    expect(pool.generation).toEqual(2)

    await pool.destroy()
  })
})
