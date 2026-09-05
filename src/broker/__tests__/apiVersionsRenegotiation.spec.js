const Broker = require('../index')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')

const createFakeConnection = (onApiVersions) => ({
  connected: false,
  generation: 0,
  authenticate: jest.fn(),
  send: jest.fn(async ({ request }) => {
    if (request.apiName !== 'ApiVersions') throw new Error(`unexpected ${request.apiName}`)
    return onApiVersions()
  }),
})

/** Mirrors ConnectionPool: two connections, a generation that is the sum of theirs. */
const createFakePool = (connections) => ({
  host: 'localhost',
  port: 9092,
  sasl: null,
  connectionTimeout: 1000,
  pool: connections,
  get generation() {
    return connections.reduce((sum, connection) => sum + connection.generation, 0)
  },
  isConnected: () => connections.some((connection) => connection.connected),
  isAuthenticated: () => true,
  getConnection: async (index = 0) => {
    const connection = connections[index]
    if (!connection.connected) {
      connection.connected = true
      connection.generation += 1
    }
    return connection
  },
  send: (protocolRequest) => connections[0].send(protocolRequest),
  setVersions: jest.fn(),
  destroy: jest.fn(),
})

describe('Broker > connect (KIP-35 rule 7)', () => {
  let apiVersionsCalls
  let connections
  let pool

  const onApiVersions = () => {
    apiVersionsCalls += 1
    return { apiVersions: [{ apiKey: 18, minVersion: 0, maxVersion: 4 }] }
  }

  beforeEach(() => {
    apiVersionsCalls = 0
    connections = [createFakeConnection(onApiVersions), createFakeConnection(onApiVersions)]
    pool = createFakePool(connections)
  })

  test('negotiates once per pool, not once per call', async () => {
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })

    await broker.connect()
    await broker.connect()

    expect(apiVersionsCalls).toEqual(1)
  })

  test('re-negotiates after the control socket is replaced', async () => {
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })

    await broker.connect()
    connections[0].connected = false // the broker closed the socket
    await broker.connect()

    expect(apiVersionsCalls).toEqual(2)
  })

  test('re-negotiates once both sockets have been replaced', async () => {
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })

    await broker.connect()
    // Index 1 (Fetch) is brought up inside the pool, as ConnectionPool does when a fetch is
    // dispatched; the pool generation changes.
    await pool.getConnection(1)
    connections[0].connected = false
    connections[1].connected = false
    await broker.connect()

    expect(apiVersionsCalls).toEqual(2)
    expect(pool.generation).toEqual(3)
  })

  test('does NOT re-negotiate while the other socket is still up (recorded limitation)', async () => {
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })

    await broker.connect()
    await pool.getConnection(1)
    // Only the control socket drops. Broker.isConnected() is pool.some(...), so connect()
    // returns at its first `if` and the changed pool generation is never read. This is the
    // documented gap, pinned so a future change to it is deliberate rather than accidental.
    connections[0].connected = false
    await broker.connect()

    expect(apiVersionsCalls).toEqual(1)
    expect(pool.generation).toEqual(2)
  })

  test('re-negotiates even when versions were injected by the broker pool', async () => {
    const broker = new Broker({
      connectionPool: pool,
      logger: newLogger(),
      versions: { 18: { minVersion: 0, maxVersion: 2 } },
    })

    await broker.connect()

    expect(apiVersionsCalls).toEqual(1)
    expect(broker.versions[18].maxVersion).toEqual(4)
  })

  test('rebuilds lookupRequest from the fresh table and shares it with both sockets', async () => {
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })

    await broker.connect()
    const first = broker.lookupRequest
    connections[0].connected = false
    await broker.connect()

    expect(broker.lookupRequest).not.toBe(first)
    expect(pool.setVersions).toHaveBeenLastCalledWith(broker.versions)
  })

  test('re-negotiates after the connection pool is replaced wholesale, even if the new pool reaches the same generation number', async () => {
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })

    await broker.connect()
    expect(apiVersionsCalls).toEqual(1)
    expect(broker.versionsGeneration).toEqual(1)

    // A fresh pool restarts its generation count at 0, so after one connect it reaches 1 —
    // the exact number `versionsGeneration` already holds. Without resetting it on
    // replacement, connect() would wrongly treat the new pool's table as already negotiated.
    const replacementConnections = [
      createFakeConnection(onApiVersions),
      createFakeConnection(onApiVersions),
    ]
    const replacementPool = createFakePool(replacementConnections)
    broker.replaceConnectionPool(replacementPool)

    expect(broker.versionsGeneration).toBeNull()

    await broker.connect()

    expect(apiVersionsCalls).toEqual(2)
    expect(replacementPool.generation).toEqual(1)
    expect(broker.versionsGeneration).toEqual(1)
  })

  test('isConnected() stays false while negotiation is still in flight, even though the socket already reports connected (BrokerPool#connectBroker must not skip connect() and hit notInitializedLookup)', async () => {
    let resolveApiVersions
    const pendingApiVersions = new Promise((resolve) => {
      resolveApiVersions = resolve
    })
    const slowConnections = [
      createFakeConnection(() => pendingApiVersions),
      createFakeConnection(() => pendingApiVersions),
    ]
    const slowPool = createFakePool(slowConnections)
    const broker = new Broker({ connectionPool: slowPool, logger: newLogger() })

    const connectPromise = broker.connect()
    await new Promise(setImmediate)

    // The socket is up (this is what BrokerPool#connectBroker's own isConnected() check saw,
    // pre-fix, as a green light to skip calling connect() entirely) but ApiVersions hasn't
    // answered yet, so lookupRequest is still the one that throws "Broker not connected".
    expect(slowPool.isConnected()).toBe(true)
    expect(broker.isConnected()).toBe(false)
    expect(() => broker.lookupRequest(apiKeys.ApiVersions, requests.ApiVersions)).toThrow(
      'Broker not connected'
    )

    resolveApiVersions({ apiVersions: [{ apiKey: 18, minVersion: 0, maxVersion: 4 }] })
    await connectPromise

    expect(broker.isConnected()).toBe(true)
    expect(() => broker.lookupRequest(apiKeys.ApiVersions, requests.ApiVersions)).not.toThrow()
  })
})
