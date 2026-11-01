const setupPool = require('../../../testHelpers/setupPool')
const Broker = require('../index')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')

const createFakeConnection = (onApiVersions) => ({
  connected: false,
  generation: 0,
  /** Returns whether this fake socket is established. @returns {boolean} */
  isConnected() {
    return this.connected
  },
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
  throwIfRetired: () => {},
  continueGenerationFrom: () => {},
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
  /**
   * Acquires the connection selected for this request, preserving the pool's generation rules.
   * @param {{ request: { apiKey: number } }} protocolRequest
   * @returns {Promise<object>} the established fake connection
   */
  async getConnectionByRequest({ request: { apiKey } }) {
    return this.getConnection(apiKey === apiKeys.Fetch ? 1 : 0)
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
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })

    await broker.connect()
    await broker.connect()

    expect(apiVersionsCalls).toEqual(1)
  })

  test('re-negotiates after the control socket is replaced', async () => {
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })

    await broker.connect()
    connections[0].connected = false // the broker closed the socket
    await broker.connect()

    expect(apiVersionsCalls).toEqual(2)
  })

  test('re-negotiates once both sockets have been replaced', async () => {
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })

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

  test('re-negotiates when only the other socket has been reconnected, even though the control socket is still up', async () => {
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })

    await broker.connect()
    // The pool re-establishes index 1 (Fetch) on its own; the control socket (index 0) is
    // untouched. isConnected() now also compares the negotiated generation against the pool's,
    // so this alone is enough to report not-connected and force a re-negotiation on both sockets.
    await pool.getConnection(1)

    expect(broker.isConnected()).toBe(false)

    await broker.connect()

    expect(apiVersionsCalls).toEqual(2)
    expect(pool.generation).toEqual(2)
  })

  test('re-negotiates even when versions were injected by the broker pool', async () => {
    const broker = new Broker({
      connectionPool: setupPool(pool),
      logger: newLogger(),
      versions: { 18: { minVersion: 0, maxVersion: 2 } },
    })

    await broker.connect()

    expect(apiVersionsCalls).toEqual(1)
    expect(broker.versions[18].maxVersion).toEqual(4)
  })

  test('rebuilds lookupRequest from the fresh table and shares it with both sockets', async () => {
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })

    await broker.connect()
    const first = broker.lookupRequest
    connections[0].connected = false
    await broker.connect()

    expect(broker.lookupRequest).not.toBe(first)
    expect(pool.setVersions).toHaveBeenLastCalledWith(broker.versions)
  })

  test('re-negotiates after the connection pool is replaced wholesale, even if the new pool reaches the same generation number', async () => {
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })

    await broker.connect()
    expect(apiVersionsCalls).toEqual(1)
    expect(broker.versionsGeneration).toEqual(1)

    // This fake pool counts from 0, so after one connect it reaches 1 — the exact number
    // `versionsGeneration` already holds. Without resetting it on replacement, connect() would
    // wrongly treat the new pool's table as already negotiated.
    const replacementConnections = [
      createFakeConnection(onApiVersions),
      createFakeConnection(onApiVersions),
    ]
    const replacementPool = createFakePool(replacementConnections)
    broker.replaceConnectionPool(setupPool(replacementPool))

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
    const broker = new Broker({ connectionPool: setupPool(slowPool), logger: newLogger() })

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
