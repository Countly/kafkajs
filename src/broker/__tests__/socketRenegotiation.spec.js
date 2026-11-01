const setupPool = require('../../../testHelpers/setupPool')
const Broker = require('../index')
const Connection = require('../../network/connection')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

/** An ApiVersions v3/v4-shaped body advertising Metadata and Fetch. */
const apiVersionsBody = () =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      new Encoder().writeInt16(apiKeys.Metadata).writeInt16(0).writeInt16(13).writeTaggedFields(),
      new Encoder().writeInt16(apiKeys.Fetch).writeInt16(4).writeInt16(11).writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer

/** A Metadata v13 body with no brokers and no topics: enough to decode, nothing to assert on. */
const metadataBody = () =>
  new Encoder()
    .writeInt32(0)
    .writeUVarIntArray([])
    .writeUVarIntString('cluster')
    .writeInt32(0)
    .writeUVarIntArray([])
    .writeInt16(0)
    .writeTaggedFields().buffer

/**
 * A two-socket pool over two fake sockets, whose `generation` is the sum of both — the real
 * `ConnectionPool` shape (its `generation` getter). `bumpIndex1()` simulates the pool
 * re-establishing the Fetch socket on its own, without any `Broker#connect()`.
 */
const createPool = () => {
  const sockets = [createFakeSocketFactory(), createFakeSocketFactory()]
  const generations = [0, 0]
  const connections = sockets.map(
    ({ factory }) =>
      new Connection({
        host: '127.0.0.1',
        port: 9092,
        logger: newLogger(),
        socketFactory: factory,
        clientId: 'test-client',
        connectionTimeout: 1000,
        requestTimeout: 1000,
        enforceRequestTimeout: false,
      })
  )

  const answered = [0, 0]
  const pump = async (index) => {
    const state = sockets[index].state
    while (answered[index] < state.writes.length) {
      const header = readRequestHeader(state.writes[answered[index]])
      answered[index] += 1
      respondWithFrame(
        state,
        header.correlationId,
        header.apiKey === apiKeys.ApiVersions ? 0 : 1,
        header.apiKey === apiKeys.ApiVersions ? apiVersionsBody() : metadataBody()
      )
    }
  }

  // `pool` is bound before its own methods reference it: `getConnectionByRequest` and `send` both
  // call `pool.getConnection`, which cannot be written inside the literal that defines it.
  const pool = {
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    connectionTimeout: 1000,
    get generation() {
      return generations[0] + generations[1]
    },
    bumpIndex1: () => {
      generations[1] += 1
    },
    isConnected: () => connections.some((c) => c.isConnected()),
    throwIfRetired: () => {},
    isAuthenticated: () => true,
    getConnection: async (index = 0) => {
      if (!connections[index].isConnected()) {
        await connections[index].connect()
        generations[index] += 1
      }
      return connections[index]
    },
    getConnectionByRequest: async ({ request: { apiKey } }) =>
      pool.getConnection({ [apiKeys.Fetch]: 1 }[apiKey] || 0),
    send: async (protocolRequest) => {
      const index = { [apiKeys.Fetch]: 1 }[protocolRequest.request.apiKey] || 0
      const connection = await pool.getConnection(index)
      const promise = connection.send(protocolRequest)
      await new Promise(setImmediate)
      await pump(index)
      return promise
    },
    setVersions: () => {},
    destroy: async () => Promise.all(connections.map((c) => c.disconnect())),
  }

  return { sockets, pool }
}

const countApiVersionsRequests = (state) =>
  state.writes.filter((frame) => readRequestHeader(frame).apiKey === apiKeys.ApiVersions).length

describe('Broker > per-socket ApiVersions re-negotiation', () => {
  test('a reconnected pooled socket re-negotiates before the next request, once', async () => {
    const { sockets, pool } = createPool()
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })

    await broker.connect()
    expect(broker.isConnected()).toBe(true)
    expect(countApiVersionsRequests(sockets[0].state)).toEqual(1)

    // The pool re-establishes the Fetch socket on its own; nothing called Broker#connect().
    pool.bumpIndex1()

    expect(broker.isConnected()).toBe(false)

    await broker.connect()
    expect(countApiVersionsRequests(sockets[0].state)).toEqual(2)
    expect(broker.isConnected()).toBe(true)

    // It converges: the following request re-negotiates nothing.
    await broker.metadata([])
    expect(countApiVersionsRequests(sockets[0].state)).toEqual(2)
  })

  test('a request sent across a generation bump re-connects before dispatching', async () => {
    const { sockets, pool } = createPool()
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })
    await broker.connect()

    pool.bumpIndex1()
    await broker.metadata([])

    // The send path noticed the stale generation and re-negotiated on the way.
    expect(countApiVersionsRequests(sockets[0].state)).toEqual(2)
    expect(broker.versionsGeneration).toEqual(pool.generation)
  })

  test('ApiVersions itself does not re-enter connect()', async () => {
    const { sockets, pool } = createPool()
    const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger() })

    // connect() calls apiVersions() while holding its own non-reentrant lock; a generation check
    // that did not exempt ApiVersions would deadlock here instead of resolving.
    await expect(broker.connect()).resolves.toBeUndefined()
    expect(countApiVersionsRequests(sockets[0].state)).toEqual(1)
  })
})
