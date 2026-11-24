const setupPool = require('../../../../testHelpers/setupPool')
const Broker = require('../../index')
const Connection = require('../../../network/connection')
const { lookup } = require('../../../protocol/requests')
const { responseHeaderVersion } = require('../../../protocol/headers')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

/**
 * A real `Broker` over a real `Connection` over a fake socket, with a PINNED ApiVersions table.
 *
 * Pinning matters twice: version negotiation cannot drift as the codec inventory grows, and no
 * ApiVersions round trip has to be answered - `versionsGeneration` is set to the pool's constant
 * generation, which is exactly the condition `Broker`'s send path checks before re-negotiating.
 * Setup gets two minutes to tolerate coverage runner delays; Jest still bounds a hung test.
 *
 * @param {object} options
 * @param {import("../../../../types").ApiVersions} options.versions the table to negotiate against
 * @param {(header: { apiKey: number, apiVersion: number }) => Buffer|null} options.respond builds
 *   the response BODY for one recorded request, which the harness frames with the correct header
 *   version; `null` leaves the request unanswered until `release`
 * @param {number} [options.maxInFlightRequests] the connection's in-flight limit
 * @param {boolean} [options.forwardGuards=false] hand each request's pre-write guard to the
 *   connection, as the production pool does, so its request queue checks it before the write
 * @returns {{ broker: Broker, requests: { apiKey: number, apiVersion: number,
 *   correlationId: number, clientId: string, byteLength: number, body: Buffer }[],
 *   pool: object, connection: Connection, release: (body: Buffer) => Promise<void> }} the broker,
 *   every request it wrote in order, the pool for teardown, the connection, and `release`, which
 *   answers the oldest unanswered request with `body` and then answers what was written meanwhile
 */
const createGroupApiBroker = ({
  versions,
  respond,
  maxInFlightRequests,
  forwardGuards = false,
}) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'test-client',
    connectionTimeout: 120000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    ...(maxInFlightRequests === undefined ? {} : { maxInFlightRequests }),
  })

  const requests = []
  const unanswered = []
  let answered = 0

  const reply = (header, body) =>
    respondWithFrame(
      state,
      header.correlationId,
      responseHeaderVersion(header.apiKey, header.apiVersion),
      body
    )

  const pump = () => {
    while (answered < state.writes.length) {
      const frame = state.writes[answered]
      const header = readRequestHeader(frame)
      answered += 1
      requests.push({ ...header, body: frame.subarray(header.byteLength) })
      const body = respond(header)
      if (body === null) unanswered.push(header)
      else reply(header, body)
    }
  }

  const release = async (body) => {
    reply(unanswered.shift(), body)
    await new Promise(setImmediate)
    pump()
  }

  const pool = {
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    connectionTimeout: 120000,
    generation: 0,
    isConnected: () => connection.isConnected(),
    isAuthenticated: () => true,
    getConnection: async () => {
      if (!connection.isConnected()) {
        await connection.connect()
      }
      return connection
    },
    getConnectionByRequest: async () => pool.getConnection(),
    send: async (protocolRequest, _connection, options) => {
      const established = await pool.getConnection()
      const guard = forwardGuards ? options?.guard : undefined
      const promise = established.send(guard ? { ...protocolRequest, guard } : protocolRequest)
      await new Promise(setImmediate)
      pump()
      return promise
    },
    setVersions: () => {},
    destroy: async () => connection.disconnect(),
  }

  const broker = new Broker({ connectionPool: setupPool(pool), logger: newLogger(), versions })
  broker.lookupRequest = lookup(versions)
  broker.versionsGeneration = pool.generation

  return { broker, requests, pool, connection, release }
}

module.exports = { createGroupApiBroker }
