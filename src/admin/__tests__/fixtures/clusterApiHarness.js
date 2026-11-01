const Broker = require('../../../broker')
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
 *
 * @param {object} options
 * @param {import("../../../../types").ApiVersions} options.versions the table to negotiate against
 * @param {(header: { apiKey: number, apiVersion: number }) => Buffer} options.respond builds the
 *   response BODY for one recorded request; the harness frames it with the correct header version
 * @returns {{ broker: Broker, requests: { apiKey: number, apiVersion: number,
 *   correlationId: number, clientId: string, byteLength: number, body: Buffer }[],
 *   pool: object }} the broker, every request it wrote in order, and the pool for teardown
 */
const createClusterApiBroker = ({ versions, respond }) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'test-client',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
  })

  const requests = []
  let answered = 0

  const pump = () => {
    while (answered < state.writes.length) {
      const frame = state.writes[answered]
      const header = readRequestHeader(frame)
      answered += 1
      requests.push({ ...header, body: frame.subarray(header.byteLength) })
      respondWithFrame(
        state,
        header.correlationId,
        responseHeaderVersion(header.apiKey, header.apiVersion),
        respond(header)
      )
    }
  }

  const pool = {
    pool: [connection],
    setupPolicy: { timeoutCeiling: 1000, waitCeiling: 0 },
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    connectionTimeout: 1000,
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
    send: async (protocolRequest) => {
      const established = await pool.getConnection()
      const promise = established.send(protocolRequest)
      await new Promise(setImmediate)
      pump()
      return promise
    },
    setVersions: () => {},
    destroy: async () => connection.disconnect(),
  }

  const broker = new Broker({ connectionPool: pool, logger: newLogger(), versions })
  broker.lookupRequest = lookup(versions)
  broker.versionsGeneration = pool.generation

  return { broker, requests, pool }
}

module.exports = { createClusterApiBroker }
