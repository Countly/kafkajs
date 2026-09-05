const { EventEmitter } = require('events')
const Broker = require('../index')
const ConnectionPool = require('../../network/connectionPool')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger, readRequestHeader } = require('testHelpers')

/** A v3/v4-shaped ApiVersions body: compact array of api keys, throttle time, tag buffer. */
const flexibleBody = (entries) =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntArray(
      entries.map(({ apiKey, minVersion, maxVersion }) =>
        new Encoder()
          .writeInt16(apiKey)
          .writeInt16(minVersion)
          .writeInt16(maxVersion)
          .writeTaggedFields()
      )
    )
    .writeInt32(0)
    .writeTaggedFields().buffer

/**
 * A fake socket factory whose sockets auto-answer every ApiVersions request (header v0, per
 * KIP-511) with a fixed table, and count how many were sent. Every other request is left
 * unanswered — this suite never sends one.
 */
const createAutoAnsweringSocketFactory = () => {
  let apiVersionsRequests = 0
  const factory = ({ onConnect }) => {
    const socket = new EventEmitter()
    socket.write = (buffer) => {
      const header = readRequestHeader(Buffer.from(buffer))
      if (header.apiKey === apiKeys.ApiVersions) {
        apiVersionsRequests += 1
        const body = flexibleBody([{ apiKey: apiKeys.ApiVersions, minVersion: 0, maxVersion: 4 }])
        const responseHeader = new Encoder().writeInt32(header.correlationId)
        const framed = new Encoder().writeEncoder(responseHeader).writeBuffer(body)
        setImmediate(() =>
          socket.emit('data', new Encoder().writeInt32(framed.size()).writeEncoder(framed).buffer)
        )
      }
      return true
    }
    socket.end = () => {}
    socket.unref = () => {}
    setImmediate(onConnect)
    return socket
  }
  return { factory, apiVersionsRequestCount: () => apiVersionsRequests }
}

describe('Broker > connect re-negotiation over a real ConnectionPool', () => {
  test('re-negotiates after the pool generation changes, and not otherwise', async () => {
    const { factory, apiVersionsRequestCount } = createAutoAnsweringSocketFactory()
    const connectionPool = new ConnectionPool({
      host: '127.0.0.1',
      port: 9092,
      logger: newLogger(),
      socketFactory: factory,
      clientId: 'test-client',
      connectionTimeout: 1000,
      requestTimeout: 1000,
      enforceRequestTimeout: false,
    })
    const broker = new Broker({ connectionPool, logger: newLogger() })

    await broker.connect()
    expect(apiVersionsRequestCount()).toEqual(1)
    const firstGeneration = broker.versionsGeneration
    expect(firstGeneration).toEqual(connectionPool.generation)

    // Already connected: no new socket, no new negotiation.
    await broker.connect()
    expect(apiVersionsRequestCount()).toEqual(1)
    expect(broker.versionsGeneration).toEqual(firstGeneration)

    // The pool is re-established: its real generation (Connection#generation summed by
    // ConnectionPool#generation) moves, so connect() re-negotiates.
    await broker.disconnect()
    await broker.connect()

    expect(apiVersionsRequestCount()).toEqual(2)
    expect(broker.versionsGeneration).toBeGreaterThan(firstGeneration)
    expect(broker.versionsGeneration).toEqual(connectionPool.generation)

    await broker.disconnect()
  })
})
