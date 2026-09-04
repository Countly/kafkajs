const Broker = require('../index')
const Connection = require('../../network/connection')
const Encoder = require('../../protocol/encoder')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

const UNSUPPORTED_VERSION = 35
const INVALID_REQUEST = 42

/** A v0-shaped ApiVersions body: INT16 error code, then a classic array of (key, min, max). */
const v0Body = (errorCode, entries) =>
  new Encoder().writeInt16(errorCode).writeArray(
    entries.map(({ apiKey, minVersion, maxVersion }) =>
      new Encoder().writeInt16(apiKey).writeInt16(minVersion).writeInt16(maxVersion)
    ),
    'object'
  ).buffer

/** A v3/v4-shaped body: compact array of api keys, throttle time, tag buffer. */
const flexibleBody = (errorCode, entries) =>
  new Encoder()
    .writeInt16(errorCode)
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
 * Builds a Broker over a real Connection on a fake socket. `answer` is called with the request
 * header of every outgoing frame and returns the body bytes to reply with; ApiVersions responses
 * are always framed with header v0.
 */
const createFakeBroker = (answer) => {
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

  let answered = 0
  const pump = async () => {
    // Answer every frame written so far, in order.
    while (answered < state.writes.length) {
      const header = readRequestHeader(state.writes[answered])
      answered += 1
      respondWithFrame(state, header.correlationId, 0, answer(header))
    }
  }

  const connectionPool = {
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    connectionTimeout: 1000,
    isConnected: () => connection.isConnected(),
    isAuthenticated: () => true,
    getConnection: async () => {
      await connection.connect()
      return connection
    },
    send: async (protocolRequest) => {
      const promise = connection.send(protocolRequest)
      await new Promise(setImmediate)
      await pump()
      return promise
    },
    setVersions: jest.fn(),
    destroy: jest.fn(async () => connection.disconnect()),
  }

  return { broker: new Broker({ connectionPool, logger: newLogger() }), connectionPool, state }
}

const versionsAsked = (state) => state.writes.map((frame) => readRequestHeader(frame).apiVersion)

describe('Broker > apiVersions (KIP-511 fallback)', () => {
  test('uses the highest implemented version when the broker accepts it', async () => {
    const { broker, connectionPool, state } = createFakeBroker(({ apiVersion }) => {
      expect(apiVersion).toEqual(4)
      return flexibleBody(0, [{ apiKey: 18, minVersion: 0, maxVersion: 4 }])
    })
    await connectionPool.getConnection()

    await expect(broker.apiVersions()).resolves.toEqual({ 18: { minVersion: 0, maxVersion: 4 } })
    expect(versionsAsked(state)).toEqual([4])
  })

  test('retries exactly once at min(ourMax, brokerMax) on UNSUPPORTED_VERSION', async () => {
    const asked = []
    const { broker, connectionPool } = createFakeBroker(({ apiVersion }) => {
      asked.push(apiVersion)
      return apiVersion === 4
        ? v0Body(UNSUPPORTED_VERSION, [{ apiKey: 18, minVersion: 0, maxVersion: 2 }])
        : v0Body(0, [{ apiKey: 18, minVersion: 0, maxVersion: 2 }]) // v2 is classic: same errorCode + INT32 array shape as v0
    })
    await connectionPool.getConnection()

    await expect(broker.apiVersions()).resolves.toEqual({ 18: { minVersion: 0, maxVersion: 2 } })
    expect(asked).toEqual([4, 2])
  })

  test('falls back to v0 when the broker sends an empty api_keys array', async () => {
    const asked = []
    const { broker, connectionPool } = createFakeBroker(({ apiVersion }) => {
      asked.push(apiVersion)
      return apiVersion === 4
        ? v0Body(UNSUPPORTED_VERSION, [])
        : v0Body(0, [{ apiKey: 18, minVersion: 0, maxVersion: 0 }])
    })
    await connectionPool.getConnection()

    await broker.apiVersions()
    expect(asked).toEqual([4, 0])
  })

  test('falls back to v0 rather than throwing on a malformed api_keys entry', async () => {
    const asked = []
    const { broker, connectionPool } = createFakeBroker(({ apiVersion }) => {
      asked.push(apiVersion)
      return apiVersion === 4
        ? v0Body(UNSUPPORTED_VERSION, [{ apiKey: 18, minVersion: 0, maxVersion: -1 }])
        : v0Body(0, [{ apiKey: 18, minVersion: 0, maxVersion: 0 }])
    })
    await connectionPool.getConnection()

    await broker.apiVersions()
    expect(asked).toEqual([4, 0])
  })

  test('does not retry a second time, and connect() surfaces the failure', async () => {
    const asked = []
    const { broker } = createFakeBroker(({ apiVersion }) => {
      asked.push(apiVersion)
      return v0Body(UNSUPPORTED_VERSION, [{ apiKey: 18, minVersion: 0, maxVersion: 2 }])
    })

    // connect() negotiates for itself, so its rejection *is* the assertion here.
    await expect(broker.connect()).rejects.toThrow('The version of API is not supported')
    expect(asked).toEqual([4, 2])
  })

  test('INVALID_REQUEST closes the connection and is non-retriable', async () => {
    const { broker, connectionPool } = createFakeBroker(() =>
      // 42 is returned at the version we asked for, so the body is flexible
      flexibleBody(INVALID_REQUEST, [])
    )

    const promise = broker.connect()
    await expect(promise).rejects.toThrow(/KIP-511|software name/)
    await expect(promise).rejects.toHaveProperty('retriable', false)
    await expect(promise).rejects.toHaveProperty('type', 'INVALID_REQUEST')
    await expect(promise).rejects.toHaveProperty('code', 42)
    await expect(promise).rejects.toHaveProperty('cause.type', 'INVALID_REQUEST')
    expect(connectionPool.destroy).toHaveBeenCalled()
  })
})
