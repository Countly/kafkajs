jest.mock('../../broker/saslAuthenticator')

const SASLAuthenticator = require('../../broker/saslAuthenticator')
const Connection = require('../connection')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const Encoder = require('../../protocol/encoder')
const { newLogger, createFakeSocketFactory, readRequestHeader } = require('testHelpers')

/** A promise and the function that resolves it. */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

/** Lets every pending promise callback run. */
const settle = () => new Promise((done) => setImmediate(done))

/**
 * A connected `Connection` over a fake socket that records what is written.
 * @param {object} [options] extra `Connection` options
 */
const connectedConnection = async (options = {}) => {
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
    ...options,
  })
  await connection.connect()
  return { connection, state }
}

/**
 * A Produce v7 request and its response codec, for `partitions` of `topic-a`, one record each;
 * more partitions take longer to encode. `acks: 0`, so nothing waits for an answer.
 * @param {number[]} partitions
 */
const produceRequest = (partitions) =>
  requests.Produce.protocol({ version: 7 })({
    acks: 0,
    timeout: 30000,
    compression: 0,
    producerId: 1000,
    producerEpoch: 0,
    topicData: [
      {
        topic: 'topic-a',
        partitions: partitions.map((partition) => ({
          partition,
          firstSequence: 0,
          messages: [{ value: `value-${partition}`, timestamp: 1700000000000 }],
        })),
      },
    ],
  })

describe('Network > Connection > send order', () => {
  beforeEach(() => {
    SASLAuthenticator.mockReset()
  })

  test('requests reach the socket in the order send() was called, whatever they take to encode', async () => {
    const { connection, state } = await connectedConnection()
    const larger = produceRequest([0, 1, 2, 3])
    const smaller = produceRequest([0])

    const sends = [connection.send(larger), connection.send(smaller)].map((sent) =>
      sent.catch(() => {})
    )
    await settle()
    await connection.disconnect()
    await Promise.all(sends)

    const written = state.writes.map((frame) => readRequestHeader(frame))
    expect(written.map(({ correlationId }) => correlationId)).toEqual([0, 1])
    expect(state.writes[0].length).toBeGreaterThan(state.writes[1].length)
  })

  test('requests sent while the connection authenticates are queued in call order once it has', async () => {
    const authenticated = deferred()
    SASLAuthenticator.mockImplementation(() => ({
      authenticate: () => authenticated.promise,
      sessionLifetime: 0,
    }))
    const { connection, state } = await connectedConnection({
      sasl: { mechanism: 'plain', username: 'user', password: 'secret' },
    })

    const sends = [
      connection.send(produceRequest([0, 1, 2, 3])),
      connection.send(produceRequest([0])),
    ].map((sent) => sent.catch(() => {}))
    await settle()
    expect(state.writes).toEqual([])

    authenticated.resolve()
    await settle()
    await connection.disconnect()
    await Promise.all(sends)

    expect(SASLAuthenticator).toHaveBeenCalledTimes(1)
    expect(state.writes.map((frame) => readRequestHeader(frame).correlationId)).toEqual([0, 1])
    expect(state.writes[0].length).toBeGreaterThan(state.writes[1].length)
  })

  test('requests encode at once, side by side, and are still queued in call order', async () => {
    const { connection, state } = await connectedConnection()
    const started = []
    const releases = []
    const heldEncode = (partition) => {
      const plain = produceRequest([partition])
      const release = deferred()
      releases.push(release)
      return {
        ...plain,
        request: {
          ...plain.request,
          encode: async () => {
            started.push(partition)
            await release.promise
            return plain.request.encode()
          },
        },
      }
    }

    const sends = [connection.send(heldEncode(0)), connection.send(heldEncode(1))]
    await settle()
    expect(started).toEqual([0, 1])
    expect(state.writes).toEqual([])

    releases[1].resolve()
    await settle()
    expect(state.writes).toEqual([])
    releases[0].resolve()
    await Promise.all(sends)
    await connection.disconnect()

    expect(state.writes.map((frame) => readRequestHeader(frame).correlationId)).toEqual([0, 1])
  })

  test('a failing authentication is attempted once, however many requests wait for it', async () => {
    const failure = new Error('authentication failed')
    const authenticate = jest.fn(async () => {
      await settle()
      throw failure
    })
    SASLAuthenticator.mockImplementation(() => ({ authenticate, sessionLifetime: 0 }))
    const { connection, state } = await connectedConnection({
      sasl: { mechanism: 'plain', username: 'user', password: 'secret' },
    })

    const outcomes = await Promise.all(
      [0, 1, 2, 3, 4].map((partition) =>
        connection.send(produceRequest([partition])).catch((e) => e)
      )
    )
    await connection.disconnect()

    expect(authenticate).toHaveBeenCalledTimes(1)
    expect(outcomes).toEqual([failure, failure, failure, failure, failure])
    expect(state.writes).toEqual([])
  })

  test('what authentication itself sends is not held behind the requests waiting for it', async () => {
    const saslRequest = (apiKey, apiName) => ({
      request: {
        apiKey,
        apiVersion: 1,
        apiName,
        expectResponse: () => false,
        encode: async () => new Encoder().writeString('PLAIN'),
      },
      response: {},
    })
    SASLAuthenticator.mockImplementation((connection) => ({
      authenticate: async () => {
        await connection.send(saslRequest(apiKeys.SaslHandshake, 'SaslHandshake'))
        await connection.send(saslRequest(apiKeys.SaslAuthenticate, 'SaslAuthenticate'))
      },
      sessionLifetime: 0,
    }))
    const { connection, state } = await connectedConnection({
      sasl: { mechanism: 'plain', username: 'user', password: 'secret' },
    })

    await Promise.all([connection.send(produceRequest([0])), connection.send(produceRequest([1]))])
    await connection.disconnect()

    expect(state.writes.map((frame) => readRequestHeader(frame).apiKey)).toEqual([
      apiKeys.SaslHandshake,
      apiKeys.SaslAuthenticate,
      apiKeys.Produce,
      apiKeys.Produce,
    ])
  })

  test('a request that fails before it is queued does not hold up the next one', async () => {
    const { connection, state } = await connectedConnection()
    const valid = produceRequest([0])
    const broken = {
      ...valid,
      request: { ...valid.request, encode: async () => Promise.reject(new Error('bad')) },
    }

    const failed = connection.send(broken).catch((e) => e)
    const sent = connection.send(produceRequest([1])).catch(() => {})
    await settle()
    await connection.disconnect()

    expect((await failed).message).toEqual('bad')
    await sent
    expect(state.writes).toHaveLength(1)
  })

  test('a request that fails while the one before it still encodes rejects its caller only', async () => {
    const unhandled = jest.fn()
    process.on('unhandledRejection', unhandled)
    const { connection, state } = await connectedConnection()
    const encoded = deferred()
    const valid = produceRequest([0])
    const slow = {
      ...valid,
      request: {
        ...valid.request,
        encode: async () => {
          await encoded.promise
          return valid.request.encode()
        },
      },
    }
    const broken = {
      ...valid,
      request: { ...valid.request, encode: async () => Promise.reject(new Error('bad')) },
    }

    const sent = connection.send(slow).catch(() => {})
    const failed = connection.send(broken).catch((e) => e)
    await new Promise((done) => setTimeout(done, 10))
    encoded.resolve()
    await sent
    await connection.disconnect()
    process.off('unhandledRejection', unhandled)

    expect((await failed).message).toEqual('bad')
    expect(unhandled).not.toHaveBeenCalled()
    expect(state.writes).toHaveLength(1)
  })

  test('a request whose connection closes while it encodes rejects, and so does the one behind it', async () => {
    const { connection, state } = await connectedConnection()
    const encoded = deferred()
    const valid = produceRequest([0])
    const slow = {
      ...valid,
      request: {
        ...valid.request,
        encode: async () => {
          await encoded.promise
          return valid.request.encode()
        },
      },
    }

    const first = connection.send(slow).catch((e) => e)
    const second = connection.send(produceRequest([1])).catch((e) => e)
    await settle()
    await connection.disconnect()
    encoded.resolve()

    for (const error of [await first, await second]) {
      expect(error).toMatchObject({ name: 'KafkaJSConnectionError', message: 'Not connected' })
    }
    expect(state.writes).toHaveLength(0)
  })
})
