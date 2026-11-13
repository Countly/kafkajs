const Connection = require('../connection')
const { requests } = require('../../protocol/requests')
const { newLogger, createFakeSocketFactory, readRequestHeader } = require('testHelpers')

/** A promise and the function that resolves it. */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const settle = () => new Promise((done) => setImmediate(done))

const HUNG = Symbol('hung')

/**
 * Resolves with `HUNG` when `promise` has not settled within `ms`, so a request that is stuck
 * fails its test instead of waiting for the test timeout.
 * @param {Promise} promise
 * @param {number} ms
 */
const within = (promise, ms) => {
  let timer
  const timeout = new Promise((done) => {
    timer = setTimeout(() => done(HUNG), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

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
    ...options,
  })
  await connection.connect()
  return { connection, state }
}

/**
 * A Produce v7 request for one partition of `topic-a`: the answer is awaited when `acks` is not 0.
 * @param {number} [partition]
 * @param {number} [acks]
 */
const produceRequest = (partition = 0, acks = 0) =>
  requests.Produce.protocol({ version: 7 })({
    acks,
    timeout: 30000,
    compression: 0,
    producerId: 1000,
    producerEpoch: 0,
    topicData: [
      {
        topic: 'topic-a',
        partitions: [
          {
            partition,
            firstSequence: 0,
            messages: [{ value: `value-${partition}`, timestamp: 1700000000000 }],
          },
        ],
      },
    ],
  })

/**
 * The request, with an encoder that finishes only once `gate` is resolved.
 * @param {ReturnType<typeof produceRequest>} protocol
 * @param {{ promise: Promise }} gate
 */
const heldEncode = (protocol, gate) => ({
  ...protocol,
  request: {
    ...protocol.request,
    encode: async () => {
      await gate.promise
      return protocol.request.encode()
    },
  },
})

const reconnect = async (connection) => {
  await connection.disconnect()
  await connection.connect()
}

describe('Network > Connection > the ordering of requests belongs to one connection attempt', () => {
  test('a request still encoding when the connection was replaced does not hold up the new one, and is never written', async () => {
    const { connection, state } = await connectedConnection()
    const gate = deferred()

    const stale = connection.send(heldEncode(produceRequest(0), gate)).catch((e) => e)
    await settle()
    await reconnect(connection)
    const fresh = await within(
      connection.send(produceRequest(1)).then(() => 'sent'),
      200
    )
    const writtenBeforeRelease = state.writes.length
    gate.resolve()
    const staleResult = await stale
    await connection.disconnect()

    expect(fresh).toEqual('sent')
    expect(writtenBeforeRelease).toEqual(1)
    expect(staleResult).toMatchObject({ name: 'KafkaJSConnectionError', message: 'Not connected' })
    expect(state.writes).toHaveLength(1)
    expect(readRequestHeader(state.writes[0]).correlationId).toEqual(1)
  })

  test('a request behind the stale one is refused too, and requests after the reconnect keep their order', async () => {
    const { connection, state } = await connectedConnection()
    const gate = deferred()
    const slow = deferred()

    const stale = [
      connection.send(heldEncode(produceRequest(0), gate)).catch((e) => e),
      connection.send(produceRequest(1)).catch((e) => e),
    ]
    await settle()
    await reconnect(connection)
    const fresh = [
      connection.send(heldEncode(produceRequest(2), slow)),
      connection.send(produceRequest(3)),
    ]
    await settle()
    expect(state.writes).toHaveLength(0)
    slow.resolve()
    await within(Promise.all(fresh), 200)
    gate.resolve()
    const staleResults = await Promise.all(stale)
    await connection.disconnect()

    for (const result of staleResults) {
      expect(result).toMatchObject({ name: 'KafkaJSConnectionError', message: 'Not connected' })
    }
    expect(state.writes.map((frame) => readRequestHeader(frame).correlationId)).toEqual([2, 3])
  })

  test('an old encoder settling cannot reset a replacement attempt queue', async () => {
    const { connection, state } = await connectedConnection()
    const oldGate = deferred()
    const freshGate = deferred()
    const stale = connection.send(heldEncode(produceRequest(0), oldGate)).catch((e) => e)
    try {
      await settle()
      await reconnect(connection)
      const first = connection.send(heldEncode(produceRequest(1), freshGate))
      await settle()
      oldGate.resolve()
      expect(await stale).toMatchObject({
        name: 'KafkaJSConnectionError',
        message: 'Not connected',
      })
      const second = connection.send(produceRequest(2))
      await settle()
      expect(state.writes).toHaveLength(0)
      freshGate.resolve()
      await Promise.all([first, second])
      expect(state.writes.map((frame) => readRequestHeader(frame).correlationId)).toEqual([1, 2])
    } finally {
      oldGate.resolve()
      freshGate.resolve()
      await connection.disconnect({ drainTimeout: 0 })
    }
  })

  test('the new request keeps its own requestTimeout while an old request is still encoding', async () => {
    const { connection, state } = await connectedConnection({ requestTimeout: 50 })
    const gate = deferred()

    const stale = connection.send(heldEncode(produceRequest(0, 1), gate)).catch((e) => e)
    await settle()
    await reconnect(connection)
    const fresh = await within(
      connection.send(produceRequest(1, 1)).catch((e) => e),
      1000
    )
    const writtenBeforeRelease = state.writes.length
    gate.resolve()
    await stale
    await connection.disconnect()

    expect(fresh).toMatchObject({ name: 'KafkaJSRequestTimeoutError' })
    expect(writtenBeforeRelease).toEqual(1)
    expect(state.writes).toHaveLength(1)
  })

  test('a socket the broker closed ends the attempt the same way', async () => {
    const { connection, state } = await connectedConnection()
    const gate = deferred()

    const stale = connection.send(heldEncode(produceRequest(0), gate)).catch((e) => e)
    await settle()
    state.socket.emit('end')
    while (connection.isConnected()) {
      await settle()
    }
    await connection.connect()
    const fresh = await within(
      connection.send(produceRequest(1)).then(() => 'sent'),
      200
    )
    gate.resolve()
    const staleResult = await stale
    await connection.disconnect()

    expect(fresh).toEqual('sent')
    expect(staleResult).toMatchObject({ name: 'KafkaJSConnectionError', message: 'Not connected' })
    expect(state.writes).toHaveLength(1)
  })

  test('requests sent on a connection that was never replaced are written in order, as before', async () => {
    const { connection, state } = await connectedConnection()
    const gate = deferred()

    const sends = [
      connection.send(heldEncode(produceRequest(0), gate)),
      connection.send(produceRequest(1)),
    ]
    await settle()
    expect(state.writes).toHaveLength(0)
    gate.resolve()
    await Promise.all(sends)
    await connection.disconnect()

    expect(state.writes.map((frame) => readRequestHeader(frame).correlationId)).toEqual([0, 1])
  })
})
