jest.mock('../../broker/saslAuthenticator')

const SASLAuthenticator = require('../../broker/saslAuthenticator')
const Connection = require('../connection')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const Encoder = require('../../protocol/encoder')
const { KafkaJSError } = require('../../errors')
const {
  newLogger,
  createFakeSocketFactory,
  readRequestHeader,
  respondWithFrame,
} = require('testHelpers')

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

describe('Network > Connection > session at write', () => {
  const SESSION_LIFETIME = '1000'
  const SESSION_REFUSAL = {
    name: 'KafkaJSError',
    retriable: true,
    message:
      'The SASL session was not valid when the request was due to be written; it was not sent',
  }
  const sasl = { mechanism: 'plain', username: 'user', password: 'secret' }
  let connection
  let state

  beforeEach(() => {
    SASLAuthenticator.mockReset()
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
  })

  afterEach(async () => {
    try {
      await connection?.disconnect({ drainTimeout: 0 })
    } finally {
      connection = null
      state = null
      jest.useRealTimers()
    }
  })

  /** A SaslHandshake frame that expects no answer, as the mocked authenticator's exchange. */
  const handshakeRequest = () => ({
    request: {
      apiKey: apiKeys.SaslHandshake,
      apiVersion: 1,
      apiName: 'SaslHandshake',
      expectResponse: () => false,
      encode: async () => new Encoder().writeString('PLAIN'),
    },
    response: {},
  })

  /**
   * A Metadata request that expects an answer; its codec reports `throttle` ms of throttling.
   * @param {number} [throttle=0]
   */
  const answeredRequest = (throttle = 0) => ({
    request: {
      apiKey: apiKeys.Metadata,
      apiVersion: 1,
      apiName: 'Metadata',
      encode: async () => new Encoder().writeInt32(-1),
    },
    response: {
      decode: async () => ({ clientSideThrottleTime: throttle }),
      parse: async (decoded) => decoded,
    },
  })

  /**
   * Mocks an authenticator that writes one SaslHandshake frame and grants a one-second session.
   * @param {Array<Promise<void>|undefined>} [holds] what each successive authentication waits for
   *   before its frame
   * @returns {jest.Mock} called once at the start of each authentication
   */
  const mockAuthenticator = (holds = []) => {
    const authenticate = jest.fn()
    SASLAuthenticator.mockImplementation((owner) => ({
      authenticate: async () => {
        authenticate()
        await holds[authenticate.mock.calls.length - 1]
        await owner.send(handshakeRequest())
      },
      sessionLifetime: SESSION_LIFETIME,
    }))
    return authenticate
  }

  /**
   * Connects over a fake socket with one request in flight at most and no re-authentication margin.
   * @param {object} [options] extra `Connection` options
   */
  const open = async (options = {}) => {
    ;({ connection, state } = await connectedConnection({
      maxInFlightRequests: 1,
      reauthenticationThreshold: 0,
      ...options,
    }))
  }

  /** @returns {number[]} the API key of every frame written, in order */
  const writtenApis = () => state.writes.map((frame) => readRequestHeader(frame).apiKey)

  /** Answers the newest Metadata frame written. */
  const answerMetadata = () => {
    const { correlationId } = state.writes
      .map((frame) => readRequestHeader(frame))
      .filter(({ apiKey }) => apiKey === apiKeys.Metadata)
      .pop()
    respondWithFrame(state, correlationId, 0, Buffer.from('ack'))
  }

  /**
   * Writes an unanswered Metadata request, queues `queued` behind it at the in-flight limit, lets
   * `elapse` ms pass, then answers the Metadata request so the queued one reaches the head.
   * @param {object} queued what to send second
   * @param {number} elapse fake milliseconds between queueing and the answer
   * @param {() => void} [beforeAnswer] runs just before the answer
   * @returns {Promise<*>} the queued request's result, or the error it rejected with
   */
  const queuedBehindAnswer = async (queued, elapse, beforeAnswer = () => {}) => {
    const first = connection.send(answeredRequest())
    await settle()
    const outcome = connection.send(queued).catch((error) => error)
    await settle()
    expect(connection.requestQueue.pending).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(elapse)
    beforeAnswer()
    answerMetadata()
    await first
    return outcome
  }

  test('a request held by the in-flight limit past the session deadline is refused unsent', async () => {
    mockAuthenticator()
    await open({ sasl })

    const outcome = await queuedBehindAnswer(produceRequest([0]), 1500)

    expect(outcome).toBeInstanceOf(KafkaJSError)
    expect(outcome).toMatchObject(SESSION_REFUSAL)
    expect(writtenApis()).toEqual([apiKeys.SaslHandshake, apiKeys.Metadata])
  })

  test('a request held by throttling past the session deadline is refused unsent', async () => {
    mockAuthenticator()
    await open({ sasl })
    const throttled = connection.send(answeredRequest(1500))
    await settle()
    answerMetadata()
    await throttled
    const outcome = connection.send(produceRequest([0])).catch((error) => error)
    await settle()
    expect(connection.requestQueue.pending).toHaveLength(1)

    await jest.advanceTimersByTimeAsync(1500)

    expect(await outcome).toMatchObject(SESSION_REFUSAL)
    expect(writtenApis()).toEqual([apiKeys.SaslHandshake, apiKeys.Metadata])
  })

  test('a request reaching the head while a re-authentication runs is refused unsent', async () => {
    const reauthentication = deferred()
    const authenticate = mockAuthenticator([undefined, reauthentication.promise])
    await open({ sasl, reauthenticationThreshold: 500 })
    const first = connection.send(answeredRequest())
    await settle()
    const held = connection.send(produceRequest([0])).catch((error) => error)
    await settle()
    expect(connection.requestQueue.pending).toHaveLength(1)
    await jest.advanceTimersByTimeAsync(600)
    const next = connection.send(produceRequest([1]))
    await settle()
    expect(authenticate).toHaveBeenCalledTimes(2)

    answerMetadata()
    await first
    reauthentication.resolve()
    await next

    expect(await held).toMatchObject(SESSION_REFUSAL)
    expect(writtenApis()).toEqual([
      apiKeys.SaslHandshake,
      apiKeys.Metadata,
      apiKeys.SaslHandshake,
      apiKeys.Produce,
    ])
  })

  test("a session-refused frame never reaches the caller's frame-time guard call", async () => {
    mockAuthenticator()
    await open({ sasl })
    let committed = false
    const guard = jest.fn((frameLength) => {
      if (frameLength !== undefined) committed = true
      return null
    })

    const outcome = await queuedBehindAnswer({ ...produceRequest([0]), guard }, 1500)

    expect(outcome).toMatchObject(SESSION_REFUSAL)
    expect(committed).toBe(false)
    expect(guard.mock.calls).toEqual([[], []])
  })

  test('a caller refusal that needs no frame length still wins over the session refusal', async () => {
    mockAuthenticator()
    await open({ sasl })
    const own = new KafkaJSError('refused by the caller', { retriable: false })
    let refusing = false
    const guard = jest.fn((frameLength) => (refusing && frameLength === undefined ? own : null))

    const outcome = await queuedBehindAnswer({ ...produceRequest([0]), guard }, 1500, () => {
      refusing = true
    })

    expect(outcome).toBe(own)
    expect(guard.mock.calls).toEqual([[], []])
  })

  test("the caller's frame-time refusal still applies on a valid session", async () => {
    mockAuthenticator()
    await open({ sasl })
    const own = new KafkaJSError('refused at the frame', { retriable: false })
    const guard = jest.fn((frameLength) => (frameLength === undefined ? null : own))

    const outcome = await connection.send({ ...produceRequest([0]), guard }).catch((e) => e)

    expect(outcome).toBe(own)
    expect(guard).toHaveBeenLastCalledWith(expect.any(Number))
    expect(writtenApis()).toEqual([apiKeys.SaslHandshake])
  })

  test('the next send after a refusal re-authenticates before it encodes', async () => {
    const authenticate = mockAuthenticator()
    await open({ sasl })
    const refused = await queuedBehindAnswer(produceRequest([0]), 1500)
    const plain = produceRequest([1])
    const encode = jest.fn(plain.request.encode)

    await connection.send({ ...plain, request: { ...plain.request, encode } })

    expect(refused).toMatchObject(SESSION_REFUSAL)
    expect(authenticate).toHaveBeenCalledTimes(2)
    expect(authenticate.mock.invocationCallOrder[1]).toBeLessThan(
      encode.mock.invocationCallOrder[0]
    )
    expect(writtenApis()).toEqual([
      apiKeys.SaslHandshake,
      apiKeys.Metadata,
      apiKeys.SaslHandshake,
      apiKeys.Produce,
    ])
  })

  test('a negative re-authentication margin still re-authenticates on the next send after a refusal', async () => {
    const authenticate = mockAuthenticator()
    await open({ sasl, reauthenticationThreshold: -5000 })
    const refused = await queuedBehindAnswer(produceRequest([0]), 1500)

    const retried = await connection.send(produceRequest([1])).catch((error) => error)

    expect(refused).toMatchObject(SESSION_REFUSAL)
    expect(retried).toBeUndefined()
    expect(authenticate).toHaveBeenCalledTimes(2)
    expect(writtenApis()).toEqual([
      apiKeys.SaslHandshake,
      apiKeys.Metadata,
      apiKeys.SaslHandshake,
      apiKeys.Produce,
    ])
  })

  test('a negative re-authentication margin does not re-authenticate within the session', async () => {
    const authenticate = mockAuthenticator()
    await open({ sasl, reauthenticationThreshold: -5000 })

    const outcome = await queuedBehindAnswer(produceRequest([0]), 500)

    expect(outcome).toBeUndefined()
    expect(authenticate).toHaveBeenCalledTimes(1)
    expect(writtenApis()).toEqual([apiKeys.SaslHandshake, apiKeys.Metadata, apiKeys.Produce])
  })

  test('a request written within the session is not refused', async () => {
    mockAuthenticator()
    await open({ sasl })

    const outcome = await queuedBehindAnswer(produceRequest([0]), 500)

    expect(outcome).toBeUndefined()
    expect(writtenApis()).toEqual([apiKeys.SaslHandshake, apiKeys.Metadata, apiKeys.Produce])
  })

  test('a connection without SASL never refuses for the session', async () => {
    await open()

    const outcome = await queuedBehindAnswer(produceRequest([0]), 1500)

    expect(outcome).toBeUndefined()
    expect(SASLAuthenticator).not.toHaveBeenCalled()
    expect(writtenApis()).toEqual([apiKeys.Metadata, apiKeys.Produce])
  })
})
