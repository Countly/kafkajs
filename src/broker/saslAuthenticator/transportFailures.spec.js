const SASLAuthenticator = require('./index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createErrorFromCode } = require('../../protocol/error')
const { createLogger, LEVELS } = require('../../loggers')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { Kafka } = require('../../..')
const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')
const Cluster = require('../../cluster')
const { ShareRunner } = require('../../consumer/share/shareRunner')
const { newLogger, connectionOpts } = require('testHelpers')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSConnectionError,
  KafkaJSConnectionClosedError,
  KafkaJSNonRetriableError,
  KafkaJSRequestTimeoutError,
  KafkaJSSASLAuthenticationError,
} = require('../../errors')

/** An inert marker placed in every credential, token and supplier text; never a real secret. */
const SECRET = 'secret-marker-5c2e'

/** The native stub the optional GSSAPI peer resolves to; each GSSAPI case installs its client. */
let gssapiClient
jest.doMock('kerberos/package.json', () => ({ version: '7.0.0' }), { virtual: true })
jest.doMock(
  'kerberos',
  () => ({ GSS_MECH_OID_KRB5: 'oid', initializeClient: async () => gssapiClient }),
  { virtual: true }
)

/** A SaslAuthenticate answer carrying one INT32-prefixed mechanism token. */
const TOKEN_ANSWER = () => ({ authBytes: new Encoder().writeBytes(Buffer.from('server')).buffer })

/** @returns {object} a GSSAPI client whose context needs one exchange */
const gssapiStub = (overrides = {}) => ({
  contextComplete: false,
  username: 'user@REALM',
  step: async () => Buffer.from('token').toString('base64'),
  unwrap: async () => Buffer.from([1, 0, 0, 0]).toString('base64'),
  wrap: async () => Buffer.from('wrapped').toString('base64'),
  ...overrides,
})

/** Built-in mechanisms with credentials that carry the marker wherever they can. */
const MECHANISMS = {
  PLAIN: () => ({ mechanism: 'plain', username: 'user', password: SECRET }),
  'SCRAM-SHA-256': () => ({ mechanism: 'scram-sha-256', username: 'user', password: SECRET }),
  'SCRAM-SHA-512': () => ({ mechanism: 'scram-sha-512', username: 'user', password: SECRET }),
  OAUTHBEARER: () => ({
    mechanism: 'oauthbearer',
    oauthBearerProvider: async () => ({ value: SECRET }),
  }),
  AWS: () => ({
    mechanism: 'aws',
    authorizationIdentity: 'identity',
    accessKeyId: 'access-key',
    secretAccessKey: SECRET,
    sessionToken: SECRET,
  }),
  GSSAPI: () => ({ mechanism: 'gssapi' }),
}

const VERSIONS = {
  [apiKeys.SaslHandshake]: { minVersion: 0, maxVersion: 1 },
  [apiKeys.SaslAuthenticate]: { minVersion: 0, maxVersion: 1 },
}

/**
 * Every string a value exposes: own properties, the stack and the cause chain.
 * @param {*} value
 * @param {Set<object>} [seen]
 * @returns {string}
 */
const textOf = (value, seen = new Set()) => {
  if (value === null || value === undefined) return ''
  if (typeof value !== 'object' && typeof value !== 'function') return String(value)
  if (seen.has(value)) return ''
  seen.add(value)
  const parts = Object.getOwnPropertyNames(value).map((name) => textOf(value[name], seen))
  if (value instanceof Error) parts.push(String(value.stack), textOf(value.cause, seen))
  return parts.join('\n')
}

/**
 * Runs one authentication over a fake connection whose SaslAuthenticate send runs `exchange`.
 * @param {string} name built-in mechanism name
 * @param {(request: object) => *} exchange the SaslAuthenticate outcome for the request sent; throws
 *   to fail the send
 * @param {{ sasl?: object, guard?: () => Error|null }} [options]
 * @returns {Promise<{ error: *, entries: object[], sent: number, reported: jest.Mock }>}
 */
const authenticate = async (name, exchange, { sasl = MECHANISMS[name](), guard } = {}) => {
  const entries = []
  const logger = createLogger({
    level: LEVELS.DEBUG,
    logCreator: () => (entry) => entries.push(entry),
  })
  let sent = 0
  const connection = {
    sasl,
    host: 'broker',
    port: 9092,
    broker: 'broker:9092',
    reportAuthenticationFailure: jest.fn(),
    send: async ({ request }) => {
      if (request.apiKey === apiKeys.SaslHandshake) {
        return { enabledMechanisms: [name], errorCode: 0 }
      }
      sent += 1
      return exchange(request)
    },
  }
  const error = await new SASLAuthenticator(connection, logger, VERSIONS, guard)
    .authenticate()
    .then(
      () => null,
      (e) => e
    )
  return { error, entries, sent, reported: connection.reportAuthenticationFailure }
}

const connectionError = () =>
  new KafkaJSConnectionError('Connection error: read ECONNRESET', { broker: 'broker:9092' })
const closedError = () =>
  new KafkaJSConnectionClosedError('Closed connection', { host: 'broker', port: 9092 })
const timeoutError = () =>
  new KafkaJSRequestTimeoutError('Request SaslAuthenticate(key: 36, version: 1) timed out')

/**
 * A send failure as the real connection delivers it: the transport failure ends the connection,
 * whose guard then refuses with the close reason (a timed-out request closes its connection).
 * @param {Error} failure what the send rejects with
 * @param {Error} [refusal=failure] the guard's refusal once the send failed
 * @returns {{ exchange: () => never, guard: () => Error|null }}
 */
const endsConnection = (failure, refusal = failure) => {
  let ended = false
  return {
    exchange: () => {
      ended = true
      throw failure
    },
    guard: () => (ended ? refusal : null),
  }
}

/** Library-class transport errors a supplier can construct, each carrying the marker. */
const SUPPLIED_TRANSPORT_ERRORS = [
  [
    'a constructed library connection error',
    () =>
      Object.assign(
        new KafkaJSConnectionError(`Connection error: ${SECRET}`, { cause: new Error(SECRET) }),
        { detail: SECRET }
      ),
  ],
  [
    'a native error adopting the connection error prototype',
    () =>
      Object.setPrototypeOf(
        Object.assign(new Error(SECRET), { detail: SECRET, retriable: true }),
        KafkaJSConnectionError.prototype
      ),
  ],
]

describe('Broker > SASLAuthenticator > transport failures during the exchange', () => {
  beforeEach(() => {
    gssapiClient = gssapiStub()
  })

  describe.each(Object.keys(MECHANISMS))('%s', (name) => {
    test.each([
      ['connection error', connectionError, (failure) => failure, KafkaJSConnectionError],
      ['closed connection', closedError, (failure) => failure, KafkaJSConnectionClosedError],
      ['request timeout', timeoutError, closedError, KafkaJSConnectionClosedError],
    ])(
      "a %s that ends the connection during the exchange rethrows the connection's refusal and stays retriable",
      async (_, make, refusalOf, Class) => {
        const failure = make()
        const refusal = refusalOf(failure)
        const { exchange, guard } = endsConnection(failure, refusal)
        const { error, entries, sent, reported } = await authenticate(name, exchange, { guard })

        expect(sent).toBe(1)
        expect(error).toBe(refusal)
        expect(Object.getPrototypeOf(error)).toBe(Class.prototype)
        expect(error.retriable).toBe(true)
        expect(error.category).toBe('retriable')
        expect(reported).not.toHaveBeenCalled()
        expect(entries.filter(({ level }) => level === LEVELS.ERROR)).toEqual([])
        expect(textOf([error, entries])).not.toContain(SECRET)
      }
    )

    test('a cancellation during the exchange keeps its own class and retriability', async () => {
      const refusal = new KafkaJSNonRetriableError('Connection setup cancelled')
      let cancelled = false
      const { error, reported } = await authenticate(
        name,
        () => {
          cancelled = true
          throw connectionError()
        },
        { guard: () => (cancelled ? refusal : null) }
      )

      expect(error).toBe(refusal)
      expect(error.retriable).toBe(false)
      expect(error).not.toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(reported).not.toHaveBeenCalled()
    })

    test('a broker rejection stays a non-retriable authentication error', async () => {
      const { error, entries, reported } = await authenticate(name, () => {
        throw createErrorFromCode(58)
      })

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(error.message).toContain('failed')
      expect(reported).toHaveBeenCalledTimes(1)
      expect(textOf([error, entries])).not.toContain(SECRET)
    })

    test('an error forging a transport name, code and retriable flag stays an authentication error without its text', async () => {
      const forged = Object.assign(new Error(`Connection error: ${SECRET}`), {
        name: 'KafkaJSConnectionError',
        code: 'ECONNRESET',
        retriable: true,
        category: 'retriable',
      })
      const { error, entries } = await authenticate(name, () => {
        throw forged
      })

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(error.cause).toBeUndefined()
      expect(textOf([error, entries])).not.toContain(SECRET)
    })

    test.each(SUPPLIED_TRANSPORT_ERRORS)(
      '%s from a send that leaves the connection open stays an authentication error without its text',
      async (_, make) => {
        const supplied = make()
        const { error, entries, sent } = await authenticate(name, () => {
          throw supplied
        })

        expect(sent).toBe(1)
        expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
        expect(error).not.toBe(supplied)
        expect(error.retriable).toBe(false)
        expect(error.cause).toBeUndefined()
        expect(textOf([error, entries])).not.toContain(SECRET)
      }
    )
  })

  test('OAUTHBEARER: a library connection error thrown by the supplied token stays an authentication error without its text', async () => {
    const token = {
      value: 'token',
      get extensions() {
        throw new KafkaJSConnectionError(`Connection error: ${SECRET}`)
      },
    }
    const { error, entries, sent } = await authenticate('OAUTHBEARER', () => ({}), {
      sasl: { mechanism: 'oauthbearer', oauthBearerProvider: async () => token },
    })

    expect(sent).toBe(0)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.retriable).toBe(false)
    expect(error.cause).toBeUndefined()
    expect(textOf([error, entries])).not.toContain(SECRET)
  })

  test.each(['step', 'unwrap', 'wrap'])(
    'GSSAPI: a library connection error from the native %s stage stays an authentication error without its text',
    async (stage) => {
      const fail = async () => {
        throw new KafkaJSConnectionError(`Connection error: ${SECRET}`)
      }
      gssapiClient = gssapiStub(
        stage === 'step' ? { step: fail } : { contextComplete: true, [stage]: fail }
      )
      const { error, entries } = await authenticate('GSSAPI', TOKEN_ANSWER)

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(error.message).toContain(`stage=${stage}`)
      expect(textOf([error, entries])).not.toContain(SECRET)
    }
  )

  test('GSSAPI: a reset during the final layer exchange is rethrown unchanged', async () => {
    gssapiClient = gssapiStub({ contextComplete: true })
    const failure = connectionError()
    let exchanges = 0
    const { error } = await authenticate(
      'GSSAPI',
      () => {
        exchanges += 1
        if (exchanges === 2) throw failure
        return TOKEN_ANSWER()
      },
      { guard: () => (exchanges === 2 ? failure : null) }
    )

    expect(exchanges).toBe(2)
    expect(error).toBe(failure)
    expect(error.retriable).toBe(true)
  })
})

describe('Broker > SASLAuthenticator > failures that remain authentication errors', () => {
  beforeEach(() => {
    gssapiClient = gssapiStub()
  })

  test.each([
    ['PLAIN', { mechanism: 'plain', username: 'user' }],
    ['SCRAM-SHA-256', { mechanism: 'scram-sha-256', username: 'user' }],
    ['SCRAM-SHA-512', { mechanism: 'scram-sha-512', username: 'user' }],
    ['AWS', { mechanism: 'aws', authorizationIdentity: 'id', accessKeyId: 'key' }],
    ['OAUTHBEARER', { mechanism: 'oauthbearer', oauthBearerProvider: async () => ({}) }],
  ])(
    '%s: missing credentials stay an authentication error and send nothing',
    async (name, sasl) => {
      const { error, sent } = await authenticate(name, () => ({}), { sasl })

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(sent).toBe(0)
    }
  )

  test('GSSAPI: an invalid service name keeps its configuration error class', async () => {
    const { error, sent } = await authenticate('GSSAPI', () => ({}), {
      sasl: { mechanism: 'gssapi', serviceName: 'kafka/host' },
    })

    expect(Object.getPrototypeOf(error)).toBe(KafkaJSInvalidConfigurationError.prototype)
    expect(sent).toBe(0)
  })

  test.each(['SCRAM-SHA-256', 'SCRAM-SHA-512'])(
    '%s: a server-first message failing validation stays an authentication error',
    async (name) => {
      const serverFirst = Buffer.from(`r=foreign-nonce,s=${SECRET},i=4096`)
      const { error, entries } = await authenticate(name, () => ({
        authBytes: new Encoder().writeBytes(serverFirst).buffer,
      }))

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(error.message).toContain('Invalid server nonce')
      expect(textOf([error, entries])).not.toContain(SECRET)
    }
  )

  test('OAUTHBEARER: a supplier rejection outside the exchange keeps its own error', async () => {
    const rejection = new KafkaJSConnectionError('supplier unavailable')
    const { error, sent } = await authenticate('OAUTHBEARER', () => ({}), {
      sasl: {
        mechanism: 'oauthbearer',
        oauthBearerProvider: async () => {
          throw rejection
        },
      },
    })

    expect(error).toBe(rejection)
    expect(sent).toBe(0)
  })
})

describe('Broker > SASLAuthenticator > the classification reaches the recovery policies', () => {
  /** @returns {Promise<Error>} what SCRAM-SHA-512 throws when the exchange fails as `make` says */
  const produce = (make) => {
    const { exchange, guard } = make()
    return authenticate('SCRAM-SHA-512', exchange, { guard }).then(({ error }) => error)
  }

  test.each([
    ['a lost connection', () => endsConnection(connectionError()), true],
    ['a request timeout', () => endsConnection(timeoutError(), closedError()), true],
    [
      'a broker rejection',
      () => ({
        exchange: () => {
          throw createErrorFromCode(58)
        },
      }),
      false,
    ],
  ])(
    '%s during the exchange decides rebootstrap, consumer restart and share restart',
    async (_, make, recoverable) => {
      const error = await produce(make)
      const cluster = new Cluster({
        ...connectionOpts(),
        logger: newLogger(),
        metadataRecoveryStrategy: 'rebootstrap',
        metadataRecoveryRebootstrapTriggerMs: 1000,
      })

      await cluster.brokerPool.onMetadataFailure(error)

      expect(cluster.brokerPool.metadataUnavailableSince !== null).toBe(recoverable)
      expect(error.retriable === true).toBe(recoverable)
      expect(ShareRunner.prototype.mustNotRestart.call(null, error)).toBe(!recoverable)
    }
  )
})

describe('Broker > SASLAuthenticator > a passed-through failure belongs to one invocation', () => {
  /**
   * An OAUTHBEARER authentication whose token's `extensions` getter throws `error` after the marker
   * was written into its message and cause, as a supplier holding the object could.
   * @param {Error} error a failure an earlier invocation produced
   * @returns {Promise<{ error: *, entries: object[], sent: number }>}
   */
  const reuseInTokenGetter = (error) => {
    error.message = `Connection error: ${SECRET}`
    error.cause = new Error(SECRET)
    const token = {
      value: 'token',
      get extensions() {
        throw error
      },
    }
    return authenticate('OAUTHBEARER', () => ({}), {
      sasl: { mechanism: 'oauthbearer', oauthBearerProvider: async () => token },
    })
  }

  test('a refusal a custom provider propagated is judged afresh when a later token getter throws it', async () => {
    const refusal = connectionError()
    const { exchange, guard } = endsConnection(refusal)
    const custom = await authenticate('PLAIN', exchange, {
      guard,
      sasl: {
        mechanism: 'plain',
        authenticationProvider: ({ saslAuthenticate }) => ({
          authenticate: () =>
            saslAuthenticate({ request: { encode: async () => Buffer.alloc(0) } }),
        }),
      },
    })
    expect(custom.error).toBe(refusal)

    const { error, entries, sent } = await reuseInTokenGetter(refusal)

    expect(sent).toBe(0)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.retriable).toBe(false)
    expect(error.cause).toBeUndefined()
    expect(textOf([error, entries])).not.toContain(SECRET)
  })

  test('a refusal before the mechanism runs is judged afresh when a later token getter throws it', async () => {
    const refusal = connectionError()
    const refused = await authenticate('PLAIN', () => ({}), { guard: () => refusal })
    expect(refused.error).toBe(refusal)
    expect(refused.sent).toBe(0)

    const { error, entries } = await reuseInTokenGetter(refusal)

    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.retriable).toBe(false)
    expect(textOf([error, entries])).not.toContain(SECRET)
  })

  test('two concurrent invocations refused with one shared error each rethrow it unchanged', async () => {
    const shared = connectionError()
    let release
    const gate = new Promise((resolve) => (release = resolve))
    const concurrent = () => {
      let ended = false
      return authenticate(
        'PLAIN',
        async () => {
          await gate
          ended = true
          throw shared
        },
        { guard: () => (ended ? shared : null) }
      )
    }
    const outcomes = Promise.all([concurrent(), concurrent()])
    release()

    const [first, second] = await outcomes

    expect(first.error).toBe(shared)
    expect(second.error).toBe(shared)
    expect(shared.retriable).toBe(true)
  })

  test('sequential invocations each rethrow their own refusal', async () => {
    const refusals = [connectionError(), closedError()]

    const errors = []
    for (const refusal of refusals) {
      const { exchange, guard } = endsConnection(refusal)
      errors.push((await authenticate('SCRAM-SHA-256', exchange, { guard })).error)
    }

    expect(errors[0]).toBe(refusals[0])
    expect(errors[1]).toBe(refusals[1])
  })
})

describe('Broker > SASLAuthenticator > OAUTHBEARER error challenge', () => {
  /** What a broker that refuses a token answers with, in a SaslAuthenticate that carries no error code. */
  const CHALLENGE = JSON.stringify({ status: 'invalid_token', scope: SECRET })
  const challengeAnswer = () => ({
    authBytes: new Encoder().writeBytes(Buffer.from(CHALLENGE)).buffer,
  })
  const emptyAnswer = (authBytes = Buffer.alloc(0)) => ({
    authBytes: new Encoder().writeBytes(authBytes).buffer,
  })

  /** @returns {Promise<Buffer|null>} the mechanism payload a SaslAuthenticate request carries */
  const payloadOf = async (request) => new Decoder((await request.encode()).buffer).readBytes()

  /**
   * Scripts one SaslAuthenticate answer per call and records each payload sent.
   * @param {Array<() => object>} answers the answer of the nth call; a function may throw
   * @returns {{ exchange: (request: object) => Promise<object>, payloads: Buffer[] }}
   */
  const scripted = (answers) => {
    const payloads = []
    return {
      payloads,
      exchange: async (request) => {
        payloads.push(await payloadOf(request))
        return answers[payloads.length - 1]()
      },
    }
  }

  const reject = () => {
    throw createErrorFromCode(58)
  }

  test('an error challenge is answered with one 0x01 byte and the broker rejection is a non-retriable authentication error without the challenge text', async () => {
    const { exchange, payloads } = scripted([challengeAnswer, reject])

    const { error, entries, sent, reported } = await authenticate('OAUTHBEARER', exchange)

    expect(sent).toBe(2)
    expect(payloads[0].toString().startsWith('n,,\u0001auth=Bearer ')).toBe(true)
    expect(payloads[1]).toEqual(Buffer.from([0x01]))
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.retriable).toBe(false)
    expect(reported).toHaveBeenCalledTimes(1)
    expect(textOf([error, entries])).not.toContain(SECRET)
    expect(textOf([error, entries])).not.toContain('invalid_token')
  })

  test('an error challenge still fails authentication when the reply to the 0x01 byte is not an error', async () => {
    const { exchange, payloads } = scripted([challengeAnswer, emptyAnswer])

    const { error, entries, sent, reported } = await authenticate('OAUTHBEARER', exchange)

    expect(sent).toBe(2)
    expect(payloads[1]).toEqual(Buffer.from([0x01]))
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.retriable).toBe(false)
    expect(error.message).toBe(
      'SASL OAUTHBEARER authentication failed: The server answered the token with an error challenge'
    )
    expect(reported).not.toHaveBeenCalled()
    expect(textOf([error, entries])).not.toContain(SECRET)
    expect(textOf([error, entries])).not.toContain('invalid_token')
  })

  test.each([
    ['empty', Buffer.alloc(0)],
    ['null', null],
  ])(
    'a token the broker accepts (%s auth bytes) completes with a single SaslAuthenticate',
    async (_, bytes) => {
      const { exchange } = scripted([() => emptyAnswer(bytes)])

      const { error, entries, sent, reported } = await authenticate('OAUTHBEARER', exchange)

      expect(error).toBeNull()
      expect(sent).toBe(1)
      expect(reported).not.toHaveBeenCalled()
      expect(entries.filter(({ level }) => level === LEVELS.ERROR)).toEqual([])
    }
  )

  test.each([
    ['connection error', connectionError, (failure) => failure],
    ['request timeout', timeoutError, closedError],
  ])(
    'a %s that ends the connection while the 0x01 byte is sent stays a retriable connection error',
    async (_, make, refusalOf) => {
      const failure = make()
      const refusal = refusalOf(failure)
      let ended = false
      const { exchange } = scripted([
        challengeAnswer,
        () => {
          ended = true
          throw failure
        },
      ])

      const { error, entries, sent, reported } = await authenticate('OAUTHBEARER', exchange, {
        guard: () => (ended ? refusal : null),
      })

      expect(sent).toBe(2)
      expect(error).toBe(refusal)
      expect(error).not.toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(true)
      expect(reported).not.toHaveBeenCalled()
      expect(entries.filter(({ level }) => level === LEVELS.ERROR)).toEqual([])
      expect(textOf([error, entries])).not.toContain(SECRET)
    }
  )
})

describe('Broker > SASLAuthenticator > transport failures through the client', () => {
  const ECONNRESET = () => Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })
  /** Logins last one second; the client re-authenticates 100 ms after each one. */
  const LOGIN = {
    errorCode: 0,
    errorMessage: null,
    authBytes: Buffer.alloc(0),
    sessionLifetimeMs: '1000',
  }
  let cluster
  let admin

  /**
   * Settles client work by advancing fake time in small steps.
   * @param {Promise<*>} work
   * @returns {Promise<*>} its outcome
   */
  const until = async (work) => {
    const state = { settled: false }
    const tracked = work.finally(() => (state.settled = true))
    tracked.catch(() => {})
    for (let elapsed = 0; !state.settled && elapsed < 5000; elapsed += 10) {
      await jest.advanceTimersByTimeAsync(10)
    }
    if (!state.settled) throw new Error('Client work did not settle within the fake-time budget')
    return tracked
  }

  /**
   * Builds an admin over one scripted SASL broker whose SaslAuthenticate runs `answer`.
   * @param {(call: number) => object} answer the response fields of the nth SaslAuthenticate
   * @param {{ wrapSocket?: (socket: object) => void, entries?: object[], oauthBearer?: boolean }}
   *   [options] a hook on every socket the client opens, a sink for every log entry, and whether
   *   the broker speaks SASL OAUTHBEARER instead of PLAIN
   * @returns {void}
   */
  const arrange = (answer, { wrapSocket = () => {}, entries, oauthBearer = false } = {}) => {
    let call = 0
    cluster = createScriptedCluster({
      nodes: [{ id: 0, host: 'broker', port: 9092 }],
      profile: 'sasl',
    })
      .answer('SaslHandshake', () => ({
        enabledMechanisms: [oauthBearer ? 'OAUTHBEARER' : 'PLAIN'],
      }))
      .answer('SaslAuthenticate', () => answer(call++))
    admin = new Kafka(
      cluster.kafkaOptions({
        sasl: oauthBearer
          ? { mechanism: 'oauthbearer', oauthBearerProvider: async () => ({ value: SECRET }) }
          : { mechanism: 'plain', username: 'user', password: SECRET },
        authenticationTimeout: 1000,
        reauthenticationThreshold: 900,
        retry: { initialRetryTime: 10, maxRetryTime: 10, factor: 0, multiplier: 1, retries: 3 },
        socketFactory: (options) => {
          const socket = cluster.socketFactory(options)
          wrapSocket(socket)
          return socket
        },
        ...(entries && {
          logLevel: LEVELS.DEBUG,
          logCreator: () => (entry) => entries.push(entry),
        }),
      })
    ).admin()
  }

  /** Resets the newest live socket, as a broker that drops the connection before replying. */
  const resetLogin = () => {
    const live = cluster.sockets().filter((socket) => !socket.destroyed)
    live[live.length - 1].destroy(ECONNRESET())
    return LOGIN
  }

  const logins = () => cluster.requests().filter(({ api }) => api === 'SaslAuthenticate')

  beforeEach(() => jest.useFakeTimers())

  afterEach(async () => {
    if (admin) await until(admin.disconnect())
    await jest.advanceTimersByTimeAsync(1)
    cluster?.assertClean()
    cluster = admin = undefined
    jest.useRealTimers()
  })

  test('a connection reset during re-authentication is retried and the request succeeds', async () => {
    arrange((call) => (call === 1 ? resetLogin() : LOGIN))
    await until(admin.connect())
    await jest.advanceTimersByTimeAsync(200)

    const topics = await until(admin.listTopics())

    expect(topics).toEqual([])
    expect(logins()).toHaveLength(3)
    expect(cluster.sockets()[0].destroyed).toBe(true)
  })

  test('a connection error a supplied socket throws while writing the login stays a sanitized authentication error', async () => {
    const entries = []
    const supplied = Object.assign(
      new KafkaJSConnectionError(`Connection error: ${SECRET}`, { cause: new Error(SECRET) }),
      { detail: SECRET }
    )
    let thrown = 0
    arrange(() => LOGIN, {
      entries,
      wrapSocket: (socket) => {
        const write = socket.write
        socket.write = (buffer, ...rest) => {
          if (thrown === 0 && Buffer.from(buffer).readInt16BE(4) === apiKeys.SaslAuthenticate) {
            thrown += 1
            throw supplied
          }
          return write(buffer, ...rest)
        }
      },
    })

    const error = await until(admin.connect()).catch((e) => e)

    expect(thrown).toBe(1)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.retriable).toBe(false)
    expect(logins()).toHaveLength(0)
    expect(textOf([error, entries])).not.toContain(SECRET)
  })

  describe('an OAUTHBEARER token the broker challenges', () => {
    const CHALLENGE = {
      ...LOGIN,
      authBytes: Buffer.from(JSON.stringify({ status: 'invalid_token', scope: SECRET })),
    }
    const REJECTED = { errorCode: 58, errorMessage: SECRET, authBytes: Buffer.alloc(0) }

    test('a challenged token at connect is a terminal authentication error after one 0x01 byte', async () => {
      const entries = []
      arrange((call) => [CHALLENGE, REJECTED][call], { oauthBearer: true, entries })

      const error = await until(admin.connect()).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(logins()).toHaveLength(2)
      expect(logins()[1].fields.authBytes).toEqual(Buffer.from([0x01]))
      expect(textOf([error, entries])).not.toContain(SECRET)
      expect(textOf([error, entries])).not.toContain('invalid_token')
    })

    test('a challenged token at re-authentication is a terminal authentication error after one 0x01 byte', async () => {
      const entries = []
      arrange((call) => [LOGIN, CHALLENGE, REJECTED][call], { oauthBearer: true, entries })
      await until(admin.connect())
      await jest.advanceTimersByTimeAsync(200)

      const error = await until(admin.listTopics()).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(logins()).toHaveLength(3)
      expect(logins()[2].fields.authBytes).toEqual(Buffer.from([0x01]))
      expect(textOf([error, entries])).not.toContain(SECRET)
      expect(textOf([error, entries])).not.toContain('invalid_token')
    })

    test('a reset while the 0x01 byte is sent at re-authentication is retried and the request succeeds', async () => {
      arrange((call) => (call === 1 ? CHALLENGE : call === 2 ? resetLogin() : LOGIN), {
        oauthBearer: true,
      })
      await until(admin.connect())
      await jest.advanceTimersByTimeAsync(200)

      const topics = await until(admin.listTopics())

      expect(topics).toEqual([])
      expect(logins()).toHaveLength(4)
      expect(logins()[2].fields.authBytes).toEqual(Buffer.from([0x01]))
      expect(cluster.sockets()[0].destroyed).toBe(true)
    })
  })

  test('a rejected password during re-authentication is not retried', async () => {
    arrange((call) =>
      call === 0 ? LOGIN : { errorCode: 58, errorMessage: SECRET, authBytes: Buffer.alloc(0) }
    )
    await until(admin.connect())
    await jest.advanceTimersByTimeAsync(200)

    const error = await until(admin.listTopics()).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.retriable).toBe(false)
    expect(logins()).toHaveLength(2)
    expect(textOf(error)).not.toContain(SECRET)
  })
})
