const SASLAuthenticator = require('./index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createErrorFromCode } = require('../../protocol/error')
const { createLogger, LEVELS } = require('../../loggers')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { Kafka } = require('../../..')
const Encoder = require('../../protocol/encoder')
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
 * @param {() => *} exchange the SaslAuthenticate outcome; throws to fail the send
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
      return exchange()
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

describe('Broker > SASLAuthenticator > transport failures during the exchange', () => {
  beforeEach(() => {
    gssapiClient = gssapiStub()
  })

  describe.each(Object.keys(MECHANISMS))('%s', (name) => {
    test.each([
      ['connection error', connectionError, KafkaJSConnectionError],
      ['closed connection', closedError, KafkaJSConnectionClosedError],
      ['request timeout', timeoutError, KafkaJSRequestTimeoutError],
    ])(
      'a %s during the exchange is rethrown unchanged and stays retriable',
      async (_, make, Class) => {
        const failure = make()
        const { error, entries, sent, reported } = await authenticate(name, () => {
          throw failure
        })

        expect(sent).toBe(1)
        expect(error).toBe(failure)
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
    const { error } = await authenticate('GSSAPI', () => {
      exchanges += 1
      if (exchanges === 2) throw failure
      return TOKEN_ANSWER()
    })

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
  /** @returns {Promise<Error>} what SCRAM-SHA-512 throws when the exchange send fails so */
  const produce = (failure) =>
    authenticate('SCRAM-SHA-512', () => {
      throw failure
    }).then(({ error }) => error)

  test.each([
    ['a lost connection', connectionError, true],
    ['a request timeout', timeoutError, true],
    ['a broker rejection', () => createErrorFromCode(58), false],
  ])(
    '%s during the exchange decides rebootstrap, consumer restart and share restart',
    async (_, make, recoverable) => {
      const error = await produce(make())
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
   * Builds an admin over one scripted SASL PLAIN broker whose SaslAuthenticate runs `answer`.
   * @param {(call: number) => object} answer the response fields of the nth SaslAuthenticate
   * @returns {void}
   */
  const arrange = (answer) => {
    let call = 0
    cluster = createScriptedCluster({
      nodes: [{ id: 0, host: 'broker', port: 9092 }],
      profile: 'sasl',
    })
      .answer('SaslHandshake', () => ({ enabledMechanisms: ['PLAIN'] }))
      .answer('SaslAuthenticate', () => answer(call++))
    admin = new Kafka(
      cluster.kafkaOptions({
        sasl: { mechanism: 'plain', username: 'user', password: SECRET },
        authenticationTimeout: 1000,
        reauthenticationThreshold: 900,
        retry: { initialRetryTime: 10, maxRetryTime: 10, factor: 0, multiplier: 1, retries: 3 },
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
