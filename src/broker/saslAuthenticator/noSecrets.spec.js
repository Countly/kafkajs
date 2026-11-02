const fs = require('fs')
const http = require('http')
const os = require('os')
const path = require('path')
const { createRequire } = require('module')
const Broker = require('../index')
const Encoder = require('../../protocol/encoder')
const ConnectionPool = require('../../network/connectionPool')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { NETWORK_AUTHENTICATION_FAILURE } = require('../../network/instrumentationEvents')
const { createLogger, LEVELS } = require('../../loggers')
const {
  KafkaJSError,
  KafkaJSProtocolError,
  KafkaJSSASLAuthenticationError,
} = require('../../errors')
const oauthBearer = require('../../oauth')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { Kafka, logLevel } = require('../../..')
const apiKeys = require('../../protocol/requests/apiKeys')
const { responseSourceOf } = require('../failureSource')

/**
 * Hand-built inert markers, one per secret class; never real credentials. Each test places them
 * in a credential, in what a token supplier returns, or in what a hostile broker echoes back.
 */
const SECRET = {
  password: 'pw-marker-6b1e',
  token: 'token-marker-c4a9',
  awsSecret: 'aws-secret-marker-11fc',
  awsSession: 'aws-session-marker-7d30',
  salt: 'salt-marker-2f07',
  nonce: 'nonce-marker-a83c',
  ticket: 'ticket-marker-90dd',
  echo: 'broker-echo-marker-37aa',
  supplier: 'supplier-marker-e512',
  clientSecret: 'client-secret-marker-41b6',
}
const BROKER = 'broker:9092'

/** The native stub the optional GSSAPI peer resolves to; each GSSAPI test installs its client. */
let gssapiClient
jest.doMock('kerberos/package.json', () => ({ version: '7.0.0' }), { virtual: true })
jest.doMock(
  'kerberos',
  () => ({ GSS_MECH_OID_KRB5: 'oid', initializeClient: async () => gssapiClient }),
  { virtual: true }
)

/**
 * Every string a value exposes: own properties of objects and errors (message, stack, cause),
 * array items, map entries, and the bytes of a Buffer read as text and as base64.
 * @param {*} value
 * @param {Set<object>} [seen]
 * @returns {string}
 */
const textOf = (value, seen = new Set()) => {
  if (value === null || value === undefined) return ''
  if (Buffer.isBuffer(value)) return `${value.toString('latin1')}\n${value.toString('base64')}`
  if (typeof value !== 'object' && typeof value !== 'function') return String(value)
  if (seen.has(value)) return ''
  seen.add(value)
  const parts = Object.getOwnPropertyNames(value).map((name) => textOf(value[name], seen))
  if (value instanceof Map) parts.push(textOf([...value.entries()], seen))
  if (value instanceof Error) parts.push(String(value.stack), textOf(value.cause, seen))
  return parts.join('\n')
}

/** Fails naming the first secret class whose marker appears in `text`. */
const expectNoSecret = (text, markers = Object.values(SECRET)) => {
  expect(markers.filter((marker) => text.includes(marker))).toEqual([])
}

/** The sanitized description a code-58 rejection publishes for `mechanism`. */
const published = (mechanism) =>
  `SASL ${mechanism} authentication failed: broker=${BROKER} stage=exchange`

describe.each([
  ['off', undefined],
  ['on', '1'],
])('Broker > SASL failures carry no secret, buffer diagnostics %s', (_, buffers) => {
  let cluster, pool, broker, entries, events, blackout, env

  /**
   * The real Broker, ConnectionPool and Connection over the scripted cluster.
   * @param {object} sasl client SASL options
   * @param {string} mechanism the one mechanism the handshake offers
   * @param {(request: object, index: number) => object} answer SaslAuthenticate fields
   */
  const arrange = (sasl, mechanism, answer) => {
    entries = []
    events = []
    blackout = new Map()
    const emitter = new InstrumentationEventEmitter()
    emitter.addListener(NETWORK_AUTHENTICATION_FAILURE, (event) => events.push(event))
    const logger = createLogger({
      level: LEVELS.DEBUG,
      logCreator: () => (entry) => entries.push(entry),
    })
    let index = 0
    cluster = createScriptedCluster({
      nodes: [{ id: 0, host: 'broker', port: 9092 }],
      profile: 'sasl',
    })
      .answer('SaslHandshake', () => ({ enabledMechanisms: [mechanism] }))
      .answer('SaslAuthenticate', (request) => answer(request, index++))
    pool = new ConnectionPool({
      host: 'broker',
      port: 9092,
      logger,
      socketFactory: cluster.socketFactory,
      sasl,
      connectionTimeout: 3000,
      requestTimeout: 5000,
      authenticationTimeout: 5000,
      authenticationBlackout: blackout,
      instrumentationEmitter: emitter,
    })
    broker = new Broker({ connectionPool: pool, logger, authenticationTimeout: 5000 })
  }

  /** Everything this run exposed besides the rejection itself. */
  const diagnostics = () => textOf([entries, events, blackout])

  /** A broker that rejects with code 58 and echoes the given text and bytes back. */
  const rejectWithEcho = (echoed) => () => ({
    errorCode: 58,
    errorMessage: `${SECRET.echo} ${echoed}`,
    authBytes: Buffer.from(`${SECRET.echo} ${echoed}`),
  })

  /** Asserts the sanitized rejection, event and blackout, then that nothing carries a marker. */
  const expectSanitizedRejection = async (mechanism, prefix) => {
    const error = await broker.connect().catch((e) => e)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message.startsWith(prefix)).toBe(true)
    expect(events.map(({ payload }) => payload)).toEqual([
      { broker: BROKER, mechanism, errorMessage: published(mechanism) },
    ])
    expect(blackout.get(BROKER).errorMessage).toBe(published(mechanism))
    const refusal = await pool.getConnection().catch((e) => e)
    expect(refusal).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(refusal.message).toContain(published(mechanism))
    expectNoSecret(`${textOf(error)}\n${textOf(refusal)}\n${diagnostics()}`)
  }

  beforeEach(() => {
    env = process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
    if (buffers === undefined) delete process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
    else process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = buffers
  })

  afterEach(async () => {
    if (env === undefined) delete process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
    else process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = env
    await broker?.disconnect()
    cluster?.assertClean()
    cluster = pool = broker = undefined
    jest.restoreAllMocks()
  })

  test('PLAIN: a broker echoing the password is sanitized everywhere', async () => {
    arrange(
      { mechanism: 'plain', username: 'user', password: SECRET.password },
      'PLAIN',
      rejectWithEcho(SECRET.password)
    )
    await expectSanitizedRejection('PLAIN', 'SASL PLAIN authentication failed: ')
  })

  test('SCRAM: a broker echoing its nonce, salt and the client proof is sanitized everywhere', async () => {
    let proof
    arrange(
      { mechanism: 'scram-sha-256', username: 'user', password: SECRET.password },
      'SCRAM-SHA-256',
      ({ authBytes }, index) => {
        const message = authBytes.toString()
        if (index === 0) {
          const nonce = message.slice('n,,n=user,r='.length)
          const salt = Buffer.from(SECRET.salt).toString('base64')
          return { authBytes: Buffer.from(`r=${nonce}${SECRET.nonce},s=${salt},i=4096`) }
        }
        proof = message.slice(message.indexOf(',p=') + ',p='.length)
        return rejectWithEcho(`${proof} ${SECRET.salt} ${SECRET.nonce} ${SECRET.password}`)()
      }
    )
    await expectSanitizedRejection('SCRAM-SHA-256', 'SASL SCRAM SHA256 authentication failed: ')
    expect(proof.length).toBeGreaterThan(0)
    expectNoSecret(diagnostics(), [proof])
  })

  test('SCRAM: a server-final error attribute carrying a secret is not echoed', async () => {
    arrange(
      { mechanism: 'scram-sha-256', username: 'user', password: SECRET.password },
      'SCRAM-SHA-256',
      ({ authBytes }, index) => {
        const message = authBytes.toString()
        return index === 0
          ? {
              authBytes: Buffer.from(
                `r=${message.slice('n,,n=user,r='.length)}${SECRET.nonce},s=c2FsdA==,i=4096`
              ),
            }
          : { authBytes: Buffer.from(`e=${SECRET.echo}-${SECRET.password}`) }
      }
    )
    const error = await broker.connect().catch((e) => e)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toBe(
      'SASL SCRAM SHA256 authentication failed: The server final message reports an error'
    )
    expect(events).toEqual([])
    expectNoSecret(`${textOf(error)}\n${diagnostics()}`)
  })

  test('OAUTHBEARER: a broker echoing the token is sanitized everywhere', async () => {
    arrange(
      { mechanism: 'oauthbearer', oauthBearerProvider: async () => ({ value: SECRET.token }) },
      'OAUTHBEARER',
      rejectWithEcho(SECRET.token)
    )
    await expectSanitizedRejection('OAUTHBEARER', 'SASL OAUTHBEARER authentication failed: ')
  })

  test.each(['name', 'message'])(
    'returned OAuth token errors cannot leak through name/message: %s',
    async (field) => {
      const injected =
        field === 'message'
          ? new KafkaJSError(SECRET.token)
          : Object.assign(new Error('safe'), { name: SECRET.token })
      const token = {
        value: 'valid-token',
        get extensions() {
          throw injected
        },
      }
      arrange(
        { mechanism: 'oauthbearer', oauthBearerProvider: async () => token },
        'OAUTHBEARER',
        () => ({})
      )

      const error = await broker.connect().catch((e) => e)
      expectNoSecret(`${textOf(error)}\n${diagnostics()}`)
      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.message).toBe('SASL OAUTHBEARER authentication failed: Error')
      expect(events).toEqual([])
      expect(cluster.requests().filter(({ api }) => api === 'SaslAuthenticate')).toEqual([])
    }
  )

  test('returned OAuth token errors cannot forge trusted text through the resolved package factory', async () => {
    const entry = createRequire(require.resolve('../../../index')).resolve('@countly/kafkajs')
    const { trustedError } = require(
      path.join(path.dirname(entry), 'src/broker/saslAuthenticator/failure')
    )
    const token = {
      value: 'valid-token',
      get extensions() {
        throw trustedError(KafkaJSSASLAuthenticationError, SECRET.token)
      },
    }
    arrange(
      { mechanism: 'oauthbearer', oauthBearerProvider: async () => token },
      'OAUTHBEARER',
      () => ({})
    )

    const error = await broker.connect().catch((e) => e)
    expectNoSecret(`${textOf(error)}\n${diagnostics()}`)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toBe('SASL OAUTHBEARER authentication failed: Error')
    expect(events).toEqual([])
    expect(cluster.requests().filter(({ api }) => api === 'SaslAuthenticate')).toEqual([])
  })

  test.each(['proxy', 'protocol-shaped getter'])(
    'returned OAuth token errors are inspected without supplier calls: %s',
    async (kind) => {
      const calls = []
      const injected =
        kind === 'proxy'
          ? new Proxy(
              {},
              {
                getPrototypeOf() {
                  calls.push('getPrototypeOf')
                  return KafkaJSProtocolError.prototype
                },
                get(_, key) {
                  calls.push(`get ${String(key)}`)
                  return key === 'code' ? 58 : undefined
                },
              }
            )
          : Object.create(KafkaJSProtocolError.prototype, {
              code: {
                get() {
                  calls.push('code')
                  return 58
                },
              },
            })
      const token = {
        value: 'valid-token',
        get extensions() {
          throw injected
        },
      }
      arrange(
        { mechanism: 'oauthbearer', oauthBearerProvider: async () => token },
        'OAUTHBEARER',
        () => ({})
      )

      const error = await broker.connect().catch((e) => e)
      expect(calls).toEqual([])
      expect(error.message).toBe('SASL OAUTHBEARER authentication failed: Error')
      expectNoSecret(`${textOf(error)}\n${diagnostics()}`)
      expect(events).toEqual([])
      expect(cluster.requests().filter(({ api }) => api === 'SaslAuthenticate')).toEqual([])
    }
  )

  test('a custom mechanism receives the protocol error with no broker text in it or its cause', async () => {
    const authenticationProvider = ({ saslAuthenticate }) => ({
      authenticate: async () => {
        await saslAuthenticate({
          request: { encode: async () => new Encoder().writeBytes(SECRET.password).buffer },
        })
      },
    })
    arrange(
      { mechanism: 'custom-plain', authenticationProvider },
      'CUSTOM-PLAIN',
      rejectWithEcho(SECRET.password)
    )

    const error = await broker.connect().catch((e) => e)
    expect(error).toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'SASL_AUTHENTICATION_FAILED',
      code: 58,
      message: 'SASL Authentication failed',
    })
    expect(responseSourceOf(error)).toBe(apiKeys.SaslAuthenticate)
    expect(events.map(({ payload }) => payload)).toEqual([
      { broker: BROKER, mechanism: 'CUSTOM-PLAIN', errorMessage: published('CUSTOM-PLAIN') },
    ])
    expectNoSecret(`${textOf(error)}\n${diagnostics()}`)
  })

  test('AWS: a broker echoing the secret key and session token is sanitized everywhere', async () => {
    arrange(
      {
        mechanism: 'aws',
        authorizationIdentity: 'identity',
        accessKeyId: 'access-key-id',
        secretAccessKey: SECRET.awsSecret,
        sessionToken: SECRET.awsSession,
      },
      'AWS',
      rejectWithEcho(`${SECRET.awsSecret} ${SECRET.awsSession}`)
    )
    await expectSanitizedRejection('AWS', 'SASL AWS-IAM authentication failed: ')
  })

  describe('GSSAPI over a stub provider', () => {
    beforeEach(() => {
      gssapiClient = {
        contextComplete: false,
        username: 'client@KAFKAJS.TEST',
        step: jest.fn(async () => {
          gssapiClient.contextComplete = true
          return Buffer.from(SECRET.ticket).toString('base64')
        }),
        unwrap: jest.fn(),
        wrap: jest.fn(),
      }
    })

    test('GSSAPI: a broker echoing the ticket is sanitized everywhere', async () => {
      arrange({ mechanism: 'gssapi' }, 'GSSAPI', rejectWithEcho(SECRET.ticket))
      await expectSanitizedRejection('GSSAPI', 'SASL GSSAPI authentication failed: ')
    })

    test('GSSAPI: a provider failure naming the ticket is sanitized', async () => {
      gssapiClient.step = jest.fn(async () => {
        throw new Error(`cannot use ${SECRET.ticket}`)
      })
      arrange({ mechanism: 'gssapi' }, 'GSSAPI', rejectWithEcho(SECRET.ticket))
      const error = await broker.connect().catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.message).toBe(`SASL GSSAPI authentication failed: broker=${BROKER} stage=step`)
      expectNoSecret(`${textOf(error)}\n${diagnostics()}`)
    })
  })

  describe('token suppliers', () => {
    let server, provider, dir

    afterEach(async () => {
      provider?.close?.()
      if (server) await new Promise((resolve) => server.close(resolve))
      if (dir) fs.rmSync(dir, { recursive: true, force: true })
      server = provider = dir = undefined
    })

    test('a helper whose endpoint refuses with a body naming the secret fails sanitized', async () => {
      server = http.createServer((request, response) => {
        request.resume()
        response.writeHead(401, { 'Content-Type': 'application/json' })
        response.end(JSON.stringify({ error: `${SECRET.supplier} ${SECRET.clientSecret}` }))
      })
      await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
      provider = oauthBearer.clientCredentials({
        tokenEndpointUrl: `http://127.0.0.1:${server.address().port}/token`,
        clientId: 'client',
        clientSecret: SECRET.clientSecret,
        allowInsecure: true,
        maxRetryTime: 1000,
      })
      arrange(
        { mechanism: 'oauthbearer', oauthBearerProvider: provider },
        'OAUTHBEARER',
        () => ({})
      )

      const error = await broker.connect().catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(cluster.requests().filter(({ api }) => api === 'SaslAuthenticate')).toEqual([])
      expect(events).toEqual([])
      expectNoSecret(`${textOf(error)}\n${diagnostics()}`)
    })

    test('a file token that is not JWT-shaped fails sanitized', async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), 'no-secrets-'))
      const file = path.join(dir, 'token')
      fs.writeFileSync(file, `${SECRET.token} ${SECRET.supplier}\n`)
      provider = oauthBearer.fileToken({ path: file })
      arrange(
        { mechanism: 'oauthbearer', oauthBearerProvider: provider },
        'OAUTHBEARER',
        () => ({})
      )

      const error = await broker.connect().catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expectNoSecret(`${textOf(error)}\n${diagnostics()}`)
    })

    test('a bare callback keeps its own rejection, and no diagnostic copies it', async () => {
      const original = new Error(`${SECRET.supplier} ${SECRET.token}`)
      arrange(
        {
          mechanism: 'oauthbearer',
          oauthBearerProvider: async () => {
            throw original
          },
        },
        'OAUTHBEARER',
        () => ({})
      )

      await expect(broker.connect()).rejects.toBe(original)
      expect(events).toEqual([])
      expectNoSecret(diagnostics())
    })
  })

  describe('token administration', () => {
    const HMAC = Buffer.from('hmac-marker-5e2d')
    const tokenAnswer = (errorCode) => ({
      errorCode,
      principalType: 'User',
      principalName: 'owner',
      tokenRequesterPrincipalType: 'User',
      tokenRequesterPrincipalName: 'owner',
      issueTimestampMs: '1',
      expiryTimestampMs: '2',
      maxTimestampMs: '3',
      tokenId: 'token-id',
      hmac: HMAC,
      throttleTimeMs: 0,
    })
    let admin

    /** An authenticated admin over the scripted cluster that answers CreateDelegationToken. */
    const tokenAdmin = async (errorCode) => {
      entries = []
      cluster = createScriptedCluster({
        nodes: [{ id: 0, host: 'broker', port: 9092 }],
        profile: 'sasl',
        advertise: { CreateDelegationToken: [3, 3] },
      })
        .answer('SaslHandshake', () => ({ enabledMechanisms: ['PLAIN'] }))
        .answer('SaslAuthenticate', () => ({ authBytes: Buffer.alloc(0), sessionLifetimeMs: '0' }))
        .answer('CreateDelegationToken', () => tokenAnswer(errorCode))
      admin = new Kafka(
        cluster.kafkaOptions({
          sasl: { mechanism: 'plain', username: 'owner', password: SECRET.password },
          retry: { retries: 0 },
          logLevel: logLevel.DEBUG,
          logCreator: () => (entry) => entries.push(entry),
        })
      ).admin()
      await admin.connect()
    }

    afterEach(async () => {
      await admin?.disconnect()
      admin = undefined
    })

    test('a created token returns its HMAC to the caller and to no logger argument', async () => {
      await tokenAdmin(0)
      const token = await admin.createDelegationToken({})
      expect(token.hmac).toEqual(HMAC)
      expectNoSecret(textOf(entries), [HMAC.toString(), HMAC.toString('base64')])
    })

    test('a refused token request carries no HMAC or payload into errors or logs', async () => {
      await tokenAdmin(61)
      const error = await admin.createDelegationToken({}).catch((e) => e)
      expect(error).toMatchObject({ type: 'DELEGATION_TOKEN_AUTH_DISABLED', code: 61 })
      expectNoSecret(`${textOf(error)}\n${textOf(entries)}`, [
        HMAC.toString(),
        HMAC.toString('base64'),
      ])
    })
  })
})
