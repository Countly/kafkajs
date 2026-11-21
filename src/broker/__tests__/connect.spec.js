const {
  createConnectionPool,
  connectionOpts,
  saslSCRAM256ConnectionOpts,
  sslConnectionOpts,
  newLogger,
  describeIfOauthbearerDisabled,
  describeIfOauthbearerEnabled,
  saslOAuthBearerConnectionOpts,
  expiredUnsecuredJwt,
  saslEntries,
  saslConnectionOpts,
  saslBrokers,
  createTopic,
  secureRandom,
  waitForMessages,
} = require('testHelpers')

const Long = require('../../utils/long')
const Broker = require('../index')
const { Kafka, logLevel } = require('../../../index')
const defaultSocketFactory = require('../../network/socketFactory')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const { SaslAuthenticate } = require('../../protocol/requests/apiKeys')
const { KafkaJSConnectionError, KafkaJSSASLAuthenticationError } = require('../../errors')

/** An inert marker placed in a wrong password; never a real secret. */
const SECRET = 'secret-marker-5c2e'

/**
 * A socket factory whose first new socket drops the answer to its first SaslAuthenticate and then
 * closes as a peer would, so the connection is lost while that login is in flight.
 * @returns {{ socketFactory: Function, armed: () => object|null, sockets: object[] }} the factory,
 *   the socket it armed (its `responseRule.count` is 0 once the login answer was dropped) and every
 *   socket it created, oldest first
 */
const resetFirstLogin = () => {
  const chaos = createChaosSocketFactory(defaultSocketFactory())
  let armed = null
  chaos.arm({}, (socket) => {
    if (armed) return
    armed = socket
    socket.dropResponses(1, { apiKey: SaslAuthenticate, thenClose: true })
  })
  return { socketFactory: chaos, armed: () => armed, sockets: chaos.sockets }
}

/**
 * Reads the subject claim of the unsecured JWT an OAUTHBEARER connection's provider returns.
 * @param {{ sasl: { oauthBearerProvider: () => { value: string }|Promise<{ value: string }> } }} opts
 *   the OAUTHBEARER connection options
 * @returns {Promise<string>} the token's `sub` claim, the principal name the broker derives from it
 */
const tokenSubject = async ({ sasl }) => {
  const { value } = await sasl.oauthBearerProvider()
  return JSON.parse(Buffer.from(value.split('.')[1], 'base64url').toString()).sub
}

describe('Broker > connect', () => {
  let broker, connectionPool

  beforeEach(() => {
    connectionPool = createConnectionPool(connectionOpts())
    broker = new Broker({ connectionPool, logger: newLogger() })
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  test('establish the connection', async () => {
    await broker.connect()
    expect(broker.connectionPool.isConnected()).toEqual(true)
  })

  test('load api versions if not provided', async () => {
    expect(broker.versions).toEqual(null)
    await broker.connect()
    expect(broker.versions).toBeTruthy()
  })

  test("throws if the mechanism isn't supported by the server", async () => {
    broker = new Broker({
      connectionPool: createConnectionPool(
        Object.assign(sslConnectionOpts(), {
          port: 9094,
          sasl: {
            mechanism: 'fake-mechanism',
            authenticationProvider: () => ({
              authenticate: async () => {
                throw new Error('🥸')
              },
            }),
          },
        })
      ),
      logger: newLogger(),
    })

    await expect(broker.connect()).rejects.toThrow(
      'The broker does not support the requested SASL mechanism'
    )
  })

  describeIfOauthbearerDisabled('when PLAIN is configured', () => {
    test('user provided authenticator overrides built in ones', async () => {
      broker = new Broker({
        connectionPool: createConnectionPool(
          Object.assign(sslConnectionOpts(), {
            port: 9094,
            sasl: {
              mechanism: 'PLAIN',
              authenticationProvider: () => ({
                authenticate: async () => {
                  throw new Error('test error')
                },
              }),
            },
          })
        ),
        logger: newLogger(),
      })

      await expect(broker.connect()).rejects.toThrow('test error')
    })
  })

  for (const e of saslEntries) {
    test(`authenticate with SASL ${e.name} if configured`, async () => {
      broker = new Broker({
        connectionPool: createConnectionPool(e.opts()),
        logger: newLogger(),
      })
      expect(broker.isConnected()).toEqual(false)
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
    })
  }

  describeIfOauthbearerDisabled('when SASL SCRAM is configured', () => {
    test('parallel calls to connect using SCRAM', async () => {
      broker = new Broker({
        connectionPool: createConnectionPool(saslSCRAM256ConnectionOpts()),
        logger: newLogger(),
      })

      expect(broker.isConnected()).toEqual(false)

      await Promise.all([
        broker.connect(),
        broker.connect(),
        broker.connect(),
        broker.connect(),
        broker.connect(),
      ])

      expect(broker.isConnected()).toEqual(true)
    })
  })

  describe('#isConnected', () => {
    test('returns false when not connected', () => {
      expect(broker.isConnected()).toEqual(false)
    })

    for (const e of saslEntries) {
      test(`returns false when connected but not authenticated on connections with SASL ${e.name}`, async () => {
        const connectionPool = createConnectionPool(e.opts())
        broker = new Broker({ connectionPool, logger: newLogger() })
        expect(broker.isConnected()).toEqual(false)
        await connectionPool.getConnection()
        expect(broker.isConnected()).toEqual(false)
      })
    }

    test('returns true when connected', async () => {
      await broker.connect()
      expect(broker.isConnected()).toEqual(true)
    })

    describe('SaslAuthenticate flow', () => {
      for (const e of saslEntries) {
        test(`returns true when connected and authenticated on connections with SASL ${e.name}`, async () => {
          const connectionPool = createConnectionPool(e.opts())
          broker = new Broker({ connectionPool, logger: newLogger() })
          await broker.connect()
          expect(broker.isConnected()).toEqual(true)
        })

        test(`returns false when the session lifetime has expired with SASL ${e.name}`, async () => {
          const sessionLifetime = 15000
          const reauthenticationThreshold = 10000
          const connectionPool = createConnectionPool({ ...e.opts(), reauthenticationThreshold })
          broker = new Broker({ connectionPool, logger: newLogger() })

          await broker.connect()
          expect(broker.isConnected()).toEqual(true)

          const connection = await connectionPool.getConnection()

          connection.sessionLifetime = Long.fromValue(sessionLifetime)
          const [seconds] = connection.authenticatedAt
          connection.authenticatedAt = [seconds - sessionLifetime / 1000, 0]

          expect(broker.isConnected()).toEqual(false)
        })

        test(`returns true when the session lifetime is 0 with SASL ${e.name}`, async () => {
          const connectionPool = createConnectionPool(e.opts())
          broker = new Broker({ connectionPool, logger: newLogger() })

          await broker.connect()
          expect(broker.isConnected()).toEqual(true)

          const connection = await connectionPool.getConnection()
          connection.sessionLifetime = Long.ZERO
          connection.authenticatedAt = [0, 0]

          expect(broker.isConnected()).toEqual(true)
        })
      }
    })

    describeIfOauthbearerDisabled('when SASL SCRAM is configured', () => {
      test('parallel calls to connect using SCRAM', async () => {
        broker = new Broker({
          connectionPool: createConnectionPool(saslSCRAM256ConnectionOpts()),
          logger: newLogger(),
        })

        expect(broker.isConnected()).toEqual(false)

        await Promise.all([
          broker.connect(),
          broker.connect(),
          broker.connect(),
          broker.connect(),
          broker.connect(),
        ])

        expect(broker.isConnected()).toEqual(true)
      })
    })
  })

  describe('a connection reset during SASL login', () => {
    for (const e of saslEntries) {
      test(`a reset while the SASL ${e.name} login is in flight is a retriable connection error and a reconnect completes a request`, async () => {
        const reset = resetFirstLogin()
        broker = new Broker({
          connectionPool: createConnectionPool({ ...e.opts(), socketFactory: reset.socketFactory }),
          logger: newLogger(),
        })

        const error = await broker.connect().catch((failure) => failure)

        expect(reset.armed().responseRule.count).toBe(0)
        expect(error).toBeInstanceOf(KafkaJSConnectionError)
        expect(error).not.toBeInstanceOf(KafkaJSSASLAuthenticationError)
        expect(error.retriable).toBe(true)

        await broker.connect()
        const { brokers } = await broker.metadata([])

        expect(broker.isConnected()).toEqual(true)
        expect(brokers.length).toBeGreaterThan(0)
        expect(reset.sockets.length).toBeGreaterThan(1)
        expect(reset.sockets[0]).toBe(reset.armed())
      })
    }

    for (const e of saslEntries.filter((entry) => entry.wrongOpts)) {
      test(`a wrong SASL ${e.name} password stays a terminal authentication error without the password`, async () => {
        const wrong = e.wrongOpts()
        const chaos = createChaosSocketFactory(defaultSocketFactory())
        broker = new Broker({
          connectionPool: createConnectionPool({
            ...wrong,
            sasl: { ...wrong.sasl, password: `wrong-${SECRET}` },
            socketFactory: chaos,
          }),
          logger: newLogger(),
        })

        const error = await broker.connect().catch((failure) => failure)

        expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
        expect(error.retriable).toBe(false)
        expect(error.message).toMatch(e.expectedErr)
        expect(`${error.message}\n${error.stack}\n${error.cause?.message}`).not.toContain(SECRET)
        expect(chaos.sockets).toHaveLength(1)
      })
    }

    describeIfOauthbearerDisabled('with an ordinary consumer', () => {
      test('a consumer whose SASL PLAIN login is reset joins and consumes after reconnecting', async () => {
        const topic = `sasl-reset-${secureRandom()}`
        await createTopic({ topic })
        const reset = resetFirstLogin()
        const { ssl, sasl } = saslConnectionOpts()
        const kafka = new Kafka({
          brokers: saslBrokers(),
          ssl,
          sasl,
          socketFactory: reset.socketFactory,
          logLevel: logLevel.NOTHING,
          retry: { initialRetryTime: 100, retries: 8 },
        })
        const producer = kafka.producer()
        const consumer = kafka.consumer({ groupId: `sasl-reset-${secureRandom()}` })
        const consumed = []
        try {
          await consumer.connect()
          await consumer.subscribe({ topic, fromBeginning: true })
          await consumer.run({
            eachMessage: async ({ message }) => {
              consumed.push(message.value.toString())
            },
          })
          await producer.connect()
          await producer.send({ topic, messages: [{ value: 'after-reset' }] })

          await waitForMessages(consumed, { number: 1 })

          expect(reset.armed().responseRule.count).toBe(0)
          expect(consumed).toEqual(['after-reset'])
        } finally {
          await Promise.allSettled([consumer.disconnect(), producer.disconnect()])
        }
      }, 60000)
    })
  })

  describeIfOauthbearerEnabled('an expired SASL OAUTHBEARER token', () => {
    test('an expired token at connect is a terminal authentication error and is not retried', async () => {
      const opts = saslOAuthBearerConnectionOpts()
      const chaos = createChaosSocketFactory(defaultSocketFactory())
      broker = new Broker({
        connectionPool: createConnectionPool({
          ...opts,
          sasl: {
            ...opts.sasl,
            oauthBearerProvider: async () => ({ value: expiredUnsecuredJwt({ sub: 'test' }) }),
          },
          socketFactory: chaos,
        }),
        logger: newLogger(),
      })

      const error = await broker.connect().catch((failure) => failure)

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.message).toBe(
        'SASL OAUTHBEARER authentication failed: SASL Authentication failed'
      )
      expect(error.retriable).toBe(false)
      expect(broker.isConnected()).toEqual(false)
      expect(chaos.sockets).toHaveLength(1)
    })
  })

  describeIfOauthbearerEnabled('SASL OAUTHBEARER authorizationIdentity', () => {
    test('an authorizationIdentity equal to the token subject logs in and the connection serves a request', async () => {
      const opts = saslOAuthBearerConnectionOpts()
      broker = new Broker({
        connectionPool: createConnectionPool({
          ...opts,
          sasl: { ...opts.sasl, authorizationIdentity: await tokenSubject(opts) },
        }),
        logger: newLogger(),
      })

      await broker.connect()
      const { brokers } = await broker.metadata([])

      expect(broker.isConnected()).toEqual(true)
      expect(brokers.length).toBeGreaterThan(0)
    })

    test('an authorizationIdentity different from the token subject is a terminal authentication error and is not retried', async () => {
      const opts = saslOAuthBearerConnectionOpts()
      const chaos = createChaosSocketFactory(defaultSocketFactory())
      broker = new Broker({
        connectionPool: createConnectionPool({
          ...opts,
          sasl: { ...opts.sasl, authorizationIdentity: `not-${await tokenSubject(opts)}` },
          socketFactory: chaos,
        }),
        logger: newLogger(),
      })

      const error = await broker.connect().catch((failure) => failure)

      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.message).toMatch(/^SASL OAUTHBEARER authentication failed: /)
      expect(error.retriable).toBe(false)
      expect(broker.isConnected()).toEqual(false)
      expect(chaos.sockets).toHaveLength(1)
    })
  })
})
