const {
  saslOAuthBearerConnectionOpts,
  newLogger,
  describeIfOauthbearerEnabled,
  createConnectionPool,
} = require('testHelpers')

const { SaslAuthenticate: apiSaslAuthenticateKey } = require('../../protocol/requests/apiKeys')
const Broker = require('../index')
const defaultSocketFactory = require('../../network/socketFactory')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const { KafkaJSConnectionError, KafkaJSSASLAuthenticationError } = require('../../errors')

/**
 * An unsecured JWT the broker's unsecured validator rejects, since it expired a minute ago.
 * @returns {string}
 */
const expiredJwt = () => {
  const encode = (value) => Buffer.from(JSON.stringify(value)).toString('base64url')
  const now = Math.floor(Date.now() / 1000)
  return `${encode({ alg: 'none', typ: 'JWT' })}.${encode({ sub: 'test', iat: now - 120, exp: now - 60 })}.`
}

/**
 * Session lifetime on broker is configured in docker-compose.oauthbearer.yml with 15000ms in env KAFKA_CONNECTIONS_MAX_REAUTH_MS
 * see https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#brokerconfigs_connections.max.reauth.ms
 */
describeIfOauthbearerEnabled('Brokers re-authentication with SASL OAUTHBEARER', () => {
  let broker

  const wait = (time = 1000) =>
    new Promise((resolve) => {
      setTimeout(resolve, time)
    })

  const getAuthCalls = (spy) =>
    spy.mock.calls.filter((mock) => mock[0].request.apiKey === apiSaslAuthenticateKey)

  test('Does not re-authenticate when still within session validity threshold', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 2000,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await broker.listGroups()
    await wait(1000)
    await broker.listGroups()
    await wait(1000)
    await broker.listGroups()

    expect(getAuthCalls(spy).length).toBe(1)
  })

  test('Re-authenticate if needed before making a request', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 14900,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await broker.listGroups()

    expect(getAuthCalls(spy).length).toBe(2)
  })

  test('Re-authenticate only once with multiple parallel requests', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 14900,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await Promise.all([
      broker.listGroups(),
      broker.listGroups(),
      broker.listGroups(),
      broker.listGroups(),
    ])

    expect(getAuthCalls(spy).length).toBe(2)
  })

  test('Re-authenticate every request sent when re-authentication threshold is same as session lifetime', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 15000,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await broker.listGroups()
    await wait(1000)
    await broker.listGroups()
    await wait(1000)
    await broker.listGroups()

    expect(getAuthCalls(spy).length).toBe(4)
  })

  test('Re-authenticates only once with multiple requests sent in parallel when re-authentication threshold is same as session lifetime', async () => {
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 15000,
    })
    const connection = await connectionPool.getConnection()
    const spy = jest.spyOn(connection, 'send')
    broker = new Broker({ connectionPool, logger: newLogger() })
    await broker.connect()
    await wait(1000)
    await Promise.all([
      broker.listGroups(),
      broker.listGroups(),
      broker.listGroups(),
      broker.listGroups(),
    ])

    expect(getAuthCalls(spy).length).toBe(2)
  })

  test('a reset during re-authentication is a retriable connection error and the next request logs in on a new socket', async () => {
    const chaos = createChaosSocketFactory(defaultSocketFactory())
    const connectionPool = createConnectionPool({
      ...saslOAuthBearerConnectionOpts(),
      reauthenticationThreshold: 14900,
      socketFactory: chaos,
    })
    broker = new Broker({ connectionPool, logger: newLogger() })
    try {
      await broker.connect()
      const [first] = chaos.sockets
      first.dropResponses(1, { apiKey: apiSaslAuthenticateKey, thenClose: true })
      await wait(1000)

      const error = await broker.listGroups().catch((failure) => failure)

      expect(first.responseRule.count).toBe(0)
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      expect(error).not.toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(true)

      const { groups } = await broker.listGroups()

      expect(Array.isArray(groups)).toBe(true)
      expect(chaos.sockets).toHaveLength(2)
    } finally {
      await broker.disconnect()
    }
  })

  test('a token the broker rejects at re-authentication stays a terminal authentication error', async () => {
    const opts = saslOAuthBearerConnectionOpts()
    let calls = 0
    const valid = opts.sasl.oauthBearerProvider
    const chaos = createChaosSocketFactory(defaultSocketFactory())
    const connectionPool = createConnectionPool({
      ...opts,
      sasl: {
        ...opts.sasl,
        oauthBearerProvider: async () => (calls++ === 0 ? valid() : { value: expiredJwt() }),
      },
      reauthenticationThreshold: 14900,
      socketFactory: chaos,
    })
    broker = new Broker({ connectionPool, logger: newLogger() })
    try {
      await broker.connect()
      await wait(1000)

      const error = await broker.listGroups().catch((failure) => failure)

      expect(calls).toBe(2)
      expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
      expect(error.retriable).toBe(false)
      expect(chaos.sockets).toHaveLength(1)
    } finally {
      await broker.disconnect()
    }
  })
})
