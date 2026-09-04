const SASLAuthenticator = require('./index')
const ConnectionPool = require('../../network/connectionPool')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { NETWORK_AUTHENTICATION_FAILURE } = require('../../network/instrumentationEvents')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSConnectionError, KafkaJSSASLAuthenticationError } = require('../../errors')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger, connectionOpts } = require('testHelpers')

const {
  events: consumerAuthEvents,
  wrap: consumerWrap,
  unwrap: consumerUnwrap,
} = require('../../consumer/instrumentationEvents')
const {
  events: producerAuthEvents,
  wrap: producerWrap,
  unwrap: producerUnwrap,
} = require('../../producer/instrumentationEvents')
const {
  events: adminAuthEvents,
  wrap: adminWrap,
  unwrap: adminUnwrap,
} = require('../../admin/instrumentationEvents')

const versionsWith = (overrides) => ({
  [apiKeys.SaslHandshake]: { minVersion: 0, maxVersion: 1 },
  [apiKeys.SaslAuthenticate]: { minVersion: 0, maxVersion: 1 },
  ...overrides,
})

const createPool = (opts = {}) =>
  new ConnectionPool({
    ...connectionOpts(),
    sasl: { mechanism: 'plain', username: 'test', password: 'wrong-password' },
    ...opts,
  })

describe('Broker > SASLAuthenticator > authentication failure diagnostics', () => {
  let pool

  afterEach(async () => {
    if (pool) {
      await pool.destroy()
      pool = null
    }
  })

  it('emits NETWORK_AUTHENTICATION_FAILURE with the broker error message, re-throws the error unchanged, and blacks the broker out with a non-retriable error carrying that message', async () => {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    pool = createPool({ authenticationTimeout: 5000, instrumentationEmitter })

    const connection = pool.pool[0]
    const brokerErrorMessage = 'Authentication failed: Invalid username or password'

    connection.send = jest
      .fn()
      .mockResolvedValueOnce({ enabledMechanisms: ['PLAIN'] }) // SaslHandshake
      .mockRejectedValueOnce(
        Object.assign(createErrorFromCode(58), { message: brokerErrorMessage })
      ) // SaslAuthenticate

    const events = []
    instrumentationEmitter.addListener(NETWORK_AUTHENTICATION_FAILURE, (event) =>
      events.push(event)
    )

    const authenticator = new SASLAuthenticator(connection, newLogger(), versionsWith({}))

    let thrown
    try {
      await authenticator.authenticate()
    } catch (e) {
      thrown = e
    }

    // Compat: the thrown class is still the mechanism's own error, not a new one.
    expect(thrown).toBeDefined()
    expect(thrown.name).toBe('KafkaJSSASLAuthenticationError')
    expect(thrown.message).toContain('SASL PLAIN authentication failed')
    expect(thrown.message).toContain(brokerErrorMessage)

    expect(events).toHaveLength(1)
    expect(events[0].payload).toEqual({
      broker: connection.broker,
      mechanism: 'PLAIN',
      errorMessage: brokerErrorMessage,
    })

    // Second (re)authentication attempt is refused locally, without ever touching the socket,
    // by a non-retriable error of the same class the original failure had, carrying its message.
    connection.connect = jest.fn()
    let blackoutError
    try {
      await pool.getConnection()
    } catch (e) {
      blackoutError = e
    }
    expect(blackoutError).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(blackoutError.retriable).toBe(false)
    expect(blackoutError.message).toContain(brokerErrorMessage)
    expect(blackoutError.message).toMatch(/refusing to reconnect/)
    expect(connection.connect).not.toHaveBeenCalled()
  })

  it('does not emit the event or blackout the broker for a failure that is not SASL_AUTHENTICATION_FAILED', async () => {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    pool = createPool({ authenticationTimeout: 5000, instrumentationEmitter })

    const connection = pool.pool[0]

    connection.send = jest
      .fn()
      .mockResolvedValueOnce({ enabledMechanisms: ['PLAIN'] })
      .mockRejectedValueOnce(new KafkaJSConnectionError('socket hang up'))

    const events = []
    instrumentationEmitter.addListener(NETWORK_AUTHENTICATION_FAILURE, (event) =>
      events.push(event)
    )

    const authenticator = new SASLAuthenticator(connection, newLogger(), versionsWith({}))
    await expect(authenticator.authenticate()).rejects.toThrow('SASL PLAIN authentication failed')

    expect(events).toHaveLength(0)

    // Not blacked out: getConnection() proceeds to (attempt to) connect as usual.
    connection.connect = jest.fn().mockResolvedValue(true)
    connection.isConnected = jest.fn().mockReturnValue(true)
    await expect(pool.getConnection()).resolves.toBe(connection)
  })

  it('only gates a (re)authentication attempt: an already-authenticated sibling connection, and traffic sent over it, are unaffected by another connection blacking the broker out', async () => {
    pool = createPool({ authenticationTimeout: 5000 })

    const healthy = pool.pool[0]
    const failing = pool.pool[1]

    // healthy (index 0, e.g. the non-Fetch connection) is already connected and authenticated
    healthy.isConnected = jest.fn().mockReturnValue(true)
    healthy.isAuthenticated = jest.fn().mockReturnValue(true)
    healthy.connect = jest.fn()
    healthy.send = jest.fn().mockResolvedValue({ ok: true })

    // failing (index 1, e.g. the Fetch connection) just had a reauthentication rejected with
    // code 58 -- the socket may still be open, but authenticatedAt is null (isAuthenticated false)
    failing.isConnected = jest.fn().mockReturnValue(true)
    failing.isAuthenticated = jest.fn().mockReturnValue(false)
    failing.connect = jest.fn()
    pool.recordAuthenticationFailure('Invalid username or password')

    // The healthy connection keeps working: getConnection() returns it without touching
    // connect(), and pool.send() (routed to index 0 via a non-Fetch apiKey) goes through.
    await expect(pool.getConnection(0)).resolves.toBe(healthy)
    expect(healthy.connect).not.toHaveBeenCalled()
    await expect(pool.send({ request: { apiKey: apiKeys.Metadata } })).resolves.toEqual({
      ok: true,
    })

    // The failing connection's broker is blacked out, so its own next attempt is refused.
    await expect(pool.getConnection(1)).rejects.toThrow(KafkaJSSASLAuthenticationError)
    expect(failing.connect).not.toHaveBeenCalled()
  })

  it('keeps blacking out a broker after the pool that recorded the failure is destroyed and a new pool is built sharing the same blackout map', async () => {
    pool = createPool({ authenticationTimeout: 5000 })
    pool.recordAuthenticationFailure('Invalid username or password')

    // Mirrors what BrokerPool#buildConnectionPool injects into a rebuilt ConnectionPool.
    const sharedBlackout = pool.authenticationBlackout
    await pool.destroy()

    const rebuiltPool = new ConnectionPool({
      ...connectionOpts(),
      sasl: { mechanism: 'plain', username: 'test', password: 'wrong-password' },
      authenticationTimeout: 5000,
      authenticationBlackout: sharedBlackout,
    })

    await expect(rebuiltPool.getConnection()).rejects.toThrow(KafkaJSSASLAuthenticationError)
    await rebuiltPool.destroy()
  })

  it('the AUTHENTICATION_FAILURE event reaches the public consumer/producer/admin surface via the same wrap/unwrap as REQUEST', () => {
    for (const [events, wrap, unwrap] of [
      [consumerAuthEvents, consumerWrap, consumerUnwrap],
      [producerAuthEvents, producerWrap, producerUnwrap],
      [adminAuthEvents, adminWrap, adminUnwrap],
    ]) {
      expect(events.AUTHENTICATION_FAILURE).toBeDefined()
      expect(unwrap(events.AUTHENTICATION_FAILURE)).toBe(NETWORK_AUTHENTICATION_FAILURE)
      expect(wrap(NETWORK_AUTHENTICATION_FAILURE)).toBe(events.AUTHENTICATION_FAILURE)
    }
  })

  describe('blackout window sizing', () => {
    afterEach(() => {
      jest.useRealTimers()
    })

    it('is at least 1000ms even when authenticationTimeout is smaller', async () => {
      jest.useFakeTimers()
      pool = createPool({ authenticationTimeout: 10 })
      pool.recordAuthenticationFailure('too fast')

      jest.advanceTimersByTime(999)
      await expect(pool.getConnection()).rejects.toThrow(/SASL authentication recently failed/)

      jest.advanceTimersByTime(2)
      pool.pool[0].connect = jest.fn().mockResolvedValue(true)
      pool.pool[0].isConnected = jest.fn().mockReturnValue(true)
      await expect(pool.getConnection()).resolves.toBe(pool.pool[0])
    })

    it('uses authenticationTimeout when it exceeds the 1000ms floor', async () => {
      jest.useFakeTimers()
      pool = createPool({ authenticationTimeout: 5000 })
      pool.recordAuthenticationFailure('too fast')

      jest.advanceTimersByTime(4999)
      await expect(pool.getConnection()).rejects.toThrow(/SASL authentication recently failed/)

      jest.advanceTimersByTime(2)
      pool.pool[0].connect = jest.fn().mockResolvedValue(true)
      pool.pool[0].isConnected = jest.fn().mockReturnValue(true)
      await expect(pool.getConnection()).resolves.toBe(pool.pool[0])
    })
  })

  it('blackout survives pool.destroy() (mirrors broker.disconnect(), which only disconnects connections)', async () => {
    pool = createPool({ authenticationTimeout: 5000 })
    pool.recordAuthenticationFailure('Invalid username or password')

    await pool.destroy()

    pool.pool[0].connect = jest.fn()
    await expect(pool.getConnection()).rejects.toThrow(KafkaJSSASLAuthenticationError)
    expect(pool.pool[0].connect).not.toHaveBeenCalled()
  })
})
