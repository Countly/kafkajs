const SASLAuthenticator = require('./index')
const ConnectionPool = require('../../network/connectionPool')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { NETWORK_AUTHENTICATION_FAILURE } = require('../../network/instrumentationEvents')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSConnectionError } = require('../../errors')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger, connectionOpts } = require('testHelpers')

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
    pool && (await pool.destroy())
  })

  it('emits NETWORK_AUTHENTICATION_FAILURE with the broker error message, re-throws the error unchanged, and blacks the broker out', async () => {
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

    // Second connection attempt is refused locally, without ever touching the socket.
    connection.connect = jest.fn()
    await expect(pool.getConnection()).rejects.toThrow(KafkaJSConnectionError)
    await expect(pool.getConnection()).rejects.toThrow(/SASL authentication recently failed/)
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

  describe('blackout window sizing', () => {
    afterEach(() => {
      jest.useRealTimers()
    })

    it('is at least 1000ms even when authenticationTimeout is smaller', async () => {
      jest.useFakeTimers()
      pool = createPool({ authenticationTimeout: 10 })
      pool.recordAuthenticationFailure()

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
      pool.recordAuthenticationFailure()

      jest.advanceTimersByTime(4999)
      await expect(pool.getConnection()).rejects.toThrow(/SASL authentication recently failed/)

      jest.advanceTimersByTime(2)
      pool.pool[0].connect = jest.fn().mockResolvedValue(true)
      pool.pool[0].isConnected = jest.fn().mockReturnValue(true)
      await expect(pool.getConnection()).resolves.toBe(pool.pool[0])
    })
  })
})
