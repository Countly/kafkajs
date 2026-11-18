const SASLAuthenticator = require('./index')
const { failureCause, failureDetail, trustedError } = require('./failure')
const ConnectionPool = require('../../network/connectionPool')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { NETWORK_AUTHENTICATION_FAILURE } = require('../../network/instrumentationEvents')
const { createErrorFromCode } = require('../../protocol/error')
const {
  KafkaJSConnectionError,
  KafkaJSProtocolError,
  KafkaJSSASLAuthenticationError,
  KafkaJSNonRetriableError,
} = require('../../errors')
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

  it('emits NETWORK_AUTHENTICATION_FAILURE with a sanitized description, re-throws the error unchanged, and blacks the broker out with a non-retriable error carrying that description', async () => {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    pool = createPool({ authenticationTimeout: 5000, instrumentationEmitter })

    const connection = pool.pool[0]
    const description = `SASL PLAIN authentication failed: broker=${connection.broker} stage=exchange`

    connection.send = jest
      .fn()
      .mockResolvedValueOnce({ enabledMechanisms: ['PLAIN'] }) // SaslHandshake
      .mockRejectedValueOnce(createErrorFromCode(58)) // SaslAuthenticate

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
    expect(thrown.message).toBe('SASL PLAIN authentication failed: SASL Authentication failed')

    expect(events).toHaveLength(1)
    expect(events[0].payload).toEqual({
      broker: connection.broker,
      mechanism: 'PLAIN',
      errorMessage: description,
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
    expect(blackoutError.message).toContain(description)
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

describe('SASL inert diagnostic inspection', () => {
  test('does not invoke supplier Proxy traps', () => {
    const calls = []
    const error = new Proxy(
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
    expect(failureDetail(error)).toBe('Error')
    expect(calls).toEqual([])
  })

  test('does not invoke a protocol-shaped object code getter', () => {
    const code = jest.fn(() => 58)
    const error = Object.create(KafkaJSProtocolError.prototype, { code: { get: code } })
    expect(failureDetail(error)).toBe('Error')
    expect(code).not.toHaveBeenCalled()
  })

  test('uses only native error own data codes', () => {
    const code = jest.fn(() => 58)
    const accessor = Object.defineProperty(new Error(), 'code', { get: code })
    expect(failureDetail(accessor)).toBe('Error')
    expect(code).not.toHaveBeenCalled()
    expect(failureDetail(Object.assign(new Error('secret'), { code: 58 }))).toBe(
      'SASL Authentication failed'
    )
    expect(failureDetail(createErrorFromCode(58))).toBe('SASL Authentication failed')
  })

  test.each([
    ['Buffers must be of the same length'],
    ['The server final message reports an error'],
    ['Invalid server signature in server final message'],
    ['The server first message reports an error'],
    ['Invalid server nonce, it does not start with the client nonce'],
    ['Invalid server nonce, it adds no server part'],
    ['Missing salt in the server first message'],
    ['Invalid salt in the server first message'],
    ['Invalid iteration count in the server first message'],
    ['Malformed SCRAM server message'],
    ['The SCRAM server message requires an unsupported mandatory extension'],
    ['The SCRAM server message has an unknown or repeated attribute'],
    ['Requested iterations 0 is less than the minimum 4096'],
    ['Requested iterations 4095 is less than the minimum 4096'],
    ['Requested iterations 1048577 is more than the maximum 1048576'],
    ['Requested iterations 1e+21 is more than the maximum 1048576'],
    ['Requested iterations Infinity is more than the maximum 1048576'],
  ])('preserves approved immutable text: %s', (reason) => {
    for (const ErrorClass of [KafkaJSSASLAuthenticationError, KafkaJSNonRetriableError]) {
      const error = trustedError(ErrorClass, reason)
      expect(failureDetail(error)).toBe(reason)
      expect(Reflect.set(error, 'message', 'secret')).toBe(false)
      expect(failureDetail(error)).toBe(reason)
    }
  })

  test.each([
    'secret-marker',
    'Requested iterations secret-marker is less than the minimum 4096',
    'Requested iterations 04095 is less than the minimum 4096',
    'Requested iterations -1 is less than the minimum 4096',
    'Requested iterations 1.5 is less than the minimum 4096',
    'Requested iterations 4096 is less than the minimum 4096',
    'Requested iterations 1048576 is more than the maximum 1048576',
    'Requested iterations NaN is more than the maximum 1048576',
    undefined,
    {},
  ])('rejects unapproved mint text: %p', (reason) => {
    const error = trustedError(KafkaJSSASLAuthenticationError, reason)
    expect(error).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    expect(error.message).toBe('Error')
    expect(failureDetail(error)).toBe('Error')
  })

  test('rejects an unapproved constructor without invoking it', () => {
    const constructor = jest.fn()
    expect(failureDetail(trustedError(constructor, 'Malformed SCRAM server message'))).toBe('Error')
    expect(constructor).not.toHaveBeenCalled()
  })
})

describe('SASL safe failure cause', () => {
  test('names a library error by its class and nothing of its text', () => {
    expect(failureCause(new KafkaJSConnectionError('secret'))).toBe(' cause=KafkaJSConnectionError')
  })

  test('names a protocol error with the type and code its protocol table holds', () => {
    const error = Object.assign(createErrorFromCode(58), { message: 'secret', type: 'secret-type' })
    expect(failureCause(error)).toBe(
      ' cause=KafkaJSProtocolError type=SASL_AUTHENTICATION_FAILED code=58'
    )
  })

  test('names the nearest known class of a subclass it does not know', () => {
    class Custom extends TypeError {}
    expect(failureCause(new Custom('secret'))).toBe(' cause=TypeError')
  })

  test('names a class by identity, not by a forged name or constructor', () => {
    const forged = Object.assign(new KafkaJSConnectionError('x'), {
      name: 'secret',
      constructor: { name: 'secret' },
    })
    expect(failureCause(forged)).toBe(' cause=KafkaJSConnectionError')
    expect(failureCause(Object.assign(new Error('x'), { name: 'TypeError' }))).toBe('')
  })

  test.each([
    ['a plain Error', new Error('secret')],
    ['an Error subclass of no known class', new (class Custom extends Error {})('secret')],
    ['a string', 'secret'],
    ['null', null],
    ['undefined', undefined],
    ['a protocol-shaped object', Object.create(KafkaJSProtocolError.prototype, {})],
  ])('names nothing for %s', (_, failure) => {
    expect(failureCause(failure)).toBe('')
  })

  test('adds no type or code for a code that is not an integer', () => {
    const error = Object.assign(createErrorFromCode(58), { code: '58' })
    expect(failureCause(error)).toBe(' cause=KafkaJSProtocolError')
  })

  test('does not invoke supplier Proxy traps', () => {
    const calls = []
    const error = new Proxy(new KafkaJSProtocolError(createErrorFromCode(58)), {
      getPrototypeOf(target) {
        calls.push('getPrototypeOf')
        return Reflect.getPrototypeOf(target)
      },
      getOwnPropertyDescriptor(target, key) {
        calls.push(`getOwnPropertyDescriptor ${String(key)}`)
        return Reflect.getOwnPropertyDescriptor(target, key)
      },
      get(target, key) {
        calls.push(`get ${String(key)}`)
        return Reflect.get(target, key)
      },
    })
    expect(failureCause(error)).toBe('')
    expect(calls).toEqual([])
  })

  test('stops at a Proxy in the prototype chain without invoking its traps', () => {
    const calls = []
    const ancestor = new Proxy(KafkaJSProtocolError.prototype, {
      getPrototypeOf() {
        calls.push('getPrototypeOf')
        throw new Error('secret')
      },
    })
    const error = Object.setPrototypeOf(new Error('secret'), ancestor)
    expect(failureCause(error)).toBe('')
    expect(calls).toEqual([])
  })

  test('adds no type or code for an integer outside the protocol table', () => {
    const forged = Object.assign(
      Object.setPrototypeOf(new Error('secret'), KafkaJSProtocolError.prototype),
      {
        code: 8675309,
      }
    )
    expect(failureCause(forged)).toBe(' cause=KafkaJSProtocolError')
  })

  test('does not invoke a code getter', () => {
    const code = jest.fn(() => 58)
    const error = Object.defineProperty(createErrorFromCode(58), 'code', { get: code })
    expect(failureCause(error)).toBe(' cause=KafkaJSProtocolError')
    expect(code).not.toHaveBeenCalled()
  })
})
