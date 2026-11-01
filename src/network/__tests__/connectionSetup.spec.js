const { EventEmitter } = require('events')
const Connection = require('../connection')
const Broker = require('../../broker')
const ConnectionPool = require('../connectionPool')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../testHelpers/schedule')
const { createLogger } = require('../../loggers')
const {
  KafkaJSConnectionError,
  KafkaJSConnectionClosedError,
  KafkaJSSASLAuthenticationError,
} = require('../../errors')
const { ConnectionSetupPolicy } = require('../connectionSetup')

const logger = createLogger({ level: 0, logCreator: () => () => {} })
let cluster, broker, schedule, gate

/** Creates the real Broker/Connection stack on deterministic fake sockets. @param {object} [options] client fields @returns {void} */
const arrange = (options = {}) => {
  schedule = new Schedule()
  cluster = createScriptedCluster({
    nodes: [{ id: 0, host: 'node', port: 9092 }],
    schedule,
    profile: 'sasl',
  })
  const pool = new ConnectionPool({
    host: 'node',
    port: 9092,
    logger,
    socketFactory: cluster.socketFactory,
    connectionTimeout: 300,
    requestTimeout: 1500,
    ...options,
  })
  broker = new Broker({
    connectionPool: pool,
    logger,
    authenticationTimeout: options.authenticationTimeout ?? 400,
  })
}

beforeEach(() => {
  jest.useFakeTimers()
  arrange()
})
afterEach(async () => {
  await broker.disconnect({ drainTimeout: 0 })
  if (gate?.settle) gate.release({})
  await jest.advanceTimersByTimeAsync(1)
  cluster.assertClean()
  expect(cluster.sockets().every((s) => s.destroyed)).toBe(true)
  expect(jest.getTimerCount()).toBe(0)
  gate = null
  jest.useRealTimers()
})

it.each([
  [false, 'setup'],
  [true, 'setup'],
  [false, 'socket'],
  [true, 'socket'],
])('preserves the connect timeout error with ssl=%p and %s expiry', async (ssl, timer) => {
  const socket = new EventEmitter()
  socket.end = jest.fn()
  socket.destroy = jest.fn()
  socket.unref = jest.fn()
  const connection = new Connection({
    host: 'localhost',
    port: 9093,
    ssl,
    logger,
    connectionTimeout: 3000,
    socketFactory: () => socket,
  })
  try {
    const connecting = connection.connect().catch((error) => error)
    if (timer === 'socket') socket.emit('timeout')
    else await jest.advanceTimersByTimeAsync(3000)
    const error = await connecting
    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error.message).toBe('Connection timeout')
    expect(error.broker).toBe('localhost:9093')
    if (timer === 'setup') {
      expect(error).toMatchObject({ phase: 'TCP/TLS', timeout: 3000 })
    }
    expect(connection.isConnected()).toBe(false)
    expect(socket.destroy).toHaveBeenCalledTimes(1)
    expect(connection.socket).toBeNull()
    expect(jest.getTimerCount()).toBe(0)
  } finally {
    await connection.disconnect({ drainTimeout: 0 })
  }
})

it('bounds an unanswered ApiVersions by the setup budget', async () => {
  gate = schedule.hold({ api: 'ApiVersions' })
  let outcome
  broker.connect().then(
    () => {
      outcome = true
    },
    (error) => {
      outcome = error
    }
  )
  await jest.advanceTimersByTimeAsync(1)
  expect(gate.call.api).toBe('ApiVersions')
  await jest.advanceTimersByTimeAsync(300)
  expect(outcome).toBeInstanceOf(KafkaJSConnectionError)
  expect(outcome.message).toMatch(/ApiVersions.*300ms/)
  expect(outcome.retriable).toBe(true)
})

it('counts a setup request timeout once and delays the next physical dial', async () => {
  await broker.disconnect({ drainTimeout: 0 })
  arrange({ connectionTimeout: 1000, requestTimeout: 50, reconnectBackoff: 300 })
  gate = schedule.hold({ api: 'ApiVersions' })
  const pool = broker.connectionPool
  const outcome = broker.connect().catch((error) => error)
  await jest.advanceTimersByTimeAsync(1)
  expect(gate.call.api).toBe('ApiVersions')
  const [socket] = cluster.sockets()
  await jest.advanceTimersByTimeAsync(99)
  expect(await outcome).toBeInstanceOf(KafkaJSConnectionClosedError)
  expect(socket.destroyed).toBe(true)
  expect(jest.getTimerCount()).toBe(0)
  expect(pool.setupStates.state(pool.brokerAddress, performance.now())).toMatchObject({
    failures: 1,
    waitMs: 300,
  })

  gate.release(cluster.apiVersionsAnswer())
  gate = null
  const reconnect = broker.connect()
  await jest.advanceTimersByTimeAsync(299)
  expect(cluster.sockets()).toHaveLength(1)
  expect(pool.setupStates.state(pool.brokerAddress, performance.now()).failures).toBe(1)
  await jest.advanceTimersByTimeAsync(1)
  expect(cluster.sockets()).toHaveLength(2)
  await jest.advanceTimersByTimeAsync(5)
  await reconnect
  expect(broker.isConnected()).toBe(true)
  expect(pool.setupStates.state(pool.brokerAddress, performance.now()).failures).toBe(0)
  await broker.disconnect({ drainTimeout: 0 })
  await jest.advanceTimersByTimeAsync(1)
  expect(cluster.sockets().every((socket) => socket.destroyed)).toBe(true)
  expect(jest.getTimerCount()).toBe(0)
})

it('bounds an unanswered handshake by the authentication cap', async () => {
  await broker.disconnect({ drainTimeout: 0 })
  arrange({
    connectionTimeout: 2000,
    authenticationTimeout: 400,
    sasl: { mechanism: 'plain', username: 'u', password: 'p' },
  })
  gate = schedule.hold({ api: 'SaslHandshake' })
  let outcome
  broker.connect().catch((error) => {
    outcome = error
  })
  await jest.advanceTimersByTimeAsync(1)
  expect(gate.call.api).toBe('SaslHandshake')
  await jest.advanceTimersByTimeAsync(400)
  expect(outcome).toBeInstanceOf(KafkaJSConnectionError)
  expect(outcome.message).toMatch(/SASL.*2000ms/)
})

it('bounds provider work between SASL requests and detaches it on reconnect', async () => {
  await broker.disconnect({ drainTimeout: 0 })
  let release
  let entered = false
  const held = new Promise((resolve) => {
    release = resolve
  })
  const sasl = {
    mechanism: 'plain',
    authenticationProvider: ({ saslAuthenticate }) => ({
      authenticate: async () => {
        entered = true
        await held
        return saslAuthenticate({
          request: { encode: async () => Buffer.from('late') },
        })
      },
    }),
  }
  arrange({ connectionTimeout: 2000, authenticationTimeout: 400, sasl })
  const pool = broker.connectionPool
  let error
  broker.connect().catch((e) => {
    error = e
  })
  await jest.advanceTimersByTimeAsync(1)
  await jest.advanceTimersByTimeAsync(5)
  expect(entered).toBe(true)
  const old = pool.pool[0]
  await jest.advanceTimersByTimeAsync(400)
  expect(error).toBeInstanceOf(KafkaJSConnectionError)
  sasl.authenticationProvider = () => ({ authenticate: async () => {} })
  const reconnected = broker.connect()
  await jest.advanceTimersByTimeAsync(100)
  await reconnected
  const socket = old.socket
  release()
  await jest.advanceTimersByTimeAsync(1)
  expect(old.socket).toBe(socket)
  expect(socket.destroyed).toBe(false)
  expect(cluster.requests().filter((r) => r.api === 'SaslAuthenticate')).toHaveLength(0)
  expect(old.isAuthenticated()).toBe(true)
})

it('ends a blackholed dial at the node budget', async () => {
  cluster.blackhole(0)
  let error
  broker.connect().catch((e) => {
    error = e
  })
  await jest.advanceTimersByTimeAsync(299)
  expect(error).toBeUndefined()
  await jest.advanceTimersByTimeAsync(1)
  expect(error).toBeInstanceOf(KafkaJSConnectionError)
  expect(error.message).toBe('Connection timeout')
  expect(error).toMatchObject({ phase: 'TCP/TLS', timeout: 300, broker: 'node:9092' })
})

it('shares the original budget when Broker joins a direct connection dial', async () => {
  gate = schedule.hold({ api: 'ApiVersions' })
  const pool = broker.connectionPool
  const dial = pool.getConnection(0)
  let error
  const ready = broker.connect().catch((e) => {
    error = e
  })
  await jest.advanceTimersByTimeAsync(5)
  await dial
  expect(cluster.sockets()).toHaveLength(1)
  await jest.advanceTimersByTimeAsync(300)
  await ready
  expect(error).toBeInstanceOf(KafkaJSConnectionError)
  expect(error.message).toMatch(/ApiVersions/)
  expect(pool.setupStates.state(pool.brokerAddress, performance.now()).failures).toBe(1)
})

it.each(['decode', 'parse'])(
  'an expired negotiation cannot publish after response %s resumes',
  async (stage) => {
    const response = require('../../protocol/requests/apiVersions/v4/response')
    const decode = response[stage]
    let release
    let arrived = false
    const held = new Promise((resolve) => {
      release = resolve
    })
    const decoder = jest.spyOn(response, stage).mockImplementationOnce(async (payload) => {
      arrived = true
      await held
      return decode(payload)
    })
    try {
      let error
      broker.connect().catch((e) => {
        error = e
      })
      await jest.advanceTimersByTimeAsync(5)
      expect(arrived).toBe(true)
      await jest.advanceTimersByTimeAsync(300)
      expect(error).toBeInstanceOf(KafkaJSConnectionError)
      const reconnect = broker.connect()
      await jest.advanceTimersByTimeAsync(100)
      await reconnect
      const connection = broker.connectionPool.pool[0]
      const socket = connection.socket
      const versions = broker.versions
      const throttle = jest.spyOn(connection.requestQueue, 'maybeThrottle')
      release()
      await jest.advanceTimersByTimeAsync(1)
      expect(throttle).not.toHaveBeenCalled()
      expect(broker.versions).toBe(versions)
      expect(connection.socket).toBe(socket)
      expect(socket.destroyed).toBe(false)
      throttle.mockRestore()
    } finally {
      release()
      decoder.mockRestore()
    }
  }
)

it('leaves the shared attempt running when one caller stops waiting', async () => {
  cluster.blackhole(0)
  let physical
  const connect = broker.connect().catch((e) => {
    physical = e
  })
  const caller = Promise.race([
    connect,
    new Promise((resolve) => setTimeout(() => resolve('caller'), 100)),
  ])
  await jest.advanceTimersByTimeAsync(100)
  expect(await caller).toBe('caller')
  expect(cluster.sockets()).toHaveLength(1)
  expect(physical).toBeUndefined()
  await jest.advanceTimersByTimeAsync(200)
  await connect
  expect(physical).toBeInstanceOf(KafkaJSConnectionError)
  expect(broker.connectionPool.setupStates.state('node:9092', 300).failures).toBe(1)
})

it('cancels a backoff wait without dialing or counting another failure', async () => {
  const pool = broker.connectionPool
  const token = pool.setupStates.begin(pool.brokerAddress)
  pool.setupStates.failed(pool.brokerAddress, performance.now(), token)
  let error
  broker.connect().catch((e) => {
    error = e
  })
  await jest.advanceTimersByTimeAsync(1)
  expect(cluster.sockets()).toHaveLength(0)
  await broker.disconnect({ drainTimeout: 0 })
  await jest.advanceTimersByTimeAsync(1)
  expect(error).toBeInstanceOf(KafkaJSConnectionError)
  expect(jest.getTimerCount()).toBe(0)
  expect(pool.setupStates.state(pool.brokerAddress, performance.now()).failures).toBe(1)
})

it('keeps a second caller on the lock through a legitimate thirty-second attempt', async () => {
  await broker.disconnect({ drainTimeout: 0 })
  arrange({
    setupPolicy: ConnectionSetupPolicy.resolve({
      connectionTimeout: 30000,
      reconnectBackoff: 0,
    }),
    connectionTimeout: 1000,
    requestTimeout: 40000,
  })
  gate = schedule.hold({ api: 'ApiVersions' })
  let second
  const first = broker.connect()
  await jest.advanceTimersByTimeAsync(1)
  const next = broker.connect().then(
    () => {
      second = true
    },
    (e) => {
      second = e
    }
  )
  await jest.advanceTimersByTimeAsync(29998)
  expect(second).toBeUndefined()
  gate.release(cluster.apiVersionsAnswer())
  await jest.advanceTimersByTimeAsync(0)
  await first
  await next
  expect(second).toBe(true)
  expect(cluster.sockets()).toHaveLength(1)
})

it.each([
  'handshake decode',
  'provider',
  'mechanism encode',
  'authenticate round trip',
  'authenticate rejection',
  'mechanism decode',
  'mechanism parse',
])('each old SASL stage refuses after replacement: %s', async (stage) => {
  await broker.disconnect({ drainTimeout: 0 })
  let release
  let entered = false
  const held = new Promise((resolve) => {
    release = resolve
  })
  const hold = async () => {
    entered = true
    await held
  }
  const mechanism = {
    request: {
      encode: async () => {
        if (stage === 'mechanism encode') await hold()
        return new (require('../../protocol/encoder'))().writeBytes('\0u\0p').buffer
      },
    },
    response: {
      decode: async (bytes) => {
        if (stage === 'mechanism decode') await hold()
        return bytes
      },
      parse: async (bytes) => {
        if (stage === 'mechanism parse') await hold()
        return bytes
      },
    },
  }
  const sasl = {
    mechanism: 'plain',
    authenticationProvider: ({ saslAuthenticate }) => ({
      authenticate: async () => {
        if (stage === 'provider') await hold()
        await saslAuthenticate(mechanism)
      },
    }),
  }
  let spy
  if (
    stage === 'handshake decode' ||
    stage === 'authenticate round trip' ||
    stage === 'authenticate rejection'
  ) {
    const response = require(
      stage === 'handshake decode'
        ? '../../protocol/requests/saslHandshake/v1/response'
        : '../../protocol/requests/saslAuthenticate/v2/response'
    )
    const decode = response.decode
    spy = jest.spyOn(response, 'decode').mockImplementationOnce(async (bytes) => {
      await hold()
      return decode(bytes)
    })
  }
  arrange({ connectionTimeout: 2000, authenticationTimeout: 400, reconnectBackoff: 0, sasl })
  if (stage === 'authenticate rejection')
    cluster.answer('SaslAuthenticate', () => ({ errorCode: 58, errorMessage: 'old rejection' }))
  try {
    const old = broker.connect().catch((e) => e)
    await jest.advanceTimersByTimeAsync(10)
    expect(entered).toBe(true)
    await jest.advanceTimersByTimeAsync(400)
    expect(await old).toBeInstanceOf(KafkaJSConnectionError)
    sasl.authenticationProvider = () => ({ authenticate: async () => {} })
    const fresh = broker.connect()
    await jest.advanceTimersByTimeAsync(100)
    await fresh
    const connection = broker.connectionPool.pool[0]
    const { socket, authenticatedAt, sessionLifetime, authEpisode } = connection
    const versions = broker.versions
    const count = cluster.requests().length
    release()
    await jest.advanceTimersByTimeAsync(1)
    expect(cluster.requests()).toHaveLength(count)
    expect(connection.socket).toBe(socket)
    expect(socket.destroyed).toBe(false)
    expect(connection.authenticatedAt).toBe(authenticatedAt)
    expect(connection.sessionLifetime).toBe(sessionLifetime)
    expect(connection.authEpisode).toBe(authEpisode)
    expect(broker.versions).toBe(versions)
    expect(broker.connectionPool.authenticationBlackoutEntry()).toBeNull()
  } finally {
    release()
    spy?.mockRestore()
  }
})

it.each(['Fetch', 'reauthentication'])('bounds standalone %s authentication', async (kind) => {
  await broker.disconnect({ drainTimeout: 0 })
  arrange({
    connectionTimeout: 2000,
    authenticationTimeout: 400,
    sasl: {
      mechanism: 'plain',
      authenticationProvider: () => ({ authenticate: async () => {} }),
    },
  })
  const pool = broker.connectionPool
  const connection = pool.pool[kind === 'Fetch' ? 1 : 0]
  const dial = connection.connect()
  await jest.advanceTimersByTimeAsync(1)
  await dial
  connection.setVersions({
    17: { minVersion: 1, maxVersion: 1 },
    36: { minVersion: 2, maxVersion: 2 },
  })
  if (kind === 'reauthentication') {
    const auth = connection.authenticate()
    await jest.advanceTimersByTimeAsync(5)
    await auth
    connection.authenticatedAt = null
  }
  gate = schedule.hold({ api: 'SaslHandshake' })
  const result = connection.authenticate().catch((e) => e)
  await jest.advanceTimersByTimeAsync(401)
  expect(await result).toBeInstanceOf(KafkaJSConnectionError)
  expect(pool.setupStates.state(pool.brokerAddress, performance.now()).failures).toBe(0)
})

it('a caller deadline does not cancel shared physical setup', async () => {
  await broker.disconnect({ drainTimeout: 0 })
  arrange({ connectionTimeout: 10000, reconnectBackoff: 0 })
  cluster.blackhole(0)
  let error
  const connection = broker.connectionPool.pool[0]
  const first = connection.connect().catch((e) => {
    error = e
  })
  const second = connection.connect().catch((e) => e)
  const caller = Promise.race([
    first,
    new Promise((resolve) => setTimeout(() => resolve('deadline'), 1000)),
  ])
  await jest.advanceTimersByTimeAsync(1000)
  expect(await caller).toBe('deadline')
  expect(error).toBeUndefined()
  expect(cluster.sockets()).toHaveLength(1)
  expect(broker.connectionPool.setupStates.state('node:9092', performance.now()).failures).toBe(0)
  await jest.advanceTimersByTimeAsync(9000)
  await first
  expect(error).toBeInstanceOf(KafkaJSConnectionError)
  expect(broker.connectionPool.setupStates.state('node:9092', performance.now()).failures).toBe(1)
  await broker.disconnect({ drainTimeout: 0 })
  expect(await second).toBe(error)
})

it('a held dial cannot install its socket after reconnect', async () => {
  await broker.disconnect({ drainTimeout: 0 })
  let originalConnect
  const factory = cluster.socketFactory
  const pool = broker.connectionPool
  pool.pool[0].socketFactory = (options) => {
    originalConnect = options.onConnect
    return factory({ ...options, onConnect: () => {} })
  }
  const first = broker.connect().catch((e) => e)
  await jest.advanceTimersByTimeAsync(300)
  expect(await first).toBeInstanceOf(KafkaJSConnectionError)
  const [oldSocket] = cluster.sockets()
  pool.pool[0].socketFactory = factory
  const fresh = broker.connect()
  await jest.advanceTimersByTimeAsync(100)
  await fresh
  const connection = pool.pool[0]
  const { socket, generation } = connection
  originalConnect()
  expect(oldSocket.destroyed).toBe(true)
  expect(connection.socket).toBe(socket)
  expect(socket.destroyed).toBe(false)
  expect(connection.generation).toBe(generation)
})

it('observes a synchronous socket factory failure without an unhandled rejection', async () => {
  const { execFileSync } = require('node:child_process')
  const script = `
    const Connection = require('./src/network/connection')
    const { ConnectionSetupPolicy, SetupAttempt } = require('./src/network/connectionSetup')
    const { createLogger } = require('./src/loggers')
    const errors = []
    process.on('unhandledRejection', (e) => errors.push(e))
    ;(async () => {
      const connection = new Connection({ host: 'a', port: 1,
        logger: createLogger({ level: 0, logCreator: () => () => {} }),
        socketFactory: () => { throw new Error('factory') }, connectionTimeout: 10 })
      await connection.connect().catch(() => {})
      await connection.disconnect({ drainTimeout: 0 })
      let now = 0
      const attempt = new SetupAttempt({ address: 'a', policy: ConnectionSetupPolicy.resolve({ connectionTimeout: 1 }), now: () => now })
      attempt.arm(() => {})
      now = 2
      await attempt.guard(Promise.reject(new Error('work')), {}).catch(() => {})
      await new Promise(setImmediate)
      if (errors.length) throw errors[0]
      process.stdout.write('observed')
    })().catch((e) => { process.stderr.write(e.stack); process.exitCode = 1 })
  `
  expect(
    execFileSync(process.execPath, ['-e', script], {
      cwd: process.cwd(),
      encoding: 'utf8',
      timeout: 10000,
    })
  ).toBe('observed')
})

it.each(['protocol', 'mechanism', 'credentials', 'provider', 'retirement'])(
  'a protocol rejection or cancelled setup does not grow node failures: %s',
  async (kind) => {
    await broker.disconnect({ drainTimeout: 0 })
    const providerError = new Error('provider rejected')
    arrange({
      connectionTimeout: 2000,
      authenticationTimeout: 400,
      sasl:
        kind === 'protocol' || kind === 'retirement'
          ? null
          : {
              mechanism: 'plain',
              username: 'u',
              password: 'p',
              ...(kind === 'provider'
                ? {
                    authenticationProvider: () => ({
                      authenticate: async () => {
                        throw providerError
                      },
                    }),
                  }
                : {}),
            },
    })
    if (kind === 'protocol') cluster.answer('ApiVersions', () => ({ errorCode: 42 }))
    if (kind === 'mechanism') cluster.answer('SaslHandshake', () => ({ enabledMechanisms: [] }))
    if (kind === 'credentials')
      cluster.answer('SaslAuthenticate', () => ({ errorCode: 58, errorMessage: 'rejected' }))
    if (kind === 'retirement') cluster.blackhole(0)
    const outcome = broker.connect().catch((e) => e)
    await jest.advanceTimersByTimeAsync(10)
    if (kind === 'retirement') await broker.retire({ drainTimeout: 0 })
    const error = await outcome
    expect(error).toBeInstanceOf(Error)
    if (kind === 'provider') expect(error).toBe(providerError)
    expect(broker.connectionPool.setupStates.state('node:9092', performance.now()).failures).toBe(0)
    expect(Boolean(broker.connectionPool.authenticationBlackoutEntry())).toBe(
      kind === 'credentials'
    )
  }
)

it.each(['success', 'rejection', 'timeout'])(
  'broker reauthentication uses the SASL cap after readiness: %s',
  async (outcome) => {
    await broker.disconnect({ drainTimeout: 0 })
    const sasl = {
      mechanism: 'plain',
      authenticationProvider: () => ({ authenticate: async () => {} }),
    }
    arrange({ connectionTimeout: 100, authenticationTimeout: 400, sasl })
    const initial = broker.connect()
    await jest.advanceTimersByTimeAsync(10)
    await initial
    const pool = broker.connectionPool
    const connection = pool.pool[0]
    const socket = connection.socket
    connection.authenticatedAt = null
    const failure = new KafkaJSSASLAuthenticationError('SASL provider rejected')
    sasl.authenticationProvider = () => ({
      authenticate: async () => {
        await new Promise((resolve) => setTimeout(resolve, outcome === 'timeout' ? 500 : 200))
        if (outcome === 'rejection') throw failure
      },
    })
    let result
    const reauth = broker.connect().then(
      () => {
        result = true
      },
      (error) => {
        result = error
      }
    )
    try {
      await jest.advanceTimersByTimeAsync(101)
      expect(result).toBeUndefined()
      expect(socket.destroyed).toBe(false)
      await jest.advanceTimersByTimeAsync(300)
      await reauth
      if (outcome === 'timeout') {
        expect(result).toBeInstanceOf(KafkaJSConnectionError)
        expect(result.retriable).toBe(true)
        expect(socket.destroyed).toBe(true)
      } else {
        expect(result).toBe(outcome === 'success' ? true : failure)
        expect(socket.destroyed).toBe(false)
      }
      expect(cluster.sockets()).toHaveLength(1)
      expect(cluster.requests().filter(({ api }) => api === 'ApiVersions')).toHaveLength(1)
      expect(pool.setupStates.state(pool.brokerAddress, performance.now()).failures).toBe(0)
    } finally {
      await jest.advanceTimersByTimeAsync(500)
    }
  }
)
