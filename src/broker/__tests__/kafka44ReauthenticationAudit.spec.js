const Broker = require('../index')
const ConnectionPool = require('../../network/connectionPool')
const { KafkaJSSASLAuthenticationError } = require('../../errors')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { newLogger } = require('testHelpers')

const BLACKOUT_MS = 1000
const ATTEMPT_EVERY_MS = 100
const SASL_AUTHENTICATION_FAILED = 58

describe('Kafka 4.4 client fix audit', () => {
  let cluster
  let broker

  beforeEach(() => {
    jest.useFakeTimers()
  })

  afterEach(async () => {
    await broker?.disconnect({ drainTimeout: 0 })
    await jest.advanceTimersByTimeAsync(1)
    cluster.assertClean()
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  test('KAFKA-20253 failed reauthentication cannot spin consumer retries', async () => {
    const attempts = []
    let failing = false
    cluster = createScriptedCluster({
      nodes: [{ id: 0, host: 'node', port: 9092 }],
      profile: 'sasl',
    })
    cluster.answer('SaslAuthenticate', () => {
      if (!failing)
        return {
          errorCode: 0,
          errorMessage: null,
          authBytes: Buffer.alloc(0),
          sessionLifetimeMs: '10000',
        }
      attempts.push(Date.now())
      return {
        errorCode: SASL_AUTHENTICATION_FAILED,
        errorMessage: 'rejected',
        authBytes: null,
        sessionLifetimeMs: '0',
      }
    })
    broker = new Broker({
      connectionPool: new ConnectionPool({
        host: 'node',
        port: 9092,
        sasl: { mechanism: 'plain', username: 'user', password: 'secret' },
        socketFactory: cluster.socketFactory,
        logger: newLogger(),
        connectionTimeout: 2000,
        authenticationTimeout: BLACKOUT_MS,
        requestTimeout: 5000,
        reauthenticationThreshold: 10000,
      }),
      logger: newLogger(),
      authenticationTimeout: BLACKOUT_MS,
    })
    const connecting = broker.connect()
    await jest.advanceTimersByTimeAsync(10)
    await connecting

    // The session lifetime equals the threshold, so the next request must reauthenticate.
    failing = true
    const errors = []
    const dials = () => cluster.sockets().length
    const socketsBefore = dials()
    const start = Date.now()
    for (let elapsed = 0; elapsed <= 3 * BLACKOUT_MS; elapsed += ATTEMPT_EVERY_MS) {
      const call = broker.metadata([]).catch((error) => errors.push(error))
      await jest.advanceTimersByTimeAsync(ATTEMPT_EVERY_MS)
      await call
      const written = attempts.length
      for (let turn = 0; turn < 20; turn++) await Promise.resolve()
      expect(attempts).toHaveLength(written)
    }

    expect(errors.length).toBe(Math.floor((3 * BLACKOUT_MS) / ATTEMPT_EVERY_MS) + 1)
    expect(errors.every((error) => error instanceof KafkaJSSASLAuthenticationError)).toBe(true)
    expect(attempts.length).toBeGreaterThanOrEqual(3)
    expect(attempts.length).toBeLessThanOrEqual(4)
    for (let index = 1; index < attempts.length; index++) {
      expect(attempts[index] - attempts[index - 1]).toBeGreaterThanOrEqual(BLACKOUT_MS)
    }
    expect(attempts[0] - start).toBeLessThan(ATTEMPT_EVERY_MS)
    expect(dials()).toBe(socketsBefore)

    await broker.disconnect({ drainTimeout: 0 })
    const written = cluster.requests().length
    await jest.advanceTimersByTimeAsync(5 * BLACKOUT_MS)
    expect(cluster.requests()).toHaveLength(written)
    expect(jest.getTimerCount()).toBe(0)
  })
})
