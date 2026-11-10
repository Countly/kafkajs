const { Kafka } = require('../../../index')
const { KafkaJSSASLAuthenticationError } = require('../../errors')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const BLACKOUT_MS = 1000
const RETRY_MS = 100
const SASL_AUTHENTICATION_FAILED = 58

/**
 * Advances the real consumer's continuations with fake time, without exhausting retry timers.
 * @param {Promise<unknown>} work public setup or shutdown call
 * @returns {Promise<unknown>} its settled outcome
 * @throws {Error} if the call exceeds its fake-time budget
 */
const until = async (work) => {
  const state = { settled: false }
  const tracked = work.finally(() => (state.settled = true))
  tracked.catch(() => {})
  for (let elapsed = 0; !state.settled && elapsed < BLACKOUT_MS; elapsed += 10) {
    await jest.advanceTimersByTimeAsync(10)
  }
  if (!state.settled) throw new Error('Consumer did not settle within the fake-time budget')
  return tracked
}

describe('Kafka 4.4 client fix audit', () => {
  let cluster
  let consumer
  let brokerAnswerTimers

  beforeEach(() => {
    jest.useFakeTimers()
    brokerAnswerTimers = new Set()
  })

  afterEach(async () => {
    if (consumer) await until(consumer.disconnect({ timeout: 0 }))
    for (const timer of brokerAnswerTimers) clearTimeout(timer)
    await jest.advanceTimersByTimeAsync(1)
    cluster.assertClean()
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  test('KAFKA-20253 failed reauthentication cannot spin consumer retries', async () => {
    const attempts = []
    const crashes = []
    const restarts = []
    let failing = false
    cluster = groupScript(
      createScriptedCluster({
        nodes: [{ id: 0, host: 'node', port: 9092 }],
        profile: 'sasl',
      }),
      { batches: 0, brokerAnswerTimers }
    )
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
    consumer = new Kafka(
      cluster.kafkaOptions({
        sasl: { mechanism: 'plain', username: 'user', password: 'secret' },
        authenticationTimeout: BLACKOUT_MS,
        reauthenticationThreshold: 9500,
        retry: { initialRetryTime: RETRY_MS, maxRetryTime: RETRY_MS, factor: 0, multiplier: 1 },
      })
    ).consumer({
      groupId: 'reauthentication-audit',
      maxWaitTimeInMs: 20,
      retry: {
        initialRetryTime: RETRY_MS,
        maxRetryTime: RETRY_MS,
        factor: 0,
        multiplier: 1,
        retries: 2,
        restartOnFailure: async (error) => {
          restarts.push(error)
          return true
        },
      },
    })
    consumer.on(consumer.events.CRASH, ({ payload }) =>
      crashes.push({ at: Date.now(), ...payload })
    )
    await until(consumer.connect())
    await until(consumer.subscribe({ topic: TOPIC }))
    await until(consumer.run({ autoCommit: false, eachMessage: async () => {} }))
    await jest.advanceTimersByTimeAsync(100)
    expect(cluster.requests().some(({ api }) => api === 'JoinGroup')).toBe(true)
    expect(cluster.requests().some(({ api }) => api === 'Fetch')).toBe(true)

    failing = true
    for (let elapsed = 0; elapsed < 4 * BLACKOUT_MS; elapsed += RETRY_MS) {
      await jest.advanceTimersByTimeAsync(RETRY_MS)
      const written = attempts.length
      for (let turn = 0; turn < 20; turn++) await Promise.resolve()
      expect(attempts).toHaveLength(written)
      expect(jest.getTimerCount()).toBeLessThanOrEqual(12)
    }

    expect(attempts.length).toBeGreaterThanOrEqual(3)
    expect(attempts.length).toBeLessThanOrEqual(1 + (4 * BLACKOUT_MS) / RETRY_MS)
    for (let index = 1; index < attempts.length; index++) {
      expect(attempts[index] - attempts[index - 1]).toBeGreaterThanOrEqual(RETRY_MS)
    }
    expect(crashes.length).toBeGreaterThanOrEqual(2)
    expect(crashes.length).toBeLessThanOrEqual((4 * BLACKOUT_MS) / RETRY_MS)
    expect(restarts).toHaveLength(crashes.length)
    expect(crashes.every(({ restart }) => restart)).toBe(true)
    for (let index = 1; index < crashes.length; index++) {
      expect(crashes[index].at - crashes[index - 1].at).toBeGreaterThanOrEqual(RETRY_MS)
    }
    const originalCause = (error) => (error.cause ? originalCause(error.cause) : error)
    expect(
      crashes.every(({ error }) => originalCause(error) instanceof KafkaJSSASLAuthenticationError)
    ).toBe(true)

    await until(consumer.disconnect({ timeout: 0 }))
    await jest.advanceTimersByTimeAsync(100)
    expect(brokerAnswerTimers.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
    expect(cluster.sockets().every((socket) => socket.destroyed)).toBe(true)
    const written = cluster.requests().length
    const dials = cluster.sockets().length
    await jest.advanceTimersByTimeAsync(5 * BLACKOUT_MS)
    expect(cluster.requests()).toHaveLength(written)
    expect(cluster.sockets()).toHaveLength(dials)
    expect(jest.getTimerCount()).toBe(0)
  })
})
