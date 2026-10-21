const { Kafka, logLevel } = require('../../../index')
const defaultSocketFactory = require('../socketFactory')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const { readLog } = require('../../../testHelpers/rawLog')
const { NODES } = require('../../../testHelpers/leaderElection')
const {
  createTopic,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  evidenceLines,
  wireVersionsOf,
} = require('testHelpers')
const { problemsOf } = require('../../../scripts/evidence/verify')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it blackholes a live broker connection. Run alone: `npm run test:e2e:request-timeout-close`.
const describeTimeoutClose =
  process.env.REQUEST_TIMEOUT_CLOSE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'request-timeout-close'
// The lines earlier runs left behind, so the last case judges only this run's.
const linesBefore = evidenceLines(EVIDENCE).length
const REQUEST_TIMEOUT_MS = 5000
// The answers to the burst are held far past the request timeout, then swallowed by the blackhole.
const HOLD_MS = 120000
const BURST = 5
const PER_SEND = 10
const APPEND_BUDGET = { attempts: 80, delay: 250 } // 20 s
// One request timeout, the reconnect and the resent batches, with room for the retry backoff.
const CASE_BUDGET_MS = 45000
const HOOK_BUDGET_MS = 60000
const DISCONNECT_BUDGET_MS = 15000

/**
 * Settles `promise` or gives up after `ms`.
 * @param {Promise<*>} promise
 * @param {number} ms
 * @returns {Promise<{ state: 'resolved'|'rejected'|'pending', error?: Error }>}
 */
const within = async (promise, ms) => {
  let timer
  const pending = new Promise((resolve) => {
    timer = setTimeout(() => resolve({ state: 'pending' }), ms)
  })
  try {
    return await Promise.race([
      promise.then(
        () => ({ state: 'resolved' }),
        (error) => ({ state: 'rejected', error })
      ),
      pending,
    ])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * A request timeout over real TCP. The broker appends a burst of idempotent batches, their answers
 * never reach the client (held, then blackholed by the chaos wrapper: application level, no
 * kernel fault), and the client must close the timed-out connection, reconnect and resend the
 * batches in sequence order, which the broker deduplicates.
 *
 * Replay sets: durable log, none (each sequence value exactly once, in order); committed offsets
 * and handler delivery do not apply (producer only).
 */
describeTimeoutClose('Request timeout closes its connection over real TCP (live)', () => {
  const topic = `e2e-p9a-request-timeout-close-${secureRandom()}`
  const chaos = createChaosSocketFactory(defaultSocketFactory())
  const requests = []
  const timeouts = []
  let admin
  let producer

  beforeAll(async () => {
    await createTopic({ topic, partitions: 1, replicas: 3 })
    admin = new Kafka({
      clientId: 'request-timeout-close-observer',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    }).admin()
    producer = new Kafka({
      clientId: 'request-timeout-close',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: chaos,
      requestTimeout: REQUEST_TIMEOUT_MS,
      // Sized to outlive one request timeout and the reconnect, as the bounce scenario's producer.
      retry: { retries: 40, initialRetryTime: 200, maxRetryTime: 2000 },
    }).producer({ idempotent: true, maxInFlightRequests: 5 })
    producer.on(producer.events.REQUEST, ({ payload }) => requests.push(payload))
    producer.on(producer.events.REQUEST_TIMEOUT, ({ payload }) => timeouts.push(payload))
    await admin.connect()
    await producer.connect()
  }, HOOK_BUDGET_MS)

  afterAll(async () => {
    chaos.sockets.forEach((socket) => socket.destroy())
    await withTimeout(
      producer?.disconnect().catch(() => {}),
      DISCONNECT_BUDGET_MS
    )
    await admin?.deleteTopics({ topics: [topic] }).catch(() => {})
    await withTimeout(
      admin?.disconnect().catch(() => {}),
      DISCONNECT_BUDGET_MS
    )
  }, HOOK_BUDGET_MS)

  test(
    'the timed-out connection is closed and the idempotent batches are resent in order, each once',
    async () => {
      const message = (seq) => ({ value: JSON.stringify({ seq }) })
      await producer.send({ topic, messages: [message(0)] })
      const { topics } = await admin.fetchTopicMetadata({ topics: [topic] })
      const leader = topics[0].partitions[0].leader
      const { port } = Object.values(NODES).find(({ nodeId }) => nodeId === leader)
      const blackholed = chaos.sockets.filter((s) => s.endpoint.port === port && !s.destroyed)
      expect(blackholed.length).toBeGreaterThan(0)

      blackholed.forEach((socket) => socket.delay({ read: HOLD_MS }))
      let seq = 1
      const sends = Array.from({ length: BURST }, () =>
        producer.send({
          topic,
          messages: Array.from({ length: PER_SEND }, () => message(seq++)),
        })
      )
      const appended = await waitForCondition(async () => {
        const [{ high }] = await admin.fetchTopicOffsets(topic)
        return Number(high) === 1 + BURST * PER_SEND
      }, APPEND_BUDGET)
      expect(appended).toBe(true)
      blackholed.forEach((socket) => socket.blackhole())

      const outcome = await within(Promise.all(sends), CASE_BUDGET_MS)
      const reopened = chaos.sockets.filter(
        (s) => s.endpoint.port === port && !blackholed.includes(s)
      )
      const [log] = (await readLog({ brokers: plainTextBrokers(), topic })).values()
      const logged = log.records.map(({ value }) => JSON.parse(value).seq)
      writeEvidence(EVIDENCE, {
        step: 'blackhole',
        leader,
        outcome: outcome.state,
        error: outcome.error?.name,
        timeouts: timeouts.length,
        blackholed: blackholed.length,
        destroyed: blackholed.filter((s) => s.destroyed).length,
        reopened: reopened.length,
        logged: logged.length,
        wireVersions: wireVersionsOf(requests),
        config: { requestTimeout: REQUEST_TIMEOUT_MS, maxInFlightRequests: 5, idempotent: true },
      })

      expect(outcome).toEqual({ state: 'resolved' })
      expect(timeouts.length).toBeGreaterThan(0)
      expect(blackholed.some((socket) => socket.destroyed)).toBe(true)
      expect(reopened.length).toBeGreaterThan(0)
      expect(logged).toEqual(Array.from({ length: 1 + BURST * PER_SEND }, (_, index) => index))
    },
    CASE_BUDGET_MS + HOOK_BUDGET_MS
  )

  test('every evidence line of this run carries the stamps a verifier checks', () => {
    const lines = evidenceLines(EVIDENCE).slice(linesBefore)
    const verified = {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      brokerVersion: process.env.KAFKA_VERSION,
    }

    expect(lines.map(({ step }) => step)).toEqual(['blackhole'])
    for (const line of lines) expect(problemsOf(line, verified)).toEqual([])
    expect(lines[0].wireVersions).toHaveProperty('Produce')
  })
})
