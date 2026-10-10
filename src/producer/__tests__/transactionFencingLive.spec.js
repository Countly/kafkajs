const { Kafka, logLevel } = require('../../../index')
const { KafkaJSApplicationRecoverableError } = require('../../errors')
const { errorCodeOf } = require('../../../scripts/e2e/idempotentAuthzProbe')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const {
  sleep,
  waitForCondition,
  withTimeout,
} = require('../../consumer/__tests__/fixtures/liveScenario')
const { outcomeOf, settledTransactionVersion } = require('./fixtures/liveTransactions')

// Gated: it fences a live producer. Run alone: `npm run test:e2e:transaction-fencing`.
const describeFencing = process.env.TRANSACTION_FENCING_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'transaction-fencing'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 60000
const HOOK_BUDGET_MS = 60000
// On the transaction version 1 arm the level is waited for, up to 120 s (the shared fixture).
const FEATURE_HOOK_BUDGET_MS = 150000
const DISCONNECT_BUDGET_MS = 15000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const QUIET_MS = 1500 // how long a reader waits for a record that should not exist
// The second producer's InitProducerId is answered CONCURRENT_TRANSACTIONS until the coordinator
// has aborted the first producer's transaction: 12 retries, 50 ms doubling to a 3 s cap, about
// 21 s in all, starting early because the abort usually completes within a second.
const RETRY = { retries: 12, initialRetryTime: 50, multiplier: 2, maxRetryTime: 3000, factor: 0 }
// The client's own budget, for the admin and the reader: 10 retries, 100 ms doubling to a 1 s cap,
// about 7.5 s, whatever the run mode's defaults are.
const CLIENT_RETRY = {
  retries: 10,
  initialRetryTime: 100,
  multiplier: 2,
  maxRetryTime: 1000,
  factor: 0,
}
const INVALID_PRODUCER_EPOCH = 47
const PRODUCER_FENCED = 90

/**
 * The class, category and broker code of a rejection, for the evidence file.
 * @param {Error|null} error
 * @returns {{ name: string, category: string, code: number|null }|null}
 */
const describeError = (error) =>
  error && { name: error.name, category: error.category, code: errorCodeOf(error) }

/**
 * Live proof of producer fencing (KIP-98): a second producer initialized on the same
 * `transactionalId` while the first has a transaction open makes the coordinator abort that
 * transaction and bump the epoch, so the first producer's next request is refused with
 * INVALID_PRODUCER_EPOCH or PRODUCER_FENCED - an application-recoverable error: its transaction
 * calls and `producer.transaction()` reject from then on, and only a new producer carries on.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1050%3A+Consistent+error+handling+for+Transactions
 */
describeFencing('Producer > fencing on one transactional id (live)', () => {
  /** The cluster's finalized transaction version, recorded with the evidence. */
  let cluster
  let kafka
  let clients

  beforeAll(async () => {
    cluster = await settledTransactionVersion()
  }, FEATURE_HOOK_BUDGET_MS)

  beforeEach(() => {
    kafka = new Kafka({
      clientId: `e2e-p7-transaction-fencing-${secureRandom()}`,
      brokers: plainTextBrokers(),
      retry: CLIENT_RETRY,
      logLevel: logLevel.NOTHING,
    })
    clients = []
  })

  afterEach(async () => {
    for (const client of clients.reverse()) {
      await withTimeout(
        client.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }
  }, HOOK_BUDGET_MS)

  /** A connected client of `kafka`, disconnected after the test. */
  const connected = async (client) => {
    clients.push(client)
    await client.connect()
    return client
  }

  /** Every value a `read_committed` reader sees on `topic`, once it has gone quiet. */
  const readCommitted = async (topic, expected) => {
    const reader = await connected(
      kafka.consumer({ groupId: `e2e-p7-transaction-fencing-reader-${secureRandom()}` })
    )
    const values = []
    await reader.subscribe({ topics: [topic], fromBeginning: true })
    await reader.run({ eachMessage: async ({ message }) => values.push(message.value.toString()) })
    await waitForCondition(() => values.length >= expected, WAIT_BUDGET)
    await sleep(QUIET_MS)
    return values.sort()
  }

  test(
    'a producer fenced by a second one on its transactional id is refused from then on, and a new producer carries on',
    async () => {
      const topic = `e2e-p7-transaction-fencing-${secureRandom()}`
      const transactionalId = `e2e-p7-transaction-fencing-${secureRandom()}`
      await createTopic({ topic })
      const producer = () => connected(kafka.producer({ transactionalId, retry: RETRY }))

      const first = await producer()
      const fenced = await first.transaction()
      await fenced.send({ topic, messages: [{ value: 'first-before' }] })
      const second = await producer()
      const fencing = await second.transaction()
      await fencing.send({ topic, messages: [{ value: 'second' }] })
      await fencing.commit()

      const { error: sendError } = await outcomeOf(
        fenced.send({ topic, messages: [{ value: 'first-after' }] })
      )
      const { error: commitError } = await outcomeOf(fenced.commit())
      const { error: abortError } = await outcomeOf(fenced.abort())
      const { error: transactionError } = await outcomeOf(first.transaction())
      const fresh = await producer()
      const carriesOn = await fresh.transaction()
      await carriesOn.send({ topic, messages: [{ value: 'fresh' }] })
      await carriesOn.commit()
      const values = await readCommitted(topic, 2)

      writeEvidence(EVIDENCE, {
        scenario: 'fencing',
        cluster,
        send: describeError(sendError),
        commit: describeError(commitError),
        abort: describeError(abortError),
        transaction: describeError(transactionError),
        values,
      })
      expect(sendError).toEqual(expect.objectContaining({ category: 'applicationRecoverable' }))
      expect([INVALID_PRODUCER_EPOCH, PRODUCER_FENCED]).toContain(errorCodeOf(sendError))
      for (const error of [commitError, abortError, transactionError]) {
        expect(error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
        expect([INVALID_PRODUCER_EPOCH, PRODUCER_FENCED]).toContain(errorCodeOf(error))
      }
      expect(values).toEqual(['fresh', 'second'])
    },
    SCENARIO_BUDGET_MS
  )
})
