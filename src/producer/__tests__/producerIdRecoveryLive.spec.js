const Broker = require('../../broker')
const { Kafka, logLevel } = require('../../../index')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it deletes records under a live producer. Run alone: `npm run test:e2e:producer-id-recovery`.
const describeRecovery = process.env.PRODUCER_ID_RECOVERY_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'producer-id-recovery'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 120000
const HOOK_BUDGET_MS = 60000
const DISCONNECT_BUDGET_MS = 15000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const RECORDS = 5
// A cold cluster creates the transaction state topic on the first transactional InitProducerId and
// answers with a retriable error until it is ready: 10 retries, 100 ms doubling to a 1 s cap, about
// 7.5 s in all.
const RETRY = { retries: 10, initialRetryTime: 100, multiplier: 2, maxRetryTime: 1000 }

/**
 * Live check of the producer-id recovery rules (KIP-360) against what a Kafka 4.x broker does when
 * the log start offset moves past every record a producer wrote. `DeleteRecords` moves it through
 * `UnifiedLog.maybeIncrementLogStartOffset`, whose `ProducerStateManager.onLogStartOffsetIncremented`
 * removes no producer, and a broker answers a producer it holds no state for by accepting any
 * sequence (`ProducerAppendInfo.checkSequence`) - never with UNKNOWN_PRODUCER_ID. So the producer
 * must go on exactly as before: no error, no new producer id, no epoch change of its own, its
 * sequence continuing; a transaction commits. The recovery branches themselves are pinned by the
 * producer state manager's unit cases.
 * @see https://github.com/apache/kafka/blob/4.3/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java
 * @see https://github.com/apache/kafka/blob/4.3/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java
 * @see https://github.com/apache/kafka/blob/4.3/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer
 */
describeRecovery('Producer > producer-id recovery after the log start moves (live)', () => {
  /** @type {{ producerId: string, producerEpoch: number, firstSequence: number, logStartOffset: string|undefined }[]} */
  const produced = []
  /** @type {{ transactionalId: string|null, producerId: number, producerEpoch: number, succeeded: boolean }[]} every InitProducerId sent, with whether it was answered */
  const initializations = []
  let kafka
  let clients

  beforeAll(() => {
    const { produce, initProducerId } = Broker.prototype
    jest.spyOn(Broker.prototype, 'produce').mockImplementation(async function (request) {
      const results = await produce.call(this, request)
      for (const { partitions } of request.topicData) {
        for (const { firstSequence } of partitions) {
          produced.push({
            producerId: String(request.producerId),
            producerEpoch: request.producerEpoch,
            firstSequence,
            logStartOffset: Array.isArray(results) ? results[0].logStartOffset : undefined,
          })
        }
      }
      return results
    })
    jest.spyOn(Broker.prototype, 'initProducerId').mockImplementation(async function (request) {
      const attempt = { ...request, succeeded: false }
      initializations.push(attempt)
      const response = await initProducerId.call(this, request)
      attempt.succeeded = true
      return response
    })
  })

  afterAll(() => {
    jest.restoreAllMocks()
  })

  beforeEach(() => {
    produced.length = 0
    initializations.length = 0
    kafka = new Kafka({
      clientId: `e2e-p7-producer-id-recovery-${secureRandom()}`,
      brokers: plainTextBrokers(),
      retry: RETRY,
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

  /** `RECORDS` records with values `<prefix>-<n>`. */
  const records = (prefix) =>
    Array.from({ length: RECORDS }, (_, index) => ({ value: `${prefix}-${index}` }))

  /**
   * What the producer's InitProducerId requests amount to: how many were answered, and how many
   * carried a producer id (a re-initialization of one it already holds).
   * @param {(request: object) => boolean} [belongsTo] narrows to one producer's requests
   */
  const initializationSummary = (belongsTo = () => true) => {
    const mine = initializations.filter(belongsTo)
    return {
      attempts: mine.length,
      answered: mine.filter(({ succeeded }) => succeeded).length,
      withProducerId: mine.filter(({ producerId }) => String(producerId) !== '-1').length,
    }
  }

  /** Moves the topic's log start past everything written so far. */
  const deleteEverything = async (topic) => {
    const admin = await connected(kafka.admin())
    // deleteTopicRecords finds leaders in the metadata the client already holds.
    await admin.fetchTopicOffsets(topic)
    await admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: '-1' }] })
    const [{ low }] = await admin.fetchTopicOffsets(topic)
    return low
  }

  /** Every value a `read_committed` reader sees on `topic`, once `expected` have arrived. */
  const readCommitted = async (topic, expected) => {
    const reader = await connected(
      kafka.consumer({ groupId: `e2e-p7-producer-id-recovery-reader-${secureRandom()}` })
    )
    const values = []
    await reader.subscribe({ topics: [topic], fromBeginning: true })
    await reader.run({ eachMessage: async ({ message }) => values.push(message.value.toString()) })
    await waitForCondition(() => values.length >= expected, WAIT_BUDGET)
    return values
  }

  test(
    'an idempotent producer keeps its producer id, epoch and sequence across the deletion',
    async () => {
      const topic = `e2e-p7-producer-id-recovery-idempotent-${secureRandom()}`
      await createTopic({ topic })
      const producer = await connected(kafka.producer())

      await producer.send({ topic, messages: records('before') })
      const low = await deleteEverything(topic)
      const [after] = await producer.send({ topic, messages: records('after') })
      const values = await readCommitted(topic, RECORDS)

      writeEvidence(EVIDENCE, {
        scenario: 'idempotent',
        low,
        produced,
        initializations: initializationSummary(),
        values,
      })
      expect(low).toEqual(String(RECORDS))
      expect(after.logStartOffset).toEqual(String(RECORDS))
      expect(initializationSummary()).toEqual(
        expect.objectContaining({ answered: 1, withProducerId: 0 })
      )
      expect(produced).toHaveLength(2)
      expect(produced[1]).toEqual(
        expect.objectContaining({
          producerId: produced[0].producerId,
          producerEpoch: produced[0].producerEpoch,
          firstSequence: RECORDS,
        })
      )
      expect(values).toEqual(records('after').map(({ value }) => value))
    },
    SCENARIO_BUDGET_MS
  )

  test(
    'a transactional producer commits its next transaction with no re-initialization',
    async () => {
      const topic = `e2e-p7-producer-id-recovery-transactional-${secureRandom()}`
      const transactionalId = `e2e-p7-producer-id-recovery-${secureRandom()}`
      await createTopic({ topic })
      const producer = await connected(kafka.producer({ transactionalId }))
      const ofThisProducer = (request) => request.transactionalId === transactionalId

      const first = await producer.transaction()
      await first.send({ topic, messages: records('before') })
      await first.commit()
      const low = await deleteEverything(topic)
      const second = await producer.transaction()
      await second.send({ topic, messages: records('after') })
      await second.commit()
      const values = await readCommitted(topic, RECORDS)

      writeEvidence(EVIDENCE, {
        scenario: 'transactional',
        low,
        epochStep: produced[1]?.producerEpoch - produced[0]?.producerEpoch,
        produced,
        initializations: initializationSummary(ofThisProducer),
        values,
      })
      expect(Number(low)).toBeGreaterThanOrEqual(RECORDS)
      expect(initializationSummary(ofThisProducer)).toEqual(
        expect.objectContaining({ answered: 1, withProducerId: 0 })
      )
      expect(produced).toHaveLength(2)
      // Transaction version 2 moves the producer one epoch on at every commit, version 1 keeps it:
      // any further step would be a re-initialization of its own.
      expect(produced[1].producerId).toEqual(produced[0].producerId)
      expect([0, 1]).toContain(produced[1].producerEpoch - produced[0].producerEpoch)
      expect(values).toEqual(records('after').map(({ value }) => value))
    },
    SCENARIO_BUDGET_MS
  )
})
