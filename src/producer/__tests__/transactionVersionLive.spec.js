const Broker = require('../../broker')
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

// Gated: every arm runs real transactions. Run alone: `npm run test:e2e:transaction-version`, then
// again on the transaction version 1 cluster arm (`COMPOSE_FILE=docker-compose.transaction-v1.yml`).
const describeTransactionVersion =
  process.env.TRANSACTION_VERSION_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'transaction-version'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 120000
const HOOK_BUDGET_MS = 60000
// On the transaction version 1 arm the level is waited for, up to 120 s (the shared fixture).
const FEATURE_HOOK_BUDGET_MS = 150000
const DISCONNECT_BUDGET_MS = 15000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const QUIET_MS = 1500 // how long a reader waits for a record that should not exist
// A cold cluster creates the transaction state topic on the first transactional InitProducerId and
// answers with a retriable error until it is ready: 10 retries, 100 ms doubling to a 1 s cap,
// about 7.5 s in all. Set on the client too, so the admin and consumer waits do not depend on the
// run mode's defaults.
const RETRY = { retries: 10, initialRetryTime: 100, multiplier: 2, maxRetryTime: 1000, factor: 0 }
const TRANSACTIONS = 10
// Short enough for the coordinator to abort the transaction within the scenario: it checks for
// timed-out transactions every 10 s (`transaction.abort.timed.out.transaction.cleanup.interval.ms`).
const SHORT_TRANSACTION_TIMEOUT_MS = 2000
const COORDINATOR_ABORT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const INVALID_TXN_STATE = 48
const PRODUCER_FENCED = 90

/**
 * The class and broker code of a rejection, for the evidence file.
 * @param {Error|null} error
 * @returns {{ name: string, code: number|null }|null}
 */
const describeError = (error) => error && { name: error.name, code: errorCodeOf(error) }

/**
 * Live proof of both transaction protocol versions (KIP-890): under version 2 a transaction sends
 * no AddPartitionsToTxn, produces at v12 and ends with EndTxn v5, whose producer id and epoch the
 * producer adopts before its next transaction; under version 1 it adds partitions with
 * AddPartitionsToTxn v3, produces at v11 and ends with EndTxn v4 under one epoch. Each arm runs only
 * on the cluster whose finalized feature it names, and otherwise records why it did not. Requests
 * are observed on `producer.events.REQUEST` and on `Broker`, because the event carries no body.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 * @see https://github.com/apache/kafka/blob/4.3.1/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala
 */
describeTransactionVersion('Producer > transaction protocol versions (live)', () => {
  /**
   * Every partition of a Produce the broker accepted, with the transaction it belongs to.
   * @type {{ transaction: number, producerEpoch: number, firstSequence: number }[]}
   */
  const produced = []
  /** @type {{ transactionResult: boolean, producerEpoch: number|undefined, error: string|null }[]} */
  const ended = []
  /** The transaction a request belongs to, set by the case before it begins one. */
  let transactionIndex = 0
  /**
   * Every InitProducerId sent, with its answer: a retried attempt is not a second initialization.
   * @type {{ transactionalId: string, producerId: string|number, producerEpoch: number, error: string|null }[]}
   */
  const initializations = []
  let cluster
  let kafka
  let clients

  beforeAll(async () => {
    cluster = await settledTransactionVersion()
    const { produce, endTxn, initProducerId } = Broker.prototype
    jest.spyOn(Broker.prototype, 'produce').mockImplementation(async function (request) {
      const transaction = transactionIndex
      const results = await produce.call(this, request)
      // A partition the coordinator is still completing the last transaction for is answered
      // with a retriable error and sent again: only the partitions the broker accepted count.
      const accepted = (topic, partition) =>
        !Array.isArray(results) ||
        results.some((r) => r.topicName === topic && r.partition === partition && !r.errorCode)
      for (const { topic, partitions } of request.topicData) {
        for (const { partition, firstSequence } of partitions) {
          if (accepted(topic, partition)) {
            produced.push({ transaction, producerEpoch: request.producerEpoch, firstSequence })
          }
        }
      }
      return results
    })
    jest.spyOn(Broker.prototype, 'endTxn').mockImplementation(async function (request) {
      const record = { transactionResult: request.transactionResult, error: null }
      ended.push(record)
      try {
        const response = await endTxn.call(this, request)
        record.producerEpoch = response.producerEpoch
        return response
      } catch (e) {
        record.error = e.type
        throw e
      }
    })
    jest.spyOn(Broker.prototype, 'initProducerId').mockImplementation(async function (request) {
      const { transactionalId, producerId = -1, producerEpoch = -1 } = request
      const record = { transactionalId, producerId, producerEpoch, error: null }
      initializations.push(record)
      try {
        return await initProducerId.call(this, request)
      } catch (e) {
        record.error = e.type
        throw e
      }
    })
  }, FEATURE_HOOK_BUDGET_MS)

  afterAll(() => {
    jest.restoreAllMocks()
  })

  beforeEach(() => {
    produced.length = 0
    ended.length = 0
    initializations.length = 0
    transactionIndex = 0
    kafka = new Kafka({
      clientId: `e2e-p7-transaction-version-${secureRandom()}`,
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

  /** A transactional producer - on a fresh transactional id unless given one - tracing its requests. */
  const transactionalProducer = async ({
    transactionalId = `e2e-p7-transaction-version-${secureRandom()}`,
    ...options
  } = {}) => {
    const producer = await connected(kafka.producer({ transactionalId, retry: RETRY, ...options }))
    const requests = []
    producer.on(producer.events.REQUEST, ({ payload }) =>
      requests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )
    return { producer, requests, transactionalId }
  }

  /** Every value a `read_committed` reader sees on `topic`, once it has gone quiet. */
  const readCommitted = async (topic, expected) => {
    const reader = await connected(
      kafka.consumer({ groupId: `e2e-p7-transaction-version-reader-${secureRandom()}` })
    )
    const values = []
    await reader.subscribe({ topics: [topic], fromBeginning: true })
    await reader.run({ eachMessage: async ({ message }) => values.push(message.value.toString()) })
    await waitForCondition(() => values.length >= expected, WAIT_BUDGET)
    await sleep(QUIET_MS)
    return values.sort()
  }

  /** `count` records spread over the topic's two partitions, values `<prefix>-<n>`. */
  const records = (prefix, count = 4) =>
    Array.from({ length: count }, (_, index) => ({
      partition: index % 2,
      value: `${prefix}-${index}`,
    }))

  /** The EndTxn requests the coordinator accepted, in order. */
  const acceptedEnds = () => ended.filter(({ error }) => error === null)

  /** The one epoch every accepted Produce of transaction `n` carried; undefined if they differ. */
  const epochOf = (n) => {
    const epochs = new Set(
      produced.filter(({ transaction }) => transaction === n).map((p) => p.producerEpoch)
    )
    return epochs.size === 1 ? [...epochs][0] : undefined
  }

  /** The versions one API was sent at, deduplicated and in ascending order. */
  const versionsOf = (requests, apiName) =>
    [...new Set(requests.filter((r) => r.apiName === apiName).map((r) => r.apiVersion))].sort(
      (a, b) => a - b
    )

  describe.each([
    ['version 2', 2],
    ['version 1', 1],
  ])('transaction %s', (arm, armLevel) => {
    /**
     * Whether this arm runs on the cluster under test. When it does not, the case records why and
     * asserts that the cluster really is the other arm, so a skip can never hide a cluster with no
     * feature at all.
     * @param {string} scenario
     * @returns {boolean}
     */
    const runsHere = (scenario) => {
      if (cluster.level === armLevel) {
        return true
      }
      writeEvidence(EVIDENCE, {
        scenario,
        arm,
        skipped: `the cluster finalizes transaction.version ${cluster.level}`,
        cluster,
      })
      expect([1, 2]).toContain(cluster.level)
      return false
    }

    test(
      'a committed and an aborted transaction send the requests of the version, and a read_committed reader sees the committed records only',
      async () => {
        if (!runsHere('requests')) return
        const topic = `e2e-p7-transaction-version-${secureRandom()}`
        await createTopic({ topic, partitions: 2 })
        const { producer, requests } = await transactionalProducer()

        const committed = await producer.transaction()
        await committed.send({ topic, messages: records('committed') })
        await committed.commit()
        const aborted = await producer.transaction()
        await aborted.send({ topic, messages: records('aborted') })
        await aborted.abort()
        const values = await readCommitted(topic, 4)

        const versions = {
          AddPartitionsToTxn: versionsOf(requests, 'AddPartitionsToTxn'),
          Produce: versionsOf(requests, 'Produce'),
          EndTxn: versionsOf(requests, 'EndTxn'),
        }
        writeEvidence(EVIDENCE, { scenario: 'requests', arm, cluster, versions, values })
        expect(values).toEqual(records('committed').map(({ value }) => value))
        expect(acceptedEnds().map(({ transactionResult }) => transactionResult)).toEqual([
          true,
          false,
        ])
        if (armLevel === 2) {
          expect(versions).toEqual({ AddPartitionsToTxn: [], Produce: [12], EndTxn: [5] })
        } else {
          expect(versions).toEqual({ AddPartitionsToTxn: [3], Produce: [11], EndTxn: [4] })
        }
      },
      SCENARIO_BUDGET_MS
    )

    test(
      'ten transactions in a row: version 2 adopts the epoch each EndTxn returns and restarts sequences at 0, version 1 keeps one epoch and carries the sequences on',
      async () => {
        if (!runsHere('epochs')) return
        const topic = `e2e-p7-transaction-version-epochs-${secureRandom()}`
        await createTopic({ topic, partitions: 2 })
        const { producer } = await transactionalProducer()

        for (let n = 0; n < TRANSACTIONS; n++) {
          transactionIndex = n
          const transaction = await producer.transaction()
          await transaction.send({ topic, messages: records(`transaction-${n}`, 2) })
          await transaction.commit()
        }

        const produceEpochs = Array.from({ length: TRANSACTIONS }, (_, n) => epochOf(n))
        const endTxnEpochs = acceptedEnds().map(({ producerEpoch }) => producerEpoch)
        writeEvidence(EVIDENCE, { scenario: 'epochs', arm, cluster, produceEpochs, endTxnEpochs })
        expect(endTxnEpochs).toHaveLength(TRANSACTIONS)
        expect(produceEpochs).not.toContain(undefined)
        // One record per partition per transaction: under version 2 every transaction starts its
        // partitions at sequence 0, under version 1 the sequence carries on from the last one.
        for (let n = 0; n < TRANSACTIONS; n++) {
          const sequences = produced
            .filter(({ transaction }) => transaction === n)
            .map(({ firstSequence }) => firstSequence)
          expect(sequences).toEqual([armLevel === 2 ? 0 : n, armLevel === 2 ? 0 : n])
        }
        for (let n = 1; n < TRANSACTIONS; n++) {
          if (armLevel === 2) {
            expect(produceEpochs[n]).toBe(endTxnEpochs[n - 1])
            expect(produceEpochs[n]).not.toBe(produceEpochs[n - 1])
          } else {
            expect(produceEpochs[n]).toBe(produceEpochs[0])
          }
        }
      },
      SCENARIO_BUDGET_MS
    )

    test(
      'a transaction the coordinator aborted on its timeout: version 2 aborts and carries on, version 1 is fenced, and a commit of one is refused under both',
      async () => {
        if (!runsHere('transaction-timeout')) return
        const topic = `e2e-p7-transaction-version-timeout-${secureRandom()}`
        await createTopic({ topic })
        const options = { transactionTimeout: SHORT_TRANSACTION_TIMEOUT_MS }
        const { producer, transactionalId } = await transactionalProducer(options)
        const admin = await connected(kafka.admin())

        /**
         * Sends one record in `transaction` and waits for the coordinator to abort it on its
         * timeout: the abort marker moves the read_committed end offset, which ListOffsets
         * answers by default, past the record.
         */
        const timeOut = async (transaction, value) => {
          const [{ baseOffset }] = await transaction.send({ topic, messages: [{ value }] })
          return waitForCondition(async () => {
            const [{ high }] = await admin.fetchTopicOffsets(topic)
            return Number(high) > Number(baseOffset)
          }, COORDINATOR_ABORT_BUDGET)
        }

        const first = await producer.transaction()
        const firstTimedOut = await timeOut(first, 'timed-out-then-aborted')
        const abort = await outcomeOf(first.abort())
        let fencedTransaction = null
        let owner = producer
        if (armLevel === 1) {
          fencedTransaction = await outcomeOf(producer.transaction())
          owner = (await transactionalProducer({ ...options, transactionalId })).producer
        }
        transactionIndex = 1
        const next = await owner.transaction()
        await next.send({ topic, messages: [{ value: 'next' }] })
        await next.commit()
        transactionIndex = 2
        const third = await owner.transaction()
        const thirdTimedOut = await timeOut(third, 'timed-out-then-committed')
        const commit = await outcomeOf(third.commit())
        const values = await readCommitted(topic, 1)
        const abortEnds = ended.filter(({ transactionResult }) => !transactionResult)
        const transactionalInitializations = initializations.filter(
          (request) => request.transactionalId === transactionalId
        )

        writeEvidence(EVIDENCE, {
          scenario: 'transaction-timeout',
          arm,
          cluster,
          firstTimedOut,
          thirdTimedOut,
          abort: describeError(abort.error),
          fencedTransaction: fencedTransaction && describeError(fencedTransaction.error),
          commit: describeError(commit.error),
          ended,
          transactionalInitializations,
          values,
        })
        expect(firstTimedOut).toBe(true)
        expect(thirdTimedOut).toBe(true)
        expect(values).toEqual(['next'])
        expect(commit.error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
        if (armLevel === 2) {
          expect(abort.error).toBeNull()
          expect(abortEnds.at(-1).error).toBeNull()
          expect(abortEnds.at(-1).producerEpoch).toBeGreaterThan(epochOf(0))
          expect(epochOf(1)).toBe(abortEnds.at(-1).producerEpoch)
          expect(transactionalInitializations.filter(({ error }) => error === null)).toHaveLength(1)
          // A re-initialization sends the held producer id: none happened, accepted or refused.
          expect(
            transactionalInitializations.filter(({ producerId }) => String(producerId) !== '-1')
          ).toEqual([])
          expect([INVALID_TXN_STATE, PRODUCER_FENCED]).toContain(errorCodeOf(commit.error))
        } else {
          expect(abort.error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
          // The coordinator answers a client EndTxn at a fenced epoch with PRODUCER_FENCED only.
          expect(errorCodeOf(abort.error)).toBe(PRODUCER_FENCED)
          expect(fencedTransaction.error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
          expect(errorCodeOf(commit.error)).toBe(PRODUCER_FENCED)
        }
      },
      SCENARIO_BUDGET_MS
    )
  })
})
