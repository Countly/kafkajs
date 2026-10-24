const Broker = require('../../broker')
const Cluster = require('../../cluster')
const { Kafka, logLevel } = require('../../../index')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProducerClosedError,
  KafkaJSProtocolError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const apiKeys = require('../../protocol/requests/apiKeys')
const ProducerStateManager = require('../state')
const TransactionOffsets = require('../state/transactionOffsets')
const { errorCodeOf } = require('../../../scripts/e2e/idempotentAuthzProbe')
const { readLog } = require('../../../testHelpers/rawLog')
const {
  createConnectionPool,
  createTopic,
  newLogger,
  plainTextBrokers,
  secureRandom,
  startBroker,
  stopBroker,
  wireVersionsOf,
  writeEvidence,
} = require('testHelpers')
const { sleep, waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const {
  crashAfterProduce,
  outcomeOf,
  settledTransactionVersion,
} = require('./fixtures/liveTransactions')
const { ResponseDrop, createLiveSockets, repliesOf } = require('./fixtures/liveWire')
const {
  COORDINATOR_TYPES,
  CleanupStack,
  ProducerProbe,
  coordinatorOf,
  describeError,
  readData,
} = require('./fixtures/liveProbe')
const {
  CASE_JOIN_MS,
  NEVER,
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
} = require('./fixtures/bounded')

// Gated: it kills a producer process and stops a broker. Run alone:
// `npm run test:e2e:transaction-restart`.
const describeRestart = process.env.TRANSACTION_RESTART_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'transaction-restart'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 120000
// The afterEach's budget; its teardown steps (three brokers back, the admin's disconnect, each
// client's) share what Jest leaves after the join reserve.
const HOOK_BUDGET_MS = 120000
const TEARDOWN_BUDGET_MS = HOOK_BUDGET_MS - CASE_JOIN_MS
// On the transaction version 1 arm the level is waited for, up to 120 s (the shared fixture).
const FEATURE_HOOK_BUDGET_MS = 150000
const DISCONNECT_BUDGET_MS = 15000
const CHILD_BUDGET_MS = 60000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
// A stopped broker leaves the cluster's metadata within seconds; a restarted one rejoins as fast.
const BROKER_BUDGET = { attempts: 80, delay: 250 } // 20 s
// Each attempt can itself take 10 s: `admin.fetchTopicMetadata` waits that long (its `waitFor`
// default) for a partition leader before it gives up. Three attempts, at most about 31 s.
const LEADER_BUDGET = { attempts: 3, delay: 250 }
const QUIET_MS = 1500 // how long a reader waits for a record that should not exist
const RECORDS = 5
// A cold cluster answers the first transactional InitProducerId with a retriable error until the
// transaction state topic is ready: 10 retries, 100 ms doubling to a 1 s cap, about 7.5 s in all.
const RETRY = { retries: 10, initialRetryTime: 100, multiplier: 2, maxRetryTime: 1000, factor: 0 }
// A takeover's InitProducerId is answered CONCURRENT_TRANSACTIONS until the coordinator has
// aborted the transaction left open: 12 retries, 50 ms doubling to a 3 s cap, about 21 s.
const TAKEOVER_RETRY = {
  retries: 12,
  initialRetryTime: 50,
  multiplier: 2,
  maxRetryTime: 3000,
  factor: 0,
}
// The production retry defaults (src/retry/defaults.js), stated in full so the run mode cannot
// change them: about 9.3 s of backoff.
const PRODUCTION_RETRY = {
  retries: 5,
  initialRetryTime: 300,
  multiplier: 2,
  maxRetryTime: 30000,
  factor: 0.2,
}
// Short enough for the coordinator to abort an open transaction within the scenario: it checks for
// timed-out transactions every 10 s (`transaction.abort.timed.out.transaction.cleanup.interval.ms`).
const SHORT_TRANSACTION_TIMEOUT_MS = 2000
const COORDINATOR_ABORT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const REQUEST_TIMED_OUT = 7
const INVALID_TXN_STATE = 48
const PRODUCER_FENCED = 90
// The broker the abortable-send scenario stops, and its node id; kafka1 stays up as the bootstrap.
const STOPPED_SERVICE = 'kafka3'
const STOPPED_NODE = 3

/**
 * Live proof of transaction recovery: a producer killed between a resolved Produce and its EndTxn
 * leaves a transaction the next owner's InitProducerId aborts, so its records never become
 * visible; a send that fails for good inside a transaction makes it abort-only and the same
 * producer carries on after `abort()`; a commit whose EndTxn answers were lost is pending, refuses
 * `abort()`, and completes when it is sent again, and one that never reached the coordinator before
 * it aborted the transaction on its timeout is refused when it is sent again; and a takeover of an
 * abandoned transaction under the production retry defaults, against both a short and the default
 * transaction timeout. The coordinator writes its own state with the transaction's timeout as the
 * write timeout, so a very short one can leave an abort pending.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
 * @see https://github.com/apache/kafka/blob/4.3.1/core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala
 * @see https://github.com/apache/kafka/blob/4.3/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala
 * @see https://issues.apache.org/jira/browse/KAFKA-20785
 * @see https://issues.apache.org/jira/browse/KAFKA-16047
 */
describeRestart('Producer > transaction recovery (live)', () => {
  /** @type {{ transactionalId: string, error: string|null }[]} every InitProducerId answered */
  const initializations = []
  /**
   * @type {{ transactionalId: string, transactionResult: boolean, producerEpoch: number,
   *   answeredEpoch?: number }[]} every EndTxn sent, and the epoch its answer carried
   */
  const ends = []
  /** When set, an EndTxn commit that the coordinator accepted is reported as timed out instead. */
  let loseCommitAnswers = false
  /** When set, an EndTxn commit is reported as timed out without reaching the coordinator. */
  let dropCommits = false
  /** The cluster's finalized transaction version, recorded with the evidence. */
  let cluster
  let kafka
  let clients
  let child

  beforeAll(async () => {
    return bounded(
      NEVER,
      FEATURE_HOOK_BUDGET_MS - CASE_JOIN_MS,
      async () => {
        cluster = await settledTransactionVersion()
        const { initProducerId, endTxn } = Broker.prototype
        jest.spyOn(Broker.prototype, 'initProducerId').mockImplementation(async function (request) {
          const record = { transactionalId: request.transactionalId, error: null }
          initializations.push(record)
          try {
            return await initProducerId.call(this, request)
          } catch (e) {
            record.error = e.type
            throw e
          }
        })
        jest.spyOn(Broker.prototype, 'endTxn').mockImplementation(async function (request) {
          const { transactionalId, transactionResult, producerEpoch } = request
          const record = { transactionalId, transactionResult, producerEpoch }
          ends.push(record)
          if (dropCommits && transactionResult) {
            throw new KafkaJSProtocolError(createErrorFromCode(REQUEST_TIMED_OUT))
          }
          const response = await endTxn.call(this, request)
          record.answeredEpoch = response.producerEpoch
          if (loseCommitAnswers && transactionResult) {
            throw new KafkaJSProtocolError(createErrorFromCode(REQUEST_TIMED_OUT))
          }
          return response
        })
      },
      { label: 'The setup' }
    )
  }, FEATURE_HOOK_BUDGET_MS)

  afterAll(() => {
    jest.restoreAllMocks()
  })

  beforeEach(() => {
    initializations.length = 0
    ends.length = 0
    loseCommitAnswers = false
    dropCommits = false
    child = null
    kafka = new Kafka({
      clientId: `e2e-p7-transaction-restart-${secureRandom()}`,
      brokers: plainTextBrokers(),
      retry: RETRY,
      logLevel: logLevel.NOTHING,
    })
    clients = []
  })

  // The broker comes back first, and the case waits for it: every client below may hold requests to
  // it that only fail on their own timeout, and a stopped broker must never outlive the case or
  // reach the next one. Only a case the gate admitted gets this: a refused one started nothing.
  // Every step is bounded and a step that fails or runs out of time fails the hook and taints the
  // run, so a teardown cannot pass while a disconnect may still be running.
  afterEach(async () => {
    const admitted = liveRun.takeAdmission()
    if (
      !admitted ||
      expect
        .getState()
        .currentTestName.endsWith(
          ' forced close preserves an unknown commit without local publication'
        )
    ) {
      return
    }
    try {
      startBroker(STOPPED_SERVICE)
    } catch (e) {
      // Already running, or compose is down: the readiness step below is the verdict.
    }
    child?.kill('SIGKILL')
    const admin = kafka.admin()
    const teardown = new CleanupStack()
    for (const client of clients) teardown.add('client disconnect', () => client.disconnect())
    teardown.add('admin disconnect', () => admin.disconnect())
    teardown.add('three brokers back', async (signal) => {
      await admin.connect()
      const restored = await waitForCondition(async () => {
        signal.throwIfAborted()
        return (await brokersUp(admin)) === 3
      }, BROKER_BUDGET)
      if (restored) return
      // Recorded, so a next case failing on two brokers is traced back here.
      writeEvidence(EVIDENCE, {
        scenario: 'teardown',
        test: expect.getState().currentTestName,
        threeBrokers: false,
      })
      throw new Error('The cluster did not return to three brokers')
    })
    await teardown.run(async () => {}, { budgetMs: TEARDOWN_BUDGET_MS })
  }, HOOK_BUDGET_MS)

  /** A connected client of `kafka`, disconnected after the test. */
  const connected = async (client) => {
    clients.push(client)
    await client.connect()
    return client
  }

  /** How many brokers `admin` sees in the cluster's metadata; 0 while it cannot be read. */
  const brokersUp = async (admin) => {
    try {
      return (await admin.describeCluster()).brokers.length
    } catch (e) {
      return 0
    }
  }

  /**
   * How many brokers `broker` names in a metadata answer for `topics` alone; 0 while it cannot be
   * read. Used while a partition has no leader: an answer naming its topic fails as a whole with
   * LEADER_NOT_AVAILABLE.
   * @param {Broker} broker
   * @param {string[]} topics
   * @returns {Promise<number>}
   */
  const brokersServing = async (broker, topics) => {
    try {
      return (await broker.metadata(topics)).brokers.length
    } catch (e) {
      return 0
    }
  }

  /** Every value a `read_committed` reader sees on `topic`, once it has gone quiet. */
  const readCommitted = async (topic, expected) => {
    const reader = await connected(
      kafka.consumer({ groupId: `e2e-p7-transaction-restart-reader-${secureRandom()}` })
    )
    const values = []
    await reader.subscribe({ topics: [topic], fromBeginning: true })
    await reader.run({ eachMessage: async ({ message }) => values.push(message.value.toString()) })
    await waitForCondition(() => values.length >= expected, WAIT_BUDGET)
    await sleep(QUIET_MS)
    return values.sort()
  }

  /** `count` records with values `<prefix>-<n>`. */
  const records = (prefix, count = RECORDS) =>
    Array.from({ length: count }, (_, index) => ({ value: `${prefix}-${index}` }))

  /** The values `records(prefix, count)` writes, sorted as `readCommitted` returns them. */
  const valuesOf = (prefix, count = RECORDS) =>
    records(prefix, count)
      .map(({ value }) => value)
      .sort()

  test(
    'a producer killed between Produce and EndTxn leaves records no read_committed reader sees, and the next owner of its transactional id commits',
    liveCase(SCENARIO_BUDGET_MS, async () => {
      const topic = `e2e-p7-transaction-restart-crash-${secureRandom()}`
      const transactionalId = `e2e-p7-transaction-restart-${secureRandom()}`
      await createTopic({ topic })

      const crash = await crashAfterProduce({
        topic,
        transactionalId,
        records: RECORDS,
        deadlineMs: CHILD_BUDGET_MS,
        track: (spawned) => (child = spawned),
      })
      const owner = await connected(kafka.producer({ transactionalId, retry: TAKEOVER_RETRY }))
      const transaction = await owner.transaction()
      await transaction.send({ topic, messages: records('next') })
      await transaction.commit()
      const values = await readCommitted(topic, RECORDS)

      const takeover = initializations.filter((r) => r.transactionalId === transactionalId)
      writeEvidence(EVIDENCE, { scenario: 'crash', cluster, crash, takeover, values })
      expect(crash.requests).toContain('Produce')
      expect(crash.requests).not.toContain('EndTxn')
      expect(takeover.at(-1).error).toBeNull()
      expect(values).toEqual(valuesOf('next'))
    }),
    caseTimeout(SCENARIO_BUDGET_MS)
  )

  // Worst case, inside its 120 s budget: 20 s for the broker to leave, 7.5 s of send retries, 31 s
  // for its leader to return, 7.5 s for the abort and 30 s to read back.
  test(
    'a send to a partition with no leader makes the transaction abort-only: commit() rejects without EndTxn, abort() resolves, and the same producer commits the next one',
    liveCase(SCENARIO_BUDGET_MS, async () => {
      const healthy = `e2e-p7-transaction-restart-healthy-${secureRandom()}`
      const leaderless = `e2e-p7-transaction-restart-leaderless-${secureRandom()}`
      const admin = await connected(kafka.admin())
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          { topic: healthy, replicaAssignment: [{ partition: 0, replicas: [1, 2] }] },
          { topic: leaderless, replicaAssignment: [{ partition: 0, replicas: [STOPPED_NODE] }] },
        ],
      })
      /**
       * The partition's leader; null while it has none, which makes a metadata request for the
       * topic fail with LEADER_NOT_AVAILABLE once the admin client's own 10 s wait runs out.
       */
      const leaderOf = async (topic) => {
        try {
          const {
            topics: [{ partitions }],
          } = await admin.fetchTopicMetadata({ topics: [topic] })
          return partitions[0].leader
        } catch (e) {
          return null
        }
      }
      const producer = await connected(
        kafka.producer({
          transactionalId: `e2e-p7-transaction-restart-${secureRandom()}`,
          retry: RETRY,
        })
      )
      // kafka1, which stays up: asked about the healthy topic only, it still answers once the
      // leaderless partition has no leader.
      const bootstrap = await connected(
        new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
      )

      const transaction = await producer.transaction()
      await transaction.send({ topic: healthy, messages: records('aborted') })
      stopBroker(STOPPED_SERVICE)
      const brokerGone = await waitForCondition(
        async () => (await brokersServing(bootstrap, [healthy])) === 2,
        BROKER_BUDGET
      )
      const send = await outcomeOf(
        transaction.send({ topic: leaderless, messages: records('leaderless', 1) })
      )
      const endsBeforeCommit = ends.length
      const commit = await outcomeOf(transaction.commit())
      const endsAtCommit = ends.length - endsBeforeCommit
      // The leaderless topic stays among the producer's target topics, and a metadata answer
      // naming a topic with a leaderless partition fails as a whole: until the partition has a
      // leader again no metadata refresh succeeds, the abort's and the next transaction's included.
      startBroker(STOPPED_SERVICE)
      const leaderBack = await waitForCondition(
        async () => (await leaderOf(leaderless)) === STOPPED_NODE,
        LEADER_BUDGET
      )
      const abort = await outcomeOf(transaction.abort())
      const next = await producer.transaction()
      await next.send({ topic: healthy, messages: records('next') })
      await next.commit()
      const values = await readCommitted(healthy, RECORDS)

      writeEvidence(EVIDENCE, {
        scenario: 'abortable-send',
        cluster,
        brokerGone,
        leaderBack,
        send: send.error && { name: send.error.name, cause: send.error.cause?.name },
        commit: commit.error && commit.error.name,
        endsAtCommit,
        abort: abort.error && abort.error.name,
        values,
      })
      expect(brokerGone).toBe(true)
      expect(send.error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(commit.error).toBeInstanceOf(KafkaJSTransactionAbortableError)
      expect(endsAtCommit).toBe(0)
      expect(leaderBack).toBe(true)
      expect(abort.error).toBeNull()
      expect(values).toEqual(valuesOf('next'))
    }),
    caseTimeout(SCENARIO_BUDGET_MS)
  )

  describe.each([
    ['tv2', 2],
    ['tv1', 1],
  ])('a commit whose outcome is unknown, under transaction version %s', (arm, level) => {
    /** Whether the cluster finalizes this arm's version; when not, records why the case did not run. */
    const runsHere = (scenario) => {
      if (cluster.level === level) {
        return true
      }
      writeEvidence(EVIDENCE, {
        scenario,
        arm,
        skipped: `the cluster finalizes transaction.version ${cluster.level}`,
      })
      return false
    }

    test(
      'with the EndTxn answers lost: commit() rejects retriable, abort() is refused, and commit() sent again completes it once',
      liveCase(SCENARIO_BUDGET_MS, async () => {
        if (!runsHere('pending-commit')) return
        const topic = `e2e-p8-pending-commit-${secureRandom()}`
        const transactionalId = `e2e-p8-pending-commit-${secureRandom()}`
        await createTopic({ topic })
        const producer = await connected(kafka.producer({ transactionalId, retry: RETRY }))
        const endsOf = () => ends.filter((end) => end.transactionalId === transactionalId)

        const transaction = await producer.transaction()
        await transaction.send({ topic, messages: records('committed') })
        loseCommitAnswers = true
        const commit = await outcomeOf(transaction.commit())
        const abort = await outcomeOf(transaction.abort())
        loseCommitAnswers = false
        const retried = await outcomeOf(transaction.commit())
        const [resent] = endsOf().slice(-1)
        const next = await producer.transaction()
        await next.send({ topic, messages: records('next', 1) })
        await next.commit()
        const [nextEnd] = endsOf().slice(-1)
        const values = await readCommitted(topic, RECORDS + 1)

        writeEvidence(EVIDENCE, {
          scenario: 'pending-commit',
          arm,
          cluster,
          commit: commit.error && {
            name: commit.error.name,
            retriable: commit.error.retriable,
            code: errorCodeOf(commit.error),
          },
          abort: abort.error && { name: abort.error.name, message: abort.error.message },
          retried: retried.error && retried.error.name,
          ends: endsOf(),
          values,
        })
        expect(commit.error.retriable).toBe(true)
        expect(commit.error.type).toBe('REQUEST_TIMED_OUT')
        expect(abort.error).toBeInstanceOf(KafkaJSNonRetriableError)
        expect(abort.error.message).toMatch(/commit\(\) did not complete; call commit\(\) again/)
        expect(retried.error).toBeNull()
        expect(endsOf().every(({ transactionResult }) => transactionResult)).toBe(true)
        expect(values).toEqual([...valuesOf('committed'), ...valuesOf('next', 1)].sort())
        expect(nextEnd.producerEpoch).toBe(
          level === 2 ? resent.answeredEpoch : resent.producerEpoch
        )
      }),
      caseTimeout(SCENARIO_BUDGET_MS)
    )

    test(
      'with the EndTxn requests lost until the coordinator aborted the transaction on its timeout: commit() sent again is refused with 48 or 90, depending on the transaction version and on whether the broker has KAFKA-20785 (fixed in 4.3.2 and 4.5.0), and the producer is unusable',
      liveCase(SCENARIO_BUDGET_MS, async () => {
        if (!runsHere('pending-commit-timed-out')) return
        const topic = `e2e-p8-pending-commit-${secureRandom()}`
        await createTopic({ topic })
        const admin = await connected(kafka.admin())
        const producer = await connected(
          kafka.producer({
            transactionalId: `e2e-p8-pending-commit-${secureRandom()}`,
            transactionTimeout: SHORT_TRANSACTION_TIMEOUT_MS,
            retry: RETRY,
          })
        )

        const transaction = await producer.transaction()
        const [{ baseOffset }] = await transaction.send({ topic, messages: records('expired', 1) })
        dropCommits = true
        const commit = await outcomeOf(transaction.commit())
        // The abort marker moves the end offset ListOffsets reads under read_committed past the record.
        const timedOut = await waitForCondition(async () => {
          const [{ high }] = await admin.fetchTopicOffsets(topic)
          return Number(high) > Number(baseOffset)
        }, COORDINATOR_ABORT_BUDGET)
        dropCommits = false
        const retried = await outcomeOf(transaction.commit())
        const next = await outcomeOf(producer.transaction())

        writeEvidence(EVIDENCE, {
          scenario: 'pending-commit-timed-out',
          arm,
          cluster,
          commit: commit.error && { name: commit.error.name, retriable: commit.error.retriable },
          timedOut,
          retried: retried.error && {
            name: retried.error.name,
            code: errorCodeOf(retried.error),
          },
        })
        expect(commit.error.retriable).toBe(true)
        expect(timedOut).toBe(true)
        expect(retried.error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
        // Version 1 checks the epoch first, which the coordinator's abort bumped: 90. Version 2
        // answers 48, or 90 on a broker with KAFKA-20785.
        expect(level === 1 ? [PRODUCER_FENCED] : [INVALID_TXN_STATE, PRODUCER_FENCED]).toContain(
          errorCodeOf(retried.error)
        )
        expect(next.error).toBe(retried.error)
      }),
      caseTimeout(SCENARIO_BUDGET_MS)
    )
  })

  describe.each([
    ['100 ms', 100],
    ['the default 60 s', undefined],
  ])(
    'a takeover on the production retry defaults, the abandoned transaction timing out after %s',
    (label, transactionTimeout) => {
      test(
        'the new owner initializes within its retries or reports them exhausted, never hangs',
        liveCase(SCENARIO_BUDGET_MS, async () => {
          const topic = `e2e-p7-transaction-restart-takeover-${secureRandom()}`
          const transactionalId = `e2e-p7-transaction-restart-${secureRandom()}`
          await createTopic({ topic })
          const abandoned = await connected(
            kafka.producer({ transactionalId, transactionTimeout, retry: RETRY })
          )
          const open = await abandoned.transaction()
          await open.send({ topic, messages: records('abandoned') })

          const owner = await connected(
            kafka.producer({ transactionalId, transactionTimeout, retry: PRODUCTION_RETRY })
          )
          const before = initializations.length
          const started = Date.now()
          const takeover = await outcomeOf(owner.transaction())
          const elapsedMs = Date.now() - started

          const attempts = initializations
            .slice(before)
            .filter((r) => r.transactionalId === transactionalId)
          writeEvidence(EVIDENCE, {
            scenario: 'takeover',
            cluster,
            transactionTimeout: label,
            elapsedMs,
            attempts,
            outcome: takeover.error
              ? { name: takeover.error.name, code: errorCodeOf(takeover.error) }
              : 'initialized',
          })
          if (transactionTimeout === undefined) {
            expect(takeover.error).toBeNull()
          } else if (takeover.error !== null) {
            expect(takeover.error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
          }
          expect(attempts.at(-1).error === null).toBe(takeover.error === null)
        }),
        caseTimeout(SCENARIO_BUDGET_MS)
      )
    }
  )

  // The commit's answer is dropped on the coordinator's connection after the broker applied it, and
  // the close is forced at 1 s, so the outcome of the commit stays unknown to this producer while the
  // broker has committed: the two truths are read apart. Replay sets: the physical log holds the
  // transaction's two records and their commit marker; the committed offset is the broker's 2 while
  // local publication stays 0; handler delivery does not apply.
  test(
    'forced close preserves an unknown commit without local publication',
    liveCase(SCENARIO_BUDGET_MS, async (scope) => {
      const id = secureRandom()
      const topic = `e2e-p9b-transaction-close-${id}`
      const inputTopic = `e2e-p9b-transaction-close-input-${id}`
      const groupId = `e2e-p9b-transaction-close-group-${id}`
      const transactionalId = `e2e-p9b-transaction-close-${id}`
      const clientId = `e2e-p9b-transaction-close-${id}`
      const brokers = plainTextBrokers()
      const sockets = createLiveSockets()
      const cleanup = new CleanupStack()
      const requests = []
      const stateManagers = new Set()
      const releases = []
      const publications = []
      const localWrites = []
      let capturing = false
      let offsets = null

      await cleanup.run(async () => {
        const { enqueue } = ProducerStateManager.prototype
        const { release, publish } = TransactionOffsets.prototype
        const { markOffsetAsCommitted } = Cluster.prototype
        const spies = [
          jest
            .spyOn(ProducerStateManager.prototype, 'enqueue')
            .mockImplementation(function (batches) {
              if (capturing) stateManagers.add(this)
              return enqueue.call(this, batches)
            }),
          jest.spyOn(TransactionOffsets.prototype, 'release').mockImplementation(function () {
            if (this === offsets) {
              releases.push({ entries: this.entries.size, snapshots: this.snapshots.size })
            }
            return release.call(this)
          }),
          jest.spyOn(TransactionOffsets.prototype, 'publish').mockImplementation(function (target) {
            if (this === offsets) publications.push(performance.now())
            return publish.call(this, target)
          }),
          jest
            .spyOn(Cluster.prototype, 'markOffsetAsCommitted')
            .mockImplementation(function (value) {
              if (value.groupId === groupId)
                localWrites.push({ ...value, offset: String(value.offset) })
              return markOffsetAsCommitted.call(this, value)
            }),
        ]
        cleanup.add('state spies', () => spies.forEach((spy) => spy.mockRestore()))

        await createTopic({ topic, partitions: 1, replicas: 3 })
        await createTopic({ topic: inputTopic, partitions: 1, replicas: 3 })
        const admin = new Kafka({ brokers, logLevel: logLevel.NOTHING }).admin()
        await admin.connect()
        cleanup.add('admin', () => admin.disconnect())
        await admin.setOffsets({
          groupId,
          topic: inputTopic,
          partitions: [{ partition: 0, offset: '0' }],
        })
        const committedOf = async () => {
          const [{ partitions }] = await admin.fetchOffsets({ groupId, topics: [inputTopic] })
          return String(partitions[0].offset)
        }
        const inputBefore = await committedOf()

        const producer = new Kafka({
          clientId,
          brokers,
          logLevel: logLevel.NOTHING,
          socketFactory: sockets.socketFactory,
          requestTimeout: 1000,
          enforceRequestTimeout: false,
        }).producer({ transactionalId, linger: 0, deliveryTimeout: 10000, retry: RETRY })
        producer.on(producer.events.REQUEST, ({ payload }) => requests.push(payload))
        const probe = new ProducerProbe({
          producer,
          frames: sockets.frames,
          clientId,
          transactionalId,
          maxRequestSize: 1048576,
        }).install()
        cleanup.add('admission probe', () => probe.restore())
        cleanup.add('resources after disconnect', () => probe.settled())
        await producer.connect()
        cleanup.add('producer', () => producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))

        const transaction = await producer.transaction()
        await probe.capture(
          async () => {
            capturing = true
            try {
              return await transaction.send({
                topic,
                messages: [{ value: 'close-0' }, { value: 'close-1' }],
                acks: -1,
              })
            } finally {
              capturing = false
            }
          },
          { signal: scope }
        )
        expect(stateManagers.size).toBe(1)
        const [state] = [...stateManagers]
        offsets = state.offsets
        expect(state.transaction.state()).toBe('IN_TRANSACTION')
        await transaction.sendOffsets({
          consumerGroupId: groupId,
          topics: [{ topic: inputTopic, partitions: [{ partition: 0, offset: '2' }] }],
        })
        const staged = { entries: offsets.entries.size, snapshots: offsets.snapshots.size }

        const coordinator = await coordinatorOf(transactionalId, COORDINATOR_TYPES.TRANSACTION)
        const drop = new ResponseDrop(sockets.chaos, {
          port: coordinator.port,
          apiKey: apiKeys.EndTxn,
        }).arm()
        cleanup.add('response drop', () => drop.release())
        const endsOf = () => ends.filter((end) => end.transactionalId === transactionalId)
        const endFrames = () => sockets.frames.since(apiKeys.EndTxn).length

        const commit = outcomeOf(transaction.commit())
        const applied = await waitForCondition(async () => {
          const [partition] = (await readLog({ brokers, topic })).values()
          return drop.dropped === 1 && partition.records.some((r) => r.batchContext.isControlBatch)
        }, WAIT_BUDGET)
        const endsBeforeClose = endsOf().length

        const closeStartedAt = performance.now()
        await producer.disconnect({ timeout: 1000 })
        const closeMs = performance.now() - closeStartedAt
        const closed = await commit
        const afterClose = {
          state: state.transaction.state(),
          commitPending: state.transaction.commitPending(),
          entries: offsets.entries.size,
          snapshots: offsets.snapshots.size,
          publications: publications.length,
          localWrites: localWrites.length,
        }

        const brokerOffset = await waitForCondition(
          async () => (await committedOf()) === '2',
          WAIT_BUDGET
        )
        const brokerCommitted = {
          offset: await committedOf(),
          values: (await readData(brokers, topic, { committed: true })).map(({ value }) =>
            String(value)
          ),
        }
        await sleep(QUIET_MS)
        const afterQuiet = {
          state: state.transaction.state(),
          publications: publications.length,
          localWrites: localWrites.length,
        }
        await producer.disconnect()
        const endsAfterRepeat = { ends: endsOf().length, frames: endFrames() }

        await producer.connect()
        const ordinary = await outcomeOf(
          producer.send({
            topic: `e2e-p9b-transaction-close-ordinary-${id}`,
            messages: [{ value: 'ordinary' }],
            acks: -1,
          })
        )
        const guarded = await outcomeOf(
          transaction.send({ topic, messages: [{ value: 'late' }], acks: -1 })
        )
        const closing = await probe.closing({
          topic: `e2e-p9b-transaction-close-probe-${id}`,
          budgetMs: 10000,
        })
        const afterProbe = {
          state: state.transaction.state(),
          commitPending: state.transaction.commitPending(),
          entries: offsets.entries.size,
          publications: publications.length,
          localWrites: localWrites.length,
        }
        await producer.disconnect({ timeout: DISCONNECT_BUDGET_MS })
        const afterFinal = probe.counters()

        writeEvidence(EVIDENCE, {
          scenario: 'forced-close',
          cluster,
          config: {
            requestTimeout: 1000,
            enforceRequestTimeout: false,
            linger: 0,
            deliveryTimeout: 10000,
            disconnectTimeout: 1000,
            retry: RETRY,
          },
          replay: {
            log: 'two records and their commit marker, applied by the broker',
            offsets: 'broker committed position 2 while local publication stays 0',
            handlers: 'n/a',
          },
          wireVersions: wireVersionsOf(requests),
          wire: { frames: sockets.frames.trace(), replies: repliesOf(requests) },
          transactionVersion: cluster.level,
          inputBefore,
          staged,
          drops: drop.dropped,
          applied,
          endsBeforeClose,
          closeMs,
          commit: describeError(closed.error),
          afterClose,
          broker: { observed: brokerOffset, ...brokerCommitted },
          afterQuiet,
          endsAfterRepeat,
          ordinary: describeError(ordinary.error),
          guarded: describeError(guarded.error),
          afterProbe,
          releases,
          ends: endsOf(),
          closing,
          afterFinal,
        })

        expect([1, 2]).toContain(cluster.level)
        expect(inputBefore).toBe('0')
        expect(staged).toEqual({ entries: 1, snapshots: 1 })
        expect(applied).toBe(true)
        expect(drop.dropped).toBe(1)
        expect(endsBeforeClose).toBe(1)
        expect(closeMs).toBeGreaterThanOrEqual(900)
        expect(closeMs).toBeLessThan(10000)
        expect(closed.error).toBeInstanceOf(KafkaJSProducerClosedError)
        expect(closed.error).toMatchObject({ outcome: 'unknown' })
        expect(afterClose).toEqual({
          state: 'COMMITTING',
          commitPending: true,
          entries: 0,
          snapshots: 0,
          publications: 0,
          localWrites: 0,
        })
        expect(releases.length).toBeGreaterThanOrEqual(1)
        expect(brokerOffset).toBe(true)
        expect(brokerCommitted).toEqual({ offset: '2', values: ['close-0', 'close-1'] })
        expect(afterQuiet).toEqual({ state: 'COMMITTING', publications: 0, localWrites: 0 })
        expect(endsOf().filter(({ transactionResult }) => !transactionResult)).toEqual([])
        expect(endsAfterRepeat).toEqual({ ends: 1, frames: 1 })
        expect(ordinary.error).toBeNull()
        expect(guarded.error).toBeInstanceOf(KafkaJSNonRetriableError)
        expect(guarded.error.message).toMatch(/while a commit is pending/)
        expect(afterProbe).toEqual({
          state: 'COMMITTING',
          commitPending: true,
          entries: 0,
          publications: 0,
          localWrites: 0,
        })
        expect(afterFinal).toMatchObject({ available: afterFinal.capacity, pins: 0 })
      })
    }),
    caseTimeout(SCENARIO_BUDGET_MS)
  )
})
