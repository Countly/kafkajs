const { Kafka, logLevel } = require('../../../index')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const { errorCodeOf } = require('../../../scripts/e2e/idempotentAuthzProbe')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { settledTransactionVersion } = require('./fixtures/liveTransactions')
const { createLiveSockets } = require('./fixtures/liveWire')
const {
  CASE_JOIN_MS,
  NEVER,
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
} = require('./fixtures/bounded')
const {
  CleanupStack,
  ProduceLedger,
  ProducerProbe,
  describeError,
  readData,
  readUntilCovered,
  within,
} = require('./fixtures/liveProbe')
const { CaseTimeline, JOIN_RESERVE_MS } = require('./fixtures/replicaOutage')

// Gated: it commits 32768 transactions on one producer. Run alone, through the guarded entry:
// `npm run test:e2e:txn-epoch-overflow`.
const describeEpoch = process.env.TXN_EPOCH_OVERFLOW_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'txn-epoch-overflow'
const TRANSACTIONS = 32768
// Back-to-back transactions on transaction version 2 each wait at the broker for the previous one
// (`add.partitions.to.txn.retry.backoff.ms`, 20 ms by default): 40 to 54 ms measured per
// transaction on 4.3.1, so 32768 of them take up to about 1770 s.
const LOOP_BUDGET_MS = 2100000
const PHASE_BUDGET_MS = Object.freeze({
  setup: 90000,
  loop: LOOP_BUDGET_MS,
  verify: 60000,
  probe: 60000,
  cleanup: 30000,
})
// The phases and the join reserve, and 6 s over, so that the case and the cluster's start end
// inside the 2700 s of the guarded run.
const CASE_BUDGET_MS = 2350000
const HOOK_BUDGET_MS = 60000
const CREATE_TOPIC_MS = 15000
const CONNECT_MS = 20000
// A cold cluster spends up to the retry budget below on the first InitProducerId.
const WARM_UP_MS = 40000
const TRANSACTION_MS = 30000
const READ_BUDGET = { attempts: 40, delay: 500 }
const DISCONNECT_BUDGET_MS = 15000
const DELIVERY_BUDGET_MS = 30000
const BUFFER_MEMORY = 1048576
const MAX_REQUEST_SIZE = 1048576
/** How many transactions pass between two reads of the probe's ledger, which forgets retired sends. */
const PRUNE_EVERY = 1024
const INVALID_PRODUCER_EPOCH = 47
// A cold cluster answers the first transactional InitProducerId with a retriable error until the
// transaction state topic is ready: 12 retries, 50 ms doubling to a 3 s cap, about 21 s in all.
const RETRY = { retries: 12, initialRetryTime: 50, multiplier: 2, maxRetryTime: 3000, factor: 0 }

/**
 * What the committed log of the workload topic says of the ids one transaction each committed.
 * @param {number[]} ids the values of the committed records, in log order
 * @param {number} expected how many transactions were committed
 * @returns {{ committed: number, first: number|null, last: number|null, duplicates: number,
 *   firstMismatch: { index: number, found: number|null }|null }} `firstMismatch` is the first
 *   position that does not hold its own index: a missing, repeated or reordered id
 */
const logVerdict = (ids, expected) => {
  let index = 0
  while (index < Math.max(ids.length, expected) && ids[index] === index) index += 1
  return {
    committed: ids.length,
    first: ids[0] ?? null,
    last: ids.at(-1) ?? null,
    duplicates: ids.length - new Set(ids).size,
    firstMismatch:
      index === Math.max(ids.length, expected) ? null : { index, found: ids[index] ?? null },
  }
}

/**
 * The producer ids the Produce requests of the workload carried, in the order they first
 * appeared, with the epochs each was used at.
 * @param {object[]} calls the workload's Produce calls, as `ProduceLedger#of` returns them
 * @returns {Array<{ producerId: string, firstEpoch: number, lastEpoch: number, calls: number }>}
 */
const identitiesOf = (calls) => {
  const seen = new Map()
  for (const { producerId, producerEpoch } of calls) {
    const known = seen.get(producerId)
    if (known) {
      known.lastEpoch = producerEpoch
      known.calls += 1
    } else {
      seen.set(producerId, {
        producerId,
        firstEpoch: producerEpoch,
        lastEpoch: producerEpoch,
        calls: 1,
      })
    }
  }
  return [...seen.values()]
}

/**
 * The acceptance of the scenario: a new producer id after the epoch boundary, no epoch error at
 * the application, every committed id once and in order, and the closing admission probe of the
 * same producer.
 * @param {object} observation what `EpochScenario#run` returned
 * @returns {void}
 * @throws {Error} through a failed expectation, naming the first invariant that does not hold
 */
const accept = (observation) => {
  expect(observation.transactions).toBe(TRANSACTIONS)
  expect(observation.pidAfter).not.toBe(observation.pidBefore)
  expect(observation.identities.length).toBeGreaterThanOrEqual(2)
  expect(observation.identities[0].calls).toBeGreaterThan(TRANSACTIONS / 2)
  expect(observation.applicationEpochErrors).toBe(0)
  expect(observation.oracle).toEqual({
    committed: TRANSACTIONS,
    first: 0,
    last: TRANSACTIONS - 1,
    duplicates: 0,
    firstMismatch: null,
  })
  expect(observation.elapsedMs).toBeLessThanOrEqual(LOOP_BUDGET_MS)
  expect(observation.admissionProbe).toEqual(
    expect.objectContaining({
      records: expect.any(Number),
      observed: expect.objectContaining({ inEntry: true, waitingAfterEntry: 0 }),
    })
  )
}

/**
 * One producer committing one record per transaction until its transaction epoch has to overflow
 * and the broker has to hand out a new producer id. Every resource it opens goes to the case's
 * `CleanupStack`. The producer ids are read from the Produce requests the producer really sent,
 * never from its own state.
 */
class EpochScenario {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses
   * @param {CleanupStack} options.cleanup receives the ledger, the probe and the producer
   * @param {number} [options.transactions=32768] one record each
   */
  constructor({ brokers, cleanup, transactions = TRANSACTIONS }) {
    const id = secureRandom()
    this.brokers = brokers
    this.cleanup = cleanup
    this.transactions = transactions
    this.topic = `e2e-p9b-epoch-overflow-${id}`
    this.warmTopic = `${this.topic}-warm`
    this.probeTopic = `${this.topic}-probe`
    this.clientId = `e2e-p9b-epoch-overflow-${id}`
    this.sockets = createLiveSockets()
    this.ledger = new ProduceLedger()
    /** @type {Record<string, number>} the highest version each API was sent at */
    this.wireVersions = {}
    this.committed = 0
    this.applicationEpochErrors = 0
    this.elapsedMs = 0
    this.producer = null
    this.probe = null
    this.warm = null
  }

  /**
   * Creates the topics, connects the producer and commits one warm-up transaction under the
   * probe's capture, which fixes the producer id the loop is compared against.
   * @param {AbortSignal} signal the phase's scope; every step is bounded under it
   * @returns {Promise<void>}
   * @throws {Error} a failed step, or the reason `signal` aborted with
   */
  async start(signal) {
    const { brokers, clientId } = this
    for (const topic of [this.topic, this.warmTopic]) {
      await bounded(
        signal,
        CREATE_TOPIC_MS,
        () => createTopic({ topic, partitions: 1, replicas: 3 }),
        {
          label: `The creation of ${topic}`,
        }
      )
    }
    this.ledger.install()
    this.cleanup.add('produce ledger', () => this.ledger.restore())
    const producer = new Kafka({
      clientId,
      brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.sockets.socketFactory,
    }).producer({
      transactionalId: clientId,
      retry: RETRY,
      linger: 0,
      bufferMemory: BUFFER_MEMORY,
      maxRequestSize: MAX_REQUEST_SIZE,
    })
    producer.on(producer.events.REQUEST, ({ payload: { apiName, apiVersion } }) => {
      this.wireVersions[apiName] = Math.max(this.wireVersions[apiName] ?? 0, apiVersion)
    })
    this.producer = producer
    this.probe = new ProducerProbe({
      producer,
      frames: this.sockets.frames,
      clientId,
      transactionalId: clientId,
      maxRequestSize: MAX_REQUEST_SIZE,
      brokers,
    }).install()
    this.cleanup.add('admission probe', () => this.probe.restore())
    this.cleanup.add('resources after disconnect', (scope) => this.probe.settled({ signal: scope }))
    this.cleanup.add('producer', () => producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))
    await bounded(signal, CONNECT_MS, () => producer.connect(), { label: 'The producer connect' })
    await bounded(
      signal,
      WARM_UP_MS,
      (scope) =>
        this.probe.capture(
          async () => {
            const transaction = await producer.transaction()
            await transaction.send({
              topic: this.warmTopic,
              messages: [{ value: 'warm' }],
              acks: -1,
            })
            await transaction.commit()
          },
          { signal: scope }
        ),
      { label: 'The warm-up transaction' }
    )
    this.warm = this.ledger.of(this.warmTopic).at(-1)
    if (!this.warm) throw new Error('The warm-up transaction left no Produce request')
  }

  /**
   * One single-record transaction.
   * @param {number} id the value of the record
   * @returns {Promise<void>}
   */
  async transact(id) {
    const transaction = await this.producer.transaction()
    await transaction.send({ topic: this.topic, messages: [{ value: String(id) }], acks: -1 })
    await transaction.commit()
  }

  /**
   * Lets go of what the observers keep per transaction: the probe's ledger forgets the sends that
   * retired, and the spies forget the calls they recorded.
   * @returns {void}
   */
  prune() {
    this.probe.resources()
    for (const spy of [...this.probe.spies, this.ledger.spy]) spy.mockClear()
  }

  /**
   * Commits one single-record transaction per id, in order. The scope is checked before every
   * transaction, so nothing is begun once it aborted, and each transaction is given
   * `TRANSACTION_MS`. The observers are pruned every `PRUNE_EVERY` transactions.
   * @param {AbortSignal} signal the loop phase's scope
   * @returns {Promise<void>}
   * @throws {Error} the first transaction's failure, counted as an application epoch error when it
   *   is INVALID_PRODUCER_EPOCH, or the reason `signal` aborted with
   */
  async loop(signal) {
    const startedAt = performance.now()
    try {
      for (let id = this.committed; id < this.transactions; id++) {
        signal.throwIfAborted()
        try {
          await within(TRANSACTION_MS, this.transact(id), `Transaction ${id}`)
        } catch (error) {
          if (errorCodeOf(error) === INVALID_PRODUCER_EPOCH) this.applicationEpochErrors += 1
          throw error
        }
        this.committed += 1
        if (this.committed % PRUNE_EVERY === 0) this.prune()
      }
    } finally {
      this.elapsedMs += performance.now() - startedAt
    }
  }

  /**
   * @returns {{ transactions: number, pidBefore: string, pidAfter: string, epochBefore: number,
   *   epochAfter: number, applicationEpochErrors: number, elapsedMs: number,
   *   identities: object[] }} what the loop did, as the Produce requests carried it
   */
  progress() {
    const calls = this.ledger.of(this.topic)
    const last = calls.at(-1)
    return {
      transactions: this.committed,
      pidBefore: this.warm.producerId,
      pidAfter: last?.producerId,
      epochBefore: this.warm.producerEpoch,
      epochAfter: last?.producerEpoch,
      applicationEpochErrors: this.applicationEpochErrors,
      elapsedMs: Math.round(this.elapsedMs),
      identities: identitiesOf(calls),
    }
  }

  /**
   * Reads the committed log as a `read_committed` reader sees it, once it covers every id.
   * @param {AbortSignal} signal the verify phase's scope
   * @returns {Promise<number[]>} the committed values in log order
   */
  async read(signal) {
    const records = await readUntilCovered(
      signal,
      () => readData(this.brokers, this.topic, { committed: true }),
      (answer) => answer.length >= this.transactions,
      READ_BUDGET
    )
    return records.map(({ value }) => Number(value.toString()))
  }

  /**
   * The closing admission probe on the same producer, in a fresh transaction that is not one of
   * the counted ones, its records on a topic of their own.
   * @param {AbortSignal} signal the probe phase's scope
   * @returns {Promise<object>} what `ProducerProbe#closing` observed
   */
  async closing(signal) {
    const sender = await bounded(signal, CONNECT_MS, () => this.producer.transaction(), {
      label: 'The probe transaction',
    })
    return this.probe.closing({
      sender,
      topic: this.probeTopic,
      finish: () => sender.commit(),
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      budgetMs: DELIVERY_BUDGET_MS,
      signal,
    })
  }

  /**
   * The loop, the committed read and the probe, each in its own phase of the case's timeline.
   * The loop's partial result is stamped even when it failed.
   * @param {CaseTimeline} timeline
   * @param {AbortSignal} scope the case's
   * @returns {Promise<object>} the observation `accept` judges
   * @throws {Error} a phase's failure; the loop's after its partial evidence was written
   */
  async run(timeline, scope) {
    try {
      await timeline.run('loop', (signal) => this.loop(signal), scope)
    } catch (error) {
      writeEvidence(EVIDENCE, {
        step: 'loop',
        failed: describeError(error),
        wireVersions: this.wireVersions,
        ...this.progress(),
      })
      throw error
    }
    const loop = this.progress()
    writeEvidence(EVIDENCE, { step: 'loop', wireVersions: this.wireVersions, ...loop })
    const ids = await timeline.run('verify', (signal) => this.read(signal), scope)
    const oracle = logVerdict(ids, this.transactions)
    writeEvidence(EVIDENCE, {
      step: 'oracle',
      log: oracle,
      committedOffsets: 'n/a',
      handlerDelivery: 'n/a',
    })
    const admissionProbe = await timeline.run('probe', (signal) => this.closing(signal), scope)
    writeEvidence(EVIDENCE, { step: 'probe', closing: admissionProbe })
    return { ...loop, oracle, admissionProbe }
  }
}

/**
 * Live proof that a transactional producer on transaction version 2 gets a new producer id when its
 * epoch runs out (KIP-890): 32768 committed transactions of one record each, more than one epoch
 * space, with no epoch error reaching the application and every committed record in the log once,
 * in order. The ids the producer used are read from its Produce requests. The closing admission
 * probe then runs on the same producer.
 *
 * Replay sets: the physical log holds the committed ids once and in order; committed offsets and
 * handler deliveries are not part of this scenario.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 */
describeEpoch('Producer > transaction epoch overflow (live)', () => {
  /** @type {{ level: number, epoch: number }} the cluster's finalized transaction version */
  let cluster

  beforeAll(async () => {
    liveRun.assertClean()
    cluster = await bounded(
      NEVER,
      HOOK_BUDGET_MS - CASE_JOIN_MS,
      () => settledTransactionVersion(),
      {
        label: 'The setup',
      }
    )
  }, HOOK_BUDGET_MS)

  test(
    'renews the producer identity across the transaction epoch boundary',
    liveCase(CASE_BUDGET_MS, async (scope) => {
      const timeline = new CaseTimeline({
        phases: PHASE_BUDGET_MS,
        limitMs: CASE_BUDGET_MS,
        reserveMs: JOIN_RESERVE_MS,
      })
      const cleanup = new CleanupStack()

      await cleanup.run(
        async () => {
          const scenario = new EpochScenario({ brokers: plainTextBrokers(), cleanup })
          writeEvidence(EVIDENCE, {
            step: 'setup',
            transactionVersion: cluster.level,
            topic: scenario.topic,
            transactions: TRANSACTIONS,
            loopBudgetMs: LOOP_BUDGET_MS,
            config: { retry: RETRY, bufferMemory: BUFFER_MEMORY, maxRequestSize: MAX_REQUEST_SIZE },
          })
          expect(cluster.level).toBe(2)
          await timeline.run('setup', (signal) => scenario.start(signal), scope)
          accept(await scenario.run(timeline, scope))
        },
        { budgetMs: PHASE_BUDGET_MS.cleanup }
      )
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
})
