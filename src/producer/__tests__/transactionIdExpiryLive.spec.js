const { Kafka, ConfigResourceTypes, logLevel } = require('../../../index')
const { KafkaJSApplicationRecoverableError } = require('../../errors')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const { errorCodeOf } = require('../../../scripts/e2e/idempotentAuthzProbe')
const { NODES, execInKafka1 } = require('../../../testHelpers/leaderElection')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { armedPermanence, outcomeOf } = require('./fixtures/liveTransactions')
const { createLiveSockets } = require('./fixtures/liveWire')
const {
  CASE_JOIN_MS,
  NEVER,
  bounded,
  caseTimeout,
  delay,
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

// Gated: it idles 90 s on a cluster that expires transactional ids after 60 s. Run alone, through
// the guarded entry on its own compose file: `npm run test:e2e:txn-id-expiry`.
const describeExpiry = process.env.TXN_ID_EXPIRY_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'txn-id-expiry'
const RECORDS = 12
const IDLE_MS = 90000
const POST_EXPIRY_MS = 30000
const PHASE_BUDGET_MS = Object.freeze({
  setup: 60000,
  idle: IDLE_MS + 5000,
  post: 75000,
  verify: 25000,
  probe: 45000,
  cleanup: 20000,
})
const CASE_BUDGET_MS = 340000
const HOOK_BUDGET_MS = 60000
const ADMIN_MS = 15000
const CREATE_TOPIC_MS = 15000
const CONNECT_MS = 15000
// A cold cluster spends up to the retry budget below on the first InitProducerId.
const COMMIT_MS = 40000
const READ_BUDGET = { attempts: 80, delay: 250 }
const DISCONNECT_BUDGET_MS = 15000
const STEP_MS = 15000
const DELIVERY_BUDGET_MS = 30000
const BUFFER_MEMORY = 1048576
const MAX_REQUEST_SIZE = 1048576
const SETTINGS = Object.freeze({
  'transactional.id.expiration.ms': '60000',
  'transaction.remove.expired.transaction.cleanup.interval.ms': '10000',
})
/** The id of the first transaction after the idle, and of the one a replacement producer commits. */
const POST_EXPIRY_ID = RECORDS
const REPLACEMENT_ID = RECORDS + 1
/**
 * The broker codes of an identity the coordinator no longer knows or has fenced: INVALID_PRODUCER_EPOCH,
 * INVALID_PRODUCER_ID_MAPPING, UNKNOWN_PRODUCER_ID, PRODUCER_FENCED and TRANSACTIONAL_ID_NOT_FOUND.
 */
const IDENTITY_LOST_CODES = Object.freeze([47, 49, 59, 90, 105])
// A cold cluster answers the first transactional InitProducerId with a retriable error until the
// transaction state topic is ready, and a takeover until the coordinator has finished with the
// previous owner: 12 retries, 50 ms doubling to a 3 s cap, about 21 s in all.
const RETRY = { retries: 12, initialRetryTime: 50, multiplier: 2, maxRetryTime: 3000, factor: 0 }
/** The ways the first transaction after the idle may end: it commits, or the identity is lost. */
const ACCEPTED_BRANCHES = Object.freeze(['new-pid', 'fenced', 'abortable'])
const REPLAY = Object.freeze({
  log: 'the committed ids before the idle appear once; the id after it commits once under a new producer id, or a replacement producer commits the next one',
  offsets: 'n/a',
  handlers: 'n/a',
})

/**
 * @param {number} count
 * @returns {number[]} 0 .. count - 1
 */
const range = (count) => Array.from({ length: count }, (_, index) => index)

/**
 * Reads both expiry settings of every broker through the public admin client.
 * @param {string[]} brokers seed addresses
 * @param {AbortSignal} signal the owner's; nothing is asked once it aborted
 * @returns {Promise<Record<string, Record<string, string>>>} each setting's value by broker id
 * @throws {Error} a failed read, or the reason `signal` aborted with
 */
const readSettings = async (brokers, signal) => {
  const admin = new Kafka({ brokers, logLevel: logLevel.NOTHING }).admin()
  await bounded(signal, ADMIN_MS, () => admin.connect(), { label: 'The admin connect' })
  try {
    const { resources } = await bounded(
      signal,
      ADMIN_MS,
      () =>
        admin.describeConfigs({
          includeSynonyms: false,
          resources: Object.values(NODES).map(({ nodeId }) => ({
            type: ConfigResourceTypes.BROKER,
            name: String(nodeId),
            configNames: Object.keys(SETTINGS),
          })),
        }),
      { label: 'The settings read' }
    )
    return Object.fromEntries(
      resources.map(({ resourceName, configEntries }) => [
        resourceName,
        Object.fromEntries(
          configEntries.map(({ configName, configValue }) => [configName, configValue])
        ),
      ])
    )
  } finally {
    await within(ADMIN_MS, admin.disconnect(), 'The admin disconnect')
  }
}

/**
 * What the broker says of a transactional id, through the administration tool in the first
 * broker's container. It is evidence only: it never throws, and an answer that cannot be read is
 * `found: null`.
 * @param {string} transactionalId
 * @returns {{ found: boolean|null, output: string }} `found` is false when the broker reports it
 *   does not know the id
 */
const describeTransactionalId = (transactionalId) => {
  try {
    const output = execInKafka1([
      '/opt/kafka/bin/kafka-transactions.sh',
      '--bootstrap-server',
      'kafka1:29092',
      'describe',
      '--transactional-id',
      transactionalId,
    ]).toString()
    return { found: true, output: output.slice(0, 600) }
  } catch (error) {
    const output = `${error.stdout ?? ''}${error.stderr ?? ''}${error.message}`
    return {
      found: /could not be found|not\s?found/i.test(output) ? false : null,
      output: output.slice(0, 600),
    }
  }
}

/**
 * How the first transaction after the idle ended.
 * @param {Error|null} error what the transaction rejected with
 * @param {string|undefined} postPid the producer id of the last Produce request after the idle
 * @param {string} initialPid the producer id before the idle
 * @returns {'new-pid'|'same-pid'|'fenced'|'abortable'|'other'} `fenced` is an identity error that
 *   makes the producer unusable (application recoverable), `abortable` one after which it carries
 *   on; any other error is `other`
 */
const branchOf = (error, postPid, initialPid) => {
  if (error === null) return postPid === initialPid ? 'same-pid' : 'new-pid'
  if (!IDENTITY_LOST_CODES.includes(errorCodeOf(error))) return 'other'
  if (error.category === 'applicationRecoverable') return 'fenced'
  if (error.category === 'abortable') return 'abortable'
  return 'other'
}

/**
 * The acceptance of the scenario: the ids committed before the idle are in the log once, and the
 * first transaction after the expiry either committed under a new producer id, or failed with an
 * identity error after which a replacement producer committed; each branch with the closing
 * admission probe of the producer that finished the scenario, and the failed one's terminal
 * cleanup.
 * @param {object} observation what `ExpiryScenario#run` returned
 * @returns {void}
 * @throws {Error} through a failed expectation, naming the first invariant that does not hold
 */
const accept = (observation) => {
  const { postExpiry, visibleIds } = observation
  expect(observation.idleMs).toBeGreaterThanOrEqual(IDLE_MS)
  expect(visibleIds.filter((id) => id < RECORDS)).toEqual(range(RECORDS))
  expect(ACCEPTED_BRANCHES).toContain(postExpiry.branch)
  const closing = expect.objectContaining({
    records: expect.any(Number),
    observed: expect.objectContaining({ inEntry: true, waitingAfterEntry: 0 }),
  })
  if (postExpiry.branch === 'new-pid') {
    expect(postExpiry.postPid).not.toBe(observation.initialPid)
    expect(visibleIds).toEqual([...range(RECORDS), POST_EXPIRY_ID])
    expect(observation.admissionProbe).toEqual(closing)
    return
  }
  expect(IDENTITY_LOST_CODES).toContain(postExpiry.error.code)
  expect(visibleIds).toEqual([...range(RECORDS), REPLACEMENT_ID])
  expect(observation.replacementIds).toEqual([REPLACEMENT_ID])
  expect(observation.fatalCleanup).toEqual(
    expect.objectContaining({ reserved: 0, waitingBytes: 0, debts: 0, pins: 0 })
  )
  expect(observation.replacementProbe).toEqual(closing)
  if (postExpiry.branch === 'fenced') {
    const { armed, flush, connect } = observation.permanence
    expect(armed).toBeInstanceOf(KafkaJSApplicationRecoverableError)
    expect(flush).toBe(armed)
    expect(connect).toBe(armed)
  }
}

/**
 * One transactional id that is committed on, left idle until the broker expires it, and used
 * again. Every producer it opens, its probe and the produce ledger go to the case's
 * `CleanupStack`. The producer ids are read from the Produce requests the producers really sent.
 */
class ExpiryScenario {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses
   * @param {CleanupStack} options.cleanup receives every resource the scenario opens
   */
  constructor({ brokers, cleanup }) {
    const id = secureRandom()
    this.brokers = brokers
    this.cleanup = cleanup
    this.name = `e2e-p9b-txn-id-expiry-${id}`
    this.topic = this.name
    this.transactionalId = this.name
    this.ledger = new ProduceLedger()
    this.original = null
    this.replacement = null
    this.initialPid = null
    this.committedAt = 0
    this.described = null
  }

  /**
   * Opens one producer on the scenario's transactional id with its own sockets and its probe. Only
   * one probe may be installed at a time, so a later producer is opened after the earlier
   * probe was restored.
   * @param {string} label `original` or `replacement`
   * @returns {{ producer: object, probe: ProducerProbe }}
   */
  open(label) {
    const clientId = `${this.name}-${label}`
    const sockets = createLiveSockets()
    const producer = new Kafka({
      clientId,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: sockets.socketFactory,
    }).producer({
      transactionalId: this.transactionalId,
      retry: RETRY,
      bufferMemory: BUFFER_MEMORY,
      maxRequestSize: MAX_REQUEST_SIZE,
    })
    const probe = new ProducerProbe({
      producer,
      frames: sockets.frames,
      clientId,
      transactionalId: this.transactionalId,
      maxRequestSize: MAX_REQUEST_SIZE,
      brokers: this.brokers,
    }).install()
    this.cleanup.add(`${label} admission probe`, () => probe.restore())
    this.cleanup.add(`${label} resources after disconnect`, (scope) =>
      probe.settled({ signal: scope })
    )
    this.cleanup.add(`${label} producer`, () =>
      producer.disconnect({ timeout: DISCONNECT_BUDGET_MS })
    )
    return { producer, probe }
  }

  /**
   * Commits one transaction of `ids` on `member` under its probe's capture.
   * @param {{ producer: object, probe: ProducerProbe }} member
   * @param {number[]} ids one record each
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   */
  async commitUnderCapture({ producer, probe }, ids, signal) {
    await bounded(signal, CONNECT_MS, () => producer.connect(), { label: 'The producer connect' })
    await bounded(
      signal,
      COMMIT_MS,
      (scope) =>
        probe.capture(
          async () => {
            const transaction = await producer.transaction()
            await transaction.send({
              topic: this.topic,
              messages: ids.map((id) => ({ value: String(id) })),
              acks: -1,
            })
            await transaction.commit()
          },
          { signal: scope }
        ),
      { label: 'The commit' }
    )
  }

  /**
   * Creates the topic and commits the first records, the producer id of which the idle is
   * compared against.
   * @param {AbortSignal} signal the setup phase's scope
   * @returns {Promise<{ initialPid: string }>}
   */
  async start(signal) {
    await bounded(
      signal,
      CREATE_TOPIC_MS,
      () => createTopic({ topic: this.topic, partitions: 1, replicas: 3 }),
      { label: 'The topic creation' }
    )
    this.ledger.install()
    this.cleanup.add('produce ledger', () => this.ledger.restore())
    this.original = this.open('original')
    await this.commitUnderCapture(this.original, range(RECORDS), signal)
    this.committedAt = performance.now()
    this.initialPid = this.ledger.of(this.topic).at(-1).producerId
    this.described = describeTransactionalId(this.transactionalId)
    return { initialPid: this.initialPid }
  }

  /**
   * Waits out the idle, with no operation on the transactional id, and asks the broker whether it
   * still knows it.
   * @param {AbortSignal} signal the idle phase's scope
   * @returns {Promise<{ idleMs: number, before: object, after: object }>} `idleMs` is measured
   *   from the end of the commit on the monotonic clock
   */
  async idle(signal) {
    await delay(signal, IDLE_MS)
    const after = describeTransactionalId(this.transactionalId)
    return {
      idleMs: Math.round(performance.now() - this.committedAt),
      before: this.described,
      after,
    }
  }

  /**
   * The first transaction after the idle on the original producer, and what follows from how it
   * ended. A producer that an identity error made unusable proves its armed permanence, then is
   * disconnected and must retain nothing; one an abortable error left usable aborts. A
   * replacement producer on the same transactional id then commits the next id under its own
   * probe.
   * @param {AbortSignal} signal the post phase's scope
   * @returns {Promise<object>} the branch and what each step of it observed
   */
  async afterExpiry(signal) {
    const { producer, probe } = this.original
    let transaction = null
    const outcome = await outcomeOf(
      bounded(
        signal,
        POST_EXPIRY_MS,
        async () => {
          transaction = await producer.transaction()
          await transaction.send({
            topic: this.topic,
            messages: [{ value: String(POST_EXPIRY_ID) }],
            acks: -1,
          })
          await transaction.commit()
        },
        { label: 'The first transaction after the idle' }
      )
    )
    const postPid = this.ledger.of(this.topic).at(-1)?.producerId
    const branch = branchOf(outcome.error, postPid, this.initialPid)
    const error = outcome.error && {
      ...describeError(outcome.error),
      code: errorCodeOf(outcome.error),
    }
    writeEvidence(EVIDENCE, { step: 'post-expiry', branch, postPid, error })
    const result = { branch, postPid, error }
    if (!ACCEPTED_BRANCHES.includes(branch) || branch === 'new-pid') return result

    if (branch === 'fenced') {
      result.permanence = await bounded(signal, STEP_MS, () => armedPermanence(producer), {
        label: 'The armed permanence of the original producer',
      })
    } else {
      const aborted = transaction
        ? await outcomeOf(
            bounded(signal, STEP_MS, () => transaction.abort(), { label: 'The abort' })
          )
        : { error: null }
      result.abortError = aborted.error
    }
    await bounded(signal, DISCONNECT_BUDGET_MS, () => producer.disconnect(), {
      label: 'The disconnect of the original producer',
    })
    result.fatalCleanup = await probe.settled({ signal })
    probe.restore()
    writeEvidence(EVIDENCE, {
      step: 'fatalCleanup',
      counters: result.fatalCleanup,
      permanence: result.permanence && {
        armed: describeError(result.permanence.armed),
        sameForFlush: result.permanence.flush === result.permanence.armed,
        sameForConnect: result.permanence.connect === result.permanence.armed,
      },
      abortError: describeError(result.abortError),
    })

    this.replacement = this.open('replacement')
    await this.commitUnderCapture(this.replacement, [REPLACEMENT_ID], signal)
    result.replacementIds = [REPLACEMENT_ID]
    writeEvidence(EVIDENCE, { step: 'replacement', ids: result.replacementIds })
    return result
  }

  /**
   * The closing admission probe on the producer that finished the scenario, in a fresh
   * transaction with its records on a topic of their own.
   * @param {AbortSignal} signal the probe phase's scope
   * @returns {Promise<object>} what `ProducerProbe#closing` observed
   */
  async closing(signal) {
    const { producer, probe } = this.replacement ?? this.original
    const sender = await bounded(signal, CONNECT_MS, () => producer.transaction(), {
      label: 'The probe transaction',
    })
    return probe.closing({
      sender,
      topic: `${this.name}-probe`,
      finish: () => sender.commit(),
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      budgetMs: DELIVERY_BUDGET_MS,
      signal,
    })
  }

  /**
   * The idle, the first transaction after it, the committed log and the closing probe, each in
   * its own phase of the case's timeline.
   * @param {CaseTimeline} timeline
   * @param {AbortSignal} scope the case's
   * @returns {Promise<object>} the observation `accept` judges
   * @throws {Error} a phase's failure
   */
  async run(timeline, scope) {
    const idle = await timeline.run('idle', (signal) => this.idle(signal), scope)
    writeEvidence(EVIDENCE, { step: 'idle', ...idle })
    const post = await timeline.run('post', (signal) => this.afterExpiry(signal), scope)
    if (!ACCEPTED_BRANCHES.includes(post.branch)) {
      throw new Error(
        `The first transaction after the idle ended as ${post.branch}, which is not an expiry outcome: ${post.error?.message ?? 'it committed'}`
      )
    }
    const visibleIds = await timeline.run(
      'verify',
      async (signal) =>
        (
          await readUntilCovered(
            signal,
            () => readData(this.brokers, this.topic, { committed: true }),
            (answer) => answer.length > RECORDS,
            READ_BUDGET
          )
        ).map(({ value }) => Number(value.toString())),
      scope
    )
    writeEvidence(EVIDENCE, {
      step: 'oracle',
      log: { expectedOnce: range(RECORDS), visible: visibleIds },
      committedOffsets: 'n/a',
      handlerDelivery: 'n/a',
      replay: REPLAY,
    })
    const closing = await timeline.run('probe', (signal) => this.closing(signal), scope)
    if (post.replacementIds) {
      writeEvidence(EVIDENCE, { step: 'replacementProbe', replacesFatal: true, closing })
    } else {
      writeEvidence(EVIDENCE, { step: 'probe', closing })
    }
    return {
      initialPid: this.initialPid,
      idleMs: idle.idleMs,
      postExpiry: { branch: post.branch, postPid: post.postPid, error: post.error },
      permanence: post.permanence,
      abortError: post.abortError,
      fatalCleanup: post.fatalCleanup,
      replacementIds: post.replacementIds,
      visibleIds,
      admissionProbe: post.replacementIds ? undefined : closing,
      replacementProbe: post.replacementIds ? closing : undefined,
    }
  }
}

/**
 * Live proof of what a transactional producer does when the broker expires its idle transactional
 * id (`transactional.id.expiration.ms`, here 60 s, checked every 10 s): after 12 committed ids and
 * a 90 s idle, the first transaction on the same id either commits under a new producer id or
 * fails with a typed identity error, after which a replacement producer on that id commits the
 * next one. The ids committed before the idle stay in the log once. The two settings are read
 * through the admin client before anything is committed, and the broker is asked whether it
 * still knows the id after the idle; the outcome of the transaction is what closes the arm, never
 * the client's own wait.
 *
 * Replay sets: the physical log holds the ids committed before the idle once; committed offsets
 * and handler deliveries are not part of this scenario.
 * @see https://kafka.apache.org/43/configuration/broker-configs/
 */
describeExpiry('Producer > transactional id expiry (live)', () => {
  /** @type {Record<string, Record<string, string>>} the settings of each broker */
  let settings

  beforeAll(async () => {
    liveRun.assertClean()
    settings = await bounded(
      NEVER,
      HOOK_BUDGET_MS - CASE_JOIN_MS,
      (scope) => readSettings(plainTextBrokers(), scope),
      { label: 'The setup' }
    )
    writeEvidence(EVIDENCE, { step: 'settings', settings })
    for (const { nodeId } of Object.values(NODES)) {
      expect(settings[nodeId]).toEqual(SETTINGS)
    }
  }, HOOK_BUDGET_MS)

  test(
    'keeps committed records when an idle transaction identity expires',
    liveCase(CASE_BUDGET_MS, async (scope) => {
      const timeline = new CaseTimeline({
        phases: PHASE_BUDGET_MS,
        limitMs: CASE_BUDGET_MS,
        reserveMs: JOIN_RESERVE_MS,
      })
      const cleanup = new CleanupStack()

      await cleanup.run(
        async () => {
          const scenario = new ExpiryScenario({ brokers: plainTextBrokers(), cleanup })
          const { initialPid } = await timeline.run(
            'setup',
            (signal) => scenario.start(signal),
            scope
          )
          writeEvidence(EVIDENCE, {
            step: 'commit',
            N: RECORDS,
            ids: range(RECORDS),
            initialPid,
            idleMs: IDLE_MS,
            postExpiryBudgetMs: POST_EXPIRY_MS,
          })
          accept(await scenario.run(timeline, scope))
        },
        { budgetMs: PHASE_BUDGET_MS.cleanup }
      )
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
})
