const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawn, spawnSync } = require('child_process')
const Broker = require('../../broker')
const { Kafka, logLevel } = require('../../../index')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const { errorCodeOf } = require('../../../scripts/e2e/idempotentAuthzProbe')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const {
  sleep,
  waitForCondition,
  withTimeout,
} = require('../../consumer/__tests__/fixtures/liveScenario')
const {
  crashAfterProduce,
  outcomeOf,
  settledTransactionVersion,
} = require('./fixtures/liveTransactions')

const ROOT = path.join(__dirname, '..', '..', '..')
const RUN_SH = path.join(ROOT, 'scripts', 'e2e', 'javaConsumer', 'run.sh')
const EVIDENCE = 'java-transaction-interop'

/**
 * Appends one record to the scenario's evidence file, naming the cluster it ran on.
 * @param {object} fields what was measured
 */
const evidence = (fields) =>
  writeEvidence(EVIDENCE, {
    kafkaVersion: process.env.KAFKA_VERSION,
    composeFile: process.env.COMPOSE_FILE || 'default',
    ...fields,
  })

/**
 * Downloads and compiles the Java member once, at collection time, so a missing JDK, an
 * unreachable Maven Central or a class missing from the classpath is decided before jest builds
 * the suite. A supplied `JAVA_E2E_WORK_DIR` is a jar cache: it is used as it is and never removed,
 * and with `JAVA_E2E_OFFLINE=1` the probe refuses to download into it.
 * @returns {{ ok: boolean, reason: string, workDir: string|null, owned: boolean }} `owned` is
 *   whether this spec created `workDir` and so removes it
 */
const preflight = () => {
  const supplied = process.env.JAVA_E2E_WORK_DIR
  const owned = !supplied
  // One fixed directory, cleared first: a run that ends before `afterAll` leaves at most this one.
  const workDir = supplied || path.join(os.tmpdir(), 'kafkajs-java-transactions')
  if (owned) {
    fs.rmSync(workDir, { recursive: true, force: true })
    fs.mkdirSync(workDir)
  }
  const probe = spawnSync(RUN_SH, ['--role', 'transactional-producer', '--probe'], {
    env: { ...process.env, JAVA_E2E_WORK_DIR: workDir },
    encoding: 'utf8',
    timeout: 120000,
  })
  if (probe.status === 0) {
    return { ok: true, reason: '', workDir, owned }
  }
  if (owned) {
    fs.rmSync(workDir, { recursive: true, force: true })
  }
  const stderr = probe.stderr || ''
  const reason =
    probe.error && probe.error.code === 'ENOENT'
      ? 'run.sh not executable'
      : /NoClassDefFoundError/.test(stderr)
        ? 'a class was missing from the classpath'
        : /command not found|javac/.test(stderr)
          ? 'no JDK on PATH'
          : /Missing cached Java dependency/.test(stderr)
            ? 'the cached jars are missing'
            : 'the jars could not be downloaded'
  return { ok: false, reason, workDir: null, owned }
}

// Gated: it spawns JVMs against the shared cluster. Run alone:
// `npm run test:e2e:java-transaction-interop`, then again on the transaction version 1 cluster arm
// and on Kafka 4.1.2.
const GATED = process.env.JAVA_TRANSACTION_INTEROP_E2E === '1'
const JAVA = GATED ? preflight() : { ok: false, reason: 'gated', workDir: null, owned: false }
if (GATED && !JAVA.ok) {
  // A gated run that cannot start must not read as a pass. No evidence is written here: that
  // discovers the broker, which a failed preflight may not have.
  test('the Java member can be built and started', () => {
    throw new Error(`The Java transaction interop scenario cannot run: ${JAVA.reason}`)
  })
}
const describeJavaTransactions = JAVA.ok ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 180000
const HOOK_BUDGET_MS = 90000
// On the transaction version 1 arm the level is waited for, up to 120 s (the shared fixture).
const FEATURE_HOOK_BUDGET_MS = 150000
const DISCONNECT_BUDGET_MS = 15000
// A Java member that hangs fails its own case on this deadline, well inside the case's budget.
const CHILD_DEADLINE_MS = 90000
const JAVA_READ_MS = 15000 // how long the Java read_committed consumer reads
const HOLD_OPEN_MS = 15000 // how long the Java member keeps the transaction it is fenced in open
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const QUIET_MS = 1500 // how long a reader waits for a record that should not exist
const RECORDS = 4
const OWN_TRANSACTIONS = 5
// A takeover's InitProducerId is answered CONCURRENT_TRANSACTIONS until the coordinator has
// aborted the transaction the previous owner left open; a cold cluster answers the first one with
// a retriable error until the transaction state topic is ready: 12 retries, 50 ms doubling to a
// 3 s cap, about 21 s. Set on the client as well, for its admin, and on every reader.
const RETRY = { retries: 12, initialRetryTime: 50, multiplier: 2, maxRetryTime: 3000, factor: 0 }
const INVALID_PRODUCER_EPOCH = 47
const INVALID_RECORD = 87
const PRODUCER_FENCED = 90
const EXIT_DONE = 0
const EXIT_FENCED = 3
const EXIT_CRASH = 137

/**
 * Live proof that this client's transactions interoperate with the Apache Kafka Java client's
 * (kafka-clients 4.3): fencing in both directions, the epoch a Java producer takes over at,
 * read_committed reads across the two, recovery from a producer of either client killed
 * mid-transaction, one transactional id passed back and forth, and a record a broker refuses in
 * the middle of a transaction. No case compares how the two clients answer STALE_MEMBER_EPOCH:
 * the Java 4.3 client has no branch for it on a transactional offset commit. Each arm runs only on
 * the cluster whose finalized `transaction.version` it names.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 * @see https://github.com/apache/kafka/blob/4.3.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java
 */
describeJavaTransactions('Producer > transactions with the Java client (live)', () => {
  /**
   * Every partition of a Produce of this client the broker accepted.
   * @type {{ topic: string, producerId: string, producerEpoch: number, firstSequence: number }[]}
   */
  const produced = []
  let cluster
  let kafka
  let clients
  let children

  beforeAll(async () => {
    cluster = await settledTransactionVersion()
    const { produce } = Broker.prototype
    jest.spyOn(Broker.prototype, 'produce').mockImplementation(async function (request) {
      const results = await produce.call(this, request)
      const accepted = (topic, partition) =>
        !Array.isArray(results) ||
        results.some((r) => r.topicName === topic && r.partition === partition && !r.errorCode)
      for (const { topic, partitions } of request.topicData) {
        for (const { partition, firstSequence } of partitions) {
          if (accepted(topic, partition)) {
            produced.push({
              topic,
              producerId: String(request.producerId),
              producerEpoch: request.producerEpoch,
              firstSequence,
            })
          }
        }
      }
      return results
    })
  }, FEATURE_HOOK_BUDGET_MS)

  afterAll(() => {
    jest.restoreAllMocks()
    if (JAVA.owned) {
      fs.rmSync(JAVA.workDir, { recursive: true, force: true })
    }
  })

  beforeEach(() => {
    produced.length = 0
    kafka = new Kafka({
      clientId: `e2e-p7-java-transactions-${secureRandom()}`,
      brokers: plainTextBrokers(),
      retry: RETRY,
      logLevel: logLevel.NOTHING,
    })
    clients = []
    children = []
  })

  afterEach(async () => {
    for (const child of children) {
      child.exitCode == null && child.kill('SIGKILL')
    }
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

  /** A connected transactional producer of this client. */
  const producerOn = (transactionalId) =>
    connected(kafka.producer({ transactionalId, retry: RETRY }))

  /**
   * Starts the Java member with `args`, collecting the JSON line it prints for each step.
   * @param {string[]} args `run.sh` arguments
   * @returns {{ events: object[], exited: Promise<number|null>,
   *   printed: (event: string) => Promise<unknown> }} `exited` resolves with its exit code - null
   *   when it had to be killed on its deadline - and `printed` once it has printed that event, or
   *   exited without printing it
   */
  const startJava = (args) => {
    const events = []
    const waiting = []
    let pending = ''
    const child = spawn(RUN_SH, args, {
      env: { ...process.env, JAVA_E2E_WORK_DIR: JAVA.workDir },
      stdio: ['ignore', 'pipe', 'ignore'],
    })
    children.push(child)
    const deadline = setTimeout(() => child.kill('SIGKILL'), CHILD_DEADLINE_MS)
    child.stdout.setEncoding('utf8')
    child.stdout.on('data', (chunk) => {
      const lines = (pending + chunk).split('\n')
      pending = lines.pop()
      for (const line of lines.filter((l) => l.startsWith('{'))) {
        let event
        try {
          event = JSON.parse(line)
        } catch (e) {
          // A malformed line is left out; throwing inside a stream handler would be uncaught.
          continue
        }
        events.push(event)
        waiting.filter((w) => w.event === event.event).forEach((w) => w.resolve())
      }
    })
    const exited = new Promise((resolve) =>
      child.on('exit', (code) => {
        clearTimeout(deadline)
        resolve(code)
      })
    )
    const printed = (event) =>
      Promise.race([
        events.some((e) => e.event === event)
          ? Promise.resolve()
          : new Promise((resolve) => waiting.push({ event, resolve })),
        exited,
      ])
    return { events, exited, printed }
  }

  /** The Java transactional producer: `transactions` of `RECORDS` records each. */
  const javaProducer = (transactionalId, topic, { transactions = 1, flags = [] } = {}) =>
    startJava([
      '--role',
      'transactional-producer',
      ...flags,
      plainTextBrokers().join(','),
      transactionalId,
      topic,
      String(transactions),
      String(RECORDS),
    ])

  /** Every value the Java read_committed consumer is handed on `topic`, sorted. */
  const javaReadCommitted = async (topic) => {
    const reader = startJava([
      '--role',
      'read-committed-consumer',
      plainTextBrokers().join(','),
      `e2e-p7-java-transactions-reader-${secureRandom()}`,
      topic,
      String(JAVA_READ_MS),
    ])
    await reader.exited
    return reader.events
      .filter(({ event }) => event === 'record')
      .map(({ value }) => value)
      .sort()
  }

  /** Every record this client's read_committed reader sees on `topic`, in offset order. */
  const readCommitted = async (topic, expected) => {
    const reader = await connected(
      kafka.consumer({ groupId: `e2e-p7-java-transactions-reader-${secureRandom()}`, retry: RETRY })
    )
    const messages = []
    await reader.subscribe({ topics: [topic], fromBeginning: true })
    await reader.run({ eachMessage: async ({ message }) => messages.push(message) })
    await waitForCondition(() => messages.length >= expected, WAIT_BUDGET)
    await sleep(QUIET_MS)
    return messages.sort((a, b) => Number(a.offset) - Number(b.offset))
  }

  /** The values `readCommitted` read, sorted. */
  const valuesOf = (messages) => messages.map(({ value }) => value.toString()).sort()

  /** The values one transaction of the Java member writes: `<id>-<transaction>-<index>`. */
  const javaValues = (transactionalId, transaction) =>
    Array.from({ length: RECORDS }, (_, index) => `${transactionalId}-${transaction}-${index}`)

  /** `RECORDS` records of this client with values `<prefix>-<n>`. */
  const records = (prefix) =>
    Array.from({ length: RECORDS }, (_, index) => ({ value: `${prefix}-${index}` }))

  /** A fresh single-partition topic and transactional id for one case. */
  const fresh = async (name) => {
    const topic = `e2e-p7-java-transactions-${name}-${secureRandom()}`
    await createTopic({ topic })
    return { topic, transactionalId: `e2e-p7-java-transactions-${name}-${secureRandom()}` }
  }

  describe.each([
    ['version 2', 2],
    ['version 1', 1],
  ])('transaction %s', (arm, armLevel) => {
    /**
     * Whether this arm runs on the cluster under test; when it does not, the case records why and
     * asserts that the cluster really is the other arm.
     * @param {string} scenario
     * @returns {boolean}
     */
    const runsHere = (scenario) => {
      if (cluster.level === armLevel) {
        return true
      }
      evidence({
        scenario,
        arm,
        skipped: `the cluster finalizes transaction.version ${cluster.level}`,
      })
      expect([1, 2]).toContain(cluster.level)
      return false
    }

    test(
      'a Java producer fences this client, and this client fences a Java producer',
      async () => {
        if (!runsHere('fencing')) return
        const { topic, transactionalId } = await fresh('fencing')

        const fenced = await producerOn(transactionalId)
        const open = await fenced.transaction()
        await open.send({ topic, messages: records('js-fenced') })
        const javaFencing = await javaProducer(transactionalId, topic).exited
        const send = await outcomeOf(open.send({ topic, messages: records('js-late') }))
        const commit = await outcomeOf(open.commit())

        const javaHolding = javaProducer(transactionalId, topic, {
          flags: ['--hold-open-ms', String(HOLD_OPEN_MS)],
        })
        await javaHolding.printed('produced')
        const fencing = await producerOn(transactionalId)
        const transaction = await fencing.transaction()
        await transaction.send({ topic, messages: records('js-fencing') })
        await transaction.commit()
        const javaFenced = await javaHolding.exited
        const values = valuesOf(await readCommitted(topic, RECORDS * 2))

        evidence({
          scenario: 'fencing',
          arm,
          javaFencing,
          send: send.error && { name: send.error.name, code: errorCodeOf(send.error) },
          commit: commit.error && { name: commit.error.name, code: errorCodeOf(commit.error) },
          javaFenced,
          values,
        })
        expect(javaFencing).toBe(EXIT_DONE)
        expect(send.error).toEqual(expect.objectContaining({ category: 'applicationRecoverable' }))
        expect([INVALID_PRODUCER_EPOCH, PRODUCER_FENCED]).toContain(errorCodeOf(send.error))
        expect(commit.error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
        expect(javaFenced).toBe(EXIT_FENCED)
        expect(values).toEqual(
          [...javaValues(transactionalId, 0), ...records('js-fencing').map((r) => r.value)].sort()
        )
      },
      SCENARIO_BUDGET_MS
    )

    test(
      "a Java producer taking over this client's transactional id after five transactions writes at a higher epoch of the same producer id",
      async () => {
        if (!runsHere('epoch')) return
        const { topic, transactionalId } = await fresh('epoch')

        const producer = await producerOn(transactionalId)
        for (let n = 0; n < OWN_TRANSACTIONS; n++) {
          const transaction = await producer.transaction()
          await transaction.send({ topic, messages: records(`js-${n}`) })
          await transaction.commit()
        }
        const ours = produced.at(-1)
        const java = await javaProducer(transactionalId, topic).exited
        const messages = await readCommitted(topic, RECORDS * (OWN_TRANSACTIONS + 1))
        const javaBatch = messages.find(({ value }) =>
          value.toString().startsWith(`${transactionalId}-`)
        ).batchContext

        evidence({
          scenario: 'epoch',
          arm,
          ourEpochs: produced.map(({ producerEpoch }) => producerEpoch),
          java: { code: java, producerId: javaBatch.producerId, epoch: javaBatch.producerEpoch },
        })
        expect(java).toBe(EXIT_DONE)
        expect(javaBatch.producerId).toBe(ours.producerId)
        expect(javaBatch.producerEpoch).toBeGreaterThan(ours.producerEpoch)
        // Version 2 moves the epoch with every transaction; version 1 holds it for all of them.
        expect(new Set(produced.map(({ producerEpoch }) => producerEpoch)).size).toBe(
          armLevel === 2 ? OWN_TRANSACTIONS : 1
        )
      },
      SCENARIO_BUDGET_MS
    )

    test(
      "each client's read_committed reader sees exactly the other client's committed records",
      async () => {
        if (!runsHere('cross-read')) return
        const ours = await fresh('cross-read-js')
        const theirs = await fresh('cross-read-java')

        const producer = await producerOn(ours.transactionalId)
        for (const [n, end] of [
          [0, 'commit'],
          [1, 'abort'],
          [2, 'commit'],
        ]) {
          const transaction = await producer.transaction()
          await transaction.send({ topic: ours.topic, messages: records(`js-${n}`) })
          await transaction[end]()
        }
        const javaRead = await javaReadCommitted(ours.topic)
        const java = await javaProducer(theirs.transactionalId, theirs.topic, {
          transactions: 3,
          flags: ['--abort-every', '2'],
        }).exited
        const ourRead = valuesOf(await readCommitted(theirs.topic, RECORDS * 2))

        evidence({ scenario: 'cross-read', arm, java, javaRead, ourRead })
        expect(javaRead).toEqual(
          [...records('js-0'), ...records('js-2')].map(({ value }) => value).sort()
        )
        expect(java).toBe(EXIT_DONE)
        expect(ourRead).toEqual(
          [
            ...javaValues(theirs.transactionalId, 0),
            ...javaValues(theirs.transactionalId, 2),
          ].sort()
        )
      },
      SCENARIO_BUDGET_MS
    )

    test(
      'a producer of either client killed between Produce and EndTxn is recovered by the other, and its records stay invisible',
      async () => {
        if (!runsHere('restart')) return
        const ours = await fresh('restart-js')
        const theirs = await fresh('restart-java')

        const crashed = await crashAfterProduce({
          ...ours,
          records: RECORDS,
          deadlineMs: CHILD_DEADLINE_MS,
          track: (child) => children.push(child),
        })
        const javaRecovers = await javaProducer(ours.transactionalId, ours.topic).exited
        const javaCrash = await javaProducer(theirs.transactionalId, theirs.topic, {
          flags: ['--crash-after-produce'],
        }).exited
        const recovering = await producerOn(theirs.transactionalId)
        const transaction = await recovering.transaction()
        await transaction.send({ topic: theirs.topic, messages: records('js-recovered') })
        await transaction.commit()
        const afterOurCrash = valuesOf(await readCommitted(ours.topic, RECORDS))
        const afterJavaCrash = valuesOf(await readCommitted(theirs.topic, RECORDS))

        evidence({
          scenario: 'restart',
          arm,
          crashed: crashed.requests,
          javaRecovers,
          javaCrash,
          afterOurCrash,
          afterJavaCrash,
        })
        expect(crashed.requests).toContain('Produce')
        expect(crashed.requests).not.toContain('EndTxn')
        expect(javaRecovers).toBe(EXIT_DONE)
        expect(afterOurCrash).toEqual(javaValues(ours.transactionalId, 0).sort())
        expect(javaCrash).toBe(EXIT_CRASH)
        expect(afterJavaCrash).toEqual(
          records('js-recovered')
            .map(({ value }) => value)
            .sort()
        )
      },
      SCENARIO_BUDGET_MS
    )

    test(
      'the two clients take turns on one transactional id: every commit is visible in order and every abort is not',
      async () => {
        if (!runsHere('alternation')) return
        const { topic, transactionalId } = await fresh('alternation')

        const turns = []
        const javaTurn = async (end) => {
          const flags = end === 'abort' ? ['--abort-every', '1'] : []
          const code = await javaProducer(transactionalId, topic, { flags }).exited
          turns.push({ client: 'java', end, code })
          return end === 'commit' ? javaValues(transactionalId, 0) : []
        }
        const ourTurn = async (end, n) => {
          const producer = await producerOn(transactionalId)
          const transaction = await producer.transaction()
          await transaction.send({ topic, messages: records(`js-${n}`) })
          await transaction[end]()
          turns.push({ client: 'kafkajs', end })
          return end === 'commit' ? records(`js-${n}`).map(({ value }) => value) : []
        }
        const expected = [
          ...(await javaTurn('commit')),
          ...(await ourTurn('commit', 1)),
          ...(await javaTurn('abort')),
          ...(await ourTurn('abort', 3)),
          ...(await ourTurn('commit', 4)),
        ]
        const read = (await readCommitted(topic, expected.length)).map(({ value }) =>
          value.toString()
        )

        evidence({ scenario: 'alternation', arm, turns, read })
        expect(turns.filter(({ client }) => client === 'java').map(({ code }) => code)).toEqual([
          EXIT_DONE,
          EXIT_DONE,
        ])
        expect(read).toEqual(expected)
      },
      SCENARIO_BUDGET_MS
    )

    test(
      'a record the broker refuses fails its own batch only: an idempotent producer moves to a new epoch and carries on, and inside a transaction the other partitions keep their batches and sequences and the transaction becomes abort-only',
      async () => {
        if (!runsHere('invalid-record')) return
        const topic = `e2e-p7-java-transactions-valid-${secureRandom()}`
        const compacted = `e2e-p7-java-transactions-compacted-${secureRandom()}`
        await createTopic({ topic, partitions: 2 })
        await createTopic({
          topic: compacted,
          config: [{ name: 'cleanup.policy', value: 'compact' }],
        })
        // One sendBatch per producer: one partition pass, so every batch is queued, numbered and
        // sent before the refusal is answered.
        const batch = (value) => ({
          topicMessages: [
            { topic: compacted, messages: [{ value: 'no-key' }] },
            { topic, messages: [0, 1].map((partition) => ({ partition, value })) },
          ],
        })

        const idempotent = await connected(kafka.producer({ retry: RETRY }))
        const refused = await outcomeOf(idempotent.sendBatch(batch('first')))
        await idempotent.send({
          topic,
          messages: [0, 1].map((partition) => ({ partition, value: 'second' })),
        })
        const valid = produced.filter((p) => p.topic === topic)
        // Each accepted batch as [epochs past the first, first sequence].
        const stamps = valid.map((p) => [p.producerEpoch - valid[0].producerEpoch, p.firstSequence])

        const transactional = await producerOn(`e2e-p7-java-transactions-invalid-${secureRandom()}`)
        const transaction = await transactional.transaction()
        const before = produced.length
        const refusedInside = await outcomeOf(transaction.sendBatch(batch('inside')))
        const inside = produced
          .slice(before)
          .filter((p) => p.topic === topic)
          .map(({ firstSequence }) => firstSequence)
        const commit = await outcomeOf(transaction.commit())
        await transaction.abort()
        const next = await transactional.transaction()
        await next.send({
          topic,
          messages: [0, 1].map((partition) => ({ partition, value: 'after' })),
        })
        await next.commit()
        const values = valuesOf(await readCommitted(topic, 6))

        evidence({
          scenario: 'invalid-record',
          arm,
          refused: errorCodeOf(refused.error),
          stamps,
          refusedInside: errorCodeOf(refusedInside.error),
          inside,
          commit: commit.error && commit.error.name,
          values,
        })
        expect(errorCodeOf(refused.error)).toBe(INVALID_RECORD)
        // The failed batch moves an idempotent producer to a new epoch, which restarts every
        // partition at sequence 0 once its batches have landed, as the Java producer does.
        expect(stamps).toEqual([
          [0, 0],
          [0, 0],
          [1, 0],
          [1, 0],
        ])
        expect(errorCodeOf(refusedInside.error)).toBe(INVALID_RECORD)
        // The broker accepted both partitions' batches of the transaction at sequence 0.
        expect(inside).toEqual([0, 0])
        expect(commit.error).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(values).toEqual(['after', 'after', 'first', 'first', 'second', 'second'])
      },
      SCENARIO_BUDGET_MS
    )
  })
})
