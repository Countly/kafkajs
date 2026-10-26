jest.mock('../../utils/lock', () => {
  const Lock = jest.requireActual('../../utils/lock')
  return jest.fn(function (...args) {
    mockLockConstructions.push(new Error().stack)
    return new Lock(...args)
  })
})

const path = require('path')
const Broker = require('../../broker')
const RequestQueue = require('../../network/requestQueue')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const apiKeys = require('../../protocol/requests/apiKeys')
const { Kafka, logLevel } = require('../../../index')
const { committedRecords, readLog } = require('../../../testHelpers/rawLog')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { sleep, waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
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
  ProducerProbe,
  readUntilCovered,
  sequenceGaps,
} = require('./fixtures/liveProbe')
const { CaseTimeline, JOIN_RESERVE_MS } = require('./fixtures/replicaOutage')

/** Every stack a `Lock` was constructed from, in this process. */
const mockLockConstructions = []

// Gated: each arm sends thousands of records. Run alone: `npm run test:e2e:idempotent-pipelining`.
const describePipelining = process.env.IDEMPOTENT_PIPELINING_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'idempotent-pipelining'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 90000
const HOOK_BUDGET_MS = 60000
const DISCONNECT_BUDGET_MS = 15000
const WAIT_BUDGET = { attempts: 240, delay: 250 } // 60 s
const SENDS = 2000
// Seven, seven and six partitions per broker on the three-broker cluster: more than the five
// requests the producer keeps unanswered to one broker, so that cap is what binds.
const PARTITIONS = 20
// The same-broker merge case: six partitions over three brokers, a window much longer than the
// millisecond the sends are apart.
const MERGE_PARTITIONS = 6
const MERGE_LINGER_MS = 50
// A partition's records carry their own request timeout, so the batches of different partitions
// never share a Produce request and every partition's batch is a request of its own.
const BASE_TIMEOUT = 30000
// The most Produce requests the producer keeps unanswered to one broker.
const MAX_REQUESTS_PER_BROKER = 5
// The only files that may construct a `Lock`: the cluster's and the brokers', as a stack frame
// names them (`<file>:<line>`).
const LOCK_OWNERS = ['cluster', 'broker'].map((dir) => path.join('src', dir, 'index.js') + ':')
// A cold cluster creates the transaction state topic on the first transactional InitProducerId and
// answers with a retriable error until it is ready: 10 retries, 100 ms doubling to a 1 s cap, about
// 7.5 s in all.
const RETRY = { retries: 10, initialRetryTime: 100, multiplier: 2, maxRetryTime: 1000 }

// The transactional arm: six partitions over three brokers, so some broker leads at least two.
const ARM_PARTITIONS = 6
// One send per partition, awaited before anything is held, registers every partition with the
// transaction (version 1 adds a partition before its first Produce); then this many are offered at
// once.
const ARM_HELD_SENDS = 24
const ARM_SENDS = ARM_PARTITIONS + ARM_HELD_SENDS
// Each answer is held this long, so a second Produce request that was not held back is seen on the
// wire while the first is unanswered.
const HOLD_MS = 300
// The queue frees a slot and sends the next request before it stamps the answered request's
// duration, so intervals of a connection kept at its limit can overlap by a millisecond or two.
const SLOT_SLACK_MS = 5
const HOLD_TOLERANCE_MS = 50
const ARM_BUDGET_MS = 150000
const ARM_PHASE_BUDGET_MS = Object.freeze({
  topics: 20000,
  control: 20000,
  serial: 25000,
  verify: 15000,
  probe: 40000,
  cleanup: 15000,
})
const ARM_STEP_MS = 10000
const ARM_READ_BUDGET = { attempts: 40, delay: 250 }
// On the transaction version 1 arm the level is waited for, up to 120 s (the shared fixture).
const FEATURE_HOOK_BUDGET_MS = 150000
const ARM_BUFFER_MEMORY = 1048576
const ARM_MAX_REQUEST_SIZE = 1048576
const ARM_DELIVERY_BUDGET_MS = 30000
// The ids a partition holds, in order: its registration send, then the held ones.
const ARM_COMMITTED = Array.from({ length: ARM_PARTITIONS }, (_, partition) => ({
  partition,
  ids: Array.from({ length: ARM_SENDS / ARM_PARTITIONS }, (_, n) => partition + n * ARM_PARTITIONS),
}))

/**
 * The most intervals that overlap at any instant. Two intervals overlap when one starts before
 * the other ends.
 * @param {{ start: number, end: number }[]} intervals
 * @returns {number}
 */
const maxOverlap = (intervals) => {
  const edges = intervals.flatMap(({ start, end }) => [
    [start, 1],
    [end, -1],
  ])
  edges.sort((a, b) => a[0] - b[0] || a[1] - b[1])
  let open = 0
  let most = 0
  for (const [, step] of edges) {
    open += step
    most = Math.max(most, open)
  }
  return most
}

/**
 * The source line a `Lock` was constructed at: the first frame of its stack outside this file and
 * outside `node_modules`.
 * @param {string} stack
 * @returns {string}
 */
const constructionSite = (stack) =>
  stack
    .split('\n')
    .slice(1)
    .find((frame) => !frame.includes('node_modules') && !frame.includes(path.basename(__filename)))

/**
 * The gaps between the Produce frames one connection wrote, which are at least the hold when each
 * request waited for the answer of the one before it.
 * @param {Array<{ endpoint: string, connection: number, at: number }>} frames in the order written
 * @returns {number[]} ms, in the order of the connections' frames
 */
const frameGaps = (frames) => {
  const byConnection = new Map()
  for (const { endpoint, connection, at } of frames) {
    const key = `${endpoint}#${connection}`
    byConnection.set(key, [...(byConnection.get(key) ?? []), at])
  }
  return [...byConnection.values()].flatMap((times) =>
    times.slice(1).map((at, index) => at - times[index])
  )
}

/**
 * One transactional producer sending one transaction whose Produce answers are held. Every
 * partition is registered with the transaction first, then many sends are offered at once while
 * every answer is held, so that a producer that let a second Produce request to a broker go before
 * the first was answered shows it on the wire. What it sees is the file's own `RequestQueue` peak,
 * the `REQUEST` events of the answers and the frames the sockets wrote; the produce ledger is not
 * used, because its `Broker#produce` spy would collide with the file's.
 */
class HeldTransaction {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses
   * @param {string} options.name the client id and transactional id
   * @param {number} options.maxInFlightRequests the producer's limit
   * @param {string} options.topic the six-partition workload topic
   * @param {string} options.warmTopic where the warm-up transaction writes
   * @param {Map<string, number>} options.peaks the file's most Produce requests one connection had
   *   in flight, by broker address, which this run clears when its held phase begins
   * @param {CleanupStack} options.cleanup receives the producer, the hold and the probe
   * @param {boolean} options.probed whether a closing admission probe is installed
   */
  constructor({ brokers, name, maxInFlightRequests, topic, warmTopic, peaks, cleanup, probed }) {
    Object.assign(this, { brokers, name, maxInFlightRequests, topic, warmTopic, peaks, cleanup })
    this.probed = probed
    this.sockets = createLiveSockets()
    /** @type {Map<string, Array<{ start: number, end: number }>>} Produce answers by broker */
    this.answers = new Map()
    this.producer = null
    this.probe = null
    this.disarm = null
    this.mark = 0
  }

  /**
   * Connects the producer and commits one warm-up transaction, under the probe's capture when
   * there is a probe.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   */
  async open(signal) {
    const { name, brokers } = this
    const producer = new Kafka({
      clientId: name,
      brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.sockets.socketFactory,
    }).producer({
      transactionalId: name,
      maxInFlightRequests: this.maxInFlightRequests,
      retry: RETRY,
      bufferMemory: ARM_BUFFER_MEMORY,
      maxRequestSize: ARM_MAX_REQUEST_SIZE,
    })
    producer.on(producer.events.REQUEST, ({ payload }) => {
      if (payload.apiName !== 'Produce') return
      const answers = this.answers.get(payload.broker) ?? []
      answers.push({ start: payload.sentAt, end: payload.sentAt + payload.duration })
      this.answers.set(payload.broker, answers)
    })
    this.producer = producer
    if (this.probed) {
      this.probe = new ProducerProbe({
        producer,
        frames: this.sockets.frames,
        clientId: name,
        transactionalId: name,
        maxRequestSize: ARM_MAX_REQUEST_SIZE,
        brokers,
      }).install()
      this.cleanup.add(`${name} admission probe`, () => this.probe.restore())
      this.cleanup.add(`${name} resources after disconnect`, (scope) =>
        this.probe.settled({ signal: scope })
      )
    }
    this.cleanup.add(`${name} producer`, () =>
      producer.disconnect({ timeout: DISCONNECT_BUDGET_MS })
    )
    this.cleanup.add(`${name} hold`, () => this.release())
    await bounded(signal, ARM_STEP_MS, () => producer.connect(), { label: 'The producer connect' })
    const warm = async () => {
      const transaction = await producer.transaction()
      await transaction.send({ topic: this.warmTopic, messages: [{ value: 'warm' }], acks: -1 })
      await transaction.commit()
    }
    await bounded(
      signal,
      ARM_STEP_MS,
      (scope) => (this.probe ? this.probe.capture(warm, { signal: scope }) : warm()),
      { label: 'The warm-up transaction' }
    )
  }

  /**
   * Lets the answers through again: later sockets are not held and open ones are not delayed.
   * @returns {void}
   */
  release() {
    this.disarm?.()
    this.disarm = null
    for (const socket of this.sockets.chaos.sockets) socket.delay({})
  }

  /**
   * One transaction: every partition registered, then the held sends offered at once, then the
   * answers released and the commit.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<void>}
   */
  async transact(signal) {
    const { topic } = this
    const step = (op, label) => bounded(signal, ARM_STEP_MS, op, { label })
    const transaction = await step(() => this.producer.transaction(), 'The transaction')
    for (let id = 0; id < ARM_PARTITIONS; id++) {
      await step(
        () =>
          transaction.send({
            topic,
            messages: [{ partition: id % ARM_PARTITIONS, value: String(id) }],
            acks: -1,
          }),
        `The registration send ${id}`
      )
    }
    this.peaks.clear()
    this.answers.clear()
    this.mark = this.sockets.frames.mark()
    this.disarm = this.sockets.chaos.arm({}, (socket) => socket.delay({ read: HOLD_MS }))
    const offered = Array.from({ length: ARM_HELD_SENDS }, (_, index) => ARM_PARTITIONS + index)
    await step(
      () =>
        Promise.all(
          offered.map((id) =>
            transaction.send({
              topic,
              timeout: BASE_TIMEOUT + (id % ARM_PARTITIONS),
              messages: [{ partition: id % ARM_PARTITIONS, value: String(id) }],
              acks: -1,
            })
          )
        ),
      'The held sends'
    )
    this.release()
    await step(() => transaction.commit(), 'The commit')
  }

  /**
   * @returns {number} the most Produce requests the file's queue saw in flight on one connection
   *   since the held phase began; 0 when none was sent
   */
  peak() {
    return this.peaks.size === 0 ? 0 : Math.max(...this.peaks.values())
  }

  /**
   * What the held phase looked like on the wire.
   * @returns {{ requests: number, maxConcurrentProduce: number, peak: number,
   *   minAnswerMs: number|null, frameGaps: number[] }} `requests` the Produce frames written;
   *   `maxConcurrentProduce` the most answers' intervals that overlap on one broker, less the slack;
   *   `minAnswerMs` the shortest answer, which the hold must have stretched
   */
  measures() {
    const durations = [...this.answers.values()].flat().map(({ start, end }) => end - start)
    return {
      requests: this.sockets.frames.since(apiKeys.Produce, this.mark).length,
      maxConcurrentProduce: Math.max(
        0,
        ...[...this.answers.values()].map((intervals) =>
          maxOverlap(intervals.map(({ start, end }) => ({ start, end: end - SLOT_SLACK_MS })))
        )
      ),
      peak: this.peak(),
      minAnswerMs: durations.length === 0 ? null : Math.min(...durations),
      frameGaps: frameGaps(this.sockets.frames.since(apiKeys.Produce, this.mark)),
    }
  }

  /**
   * Reads the committed records of the workload topic and checks the idempotent sequences.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<{ byPartition: Array<{ partition: number, ids: number[] }>, gaps: object[] }>}
   */
  async committed(signal) {
    const log = await readUntilCovered(
      signal,
      () =>
        readLog({
          brokers: this.brokers,
          topic: this.topic,
          isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
        }),
      (answer) =>
        [...answer.values()].reduce((count, part) => count + committedRecords(part).length, 0) >=
        ARM_SENDS,
      ARM_READ_BUDGET
    )
    return {
      byPartition: [...log.entries()].map(([partition, part]) => ({
        partition,
        ids: committedRecords(part).map(({ value }) => Number(value.toString())),
      })),
      gaps: [...log.values()].flatMap(({ records }) => sequenceGaps(records)),
    }
  }

  /**
   * The closing admission probe on this producer, in a fresh transaction with its records on a
   * topic of their own.
   * @param {AbortSignal} signal the phase's scope
   * @returns {Promise<object>} what `ProducerProbe#closing` observed
   */
  async closing(signal) {
    const sender = await bounded(signal, ARM_STEP_MS, () => this.producer.transaction(), {
      label: 'The probe transaction',
    })
    return this.probe.closing({
      sender,
      topic: `${this.topic}-probe`,
      finish: () => sender.commit(),
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      budgetMs: ARM_DELIVERY_BUDGET_MS,
      signal,
    })
  }
}

/**
 * The acceptance of the arm: the held phase had at least two Produce requests that each waited for
 * the answer before it, on the wire and in the queue, the control with the default limit overlapped
 * (so the hold can show an overlap), every committed id is once and in order per partition with no
 * sequence conflict, and the closing probe ran on the same producer with the limit still holding.
 * @param {object} result
 * @param {object} result.serial the limit-one producer's `HeldTransaction#measures`
 * @param {object} result.control the default-limit producer's measures
 * @param {{ byPartition: object[], gaps: object[] }} result.committed
 * @param {number} result.peakAfterProbe `HeldTransaction#peak` once the probe finished
 * @param {object} [result.admissionProbe] what `ProducerProbe#closing` observed
 * @returns {void}
 * @throws {Error} through a failed expectation, naming the first invariant that does not hold
 */
const acceptOneInFlight = ({ serial, control, committed, peakAfterProbe, admissionProbe }) => {
  expect(serial.requests).toBeGreaterThanOrEqual(2)
  expect(serial.maxConcurrentProduce).toBe(1)
  expect(serial.peak).toBe(1)
  expect(peakAfterProbe).toBe(1)
  expect(serial.minAnswerMs).toBeGreaterThanOrEqual(HOLD_MS - HOLD_TOLERANCE_MS)
  expect(serial.frameGaps.length).toBeGreaterThan(0)
  expect(Math.min(...serial.frameGaps)).toBeGreaterThanOrEqual(HOLD_MS - HOLD_TOLERANCE_MS)
  expect(control.peak).toBeGreaterThanOrEqual(2)
  expect(control.maxConcurrentProduce).toBeGreaterThanOrEqual(2)
  expect(committed.byPartition).toEqual(ARM_COMMITTED)
  expect(committed.gaps).toEqual([])
  expect(admissionProbe).toEqual(
    expect.objectContaining({
      records: expect.any(Number),
      observed: expect.objectContaining({ inEntry: true, waitingAfterEntry: 0 }),
    })
  )
}

/**
 * Live proof that an idempotent producer pipelines the batches of different partitions to a
 * broker - at most 5 Produce requests unanswered per broker, at most `maxInFlightRequests` on the
 * wire - while keeping one batch in flight per partition and every partition's records in order
 * (KIP-98 sequencing with no per-broker lock), and that neither an idempotent nor a transactional
 * producer constructs a lock. The `REQUEST` event carries no partition and counts only what reached
 * the wire, so the producer's own outstanding requests and their partitions are observed on
 * `Broker.prototype.produce`. The wire limit is counted on the connection's own in-flight set at
 * the moment each request is sent: a request queue frees a slot and sends the next request before
 * it stamps the answered request's duration, so the `sentAt` .. `sentAt + duration` intervals of a
 * connection kept at its limit can overlap by a millisecond more than its requests do.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
 */
describePipelining('Producer > idempotent pipelining (live)', () => {
  /** @type {{ nodeId: number, partitions: number[], start: number, end: number }[]} */
  const produceCalls = []
  /** @type {Map<string, number>} the most Produce requests one connection had in flight, by broker address */
  const inFlightPeaks = new Map()
  let kafka
  let clients

  beforeAll(() => {
    const { produce } = Broker.prototype
    jest.spyOn(Broker.prototype, 'produce').mockImplementation(async function (request) {
      const call = {
        nodeId: this.nodeId,
        partitions: request.topicData.flatMap(({ partitions }) =>
          partitions.map(({ partition }) => partition)
        ),
        start: Date.now(),
        end: Infinity,
      }
      produceCalls.push(call)
      try {
        return await produce.call(this, request)
      } finally {
        call.end = Date.now()
      }
    })
    const { sendSocketRequest } = RequestQueue.prototype
    jest.spyOn(RequestQueue.prototype, 'sendSocketRequest').mockImplementation(function (request) {
      sendSocketRequest.call(this, request)
      if (request.entry.apiName === 'Produce') {
        const inFlight = [...this.inflight.values()].filter(
          ({ entry }) => entry.apiName === 'Produce'
        ).length
        inFlightPeaks.set(this.broker, Math.max(inFlightPeaks.get(this.broker) ?? 0, inFlight))
      }
    })
  })

  afterAll(() => {
    jest.restoreAllMocks()
  })

  beforeEach(() => {
    produceCalls.length = 0
    inFlightPeaks.clear()
    kafka = new Kafka({
      clientId: `e2e-p7-idempotent-pipelining-${secureRandom()}`,
      brokers: plainTextBrokers(),
      retry: RETRY,
      logLevel: logLevel.NOTHING,
    })
    clients = []
  })

  // Only a case the gate admitted gets this: a refused one started nothing. Every disconnect is
  // bounded, and one that fails or runs out of time fails the hook and taints the run.
  afterEach(async () => {
    if (!liveRun.takeAdmission()) return
    const teardown = new CleanupStack()
    for (const client of clients) teardown.add('client disconnect', () => client.disconnect())
    await teardown.run(async () => {}, { budgetMs: HOOK_BUDGET_MS - CASE_JOIN_MS })
  }, HOOK_BUDGET_MS)

  /** A connected client of `kafka`, disconnected after the test. */
  const connected = async (client) => {
    clients.push(client)
    await client.connect()
    return client
  }

  /** Every record of `topic`, by partition, in offset order. */
  const readBack = async (topic) => {
    const consumer = await connected(
      kafka.consumer({ groupId: `e2e-p7-idempotent-pipelining-reader-${secureRandom()}` })
    )
    const byPartition = Array.from({ length: PARTITIONS }, () => [])
    let count = 0
    await consumer.subscribe({ topics: [topic], fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        byPartition[partition].push(Number(message.value.toString()))
        count += 1
      },
    })
    await waitForCondition(() => count >= SENDS, WAIT_BUDGET)
    await consumer.stop()
    return byPartition
  }

  /** The values of the first `count` records of `topic`, in the order they arrive. */
  const readAll = async (topic, count) => {
    const consumer = await connected(
      kafka.consumer({ groupId: `e2e-p8-same-broker-merge-reader-${secureRandom()}` })
    )
    const values = []
    await consumer.subscribe({ topics: [topic], fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ message }) => {
        values.push(message.value.toString())
      },
    })
    await waitForCondition(() => values.length >= count, WAIT_BUDGET)
    await consumer.stop()
    return values
  }

  describe.each([5, 2])('maxInFlightRequests: %d', (maxInFlightRequests) => {
    test(
      'the producer keeps 5 requests unanswered per broker, the wire carries up to the in-flight limit, a partition has one batch in flight, and every record lands once, in order',
      liveCase(SCENARIO_BUDGET_MS - CASE_JOIN_MS, async () => {
        const topic = `e2e-p7-idempotent-pipelining-${secureRandom()}`
        await createTopic({ topic, partitions: PARTITIONS, replicas: 3 })
        const admin = await connected(kafka.admin())
        const {
          topics: [{ partitions: layout }],
        } = await admin.fetchTopicMetadata({ topics: [topic] })
        const { brokers: nodes } = await admin.describeCluster()
        const producer = await connected(kafka.producer({ maxInFlightRequests }))
        /** @type {Map<string, { start: number, end: number }[]>} */
        const wire = new Map()
        let lockTimeouts = 0
        producer.on(producer.events.REQUEST, ({ payload }) => {
          if (payload.apiName === 'Produce') {
            const intervals = wire.get(payload.broker) || []
            intervals.push({ start: payload.sentAt, end: payload.sentAt + payload.duration })
            wire.set(payload.broker, intervals)
          }
        })
        producer.on(producer.events.LOCK_TIMEOUT, () => (lockTimeouts += 1))

        await Promise.all(
          Array.from({ length: SENDS }, (_, index) =>
            producer.send({
              topic,
              timeout: BASE_TIMEOUT + (index % PARTITIONS),
              messages: [{ partition: index % PARTITIONS, value: String(index) }],
            })
          )
        )
        const written = await readBack(topic)

        const brokers = nodes.map(({ nodeId, host, port }) => {
          const intervals = wire.get(`${host}:${port}`) || []
          return {
            nodeId,
            partitionsLed: layout.filter(({ leader }) => leader === nodeId).length,
            outstanding: maxOverlap(produceCalls.filter((call) => call.nodeId === nodeId)),
            inFlight: inFlightPeaks.get(`${host}:${port}`) ?? 0,
            onTheWire: maxOverlap(intervals),
            intervals,
          }
        })
        const partitionOverlaps = Array.from({ length: PARTITIONS }, (_, partition) =>
          maxOverlap(produceCalls.filter((call) => call.partitions.includes(partition)))
        )
        writeEvidence(EVIDENCE, {
          scenario: 'pipelining',
          maxInFlightRequests,
          brokers,
          partitionOverlaps,
          lockTimeouts,
          records: written.flat().length,
        })

        for (const broker of brokers) {
          expect(broker.partitionsLed).toBeGreaterThan(MAX_REQUESTS_PER_BROKER)
          expect(broker.outstanding).toBe(MAX_REQUESTS_PER_BROKER)
          expect(broker.inFlight).toBe(maxInFlightRequests)
          expect(broker.onTheWire).toBeGreaterThanOrEqual(maxInFlightRequests)
          expect(broker.onTheWire).toBeLessThanOrEqual(maxInFlightRequests + 1)
        }
        expect(partitionOverlaps).toEqual(Array(PARTITIONS).fill(1))
        written.forEach((values, partition) => {
          expect(values).toEqual(
            Array.from({ length: SENDS / PARTITIONS }, (_, n) => partition + n * PARTITIONS)
          )
        })
        expect(lockTimeouts).toBe(0)
      }),
      SCENARIO_BUDGET_MS
    )
  })

  test(
    'sends to partitions of one broker that fall due a millisecond apart reach it in one Produce request',
    liveCase(SCENARIO_BUDGET_MS - CASE_JOIN_MS, async () => {
      const topic = `e2e-p8-same-broker-merge-${secureRandom()}`
      await createTopic({ topic, partitions: MERGE_PARTITIONS, replicas: 3 })
      const admin = await connected(kafka.admin())
      const {
        topics: [{ partitions: layout }],
      } = await admin.fetchTopicMetadata({ topics: [topic] })
      const leaders = layout
        .slice()
        .sort((a, b) => a.partitionId - b.partitionId)
        .map(({ leader }) => leader)
      const producer = await connected(kafka.producer({ linger: MERGE_LINGER_MS }))
      let produceRequests = 0
      let measuring = false
      producer.on(producer.events.REQUEST, ({ payload }) => {
        if (measuring && payload.apiName === 'Produce') {
          produceRequests += 1
        }
      })

      // One awaited record per partition puts the metadata and the producer id in place, so the
      // measured sends are the only ones whose timers run.
      for (let partition = 0; partition < MERGE_PARTITIONS; partition += 1) {
        await producer.send({ topic, messages: [{ partition, value: `warm-${partition}` }] })
      }
      measuring = true
      const sends = []
      for (let partition = 0; partition < MERGE_PARTITIONS; partition += 1) {
        sends.push(producer.send({ topic, messages: [{ partition, value: `sent-${partition}` }] }))
        await sleep(1)
      }
      await Promise.all(sends)
      measuring = false
      const written = await readAll(topic, 2 * MERGE_PARTITIONS)

      writeEvidence(EVIDENCE, {
        scenario: 'same-broker-merge',
        leaders,
        distinctLeaders: new Set(leaders).size,
        produceRequests,
        written,
      })
      expect(produceRequests).toBeGreaterThan(0)
      expect(produceRequests).toBeLessThanOrEqual(new Set(leaders).size)
      expect(written.sort()).toEqual(
        Array.from({ length: MERGE_PARTITIONS }, (_, partition) => [
          `sent-${partition}`,
          `warm-${partition}`,
        ])
          .flat()
          .sort()
      )
    }),
    SCENARIO_BUDGET_MS
  )

  test(
    'neither an idempotent nor a transactional producer constructs a lock, and neither emits LOCK_TIMEOUT',
    liveCase(SCENARIO_BUDGET_MS - CASE_JOIN_MS, async () => {
      const topic = `e2e-p7-idempotent-pipelining-lock-${secureRandom()}`
      await createTopic({ topic, partitions: 3 })
      mockLockConstructions.length = 0
      const idempotent = await connected(kafka.producer())
      const transactional = await connected(
        kafka.producer({ transactionalId: `e2e-p7-idempotent-pipelining-${secureRandom()}` })
      )
      let lockTimeouts = 0
      for (const producer of [idempotent, transactional]) {
        producer.on(producer.events.LOCK_TIMEOUT, () => (lockTimeouts += 1))
      }

      const messages = Array.from({ length: 30 }, (_, index) => ({
        partition: index % 3,
        value: String(index),
      }))
      await Promise.all(messages.map((message) => idempotent.send({ topic, messages: [message] })))
      const transaction = await transactional.transaction()
      await Promise.all(messages.map((message) => transaction.send({ topic, messages: [message] })))
      await transaction.commit()

      const sites = mockLockConstructions.map(constructionSite)
      writeEvidence(EVIDENCE, { scenario: 'no-lock', lockConstructionSites: sites, lockTimeouts })
      expect(sites.length).toBeGreaterThan(0)
      expect(sites.filter((site) => !LOCK_OWNERS.some((owner) => site.includes(owner)))).toEqual([])
      expect(lockTimeouts).toBe(0)
    }),
    SCENARIO_BUDGET_MS
  )

  describe('transactions', () => {
    /** @type {{ level: number, epoch: number }} the cluster's finalized transaction version */
    let cluster

    beforeAll(async () => {
      liveRun.assertClean()
      cluster = await bounded(
        NEVER,
        FEATURE_HOOK_BUDGET_MS - CASE_JOIN_MS,
        () => settledTransactionVersion(),
        { label: 'The setup' }
      )
    }, FEATURE_HOOK_BUDGET_MS)

    // Replay sets: the log holds every committed id once and in order per partition; committed
    // offsets and handler deliveries do not apply. The control runs the same transaction with the
    // default limit of five: the held answers must then show an overlap, or they could not have
    // shown one for the limit of one.
    test(
      'serializes transactional produce requests with one in flight',
      liveCase(ARM_BUDGET_MS, async (scope) => {
        const id = secureRandom()
        const brokers = plainTextBrokers()
        const topics = {
          control: `e2e-p9b-one-in-flight-control-${id}`,
          serial: `e2e-p9b-one-in-flight-${id}`,
          warm: `e2e-p9b-one-in-flight-warm-${id}`,
        }
        const timeline = new CaseTimeline({
          phases: ARM_PHASE_BUDGET_MS,
          limitMs: ARM_BUDGET_MS,
          reserveMs: JOIN_RESERVE_MS,
        })
        const cleanup = new CleanupStack()
        const run = (name, maxInFlightRequests, topic, probed) =>
          new HeldTransaction({
            brokers,
            name: `e2e-p9b-one-in-flight-${name}-${id}`,
            maxInFlightRequests,
            topic,
            warmTopic: topics.warm,
            peaks: inFlightPeaks,
            cleanup,
            probed,
          })

        await cleanup.run(
          async () => {
            await timeline.run(
              'topics',
              () =>
                Promise.all([
                  createTopic({ topic: topics.control, partitions: ARM_PARTITIONS, replicas: 3 }),
                  createTopic({ topic: topics.serial, partitions: ARM_PARTITIONS, replicas: 3 }),
                  createTopic({ topic: topics.warm, partitions: 1, replicas: 3 }),
                ]),
              scope
            )
            const control = run('control', 5, topics.control, false)
            const controlMeasures = await timeline.run(
              'control',
              async (signal) => {
                await control.open(signal)
                await control.transact(signal)
                return control.measures()
              },
              scope
            )
            writeEvidence(EVIDENCE, {
              step: 'control',
              maxInFlightRequests: 5,
              transactionVersion: cluster.level,
              holdMs: HOLD_MS,
              ...controlMeasures,
            })
            const serial = run('serial', 1, topics.serial, true)
            const serialMeasures = await timeline.run(
              'serial',
              async (signal) => {
                await serial.open(signal)
                await serial.transact(signal)
                return serial.measures()
              },
              scope
            )
            const committed = await timeline.run(
              'verify',
              (signal) => serial.committed(signal),
              scope
            )
            writeEvidence(EVIDENCE, {
              step: 'transactional-one-in-flight',
              transactionVersion: cluster.level,
              maxConcurrentProduce: serialMeasures.maxConcurrentProduce,
              requests: serialMeasures.requests,
              committed: committed.byPartition,
              measures: serialMeasures,
              control: { maxInFlightRequests: 5, ...controlMeasures },
              sequenceGaps: committed.gaps,
              holdMs: HOLD_MS,
            })
            const admissionProbe = await timeline.run(
              'probe',
              (signal) => serial.closing(signal),
              scope
            )
            writeEvidence(EVIDENCE, { step: 'probe', closing: admissionProbe })

            expect([1, 2]).toContain(cluster.level)
            acceptOneInFlight({
              serial: serialMeasures,
              control: controlMeasures,
              committed,
              peakAfterProbe: serial.peak(),
              admissionProbe,
            })
          },
          { budgetMs: ARM_PHASE_BUDGET_MS.cleanup }
        )
      }),
      caseTimeout(ARM_BUDGET_MS)
    )
  })
})
