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
const { Kafka, logLevel } = require('../../../index')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

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

  describe.each([5, 2])('maxInFlightRequests: %d', (maxInFlightRequests) => {
    test(
      'the producer keeps 5 requests unanswered per broker, the wire carries up to the in-flight limit, a partition has one batch in flight, and every record lands once, in order',
      async () => {
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
      },
      SCENARIO_BUDGET_MS
    )
  })

  test(
    'neither an idempotent nor a transactional producer constructs a lock, and neither emits LOCK_TIMEOUT',
    async () => {
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
    },
    SCENARIO_BUDGET_MS
  )
})
