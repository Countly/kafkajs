const { Kafka, logLevel } = require('../../../index')
const {
  createTopic,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { readLog } = require('../../../testHelpers/rawLog')
const { sleep, waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const { createLiveSockets, repliesOf } = require('./fixtures/liveWire')
const {
  CleanupStack,
  ProducerProbe,
  dataRecords,
  describeError,
  readData,
  settle,
} = require('./fixtures/liveProbe')
const { caseTimeout, liveCase } = require('./fixtures/bounded')

// Gated: it deletes, grows and recreates topics under a producer. Run alone:
// `npm run test:e2e:topic-lifecycle-produce`.
const describeLifecycle = process.env.TOPIC_LIFECYCLE_PRODUCE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'topic-lifecycle-produce'
const CASE_BUDGET_MS = 120000
const REQUEST_TIMEOUT_MS = 1000
const DELIVERY_TIMEOUT_MS = 10000
const FAILED_SEND_BOUND_MS = 11000
const MAX_REQUEST_SIZE = 1048576
const METADATA_MAX_IDLE_MS = 5000
const METADATA_MAX_AGE_MS = 1000
const DISCONNECT_BUDGET_MS = 15000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const SEND_INTERVAL_MS = 50
const SENDS_AROUND_DELETE = 60
const ACKED_BEFORE_DELETE = 8
const GROWTH_SENDS = 30
const GROWTH_RECORDS_PER_SEND = 10
const MARKERS = 10
const IN_FLIGHT = 5
const UNKNOWN_TOPIC = new Set(['UNKNOWN_TOPIC_OR_PARTITION', 'UNKNOWN_TOPIC_ID'])

const REPLAY = Object.freeze({
  log: 'acknowledged records of the live incarnation once; a failed send may be absent or present once',
  offsets: 'n/a',
  handlers: 'n/a',
})

/**
 * @param {Error|undefined} error
 * @returns {boolean} whether the error, or a cause up to five deep, is the broker's answer for a
 *   topic that does not exist
 */
const namesUnknownTopic = (error) => {
  for (let cause = error, depth = 0; cause && depth < 5; cause = cause.cause, depth += 1) {
    if (UNKNOWN_TOPIC.has(cause.type)) return true
  }
  return false
}

/**
 * The producer, admin client and probe of one case, closed together.
 */
class Scenario {
  /**
   * @param {string} name the case, for topic and client names
   * @param {object} [producerOptions] added to the declared producer settings
   */
  constructor(name, producerOptions = {}) {
    this.id = secureRandom()
    this.name = name
    this.brokers = plainTextBrokers()
    this.clientId = `e2e-p9b-topic-lifecycle-produce-${name}-${this.id}`
    this.producerOptions = producerOptions
    this.sockets = createLiveSockets()
    this.cleanup = new CleanupStack()
    this.requests = []
  }

  /** The declared settings, for the evidence. */
  get config() {
    return {
      requestTimeout: REQUEST_TIMEOUT_MS,
      enforceRequestTimeout: true,
      linger: 0,
      deliveryTimeout: DELIVERY_TIMEOUT_MS,
      idempotent: true,
      acks: -1,
      allowAutoTopicCreation: false,
      ...this.producerOptions,
    }
  }

  /**
   * @param {string} label
   * @returns {string} a topic name no other case shares
   */
  topic(label) {
    return `e2e-p9b-topic-lifecycle-produce-${this.name}-${label}-${this.id}`
  }

  /**
   * Opens the admin client and the producer and installs the admission probe.
   * @returns {Promise<void>}
   */
  async start() {
    const kafka = new Kafka({
      clientId: this.clientId,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.sockets.socketFactory,
      requestTimeout: REQUEST_TIMEOUT_MS,
      enforceRequestTimeout: true,
    })
    this.producer = kafka.producer({
      idempotent: true,
      linger: 0,
      deliveryTimeout: DELIVERY_TIMEOUT_MS,
      allowAutoTopicCreation: false,
      maxRequestSize: MAX_REQUEST_SIZE,
      ...this.producerOptions,
    })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.admin = new Kafka({ brokers: this.brokers, logLevel: logLevel.NOTHING }).admin()
    this.probe = new ProducerProbe({
      producer: this.producer,
      frames: this.sockets.frames,
      clientId: this.clientId,
      maxRequestSize: MAX_REQUEST_SIZE,
    }).install()
    this.cleanup.add('admission probe', () => this.probe.restore())
    this.cleanup.add('resources after disconnect', () => this.probe.settled())
    await this.admin.connect()
    this.cleanup.add('admin', () => this.admin.disconnect())
    await this.producer.connect()
    this.cleanup.add('producer', () => this.producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))
  }

  /**
   * Runs the case, then closes everything it opened; the case's own failure is the one thrown.
   * @template T
   * @param {() => Promise<T>} work
   * @returns {Promise<T>}
   */
  run(work) {
    return this.cleanup.run(async () => {
      await this.start()
      return work()
    })
  }

  /**
   * A send of one record, timed from its own public call to its settlement.
   * @param {string} topic
   * @param {string} value the record's id
   * @returns {Promise<{ value: string, ms: number, error: Error|null }>} never rejects
   */
  timed(topic, value) {
    const startedAt = performance.now()
    return settle(this.producer.send({ topic, messages: [{ value }], acks: -1 })).then(
      ({ error }) => ({ value, ms: performance.now() - startedAt, error })
    )
  }

  /**
   * @param {string} topic
   * @returns {Promise<string|null|undefined>} the id the cluster gives the topic
   */
  async topicIdOf(topic) {
    const {
      topics: [metadata],
    } = await this.admin.fetchTopicMetadata({ topics: [topic] })
    return metadata.topicId
  }

  /**
   * What the producer's cluster remembers of a topic and what it holds back.
   * @param {string} topic
   * @returns {{ pins: number, targeted: boolean, state: boolean, topicId: string|undefined }}
   */
  remembered(topic) {
    const { cluster } = this.probe
    return {
      pins: cluster.topicPins.size,
      targeted: cluster.targetTopics.has(topic),
      state: cluster.targetTopicState.has(topic),
      topicId: cluster.topicId(topic),
    }
  }

  /**
   * Writes the case's evidence line.
   * @param {string} name
   * @param {object} fields
   * @returns {void}
   */
  record(name, fields) {
    writeEvidence(EVIDENCE, {
      scenario: 'topic-lifecycle-produce',
      case: name,
      config: this.config,
      replay: REPLAY,
      wireVersions: wireVersionsOf(this.requests),
      wire: { frames: this.sockets.frames.trace(), replies: repliesOf(this.requests) },
      ...fields,
    })
  }
}

/**
 * Live proof of what a producer does while its topic is deleted, grown and recreated. Deleting the
 * topic under sends fails each of them with a typed unknown-topic error within the delivery
 * budget, settles every send and leaves the producer usable on another topic; adding partitions
 * is seen after a metadata refresh, and keyless records then reach the new partitions; a topic
 * recreated under the same name has a new id, and the records acknowledged after the recreation
 * are in the new log once while none of the old incarnation's are.
 *
 * Replay sets: the physical log holds the live incarnation's acknowledged records once and no
 * record of a deleted incarnation; a send that failed with an unknown outcome may be absent or
 * present once; committed offsets and handler deliveries are not part of this scenario.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers
 */
describeLifecycle('Producer > a topic deleted, grown and recreated under sends (live)', () => {
  test(
    'sends to a deleted topic fail typed within the delivery budget, every send settles and the producer still sends to another topic',
    liveCase(CASE_BUDGET_MS, async (scope) => {
      const scenario = new Scenario('delete', { metadataMaxIdle: METADATA_MAX_IDLE_MS })
      await scenario.run(async () => {
        const doomed = scenario.topic('doomed')
        const spare = scenario.topic('spare')
        await createTopic({ topic: doomed, partitions: 1, replicas: 3 })
        await createTopic({ topic: spare, partitions: 1, replicas: 3 })
        await scenario.probe.capture(
          () => scenario.producer.send({ topic: doomed, messages: [{ value: 'warm' }], acks: -1 }),
          { signal: scope }
        )
        const topicId = await scenario.topicIdOf(doomed)

        const sends = []
        let settled = 0
        let deletedAt = null
        const stream = (async () => {
          for (let index = 0; index < SENDS_AROUND_DELETE; index++) {
            sends.push(
              scenario.timed(doomed, `doomed-${index}`).then((outcome) => {
                settled += 1
                return { ...outcome, afterDelete: deletedAt !== null }
              })
            )
            await sleep(SEND_INTERVAL_MS)
          }
        })()
        await waitForCondition(() => settled >= ACKED_BEFORE_DELETE, WAIT_BUDGET)
        await scenario.admin.deleteTopics({ topics: [doomed], timeout: 10000 })
        deletedAt = performance.now()
        await stream
        const outcomes = await Promise.all(sends)
        const failed = outcomes.filter(({ error }) => error !== null)
        const acknowledged = outcomes.filter(({ error }) => error === null)

        const second = await scenario.timed(spare, 'spare-0')
        const pinsAfterFailures = scenario.probe.counters().pins
        await sleep(METADATA_MAX_IDLE_MS + 500)
        await scenario.probe.cluster.refreshMetadata()
        const remembered = scenario.remembered(doomed)
        const closing = await scenario.probe.closing({
          topic: scenario.topic('probe'),
          budgetMs: DELIVERY_TIMEOUT_MS,
        })

        scenario.record('delete-during-sends', {
          topicId,
          acknowledged: acknowledged.map(({ value, ms }) => ({ value, ms })),
          failed: failed.map(({ value, ms, error }) => ({
            value,
            ms,
            error: describeError(error),
          })),
          second: { ms: second.ms, error: describeError(second.error) },
          pinsAfterFailures,
          remembered,
          closing,
        })

        expect(outcomes).toHaveLength(SENDS_AROUND_DELETE)
        expect(acknowledged.length).toBeGreaterThanOrEqual(ACKED_BEFORE_DELETE)
        expect(failed.length).toBeGreaterThan(0)
        for (const { error, ms } of failed) {
          expect(namesUnknownTopic(error)).toBe(true)
          expect(ms).toBeLessThanOrEqual(FAILED_SEND_BOUND_MS)
        }
        expect(second.error).toBeNull()
        expect(pinsAfterFailures).toBe(0)
        expect(remembered).toMatchObject({ pins: 0, targeted: false, state: false })
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'keyless records reach the partitions added to a topic after the producer refreshed its metadata',
    liveCase(CASE_BUDGET_MS, async (scope) => {
      const scenario = new Scenario('grow', { metadataMaxAge: METADATA_MAX_AGE_MS })
      await scenario.run(async () => {
        const topic = scenario.topic('grown')
        await createTopic({ topic, partitions: 1, replicas: 3 })
        await scenario.probe.capture(
          () => scenario.producer.send({ topic, messages: [{ value: 'warm:' }], acks: -1 }),
          { signal: scope }
        )
        await scenario.admin.createPartitions({ topicPartitions: [{ topic, count: 3 }] })
        const grown = await waitForCondition(async () => {
          const {
            topics: [metadata],
          } = await scenario.admin.fetchTopicMetadata({ topics: [topic] })
          return (
            metadata.partitions.length === 3 &&
            metadata.partitions.every(({ leader, isr, replicas }) => {
              return leader >= 0 && isr.length === replicas.length
            })
          )
        }, WAIT_BUDGET)
        await sleep(METADATA_MAX_AGE_MS + 500)

        const ids = []
        const acknowledged = []
        for (let batch = 0; batch < GROWTH_SENDS; batch++) {
          const messages = Array.from({ length: GROWTH_RECORDS_PER_SEND }, (_, offset) => {
            const index = batch * GROWTH_RECORDS_PER_SEND + offset
            const id = `record-${String(index).padStart(4, '0')}:`
            ids.push(id)
            return { value: id + 'x'.repeat(256 + ((index * 37) % 7000)) }
          })
          const { error } = await settle(scenario.producer.send({ topic, messages, acks: -1 }))
          if (error === null) acknowledged.push(...messages.map(({ value }) => value.slice(0, 12)))
        }

        const log = await readLog({ brokers: scenario.brokers, topic })
        const perPartition = [...log.entries()]
          .sort(([a], [b]) => a - b)
          .map(([partition, { records }]) => ({
            partition,
            records: records.filter(({ batchContext }) => !batchContext.isControlBatch).length,
          }))
        const stored = dataRecords(log).map(({ value }) => value.toString('latin1', 0, 12))
        const closing = await scenario.probe.closing({
          topic: scenario.topic('probe'),
          budgetMs: DELIVERY_TIMEOUT_MS,
        })

        scenario.record('grow-partitions', {
          grown,
          perPartition,
          acknowledged: acknowledged.length,
          closing,
        })

        expect(grown).toBe(true)
        expect(acknowledged).toHaveLength(ids.length)
        expect(perPartition.map(({ partition }) => partition)).toEqual([0, 1, 2])
        expect(perPartition[1].records).toBeGreaterThan(0)
        expect(perPartition[2].records).toBeGreaterThan(0)
        expect(stored.filter((value) => value.startsWith('record-')).sort()).toEqual(
          [...ids].sort()
        )
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'a topic recreated under the same name has a new id, and only the records acknowledged after the recreation are in its log',
    liveCase(CASE_BUDGET_MS, async (scope) => {
      const scenario = new Scenario('recreate', {
        metadataMaxIdle: METADATA_MAX_IDLE_MS,
        metadataMaxAge: METADATA_MAX_AGE_MS,
      })
      await scenario.run(async () => {
        const topic = scenario.topic('recreated')
        await createTopic({ topic, partitions: 1, replicas: 3 })
        await scenario.probe.capture(
          () => scenario.producer.send({ topic, messages: [{ value: 'warm' }], acks: -1 }),
          { signal: scope }
        )
        const oldTopicId = await scenario.topicIdOf(topic)
        const oldOutcomes = []
        for (let index = 0; index < ACKED_BEFORE_DELETE; index++) {
          oldOutcomes.push(await scenario.timed(topic, `old-${index}`))
        }
        const inFlight = Array.from({ length: IN_FLIGHT }, (_, index) =>
          scenario.timed(topic, `inflight-${index}`)
        )
        await scenario.admin.deleteTopics({ topics: [topic], timeout: 10000 })
        const inFlightOutcomes = await Promise.all(inFlight)
        const absent = await waitForCondition(
          async () => !(await scenario.admin.listTopics()).includes(topic),
          WAIT_BUDGET
        )
        await createTopic({ topic, partitions: 1, replicas: 3 })
        const newTopicId = await scenario.topicIdOf(topic)

        const markerOutcomes = []
        for (let index = 0; index < MARKERS; index++) {
          markerOutcomes.push(await scenario.timed(topic, `new-${index}`))
        }
        const values = (await readData(scenario.brokers, topic)).map(({ value }) => String(value))
        const remembered = scenario.remembered(topic)
        const closing = await scenario.probe.closing({
          topic: scenario.topic('probe'),
          budgetMs: DELIVERY_TIMEOUT_MS,
        })

        const acknowledgedOld = [...oldOutcomes, ...inFlightOutcomes]
          .filter(({ error }) => error === null)
          .map(({ value }) => value)
        const failedInFlight = inFlightOutcomes
          .filter(({ error }) => error !== null)
          .map(({ value }) => value)
        const acknowledgedNew = markerOutcomes
          .filter(({ error }) => error === null)
          .map(({ value }) => value)
        const firstAcknowledged = markerOutcomes.findIndex(({ error }) => error === null)

        scenario.record('delete-and-recreate', {
          oldTopicId,
          newTopicId,
          absent,
          old: oldOutcomes.map(({ value, ms, error }) => ({
            value,
            ms,
            error: describeError(error),
          })),
          inFlight: inFlightOutcomes.map(({ value, ms, error }) => ({
            value,
            ms,
            error: describeError(error),
          })),
          markers: markerOutcomes.map(({ value, ms, error }) => ({
            value,
            ms,
            error: describeError(error),
          })),
          values,
          remembered,
          closing,
        })

        expect(absent).toBe(true)
        expect(newTopicId).not.toBe(oldTopicId)
        for (const { error, ms } of [...inFlightOutcomes, ...markerOutcomes]) {
          if (error !== null) expect(ms).toBeLessThanOrEqual(FAILED_SEND_BOUND_MS)
        }
        expect(firstAcknowledged).toBeGreaterThanOrEqual(0)
        expect(markerOutcomes.slice(firstAcknowledged).map(({ error }) => error)).toEqual(
          Array(MARKERS - firstAcknowledged).fill(null)
        )
        expect(markerOutcomes.at(-1).error).toBeNull()
        for (const value of acknowledgedNew) {
          expect(values.filter((stored) => stored === value)).toHaveLength(1)
        }
        for (const value of acknowledgedOld) expect(values).not.toContain(value)
        for (const value of failedInFlight) {
          expect(values.filter((stored) => stored === value).length).toBeLessThanOrEqual(1)
        }
        expect(remembered.pins).toBe(0)
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
})
