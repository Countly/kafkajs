const { Kafka, PartitionAssigners, logLevel } = require('../../../index')
const { readLog } = require('../../../testHelpers/rawLog')
const { plainTextBrokers, secureRandom, writeEvidence, wireVersionsOf } = require('testHelpers')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  NEVER,
  STOP_GRACE_MS,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: it needs a real cluster. Run alone: `npm run test:e2e:regex-discovery`.
const describeRegexDiscovery = process.env.REGEX_DISCOVERY_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'regex-discovery'
const CASE_BUDGET_MS = 180000
const CLEANUP_STACK_BUDGET_MS = 30000
const CLEANUP_HOOK_TIMEOUT_MS = 32000
const STEP_MS = 30000
const FIRST_COVERAGE_MS = 60000
const DISCOVERY_MS = 20000
const ABSENCE_MS = 20000
const ABSENCE_POLL_MS = 1000
const METADATA_MAX_AGE_MS = 5000
const PARTITIONS = 3
const RECORDS = { initial: 6, later: 4, recreated: 8 }
const TITLE = 'discovers later matches and retires deleted incarnations without recreating topics'

/** Both arms: a label, then the consumer options that distinguish the group protocol. */
const ARMS = [
  ['classic cooperative', { partitionAssigners: [PartitionAssigners.cooperativeSticky] }],
  ['consumer protocol', { groupProtocol: 'consumer' }],
]

/**
 * @param {{ topic: string, partition: number, value: string }} record
 * @returns {string} the identity a record is compared by: the same value on another topic or
 *   partition is another record
 */
const identityOf = ({ topic, partition, value }) => `${topic}|${partition}|${value}`

/**
 * @param {Array<{ topic: string, partition: number, value: string }>} records
 * @returns {string[]} the distinct identities, sorted
 */
const identitiesOf = (records) => [...new Set(records.map(identityOf))].sort()

/**
 * Runs one operation as a step of the case: it never starts under an aborted scope, and an answer
 * that comes after the abort is dropped.
 * @template T
 * @param {AbortSignal} scope the case scope
 * @param {string} label names the step in its time-out message
 * @param {(child: AbortSignal) => Promise<T>} op
 * @returns {Promise<T>}
 */
const step = (scope, label, op) => bounded(scope, STEP_MS, op, { label })

/**
 * Deletes a topic through an admin of its own, for a cleanup. Both the connect and the deletion
 * are bounded by the cleanup's own signal, and a topic that is already gone is not a failure.
 * @param {Kafka} kafka
 * @param {string} topic
 * @param {AbortSignal} signal the cleanup's own, which aborts when its allowance ends
 * @returns {Promise<void>}
 * @throws {Error} a deletion failure other than a missing topic
 */
const deleteTopic = async (kafka, topic, signal) => {
  const admin = kafka.admin()
  try {
    await bounded(signal, STEP_MS, () => admin.connect(), { label: 'The cleanup admin connect' })
    await bounded(signal, STEP_MS, () => admin.deleteTopics({ topics: [topic], timeout: 10000 }), {
      label: 'The topic deletion',
    })
  } catch (error) {
    if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
  } finally {
    await bounded(NEVER, STOP_GRACE_MS, () => admin.disconnect(), {
      label: 'The cleanup admin disconnect',
    })
  }
}

/**
 * One arm's fixture: the clients, the topics, the three record sets (acknowledged, read back from
 * the log, delivered) and the checks over them. Every await goes through the case scope. These are
 * fixture methods, not product APIs.
 */
class RegexDiscoveryScenario {
  /**
   * @param {object} options
   * @param {AbortSignal} options.scope the case scope; nothing is issued once it aborted
   * @param {Kafka} options.kafka the client the admin and producer use
   * @param {string[]} options.brokers the bootstrap list, for the independent log reads
   * @param {string} options.prefix the unique topic-name prefix the pattern matches
   * @param {string} options.groupId
   * @param {object} options.consumerOptions what distinguishes the arm's group protocol
   * @param {CleanupStack} options.cleanup the stack the afterEach hook runs
   */
  constructor({ scope, kafka, brokers, prefix, groupId, consumerOptions, cleanup }) {
    this.scope = scope
    this.kafka = kafka
    this.brokers = brokers
    this.prefix = prefix
    this.groupId = groupId
    this.cleanup = cleanup
    this.admin = kafka.admin()
    this.producer = kafka.producer()
    this.consumer = kafka.consumer({
      groupId,
      metadataMaxAge: METADATA_MAX_AGE_MS,
      maxWaitTimeInMs: 500,
      ...consumerOptions,
    })
    /** @type {Map<string, string>} topic name to the id of its current incarnation */
    this.ids = new Map()
    /** @type {Array<{topic: string, partition: number, value: string, topicId: string}>} acknowledged */
    this.acknowledged = []
    /** @type {Array<{topic: string, partition: number, offset: string, value: string, at: number}>} */
    this.deliveries = []
    this.requests = []
    this.crashes = []
    this.unsubscribed = []
    this.removeListeners = []
  }

  /**
   * Connects the admin, producer and consumer, each registered for cleanup before the operation
   * that can leave it half open, and starts recording the consumer's events.
   * @returns {Promise<void>}
   */
  async open() {
    const { cleanup, consumer, producer, admin } = this
    cleanup.add('consumer listeners', () => this.removeListeners.forEach((remove) => remove()))
    for (const [event, sink] of [
      [consumer.events.CRASH, ({ payload }) => this.crashes.push(describeError(payload.error))],
      [
        consumer.events.RECEIVED_UNSUBSCRIBED_TOPICS,
        ({ payload }) => this.unsubscribed.push(payload.topicsNotSubscribed),
      ],
      [consumer.events.REQUEST, ({ payload }) => this.requests.push(payload)],
    ]) {
      this.removeListeners.push(consumer.on(event, sink))
    }
    cleanup.add('consumer', async () => {
      await consumer.stop()
      await consumer.disconnect()
    })
    cleanup.add('producer', () => producer.disconnect())
    cleanup.add('admin', () => admin.disconnect())
    await step(this.scope, 'The admin connect', () => admin.connect())
    await step(this.scope, 'The producer connect', () => producer.connect())
  }

  /**
   * Creates a topic incarnation, waits for every leader, produces its deterministic records with
   * `acks: -1` and records only what the broker acknowledged. A deleted and recreated name is a
   * new incarnation with another topic id.
   * @param {string} topic
   * @param {string} incarnation a label put in every value, so two incarnations never share one
   * @param {number} count records per partition
   * @returns {Promise<string>} the new topic id
   */
  async create(topic, incarnation, count) {
    const { scope, admin, producer } = this
    this.cleanup.add(`topic ${topic}`, (signal) => deleteTopic(this.kafka, topic, signal))
    await step(scope, 'The topic creation', () =>
      admin.createTopics({
        waitForLeaders: true,
        timeout: 20000,
        topics: [{ topic, numPartitions: PARTITIONS, replicationFactor: 2 }],
      })
    )
    let topicId = null
    await pollUntil(
      scope,
      STEP_MS,
      async () => {
        try {
          const { topics } = await admin.fetchTopicMetadata({ topics: [topic] })
          const [found] = topics
          if (found?.partitions.length !== PARTITIONS || found.topicId == null) return false
          topicId = found.topicId
          return found.partitions.every(({ leader }) => leader >= 0)
        } catch (error) {
          if (['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE'].includes(error.type)) {
            return false
          }
          throw error
        }
      },
      { label: 'The topic leaders' }
    )
    this.ids.set(topic, topicId)
    for (let partition = 0; partition < PARTITIONS; partition++) {
      const messages = Array.from({ length: count }, (_, index) => ({
        partition,
        value: `${incarnation}-${partition}-${index}`,
      }))
      await step(scope, 'The produce', () => producer.send({ topic, messages, acks: -1 }))
      for (const { value } of messages) this.acknowledged.push({ topic, partition, value, topicId })
    }
    return topicId
  }

  /**
   * Starts the consumer on a pattern over this arm's prefix, from the beginning, without automatic
   * commits, collecting every delivery.
   * @returns {Promise<void>}
   */
  async run() {
    const { consumer, scope } = this
    await step(scope, 'The consumer connect', () => consumer.connect())
    await step(scope, 'The subscription', () =>
      consumer.subscribe({ topics: [new RegExp(`^${this.prefix}-`)], fromBeginning: true })
    )
    await step(scope, 'The consumer run', () =>
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          this.deliveries.push({
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString(),
            at: performance.now(),
          })
        },
      })
    )
  }

  /**
   * @param {string} topicId the incarnation to cover
   * @returns {Array<{topic: string, partition: number, value: string}>} what was acknowledged for it
   */
  corpus(topicId) {
    return this.acknowledged.filter((record) => record.topicId === topicId)
  }

  /**
   * Waits until every acknowledged record of an incarnation has been delivered at least once, by
   * the identity `(topic, partition, value)`.
   * @param {string} topicId
   * @param {number} deadlineMs the sub-deadline of this wait, inside the case budget
   * @returns {Promise<void>}
   */
  async waitForCoverage(topicId, deadlineMs) {
    const wanted = identitiesOf(this.corpus(topicId))
    await pollUntil(
      this.scope,
      deadlineMs,
      () => {
        const seen = new Set(this.deliveries.map(identityOf))
        return wanted.every((identity) => seen.has(identity))
      },
      { label: `The delivery of incarnation ${topicId}`, delayMs: 100 }
    )
  }

  /**
   * Explicitly commits the next offset of every partition of an incarnation, after its records
   * were processed, and reads the committed offsets back from the broker. A call that was queued
   * or rejected is not a confirmation.
   * @param {string} topicId
   * @param {number} next the offset after the last processed record
   * @returns {Promise<Record<number, string>>} the broker-confirmed offset of each partition
   */
  async commitProcessed(topicId, next) {
    const topic = [...this.ids].find(([, id]) => id === topicId)[0]
    const offsets = Array.from({ length: PARTITIONS }, (_, partition) => ({
      topic,
      partition,
      offset: String(next),
    }))
    await step(this.scope, 'The explicit commit', () => this.consumer.commitOffsets(offsets))
    return this.committed(topic)
  }

  /**
   * @param {string} topic
   * @returns {Promise<Record<number, string>>} the broker's committed offset of each partition
   */
  async committed(topic) {
    const [{ partitions }] = await step(this.scope, 'The offset read', () =>
      this.admin.fetchOffsets({ groupId: this.groupId, topics: [topic] })
    )
    return Object.fromEntries(
      partitions.map(({ partition, offset }) => [partition, String(offset)])
    )
  }

  /**
   * Reads the incarnation's log independently of the consumer and checks it against what was
   * acknowledged: the same identities, and the log end of every partition.
   * @param {string} topicId
   * @param {number} end the expected log end of each partition
   * @returns {Promise<Array<{topic: string, partition: number, value: string}>>} what the log holds
   */
  async assertLog(topicId, end) {
    const topic = [...this.ids].find(([, id]) => id === topicId)[0]
    const log = await step(this.scope, 'The raw log read', () =>
      readLog({ brokers: this.brokers, topic })
    )
    const read = [...log.entries()].flatMap(([partition, { records, endOffset }]) => {
      expect(String(endOffset)).toBe(String(end))
      return records
        .filter((record) => !record.batchContext.isControlBatch)
        .map((record) => ({ topic, partition, value: record.value.toString() }))
    })
    expect(identitiesOf(read)).toEqual(identitiesOf(this.corpus(topicId)))
    return read
  }

  /**
   * Checks that a deleted topic stays absent from the cluster's topic list for at least `ms` while
   * the pattern consumer keeps running. It asks only for the whole list, never for the topic by
   * name, so nothing here can create it; any reappearance fails.
   * @param {string} topic
   * @param {number} ms how long it must stay absent
   * @returns {Promise<number>} how many checks found it absent
   */
  async assertAbsent(topic, ms) {
    const until = performance.now() + ms
    let checks = 0
    do {
      const topics = await step(this.scope, 'The topic list', () => this.admin.listTopics())
      expect(topics).not.toContain(topic)
      checks++
      await delay(this.scope, ABSENCE_POLL_MS)
    } while (performance.now() < until)
    return checks
  }

  /**
   * Deletes an incarnation and waits until the topic list no longer names it.
   * @param {string} topic
   * @returns {Promise<void>}
   */
  async remove(topic) {
    await step(this.scope, 'The topic deletion', () =>
      this.admin.deleteTopics({ topics: [topic], timeout: 20000 })
    )
    await pollUntil(
      this.scope,
      STEP_MS,
      async () => !(await this.admin.listTopics()).includes(topic),
      { label: 'The topic disappearance' }
    )
  }

  /**
   * Stops the consumer and then disconnects it, waiting for both.
   * @returns {Promise<void>}
   */
  async close() {
    await step(this.scope, 'The consumer stop', () => this.consumer.stop())
    await step(this.scope, 'The consumer disconnect', () => this.consumer.disconnect())
  }
}

describeRegexDiscovery(
  'Consumer > a client-side pattern over topics created and deleted (live)',
  () => {
    let cleanup

    beforeEach(() => {
      cleanup = new CleanupStack()
    })

    // Outside the case budget and inside the guard's cap; runs only for a case the run admitted.
    afterEach(async () => {
      if (!liveRun.takeAdmission()) return
      await cleanup.run(async () => {}, { budgetMs: CLEANUP_STACK_BUDGET_MS })
    }, CLEANUP_HOOK_TIMEOUT_MS)

    test.each(ARMS)(
      `%s: ${TITLE}`,
      liveCase(CASE_BUDGET_MS, async (scope, label, consumerOptions) => {
        const id = secureRandom()
        const prefix = `e2e-regex-discovery-${id}`
        const brokers = plainTextBrokers()
        const kafka = new Kafka({ clientId: prefix, brokers, logLevel: logLevel.NOTHING })
        const scenario = new RegexDiscoveryScenario({
          scope,
          kafka,
          brokers,
          prefix,
          groupId: `${prefix}-group`,
          consumerOptions,
          cleanup,
        })
        const first = `${prefix}-a`
        const second = `${prefix}-b`
        const record = {
          scenario: 'regex-discovery',
          arm: label,
          config: {
            metadataMaxAge: METADATA_MAX_AGE_MS,
            autoCommit: false,
            partitions: PARTITIONS,
            records: RECORDS,
            caseBudgetMs: CASE_BUDGET_MS,
          },
          replay:
            "at-least-once per incarnation; a replay lies after that incarnation's last confirmed commit, or restarts at offset 0 before its first confirmed commit",
          ok: false,
        }
        let evidenceError

        try {
          await scenario.open()

          const idA = await scenario.create(first, 'a', RECORDS.initial)
          await scenario.run()
          await scenario.waitForCoverage(idA, FIRST_COVERAGE_MS)
          expect(identitiesOf(scenario.deliveries)).toEqual(identitiesOf(scenario.corpus(idA)))

          const createdAt = performance.now()
          const idB = await scenario.create(second, 'b', RECORDS.later)
          const createdBAt = performance.now()
          await scenario.waitForCoverage(idB, DISCOVERY_MS)
          record.discoveryMs = performance.now() - createdBAt
          record.createdAfterSubscribeMs = createdBAt - createdAt
          expect(record.discoveryMs).toBeLessThanOrEqual(DISCOVERY_MS)

          const committedA = await scenario.commitProcessed(idA, RECORDS.initial)
          const committedB = await scenario.commitProcessed(idB, RECORDS.later)
          expect(Object.values(committedA)).toEqual(Array(PARTITIONS).fill(String(RECORDS.initial)))
          expect(Object.values(committedB)).toEqual(Array(PARTITIONS).fill(String(RECORDS.later)))

          const logA = await scenario.assertLog(idA, RECORDS.initial)
          record.beforeDelete = {
            topicId: idA,
            log: identitiesOf(logA),
            delivered: identitiesOf(scenario.deliveries.filter(({ topic }) => topic === first)),
            committed: committedA,
          }
          writeEvidence(EVIDENCE, { ...record, phase: 'before-delete' })

          await scenario.remove(first)
          record.absentChecks = await scenario.assertAbsent(first, ABSENCE_MS)
          const lateValues = Array.from({ length: PARTITIONS }, (_, partition) => ({
            partition,
            value: `b-late-${partition}`,
          }))
          await step(scope, 'The late produce', () =>
            scenario.producer.send({ topic: second, messages: lateValues, acks: -1 })
          )
          for (const { partition, value } of lateValues) {
            scenario.acknowledged.push({ topic: second, partition, value, topicId: idB })
          }
          await scenario.waitForCoverage(idB, DISCOVERY_MS)

          const idC = await scenario.create(first, 'c', RECORDS.recreated)
          expect(idC).not.toBe(idA)
          record.committedAfterRecreate = await scenario.committed(first)
          await scenario.waitForCoverage(idC, DISCOVERY_MS)
          const recreatedOffsets = scenario.deliveries
            .filter(({ topic, value }) => topic === first && value.startsWith('c-'))
            .map(({ partition, offset }) => `${partition}:${offset}`)
          for (let partition = 0; partition < PARTITIONS; partition++) {
            for (let offset = 0; offset < RECORDS.recreated; offset++) {
              expect(recreatedOffsets).toContain(`${partition}:${offset}`)
            }
          }

          const committedC = await scenario.commitProcessed(idC, RECORDS.recreated)
          expect(Object.values(committedC)).toEqual(
            Array(PARTITIONS).fill(String(RECORDS.recreated))
          )
          await scenario.assertLog(idC, RECORDS.recreated)

          const acknowledged = identitiesOf(scenario.acknowledged)
          const delivered = identitiesOf(scenario.deliveries)
          expect(delivered).toEqual(acknowledged)
          expect(scenario.crashes).toEqual([])
          expect(scenario.unsubscribed).toEqual([])
          await scenario.close()
          record.ok = true
        } catch (error) {
          record.error = describeError(error)
          throw error
        } finally {
          record.acknowledged = identitiesOf(scenario.acknowledged)
          record.delivered = identitiesOf(scenario.deliveries)
          record.ids = Object.fromEntries(scenario.ids)
          record.wireVersions = wireVersionsOf(scenario.requests)
          try {
            writeEvidence(EVIDENCE, { ...record, phase: 'case' })
          } catch (error) {
            evidenceError = error
          }
        }
        // Reached only when the body passed, so a failed body keeps its own error.
        if (evidenceError) throw evidenceError
      }),
      caseTimeout(CASE_BUDGET_MS)
    )
  }
)
