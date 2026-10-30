const { Kafka, logLevel } = require('../../../index')
const {
  plainTextBrokers,
  secureRandom,
  waitForConsumerToJoinGroup,
  generateMessages,
  writeEvidence,
} = require('testHelpers')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const { positionOf, runWithEvidence } = require('./fixtures/evidenceCase')
const {
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: needs TOPIC_RECREATED_E2E=1 and the compose cluster. Run alone through the guarded live
// entry.
const describeRecreation = process.env.TOPIC_RECREATED_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'topic-recreated'

/** What one case may take; its Jest timeout adds the harness's join reserve. */
const CASE_BUDGET_MS = 120000
/** What one awaited broker or client operation of the new arm may take. */
const STEP_MS = 30000
/** What one delivery or metadata wait of the new arm may take. */
const WAIT_MS = 30000
const POLL_MS = 250
/** What the new arm's cleanup steps may take together. */
const CLEANUP_BUDGET_MS = 20000
/** What each broker read of the new arm's evidence may take. */
const TRUTH_MS = 5000
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'
const TOPIC_NOT_READY = [MISSING_TOPIC, 'LEADER_NOT_AVAILABLE']

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Partition count every incarnation of the test topic is created with.
 *
 * A subscribed consumer keeps refreshing metadata while the topic is gone, and on a broker with
 * automatic topic creation enabled that refresh brings the topic back on its own - with the
 * broker's default partition count, before this test's `createTopics()` runs, which then fails
 * with "topic already exists" and is silently ignored. Creating both incarnations with the same
 * count the cluster defaults to makes the two paths produce the same topic, so the scenario no
 * longer depends on which of them wins that race.
 */
const PARTITIONS = 2

/**
 * Polls `predicate` on a plain interval until it is true, bounded rather than indefinite: a
 * regression that stalls delivery must fail on the assertion that follows, not hang for the
 * whole suite timeout.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {{attempts?: number, delay?: number}} [options]
 * @returns {Promise<boolean>} whether `predicate` became true within the attempt budget
 */
const waitForCondition = async (predicate, { attempts = 40, delay = 500 } = {}) => {
  for (let attempt = 0; attempt < attempts; attempt++) {
    if (await predicate()) {
      return true
    }
    await sleep(delay)
  }
  return false
}

/**
 * `count` messages for each partition of the test topic, addressed explicitly so the split is
 * fixed rather than left to the partitioner - the per-partition offsets these tests assert on are
 * only exact if every partition receives the same number of records.
 * @param {number} count records per partition
 * @param {string} [prefix] carried into every value, so the records of one incarnation can be
 *   told apart from the other's by content instead of by their position in the delivery order
 * @returns {{key: string, value: string, partition: number}[]}
 */
const messagesPerPartition = (count, prefix) =>
  generateMessages({ number: count * PARTITIONS, prefix }).map((message, index) => ({
    ...message,
    partition: index % PARTITIONS,
  }))

/**
 * Every partition of the test topic, as a list to iterate or map over.
 * @returns {number[]}
 */
const partitionRange = () => Array.from({ length: PARTITIONS }, (_, partition) => partition)

/**
 * The distinct `partition:offset` positions a set of `eachMessage` payloads covers. Kafka's
 * ordinary at-least-once delivery means a rebalance can redeliver an already-processed record,
 * including one racing this test's own delete/recreate window - that is expected and not what
 * this mechanism guarantees against, so tests compare distinct position *coverage*, not raw
 * delivery counts, to avoid asserting on incidental duplicates. Offsets are only unique within a
 * partition, so the partition is part of the key.
 * @param {Array<{partition: number, message: {offset: string}}>} records
 * @returns {string[]}
 */
const distinctPositions = (records) =>
  [...new Set(records.map(({ partition, message }) => `${partition}:${message.offset}`))].sort()

/**
 * The distinct, sorted offsets a set of `eachMessage` payloads covers on one partition.
 * @param {Array<{partition: number, message: {offset: string}}>} records
 * @param {number} partition
 * @returns {number[]}
 */
const offsetsOfPartition = (records, partition) =>
  [
    ...new Set(
      records
        .filter((record) => record.partition === partition)
        .map(({ message }) => Number(message.offset))
    ),
  ].sort((a, b) => a - b)

/**
 * @param {Array<{partition: number, offset: string}>} records handled records
 * @param {number} partition
 * @returns {number[]} the distinct, sorted offsets handled on the partition
 */
const offsetsOn = (records, partition) =>
  [
    ...new Set(
      records.filter((record) => record.partition === partition).map(({ offset }) => Number(offset))
    ),
  ].sort((a, b) => a - b)

/**
 * @param {Array<{topic: string, partition: number, value: string}>} acknowledged one fresh
 *   incarnation's records, in production order
 * @returns {Array<{offset: number, id: string}>} each record with the offset its partition's log
 *   gave it and its complete identity
 */
const expectedPositions = (acknowledged) => {
  const next = new Map()
  return acknowledged.map((record) => {
    const offset = next.get(record.partition) ?? 0
    next.set(record.partition, offset + 1)
    return { offset, id: positionOf({ ...record, offset }) }
  })
}

/**
 * @param {Array<{topic: string, partition: number, offset: string, value: string}>} handled
 * @returns {string[]} the distinct complete identities handled, sorted. At-least-once delivery
 *   means a rebalance may redeliver a record, so distinct coverage is what the arm compares.
 */
const distinctIdentities = (handled) => [...new Set(handled.map(positionOf))].sort()

/**
 * The new arm's clients and everything it reports. Every awaited operation goes through the case
 * scope, and every resource has its own cleanup step, registered before the work that creates it.
 * Its evidence row is written from whatever the arm retained, before any cleanup runs.
 */
class RecreationArm {
  /**
   * @param {AbortSignal} scope the case's scope
   * @param {Kafka} kafka
   * @param {string} topic
   * @param {string} groupId
   */
  constructor(scope, kafka, topic, groupId) {
    this.scope = scope
    this.topic = topic
    this.groupId = groupId
    this.cleanup = new CleanupStack()
    this.admin = kafka.admin()
    this.producer = kafka.producer()
    this.consumer = kafka.consumer({ groupId })
    /** @type {string} the step the arm reached */
    this.stage = 'created'
    /** @type {Array<{topic: string, partition: number, value: string}>} what the sends confirmed */
    this.acknowledged = []
    /** @type {Array<{topic: string, partition: number, offset: string, value: string}>} */
    this.handled = []
    this.recreated = []
    this.joins = []
    this.crashes = []
    /** @type {Record<string, number>} handler counts at each stop and rebuild */
    this.marks = {}
    /** @type {Record<string, unknown>} the offsets the arm committed or saw committed */
    this.commits = {}
  }

  /**
   * @param {string} stage the step the arm is about to await
   * @returns {void}
   */
  at(stage) {
    this.stage = stage
  }

  /**
   * @template T
   * @param {string} label names the operation in its time-out message
   * @param {() => Promise<T>} op
   * @returns {Promise<T>}
   */
  step(label, op) {
    return bounded(this.scope, STEP_MS, op, { label })
  }

  /**
   * @param {string} label names the wait in its time-out message
   * @param {() => boolean|Promise<boolean>} check
   * @returns {Promise<void>}
   * @throws {Error} when the wait times out, or when a handler crashed
   */
  async wait(label, check) {
    await pollUntil(this.scope, WAIT_MS, () => this.crashes.length > 0 || check(), {
      delayMs: POLL_MS,
      label,
    })
    expect(this.crashes).toEqual([])
  }

  /**
   * Connects the admin and the producer, then creates the topic. The closes, and the topic's
   * deletion, are separate cleanup steps; the consumer's is added when it starts.
   * @returns {Promise<void>}
   */
  async open() {
    this.cleanup.add('admin', () => this.admin.disconnect())
    this.cleanup.add('producer', () => this.producer.disconnect())
    this.at('connect')
    await this.step('The admin connect', () => this.admin.connect())
    await this.step('The producer connect', () => this.producer.connect())
    this.cleanup.add('topic', (signal) => this.deleteTopic(signal))
    this.at('create the topic')
    await this.createTopic()
  }

  /**
   * Creates the topic with `PARTITIONS` partitions, retrying while the cluster is not ready, and
   * waits until the metadata shows them.
   * @returns {Promise<void>}
   */
  async createTopic() {
    await pollUntil(
      this.scope,
      STEP_MS,
      async () => {
        try {
          await this.admin.createTopics({
            topics: [{ topic: this.topic, numPartitions: PARTITIONS }],
            waitForLeaders: true,
          })
          return true
        } catch (error) {
          if (!TOPIC_NOT_READY.includes(error.type)) throw error
          return false
        }
      },
      { delayMs: POLL_MS, label: 'The topic creation' }
    )
    await pollUntil(
      this.scope,
      STEP_MS,
      async () => {
        const { topics } = await this.admin
          .fetchTopicMetadata({ topics: [this.topic] })
          .catch(() => ({ topics: [] }))
        return topics.length === 1 && topics[0].partitions.length === PARTITIONS
      },
      { delayMs: POLL_MS, label: 'The topic metadata' }
    )
  }

  /**
   * Deletes the topic as a cleanup; a confirmed missing topic is success.
   * @param {AbortSignal} signal the cleanup's own, which aborts when its allowance ends
   * @returns {Promise<void>}
   * @throws {Error} a deletion failure other than a missing topic
   */
  async deleteTopic(signal) {
    try {
      await bounded(
        signal,
        STEP_MS,
        () => this.admin.deleteTopics({ topics: [this.topic], timeout: 10000 }),
        { label: 'The topic deletion' }
      )
    } catch (error) {
      if (error.type !== MISSING_TOPIC) throw error
    }
  }

  /**
   * Deletes the topic and creates it again under the same name. It also ends when the running
   * consumer's own metadata refresh made the broker auto-create the topic, which the consumer
   * reports as a recreation.
   * @returns {Promise<void>}
   */
  async recreate() {
    await this.step('The topic deletion', () =>
      this.admin.deleteTopics({ topics: [this.topic], timeout: 10000 })
    )
    await pollUntil(
      this.scope,
      WAIT_MS,
      async () =>
        this.recreated.length > 0 || !(await this.admin.listTopics()).includes(this.topic),
      { delayMs: POLL_MS, label: 'The topic deletion to settle' }
    )
    await this.createTopic()
  }

  /**
   * Sends the messages and, once the send is confirmed, records them as acknowledged.
   * @param {Array<{key: string, value: string, partition: number}>} messages
   * @returns {Promise<void>}
   */
  async produce(messages) {
    await this.step('The send', () => this.producer.send({ topic: this.topic, messages }))
    for (const { partition, value } of messages) {
      this.acknowledged.push({ topic: this.topic, partition, value })
    }
  }

  /**
   * Subscribes and starts the consumer with manual commits, its disconnect registered first.
   * @returns {Promise<void>}
   */
  async start() {
    const { consumer } = this
    this.cleanup.add('consumer', () => consumer.disconnect())
    consumer.on(consumer.events.TOPIC_RECREATED, (event) => this.recreated.push(event.payload))
    consumer.on(consumer.events.GROUP_JOIN, (event) => this.joins.push(event.payload))
    consumer.on(consumer.events.CRASH, (event) => this.crashes.push(event.payload.error))
    await this.step('The consumer connect', () => consumer.connect())
    await this.step('The subscription', () =>
      consumer.subscribe({ topic: this.topic, fromBeginning: true })
    )
    await this.run()
  }

  /** @returns {Promise<void>} once the consumer runs */
  run() {
    return this.step('The consumer run', () =>
      this.consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          this.handled.push({
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString(),
          })
        },
      })
    )
  }

  /**
   * @param {Array<{topic: string, partition: number, offset: string}>} offsets
   * @returns {Promise<void>}
   */
  commit(offsets) {
    return this.step('The commit', () => this.consumer.commitOffsets(offsets))
  }

  /** @returns {Promise<void>} once the consumer stopped */
  stop() {
    return this.step('The consumer stop', () => this.consumer.stop())
  }

  /**
   * Reads what the broker holds, each read bounded and its failure recorded, never thrown.
   * @returns {Promise<{ committed: unknown, endOffsets: unknown }>}
   */
  async readTruth() {
    const read = (label, op) =>
      bounded(this.scope, TRUTH_MS, op, { label }).catch((error) => ({
        unavailable: describeError(error),
      }))
    return {
      committed: await read('The committed read', () =>
        this.admin.fetchOffsets({ groupId: this.groupId, topics: [this.topic] })
      ),
      endOffsets: await read('The end offsets read', () =>
        this.admin.fetchTopicOffsets(this.topic)
      ),
    }
  }

  /**
   * Writes the evidence row. It is synchronous after the broker reads, so an aborted case scope
   * cannot stop it.
   * @param {Error|null} failure the arm's own failure, when it has one
   * @returns {Promise<Error|null>} the write's failure, which never throws
   */
  async write(failure) {
    try {
      const truth = await this.readTruth()
      writeEvidence(EVIDENCE, {
        scenario: EVIDENCE,
        case: 'recreation-before-first-confirmed-commit',
        stage: this.stage,
        error: describeError(failure),
        topic: this.topic,
        groupId: this.groupId,
        acknowledged: this.acknowledged,
        handled: this.handled,
        recreated: this.recreated,
        joins: this.joins.length,
        crashes: this.crashes.map((error) => describeError(error)),
        marks: this.marks,
        commits: this.commits,
        brokerTruth: truth,
      })
      return null
    } catch (error) {
      return error
    }
  }

  /**
   * Runs the arm's body, writes its row however it ended, and only then runs the cleanups.
   * @param {() => Promise<void>} body
   * @returns {Promise<void>}
   * @throws {Error} see `runWithEvidence`
   */
  guarded(body) {
    return runWithEvidence(this.cleanup, (failure) => this.write(failure), body, CLEANUP_BUDGET_MS)
  }
}

/**
 * Live-cluster proof of the topic-recreation one-shot offset reset: a single,
 * continuously running consumer survives a real delete + recreate of its subscribed topic, sees
 * `TOPIC_RECREATED`, and resumes per its own `fromBeginning` instead of trusting a committed
 * offset that belonged to the deleted incarnation - no record silently skipped, though an
 * ordinary rebalance racing the same window may still redeliver one (see `distinctPositions`).
 *
 * Each client is its own `Kafka` instance (real `admin()`/`producer()`/`consumer()` construction,
 * production `metadataMaxAge` defaults) rather than a hand-built `Cluster` - a raw `Cluster` with
 * `metadataMaxAge: 0` refreshes on every call and can observe a freshly created topic's id still
 * settling across replicas, firing spurious `TOPIC_RECREATED` events that have nothing to do with
 * this mechanism.
 *
 * Needs a live cluster, so it runs in the consumer group rather than the broker-free unit suite.
 */
describeRecreation('Consumer > topic recreation (live)', () => {
  let kafka, admin, producer, consumer, topic, groupId

  beforeEach(() => {
    topic = `test-topic-${secureRandom()}`
    groupId = `test-group-${secureRandom()}`
    kafka = new Kafka({
      clientId: `test-${secureRandom()}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    admin = kafka.admin()
  })

  afterEach(async () => {
    // A refused case issued nothing, so there is nothing to disconnect.
    if (!liveRun.takeAdmission()) return
    consumer && (await consumer.disconnect().catch(() => {}))
    producer && (await producer.disconnect().catch(() => {}))
    admin && (await admin.disconnect().catch(() => {}))
  })

  /**
   * Deletes the topic and creates it again under the same name.
   *
   * The delete is only followed up once the cluster stops listing the topic - or, when the running
   * consumer's own metadata refresh lands in that window and the broker auto-creates the topic
   * from it, once the consumer reports that recreation, because the topic then never disappears
   * from `listTopics()` at all. Whichever path created it, the topic is handed back only once the
   * cluster reports `PARTITIONS` partitions for it: another shape means the cluster's default
   * partition count no longer matches this test's, which must fail here and not as a delivery
   * shortfall a minute later.
   *
   * @param {Array<unknown>} recreatedEvents `TOPIC_RECREATED` payloads the caller is collecting
   * @returns {Promise<boolean>} whether the recreated topic settled at the expected shape
   */
  const recreateTopic = async (recreatedEvents) => {
    await admin.deleteTopics({ topics: [topic], timeout: 10000 })
    const deleted = await waitForCondition(
      async () => recreatedEvents.length > 0 || !(await admin.listTopics()).includes(topic)
    )
    expect(deleted).toBe(true)

    await admin.createTopics({
      topics: [{ topic, numPartitions: PARTITIONS }],
      waitForLeaders: true,
    })

    return waitForCondition(async () => {
      const { topics } = await admin
        .fetchTopicMetadata({ topics: [topic] })
        .catch(() => ({ topics: [] }))
      return topics.length === 1 && topics[0].partitions.length === PARTITIONS
    })
  }

  test(
    'TOPIC_RECREATED fires and the consumer resumes from fromBeginning',
    liveCase(CASE_BUDGET_MS, async () => {
      await admin.connect()
      await admin.createTopics({
        topics: [{ topic, numPartitions: PARTITIONS }],
        waitForLeaders: true,
      })

      producer = kafka.producer()
      await producer.connect()

      const firstBatch = messagesPerPartition(5)
      await producer.send({ topic, messages: firstBatch })

      consumer = kafka.consumer({ groupId })
      const recreatedEvents = []
      consumer.on(consumer.events.TOPIC_RECREATED, (event) => recreatedEvents.push(event.payload))

      const messagesConsumed = []
      await consumer.subscribe({ topic, fromBeginning: true })
      consumer.run({ eachMessage: async (payload) => messagesConsumed.push(payload) })
      await waitForConsumerToJoinGroup(consumer)

      const gotFirstBatch = await waitForCondition(
        () => messagesConsumed.length >= firstBatch.length
      )
      expect(gotFirstBatch).toBe(true)

      const recreated = await recreateTopic(recreatedEvents)
      expect(recreated).toBe(true)

      const marker = `recreated-${secureRandom()}`
      const secondBatch = messagesPerPartition(10, marker)
      await producer.send({ topic, messages: secondBatch })

      const newIncarnation = () =>
        messagesConsumed.filter(({ message }) => message.value.toString().includes(marker))
      const gotEverything = await waitForCondition(
        () => distinctPositions(newIncarnation()).length >= secondBatch.length,
        { attempts: 60 }
      )
      expect(gotEverything).toBe(true)

      expect(recreatedEvents).toHaveLength(1)
      expect(recreatedEvents[0].topic).toEqual(topic)
      expect(recreatedEvents[0].previousTopicId).not.toEqual(recreatedEvents[0].topicId)

      // Every partition of the new incarnation delivered from its own beginning.
      const perPartition = secondBatch.length / PARTITIONS
      for (const partition of partitionRange()) {
        expect(offsetsOfPartition(newIncarnation(), partition)).toEqual(
          Array.from({ length: perPartition }, (_, i) => i)
        )
      }
    }),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'a stale committed offset inside the recreated topic range is ignored',
    liveCase(CASE_BUDGET_MS, async () => {
      await admin.connect()
      await admin.createTopics({
        topics: [{ topic, numPartitions: PARTITIONS }],
        waitForLeaders: true,
      })

      producer = kafka.producer()
      await producer.connect()

      const firstBatch = messagesPerPartition(5)
      await producer.send({ topic, messages: firstBatch })

      consumer = kafka.consumer({ groupId })
      const recreatedEvents = []
      consumer.on(consumer.events.TOPIC_RECREATED, (event) => recreatedEvents.push(event.payload))

      const messagesConsumed = []
      await consumer.subscribe({ topic, fromBeginning: true })
      consumer.run({
        autoCommit: false,
        eachMessage: async (payload) => messagesConsumed.push(payload),
      })
      await waitForConsumerToJoinGroup(consumer)

      // Every partition, not just the total: a redelivery on one partition can satisfy a total-count
      // wait while the other is still short, and the offset committed below - and with it the
      // strength of this test's negative control - would silently shrink with it.
      const perPartitionFirst = firstBatch.length / PARTITIONS
      const gotFirstBatch = await waitForCondition(() =>
        partitionRange().every(
          (partition) => offsetsOfPartition(messagesConsumed, partition).length >= perPartitionFirst
        )
      )
      expect(gotFirstBatch).toBe(true)

      // Commit the position reached in the deleted incarnation. The new one is twice as long per
      // partition, so every one of these offsets is still a valid position there - which is exactly
      // what makes the stale offset dangerous rather than self-correcting.
      await consumer.commitOffsets(
        partitionRange().map((partition) => ({
          topic,
          partition,
          offset: String(perPartitionFirst),
        }))
      )

      const recreated = await recreateTopic(recreatedEvents)
      expect(recreated).toBe(true)

      const marker = `recreated-${secureRandom()}`
      const secondBatch = messagesPerPartition(10, marker)
      await producer.send({ topic, messages: secondBatch })

      const newIncarnation = () =>
        messagesConsumed.filter(({ message }) => message.value.toString().includes(marker))
      await waitForCondition(
        () => distinctPositions(newIncarnation()).length >= secondBatch.length,
        {
          attempts: 60,
        }
      )

      // Without the one-shot reset the committed offset of the deleted incarnation is still valid
      // in the new one, so each partition starts halfway and its first records are silently skipped.
      const perPartition = secondBatch.length / PARTITIONS
      for (const partition of partitionRange()) {
        expect(offsetsOfPartition(newIncarnation(), partition)).toEqual(
          Array.from({ length: perPartition }, (_, i) => i)
        )
      }
    }),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'a recreation that leaves the partition count unchanged still resets the position',
    liveCase(CASE_BUDGET_MS, async () => {
      await admin.connect()
      await admin.createTopics({
        topics: [{ topic, numPartitions: PARTITIONS }],
        waitForLeaders: true,
      })

      producer = kafka.producer()
      await producer.connect()

      const firstBatch = messagesPerPartition(5)
      await producer.send({ topic, messages: firstBatch })

      consumer = kafka.consumer({ groupId })
      const recreatedEvents = []
      const groupJoins = []
      consumer.on(consumer.events.TOPIC_RECREATED, (event) => recreatedEvents.push(event.payload))
      consumer.on(consumer.events.GROUP_JOIN, (event) => groupJoins.push(event.payload))

      const messagesConsumed = []
      await consumer.subscribe({ topic, fromBeginning: true })
      consumer.run({ eachMessage: async (payload) => messagesConsumed.push(payload) })
      await waitForConsumerToJoinGroup(consumer)

      const gotFirstBatch = await waitForCondition(
        () => messagesConsumed.length >= firstBatch.length
      )
      expect(gotFirstBatch).toBe(true)

      const assignmentBeforeRecreation = groupJoins[groupJoins.length - 1].memberAssignment

      // Recreated with the same partition count, so the assignment is unchanged and the
      // stale-assignment check has nothing to notice: the topic id is the only thing that moved,
      // which makes this the case that would expose a reset relying on a partition-count change.
      const recreated = await recreateTopic(recreatedEvents)
      expect(recreated).toBe(true)

      const marker = `recreated-${secureRandom()}`
      const secondBatch = messagesPerPartition(10, marker)
      await producer.send({ topic, messages: secondBatch })

      const newIncarnation = () =>
        messagesConsumed.filter(({ message }) => message.value.toString().includes(marker))
      const gotFirstRecord = await waitForCondition(() => newIncarnation().length > 0, {
        attempts: 60,
      })
      expect(gotFirstRecord).toBe(true)

      // Once the first record of the new incarnation is in hand the consumer is delivering it, so
      // nothing after this point needs a rejoin. The joins before it are not counted: while the
      // topic genuinely does not exist, a metadata refresh legitimately recovers through one.
      const joinsAtFirstDelivery = groupJoins.length

      const gotEverything = await waitForCondition(
        () => distinctPositions(newIncarnation()).length >= secondBatch.length,
        { attempts: 60 }
      )
      expect(gotEverything).toBe(true)
      expect(groupJoins).toHaveLength(joinsAtFirstDelivery)

      expect(recreatedEvents).toHaveLength(1)
      expect(recreatedEvents[0].topic).toEqual(topic)
      expect(recreatedEvents[0].previousTopicId).not.toEqual(recreatedEvents[0].topicId)

      // What proves the reset ran is this: auto-commit had recorded a position inside each partition
      // of the deleted incarnation, and that position is still valid in the new one, so without the
      // one-shot reset the records before it would be missing here.
      const perPartition = secondBatch.length / PARTITIONS
      for (const partition of partitionRange()) {
        expect(offsetsOfPartition(newIncarnation(), partition)).toEqual(
          Array.from({ length: perPartition }, (_, i) => i)
        )
      }

      // The case's premise, kept as documentation: the recreated topic carries the same partitions
      // as the deleted one, so the assignment never changes and the stale-assignment check cannot be
      // what recovered the consumer.
      expect(groupJoins[groupJoins.length - 1].memberAssignment).toEqual(assignmentBeforeRecreation)
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
  test(
    'a recreation before the first confirmed commit resets again after stop and run',
    liveCase(CASE_BUDGET_MS, (scope) => {
      const arm = new RecreationArm(scope, kafka, topic, groupId)
      return arm.guarded(async () => {
        await arm.open()
        const firstBatch = messagesPerPartition(5)
        arm.at('produce the first incarnation')
        await arm.produce(firstBatch)
        arm.at('start')
        await arm.start()

        const perPartitionFirst = firstBatch.length / PARTITIONS
        arm.at('wait for the first batch')
        await arm.wait('The first batch', () =>
          partitionRange().every(
            (partition) => offsetsOn(arm.handled, partition).length >= perPartitionFirst
          )
        )

        // A numerically valid position of the deleted incarnation, committed before it is deleted.
        const stale = partitionRange().map((partition) => ({
          topic,
          partition,
          offset: String(perPartitionFirst),
        }))
        arm.at('commit the old position')
        await arm.commit(stale)
        arm.commits.old = stale

        arm.at('recreate')
        await arm.recreate()

        const marker = `recreated-${secureRandom()}`
        const secondBatch = messagesPerPartition(10, marker)
        arm.at('produce the second incarnation')
        await arm.produce(secondBatch)
        const perPartition = secondBatch.length / PARTITIONS
        const fromStart = Array.from({ length: perPartition }, (_, i) => i)
        const expected = expectedPositions(
          arm.acknowledged.filter(({ value }) => value.includes(marker))
        )

        const since = (mark) =>
          arm.handled.slice(mark).filter(({ value }) => value.includes(marker))
        const delivered = (mark, count) => () =>
          partitionRange().every((partition) => offsetsOn(since(mark), partition).length >= count)
        arm.at('wait for the second incarnation')
        await arm.wait('The second incarnation', delivered(0, perPartition))

        // Stop and run before any commit of the new incarnation: the reset is repeated, so the
        // position never becomes the deleted incarnation's committed one.
        arm.at('stop')
        await arm.stop()
        const restartedAt = arm.handled.length
        arm.marks.restartedAt = restartedAt
        arm.at('run again')
        await arm.run()
        arm.at('wait for the restarted deliveries')
        await arm.wait('The restarted deliveries', delivered(restartedAt, perPartition))
        const restarted = since(restartedAt)

        // A confirmed commit of the new incarnation ends the debt: the next run starts there.
        const confirmed = partitionRange().map((partition) => ({ topic, partition, offset: '3' }))
        arm.at('commit the new position')
        await arm.commit(confirmed)
        arm.commits.confirmed = confirmed
        arm.at('stop before the rebuild')
        await arm.stop()
        const committedAt = arm.handled.length
        arm.marks.committedAt = committedAt
        arm.at('rebuild')
        await arm.run()
        arm.at('wait for the rebuilt deliveries')
        await arm.wait('The rebuilt deliveries', delivered(committedAt, perPartition - 3))
        const rebuilt = since(committedAt)

        arm.at('assert')
        expect(arm.recreated).toHaveLength(1)
        for (const partition of partitionRange()) {
          expect(offsetsOn(restarted, partition)).toEqual(fromStart)
          expect(offsetsOn(rebuilt, partition)).toEqual(fromStart.slice(3))
        }
        expect(distinctIdentities(since(0))).toEqual(expected.map(({ id }) => id).sort())
        expect(distinctIdentities(restarted)).toEqual(expected.map(({ id }) => id).sort())
        expect(distinctIdentities(rebuilt)).toEqual(
          expected
            .filter(({ offset }) => offset >= 3)
            .map(({ id }) => id)
            .sort()
        )
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
})
