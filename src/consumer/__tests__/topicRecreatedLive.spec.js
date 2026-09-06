jest.setTimeout(120000)

const { Kafka, logLevel } = require('../../../index')
const {
  plainTextBrokers,
  secureRandom,
  waitForConsumerToJoinGroup,
  generateMessages,
} = require('testHelpers')

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
describe('Consumer > topic recreation (live)', () => {
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

  test('TOPIC_RECREATED fires and the consumer resumes from fromBeginning', async () => {
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

    const gotFirstBatch = await waitForCondition(() => messagesConsumed.length >= firstBatch.length)
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
  })

  test('a stale committed offset inside the recreated topic range is ignored', async () => {
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
    await waitForCondition(() => distinctPositions(newIncarnation()).length >= secondBatch.length, {
      attempts: 60,
    })

    // Without the one-shot reset the committed offset of the deleted incarnation is still valid
    // in the new one, so each partition starts halfway and its first records are silently skipped.
    const perPartition = secondBatch.length / PARTITIONS
    for (const partition of partitionRange()) {
      expect(offsetsOfPartition(newIncarnation(), partition)).toEqual(
        Array.from({ length: perPartition }, (_, i) => i)
      )
    }
  })

  test('a recreation that leaves the partition count unchanged still resets the position', async () => {
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

    const gotFirstBatch = await waitForCondition(() => messagesConsumed.length >= firstBatch.length)
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
  })
})
