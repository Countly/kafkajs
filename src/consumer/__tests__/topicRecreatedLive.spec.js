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
 * The distinct, sorted offsets a slice of `eachMessage` payloads carries. Kafka's ordinary
 * at-least-once delivery means a rebalance can redeliver an already-processed record, including
 * one racing this test's own delete/recreate window - that is expected and not what this
 * mechanism guarantees against, so tests compare distinct offset *coverage*, not raw delivery
 * counts, to avoid asserting on incidental duplicates.
 * @param {Array<{message: {offset: string}}>} records
 * @returns {number[]}
 */
const distinctOffsets = (records) =>
  [...new Set(records.map(({ message }) => Number(message.offset)))].sort((a, b) => a - b)

/**
 * Live-cluster proof of the topic-recreation one-shot offset reset: a single,
 * continuously running consumer survives a real delete + recreate of its subscribed topic, sees
 * `TOPIC_RECREATED`, and resumes per its own `fromBeginning` instead of trusting a committed
 * offset that belonged to the deleted incarnation - no record silently skipped, though an
 * ordinary rebalance racing the same window may still redeliver one (see `distinctOffsets`).
 *
 * Each client is its own `Kafka` instance (real `admin()`/`producer()`/`consumer()` construction,
 * production `metadataMaxAge` defaults) rather than a hand-built `Cluster` - a raw `Cluster` with
 * `metadataMaxAge: 0` refreshes on every call and can observe a freshly created topic's id still
 * settling across replicas, firing spurious `TOPIC_RECREATED` events that have nothing to do with
 * this mechanism.
 *
 * Opt-in like `rebootstrapLive.spec.js`: timing against a live cluster is inherently less
 * predictable than the fake-socket unit suite, so this stays out of the default
 * `test:group:consumer` gate. Run explicitly with `TOPIC_RECREATED_E2E=1`.
 */
const enabled = process.env.TOPIC_RECREATED_E2E === '1'
const describeLive = enabled ? describe : describe.skip

describeLive('Consumer > topic recreation (live)', () => {
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

  test('TOPIC_RECREATED fires and the consumer resumes from fromBeginning', async () => {
    await admin.connect()
    await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true })

    producer = kafka.producer()
    await producer.connect()

    const firstBatch = generateMessages({ number: 5 })
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

    // 2. admin.deleteTopics + admin.createTopics (same name, new id), wait for propagation
    await admin.deleteTopics({ topics: [topic], timeout: 10000 })
    const topicGone = await waitForCondition(
      async () => !(await admin.listTopics()).includes(topic)
    )
    expect(topicGone).toBe(true)
    await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true })

    // 3. produce a fresh, larger batch
    const secondBatch = generateMessages({ number: 20 })
    await producer.send({ topic, messages: secondBatch })

    // 5. assert every record of the new incarnation is delivered, starting at offset 0. Coverage
    // (distinct offsets), not raw count, tolerates an ordinary rebalance redelivering one.
    const newIncarnation = () => messagesConsumed.slice(firstBatch.length)
    const gotEverything = await waitForCondition(
      () => distinctOffsets(newIncarnation()).length >= secondBatch.length,
      { attempts: 60 }
    )
    expect(gotEverything).toBe(true)

    // 4. assert TOPIC_RECREATED fired with previousTopicId !== topicId
    expect(recreatedEvents).toHaveLength(1)
    expect(recreatedEvents[0].topic).toEqual(topic)
    expect(recreatedEvents[0].previousTopicId).not.toEqual(recreatedEvents[0].topicId)

    expect(distinctOffsets(newIncarnation())).toEqual(
      Array.from({ length: secondBatch.length }, (_, i) => i)
    )
  })

  test('a stale committed offset inside the recreated topic range is ignored', async () => {
    await admin.connect()
    await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true })

    producer = kafka.producer()
    await producer.connect()

    // 1. produce 10 records, consume all 10, commit (committed offset = 10)
    const firstBatch = generateMessages({ number: 10 })
    await producer.send({ topic, messages: firstBatch })

    consumer = kafka.consumer({ groupId })
    const messagesConsumed = []
    await consumer.subscribe({ topic, fromBeginning: true })
    consumer.run({
      autoCommit: false,
      eachMessage: async (payload) => messagesConsumed.push(payload),
    })
    await waitForConsumerToJoinGroup(consumer)

    const gotFirstBatch = await waitForCondition(() => messagesConsumed.length >= firstBatch.length)
    expect(gotFirstBatch).toBe(true)

    await consumer.commitOffsets([{ topic, partition: 0, offset: String(firstBatch.length) }])

    // 2. delete and recreate the topic
    await admin.deleteTopics({ topics: [topic], timeout: 10000 })
    const topicGone = await waitForCondition(
      async () => !(await admin.listTopics()).includes(topic)
    )
    expect(topicGone).toBe(true)
    await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true })

    // 3. produce 20 records to the new incarnation
    const secondBatch = generateMessages({ number: 20 })
    await producer.send({ topic, messages: secondBatch })

    // 4. subscribe fromBeginning: true and assert all 20 are delivered - without the one-shot
    // reset this delivers only offsets 10-19 (10 records), silently skipping the first 10 of the
    // new incarnation because the stale committed offset of 10 is still valid there. Coverage
    // (distinct offsets), not raw count, tolerates an ordinary rebalance redelivering one; the
    // bounded wait (not an indefinite one) means a regression fails on the assertion below, not a
    // timeout.
    const newIncarnation = () => messagesConsumed.slice(firstBatch.length)
    await waitForCondition(() => distinctOffsets(newIncarnation()).length >= secondBatch.length, {
      attempts: 60,
    })

    expect(distinctOffsets(newIncarnation())).toEqual(
      Array.from({ length: secondBatch.length }, (_, i) => i)
    )
  })
})
