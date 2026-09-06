const createProducer = require('../../producer')
const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  createModPartitioner,
  newLogger,
  waitForMessages,
  waitForConsumerToJoinGroup,
  addPartitions,
} = require('testHelpers')

const MAX_WAIT_TIME_IN_MS = 100

// Wall clock for "partition change -> rejoin -> first batch of the new assignment". Generous
// against a loaded host, and far below the 90s a stalled round would take to fail the suite.
const POST_REBALANCE_BUDGET_MS = 20000

/**
 * @param {number} count
 * @returns {{key: string, value: string}[]}
 */
const randomMessages = (count) =>
  Array(count)
    .fill()
    .map(() => {
      const value = secureRandom()
      return { key: `key-${value}`, value: `value-${value}` }
    })

describe('Consumer', () => {
  let topicName, groupId, producer, consumer1, consumer2

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    producer = createProducer({
      cluster: createCluster({ metadataMaxAge: 20 }),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    consumer1 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 50 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: MAX_WAIT_TIME_IN_MS,
      logger: newLogger(),
    })

    consumer2 = createConsumer({
      cluster: createCluster({ metadataMaxAge: 3000 }),
      groupId,
      heartbeatInterval: 100,
      maxWaitTimeInMs: MAX_WAIT_TIME_IN_MS,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer1 && (await consumer1.disconnect())
    consumer2 && (await consumer2.disconnect())
    producer && (await producer.disconnect())
  })

  test('trigger rebalance if partitions change after metadata update', async () => {
    await consumer1.connect()
    await consumer2.connect()
    await producer.connect()

    await consumer1.subscribe({ topic: topicName, fromBeginning: true })
    await consumer2.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer1.run({ eachMessage: async (event) => messagesConsumed.push(event) })
    consumer2.run({ eachMessage: async (event) => messagesConsumed.push(event) })

    await Promise.all([
      waitForConsumerToJoinGroup(consumer1),
      waitForConsumerToJoinGroup(consumer2),
    ])

    await producer.send({ acks: 1, topic: topicName, messages: randomMessages(2) })
    await waitForMessages(messagesConsumed, { number: 2 })

    await addPartitions({ topic: topicName, partitions: 4 })

    await producer.send({ acks: 1, topic: topicName, messages: randomMessages(6) })
    await waitForMessages(messagesConsumed, { number: 8 })
  })

  test('delivers the first batch after a rebalance within the normal fetch budget', async () => {
    await consumer1.connect()
    await consumer2.connect()
    await producer.connect()

    await consumer1.subscribe({ topic: topicName, fromBeginning: true })
    await consumer2.subscribe({ topic: topicName, fromBeginning: true })

    const messagesConsumed = []
    consumer1.run({ eachMessage: async (event) => messagesConsumed.push(event) })
    consumer2.run({ eachMessage: async (event) => messagesConsumed.push(event) })

    await Promise.all([
      waitForConsumerToJoinGroup(consumer1),
      waitForConsumerToJoinGroup(consumer2),
    ])

    await producer.send({ acks: 1, topic: topicName, messages: randomMessages(2) })
    await waitForMessages(messagesConsumed, { number: 2 })

    // The clock covers the whole window the change touches: the partition change, the rejoin, and
    // the first fetch round of the new assignment - the one that now carries one
    // OffsetForLeaderEpoch per leader on a blocking pre-fetch gate. Measuring only what follows
    // the rejoin would measure a steady-state round the gate has already left.
    const started = Date.now()
    await addPartitions({ topic: topicName, partitions: 4 })
    await producer.send({ acks: 1, topic: topicName, messages: randomMessages(6) })
    await waitForMessages(messagesConsumed, { number: 8 })

    expect(Date.now() - started).toBeLessThan(POST_REBALANCE_BUDGET_MS)
  })
})
