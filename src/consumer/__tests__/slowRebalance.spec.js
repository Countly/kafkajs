const createConsumer = require('../index')
const createProducer = require('../../producer')
const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

// The coordinator holds a follower's SyncGroup pending until the leader delivers the group
// assignment, which can take up to rebalanceTimeout. With the client-wide requestTimeout (30s
// default, here lowered to make the test fast) that follower request dies client-side as
// KafkaJSRequestTimeoutError, and the consumer rejoins into a non-converging loop. SyncGroup
// must inherit rebalanceTimeout + 5s instead, exactly like JoinGroup already does.
//
// A single consumer can't exercise this: the leader computes its assignment locally and only
// then sends SyncGroup, so the slow part never touches the network. Reproducing the real bug
// needs a second (follower) member whose SyncGroup is sent immediately and held by the broker
// while the leader is still assigning.
const SLOW_ASSIGN_MS = 8000
const REQUEST_TIMEOUT = 3000
const REBALANCE_TIMEOUT = 20000

const slowRoundRobin =
  (roundRobin) =>
  ({ cluster }) => {
    const inner = roundRobin({ cluster })
    return {
      ...inner,
      async assign(group) {
        await new Promise((resolve) => setTimeout(resolve, SLOW_ASSIGN_MS))
        return inner.assign(group)
      },
    }
  }

describe('Consumer > slow rebalance', () => {
  let topicName, groupId, consumer1, consumer2, producer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
    await createTopic({ topic: topicName, partitions: 2 })
  })

  afterEach(async () => {
    consumer1 && (await consumer1.disconnect())
    consumer2 && (await consumer2.disconnect())
    producer && (await producer.disconnect())
  })

  it(
    'completes a rebalance whose SyncGroup wait exceeds requestTimeout',
    async () => {
      const { roundRobin } = require('../assigners')

      // consumer1 joins first so it is elected group leader, and carries the slow assigner: its
      // SyncGroup will only be sent to the broker after SLOW_ASSIGN_MS.
      consumer1 = createConsumer({
        cluster: createCluster({ requestTimeout: REQUEST_TIMEOUT }),
        groupId,
        logger: newLogger(),
        rebalanceTimeout: REBALANCE_TIMEOUT,
        sessionTimeout: REBALANCE_TIMEOUT,
        partitionAssigners: [slowRoundRobin(roundRobin)],
        maxWaitTimeInMs: 100,
      })

      // consumer2 joins as a follower with the default (fast) assigner. Its SyncGroup is sent
      // immediately and is held by the broker until the leader (consumer1) submits its assignment.
      consumer2 = createConsumer({
        cluster: createCluster({ requestTimeout: REQUEST_TIMEOUT }),
        groupId,
        logger: newLogger(),
        rebalanceTimeout: REBALANCE_TIMEOUT,
        sessionTimeout: REBALANCE_TIMEOUT,
        maxWaitTimeInMs: 100,
      })

      await consumer1.connect()
      await consumer1.subscribe({ topic: topicName, fromBeginning: true })
      const messages = []
      consumer1.run({ eachMessage: async ({ message }) => messages.push(message) })

      // Give consumer1's JoinGroup a head start so it is deterministically the first (and thus
      // leader) member of the generation instead of racing consumer2 for the role.
      await new Promise((resolve) => setTimeout(resolve, 1000))

      await consumer2.connect()
      await consumer2.subscribe({ topic: topicName, fromBeginning: true })
      consumer2.run({ eachMessage: async ({ message }) => messages.push(message) })

      await Promise.all([
        waitForConsumerToJoinGroup(consumer1, { maxWait: REBALANCE_TIMEOUT, label: 'consumer1' }),
        waitForConsumerToJoinGroup(consumer2, { maxWait: REBALANCE_TIMEOUT, label: 'consumer2' }),
      ])

      producer = createProducer({ cluster: createCluster(), logger: newLogger() })
      await producer.connect()
      await producer.send({
        topic: topicName,
        messages: [
          { key: 'k0', value: 'v0', partition: 0 },
          { key: 'k1', value: 'v1', partition: 1 },
        ],
      })

      await expect(
        new Promise((resolve) => {
          const t = setInterval(() => {
            if (messages.length >= 2) {
              clearInterval(t)
              resolve(true)
            }
          }, 100)
        })
      ).resolves.toBe(true)
    },
    REBALANCE_TIMEOUT + 15000
  )
})
