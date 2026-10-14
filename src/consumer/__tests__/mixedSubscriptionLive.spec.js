const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic, writeEvidence } = require('testHelpers')
const { waitForCondition, withTimeout, createLedger } = require('./fixtures/liveScenario')

// Gated: it runs two-member classic groups on a cold cluster. Run it alone through
// `npm run test:e2e:mixed-subscription`.
const describeMixedSubscription =
  process.env.MIXED_SUBSCRIPTION_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'mixed-subscription'
// Named so each wait says how long it is prepared to be wrong for.
const PARTITIONS = 4
const MESSAGES_PER_PARTITION = 3
const LEADER_BUDGET = { attempts: 120, delay: 250 } // 30 s for the first member to lead
const SETTLE_BUDGET = { attempts: 120, delay: 250 } // 30 s for both members to join one round
const CALM_POLLS = 8 // 2 s without a rebalance on either member
const DELIVERY_BUDGET = { attempts: 120, delay: 250 } // 30 s for every record to arrive
const SESSION_TIMEOUT = 30000
const LEADER_NAME = 'a-and-b'
const OTHER_NAME = 'a-only'

/**
 * Live proof that a classic group whose members subscribe to different topics, under the default
 * `roundRobin` assigner, gives each partition only to a member subscribed to its topic. One member
 * subscribes to topics A and B, the other to A alone, and the one subscribed to both leads, as the
 * member that starts first. Every record produced to either topic is delivered exactly once, by a
 * subscriber, and neither member reports `RECEIVED_UNSUBSCRIBED_TOPICS`.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/RoundRobinAssignor.java
 */
describeMixedSubscription('Consumer > roundRobin with mixed subscriptions (live, classic)', () => {
  const kafka = new Kafka({
    clientId: 'mixed-subscription-live',
    brokers: plainTextBrokers(),
    logLevel: logLevel.NOTHING,
  })
  const producer = kafka.producer()
  const consumers = []

  beforeAll(async () => {
    await producer.connect()
  }, 60000)

  afterAll(async () => {
    for (const consumer of consumers) {
      await withTimeout(
        consumer.disconnect().catch(() => {}),
        10000
      )
    }
    await producer.disconnect().catch(() => {})
  }, 60000)

  test('with the member subscribed to both topics leading, delivers every record of both topics once, to a subscriber, and no member receives a topic it does not subscribe to', async () => {
    const runId = secureRandom()
    const topicName = (letter) => `e2e-mixed-subscription-${letter}-${runId}`
    const [topicA, topicB] = [topicName('a'), topicName('b')]
    const groupId = `e2e-mixed-subscription-${runId}`
    const ledger = createLedger()
    /** @type {Array<{ member: string, topic: string }>} */
    const deliveries = []

    await createTopic({ topic: topicA, partitions: PARTITIONS })
    await createTopic({ topic: topicB, partitions: PARTITIONS })

    /**
     * Starts a member of the group subscribed to `topics` and records what it sees.
     * @param {string} name how the member is reported
     * @param {string[]} topics
     * @returns {Promise<{ name: string, joins: object[], rebalances: number,
     *   unsubscribed: object[] }>}
     */
    const start = async (name, topics) => {
      const consumer = kafka.consumer({
        groupId,
        maxWaitTimeInMs: 500,
        sessionTimeout: SESSION_TIMEOUT,
        heartbeatInterval: 1000,
      })
      const member = { name, joins: [], rebalances: 0, unsubscribed: [] }
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => member.joins.push(payload))
      consumer.on(consumer.events.REBALANCING, () => (member.rebalances += 1))
      consumer.on(consumer.events.RECEIVED_UNSUBSCRIBED_TOPICS, ({ payload }) =>
        member.unsubscribed.push(payload)
      )
      consumers.push(consumer)

      await consumer.connect()
      await consumer.subscribe({ topics, fromBeginning: true })
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          ledger.delivered(name, { topic, partition, offset: message.offset })
          deliveries.push({ member: name, topic })
        },
      })
      return member
    }

    /** @returns {object|undefined} the member's last GROUP_JOIN payload */
    const lastJoin = ({ joins }) => joins[joins.length - 1]

    /** @returns {number[]} the partitions of `topic` the member's last join gave it */
    const held = (member, topic) => (lastJoin(member) || {}).memberAssignment?.[topic] || []

    const leader = await start(LEADER_NAME, [topicA, topicB])
    expect(await waitForCondition(() => lastJoin(leader)?.isLeader === true, LEADER_BUDGET)).toBe(
      true
    )

    const other = await start(OTHER_NAME, [topicA])
    const settled = await waitForCondition(
      () =>
        lastJoin(other) !== undefined &&
        lastJoin(other).leaderId === lastJoin(leader).memberId &&
        held(leader, topicA).length + held(other, topicA).length === PARTITIONS,
      SETTLE_BUDGET
    )
    expect(settled).toBe(true)

    let last = -1
    let calm = 0
    const calmed = await waitForCondition(() => {
      const rebalances = leader.rebalances + other.rebalances
      calm = rebalances === last ? calm + 1 : 0
      last = rebalances
      return calm >= CALM_POLLS
    }, SETTLE_BUDGET)
    expect(calmed).toBe(true)

    for (const produced of [topicA, topicB]) {
      for (let partition = 0; partition < PARTITIONS; partition++) {
        const [{ baseOffset }] = await producer.send({
          topic: produced,
          messages: Array.from({ length: MESSAGES_PER_PARTITION }, (_, i) => ({
            partition,
            value: `${produced}-${partition}-${i}`,
          })),
        })
        for (let i = 0; i < MESSAGES_PER_PARTITION; i++) {
          ledger.produced({
            topic: produced,
            partition,
            offset: String(BigInt(baseOffset) + BigInt(i)),
          })
        }
      }
    }

    await waitForCondition(() => ledger.summary().missing.length === 0, DELIVERY_BUDGET)
    const summary = ledger.summary()
    const record = {
      case: `mixed-subscription-round-robin-${LEADER_NAME}-leads`,
      leader: lastJoin(leader).isLeader ? leader.name : other.name,
      assignments: {
        [leader.name]: lastJoin(leader).memberAssignment,
        [other.name]: lastJoin(other).memberAssignment,
      },
      produced: summary.produced,
      delivered: summary.delivered,
      missing: summary.missing.length,
      duplicates: summary.duplicates.length,
      unsubscribedEvents: leader.unsubscribed.length + other.unsubscribed.length,
      droppedTopics: [...leader.unsubscribed, ...other.unsubscribed].flatMap(
        ({ topicsNotSubscribed }) => topicsNotSubscribed
      ),
    }
    writeEvidence(EVIDENCE, record)

    expect(record.leader).toEqual(LEADER_NAME)
    expect(held(leader, topicB)).toHaveLength(PARTITIONS)
    expect(record.unsubscribedEvents).toEqual(0)
    expect(summary.missing).toEqual([])
    expect(summary.duplicates).toEqual([])
    expect(summary.produced).toEqual(2 * PARTITIONS * MESSAGES_PER_PARTITION)
    expect(
      deliveries.filter(({ member, topic }) => member === OTHER_NAME && topic !== topicA)
    ).toEqual([])
  }, 150000)
})
