const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')
const { sleep, waitForCondition, withTimeout, createLedger } = require('./fixtures/liveScenario')

// Gated: this scenario creates topics while a group is running and would race the other consumer
// specs. Run it alone: `npm run test:e2e:regex-subscription`.
const describeRegexSubscription =
  process.env.REGEX_SUBSCRIPTION_E2E === '1' ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const ROW_BUDGET_MS = 180000
const PARTITIONS = 2
const MAX_WAIT_TIME_MS = 1000
const ASSIGNMENT_BUDGET = { attempts: 60, delay: 500 } // 30 s
// The coordinator re-evaluates a group's patterns when the metadata image gains a topic, but no
// more often than every 10 s, so a topic created after run() is picked up on that tick rather than
// at once. 60 s is six of those.
// @see https://github.com/apache/kafka/blob/4.3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java REGEX_BATCH_REFRESH_MIN_INTERVAL_MS
const SECOND_TOPIC_BUDGET = { attempts: 120, delay: 500 } // 60 s
const DELIVERY_BUDGET = { attempts: 120, delay: 500 } // 60 s
const CRASH_BUDGET = { attempts: 40, delay: 250 } // 10 s
// A deliberate idle stretch between the first assignment and the second topic's creation, so
// "the client is not polling metadata looking for topics" is measured over a window.
const QUIET_WINDOW_MS = 3000
const DISCONNECT_BUDGET_MS = 15000
const HOOK_BUDGET_MS = 120000

/**
 * Live proof that a broker-evaluated subscription regex behaves the way KIP-848 says it does: the
 * pattern goes to the coordinator verbatim, the coordinator resolves it, and a topic created after
 * `run()` arrives in the assignment without the client ever going looking for it.
 *
 * Four things are asserted. A member subscribed only by pattern reaches its first assignment; a
 * topic created later is picked up, its records delivered, and no `Metadata` request is sent while
 * the member sits idle waiting for it; `fromBeginning` is honoured on a topic whose name the
 * subscription only resolved after records were already in it; and a pattern RE2 rejects crashes
 * the consumer once, with the Java client's message prefix and without restarting.
 *
 * Assignment is read from `PARTITIONS_ASSIGNED` and unioned across rounds: the consumer protocol
 * is cooperative, so each event carries only that round's delta, and "which topics has this member
 * ever been given" is exactly what the union answers.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 */
describeRegexSubscription('Consumer > regex subscription (live)', () => {
  let kafka
  let producer
  let prefix
  let topicA
  let topicB
  let groupId
  /** @type {object[]} */
  let consumers

  /**
   * Builds the single member of this group and records what it sees. `restartOnFailure` answers
   * yes deliberately: a fatal membership error has to override it, and the invalid-pattern case is
   * what proves it does.
   * @returns {{consumer: object, observed: object}}
   */
  const createMember = () => {
    const consumer = kafka.consumer({
      groupId,
      groupProtocol: 'consumer',
      maxWaitTimeInMs: MAX_WAIT_TIME_MS,
      retry: { retries: 0, restartOnFailure: async () => true },
    })

    const observed = { assigned: [], groupJoins: [], crashes: [], unsubscribed: [], requests: [] }

    consumer.on(consumer.events.PARTITIONS_ASSIGNED, ({ timestamp, payload }) =>
      observed.assigned.push({ timestamp, payload })
    )
    consumer.on(consumer.events.GROUP_JOIN, ({ timestamp }) => observed.groupJoins.push(timestamp))
    consumer.on(consumer.events.CRASH, ({ payload }) => observed.crashes.push(payload))
    consumer.on(consumer.events.RECEIVED_UNSUBSCRIBED_TOPICS, ({ payload }) =>
      observed.unsubscribed.push(payload)
    )
    consumer.on(consumer.events.REQUEST, ({ payload }) =>
      observed.requests.push({ apiName: payload.apiName, sentAt: payload.sentAt })
    )

    consumers.push(consumer)
    return { consumer, observed }
  }

  /**
   * Every topic this member has ever been assigned a partition of.
   * @param {object} observed
   * @returns {string[]}
   */
  const assignedTopics = (observed) => [
    ...new Set(observed.assigned.flatMap(({ payload }) => payload.partitions.map((p) => p.topic))),
  ]

  /**
   * The `Metadata` requests this member sent inside a window, by the time they left the socket.
   * Read after the window has closed, so a request that was still in flight is counted too: the
   * event carries the round trip and is emitted on completion.
   * @param {object} observed
   * @param {number} from inclusive, epoch ms
   * @param {number} to inclusive, epoch ms
   * @returns {number[]} the `sentAt` of each request, so a failure names when it happened
   */
  const metadataSentBetween = (observed, from, to) =>
    observed.requests
      .filter(({ apiName, sentAt }) => apiName === 'Metadata' && sentAt >= from && sentAt <= to)
      .map(({ sentAt }) => sentAt)

  /**
   * Produces one record and records it in the ledger under the offset the broker assigned.
   * @param {object} ledger
   * @param {string} topic
   * @param {string} value
   * @returns {Promise<void>}
   */
  const produceOne = async (ledger, topic, value) => {
    const produced = await producer.send({
      topic,
      messages: [{ key: value, value, partition: 0 }],
    })

    for (const { partition, baseOffset } of produced) {
      ledger.produced({ topic, partition, offset: `${baseOffset}` })
    }
  }

  beforeEach(async () => {
    prefix = `e2e-regex-${secureRandom()}`
    topicA = `${prefix}-a`
    topicB = `${prefix}-b`
    groupId = `${prefix}-group`
    consumers = []

    kafka = new Kafka({
      clientId: 'regex-subscription-live',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    producer = kafka.producer()

    await producer.connect()
    await createTopic({ topic: topicA, partitions: PARTITIONS })
  }, HOOK_BUDGET_MS)

  afterEach(async () => {
    // Even on failure, and one at a time: a live member left in a group poisons every following
    // row, and a rejected disconnect must not skip the rest of the teardown.
    for (const consumer of consumers) {
      await withTimeout(
        consumer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }

    producer &&
      (await withTimeout(
        producer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      ))
    producer = kafka = null
    consumers = []
  }, HOOK_BUDGET_MS)

  it(
    'starts cleanly with no target topics and reaches its first assignment',
    async () => {
      const { consumer, observed } = createMember()

      await consumer.connect()
      await consumer.subscribe({ regex: `^${prefix}-.*`, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      // A regex subscription registers no target topic, so the group is constructed with an empty
      // topic list and the first metadata refresh runs over nothing.
      expect(await waitForCondition(() => observed.groupJoins.length > 0, ASSIGNMENT_BUDGET)).toBe(
        true
      )
      expect(observed.crashes).toEqual([])
      expect(
        await waitForCondition(() => assignedTopics(observed).includes(topicA), ASSIGNMENT_BUDGET)
      ).toBe(true)
    },
    ROW_BUDGET_MS
  )

  it(
    'picks up a topic created after run(), without polling metadata for it',
    async () => {
      const ledger = createLedger()
      const { consumer, observed } = createMember()

      await consumer.connect()
      await consumer.subscribe({ regex: `^${prefix}-.*`, fromBeginning: true })
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) =>
          ledger.delivered('regex', { topic, partition, offset: message.offset }),
      })

      expect(
        await waitForCondition(() => assignedTopics(observed).includes(topicA), ASSIGNMENT_BUDGET)
      ).toBe(true)

      // The member is settled on the first topic and nothing else happens for a while. A
      // client-side pattern would have to keep asking the cluster for topic names through here.
      const quietFrom = Date.now()
      await sleep(QUIET_WINDOW_MS)
      const quietUntil = Date.now()

      // Created through a separate short-lived admin client, so nothing this member holds is
      // touched by the creation itself.
      await createTopic({ topic: topicB, partitions: PARTITIONS })
      await produceOne(ledger, topicB, 'b-0')

      expect(
        await waitForCondition(() => assignedTopics(observed).includes(topicB), SECOND_TOPIC_BUDGET)
      ).toBe(true)
      expect(
        await waitForCondition(() => ledger.summary().missing.length === 0, DELIVERY_BUDGET)
      ).toBe(true)

      expect(ledger.summary().delivered).toBeGreaterThan(0)
      // The coordinator matched the pattern and the member was told; it never reported the new
      // topic as one it had not subscribed to, which is what the widened subscription prevents.
      expect(observed.unsubscribed).toEqual([])
      expect(metadataSentBetween(observed, quietFrom, quietUntil)).toEqual([])
      expect(observed.crashes).toEqual([])
    },
    ROW_BUDGET_MS
  )

  it(
    'honours fromBeginning on a topic that was produced to before it was assigned',
    async () => {
      const ledger = createLedger()

      await createTopic({ topic: topicB, partitions: PARTITIONS })
      await produceOne(ledger, topicB, 'early')

      const { consumer, observed } = createMember()
      await consumer.connect()
      await consumer.subscribe({ regex: `^${prefix}-.*`, fromBeginning: true })
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) =>
          ledger.delivered('regex', { topic, partition, offset: message.offset }),
      })

      expect(
        await waitForCondition(() => assignedTopics(observed).includes(topicB), SECOND_TOPIC_BUDGET)
      ).toBe(true)
      expect(
        await waitForCondition(() => ledger.summary().missing.length === 0, DELIVERY_BUDGET)
      ).toBe(true)

      // The offset produced before the subscription resolved the name is delivered, which is what
      // `fromBeginning` has to mean for a topic the regex only later matched.
      expect(ledger.summary().delivered).toBeGreaterThan(0)
      expect(ledger.summary().missing).toEqual([])
      expect(observed.crashes).toEqual([])
    },
    ROW_BUDGET_MS
  )

  it(
    'crashes without restarting on a pattern the broker rejects',
    async () => {
      const { consumer, observed } = createMember()

      await consumer.connect()
      // A JavaScript-legal pattern RE2 rejects: back-references are not in RE2's grammar.
      await consumer.subscribe({ regex: '(a)\\1', fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      expect(await waitForCondition(() => observed.crashes.length > 0, CRASH_BUDGET)).toBe(true)

      const [crash] = observed.crashes
      expect(crash.restart).toBe(false)
      expect(crash.error.message).toMatch(
        /^Invalid RE2J SubscriptionPattern provided in the call to subscribe\. /
      )
      // `restart: false` means it: nothing joined before the crash and nothing rejoins after.
      expect(observed.groupJoins).toEqual([])
      expect(observed.crashes).toHaveLength(1)
    },
    ROW_BUDGET_MS
  )
})
