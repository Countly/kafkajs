const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic, writeEvidence } = require('testHelpers')
const { waitForCondition, withTimeout } = require('./fixtures/liveScenario')

// Gated: it runs real consumer groups on a cold cluster. Run it alone through
// `npm run test:e2e:offsets-topic-id`; `KAFKA_VERSION` names the cluster under test.
const describeOffsetsTopicId = process.env.OFFSETS_TOPIC_ID_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'offsets-topic-id'
// Named so each wait says how long it is prepared to be wrong for: the file has 90 s, a case 40 s.
const SCENARIO_BUDGET_MS = 90000
const CASE_BUDGET_MS = 40000
const DELIVERY_BUDGET = { attempts: 120, delay: 250 } // 30 s
const DISCONNECT_BUDGET_MS = 10000

const [MAJOR, MINOR] = process.env.KAFKA_VERSION.split('.').map(Number)
// OffsetCommit and OffsetFetch v10 are stable from Kafka 4.2; a 4.0 or 4.1 broker does not
// advertise them, so the same client negotiates v9 there.
const OFFSETS_VERSION = MAJOR > 4 || (MAJOR === 4 && MINOR >= 2) ? 10 : 9
const OFFSETS_APIS = ['OffsetCommit', 'OffsetFetch']

/**
 * Live proof of the version an offsets request goes out at: a consumer on the consumer group
 * protocol commits and fetches by topic id where the broker advertises v10 and by name where it does
 * not, a classic consumer always by name, and what the first commits the second reads back, so the
 * v10 round trip is real. Versions are read from `REQUEST` events: counts and versions are asserted,
 * never durations.
 *
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java
 */
describeOffsetsTopicId('Consumer > offsets by topic id (live)', () => {
  const runId = secureRandom()
  const newKafka = (clientId) =>
    new Kafka({ clientId, brokers: plainTextBrokers(), logLevel: logLevel.NOTHING })
  const kafka = newKafka('offsets-topic-id-live')
  const admin = kafka.admin()
  const producer = kafka.producer()
  const members = []
  let startedAt

  /**
   * A consumer whose offsets requests are recorded by API name and version. Consumers of one
   * `Kafka` instance share the committed offsets they hold in memory, so a member that must read
   * its offsets from the broker is built from an instance of its own.
   * @param {string} groupId
   * @param {boolean} byConsumerProtocol
   * @param {object} [client] the `Kafka` instance the consumer is built from
   * @returns {{ consumer: object, delivered: string[], versions: Object<string, Set<number>> }}
   */
  const member = (groupId, byConsumerProtocol, client = kafka) => {
    const consumer = client.consumer({
      groupId,
      maxWaitTimeInMs: 500,
      ...(byConsumerProtocol ? { groupProtocol: 'consumer' } : {}),
    })
    const observed = {
      consumer,
      delivered: [],
      versions: { OffsetCommit: new Set(), OffsetFetch: new Set() },
    }
    consumer.on(consumer.events.REQUEST, ({ payload: { apiName, apiVersion } }) => {
      if (OFFSETS_APIS.includes(apiName)) observed.versions[apiName].add(apiVersion)
    })
    members.push(consumer)
    return observed
  }

  const start = async (observed, topic) => {
    await observed.consumer.connect()
    await observed.consumer.subscribe({ topic, fromBeginning: true })
    await observed.consumer.run({
      eachMessage: async ({ message }) => observed.delivered.push(message.offset),
    })
  }

  /** @returns {Promise<void>} resolves when `count` records have reached `observed`, or fails the case */
  const delivered = async (observed, count) =>
    expect(await waitForCondition(() => observed.delivered.length >= count, DELIVERY_BUDGET)).toBe(
      true
    )

  const produce = (topic, from, count) =>
    producer.send({
      topic,
      messages: Array.from({ length: count }, (_, index) => ({ value: `m-${from + index}` })),
    })

  const versionsOf = (observed) =>
    Object.fromEntries(OFFSETS_APIS.map((api) => [api, [...observed.versions[api]].sort()]))

  beforeAll(async () => {
    await Promise.all([admin.connect(), producer.connect()])
    startedAt = Date.now()
  }, CASE_BUDGET_MS)

  afterEach(() => {
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  })

  afterAll(async () => {
    for (const consumer of members) {
      await withTimeout(
        consumer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }
    await Promise.all([admin.disconnect().catch(() => {}), producer.disconnect().catch(() => {})])
  }, 60000)

  test(
    'a consumer-protocol member commits and fetches by topic id from Kafka 4.2, by name before, and a second member resumes from its commit',
    async () => {
      const topic = `e2e-p8-offsets-topic-id-${runId}-cp`
      const groupId = `${topic}-group`
      await createTopic({ topic, partitions: 1 })
      await produce(topic, 0, 5)

      const first = member(groupId, true)
      await start(first, topic)
      await delivered(first, 5)
      await first.consumer.commitOffsets([{ topic, partition: 0, offset: '5' }])
      await first.consumer.disconnect()

      // Read by the admin, which never sends ids, at v9
      const [{ partitions }] = await admin.fetchOffsets({ groupId, topics: [topic] })
      expect(partitions).toEqual([{ partition: 0, offset: '5', metadata: null }])

      await produce(topic, 5, 3)
      const second = member(groupId, true, newKafka('offsets-topic-id-live-reader'))
      await start(second, topic)
      await delivered(second, 3)

      expect(second.delivered).toEqual(['5', '6', '7'])
      await second.consumer.commitOffsets([{ topic, partition: 0, offset: '8' }])
      writeEvidence(EVIDENCE, {
        case: 'consumer-protocol',
        kafkaVersion: process.env.KAFKA_VERSION,
        expected: OFFSETS_VERSION,
        first: versionsOf(first),
        second: versionsOf(second),
      })
      for (const observed of [first, second]) {
        expect(versionsOf(observed)).toEqual({
          OffsetCommit: [OFFSETS_VERSION],
          OffsetFetch: [OFFSETS_VERSION],
        })
      }
    },
    CASE_BUDGET_MS
  )

  test(
    'a classic member commits and fetches by name on every broker',
    async () => {
      const topic = `e2e-p8-offsets-topic-id-${runId}-classic`
      const groupId = `${topic}-group`
      await createTopic({ topic, partitions: 1 })
      await produce(topic, 0, 5)

      const classic = member(groupId, false)
      await start(classic, topic)
      await delivered(classic, 5)
      await classic.consumer.commitOffsets([{ topic, partition: 0, offset: '5' }])

      writeEvidence(EVIDENCE, {
        case: 'classic',
        kafkaVersion: process.env.KAFKA_VERSION,
        versions: versionsOf(classic),
      })
      expect(versionsOf(classic)).toEqual({ OffsetCommit: [9], OffsetFetch: [9] })
    },
    CASE_BUDGET_MS
  )
})
