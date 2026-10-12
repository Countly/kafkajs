const Broker = require('../../broker')
const { Kafka, logLevel } = require('../../../index')
const { createCluster, createTopic, plainTextBrokers, writeEvidence } = require('testHelpers')
const { sleep, waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it creates topics on a live cluster. Run alone: `npm run test:e2e:describe-topic-partitions`.
const describeDtp = process.env.DTP_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'describe-topic-partitions'
// Named so each wait says how long it is prepared to be wrong for. The scenario budget is the
// whole file, from the first case to the last; a case has its own, shorter one.
const SCENARIO_BUDGET_MS = 60000
const CASE_BUDGET_MS = 30000
const HOOK_BUDGET_MS = 30000
// Two reads a moment apart may meet an in-sync set that is still changing right after a topic was
// created, so the comparison is repeated until both reads agree.
const AGREEMENT_BUDGET = { attempts: 40, delay: 250 } // 10 s
const QUIET_MS = 1500 // how long a topic that must not appear is waited for
const MAX_PAGES = 40
const PARTITIONS_A = 5
const PARTITIONS_B = 7
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

/**
 * Live proof of DescribeTopicPartitions: a broker pages through two topics one partition at a time
 * and ends on a null cursor, and `fetchTopicMetadata({ useDescribeTopicPartitions: true })` answers
 * what the Metadata path answers for leaders, replicas, in-sync replicas and topic ids, plus the
 * keys only DescribeTopicPartitions has. A topic that does not exist fails the call and is not
 * created by it.
 *
 * The admin asks for 2000 partitions a page, which a three-broker test cluster answers in one page,
 * so `Broker#describeTopicPartitions` is wrapped for the whole file to send a page limit of 1: the
 * admin's loop then runs against a real broker for 12 pages, and the case that checks it counts them.
 *
 * The first case drives the pages itself, from the cursor the broker names, so that it tests the
 * broker's paging and not the admin's loop: the topics still to read are the ones whose names do
 * not sort before the cursor topic, and the cursor topic stays in the list.
 *
 * @see https://github.com/apache/kafka/blob/4.3.1/core/src/main/java/kafka/server/handlers/DescribeTopicPartitionsRequestHandler.java
 * @see https://github.com/apache/kafka/blob/4.3.1/metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java
 */
describeDtp('Admin > fetchTopicMetadata > DescribeTopicPartitions (live)', () => {
  const runId = Date.now().toString(36)
  const topicA = `e2e-p8-dtp-${runId}-a`
  const topicB = `e2e-p8-dtp-${runId}-b`
  const missing = `e2e-p8-dtp-${runId}-missing`
  const topics = [topicA, topicB]
  let cluster
  let broker
  let admin
  let pageLimit
  let startedAt
  /** @type {object[]} what `Broker#describeTopicPartitions` was asked, before the limit is forced */
  const asked = []

  /**
   * What two reads of the same topics must agree on, in a stable order.
   * @param {{ name: string, topicId: string|null, partitions: object[] }[]} described
   */
  const comparable = (described) =>
    [...described]
      .sort((x, y) => (x.name < y.name ? -1 : 1))
      .map(({ name, topicId, partitions }) => ({
        name,
        topicId,
        partitions: [...partitions]
          .sort((x, y) => x.partitionId - y.partitionId)
          .map(({ partitionId, leader, replicas, isr }) => ({
            partitionId,
            leader,
            replicas,
            isr: [...isr].sort((x, y) => x - y),
          })),
      }))

  beforeAll(async () => {
    cluster = createCluster()
    await cluster.connect()
    await cluster.refreshMetadata()
    broker = await cluster.findControllerBroker()
    admin = new Kafka({
      clientId: 'describe-topic-partitions-live',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    }).admin()
    await admin.connect()
    await createTopic({ topic: topicA, partitions: PARTITIONS_A, replicas: 2 })
    await createTopic({ topic: topicB, partitions: PARTITIONS_B, replicas: 2 })
    const describeTopicPartitions = Broker.prototype.describeTopicPartitions
    pageLimit = jest
      .spyOn(Broker.prototype, 'describeTopicPartitions')
      .mockImplementation(function (request) {
        asked.push(request)
        return describeTopicPartitions.call(this, { ...request, responsePartitionLimit: 1 })
      })
    startedAt = Date.now()
  }, HOOK_BUDGET_MS)

  afterEach(() => {
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  })

  afterAll(async () => {
    pageLimit.mockRestore()
    await admin.deleteTopics({ topics }).catch(() => {})
    await admin.disconnect()
    await cluster.disconnect()
  }, HOOK_BUDGET_MS)

  test(
    'a broker pages through 12 partitions one at a time and ends on a null cursor',
    async () => {
      const read = new Map(topics.map((name) => [name, []]))
      let pending = [...topics].sort()
      let cursor = null
      let pages = 0

      do {
        const page = await broker.describeTopicPartitions({
          topics: pending,
          responsePartitionLimit: 1,
          cursor,
        })
        pages += 1
        for (const { name, errorCode, partitions } of page.topics) {
          expect(errorCode).toBe(0)
          read.get(name).push(...partitions.map(({ partitionIndex }) => partitionIndex))
        }
        cursor = page.nextCursor
        if (cursor) {
          pending = pending.filter((name) => name >= cursor.topicName)
        }
      } while (cursor && pages < MAX_PAGES)

      writeEvidence(EVIDENCE, { step: 'pages', pages, endedOnNull: cursor === null })
      expect(cursor).toBeNull()
      expect(pages).toBe(PARTITIONS_A + PARTITIONS_B)
      expect(read.get(topicA)).toEqual(Array.from({ length: PARTITIONS_A }, (_, index) => index))
      expect(read.get(topicB)).toEqual(Array.from({ length: PARTITIONS_B }, (_, index) => index))
    },
    CASE_BUDGET_MS
  )

  test(
    'fetchTopicMetadata through DescribeTopicPartitions agrees with the Metadata path',
    async () => {
      let viaMetadata
      let viaDescribe
      const agreed = await waitForCondition(async () => {
        viaMetadata = (await admin.fetchTopicMetadata({ topics })).topics
        asked.length = 0
        viaDescribe = (await admin.fetchTopicMetadata({ topics, useDescribeTopicPartitions: true }))
          .topics
        return JSON.stringify(comparable(viaMetadata)) === JSON.stringify(comparable(viaDescribe))
      }, AGREEMENT_BUDGET)

      writeEvidence(EVIDENCE, {
        step: 'agreement',
        agreed,
        pages: asked.length,
        topicIds: viaDescribe.map(({ name, topicId }) => ({ name, topicId })),
        eligibleLeaderReplicas: viaDescribe.flatMap(({ partitions }) =>
          partitions.map(({ eligibleLeaderReplicas }) => eligibleLeaderReplicas)
        ),
      })
      expect(agreed).toBe(true)

      // the last read took one request per partition, and the admin asked for 2000 a page each time
      expect(asked.map(({ responsePartitionLimit }) => responsePartitionLimit)).toEqual(
        Array(PARTITIONS_A + PARTITIONS_B).fill(2000)
      )
      expect(viaDescribe.map(({ name }) => name)).toEqual([...topics].sort())
      for (const { topicId, isInternal, partitions } of viaDescribe) {
        expect(topicId).toMatch(UUID)
        expect(isInternal).toBe(false)
        for (const partition of partitions) {
          expect(Number.isInteger(partition.leaderEpoch)).toBe(true)
          for (const list of [partition.eligibleLeaderReplicas, partition.lastKnownElr]) {
            expect(list === null || (Array.isArray(list) && list.every(Number.isInteger))).toBe(
              true
            )
          }
        }
      }
      expect(viaDescribe.map(({ partitions }) => partitions.length)).toEqual(
        [...topics].sort().map((name) => (name === topicA ? PARTITIONS_A : PARTITIONS_B))
      )
    },
    CASE_BUDGET_MS
  )

  test(
    'a topic that does not exist fails the call and is not created by it',
    async () => {
      await expect(
        admin.fetchTopicMetadata({ topics: [topicA, missing], useDescribeTopicPartitions: true })
      ).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })

      await sleep(QUIET_MS)
      const listed = await admin.listTopics()
      writeEvidence(EVIDENCE, { step: 'no-auto-create', created: listed.includes(missing) })
      expect(listed).not.toContain(missing)
    },
    CASE_BUDGET_MS
  )
})
