const Broker = require('../../broker')
const { Kafka, logLevel } = require('../../../index')
const CONFIG_RESOURCE_TYPES = require('../../protocol/configResourceTypes')
const {
  createConnectionPool,
  newLogger,
  plainTextBrokers,
  secureRandom,
  startBroker,
  stopBroker,
  writeEvidence,
} = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it stops a broker. Run alone: `npm run test:e2e:leaderless`.
const describeLeaderless = process.env.LEADERLESS_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'leaderless'
// Named so each wait says how long it is prepared to be wrong for. The scenario budget runs from the
// first case to the last; a case has its own, shorter one.
const SCENARIO_BUDGET_MS = 120000
const CASE_BUDGET_MS = 30000
// A stopped broker leaves the cluster's metadata within seconds, and a restarted one rejoins as
// fast; each hook also connects and disconnects its clients.
const HOOK_BUDGET_MS = 60000
const BROKER_BUDGET = { attempts: 80, delay: 250 } // 20 s
const DISCONNECT_BUDGET_MS = 15000
// A partition with no leader fails every named Metadata request until the budget runs out: kept
// short, so the case that meets it ends in about a second. 100 ms doubling to a 400 ms cap.
const RETRY = { retries: 3, initialRetryTime: 100, multiplier: 2, maxRetryTime: 400, factor: 0 }
const LEADER_NOT_AVAILABLE = 5
// The broker stopped, and its node id; kafka1 stays up as the bootstrap.
const STOPPED_SERVICE = 'kafka3'
const STOPPED_NODE = 3

/**
 * The name and Kafka type of an error and of every `cause` under it, outermost first.
 * @param {Error} error
 * @returns {{ name: string, type?: string }[]}
 */
const chainOf = (error) => {
  const chain = []
  for (let link = error; link != null && chain.length < 10; link = link.cause) {
    chain.push({ name: link.name, type: link.type })
  }
  return chain
}

/**
 * Live proof that a partition with no leader no longer fails the calls that read the metadata of
 * every topic. One topic's only replica is on a broker that is stopped, so its partition has no
 * leader for the whole file; `describeCluster`, `listTopics`, `fetchTopicMetadata()`,
 * `createTopics`, `describeConfigs` and a regex `subscribe()` all succeed meanwhile, and
 * `fetchTopicMetadata()` reports the partition as the broker does. `fetchTopicOffsets` of that
 * topic still fails on `LEADER_NOT_AVAILABLE`, as it always has, and never looks for a node -1.
 * @see https://github.com/apache/kafka/blob/4.3/metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java
 * @see https://github.com/apache/kafka/blob/4.3/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
 */
describeLeaderless('Admin > a partition with no leader (live)', () => {
  const prefix = `e2e-p8-leaderless-${secureRandom(4)}`
  const healthy = `${prefix}-healthy`
  const leaderless = `${prefix}-alone`
  const created = `${prefix}-created`
  let kafka
  let admin
  let bootstrap
  let brokerGone
  let startedAt
  const clients = []

  /** A connected client of `kafka`, disconnected after the file. */
  const connected = async (client) => {
    clients.push(client)
    await client.connect()
    return client
  }

  /** How many brokers kafka1 names in its answer for the healthy topic; 0 while it cannot answer. */
  const brokersServing = async () => {
    try {
      return (await bootstrap.metadata([healthy])).brokers.length
    } catch (e) {
      return 0
    }
  }

  beforeAll(async () => {
    kafka = new Kafka({
      clientId: `${prefix}-client`,
      brokers: plainTextBrokers(),
      retry: RETRY,
      logLevel: logLevel.NOTHING,
    })
    admin = await connected(kafka.admin())
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        { topic: healthy, replicaAssignment: [{ partition: 0, replicas: [1, 2] }] },
        { topic: leaderless, replicaAssignment: [{ partition: 0, replicas: [STOPPED_NODE] }] },
      ],
    })
    bootstrap = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
    await bootstrap.connect()
    stopBroker(STOPPED_SERVICE)
    // The controller fences the broker and takes its partitions' leadership away in one batch, so
    // an answer that no longer names the broker reports the partition with no leader.
    brokerGone = await waitForCondition(async () => (await brokersServing()) === 2, BROKER_BUDGET)
    startedAt = Date.now()
  }, HOOK_BUDGET_MS)

  afterEach(() => {
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  })

  // The broker comes back first, and is waited for: no later file may start on two brokers. The
  // clients are disconnected whatever happens before, and the restart is asserted last.
  afterAll(async () => {
    let restored = false
    try {
      try {
        startBroker(STOPPED_SERVICE)
      } catch (e) {
        // Already running, or compose is down.
      }
      restored = await waitForCondition(
        async () =>
          (await admin.describeCluster().catch(() => ({ brokers: [] }))).brokers.length === 3,
        BROKER_BUDGET
      )
      writeEvidence(EVIDENCE, { step: 'teardown', threeBrokers: restored })
      await admin.deleteTopics({ topics: [healthy, leaderless, created] }).catch(() => {})
    } finally {
      await withTimeout(
        bootstrap.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
      for (const client of clients.reverse()) {
        await withTimeout(
          client.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        )
      }
    }
    expect(restored).toBe(true)
  }, HOOK_BUDGET_MS)

  test(
    'describeCluster, listTopics and fetchTopicMetadata() succeed, and report the partition with no leader',
    async () => {
      const described = await admin.describeCluster()
      const listed = await admin.listTopics()
      const { topics } = await admin.fetchTopicMetadata()
      const [partition] = topics.find(({ name }) => name === leaderless).partitions

      writeEvidence(EVIDENCE, {
        step: 'all-topics',
        brokerGone,
        brokers: described.brokers.map(({ nodeId }) => nodeId).sort(),
        listed: listed.includes(leaderless),
        partition: { errorCode: partition.partitionErrorCode, leader: partition.leader },
      })
      expect(brokerGone).toBe(true)
      expect(described.brokers.map(({ nodeId }) => nodeId)).not.toContain(STOPPED_NODE)
      expect(listed).toEqual(expect.arrayContaining([healthy, leaderless]))
      expect(partition).toMatchObject({ partitionErrorCode: LEADER_NOT_AVAILABLE, leader: -1 })
    },
    CASE_BUDGET_MS
  )

  test(
    'createTopics and describeConfigs, which first refresh the metadata of every topic, succeed',
    async () => {
      const createdTopic = await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: created, replicaAssignment: [{ partition: 0, replicas: [1, 2] }] }],
      })
      const { resources } = await admin.describeConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: healthy }],
      })

      writeEvidence(EVIDENCE, {
        step: 'refreshing-calls',
        created: createdTopic,
        describedErrorCode: resources[0].errorCode,
      })
      expect(createdTopic).toBe(true)
      expect(resources).toEqual([expect.objectContaining({ resourceName: healthy, errorCode: 0 })])
    },
    CASE_BUDGET_MS
  )

  test(
    'a regex subscription to the healthy topic resolves',
    async () => {
      const consumer = await connected(kafka.consumer({ groupId: `${prefix}-group` }))

      const outcome = await consumer
        .subscribe({ topics: [new RegExp(`^${healthy}$`)] })
        .then(() => null, chainOf)

      writeEvidence(EVIDENCE, { step: 'regex-subscribe', outcome })
      expect(outcome).toBeNull()
    },
    CASE_BUDGET_MS
  )

  test(
    'fetchTopicMetadata through DescribeTopicPartitions, as the first call of a new admin, reports the partition with no leader',
    async () => {
      const fresh = await connected(kafka.admin())

      const { topics } = await fresh.fetchTopicMetadata({
        topics: [leaderless],
        useDescribeTopicPartitions: true,
      })
      const [partition] = topics[0].partitions

      writeEvidence(EVIDENCE, {
        step: 'fresh-admin-describe-topic-partitions',
        partition: { errorCode: partition.partitionErrorCode, leader: partition.leader },
      })
      // DescribeTopicPartitions sets no error code on a partition with no leader, only leaderId -1:
      // https://github.com/apache/kafka/blob/4.3/metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java
      expect(partition).toMatchObject({ partitionErrorCode: 0, leader: -1 })
    },
    CASE_BUDGET_MS
  )

  test(
    'fetchTopicOffsets of the topic with no leader still fails on LEADER_NOT_AVAILABLE, never looking for a node -1',
    async () => {
      const error = await admin.fetchTopicOffsets(leaderless).then(
        () => null,
        (e) => e
      )
      const chain = error && chainOf(error)

      writeEvidence(EVIDENCE, { step: 'fetch-topic-offsets', chain })
      expect(chain).toEqual(
        expect.arrayContaining([expect.objectContaining({ type: 'LEADER_NOT_AVAILABLE' })])
      )
      expect(chain.map(({ name }) => name)).not.toContain('KafkaJSBrokerNotFound')
    },
    CASE_BUDGET_MS
  )
})
