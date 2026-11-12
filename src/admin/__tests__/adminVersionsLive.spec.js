const Broker = require('../../broker')
const { newestVersionsFromCapture } = require('../../../testHelpers/brokerCapture')
const { readBrokerVersion } = require('../../../testHelpers/brokerVersion')
const {
  Kafka,
  logLevel,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('../../../index')
const { plainTextBrokers, writeEvidence, evidenceLines, wireVersionsOf } = require('testHelpers')
const { problemsOf } = require('../../../scripts/evidence/verify')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

describe('Admin > captured newest versions', () => {
  test('4.3.2 capture limits DeleteGroups to v2', () => {
    expect(newestVersionsFromCapture('4.3.2', ['DeleteGroups'])).toEqual({ DeleteGroups: 2 })
  })

  test('4.4.0 capture permits DeleteGroups v3', () => {
    expect(newestVersionsFromCapture('4.4.0', ['DeleteGroups'])).toEqual({ DeleteGroups: 3 })
  })

  test('a capture cannot supply a broker-only API', () => {
    expect(() => newestVersionsFromCapture('4.4.0', ['LeaderAndIsr'])).toThrow(
      'No common captured version for LeaderAndIsr on 4.4.0'
    )
  })

  test('an uncaptured broker version cannot fall back to another recording', () => {
    expect(() => newestVersionsFromCapture('0.0.0', ['DeleteGroups'])).toThrow(
      'Incomplete broker recording for 0.0.0'
    )
  })
})

// Gated: it creates and deletes topics, groups and ACLs on a live cluster. Run alone:
// `npm run test:e2e:admin-versions`.
const describeAdminVersions = process.env.ADMIN_VERSIONS_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'admin-versions'
// The lines earlier runs left behind, so the last case judges only this run's.
const linesBefore = evidenceLines(EVIDENCE).length
// Named so each wait says how long it is prepared to be wrong for. The scenario budget is the
// whole file, from the first case to the last; a case has its own, shorter one.
const SCENARIO_BUDGET_MS = 90000
const CASE_BUDGET_MS = 45000
const HOOK_BUDGET_MS = 30000
const DISCONNECT_BUDGET_MS = 15000
// A topic, a partition count, a low watermark and an ACL are applied by the controller before the
// call answers and reach the broker that answers the read-back a moment later.
const PROPAGATION_BUDGET = { attempts: 40, delay: 250 } // 10 s
const JOIN_BUDGET = { attempts: 120, delay: 250 } // 30 s
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const RECORDS = 5

/**
 * Live proof that the topic, partition, record, group and ACL administration APIs run at the
 * newest version every Kafka 4.x broker offers and that their effects are real: the REQUEST events
 * name the versions, the topic id `createTopics` answers is the one Metadata reports, a topic
 * disappears, a partition count grows, the low watermark moves, a group is deleted with a numeric
 * throttle time, and an ACL on a `USER` resource is created, described and deleted by the
 * `User:ANONYMOUS` super user of the test cluster (`docker-compose.yml`).
 *
 * Counts and versions are asserted, never timings; the broker-level answers `admin` does not return
 * (`topicId`, `clientSideThrottleTime`) are read by wrapping `Broker#createTopics`, `#deleteGroups`
 * and `#metadata`.
 *
 * The cases share one topic and each reads what the previous one left, so they run in order: run
 * the file whole, never a single case with `-t`.
 *
 * @see https://github.com/apache/kafka/blob/4.3.1/metadata/src/main/java/org/apache/kafka/controller/AclControlManager.java
 */
describeAdminVersions(
  'Admin > topic, record, group and ACL APIs at their newest versions (live)',
  () => {
    const runId = Date.now().toString(36)
    const topic = `e2e-p8-admin-versions-${runId}-topic`
    const groupId = `e2e-p8-admin-versions-${runId}-group`
    const userResource = `User:e2e-p8-admin-versions-${runId}-target`
    const acl = {
      resourceType: AclResourceTypes.USER,
      resourceName: userResource,
      resourcePatternType: ResourcePatternTypes.LITERAL,
      principal: `User:e2e-p8-admin-versions-${runId}-grantee`,
      host: '*',
      operation: AclOperationTypes.READ,
      permissionType: AclPermissionTypes.ALLOW,
    }
    /** @type {{ apiName: string, apiVersion: number }[]} */
    const requests = []
    /** @type {object[]} what `Broker#createTopics` answered */
    const createdTopics = []
    /** @type {object[]} what `Broker#deleteGroups` answered */
    const deletedGroups = []
    /** @type {Map<string, string|null>} the latest topic id Metadata reported for each topic */
    const metadataIds = new Map()
    let kafka
    let admin
    let producer
    let spies
    let startedAt
    let expectedVersions

    /**
     * Calls `read` until `holds(result)` is true, within the propagation budget.
     * @param {() => Promise<*>} read
     * @param {(result: *) => boolean} holds
     * @returns {Promise<{ held: boolean, result: * }>}
     */
    const settled = async (read, holds) => {
      let result
      const held = await waitForCondition(async () => {
        result = await read()
        return holds(result)
      }, PROPAGATION_BUDGET)
      return { held, result }
    }

    /** Wraps `method` of `Broker.prototype` so that every answer is handed to `record`. */
    const recording = (method, record) => {
      const original = Broker.prototype[method]
      return jest.spyOn(Broker.prototype, method).mockImplementation(async function (...args) {
        const answer = await original.apply(this, args)
        record(answer)
        return answer
      })
    }

    beforeAll(async () => {
      expectedVersions = newestVersionsFromCapture(readBrokerVersion(), [
        'CreateTopics',
        'DeleteTopics',
        'CreatePartitions',
        'DeleteRecords',
        'DeleteGroups',
        'DescribeAcls',
        'CreateAcls',
        'DeleteAcls',
      ])
      spies = [
        recording('createTopics', (answer) => createdTopics.push(answer)),
        recording('deleteGroups', (answer) => deletedGroups.push(answer)),
        recording('metadata', ({ topicMetadata }) =>
          topicMetadata.forEach(({ topic: name, topicId }) => metadataIds.set(name, topicId))
        ),
      ]
      kafka = new Kafka({
        clientId: 'admin-versions-live',
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
      })
      admin = kafka.admin()
      admin.on(admin.events.REQUEST, ({ payload }) =>
        requests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
      )
      producer = kafka.producer()
      await admin.connect()
      await producer.connect()
      startedAt = Date.now()
    }, HOOK_BUDGET_MS)

    afterEach(() => {
      expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
    })

    afterAll(async () => {
      spies?.forEach((spy) => spy.mockRestore())
      if (!admin) return
      await admin.deleteAcls({ filters: [acl] }).catch(() => {})
      await admin.deleteTopics({ topics: [topic] }).catch(() => {})
      await withTimeout(producer.disconnect(), DISCONNECT_BUDGET_MS)
      await withTimeout(admin.disconnect(), DISCONNECT_BUDGET_MS)
    }, HOOK_BUDGET_MS)

    test(
      'createTopics answers the topic id that Metadata reports for the topic',
      async () => {
        const created = await admin.createTopics({
          topics: [{ topic, numPartitions: 3, replicationFactor: 2 }],
          waitForLeaders: true,
        })
        expect(created).toBe(true)

        const [{ topicErrors }] = createdTopics.slice(-1)
        const [answer] = topicErrors
        expect(answer).toMatchObject({
          topic,
          errorCode: 0,
          numPartitions: 3,
          replicationFactor: 2,
        })
        expect(answer.topicId).toMatch(UUID)

        await admin.fetchTopicMetadata({ topics: [topic] })
        writeEvidence(EVIDENCE, { step: 'topic-id', createTopics: answer.topicId })
        expect(metadataIds.get(topic)).toBe(answer.topicId)
      },
      CASE_BUDGET_MS
    )

    test(
      'createPartitions grows the topic from 3 to 5 partitions',
      async () => {
        await admin.createPartitions({ topicPartitions: [{ topic, count: 5 }] })

        const { held, result } = await settled(
          async () => (await admin.fetchTopicMetadata({ topics: [topic] })).topics[0].partitions,
          (partitions) => partitions.length === 5
        )
        writeEvidence(EVIDENCE, { step: 'partitions', held, partitions: result.length })
        expect(held).toBe(true)
      },
      CASE_BUDGET_MS
    )

    test(
      'deleteTopicRecords moves the low watermark of the partition',
      async () => {
        await producer.send({
          topic,
          messages: Array.from({ length: RECORDS }, (_, index) => ({
            partition: 0,
            value: `record-${index}`,
          })),
        })
        await admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: '3' }] })

        const { held, result } = await settled(
          async () =>
            (await admin.fetchTopicOffsets(topic)).find(({ partition }) => partition === 0),
          (offsets) => offsets.low === '3'
        )
        writeEvidence(EVIDENCE, { step: 'records', held, offsets: result })
        expect(held).toBe(true)
        expect(result.high).toBe(String(RECORDS))
      },
      CASE_BUDGET_MS
    )

    test(
      'deleteGroups deletes an empty group and reports a numeric clientSideThrottleTime',
      async () => {
        const consumer = kafka.consumer({ groupId })
        let joined = false
        consumer.on(consumer.events.GROUP_JOIN, () => {
          joined = true
        })
        await consumer.connect()
        await consumer.subscribe({ topic, fromBeginning: true })
        await consumer.run({ eachMessage: async () => {} })
        expect(await waitForCondition(() => joined, JOIN_BUDGET)).toBe(true)
        await withTimeout(consumer.disconnect(), DISCONNECT_BUDGET_MS)

        const results = await admin.deleteGroups([groupId])

        expect(results).toEqual([expect.objectContaining({ groupId, errorCode: 0 })])
        const [{ clientSideThrottleTime }] = deletedGroups.slice(-1)
        writeEvidence(EVIDENCE, { step: 'delete-groups', clientSideThrottleTime })
        expect(typeof clientSideThrottleTime).toBe('number')
      },
      CASE_BUDGET_MS
    )

    test(
      'ACLs on a USER resource are created, described and deleted',
      async () => {
        expect(await admin.createAcls({ acl: [acl] })).toBe(true)

        const described = await settled(
          async () => admin.describeAcls(acl),
          ({ resources }) => resources.length === 1
        )
        expect(described.held).toBe(true)
        expect(described.result.resources[0]).toMatchObject({
          resourceType: AclResourceTypes.USER,
          resourceName: userResource,
          acls: [expect.objectContaining({ principal: acl.principal, operation: acl.operation })],
        })

        const { filterResponses } = await admin.deleteAcls({ filters: [acl] })
        expect(filterResponses[0].matchingAcls).toHaveLength(1)

        const gone = await settled(
          async () => admin.describeAcls(acl),
          ({ resources }) => resources.length === 0
        )
        writeEvidence(EVIDENCE, { step: 'acl-user', described: described.held, gone: gone.held })
        expect(gone.held).toBe(true)
      },
      CASE_BUDGET_MS
    )

    test(
      'deleteTopics removes the topic',
      async () => {
        await admin.deleteTopics({ topics: [topic] })

        const { held } = await settled(
          async () => admin.listTopics(),
          (topics) => !topics.includes(topic)
        )
        writeEvidence(EVIDENCE, { step: 'delete-topics', held })
        expect(held).toBe(true)
      },
      CASE_BUDGET_MS
    )

    test(
      'every call above negotiated the newest version',
      () => {
        const versionsOf = (apiName) => [
          ...new Set(requests.filter((r) => r.apiName === apiName).map((r) => r.apiVersion)),
        ]
        const negotiated = Object.fromEntries(
          [
            'CreateTopics',
            'DeleteTopics',
            'CreatePartitions',
            'DeleteRecords',
            'DeleteGroups',
            'DescribeAcls',
            'CreateAcls',
            'DeleteAcls',
          ].map((apiName) => [apiName, versionsOf(apiName)])
        )

        writeEvidence(EVIDENCE, {
          step: 'versions',
          negotiated,
          wireVersions: wireVersionsOf(requests),
        })
        expect(negotiated).toEqual(
          Object.fromEntries(
            Object.entries(expectedVersions).map(([name, version]) => [name, [version]])
          )
        )
      },
      CASE_BUDGET_MS
    )

    test(
      'every evidence line of this run carries the stamps a verifier checks',
      () => {
        const lines = evidenceLines(EVIDENCE).slice(linesBefore)
        const verified = {
          tree: process.env.KAFKAJS_TREE || 'unfrozen',
          node: process.version,
          brokerVersion: process.env.KAFKA_VERSION,
        }

        expect(lines.length).toBeGreaterThan(0)
        for (const line of lines) {
          expect(problemsOf(line, verified)).toEqual([])
        }
        expect(lines.find(({ step }) => step === 'versions').wireVersions).toMatchObject(
          expectedVersions
        )
      },
      CASE_BUDGET_MS
    )
  }
)
