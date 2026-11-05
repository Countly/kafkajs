const { Kafka, logLevel } = require('../../../index')
const { KafkaJSDeleteGroupsError } = require('../../errors')
const {
  secureRandom,
  plainTextBrokers,
  createTopic,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it creates and deletes a topic and consumer groups on a live cluster. Run it alone through
// `npm run test:e2e:delete-groups-v3`; `KAFKA_VERSION` names the cluster under test.
const describeDeleteGroupsV3 = process.env.DELETE_GROUPS_V3_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'delete-groups-v3'
// Named so each wait says how long it is prepared to be wrong for. The scenario budget is the whole
// file, from the first hook to the last cleanup; a case and a hook have their own, shorter ones.
const SCENARIO_BUDGET_MS = 120000
const CASE_BUDGET_MS = 40000
const HOOK_BUDGET_MS = 30000
const DISCONNECT_BUDGET_MS = 10000
const PROPAGATION_BUDGET = { attempts: 40, delay: 250 } // 10 s
const JOIN_BUDGET = { attempts: 80, delay: 250 } // 20 s
const RECORDS = 5
const GROUP_ID_NOT_FOUND = 69
const NON_EMPTY_GROUP = 68

/**
 * The DeleteGroups version a broker of the lane's `KAFKA_VERSION` offers: v3, which adds the
 * per-group message, from Kafka 4.4, and v2 before it.
 * @returns {2|3}
 * @throws {Error} when `KAFKA_VERSION` is not `major.minor.patch`, so no lane is guessed
 */
const offeredVersion = () => {
  const [major, minor] = (process.env.KAFKA_VERSION || '').split('.').map(Number)
  if (!Number.isInteger(major) || !Number.isInteger(minor)) {
    throw new Error(
      `KAFKA_VERSION must name the cluster under test, got ${process.env.KAFKA_VERSION}`
    )
  }

  return major > 4 || (major === 4 && minor >= 4) ? 3 : 2
}

/**
 * Live proof of DeleteGroups at the version the broker offers. A group with a committed offset that
 * no longer has a member is deleted: one result with error code 0, the broker's own `errorMessage`
 * (`null`) at v3 and none at v2, the group gone from DescribeGroups and ListGroups and its commit
 * gone from OffsetFetch. A group that does not exist and one that still has a member are refused
 * each under its own id with its own typed error, the second one untouched, with the broker's
 * message where v3 carries one. Versions are read from `REQUEST` events; counts and versions are
 * asserted, never durations. A stock broker has no reason to answer GROUP_DELETION_FAILED (134);
 * the independent Java vectors pin that message.
 *
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/DeleteGroupsResponse.json
 */
describeDeleteGroupsV3('Admin > deleteGroups message per group (live)', () => {
  const runId = secureRandom()
  const topic = `e2e-delete-groups-v3-${runId}-topic`
  const kafka = new Kafka({
    clientId: 'delete-groups-v3-live',
    brokers: plainTextBrokers(),
    logLevel: logLevel.NOTHING,
  })
  const admin = kafka.admin()
  const producer = kafka.producer()
  /** @type {{ apiName: string, apiVersion: number }[]} */
  const requests = []
  const consumers = []
  const groups = []
  /** what cleanup could not undo; an already deleted group is no failure */
  const cleanupFailures = []
  let startedAt

  const deleteRequests = () =>
    requests.filter(({ apiName }) => apiName === 'DeleteGroups').map(({ apiVersion }) => apiVersion)

  /**
   * A consumer that joins `groupId`, reads the topic from the start without committing on its own,
   * and has committed offset `RECORDS` (the next offset to read) before this resolves.
   * @param {string} groupId
   * @returns {Promise<object>} the consumer, still a member of the group
   */
  const memberWithCommit = async (groupId) => {
    groups.push(groupId)
    const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 500 })
    consumers.push(consumer)
    let joined = false
    const read = []
    consumer.on(consumer.events.GROUP_JOIN, () => {
      joined = true
    })
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ message }) => read.push(message.offset),
    })
    expect(await waitForCondition(() => joined && read.length >= RECORDS, JOIN_BUDGET)).toBe(true)
    await consumer.commitOffsets([{ topic, partition: 0, offset: String(RECORDS) }])

    return consumer
  }

  /** @returns {Promise<object>} the group's DescribeGroups row once `holds` accepts it */
  const describedWhen = async (groupId, holds) => {
    let row
    const held = await waitForCondition(async () => {
      ;[row] = (await admin.describeGroups([groupId])).groups
      return holds(row)
    }, PROPAGATION_BUDGET)
    expect({ held, row }).toEqual({ held: true, row: expect.objectContaining({ groupId }) })

    return row
  }

  const committedOffsets = async (groupId) =>
    (await admin.fetchOffsets({ groupId, topics: [topic] }))[0].partitions

  /** Runs one cleanup step; whatever it could not undo, other than a group already gone, is kept. */
  const cleanup = async (work) => {
    try {
      await work()
    } catch (error) {
      const alreadyGone =
        error instanceof KafkaJSDeleteGroupsError &&
        error.groups.every(({ errorCode }) => errorCode === GROUP_ID_NOT_FOUND)
      if (!alreadyGone) cleanupFailures.push(error)
    }
  }

  beforeAll(async () => {
    admin.on(admin.events.REQUEST, ({ payload }) =>
      requests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )
    startedAt = Date.now()
    await Promise.all([admin.connect(), producer.connect()])
    await createTopic({ topic, partitions: 1 })
    await producer.send({
      topic,
      messages: Array.from({ length: RECORDS }, (_, index) => ({
        partition: 0,
        value: `r-${index}`,
      })),
    })
  }, HOOK_BUDGET_MS)

  afterEach(() => {
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  })

  afterAll(async () => {
    for (const consumer of consumers) {
      await cleanup(() => withTimeout(consumer.disconnect(), DISCONNECT_BUDGET_MS))
    }
    for (const groupId of groups) await cleanup(() => admin.deleteGroups([groupId]))
    await cleanup(() => admin.deleteTopics({ topics: [topic] }))
    await cleanup(() => withTimeout(producer.disconnect(), DISCONNECT_BUDGET_MS))
    await cleanup(() => withTimeout(admin.disconnect(), DISCONNECT_BUDGET_MS))

    expect(cleanupFailures).toEqual([])
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  }, HOOK_BUDGET_MS)

  test(
    'deletes a group whose member has stopped and leaves neither the group nor its commit',
    async () => {
      const version = offeredVersion()
      const groupId = `e2e-delete-groups-v3-${runId}-stopped`
      const consumer = await memberWithCommit(groupId)
      expect(await committedOffsets(groupId)).toEqual([
        { partition: 0, offset: String(RECORDS), metadata: null },
      ])
      await withTimeout(consumer.disconnect(), DISCONNECT_BUDGET_MS)
      await describedWhen(groupId, ({ state }) => state === 'Empty')
      expect((await admin.listGroups()).groups.map((group) => group.groupId)).toContain(groupId)

      const results = await admin.deleteGroups([groupId])

      expect(results).toEqual([
        version === 3 ? { groupId, errorCode: 0, errorMessage: null } : { groupId, errorCode: 0 },
      ])
      if (version === 2) expect(results[0]).not.toHaveProperty('errorMessage')
      expect(new Set(deleteRequests())).toEqual(new Set([version]))
      await describedWhen(groupId, ({ state, errorCode }) => state === 'Dead' && errorCode === 69)
      expect(await committedOffsets(groupId)).toEqual([
        expect.objectContaining({ partition: 0, offset: '-1' }),
      ])
      expect((await admin.listGroups()).groups.map((group) => group.groupId)).not.toContain(groupId)
      writeEvidence(EVIDENCE, {
        step: 'delete-stopped-group',
        wireVersions: wireVersionsOf(requests.filter(({ apiName }) => apiName === 'DeleteGroups')),
        expectedVersion: version,
        result: results[0],
      })
    },
    CASE_BUDGET_MS
  )

  test(
    'refuses a missing group and a group with a member each under its own id, and leaves the member group as it was',
    async () => {
      const version = offeredVersion()
      const missing = `e2e-delete-groups-v3-${runId}-missing`
      const occupied = `e2e-delete-groups-v3-${runId}-occupied`
      await memberWithCommit(occupied)
      const before = deleteRequests().length

      const failure = await admin.deleteGroups([missing, occupied]).catch((error) => error)

      expect(failure).toBeInstanceOf(KafkaJSDeleteGroupsError)
      expect(failure.groups.map(({ groupId }) => groupId).sort()).toEqual(
        [missing, occupied].sort()
      )
      const byGroup = Object.fromEntries(failure.groups.map((group) => [group.groupId, group]))
      expect(byGroup[missing]).toMatchObject({
        errorCode: GROUP_ID_NOT_FOUND,
        error: expect.objectContaining({ type: 'GROUP_ID_NOT_FOUND', code: GROUP_ID_NOT_FOUND }),
      })
      expect(byGroup[occupied]).toMatchObject({
        errorCode: NON_EMPTY_GROUP,
        error: expect.objectContaining({ type: 'NON_EMPTY_GROUP', code: NON_EMPTY_GROUP }),
      })
      for (const group of failure.groups) {
        if (version === 3) {
          expect(group).toHaveProperty('errorMessage')
          expect(group.errorMessage === null || typeof group.errorMessage === 'string').toBe(true)
        } else {
          expect(group).not.toHaveProperty('errorMessage')
        }
      }
      expect(new Set(deleteRequests().slice(before))).toEqual(new Set([version]))

      const row = await describedWhen(occupied, ({ state }) => state === 'Stable')
      expect(row).toMatchObject({ groupId: occupied, errorCode: 0 })
      expect(row.members).toHaveLength(1)
      expect(await committedOffsets(occupied)).toEqual([
        { partition: 0, offset: String(RECORDS), metadata: null },
      ])
      writeEvidence(EVIDENCE, {
        step: 'refuse-missing-and-occupied',
        wireVersions: wireVersionsOf(requests.filter(({ apiName }) => apiName === 'DeleteGroups')),
        expectedVersion: version,
        groups: failure.groups.map(({ groupId, errorCode, errorMessage }) => ({
          groupId,
          errorCode,
          errorMessage: errorMessage === undefined ? '(absent)' : errorMessage,
        })),
      })
    },
    CASE_BUDGET_MS
  )
})
