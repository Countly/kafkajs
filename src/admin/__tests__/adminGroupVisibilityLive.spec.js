const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')
const { MemberAssignment } = require('../../consumer/assignerProtocol')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: this scenario stands up two live groups at once and would race the consumer specs. Run it
// alone: `npm run test:e2e:admin-group-visibility`.
const describeAdminGroupVisibility =
  process.env.ADMIN_GROUP_VISIBILITY_E2E === '1' ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const ROW_BUDGET_MS = 120000
const PARTITIONS = 3
const MAX_WAIT_TIME_MS = 1000
const ASSIGNMENT_BUDGET = { attempts: 60, delay: 500 } // 30 s
const STABLE_BUDGET = { attempts: 60, delay: 500 } // 30 s - both groups reaching Stable
const DISCONNECT_BUDGET_MS = 15000

/** @returns {number[]} every partition of a scenario topic, ascending */
const allPartitions = () => Array.from({ length: PARTITIONS }, (_, index) => index)

/** @param {number} a @param {number} b @returns {number} */
const ascending = (a, b) => a - b

/**
 * Live proof that one admin client sees a classic group and a KIP-848 consumer group side by side:
 * `listGroups` reports `groupType`/`groupState` for both and honours the `types` and `states`
 * filters, and `describeGroups` returns real members for each - the consumer group through
 * ConsumerGroupDescribe, the classic one through the DescribeGroups fallback.
 *
 * Ownership is read from `GROUP_JOIN`'s `memberAssignment`, which carries the member's whole
 * assignment. `PARTITIONS_ASSIGNED` cannot be used for it: the consumer protocol is cooperative, so
 * that event carries only the round's delta.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 */
describeAdminGroupVisibility('Admin > group visibility (live)', () => {
  let kafka
  let admin
  let classicTopic
  let consumerTopic
  let classicGroupId
  let consumerGroupId
  let classicMember
  let consumerMember
  /** @type {object[]} */
  let consumers
  /** @type {string[]} */
  let adminRequests

  /**
   * Starts one running member and records what it joins with.
   * @param {string} topic
   * @param {string} groupId
   * @param {object} [config] extra consumer configuration, e.g. `{ groupProtocol: 'consumer' }`
   * @returns {Promise<{consumer: object, observed: {topic: string, groupJoins: object[]}}>}
   */
  const startMember = async (topic, groupId, config = {}) => {
    const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: MAX_WAIT_TIME_MS, ...config })
    const observed = { topic, groupJoins: [] }

    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => observed.groupJoins.push(payload))
    consumers.push(consumer)

    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({ eachMessage: async () => {} })

    return { consumer, observed }
  }

  /**
   * The partitions this member currently owns, from the whole assignment its last `GROUP_JOIN`
   * reported.
   * @param {{topic: string, groupJoins: object[]}} observed
   * @returns {number[]} sorted ascending, empty before the member's first round
   */
  const ownedBy = (observed) => {
    const last = observed.groupJoins[observed.groupJoins.length - 1]
    if (!last) return []
    return [...(last.memberAssignment[observed.topic] || [])].sort(ascending)
  }

  /**
   * @param {object[]} groups
   * @param {string} groupId
   * @returns {object|undefined}
   */
  const rowFor = (groups, groupId) => groups.find((group) => group.groupId === groupId)

  beforeAll(async () => {
    classicTopic = `e2e-admin-classic-${secureRandom()}`
    consumerTopic = `e2e-admin-consumer-${secureRandom()}`
    classicGroupId = `e2e-admin-classic-${secureRandom()}`
    consumerGroupId = `e2e-admin-consumer-${secureRandom()}`
    consumers = []
    adminRequests = []

    kafka = new Kafka({
      clientId: 'admin-group-visibility-live',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    admin = kafka.admin()
    admin.on(admin.events.REQUEST, ({ payload }) => adminRequests.push(payload.apiName))

    await admin.connect()
    await createTopic({ topic: classicTopic, partitions: PARTITIONS })
    await createTopic({ topic: consumerTopic, partitions: PARTITIONS })

    classicMember = await startMember(classicTopic, classicGroupId)
    consumerMember = await startMember(consumerTopic, consumerGroupId, {
      groupProtocol: 'consumer',
    })

    // Neither group may be Empty when the assertions run, or every row below is about nothing. The
    // sole member of each group must hold the whole topic, which is also what the describe case
    // asserts the assignment to be.
    const assigned = await waitForCondition(
      () =>
        ownedBy(classicMember.observed).length === PARTITIONS &&
        ownedBy(consumerMember.observed).length === PARTITIONS,
      ASSIGNMENT_BUDGET
    )
    expect(assigned).toBe(true)

    const stable = await waitForCondition(async () => {
      const { groups } = await admin.listGroups()
      const classic = rowFor(groups, classicGroupId)
      const consumer = rowFor(groups, consumerGroupId)
      return Boolean(
        classic && consumer && classic.groupState === 'Stable' && consumer.groupState === 'Stable'
      )
    }, STABLE_BUDGET)
    expect(stable).toBe(true)
  }, ROW_BUDGET_MS)

  afterAll(async () => {
    for (const consumer of consumers || []) {
      await withTimeout(
        consumer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }

    if (admin) {
      await withTimeout(
        admin.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }
  })

  it(
    'reports groupType and groupState on every row, for both protocols',
    async () => {
      const { groups } = await admin.listGroups()

      // Lower-case on this API for the type, wire-cased for the state.
      expect(rowFor(groups, classicGroupId)).toMatchObject({
        groupType: 'classic',
        protocolType: 'consumer',
        groupState: 'Stable',
      })
      expect(rowFor(groups, consumerGroupId)).toMatchObject({
        groupType: 'consumer',
        groupState: 'Stable',
      })
    },
    ROW_BUDGET_MS
  )

  it(
    'filters by type, excluding the group that does not match',
    async () => {
      const { groups } = await admin.listGroups({ types: ['consumer'] })

      // Absence is the assertion: a filter that is silently dropped would still contain the
      // consumer group, so asserting its presence alone proves nothing.
      expect(rowFor(groups, consumerGroupId)).toBeDefined()
      expect(rowFor(groups, classicGroupId)).toBeUndefined()

      const classicOnly = await admin.listGroups({ types: ['classic'] })
      expect(rowFor(classicOnly.groups, classicGroupId)).toBeDefined()
      expect(rowFor(classicOnly.groups, consumerGroupId)).toBeUndefined()
    },
    ROW_BUDGET_MS
  )

  it(
    'filters by state',
    async () => {
      const stable = await admin.listGroups({ states: ['stable'] })
      expect(rowFor(stable.groups, classicGroupId)).toBeDefined()
      expect(rowFor(stable.groups, consumerGroupId)).toBeDefined()

      const empty = await admin.listGroups({ states: ['empty'] })
      expect(rowFor(empty.groups, classicGroupId)).toBeUndefined()
      expect(rowFor(empty.groups, consumerGroupId)).toBeUndefined()
    },
    ROW_BUDGET_MS
  )

  it(
    'describes the consumer group with real members, a real assignment and a decodable buffer',
    async () => {
      const { groups } = await admin.describeGroups([consumerGroupId])
      const [group] = groups

      expect(group).toMatchObject({
        groupId: consumerGroupId,
        errorCode: 0,
        groupType: 'consumer',
        protocolType: 'consumer',
      })
      expect(group.assignorName).toEqual(expect.any(String))
      expect(group.assignorName.length).toBeGreaterThan(0)
      // Asserted as membership of the three live states, never as "not Dead": 'Assigning' and
      // 'Reconciling' are new strings this release has to be able to report.
      expect(['Stable', 'Assigning', 'Reconciling']).toContain(group.state)
      expect(group.groupEpoch).toEqual(expect.any(Number))

      expect(group.members).toHaveLength(1)
      const [member] = group.members
      expect(member.memberEpoch).toEqual(expect.any(Number))

      const assigned = member.assignment.find(({ topic }) => topic === consumerTopic)
      expect(assigned).toBeDefined()
      expect([...assigned.partitions].sort(ascending)).toEqual(allPartitions())

      // The synthesised buffer decodes to the same assignment, which is what keeps tooling working.
      const { assignment } = MemberAssignment.decode(member.memberAssignment)
      expect([...assignment[consumerTopic]].sort(ascending)).toEqual(allPartitions())
      expect(member.memberMetadata).toEqual(Buffer.alloc(0))
    },
    ROW_BUDGET_MS
  )

  it(
    'describes the classic group through the fallback, and both groups in one call',
    async () => {
      const before = adminRequests.length
      const { groups } = await admin.describeGroups([classicGroupId])
      const [group] = groups

      expect(group).toMatchObject({
        groupId: classicGroupId,
        errorCode: 0,
        groupType: 'classic',
        protocolType: 'consumer',
        groupEpoch: null,
        assignorName: null,
      })
      expect(group.members).toHaveLength(1)
      expect(group.members[0].memberEpoch).toBeNull()
      expect(group.members[0].targetAssignment).toBeNull()
      expect(group.members[0].memberMetadata.length).toBeGreaterThan(0)

      // The evidence that the fallback ran: the newer API was tried first, then the older one.
      const sequence = adminRequests
        .slice(before)
        .filter((apiName) => apiName === 'ConsumerGroupDescribe' || apiName === 'DescribeGroups')
      expect(sequence.indexOf('ConsumerGroupDescribe')).toBeGreaterThanOrEqual(0)
      expect(sequence.indexOf('DescribeGroups')).toBeGreaterThan(
        sequence.indexOf('ConsumerGroupDescribe')
      )

      const both = await admin.describeGroups([classicGroupId, consumerGroupId])
      expect(both.groups).toHaveLength(2)
      expect(both.groups.map(({ errorCode }) => errorCode)).toEqual([0, 0])
      expect(both.groups.map(({ groupId }) => groupId).sort()).toEqual(
        [classicGroupId, consumerGroupId].sort()
      )
      expect(both.groups.map(({ groupId, groupType }) => [groupId, groupType]).sort()).toEqual(
        [
          [classicGroupId, 'classic'],
          [consumerGroupId, 'consumer'],
        ].sort()
      )
    },
    ROW_BUDGET_MS
  )
})
