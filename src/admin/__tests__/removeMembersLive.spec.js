const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic, writeEvidence } = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it removes members from live groups on a cold cluster. Run it alone through
// `npm run test:e2e:remove-members`.
const describeRemoveMembers = process.env.REMOVE_MEMBERS_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'remove-members'
// Named so each wait says how long it is prepared to be wrong for: the file has 120 s from the end of
// `beforeAll`, and each protocol's cases share what is left.
const SCENARIO_BUDGET_MS = 120000
const CASE_BUDGET_MS = 60000
const PARTITIONS = 2
const SETTLE_BUDGET = { attempts: 120, delay: 250 } // 30 s for both members to own a share
// "Well before the session timeout": a classic member here has 30 s, and the consumer-protocol
// coordinator is pinned to 6 s by `docker-compose.yml`.
const TAKEOVER_BUDGET = {
  classic: { attempts: 80, delay: 250 }, // 20 s, under 30 s
  consumer: { attempts: 16, delay: 250 }, // 4 s, under 6 s
}
const UNKNOWN_MEMBER_ID = 25

/**
 * Live proof of `admin.removeMembersFromConsumerGroup` under both group protocols: a static member
 * that stopped without leaving is removed by its instance id and the survivor owns every partition
 * long before the session would have expired; removing every member of a group with one dynamic
 * and one static member reports two removals; an unknown instance id, and a group that does not
 * exist, are `UNKNOWN_MEMBER_ID` in the results and never a rejection.
 *
 * Ownership is read from `GROUP_JOIN`'s `memberAssignment`, which carries a member's whole
 * assignment under both protocols (`PARTITIONS_ASSIGNED` carries only a round's delta).
 *
 * @see https://github.com/apache/kafka/blob/4.3.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java
 */
describeRemoveMembers.each(['classic', 'consumer'])(
  'Admin > removeMembersFromConsumerGroup (live, %s)',
  (protocol) => {
    const runId = secureRandom()
    const topic = `e2e-p8-remove-members-${runId}-${protocol}`
    const kafka = new Kafka({
      clientId: `remove-members-live-${protocol}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    const admin = kafka.admin()
    const consumers = []
    let startedAt

    /**
     * Starts a member of `groupId`, static when `groupInstanceId` is given.
     * @param {string} groupId
     * @param {string} [groupInstanceId]
     * @returns {Promise<{ consumer: object, groupJoins: object[] }>}
     */
    const start = async (groupId, groupInstanceId) => {
      const consumer = kafka.consumer({
        groupId,
        maxWaitTimeInMs: 500,
        ...(groupInstanceId ? { groupInstanceId } : {}),
        ...(protocol === 'consumer'
          ? { groupProtocol: 'consumer' }
          : { sessionTimeout: 30000, heartbeatInterval: 1000 }),
      })
      const member = { consumer, groupJoins: [] }
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => member.groupJoins.push(payload))
      consumers.push(consumer)

      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })
      return member
    }

    /** @returns {number} how many partitions the member's last join gave it */
    const owned = ({ groupJoins }) =>
      groupJoins.length === 0
        ? 0
        : (groupJoins[groupJoins.length - 1].memberAssignment[topic] || []).length

    const settled = (first, second) =>
      owned(first) > 0 && owned(second) > 0 && owned(first) + owned(second) === PARTITIONS

    /** Starts two members and waits until each owns a share of the topic. */
    const startPair = async (groupId, firstInstance, secondInstance) => {
      const first = await start(groupId, firstInstance)
      const second = await start(groupId, secondInstance)
      expect(await waitForCondition(() => settled(first, second), SETTLE_BUDGET)).toBe(true)
      return [first, second]
    }

    beforeAll(async () => {
      await admin.connect()
      await createTopic({ topic, partitions: PARTITIONS })
      startedAt = Date.now()
    }, 60000)

    afterEach(() => {
      expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
    })

    afterAll(async () => {
      for (const consumer of consumers) {
        await withTimeout(
          consumer.disconnect().catch(() => {}),
          10000
        )
      }
      await admin.deleteTopics({ topics: [topic] }).catch(() => {})
      await admin.disconnect().catch(() => {})
    }, 120000)

    test(
      'removes a static member that stopped without leaving, and the survivor takes its partitions well before the session timeout',
      async () => {
        const groupId = `${topic}-static`
        const [gone, survivor] = await startPair(groupId, 'instance-a', 'instance-b')
        await gone.consumer.disconnect()

        const results = await admin.removeMembersFromConsumerGroup({
          groupId,
          members: [{ groupInstanceId: 'instance-a' }],
        })

        expect(results).toEqual([
          { memberId: '', groupInstanceId: 'instance-a', errorCode: 0, error: null },
        ])
        let polls = 0
        const tookOver = await waitForCondition(() => {
          polls += 1
          return owned(survivor) === PARTITIONS
        }, TAKEOVER_BUDGET[protocol])
        writeEvidence(EVIDENCE, { case: 'static-removal', protocol, tookOver, polls })
        expect(tookOver).toBe(true)
      },
      CASE_BUDGET_MS
    )

    describe('on a group with a dynamic and a static member', () => {
      const groupId = `${topic}-all`

      test(
        'removing every member answers one removal for each',
        async () => {
          await startPair(groupId, undefined, 'instance-s')

          const results = await admin.removeMembersFromConsumerGroup({ groupId })

          writeEvidence(EVIDENCE, {
            case: 'remove-all',
            protocol,
            results: results.map(({ error, ...rest }) => rest),
          })
          expect(results).toHaveLength(2)
          expect(results.map(({ errorCode, error }) => [errorCode, error])).toEqual([
            [0, null],
            [0, null],
          ])
          expect(
            results.filter(({ groupInstanceId }) => groupInstanceId === 'instance-s')
          ).toHaveLength(1)
          expect(
            results.filter(
              ({ groupInstanceId, memberId }) => groupInstanceId === null && memberId !== ''
            )
          ).toHaveLength(1)
        },
        CASE_BUDGET_MS
      )

      test(
        'an instance id the group does not know is UNKNOWN_MEMBER_ID in the results, not a rejection',
        async () => {
          const results = await admin.removeMembersFromConsumerGroup({
            groupId,
            members: [{ groupInstanceId: 'nobody' }],
          })

          expect(results.map(({ errorCode, error }) => [errorCode, error.type])).toEqual([
            [UNKNOWN_MEMBER_ID, 'UNKNOWN_MEMBER_ID'],
          ])
        },
        CASE_BUDGET_MS
      )
    })

    test(
      'a group that does not exist answers every member UNKNOWN_MEMBER_ID, without a rejection',
      async () => {
        const results = await admin.removeMembersFromConsumerGroup({
          groupId: `${topic}-absent`,
          members: [{ groupInstanceId: 'x' }, { groupInstanceId: 'y' }],
        })

        expect(results.map(({ errorCode }) => errorCode)).toEqual([
          UNKNOWN_MEMBER_ID,
          UNKNOWN_MEMBER_ID,
        ])
      },
      CASE_BUDGET_MS
    )
  }
)
