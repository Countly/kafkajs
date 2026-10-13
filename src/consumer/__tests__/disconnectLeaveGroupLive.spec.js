const { Kafka, logLevel, PartitionAssigners } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic, writeEvidence } = require('testHelpers')
const { waitForCondition, withTimeout } = require('./fixtures/liveScenario')

// Gated: it disconnects members of live groups on a cold cluster and waits out session timeouts.
// Run it alone through `npm run test:e2e:leave-group`.
const describeLeaveGroup = process.env.LEAVE_GROUP_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'leave-group'
// Named so each wait says how long it is prepared to be wrong for: each protocol has 200 s from
// the end of `beforeAll`, and its cases share what is left.
const SCENARIO_BUDGET_MS = 200000
const CASE_BUDGET_MS = 90000
const PARTITIONS = 2
const SETTLE_BUDGET = { attempts: 120, delay: 250 } // 30 s for both members to own a share
const CALM_POLLS = 8 // 2 s without a rebalance on either member, before a case starts counting
const SESSION_TIMEOUT = 30000 // classic; the consumer-protocol coordinator is pinned to 6 s by docker-compose.yml
// Every window is counted from the start of the disconnect: the coordinator's clock starts at the
// leaving member's last heartbeat, before the disconnect began. A takeover must come "well before
// the session timeout", and a quiet window must end "before it".
const TAKEOVER_BUDGET = {
  classic: { attempts: 80, delay: 250 }, // 20 s, under 30 s
  consumer: { attempts: 16, delay: 250 }, // 4 s, under 6 s
}
const QUIET_BUDGET = {
  classic: { attempts: 32, delay: 250 }, // 8 s of a 30 s session
  consumer: { attempts: 12, delay: 250 }, // 3 s of a 6 s session
}
const EXPIRY_BUDGET = {
  classic: { attempts: 180, delay: 250 }, // 45 s: the session, then the rebalance
  consumer: { attempts: 48, delay: 250 }, // 12 s
}
const REJOIN_BUDGET = { attempts: 120, delay: 250 } // 30 s, the classic session timeout

/** @param {{ groupJoins: object[] }} member @returns {object} the payload of its last GROUP_JOIN */
const lastJoin = ({ groupJoins }) => groupJoins[groupJoins.length - 1]

/**
 * Live proof of `consumer.disconnect({ leaveGroup })` under both group protocols. A static member
 * that disconnects with `leaveGroup: true` hands its partitions to the other member well before
 * the session would expire; a dynamic member that disconnects with `leaveGroup: false` is still in
 * the group, and the other member sees no rebalance until the session expires; and the default
 * keeps today's timings (a dynamic member leaves at once, a static one stays).
 *
 * Ownership is read from `GROUP_JOIN`'s `memberAssignment`, which carries a member's whole
 * assignment under both protocols; a rebalance is a `REBALANCING` event, which the first join of a
 * run never emits.
 *
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerMembershipManager.java
 */
describeLeaveGroup.each(['classic', 'consumer'])(
  'Consumer > disconnect({ leaveGroup }) (live, %s)',
  (protocol) => {
    const runId = secureRandom()
    const topic = `e2e-p8-leave-group-${runId}-${protocol}`
    const kafka = new Kafka({
      clientId: `leave-group-live-${protocol}`,
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
     * @param {object} [extra] more consumer options
     * @returns {Promise<{ consumer: object, groupInstanceId: string|undefined, groupJoins: object[],
     *   rebalances: number, requests: { apiName: string, apiVersion: number }[] }>} `requests`
     *   holds every request the member sent
     */
    const start = async (groupId, groupInstanceId, extra = {}) => {
      const consumer = kafka.consumer({
        groupId,
        maxWaitTimeInMs: 500,
        ...(groupInstanceId ? { groupInstanceId } : {}),
        ...(protocol === 'consumer'
          ? { groupProtocol: 'consumer' }
          : { sessionTimeout: SESSION_TIMEOUT, heartbeatInterval: 1000 }),
        ...extra,
      })
      const member = { consumer, groupInstanceId, groupJoins: [], rebalances: 0, requests: [] }
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => member.groupJoins.push(payload))
      consumer.on(consumer.events.REBALANCING, () => (member.rebalances += 1))
      consumer.on(consumer.events.REQUEST, ({ payload }) =>
        member.requests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
      )
      consumers.push(consumer)

      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })
      return member
    }

    /** @returns {number} how many partitions the member's last join gave it */
    const owned = ({ groupJoins }) =>
      groupJoins.length === 0 ? 0 : (lastJoin({ groupJoins }).memberAssignment[topic] || []).length

    const settled = (first, second) =>
      owned(first) > 0 && owned(second) > 0 && owned(first) + owned(second) === PARTITIONS

    /**
     * Starts two members, waits until each owns a share, then until neither has rebalanced for
     * `CALM_POLLS` polls: a fresh topic's metadata lag can cost a legitimate resync early on, and a
     * rebalance caused by the set-up must not be counted as the behaviour under test.
     */
    const startPair = async (groupId, firstInstance, secondInstance, extra) => {
      const first = await start(groupId, firstInstance, extra)
      const second = await start(groupId, secondInstance, extra)
      expect(await waitForCondition(() => settled(first, second), SETTLE_BUDGET)).toBe(true)

      let last = -1
      let calm = 0
      const calmed = await waitForCondition(() => {
        const rebalances = first.rebalances + second.rebalances
        calm = rebalances === last ? calm + 1 : 0
        last = rebalances
        return calm >= CALM_POLLS
      }, SETTLE_BUDGET)
      expect(calmed).toBe(true)
      return [first, second]
    }

    /** @returns {number} the LeaveGroup requests the member sent (always 0 under the consumer protocol) */
    const leaveRequests = ({ requests }) =>
      requests.filter(({ apiName }) => apiName === 'LeaveGroup').length

    /**
     * Disconnects `member` and, from the same instant, polls `condition` for `budget`. Counting from
     * the start of the disconnect keeps every window anchored to the coordinator's own clock, which
     * starts at the member's last heartbeat, however long the disconnect takes.
     * @param {{ consumer: object }} member
     * @param {{ leaveGroup?: boolean }|undefined} options what `disconnect()` is given
     * @param {() => boolean} condition
     * @param {{ attempts: number, delay: number }} budget
     * @returns {Promise<boolean>} whether `condition` held within `budget`
     */
    const disconnectAndPoll = async (member, options, condition, budget) => {
      const [, held] = await Promise.all([
        member.consumer.disconnect(options),
        waitForCondition(condition, budget),
      ])
      return held
    }

    /** @returns {() => boolean} whether `survivor` owns every partition */
    const tookOver = (survivor) => () => owned(survivor) === PARTITIONS

    /** @returns {() => boolean} whether `survivor` rebalanced or took the partitions */
    const moved = (survivor, rebalancesBefore) => () =>
      survivor.rebalances > rebalancesBefore || owned(survivor) === PARTITIONS

    beforeAll(async () => {
      await admin.connect()
      await createTopic({ topic, partitions: PARTITIONS })
      startedAt = Date.now()
    }, 60000)

    afterEach(() => {
      const elapsedMs = Date.now() - startedAt
      writeEvidence(EVIDENCE, { case: 'scenario-elapsed', protocol, elapsedMs })
      expect(elapsedMs).toBeLessThan(SCENARIO_BUDGET_MS)
    })

    afterAll(async () => {
      for (const consumer of consumers) {
        await withTimeout(
          consumer.disconnect({ leaveGroup: true }).catch(() => {}),
          10000
        )
      }
      await admin.deleteTopics({ topics: [topic] }).catch(() => {})
      await admin.disconnect().catch(() => {})
    }, 120000)

    test(
      'a static member that disconnects with leaveGroup: true hands its partitions over well before the session timeout',
      async () => {
        const [gone, survivor] = await startPair(`${topic}-static-true`, 'instance-a', 'instance-b')

        let polls = 0
        const handedOver = await disconnectAndPoll(
          gone,
          { leaveGroup: true },
          () => {
            polls += 1
            return owned(survivor) === PARTITIONS
          },
          TAKEOVER_BUDGET[protocol]
        )
        writeEvidence(EVIDENCE, {
          case: 'static-true',
          protocol,
          tookOver: handedOver,
          polls,
          leaveRequests: leaveRequests(gone),
        })
        expect(handedOver).toBe(true)
        if (protocol === 'classic') {
          expect(leaveRequests(gone)).toBe(1)
        }
      },
      CASE_BUDGET_MS
    )

    test(
      'a dynamic member that disconnects with leaveGroup: false is still in the group: no rebalance until its session expires',
      async () => {
        const [gone, survivor] = await startPair(`${topic}-dynamic-false`)
        const rebalancesBefore = survivor.rebalances

        const movedEarly = await disconnectAndPoll(
          gone,
          { leaveGroup: false },
          moved(survivor, rebalancesBefore),
          QUIET_BUDGET[protocol]
        )
        const afterExpiry = await waitForCondition(tookOver(survivor), EXPIRY_BUDGET[protocol])
        writeEvidence(EVIDENCE, {
          case: 'dynamic-false',
          protocol,
          movedEarly,
          tookOver: afterExpiry,
          rebalances: survivor.rebalances - rebalancesBefore,
          leaveRequests: leaveRequests(gone),
        })
        expect(movedEarly).toBe(false)
        expect(leaveRequests(gone)).toBe(0)
        // The member really was still in the group: only its session expiring moves the survivor.
        expect(afterExpiry).toBe(true)
        expect(survivor.rebalances).toBeGreaterThan(rebalancesBefore)
      },
      CASE_BUDGET_MS
    )

    test(
      'a dynamic member that disconnects by default leaves at once',
      async () => {
        const [gone, survivor] = await startPair(`${topic}-dynamic-default`)

        const handedOver = await disconnectAndPoll(
          gone,
          undefined,
          tookOver(survivor),
          TAKEOVER_BUDGET[protocol]
        )
        writeEvidence(EVIDENCE, {
          case: 'dynamic-default',
          protocol,
          tookOver: handedOver,
          leaveRequests: leaveRequests(gone),
        })
        expect(handedOver).toBe(true)
      },
      CASE_BUDGET_MS
    )

    test(
      'a static member that disconnects by default stays in the group: no rebalance until its session expires',
      async () => {
        const [gone, survivor] = await startPair(
          `${topic}-static-default`,
          'instance-a',
          'instance-b'
        )
        const rebalancesBefore = survivor.rebalances

        const movedEarly = await disconnectAndPoll(
          gone,
          undefined,
          moved(survivor, rebalancesBefore),
          QUIET_BUDGET[protocol]
        )
        const afterExpiry = await waitForCondition(tookOver(survivor), EXPIRY_BUDGET[protocol])
        writeEvidence(EVIDENCE, {
          case: 'static-default',
          protocol,
          movedEarly,
          tookOver: afterExpiry,
          leaveRequests: leaveRequests(gone),
        })
        expect(movedEarly).toBe(false)
        expect(leaveRequests(gone)).toBe(0)
        // The member really was still in the group: only its session expiring moves the survivor.
        expect(afterExpiry).toBe(true)
      },
      CASE_BUDGET_MS
    )

    // A classic static leader that bounces. The new instance joins at v8 and is not told it leads,
    // so the assigner is not asked again, and it takes over the old instance's assignment with no
    // rebalance for the other member. A JoinGroup v9 client would be told to skip the assignment
    // instead (KIP-814); capping at v8 is what keeps this path unchanged.
    if (protocol === 'classic') {
      test(
        'a static leader that bounces rejoins at JoinGroup v8 with its assignment, and the assigner is not asked again',
        async () => {
          const assignerCalls = { count: 0 }
          const countingAssigner = (config) => {
            const inner = PartitionAssigners.roundRobin(config)
            return {
              ...inner,
              assign: async (group) => {
                assignerCalls.count += 1
                return inner.assign(group)
              },
            }
          }
          const extra = { partitionAssigners: [countingAssigner] }
          const groupId = `${topic}-bounce`
          const [first, second] = await startPair(groupId, 'instance-a', 'instance-b', extra)
          const [leader, other] = lastJoin(first).isLeader ? [first, second] : [second, first]
          const assignmentBefore = lastJoin(leader).memberAssignment
          const callsBefore = assignerCalls.count
          const rebalancesBefore = other.rebalances

          await leader.consumer.disconnect()
          const bounced = await start(groupId, leader.groupInstanceId, extra)
          const rejoined = await waitForCondition(() => owned(bounced) > 0, REJOIN_BUDGET)

          const joinVersions = bounced.requests
            .filter(({ apiName }) => apiName === 'JoinGroup')
            .map(({ apiVersion }) => apiVersion)
          writeEvidence(EVIDENCE, {
            case: 'static-leader-bounce',
            protocol,
            rejoined,
            joinVersions,
            isLeader: rejoined ? lastJoin(bounced).isLeader : null,
            assignerCallsBefore: callsBefore,
            assignerCallsAfter: assignerCalls.count,
            otherRebalances: other.rebalances - rebalancesBefore,
          })
          expect(rejoined).toBe(true)
          expect(joinVersions.length).toBeGreaterThan(0)
          expect(new Set(joinVersions)).toEqual(new Set([8]))
          expect(lastJoin(bounced).isLeader).toBe(false)
          expect(lastJoin(bounced).memberAssignment).toEqual(assignmentBefore)
          expect(assignerCalls.count).toBe(callsBefore)
          expect(other.rebalances).toBe(rebalancesBefore)
        },
        CASE_BUDGET_MS
      )
    }
  }
)
