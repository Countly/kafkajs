const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, waitForTopicPropagation } = require('testHelpers')

// Gated: both rows evict a member on purpose and would race the other consumer specs. Run them
// alone: `npm run test:e2e:poll-guard`.
const describePollGuard = process.env.POLL_GUARD_E2E === '1' ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const ROW_BUDGET_MS = 120000
const MAX_POLL_INTERVAL_MS = 8000
const SESSION_TIMEOUT_MS = 30000
// Pinned rather than left at the default 3000: expiry is only detected on a heartbeat tick, so
// the tick interval is the guard's detection granularity and belongs in this file's arithmetic.
const HEARTBEAT_INTERVAL_MS = 500
// Worst case before the assertions below run: the deadline (8000) + one missed tick (500) + one
// round trip + this file's own 500 ms poll granularity + LEAVE_GROUP_BUDGET (2000) - comfortably
// under half the handler, so "the handler has not returned yet" is a margin, not a race.
const HANDLER_MS = 25000
const MAX_WAIT_TIME_MS = 1000
const NO_LEAVE_GROUP_WINDOW_MS = 20000
// The two epochs a KIP-848 leave goes out under, from the member's kind of membership.
const LEAVE_EPOCH_DYNAMIC = -1
const LEAVE_EPOCH_STATIC = -2
const EXPIRY_BUDGET = { attempts: 60, delay: 500 } // 30 s - the deadline plus one heartbeat tick
const LEAVE_GROUP_BUDGET = { attempts: 10, delay: 200 } // 2 s - one round trip, inside the handler
const REJOIN_BUDGET = { attempts: 120, delay: 500 } // 60 s - the overrunning handler plus a round
const DELIVERY_BUDGET = { attempts: 120, delay: 500 } // 60 s - records already on the broker
const DISCONNECT_BUDGET_MS = 15000

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `predicate` on a plain interval until it is true, bounded rather than indefinite: a guard
 * that never fires must fail on the assertion that follows, not hang for the suite timeout.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {{attempts: number, delay: number}} budget
 * @returns {Promise<boolean>} whether `predicate` became true within the attempt budget
 */
const waitForCondition = async (predicate, budget) => {
  for (let attempt = 0; attempt < budget.attempts; attempt++) {
    if (await predicate()) {
      return true
    }
    await sleep(budget.delay)
  }
  return false
}

/**
 * Awaits `promise` but gives up after `ms`, so a teardown can never outlive its hook budget.
 * @param {Promise<unknown>} promise
 * @param {number} ms
 * @returns {Promise<void>}
 */
const withTimeout = async (promise, ms) => {
  await Promise.race([promise, sleep(ms)])
}

/**
 * Live confirmation that the poll-interval guard evicts a member whose handler overruns, and that
 * a static member takes the local half of that sequence without leaving the group.
 *
 * Two setup details are what make the guard fire deterministically rather than by luck.
 *
 * 1. `partitionsConsumedConcurrently: 1`. The deadline is reset per batch hand-off, so with more
 *    than one worker a second batch handed to a second worker resets it while the first handler
 *    is still overrunning.
 * 2. **One partition per broker, all of them carrying records.** Fetchers are per broker, and a
 *    fetch round that returns nothing counts as a poll - an idle consumer must not be evicted for
 *    having nothing to hand a handler. A broker with no assigned partition, or with an assigned
 *    partition and no records, therefore resets the deadline from its own idle loop while another
 *    broker's partition is being overrun. Every broker owning exactly one partition, and every
 *    partition holding records, leaves every fetcher parked on the worker queue instead.
 *
 * Both constructions are protocol-independent, which is why both arms carry them verbatim.
 */
describePollGuard('Consumer > poll-interval guard (live)', () => {
  describe.each(['classic', 'consumer'])('%s protocol', (groupProtocol) => {
    // The consumer protocol rejects both classic timings at construction - the coordinator owns
    // them - so this arm omits them rather than passing values it would ignore.
    const membershipOptions =
      groupProtocol === 'consumer'
        ? { groupProtocol }
        : {
            groupProtocol,
            sessionTimeout: SESSION_TIMEOUT_MS,
            heartbeatInterval: HEARTBEAT_INTERVAL_MS,
          }

    // The request that carries this arm's rejoin after the eviction. The consumer protocol runs
    // its whole membership over one API, so the rejoin is a heartbeat like every other request:
    // it is told apart by being the first one sent after the leave.
    const rejoinApiName = groupProtocol === 'consumer' ? 'ConsumerGroupHeartbeat' : 'JoinGroup'

    let kafka
    let admin
    let producer
    let consumer

    afterEach(async () => {
      consumer &&
        (await withTimeout(
          consumer.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        ))
      producer &&
        (await withTimeout(
          producer.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        ))
      admin &&
        (await withTimeout(
          admin.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        ))
      consumer = producer = admin = kafka = null
    }, 60000)

    /**
     * Creates the scenario's topic with exactly one partition on every broker, so no fetcher can
     * go idle while a handler overruns. Returns the partition count, which is the broker count.
     * @param {string} topic
     * @returns {Promise<number>}
     */
    const createTopicOnEveryBroker = async (topic) => {
      const { brokers } = await admin.describeCluster()
      const nodeIds = brokers.map(({ nodeId }) => nodeId).sort((a, b) => a - b)

      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic,
            replicaAssignment: nodeIds.map((nodeId, partition) => ({
              partition,
              replicas: [nodeId],
            })),
          },
        ],
      })
      await waitForTopicPropagation(topic, nodeIds.length)

      return nodeIds.length
    }

    /**
     * Builds the scenario's consumer, wires the observation arrays and starts it consuming. Its
     * first delivery blocks for `HANDLER_MS`; every later one returns immediately, so the member
     * can be seen to resume after it is evicted.
     * @param {object} options
     * @param {string} options.groupId
     * @param {string} options.topic
     * @param {string|null} options.groupInstanceId
     * @returns {Promise<object>} the observation arrays and the handler's timestamps
     */
    const startConsumer = async ({ groupId, topic, groupInstanceId }) => {
      consumer = kafka.consumer({
        groupId,
        groupInstanceId,
        ...membershipOptions,
        maxPollInterval: MAX_POLL_INTERVAL_MS,
        maxWaitTimeInMs: MAX_WAIT_TIME_MS,
      })

      const observed = {
        expiries: [],
        lost: [],
        groupJoins: [],
        heartbeats: [],
        requests: [],
        crashes: [],
        received: [],
        slowHandlerStartedAt: null,
        slowHandlerFinishedAt: null,
      }

      consumer.on(consumer.events.POLL_INTERVAL_EXCEEDED, ({ timestamp, payload }) =>
        observed.expiries.push({ timestamp, payload })
      )
      consumer.on(consumer.events.PARTITIONS_LOST, ({ timestamp, payload }) =>
        observed.lost.push({ timestamp, payload })
      )
      consumer.on(consumer.events.GROUP_JOIN, ({ timestamp, payload }) =>
        observed.groupJoins.push({ timestamp, payload })
      )
      consumer.on(consumer.events.HEARTBEAT, ({ timestamp, payload }) =>
        observed.heartbeats.push({ timestamp, payload })
      )
      consumer.on(consumer.events.REQUEST, ({ payload }) =>
        observed.requests.push({ apiName: payload.apiName, sentAt: payload.sentAt })
      )
      consumer.on(consumer.events.CRASH, ({ payload }) => observed.crashes.push(payload))

      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })
      consumer
        .run({
          partitionsConsumedConcurrently: 1,
          eachMessage: async ({ partition, message }) => {
            observed.received.push(`${partition}:${message.offset}`)

            if (observed.slowHandlerStartedAt == null) {
              observed.slowHandlerStartedAt = Date.now()
              await sleep(HANDLER_MS)
              observed.slowHandlerFinishedAt = Date.now()
            }
          },
        })
        .catch((e) => observed.crashes.push({ error: e }))

      return observed
    }

    /**
     * Waits, briefly, for the wire evidence that this arm's member announced its eviction.
     *
     * The classic protocol sends a `LeaveGroup`, which the request timeline names outright. The
     * consumer protocol has no such request: the leave is a `ConsumerGroupHeartbeat` carrying a
     * negative member epoch, and the epoch a heartbeat went out under is only readable from the
     * `HEARTBEAT` event's `groupGenerationId` - the `REQUEST` event carries no body.
     * @see src/consumer/groupProtocol/consumer.js `sendLeaveHeartbeat`
     * @param {object} observed
     * @param {number} since the expiry's timestamp
     * @param {number} leaveEpoch -1 for a dynamic member, -2 for a static one; classic ignores it
     * @returns {Promise<object|undefined>} what announced the leave, or undefined within the budget
     */
    const waitForLeave = async (observed, since, leaveEpoch) => {
      const find = () =>
        groupProtocol === 'consumer'
          ? observed.heartbeats.find(
              ({ timestamp, payload }) =>
                timestamp >= since && payload.groupGenerationId === leaveEpoch
            )
          : observed.requests.find(
              ({ apiName, sentAt }) => apiName === 'LeaveGroup' && sentAt >= since
            )

      await waitForCondition(() => Boolean(find()), LEAVE_GROUP_BUDGET)
      return find()
    }

    /**
     * Every `LeaveGroup` this member ever sent. The consumer protocol must send none at all: a
     * single one would mean a classic path ran under the wrong membership.
     * @param {object} observed
     * @returns {object[]}
     */
    const leaveGroupRequests = (observed) =>
      observed.requests.filter(({ apiName }) => apiName === 'LeaveGroup')

    it(
      'evicts a dynamic member that overruns the poll interval, then rejoins and resumes',
      async () => {
        const topic = `test-topic-${secureRandom()}`
        const groupId = `test-group-${secureRandom()}`

        kafka = new Kafka({
          clientId: 'poll-guard-dynamic',
          brokers: plainTextBrokers(),
          logLevel: logLevel.NOTHING,
        })

        admin = kafka.admin()
        await admin.connect()
        const partitions = await createTopicOnEveryBroker(topic)
        expect(partitions).toBeGreaterThanOrEqual(2)

        producer = kafka.producer()
        await producer.connect()
        await producer.send({
          topic,
          messages: Array.from({ length: partitions }, (_, partition) => ({
            partition,
            key: `k-${partition}`,
            value: `v-${partition}`,
          })),
        })

        const observed = await startConsumer({ groupId, topic, groupInstanceId: null })

        const joined = await waitForCondition(
          () =>
            observed.groupJoins.some(
              ({ payload }) => (payload.memberAssignment[topic] || []).length > 0
            ),
          REJOIN_BUDGET
        )
        expect(joined).toBe(true)
        const assignmentBeforeExpiry = observed.groupJoins.at(-1).payload.memberAssignment
        const joinsBeforeExpiry = observed.groupJoins.length

        const expired = await waitForCondition(() => observed.expiries.length > 0, EXPIRY_BUDGET)
        expect(expired).toBe(true)

        const [expiry] = observed.expiries
        expect(expiry.payload.elapsedMs).toBeGreaterThanOrEqual(MAX_POLL_INTERVAL_MS)
        expect(expiry.payload.maxPollInterval).toEqual(MAX_POLL_INTERVAL_MS)
        expect(expiry.payload.isStatic).toBe(false)
        // The guard measures handler time, which is not per partition: naming partitions in this
        // payload would claim a precision it does not have.
        expect(Object.keys(expiry.payload)).not.toContain('partitions')

        // The leave goes out while the handler is still inside its sleep: a protocol action must
        // not queue behind the very handler that is overrunning. It is waited for - briefly, well
        // inside the handler's own sleep - because both arms only observe it once it has landed.
        const leave = await waitForLeave(observed, expiry.timestamp, LEAVE_EPOCH_DYNAMIC)
        expect(leave).toBeDefined()
        expect(observed.slowHandlerFinishedAt).toBeNull()

        if (groupProtocol === 'consumer') {
          expect(leaveGroupRequests(observed)).toEqual([])
        }

        const rejoined = await waitForCondition(
          () => observed.groupJoins.length > joinsBeforeExpiry,
          REJOIN_BUDGET
        )
        expect(rejoined).toBe(true)
        const rejoin = observed.groupJoins.at(-1)

        // PARTITIONS_LOST carries what this member held before the expiry, and it is emitted before
        // the rejoin's first request rather than after the assignment that replaces it.
        expect(observed.lost.length).toBeGreaterThanOrEqual(1)
        const [lost] = observed.lost
        const lostByTopic = lost.payload.partitions.reduce(
          (byTopic, entry) => ({
            ...byTopic,
            [entry.topic]: [...entry.partitions].sort((a, b) => a - b),
          }),
          {}
        )
        expect(lostByTopic).toEqual({
          [topic]: [...assignmentBeforeExpiry[topic]].sort((a, b) => a - b),
        })

        // The rejoin's own request is the first one of this arm's join API sent after the leave
        // had already gone out, so a consumer-protocol leave heartbeat cannot be mistaken for it.
        const leaveObservedAt = groupProtocol === 'consumer' ? leave.timestamp : expiry.timestamp
        const rejoinRequest = observed.requests.find(
          ({ apiName, sentAt }) => apiName === rejoinApiName && sentAt >= leaveObservedAt
        )
        expect(rejoinRequest).toBeDefined()
        expect(lost.timestamp).toBeLessThanOrEqual(rejoinRequest.sentAt)

        // Nothing is committed against the generation the member has already lost. Every request in
        // this window predates the rejoin's first request or sits between it and the new
        // GROUP_JOIN, so the ordered timeline is carried into the failure: it is the only way to
        // tell which generation a commit here belonged to.
        const commitsInWindow = observed.requests.filter(
          ({ apiName, sentAt }) =>
            apiName === 'OffsetCommit' && sentAt >= expiry.timestamp && sentAt <= rejoin.timestamp
        )
        const windowTimeline = observed.requests
          .filter(
            ({ sentAt }) => sentAt >= expiry.timestamp - 1000 && sentAt <= rejoin.timestamp + 1000
          )
          .sort((left, right) => left.sentAt - right.sentAt)
          .map(({ apiName, sentAt }) => `${apiName}@+${sentAt - expiry.timestamp}ms`)

        expect({ commits: commitsInWindow, timeline: windowTimeline }).toMatchObject({
          commits: [],
        })

        // It resumes: records produced after the rejoin are delivered by the same consumer.
        const deliveredBeforeResume = observed.received.length
        await producer.send({
          topic,
          messages: Array.from({ length: partitions }, (_, partition) => ({
            partition,
            key: `resumed-${partition}`,
            value: `resumed-${partition}`,
          })),
        })
        const resumed = await waitForCondition(
          () => observed.received.length > deliveredBeforeResume,
          DELIVERY_BUDGET
        )
        expect(resumed).toBe(true)

        console.log('poll-interval guard, dynamic member', {
          groupProtocol,
          topic,
          groupId,
          partitions,
          pollIntervalExceededPayload: expiry.payload,
          leaveEvidence: leave,
          leaveGroupRequests: leaveGroupRequests(observed).length,
          handlerStartedAt: observed.slowHandlerStartedAt,
          handlerFinishedAt: observed.slowHandlerFinishedAt,
          partitionsLost: lost.payload.partitions,
          offsetCommitsBetweenExpiryAndRejoin: commitsInWindow.length,
          timelineAroundExpiry: windowTimeline,
          deliveriesAfterRejoin: observed.received.length - deliveredBeforeResume,
        })
      },
      ROW_BUDGET_MS
    )

    it(
      'resets a static member locally, sends what its protocol demands, and rejoins',
      async () => {
        const topic = `test-topic-${secureRandom()}`
        const groupId = `test-group-${secureRandom()}`
        const groupInstanceId = `test-instance-${secureRandom()}`

        kafka = new Kafka({
          clientId: 'poll-guard-static',
          brokers: plainTextBrokers(),
          logLevel: logLevel.NOTHING,
        })

        admin = kafka.admin()
        await admin.connect()
        const partitions = await createTopicOnEveryBroker(topic)
        expect(partitions).toBeGreaterThanOrEqual(2)

        producer = kafka.producer()
        await producer.connect()
        await producer.send({
          topic,
          messages: Array.from({ length: partitions }, (_, partition) => ({
            partition,
            key: `k-${partition}`,
            value: `v-${partition}`,
          })),
        })

        const observed = await startConsumer({ groupId, topic, groupInstanceId })

        const joined = await waitForCondition(
          () =>
            observed.groupJoins.some(
              ({ payload }) => (payload.memberAssignment[topic] || []).length > 0
            ),
          REJOIN_BUDGET
        )
        expect(joined).toBe(true)
        const joinsBeforeExpiry = observed.groupJoins.length

        const expired = await waitForCondition(() => observed.expiries.length > 0, EXPIRY_BUDGET)
        expect(expired).toBe(true)

        const [expiry] = observed.expiries
        expect(expiry.payload.isStatic).toBe(true)
        expect(expiry.payload.elapsedMs).toBeGreaterThanOrEqual(MAX_POLL_INTERVAL_MS)

        // The two protocols take opposite halves of this row. A classic static member sends
        // NOTHING - KIP-345 keeps its slot for the session timeout - while a consumer-protocol
        // static member always tells the coordinator it is gone, at epoch -2, and rejoins out of
        // STALE. Reading the evidence the wrong way round is the regression this row exists to
        // catch, so each arm asserts its own mechanism rather than a shared weaker one.
        let staticLeave = null
        if (groupProtocol === 'consumer') {
          staticLeave = await waitForLeave(observed, expiry.timestamp, LEAVE_EPOCH_STATIC)
          expect(staticLeave).toBeDefined()
          expect(observed.slowHandlerFinishedAt).toBeNull()
          expect(leaveGroupRequests(observed)).toEqual([])
        } else {
          // A negative assertion needs a bound: this one is prepared to be wrong for 20 s, which is
          // longer than the whole eviction-and-rejoin sequence takes for a dynamic member.
          await sleep(NO_LEAVE_GROUP_WINDOW_MS)
          expect(
            observed.requests.filter(
              ({ apiName, sentAt }) => apiName === 'LeaveGroup' && sentAt >= expiry.timestamp
            )
          ).toEqual([])
        }

        // The local half still runs: the member resets its generation and rejoins under the same
        // instance id, keeping its group slot throughout.
        const rejoined = await waitForCondition(
          () => observed.groupJoins.length > joinsBeforeExpiry,
          REJOIN_BUDGET
        )
        expect(rejoined).toBe(true)

        const deliveredBeforeResume = observed.received.length
        await producer.send({
          topic,
          messages: Array.from({ length: partitions }, (_, partition) => ({
            partition,
            key: `resumed-${partition}`,
            value: `resumed-${partition}`,
          })),
        })
        const resumed = await waitForCondition(
          () => observed.received.length > deliveredBeforeResume,
          DELIVERY_BUDGET
        )
        expect(resumed).toBe(true)

        console.log('poll-interval guard, static member', {
          groupProtocol,
          topic,
          groupId,
          groupInstanceId,
          partitions,
          pollIntervalExceededPayload: expiry.payload,
          staticLeaveEvidence: staticLeave,
          leaveGroupRequests: leaveGroupRequests(observed).length,
          groupJoins: observed.groupJoins.length,
          deliveriesAfterRejoin: observed.received.length - deliveredBeforeResume,
        })
      },
      ROW_BUDGET_MS
    )
  })
})
