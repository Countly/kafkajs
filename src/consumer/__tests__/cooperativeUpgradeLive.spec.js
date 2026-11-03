const { Kafka, logLevel } = require('../../../index')
const { roundRobin, cooperativeSticky } = require('../assigners')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')
const { withDeadline, disconnectAll, startTrackedMember } = require('./fixtures/boundedCleanup')

// Gated: the rolling upgrade needs its own cluster time and would race the other consumer specs.
// Run it alone: `npm run test:e2e:cooperative-upgrade`.
const describeCooperativeUpgrade =
  process.env.COOPERATIVE_UPGRADE_E2E === '1' ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const ROUND_BUDGET_MS = 180000
const SLOW_ASSIGN_MS = 3000
const PARTITIONS = 4
const PRODUCE_INTERVAL_MS = 200
const MAX_WAIT_TIME_MS = 1000
const QUIET_MS = 4000 // longer than one slow-assign round, so a follow-up round cannot hide in it
const QUIET_BUDGET = { attempts: 15 } // 60 s of quiet windows
const JOIN_BUDGET = { attempts: 120, delay: 500 } // 60 s - a round plus the slow assign
const DELIVERY_BUDGET = { attempts: 120, delay: 500 } // 60 s - records already on the broker
const DISCONNECT_BUDGET_MS = 15000

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `predicate` on a plain interval until it is true, bounded rather than indefinite: a
 * regression that stalls a round must fail on the assertion that follows, not hang for the whole
 * suite timeout.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {{attempts: number, delay: number}} [budget]
 * @returns {Promise<boolean>} whether `predicate` became true within the attempt budget
 */
const waitForCondition = async (predicate, budget = DELIVERY_BUDGET) => {
  for (let attempt = 0; attempt < budget.attempts; attempt++) {
    if (await predicate()) {
      return true
    }
    await sleep(budget.delay)
  }
  return false
}

/**
 * Wraps an assigner factory so the leader's `assign()` takes at least `delayMs`, stretching the
 * window between a round's `REBALANCING` and its `GROUP_JOIN` into something a delivery can
 * provably fall inside. Every other member of the interface is forwarded to the wrapped
 * assigner, which stays the one holding the assigner's own state.
 * @param {import('types').PartitionAssigner} createAssigner
 * @param {number} delayMs
 * @returns {import('types').PartitionAssigner}
 */
const slowAssign = (createAssigner, delayMs) => (options) => {
  const inner = createAssigner(options)
  return {
    name: inner.name,
    version: inner.version,
    supportedProtocols: () => inner.supportedProtocols(),
    protocol: (subscription) => inner.protocol(subscription),
    onAssignment: (assignment, metadata) => inner.onAssignment(assignment, metadata),
    assign: async (group) => {
      await sleep(delayMs)
      return inner.assign(group)
    },
  }
}

/**
 * Waits until no member of `members` has emitted a `GROUP_JOIN` for a whole `quietMs` window.
 *
 * A cooperative round that revoked something runs a follow-up round, so a single `GROUP_JOIN` does
 * not mean the chain is over. Without this, a marker taken between two rounds of one chain leaves
 * that round's `REBALANCING` before it and its `GROUP_JOIN` after it, and any later pairing reads
 * across rounds.
 *
 * @param {object[]} members
 * @param {number} quietMs
 * @param {{attempts: number}} budget
 * @returns {Promise<boolean>} whether the group went quiet within the attempt budget
 */
const waitForQuiet = async (members, quietMs, budget) => {
  const joins = () =>
    members.reduce(
      (total, member) =>
        total +
        member.events.filter(({ type }) => type === member.consumer.events.GROUP_JOIN).length,
      0
    )

  for (let attempt = 0; attempt < budget.attempts; attempt++) {
    const before = joins()
    await sleep(quietMs)
    if (joins() === before) {
      return true
    }
  }
  return false
}

/**
 * The brackets of the first round a member ran at or after `since`: the first `REBALANCING`, and
 * the first `GROUP_JOIN` **after that event**. Taking the two from one filter independently can
 * pair the tail of one round with the head of the next and yield a negative duration.
 *
 * @param {object} member
 * @param {number} since
 * @returns {{rebalancing: object|undefined, groupJoin: object|undefined}}
 */
const firstRoundAfter = (member, since) => {
  const rebalancing = member.events.find(
    ({ type, timestamp }) => type === member.consumer.events.REBALANCING && timestamp >= since
  )
  const groupJoin =
    rebalancing &&
    member.events.find(
      ({ type, timestamp }) =>
        type === member.consumer.events.GROUP_JOIN && timestamp >= rebalancing.timestamp
    )

  return { rebalancing, groupJoin }
}

/**
 * The partitions a member held both before and after one round, read from the `GROUP_JOIN`
 * payloads on either side of it rather than from the round's delta.
 * @param {{[topic: string]: number[]}} before
 * @param {{[topic: string]: number[]}} after
 * @param {string} topic
 * @returns {Set<number>}
 */
const keptPartitionsOf = (before, after, topic) => {
  const held = new Set(after[topic] || [])
  return new Set((before[topic] || []).filter((partition) => held.has(partition)))
}

/**
 * Live proof that Java's two-step rolling upgrade to cooperative rebalancing works against a
 * real coordinator, and that a cooperative round does not stop delivery on the partitions a
 * member keeps.
 *
 * Step 1 runs every member with `[roundRobin, cooperativeSticky]`, whose protocol intersection is
 * still EAGER; step 2 drops the eager assigner and the same group turns COOPERATIVE without an
 * `INCONSISTENT_GROUP_PROTOCOL` at any point.
 *
 * The kept-partition window is observed through `START_BATCH_PROCESS`, which names its topic and
 * partition; `FETCH` cannot, because its payload identifies a broker rather than a partition. The
 * leader's assigner is wrapped so the round takes at least `SLOW_ASSIGN_MS`, which is what makes
 * the window real rather than a race.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 */
describeCooperativeUpgrade('Consumer > cooperative rolling upgrade (live)', () => {
  let kafka
  let producer
  let members
  const producing = { on: false }

  beforeEach(() => {
    members = []
    producing.on = false
  })

  afterEach(async () => {
    producing.on = false

    try {
      await disconnectAll(
        [...members.map((member) => member.consumer), producer],
        DISCONNECT_BUDGET_MS
      )
    } finally {
      producer = kafka = null
      members = []
    }
  }, 120000)

  it(
    'keeps delivering on a kept partition across a cooperative round',
    async () => {
      const topic = `test-topic-${secureRandom()}`
      const groupId = `test-group-${secureRandom()}`
      await createTopic({ topic, partitions: PARTITIONS })

      kafka = new Kafka({
        clientId: 'cooperative-upgrade',
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
      })

      // Every member pushes into the same arrays, so "no loss" is a property of the group rather
      // than of whichever member happened to hold a partition at the time.
      const received = []
      const crashes = []

      /**
       * Builds one member of the group and starts it consuming.
       * @param {string} label
       * @param {import('types').PartitionAssigner[]} partitionAssigners
       * @returns {Promise<object>} the member, with its own event log
       */
      const startMember = async (label, partitionAssigners) =>
        startTrackedMember({
          kafka,
          consumerOptions: {
            groupId,
            partitionAssigners,
            maxWaitTimeInMs: MAX_WAIT_TIME_MS,
          },
          label,
          members,
          crashes,
          received,
          topic,
        })

      /** @param {object} member @returns {Promise<boolean>} */
      const waitForJoin = (member, since) =>
        waitForCondition(
          () =>
            member.events.some(
              ({ type, timestamp }) =>
                type === member.consumer.events.GROUP_JOIN && timestamp >= since
            ),
          JOIN_BUDGET
        )

      /** @param {object} member @returns {object} the payload of the member's last GROUP_JOIN */
      const lastAssignment = (member) =>
        member.events.filter(({ type }) => type === member.consumer.events.GROUP_JOIN).at(-1)
          .payload

      producer = kafka.producer()
      await producer.connect()

      // One record per partition per tick, so every partition keeps producing work for whoever
      // owns it and the produced count per partition is exactly the number of ticks.
      let ticks = 0
      producing.on = true
      const produceLoop = (async () => {
        while (producing.on) {
          await producer.send({
            topic,
            messages: Array.from({ length: PARTITIONS }, (_, partition) => ({
              partition,
              key: `k-${ticks}`,
              value: `v-${ticks}`,
            })),
          })
          ticks++
          await sleep(PRODUCE_INTERVAL_MS)
        }
      })().catch((e) => crashes.push({ label: 'producer', error: e }))

      // Step 1: both members carry the eager assigner first, so the group protocol stays eager.
      const stepOneStartedAt = Date.now()
      const memberA = await startMember('A(step1)', [roundRobin, cooperativeSticky])
      const memberB = await startMember('B(step1)', [roundRobin, cooperativeSticky])
      expect(await waitForJoin(memberA, stepOneStartedAt)).toBe(true)
      expect(await waitForJoin(memberB, stepOneStartedAt)).toBe(true)

      expect(lastAssignment(memberA).rebalanceProtocol).toEqual('eager')
      expect(lastAssignment(memberB).rebalanceProtocol).toEqual('eager')

      // Step 2, first half: A comes back with the cooperative assigner alone. The group protocol
      // is now cooperative-sticky, and A reports the cooperative mode while B still reports eager.
      await withDeadline(memberA.consumer.disconnect(), DISCONNECT_BUDGET_MS)
      const upgradeStartedAt = Date.now()
      const memberA2 = await startMember('A(step2)', [
        slowAssign(cooperativeSticky, SLOW_ASSIGN_MS),
      ])
      expect(await waitForJoin(memberA2, upgradeStartedAt)).toBe(true)
      expect(lastAssignment(memberA2).rebalanceProtocol).toEqual('cooperative')

      // Step 2, second half: B leaves and A2 converges onto the whole topic, so the round B2's
      // arrival triggers is the one that takes partitions away from a member that keeps others.
      await withDeadline(memberB.consumer.disconnect(), DISCONNECT_BUDGET_MS)
      const soleOwner = await waitForCondition(() => {
        const assignment = lastAssignment(memberA2).memberAssignment
        return (assignment[topic] || []).length === PARTITIONS
      }, JOIN_BUDGET)
      expect(soleOwner).toBe(true)

      // `soleOwner` proves the previous chain converged, but not that it has stopped emitting: a
      // follow-up round still in flight would straddle the marker taken below.
      expect(await waitForQuiet([memberA2], QUIET_MS, QUIET_BUDGET)).toBe(true)

      const beforeRound = lastAssignment(memberA2).memberAssignment
      const roundStartedAt = Date.now()
      // Both step-2 members carry the slow wrapper, so the round is stretched whichever of them
      // the coordinator elects leader.
      const memberB2 = await startMember('B(step2)', [
        slowAssign(cooperativeSticky, SLOW_ASSIGN_MS),
      ])
      expect(await waitForJoin(memberB2, roundStartedAt)).toBe(true)
      expect(await waitForJoin(memberA2, roundStartedAt)).toBe(true)

      // The GROUP_JOIN is taken from after the REBALANCING rather than from the same filter, so
      // the two can only ever be the brackets of one round.
      const { rebalancing, groupJoin } = firstRoundAfter(memberA2, roundStartedAt)
      expect(rebalancing).toBeDefined()
      expect(groupJoin).toBeDefined()

      // Both brackets belong to the same round by construction: they are emitted by one `runRound`
      // call, and the slow assigner makes the gap between them at least SLOW_ASSIGN_MS.
      const rebalancingAt = rebalancing.timestamp
      const groupJoinAt = groupJoin.timestamp
      expect(groupJoinAt - rebalancingAt).toBeGreaterThanOrEqual(SLOW_ASSIGN_MS)

      const keptPartitions = keptPartitionsOf(
        beforeRound,
        groupJoin.payload.memberAssignment,
        topic
      )
      expect(keptPartitions.size).toBeGreaterThan(0)

      const deliveredDuringRound = memberA2.batches.filter(
        ({ timestamp, payload }) =>
          timestamp > rebalancingAt &&
          timestamp < groupJoinAt &&
          payload.topic === topic &&
          keptPartitions.has(payload.partition)
      )

      // The point of the cooperative protocol: a round does not stop the fetchers, so a partition
      // this member keeps carries on being delivered while the round runs.
      expect(deliveredDuringRound.length).toBeGreaterThanOrEqual(1)

      producing.on = false
      await produceLoop
      const producedPerPartition = ticks

      const covered = () => {
        const distinct = new Set(received)
        return Array.from({ length: PARTITIONS }).every((_, partition) =>
          Array.from({ length: producedPerPartition }).every((_, offset) =>
            distinct.has(`${partition}:${offset}`)
          )
        )
      }
      expect(await waitForCondition(covered)).toBe(true)

      // No loss: `covered()` above is the property. This bound only says nothing was dropped from
      // the range; it is `>=` because the producer is not idempotent in this release, so a
      // producer-side retry can legitimately write more records than there were ticks.
      expect(new Set(received).size).toBeGreaterThanOrEqual(producedPerPartition * PARTITIONS)

      // No INCONSISTENT_GROUP_PROTOCOL (23) at any point of the upgrade, and - deliberately - no
      // crash of any kind, restartable ones included: a planned two-step upgrade with one member
      // moving at a time is a sequence this client is expected to absorb without ever crashing.
      expect(crashes.map(({ error }) => error && error.type).filter(Boolean)).not.toContain(
        'INCONSISTENT_GROUP_PROTOCOL'
      )
      expect(crashes).toEqual([])

      console.log('cooperative rolling upgrade observed', {
        topic,
        groupId,
        producedPerPartition,
        distinctPositions: new Set(received).size,
        duplicateDeliveries: received.length - new Set(received).size,
        roundDurationMs: groupJoinAt - rebalancingAt,
        keptPartitions: [...keptPartitions],
        deliveriesOnKeptPartitionsDuringRound: deliveredDuringRound.length,
        finalAssignments: {
          A: lastAssignment(memberA2).memberAssignment,
          B: lastAssignment(memberB2).memberAssignment,
        },
      })
    },
    ROUND_BUDGET_MS
  )
})
