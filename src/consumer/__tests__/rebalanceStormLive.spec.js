const { Kafka, logLevel } = require('../../../index')
const { cooperativeSticky } = require('../assigners')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')
const { withDeadline, disconnectAll, startTrackedMember } = require('./fixtures/boundedCleanup')

// Gated: the storm holds the group in rebalance for a minute and would race the other consumer
// specs. Run it alone: `npm run test:e2e:rebalance-storm`.
const describeRebalanceStorm = process.env.REBALANCE_STORM_E2E === '1' ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const STORM_BUDGET_MS = 240000
const STORM_DURATION_MS = 60000
const SLOW_ASSIGN_MS = 3000
const PARTITIONS = 6
const PRODUCE_INTERVAL_MS = 200
const MAX_WAIT_TIME_MS = 1000
const MEMBER_UPTIME_MS = 6000
const MEMBER_DOWNTIME_MS = 3000
// The consumer arm churns faster. There is no client assigner to slow down under that protocol -
// the coordinator computes the assignment - so a single round cannot be stretched into something a
// delivery provably falls inside; the storm runs more of them instead, and the assertion is made
// over every round rather than over the first.
const CONSUMER_MEMBER_UPTIME_MS = 3000
const CONSUMER_MEMBER_DOWNTIME_MS = 1500
const MIN_CONSUMER_ROUNDS = 4
// The producer writes every 200 ms and a fetch waits at most 1000 ms for records, so a kept
// partition that is still being fetched is delivered on again well inside this. A round that
// stopped and restarted its fetcher would not be.
const KEPT_DELIVERY_SPAN_MS = 3000
const QUIET_MS = 4000 // longer than one slow-assign round, so a follow-up round cannot hide in it
const QUIET_BUDGET = { attempts: 15 } // 60 s of quiet windows
const JOIN_BUDGET = { attempts: 120, delay: 500 } // 60 s - a round plus the slow assign
const CONVERGENCE_BUDGET = { attempts: 120, delay: 1000 } // 120 s - the group settling after the storm
const DISCONNECT_BUDGET_MS = 15000

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `predicate` on a plain interval until it is true, bounded rather than indefinite: a group
 * that never converges must fail on the assertion that follows, not hang for the suite timeout.
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
 * Wraps an assigner factory so the leader's `assign()` takes at least `delayMs`, stretching the
 * window between a round's `REBALANCING` and its `GROUP_JOIN` into something a delivery can
 * provably fall inside. Every other member of the interface is forwarded to the wrapped assigner,
 * which stays the one holding the assigner's own state.
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
 * Every round a member ran at or after `since`, each as the `GROUP_JOIN` that closed it and the
 * **last** `REBALANCING` before it. Pairing a `GROUP_JOIN` with the closest preceding
 * `REBALANCING` is what keeps a window from spanning two rounds of one chain.
 *
 * @param {object} member
 * @param {number} since
 * @returns {{rebalancing: object, groupJoin: object}[]}
 */
const roundsAfter = (member, since) => {
  const rounds = []
  let rebalancing = null

  for (const event of member.events.filter(({ timestamp }) => timestamp >= since)) {
    if (event.type === member.consumer.events.REBALANCING) {
      rebalancing = event
    } else if (event.type === member.consumer.events.GROUP_JOIN && rebalancing) {
      rounds.push({ rebalancing, groupJoin: event })
      rebalancing = null
    }
  }

  return rounds
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
 * Walks every round of the storm and measures what it cost the partitions it kept.
 *
 * Two numbers per round, because a broker-computed round is too short for one of them alone to
 * mean anything: how many deliveries landed **inside** the round's window, and how long the
 * partitions it kept went undelivered **across** it - from the last delivery before the round
 * opened to the first after it closed. Each round's "before" is the previous round's assignment,
 * so a member that gave a partition up and took it back later is never credited with keeping it.
 *
 * @param {object} member
 * @param {{rebalancing: object, groupJoin: object}[]} rounds
 * @param {{[topic: string]: number[]}} beforeStorm
 * @param {string} topic
 * @returns {{deliveries: number, roundsWithKeptPartitions: number, spansMs: number[]}} `spansMs`
 *   holds one gap per round that had a delivery on both sides of it
 */
const deliveriesOnKeptPartitions = (member, rounds, beforeStorm, topic) => {
  let before = beforeStorm
  let deliveries = 0
  let roundsWithKeptPartitions = 0
  const spansMs = []

  for (const { rebalancing, groupJoin } of rounds) {
    const kept = keptPartitionsOf(before, groupJoin.payload.memberAssignment, topic)
    before = groupJoin.payload.memberAssignment

    if (kept.size === 0) {
      continue
    }

    roundsWithKeptPartitions++
    const onKeptPartitions = member.batches.filter(
      ({ payload }) => payload.topic === topic && kept.has(payload.partition)
    )

    deliveries += onKeptPartitions.filter(
      ({ timestamp }) => timestamp > rebalancing.timestamp && timestamp < groupJoin.timestamp
    ).length

    const lastBefore = onKeptPartitions
      .filter(({ timestamp }) => timestamp <= rebalancing.timestamp)
      .at(-1)
    const firstAfter = onKeptPartitions.find(({ timestamp }) => timestamp >= groupJoin.timestamp)

    if (lastBefore && firstAfter) {
      spansMs.push(firstAfter.timestamp - lastBefore.timestamp)
    }
  }

  return { deliveries, roundsWithKeptPartitions, spansMs }
}

/**
 * Live proof that a group under sustained cooperative churn keeps delivering on the partitions
 * its stable members keep, and still converges once the churn stops.
 *
 * A third member joins and leaves for a minute while two members stay up and a producer keeps
 * writing to every partition. The delivery window is observed through `START_BATCH_PROCESS`,
 * which names its topic and partition; `FETCH` cannot, because its payload identifies a broker
 * rather than a partition. On the classic arm every member's assigner is wrapped so a round takes
 * at least `SLOW_ASSIGN_MS` whichever member the coordinator elects leader.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 */
describeRebalanceStorm('Consumer > cooperative rebalance storm (live)', () => {
  describe.each(['classic', 'consumer'])('%s protocol', (groupProtocol) => {
    // `partitionAssigners` is one of the three options the consumer protocol rejects at
    // construction, so that arm omits it: there is no client assigner under KIP-848 at all.
    const membershipOptions =
      groupProtocol === 'consumer'
        ? { groupProtocol }
        : { groupProtocol, partitionAssigners: [slowAssign(cooperativeSticky, SLOW_ASSIGN_MS)] }

    const cycle =
      groupProtocol === 'consumer'
        ? { uptimeMs: CONSUMER_MEMBER_UPTIME_MS, downtimeMs: CONSUMER_MEMBER_DOWNTIME_MS }
        : { uptimeMs: MEMBER_UPTIME_MS, downtimeMs: MEMBER_DOWNTIME_MS }

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
      'keeps delivering on kept partitions through the storm and converges afterwards',
      async () => {
        const topic = `test-topic-${secureRandom()}`
        const groupId = `test-group-${secureRandom()}`
        await createTopic({ topic, partitions: PARTITIONS })

        kafka = new Kafka({
          clientId: 'rebalance-storm',
          brokers: plainTextBrokers(),
          logLevel: logLevel.NOTHING,
        })

        const received = []
        const crashes = []

        /**
         * Builds one member of the group and starts it consuming.
         * @param {string} label
         * @returns {Promise<object>} the member, with its own event log
         */
        const startMember = async (label) =>
          startTrackedMember({
            kafka,
            consumerOptions: {
              groupId,
              ...membershipOptions,
              maxWaitTimeInMs: MAX_WAIT_TIME_MS,
            },
            label,
            members,
            crashes,
            received,
            topic,
          })

        /** @param {object} member @param {number} since @returns {Promise<boolean>} */
        const waitForJoin = (member, since) =>
          waitForCondition(
            () =>
              member.events.some(
                ({ type, timestamp, payload }) =>
                  type === member.consumer.events.GROUP_JOIN &&
                  timestamp >= since &&
                  (payload.memberAssignment[topic] || []).length > 0
              ),
            JOIN_BUDGET
          )

        /** @param {object} member @returns {object} the payload of the member's last GROUP_JOIN */
        const lastAssignment = (member) =>
          member.events.filter(({ type }) => type === member.consumer.events.GROUP_JOIN).at(-1)
            .payload

        producer = kafka.producer()
        await producer.connect()

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

        const startedAt = Date.now()
        const memberA = await startMember('A')
        const memberB = await startMember('B')
        expect(await waitForJoin(memberA, startedAt)).toBe(true)
        expect(await waitForJoin(memberB, startedAt)).toBe(true)
        expect(lastAssignment(memberA).rebalanceProtocol).toEqual('cooperative')

        // A cooperative round that revoked something runs a follow-up round, so a member's first
        // GROUP_JOIN does not mean the chain is over. The marker below is only taken once no member
        // has joined for a whole quiet window: otherwise a round still in flight would put its
        // GROUP_JOIN after the marker and its REBALANCING before it.
        expect(await waitForQuiet([memberA, memberB], QUIET_MS, QUIET_BUDGET)).toBe(true)

        const beforeStorm = lastAssignment(memberA).memberAssignment
        const stormStartedAt = Date.now()

        // The storm: a third member joins and leaves for a minute while A and B stay up.
        let cycles = 0
        while (Date.now() - stormStartedAt < STORM_DURATION_MS) {
          const transient = await startMember(`C${cycles}`)
          await sleep(cycle.uptimeMs)
          await withDeadline(transient.consumer.disconnect(), DISCONNECT_BUDGET_MS)
          members = members.filter((member) => member !== transient)
          await sleep(cycle.downtimeMs)
          cycles++
        }
        expect(cycles).toBeGreaterThanOrEqual(2)

        // What the storm did to A, and what kept being delivered while it did it.
        const rounds = roundsAfter(memberA, stormStartedAt)
        const kept = deliveriesOnKeptPartitions(memberA, rounds, beforeStorm, topic)
        let roundDurationMs = null
        let keptPartitions = new Set()

        if (groupProtocol === 'classic') {
          // The first round the storm caused on A: it gave partitions up and kept others, and its
          // fetchers were never stopped, so the kept ones carried on being delivered while it ran.
          // The GROUP_JOIN is taken from after the REBALANCING rather than from the same filter, so
          // the two can only ever be the brackets of one round.
          const { rebalancing, groupJoin } = firstRoundAfter(memberA, stormStartedAt)
          expect(rebalancing).toBeDefined()
          expect(groupJoin).toBeDefined()

          roundDurationMs = groupJoin.timestamp - rebalancing.timestamp
          expect(roundDurationMs).toBeGreaterThanOrEqual(SLOW_ASSIGN_MS)

          keptPartitions = keptPartitionsOf(beforeStorm, groupJoin.payload.memberAssignment, topic)
          expect(keptPartitions.size).toBeGreaterThan(0)

          const deliveredDuringRound = memberA.batches.filter(
            ({ timestamp, payload }) =>
              timestamp > rebalancing.timestamp &&
              timestamp < groupJoin.timestamp &&
              payload.topic === topic &&
              keptPartitions.has(payload.partition)
          )
          // The whole round is carried into the failure: a zero here is either a fetcher this
          // round stopped or a producer that had gone quiet, and only the numbers tell them apart.
          expect({
            deliveredOnKeptPartitions: deliveredDuringRound.length > 0,
            deliveredDuringRound: deliveredDuringRound.length,
            roundDurationMs,
            keptPartitions: [...keptPartitions],
            batchesDuringRound: memberA.batches.filter(
              ({ timestamp }) =>
                timestamp > rebalancing.timestamp && timestamp < groupJoin.timestamp
            ).length,
            batchesDuringStorm: memberA.batches.filter(
              ({ timestamp }) => timestamp >= stormStartedAt
            ).length,
            keptDeliverySpansMs: kept.spansMs,
          }).toMatchObject({ deliveredOnKeptPartitions: true })
        } else {
          // A round on this arm is one coordinator round trip long - single-digit milliseconds -
          // so "a delivery landed inside the window" would be a coin toss rather than a test. The
          // guarantee is asserted across the round instead: for every round A ran, the partitions
          // it kept were delivered on both sides of it and the interruption between those two
          // deliveries stayed inside the bound. A round that stopped and restarted a kept
          // partition's fetcher is what that bound exists to catch.
          expect(rounds.length).toBeGreaterThanOrEqual(MIN_CONSUMER_ROUNDS)
          expect(kept.roundsWithKeptPartitions).toBeGreaterThanOrEqual(MIN_CONSUMER_ROUNDS)
          expect(kept.spansMs.length).toBeGreaterThanOrEqual(MIN_CONSUMER_ROUNDS)
          expect({
            everySpanInsideTheBound: Math.max(...kept.spansMs) <= KEPT_DELIVERY_SPAN_MS,
            widestKeptDeliverySpanMs: Math.max(...kept.spansMs),
            keptDeliverySpanBoundMs: KEPT_DELIVERY_SPAN_MS,
            keptDeliverySpansMs: kept.spansMs,
            deliveriesInsideRounds: kept.deliveries,
          }).toMatchObject({ everySpanInsideTheBound: true })
        }

        // The group converges once the last event is over: the two surviving members hold every
        // partition exactly once between them.
        const converged = await waitForCondition(() => {
          const held = [memberA, memberB].flatMap(
            (member) => lastAssignment(member).memberAssignment[topic] || []
          )
          return held.length === PARTITIONS && new Set(held).size === PARTITIONS
        }, CONVERGENCE_BUDGET)
        expect(converged).toBe(true)

        producing.on = false
        await produceLoop

        // Deliberately strict, restartable crashes included: a member joining and leaving is the
        // ordinary event a cooperative group exists to absorb, so any crash under this storm is a
        // regression rather than noise.
        expect(crashes).toEqual([])

        console.log('cooperative rebalance storm observed', {
          groupProtocol,
          topic,
          groupId,
          cycles,
          producedPerPartition: ticks,
          distinctPositions: new Set(received).size,
          stormRounds: rounds.length,
          roundDurationsMs: rounds.map(
            ({ rebalancing, groupJoin }) => groupJoin.timestamp - rebalancing.timestamp
          ),
          firstRoundDurationMs: roundDurationMs,
          keptPartitionsOfFirstRound: [...keptPartitions],
          roundsWithKeptPartitions: kept.roundsWithKeptPartitions,
          deliveriesOnKeptPartitionsDuringRounds: kept.deliveries,
          keptDeliverySpansMs: kept.spansMs,
          widestKeptDeliverySpanMs: kept.spansMs.length ? Math.max(...kept.spansMs) : null,
          finalAssignments: {
            A: lastAssignment(memberA).memberAssignment,
            B: lastAssignment(memberB).memberAssignment,
          },
        })
      },
      STORM_BUDGET_MS
    )
  })
})
