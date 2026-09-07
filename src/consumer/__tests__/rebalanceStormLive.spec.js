const { Kafka, logLevel } = require('../../../index')
const { cooperativeSticky } = require('../assigners')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')

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
 * Awaits `promise` but gives up after `ms`, so a teardown can never outlive its hook budget.
 * @param {Promise<unknown>} promise
 * @param {number} ms
 * @returns {Promise<void>}
 */
const withTimeout = async (promise, ms) => {
  await Promise.race([promise, sleep(ms)])
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
 * Live proof that a group under sustained cooperative churn keeps delivering on the partitions
 * its stable members keep, and still converges once the churn stops.
 *
 * A third member joins and leaves for a minute while two members stay up and a producer keeps
 * writing to every partition. The delivery window is observed through `START_BATCH_PROCESS`,
 * which names its topic and partition; `FETCH` cannot, because its payload identifies a broker
 * rather than a partition. Every member's assigner is wrapped so a round takes at least
 * `SLOW_ASSIGN_MS` whichever member the coordinator elects leader.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 */
describeRebalanceStorm('Consumer > cooperative rebalance storm (live)', () => {
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

    for (const member of members) {
      await withTimeout(
        member.consumer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }

    producer &&
      (await withTimeout(
        producer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      ))
    producer = kafka = null
    members = []
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
      const startMember = async (label) => {
        const consumer = kafka.consumer({
          groupId,
          partitionAssigners: [slowAssign(cooperativeSticky, SLOW_ASSIGN_MS)],
          maxWaitTimeInMs: MAX_WAIT_TIME_MS,
        })

        const member = { label, consumer, events: [], batches: [] }

        for (const type of [consumer.events.REBALANCING, consumer.events.GROUP_JOIN]) {
          consumer.on(type, ({ timestamp, payload }) =>
            member.events.push({ type, timestamp, payload })
          )
        }
        consumer.on(consumer.events.START_BATCH_PROCESS, ({ timestamp, payload }) =>
          member.batches.push({ timestamp, payload })
        )
        consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push({ label, ...payload }))

        await consumer.connect()
        await consumer.subscribe({ topic, fromBeginning: true })
        consumer
          .run({
            eachBatch: async ({ batch }) => {
              for (const message of batch.messages) {
                received.push(`${batch.partition}:${message.offset}`)
              }
            },
          })
          .catch((e) => crashes.push({ label, error: e }))

        members.push(member)
        return member
      }

      /** @param {object} member @param {number} since @returns {Promise<boolean>} */
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
        await sleep(MEMBER_UPTIME_MS)
        await withTimeout(transient.consumer.disconnect(), DISCONNECT_BUDGET_MS)
        members = members.filter((member) => member !== transient)
        await sleep(MEMBER_DOWNTIME_MS)
        cycles++
      }
      expect(cycles).toBeGreaterThanOrEqual(2)

      // The first round the storm caused on A: it gave partitions up and kept others, and its
      // fetchers were never stopped, so the kept ones carried on being delivered while it ran.
      // The GROUP_JOIN is taken from after the REBALANCING rather than from the same filter, so
      // the two can only ever be the brackets of one round.
      const { rebalancing, groupJoin } = firstRoundAfter(memberA, stormStartedAt)
      expect(rebalancing).toBeDefined()
      expect(groupJoin).toBeDefined()

      const rebalancingAt = rebalancing.timestamp
      const groupJoinAt = groupJoin.timestamp
      expect(groupJoinAt - rebalancingAt).toBeGreaterThanOrEqual(SLOW_ASSIGN_MS)

      const keptPartitions = keptPartitionsOf(
        beforeStorm,
        groupJoin.payload.memberAssignment,
        topic
      )
      expect(keptPartitions.size).toBeGreaterThan(0)

      const deliveredDuringRound = memberA.batches.filter(
        ({ timestamp, payload }) =>
          timestamp > rebalancingAt &&
          timestamp < groupJoinAt &&
          payload.topic === topic &&
          keptPartitions.has(payload.partition)
      )
      expect(deliveredDuringRound.length).toBeGreaterThanOrEqual(1)

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
        topic,
        groupId,
        cycles,
        producedPerPartition: ticks,
        distinctPositions: new Set(received).size,
        roundDurationMs: groupJoinAt - rebalancingAt,
        keptPartitions: [...keptPartitions],
        deliveriesOnKeptPartitionsDuringRound: deliveredDuringRound.length,
        finalAssignments: {
          A: lastAssignment(memberA).memberAssignment,
          B: lastAssignment(memberB).memberAssignment,
        },
      })
    },
    STORM_BUDGET_MS
  )
})
