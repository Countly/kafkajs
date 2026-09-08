const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')
const { sleep, waitForCondition, withTimeout, createLedger } = require('./fixtures/liveScenario')

// Gated: this scenario converts a group between protocols while members are in it, and would race
// every other consumer spec. Run it alone:
// `npm run test:e2e:consumer-protocol-migration`.
const describeMigration =
  process.env.CONSUMER_PROTOCOL_MIGRATION_E2E === '1' ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const ROW_BUDGET_MS = 300000
const PARTITIONS = 6
const MAX_WAIT_TIME_MS = 1000
const PRODUCE_INTERVAL_MS = 100
// How long each phase is left running once it has settled, so "kept consuming" is measured over a
// window rather than at an instant.
const PHASE_MS = 6000
const ASSIGNMENT_BUDGET = { attempts: 60, delay: 500 } // 30 s
const GROUP_TYPE_BUDGET = { attempts: 30, delay: 500 } // 15 s
const DRAIN_BUDGET = { attempts: 120, delay: 500 } // 60 s
const DISCONNECT_BUDGET_MS = 15000
const HOOK_BUDGET_MS = 120000

// The 4.3 coordinator converts a running group from `Classic` to `Consumer` when the first
// consumer-protocol member joins it, and back when the last one leaves; the documented
// precondition is only that the classic assignor embeds no custom metadata, which this client's
// default round-robin assigner does not. So the type is asserted at each phase, not merely
// recorded. Flip this to `false` for a broker whose `group.consumer.migration.policy` is not the
// default `bidirectional`: the four observations are then logged and only the delivery half of the
// scenario is asserted.
// @see https://kafka.apache.org/43/operations/consumer-rebalance-protocol/
const ONLINE_CONVERSION_GUARANTEED = true

/**
 * Live proof that a group survives being converted between the classic and the KIP-848 consumer
 * protocol with members still in it, in both directions: two classic members are running and
 * producing throughout when a consumer-protocol member joins, the classic members leave, fresh
 * classic members join, and the consumer-protocol member leaves.
 *
 * Two things are asserted across the four phases. The offset ledger: every produced record is
 * delivered, nothing is lost at either transition, and redelivery stays bounded. And the group's
 * type, read through `admin.describeGroups`, which routes a consumer group through
 * ConsumerGroupDescribe and falls back to DescribeGroups for a classic one.
 *
 * Ownership is read from `GROUP_JOIN`'s `memberAssignment`, which carries the member's whole
 * assignment under either protocol. `PARTITIONS_ASSIGNED` cannot be used for it: the consumer
 * protocol is always cooperative, so that event carries only the round's delta.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 */
describeMigration('Consumer > classic to consumer protocol migration (live)', () => {
  let kafka
  let admin
  let producer
  let topic
  let groupId
  /** @type {object[]} */
  let consumers

  /**
   * Builds a member of this group and records what it sees.
   * @param {string} name the label this member's deliveries are attributed to
   * @param {{groupProtocol?: 'classic'|'consumer'}} [config] extra consumer configuration
   * @returns {{consumer: object, observed: {name: string, groupJoins: object[], crashes: object[]}}}
   */
  const createMember = (name, config = {}) => {
    const consumer = kafka.consumer({
      groupId,
      maxWaitTimeInMs: MAX_WAIT_TIME_MS,
      ...config,
    })

    const observed = { name, groupJoins: [], crashes: [] }

    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => observed.groupJoins.push(payload))
    consumer.on(consumer.events.CRASH, ({ payload }) => observed.crashes.push(payload))

    consumers.push(consumer)
    return { consumer, observed }
  }

  /**
   * The partitions this member currently owns, from the whole assignment its last `GROUP_JOIN`
   * reported.
   * @param {object} observed
   * @returns {number[]} sorted ascending, empty before the member's first round
   */
  const ownedBy = (observed) => {
    const last = observed.groupJoins[observed.groupJoins.length - 1]
    if (!last) return []
    return [...(last.memberAssignment[topic] || [])].sort((a, b) => a - b)
  }

  /**
   * Whether every listed member holds part of the topic and the holdings together cover it exactly.
   * The "each holds something" half matters: a transient where one member still owns everything
   * also sums to `PARTITIONS`, and a delivery assertion made on it would prove nothing.
   * @param {...object} members the `observed` records to weigh
   * @returns {boolean}
   */
  const allSettled = (...members) => {
    const owned = members.flatMap((observed) => ownedBy(observed))
    return members.every((observed) => ownedBy(observed).length > 0) && owned.length === PARTITIONS
  }

  /**
   * Connects a member, subscribes it to the scenario topic and starts recording its deliveries.
   * @param {{consumer: object, observed: object}} member
   * @param {object} ledger
   * @returns {Promise<void>}
   */
  const start = async ({ consumer, observed }, ledger) => {
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ topic: deliveredTopic, partition, message }) =>
        ledger.delivered(observed.name, {
          topic: deliveredTopic,
          partition,
          offset: message.offset,
        }),
    })
  }

  /**
   * Produces one record per partition every `PRODUCE_INTERVAL_MS` until `stop()` is called, taking
   * each offset from the broker's own `baseOffset` rather than assuming it.
   * @param {object} ledger
   * @returns {{stop: () => Promise<void>}} `stop` rethrows whatever the loop failed with
   */
  const startProducing = (ledger) => {
    // An object rather than a plain boolean: `stop()` flips it from outside the loop, which a
    // bare local would read as never modified.
    const state = { running: true }
    let sequence = 0
    let failure = null

    const loop = (async () => {
      while (state.running) {
        const messages = Array.from({ length: PARTITIONS }, (_, partition) => ({
          key: `k-${sequence}-${partition}`,
          value: `v-${sequence}-${partition}`,
          partition,
        }))
        sequence += 1

        const result = await producer.send({ topic, messages })
        for (const { partition, baseOffset } of result) {
          ledger.produced({ topic, partition, offset: `${baseOffset}` })
        }

        await sleep(PRODUCE_INTERVAL_MS)
      }
    })().catch((error) => {
      failure = error
    })

    return {
      stop: async () => {
        state.running = false
        await loop
        if (failure) throw failure
      },
    }
  }

  /** @returns {Promise<string>} the group's type as the coordinator reports it right now */
  const observedGroupType = async () => {
    const { groups } = await admin.describeGroups([groupId])
    const [group] = groups
    // A missing-group fallback row also says 'classic'; only a described group counts.
    return group.errorCode === 0 ? group.groupType : null
  }

  beforeEach(async () => {
    topic = `e2e-migration-${secureRandom()}`
    groupId = `e2e-migration-${secureRandom()}`
    consumers = []

    await createTopic({ topic, partitions: PARTITIONS })

    kafka = new Kafka({
      clientId: 'migration-live',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    admin = kafka.admin()
    producer = kafka.producer()

    await admin.connect()
    await producer.connect()
  })

  afterEach(async () => {
    // Even on failure: a live member left in a group, or a producer still looping, poisons every
    // following row.
    for (const consumer of consumers) {
      await withTimeout(
        consumer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }

    for (const client of [producer, admin]) {
      client &&
        (await withTimeout(
          client.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        ))
    }

    producer = admin = kafka = null
    consumers = []
  }, HOOK_BUDGET_MS)

  it(
    'delivers every record while the group changes protocol and changes back',
    async () => {
      const ledger = createLedger()
      /** @type {Array<[string, string]>} phase label and the type read in it */
      const observedTypes = []

      /**
       * Records the group's type at one phase, after a bounded window in which the coordinator may
       * still be converting. What is recorded is always what was read, so a conversion that never
       * happens fails on the assertion naming the observed type rather than on a timeout here.
       * @param {string} phase
       * @param {'classic'|'consumer'} expected
       * @returns {Promise<void>}
       */
      const recordGroupType = async (phase, expected) => {
        let latest = ''
        await waitForCondition(async () => {
          latest = await observedGroupType()
          return latest === expected
        }, GROUP_TYPE_BUDGET)
        observedTypes.push([phase, latest])
      }

      const classicOne = createMember('classic-one')
      const classicTwo = createMember('classic-two')
      const modern = createMember('modern', { groupProtocol: 'consumer' })

      // Phase 1: two classic members, producing throughout.
      const producing = startProducing(ledger)
      await start(classicOne, ledger)
      await start(classicTwo, ledger)

      expect(
        await waitForCondition(
          () => allSettled(classicOne.observed, classicTwo.observed),
          ASSIGNMENT_BUDGET
        )
      ).toBe(true)
      await sleep(PHASE_MS)
      await recordGroupType('phase-1-classic-only', 'classic')

      // Phase 2: a consumer-protocol member joins the same group, which converts it.
      const beforePhaseTwo = {
        one: ledger.deliveriesBy('classic-one'),
        two: ledger.deliveriesBy('classic-two'),
      }
      await start(modern, ledger)

      expect(
        await waitForCondition(
          () => allSettled(classicOne.observed, classicTwo.observed, modern.observed),
          ASSIGNMENT_BUDGET
        )
      ).toBe(true)
      await sleep(PHASE_MS)
      await recordGroupType('phase-2-mixed', 'consumer')

      // All three consumed during the mixed phase - which is what "consuming throughout" means.
      expect(ledger.deliveriesBy('classic-one')).toBeGreaterThan(beforePhaseTwo.one)
      expect(ledger.deliveriesBy('classic-two')).toBeGreaterThan(beforePhaseTwo.two)
      expect(ledger.deliveriesBy('modern')).toBeGreaterThan(0)

      // Phase 3: the classic members leave; the consumer-protocol member takes the whole topic.
      await withTimeout(classicOne.consumer.disconnect(), DISCONNECT_BUDGET_MS)
      await withTimeout(classicTwo.consumer.disconnect(), DISCONNECT_BUDGET_MS)

      expect(
        await waitForCondition(
          () => ownedBy(modern.observed).length === PARTITIONS,
          ASSIGNMENT_BUDGET
        )
      ).toBe(true)
      await sleep(PHASE_MS)
      await recordGroupType('phase-3-consumer-only', 'consumer')

      // Phase 4: the reverse leg - classic members rejoin, the consumer-protocol member leaves.
      const classicThree = createMember('classic-three')
      const classicFour = createMember('classic-four')
      await start(classicThree, ledger)
      await start(classicFour, ledger)
      await withTimeout(modern.consumer.disconnect(), DISCONNECT_BUDGET_MS)

      expect(
        await waitForCondition(
          () => allSettled(classicThree.observed, classicFour.observed),
          ASSIGNMENT_BUDGET
        )
      ).toBe(true)
      await sleep(PHASE_MS)
      await recordGroupType('phase-4-classic-again', 'classic')

      // The two members that joined for the downgrade leg actually took work: the consumer-protocol
      // member is not asserted on here, because phase 4 is where it is being removed.
      expect(ledger.deliveriesBy('classic-three')).toBeGreaterThan(0)
      expect(ledger.deliveriesBy('classic-four')).toBeGreaterThan(0)

      // Stop producing, then let the survivors drain what is left.
      await producing.stop()

      expect(
        await waitForCondition(() => ledger.summary().missing.length === 0, DRAIN_BUDGET)
      ).toBe(true)

      const summary = ledger.summary()
      // The whole point of the scenario: nothing was lost across either transition.
      expect(summary.missing).toEqual([])
      expect(summary.produced).toBeGreaterThan(0)
      // Redelivery at a rebalance boundary is legal under at-least-once; a duplicate is reported
      // rather than tolerated silently, and the count is bounded by the number of transitions.
      expect(summary.duplicates.length).toBeLessThanOrEqual(PARTITIONS * 4)
      // No member fell out of the group at either transition.
      for (const { observed } of [classicOne, classicTwo, modern, classicThree, classicFour]) {
        expect(observed.crashes).toEqual([])
      }

      if (ONLINE_CONVERSION_GUARANTEED) {
        expect(observedTypes).toEqual([
          ['phase-1-classic-only', 'classic'],
          ['phase-2-mixed', 'consumer'],
          ['phase-3-consumer-only', 'consumer'],
          ['phase-4-classic-again', 'classic'],
        ])
      } else {
        console.log('observed group types per phase', observedTypes)
        for (const [, type] of observedTypes) {
          expect(typeof type).toEqual('string')
        }
      }
    },
    ROW_BUDGET_MS
  )
})
