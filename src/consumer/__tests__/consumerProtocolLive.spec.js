const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')
const { waitForCondition, withTimeout, createLedger } = require('./fixtures/liveScenario')

// Gated: this scenario runs a real KIP-848 group and would race the other consumer specs. Run it
// alone: `npm run test:e2e:consumer-protocol`.
const describeConsumerProtocol =
  process.env.CONSUMER_PROTOCOL_E2E === '1' ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const ROW_BUDGET_MS = 180000
const PARTITIONS = 6
const MESSAGES = 60
const MAX_WAIT_TIME_MS = 1000
// The compose file pins the coordinator to a 500 ms heartbeat and a 6000 ms session timeout, so a
// dynamic leave must be observed well inside the session timeout for the assertion to mean
// "the leave released them" rather than "the session expired".
const LEAVE_RELEASE_BUDGET = { attempts: 20, delay: 200 } // 4 s, under the 6 s session timeout
const DELIVERY_BUDGET = { attempts: 120, delay: 500 } // 60 s
const ASSIGNMENT_BUDGET = { attempts: 60, delay: 500 } // 30 s
const DISCONNECT_BUDGET_MS = 15000

/**
 * Live proof that a member on the KIP-848 consumer group protocol joins, reconciles and
 * acknowledges against a real coordinator: records reach `eachMessage` and `eachBatch`, the whole
 * membership rides ConsumerGroupHeartbeat with no JoinGroup or LeaveGroup anywhere, a static member
 * keeps the same partitions across a restart, and a dynamic member's leave releases its partitions
 * inside the session timeout rather than by lapsing it.
 *
 * Ownership is read from `GROUP_JOIN`'s `memberAssignment`, which carries the member's whole
 * assignment. `PARTITIONS_ASSIGNED` cannot be used for it: this protocol is cooperative, so that
 * event carries only the round's delta.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 */
describeConsumerProtocol('Consumer > the consumer group protocol (live)', () => {
  let kafka
  let producer
  let topic
  let groupId
  /** @type {object[]} */
  let consumers

  /**
   * Builds a consumer on the KIP-848 protocol and records what it sees. `sessionTimeout` and
   * `heartbeatInterval` are deliberately absent: the coordinator owns both, and passing either
   * throws at construction.
   * @param {string} name the label this member's deliveries are attributed to
   * @param {{groupInstanceId?: string}} [options] static membership id, when the member is static
   * @returns {{consumer: object, observed: object}}
   */
  const createMember = (name, { groupInstanceId } = {}) => {
    const consumer = kafka.consumer({
      groupId,
      groupProtocol: 'consumer',
      maxWaitTimeInMs: MAX_WAIT_TIME_MS,
      ...(groupInstanceId ? { groupInstanceId } : {}),
    })

    const observed = { name, requests: [], groupJoins: [], assigned: [], lost: [], crashes: [] }

    consumer.on(consumer.events.REQUEST, ({ payload }) =>
      observed.requests.push({ apiName: payload.apiName, sentAt: payload.sentAt })
    )
    consumer.on(consumer.events.GROUP_JOIN, ({ timestamp, payload }) =>
      observed.groupJoins.push({ timestamp, payload })
    )
    consumer.on(consumer.events.PARTITIONS_ASSIGNED, ({ timestamp, payload }) =>
      observed.assigned.push({ timestamp, payload })
    )
    consumer.on(consumer.events.PARTITIONS_LOST, ({ payload }) => observed.lost.push(payload))
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
    return [...(last.payload.memberAssignment[topic] || [])].sort((a, b) => a - b)
  }

  /** @returns {number[]} every partition of the scenario topic, ascending */
  const allPartitions = () => Array.from({ length: PARTITIONS }, (_, index) => index)

  /**
   * Whether both members hold part of the topic and the two holdings together cover it exactly.
   * The "both hold something" half matters: a transient where one member still owns everything
   * also sums to `PARTITIONS`, and asserting on it would prove nothing about the assignment.
   * @param {object} first
   * @param {object} second
   * @returns {boolean}
   */
  const bothSettled = (first, second) => {
    const owned = [...ownedBy(first), ...ownedBy(second)]
    return ownedBy(first).length > 0 && ownedBy(second).length > 0 && owned.length === PARTITIONS
  }

  beforeEach(async () => {
    topic = `e2e-consumer-protocol-${secureRandom()}`
    groupId = `e2e-consumer-protocol-${secureRandom()}`
    consumers = []

    await createTopic({ topic, partitions: PARTITIONS })

    kafka = new Kafka({
      clientId: 'consumer-protocol-live',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    producer = kafka.producer()
    await producer.connect()
  })

  afterEach(async () => {
    // Even on failure: a live member left in a group poisons every following row.
    for (const consumer of consumers) {
      await withTimeout(
        consumer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }

    producer &&
      (await withTimeout(
        producer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      ))
    producer = kafka = null
    consumers = []
  }, 120000)

  /**
   * Produces `MESSAGES` records round-robin over every partition and records each one's offset in
   * the ledger, taken from the broker's own `baseOffset` rather than assumed.
   * @param {object} ledger
   * @returns {Promise<void>}
   */
  const produceRound = async (ledger) => {
    const messages = Array.from({ length: MESSAGES }, (_, index) => ({
      key: `k-${index}`,
      value: `v-${index}`,
      partition: index % PARTITIONS,
    }))

    const result = await producer.send({ topic, messages })
    for (const { partition, baseOffset } of result) {
      const count = messages.filter((message) => message.partition === partition).length
      for (let index = 0; index < count; index++) {
        ledger.produced({ topic, partition, offset: `${Number(baseOffset) + index}` })
      }
    }
  }

  it(
    'delivers every record exactly once to two members with eachMessage',
    async () => {
      const ledger = createLedger()
      const one = createMember('one')
      const two = createMember('two')

      for (const { consumer, observed } of [one, two]) {
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

      expect(
        await waitForCondition(() => bothSettled(one.observed, two.observed), ASSIGNMENT_BUDGET)
      ).toBe(true)

      await produceRound(ledger)

      expect(
        await waitForCondition(() => ledger.summary().delivered === MESSAGES, DELIVERY_BUDGET)
      ).toBe(true)

      const summary = ledger.summary()
      expect(summary.missing).toEqual([])
      expect(summary.duplicates).toEqual([])
      // Both members actually worked: a scenario where one takes everything proves nothing about
      // a broker-computed assignment.
      expect(ledger.deliveriesBy('one')).toBeGreaterThan(0)
      expect(ledger.deliveriesBy('two')).toBeGreaterThan(0)
      // The two owned sets are disjoint and together cover the topic.
      expect([...ownedBy(one.observed), ...ownedBy(two.observed)].sort((a, b) => a - b)).toEqual(
        allPartitions()
      )
    },
    ROW_BUDGET_MS
  )

  it(
    'delivers every record exactly once with eachBatch, touching every partition',
    async () => {
      const ledger = createLedger()
      const seenPartitions = new Set()
      const { consumer, observed } = createMember('batches')

      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: async ({ batch }) => {
          seenPartitions.add(batch.partition)
          for (const message of batch.messages) {
            ledger.delivered(observed.name, {
              topic: batch.topic,
              partition: batch.partition,
              offset: message.offset,
            })
          }
        },
      })

      expect(
        await waitForCondition(() => ownedBy(observed).length === PARTITIONS, ASSIGNMENT_BUDGET)
      ).toBe(true)

      await produceRound(ledger)

      expect(
        await waitForCondition(() => ledger.summary().delivered === MESSAGES, DELIVERY_BUDGET)
      ).toBe(true)

      expect(ledger.summary().missing).toEqual([])
      expect(ledger.summary().duplicates).toEqual([])
      expect([...seenPartitions].sort((a, b) => a - b)).toEqual(allPartitions())
    },
    ROW_BUDGET_MS
  )

  it(
    'joins and acknowledges over ConsumerGroupHeartbeat, and never sends JoinGroup',
    async () => {
      const { consumer, observed } = createMember('single')

      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      expect(
        await waitForCondition(() => ownedBy(observed).length === PARTITIONS, ASSIGNMENT_BUDGET)
      ).toBe(true)

      const [groupJoin] = observed.groupJoins
      const heartbeatsBeforeJoin = observed.requests.filter(
        ({ apiName, sentAt }) =>
          apiName === 'ConsumerGroupHeartbeat' && sentAt <= groupJoin.timestamp
      )
      // At least one heartbeat sent after the assignment landed: the acknowledgement.
      expect(
        await waitForCondition(
          () =>
            observed.requests.some(
              ({ apiName, sentAt }) =>
                apiName === 'ConsumerGroupHeartbeat' && sentAt > groupJoin.timestamp
            ),
          ASSIGNMENT_BUDGET
        )
      ).toBe(true)

      // The joining heartbeat, and at least one after the assignment: the acknowledgement. The
      // request payload carries no epoch, so ordering against the two events is the assertion.
      expect(heartbeatsBeforeJoin.length).toBeGreaterThanOrEqual(1)

      // The classic protocol is not used at all - not as a fallback, not for the first round.
      expect(
        observed.requests.filter(
          ({ apiName }) => apiName === 'JoinGroup' || apiName === 'SyncGroup'
        )
      ).toEqual([])

      expect(groupJoin.payload).toMatchObject({
        groupId,
        membershipProtocol: 'consumer',
        leaderId: null,
        isLeader: false,
        rebalanceProtocol: 'cooperative',
      })
      expect(observed.lost).toEqual([])
      expect(observed.crashes).toEqual([])
    },
    ROW_BUDGET_MS
  )

  // `consumer.describeGroup()` used to ask DescribeGroups directly, which answers
  // GROUP_ID_NOT_FOUND for a consumer-type group - so a live KIP-848 member described its own
  // group as `Dead` with no members.
  it(
    'describes its own group correctly through ConsumerGroupDescribe',
    async () => {
      const { consumer, observed } = createMember('describer')

      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      expect(
        await waitForCondition(() => ownedBy(observed).length === PARTITIONS, ASSIGNMENT_BUDGET)
      ).toBe(true)

      const description = await consumer.describeGroup()

      expect(description.groupId).toEqual(groupId)
      expect(description.errorCode).toEqual(0)
      expect(description.groupType).toEqual('consumer')
      expect(description.state).not.toEqual('Dead')
      expect(description.members).toHaveLength(1)
      expect(description.members[0].memberEpoch).toBeGreaterThan(0)
      expect(description.members[0].subscribedTopics).toEqual([topic])
      expect(
        description.members[0].assignment
          .filter(({ topic: assignedTopic }) => assignedTopic === topic)
          .flatMap(({ partitions }) => partitions)
          .sort((a, b) => a - b)
      ).toEqual(allPartitions())
      // The coordinator's own fields, which the classic API cannot report.
      expect(description.groupEpoch).toBeGreaterThan(0)
      expect(typeof description.assignorName).toEqual('string')
      expect(observed.crashes).toEqual([])
    },
    ROW_BUDGET_MS
  )

  it(
    'keeps a static member’s partitions across a restart, and releases a dynamic member’s on leave',
    async () => {
      const instanceId = `static-${secureRandom()}`
      const stat = createMember('static', { groupInstanceId: instanceId })
      const dyn = createMember('dynamic')

      for (const { consumer } of [stat, dyn]) {
        await consumer.connect()
        await consumer.subscribe({ topic, fromBeginning: true })
        await consumer.run({ eachMessage: async () => {} })
      }

      expect(
        await waitForCondition(() => bothSettled(stat.observed, dyn.observed), ASSIGNMENT_BUDGET)
      ).toBe(true)

      const staticPartitionsBefore = ownedBy(stat.observed)
      const dynamicPartitions = ownedBy(dyn.observed)
      await withTimeout(stat.consumer.disconnect(), DISCONNECT_BUDGET_MS)

      const restarted = createMember('static-restarted', { groupInstanceId: instanceId })
      await restarted.consumer.connect()
      await restarted.consumer.subscribe({ topic, fromBeginning: true })
      await restarted.consumer.run({ eachMessage: async () => {} })

      expect(
        await waitForCondition(
          () => ownedBy(restarted.observed).length === staticPartitionsBefore.length,
          ASSIGNMENT_BUDGET
        )
      ).toBe(true)
      // Static membership means the same partitions come back, not merely some partitions.
      expect(ownedBy(restarted.observed)).toEqual(staticPartitionsBefore)
      // And the dynamic member kept its own throughout the static member's absence.
      expect(ownedBy(dyn.observed)).toEqual(dynamicPartitions)

      const leftAt = Date.now()
      await withTimeout(dyn.consumer.disconnect(), DISCONNECT_BUDGET_MS)

      // Inside the 6 s session timeout, which is what makes this evidence that a leave heartbeat
      // was sent rather than that the session lapsed.
      expect(
        await waitForCondition(
          () =>
            dynamicPartitions.every((partition) => ownedBy(restarted.observed).includes(partition)),
          LEAVE_RELEASE_BUDGET
        )
      ).toBe(true)
      expect(Date.now() - leftAt).toBeLessThan(
        LEAVE_RELEASE_BUDGET.attempts * LEAVE_RELEASE_BUDGET.delay
      )

      // A leave is a heartbeat under this protocol, and no LeaveGroup is ever sent.
      expect(
        dyn.observed.requests.filter(({ apiName }) => apiName === 'ConsumerGroupHeartbeat').length
      ).toBeGreaterThan(0)
      expect(dyn.observed.requests.filter(({ apiName }) => apiName === 'LeaveGroup')).toEqual([])
    },
    ROW_BUDGET_MS
  )
})
