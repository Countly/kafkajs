const { Kafka, logLevel } = require('../../../index')
const Cluster = require('../../cluster')
const {
  secureRandom,
  plainTextBrokers,
  createCluster,
  createTopic,
  generateMessages,
  waitForConsumerToJoinGroup,
  writeEvidence,
  reassignPreferredReplica,
  waitForReassignmentToComplete,
  triggerPreferredElection,
  triggerUncleanElection,
  stopBroker,
  startBroker,
} = require('testHelpers')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { positionOf, runWithEvidence } = require('./fixtures/evidenceCase')
const { disconnectAll } = require('./fixtures/boundedCleanup')

// Gated because both scenarios move leadership on the shared cluster, which is not safe alongside
// the other consumer specs. Run them one at a time, each in its own foreground cluster:
// `npm run test:e2e:leader-move` and `npm run test:e2e:log-truncation` - the truncation scenario
// stops and starts brokers, so it must run alone.
const describeLeaderMove = process.env.LEADER_MOVE_E2E === '1' ? describe : describe.skip

// Produced after the move is confirmed, so every one of them is delivered post-move.
const SECOND_BATCH_SIZE = 60
const describeLogTruncation = process.env.LOG_TRUNCATION_E2E === '1' ? describe : describe.skip

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

// Named so each wait says how long it is prepared to be wrong for, rather than inheriting a
// default whose size has nothing to do with what it is waiting on.
const DELIVERY_BUDGET = { attempts: 120, delay: 500 } // 60 s - records already on the broker
const HINT_BUDGET = { attempts: 120, delay: 500 } // 60 s - one idle long poll plus the election
const BROKER_RESTART_BUDGET = { attempts: 120, delay: 1000 } // 120 s - container start + registration
const METADATA_BUDGET = { attempts: 60, delay: 1000 } // 60 s - post-election metadata convergence
// One dead-broker request timeout plus slack: a client with in-flight requests to a stopped
// container only fails them after its own `requestTimeout`, and several such disconnects in a row
// can exceed Jest's hook timeout.
const DISCONNECT_BUDGET_MS = 15000

/**
 * Prints one line per phase of a live scenario with the elapsed time since the test started.
 * A live sequence that stops making progress otherwise fails as a bare suite timeout with no
 * indication of which broker operation it was waiting on.
 * @param {number} startedAt `Date.now()` at the top of the test
 * @returns {(label: string, detail?: object) => void}
 */
const phaseLogger = (startedAt) => (label, detail) =>
  console.log(`[+${((Date.now() - startedAt) / 1000).toFixed(1)}s] ${label}`, detail || '')

/**
 * Polls `predicate` on a plain interval until it is true, bounded rather than indefinite: a
 * regression that stalls delivery must fail on the assertion that follows, not hang for the whole
 * suite timeout.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {{attempts?: number, delay?: number}} [options]
 * @returns {Promise<boolean>} whether `predicate` became true within the attempt budget
 */
const waitForCondition = async (predicate, { attempts, delay } = DELIVERY_BUDGET) => {
  for (let attempt = 0; attempt < attempts; attempt++) {
    if (await predicate()) {
      return true
    }
    await sleep(delay)
  }
  return false
}

/**
 * Live confirmation that a leadership change under a running consumer is absorbed by the fetch
 * path's leader hint, not by a group rebalance.
 *
 * The mechanism is the one `src/producer/__tests__/leaderMove.spec.js` proved for produce and is
 * the only one that yields a *hinted* `NOT_LEADER_OR_FOLLOWER`: a partition reassignment reorders
 * the replica list so a different broker is preferred, then an explicit preferred election hands
 * leadership over while the **old leader stays alive**. Stopping a container proves nothing here -
 * its socket closes before any hinted response could be written.
 *
 * The consumer's `metadataMaxAge` is far longer than the test, so an ordinary background refresh
 * cannot be what teaches this client the new leader; the producer is disconnected before the move
 * and the admin never fetches, so the hint that is observed is the consumer's own.
 *
 * The ordering is what makes the assertion deterministic rather than a race: the move happens
 * first, the test waits for the hint to actually be applied, and only then are more records
 * produced. Every one of those is delivered after the move by construction. Producing them
 * beforehand does not work - the fetcher buffers batches ahead of `eachMessage`, so a backlog can
 * be fully delivered from that buffer before the hinted response ever lands, leaving nothing in
 * the post-move window.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
 */
describeLeaderMove('Consumer > leader move during consumption (live)', () => {
  let kafka
  let adminKafka
  let admin
  let producer
  let consumer
  let applyLeaderHintSpy

  beforeEach(() => {
    applyLeaderHintSpy = jest.spyOn(Cluster.prototype, 'applyLeaderHint')
  })

  // The spy is restored only after the consumer is down: `eachMessage` reads its call count on
  // every delivery, so removing it while a batch is still in flight would race the handler.
  afterEach(async () => {
    const admitted = liveRun.takeAdmission()
    try {
      admitted && (await disconnectAll([consumer, producer, admin], DISCONNECT_BUDGET_MS))
    } finally {
      applyLeaderHintSpy && applyLeaderHintSpy.mockRestore()
      applyLeaderHintSpy = consumer = producer = admin = kafka = adminKafka = null
    }
  })

  it(
    'keeps consuming through a leader move, takes the hint path, and revalidates',
    liveCase(240000, async () => {
      const topic = `test-topic-${secureRandom()}`
      await createTopic({ topic, partitions: 1, replicas: 3 })

      // A separate client (and Cluster) for admin work, so reading metadata and driving the CLI
      // tools never touches the consumer's own cached routing.
      adminKafka = new Kafka({
        clientId: 'consumer-leader-move-admin',
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
      })
      admin = adminKafka.admin()
      await admin.connect()

      const readLeader = async () => {
        const { topics } = await admin.fetchTopicMetadata({ topics: [topic] })
        const [partition] = topics[0].partitions
        return {
          leader: partition.leader,
          leaderEpoch: partition.leaderEpoch,
          replicas: partition.replicas,
        }
      }

      const before = await readLeader()
      const newLeaderId = before.replicas.find((replicaId) => replicaId !== before.leader)
      const { brokers: nodes } = await admin.describeCluster()
      const newLeaderNode = nodes.find(({ nodeId }) => nodeId === newLeaderId)
      expect(newLeaderNode).toBeDefined()

      kafka = new Kafka({
        clientId: 'consumer-leader-move',
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
        retry: { retries: 30, initialRetryTime: 300, maxRetryTime: 1000 },
      })

      producer = kafka.producer()
      await producer.connect()

      const firstBatch = generateMessages({ number: 20 })
      await producer.send({ topic, messages: firstBatch })

      consumer = kafka.consumer({
        groupId: `test-group-${secureRandom()}`,
        // Far longer than this test: the consumer must learn of the new leader from the hint the
        // old leader sends back, not from an ordinary background refresh racing in first.
        metadataMaxAge: 600000,
        // Shorter than the default, so the idle long poll that lands on the old leader after the
        // election - and comes back hinted - is never more than this away.
        maxWaitTimeInMs: 1000,
      })

      const groupJoins = []
      const crashes = []
      const requests = []
      consumer.on(consumer.events.GROUP_JOIN, () => groupJoins.push(Date.now()))
      consumer.on(consumer.events.CRASH, (event) => crashes.push(event.payload))
      consumer.on(consumer.events.REQUEST, ({ payload }) =>
        requests.push({ apiName: payload.apiName, broker: payload.broker, observedAt: Date.now() })
      )

      // Every delivery carries the number of rejoins and the number of applied leader hints seen up
      // to that point, which is what turns "no GROUP_JOIN between the move and the next delivered
      // batch" into an assertion about ordering rather than about a final total.
      const received = []
      await consumer.subscribe({ topic, fromBeginning: true })
      consumer
        .run({
          eachMessage: async ({ message }) => {
            received.push({
              offset: Number(message.offset),
              groupJoinsSoFar: groupJoins.length,
              hintsSoFar: applyLeaderHintSpy.mock.calls.length,
            })
          },
        })
        .catch((e) => crashes.push(e))
      await waitForConsumerToJoinGroup(consumer)

      const gotFirstBatch = await waitForCondition(() => received.length >= firstBatch.length)
      expect(gotFirstBatch).toBe(true)

      // The producer is closed for the whole move window, so the only client that can be hinted
      // while leadership changes is the consumer, whose idle long polls keep hitting the old leader.
      await producer.disconnect()
      producer = null

      const groupJoinsBeforeMove = groupJoins.length
      const hintsBeforeMove = applyLeaderHintSpy.mock.calls.length
      const electionTriggeredAt = Date.now()
      reassignPreferredReplica(topic, before.replicas, newLeaderId)
      await waitForReassignmentToComplete()
      triggerPreferredElection(topic)

      // The hint path: a real hinted NOT_LEADER_OR_FOLLOWER naming the new leader reached this
      // consumer. Waiting for it here - rather than asserting on it at the end - is what makes
      // everything produced below provably post-move.
      const matchesTheMove = ([hint]) =>
        hint.topic === topic &&
        hint.leaderId === newLeaderId &&
        hint.leaderEpoch > before.leaderEpoch
      const gotHint = await waitForCondition(
        () => applyLeaderHintSpy.mock.calls.some(matchesTheMove),
        HINT_BUDGET
      )
      expect(gotHint).toBe(true)
      const hintCallIndex = applyLeaderHintSpy.mock.calls.findIndex(matchesTheMove)

      const after = await readLeader()
      expect(after.leader).toEqual(newLeaderId)

      // Consumption continues on the new leader: every one of these is produced after the move.
      const deliveredAtHint = received.length
      producer = kafka.producer()
      await producer.connect()
      const secondBatch = generateMessages({ number: SECOND_BATCH_SIZE })
      await producer.send({ topic, messages: secondBatch })

      const total = firstBatch.length + secondBatch.length
      const gotEverything = await waitForCondition(() => received.length >= total, DELIVERY_BUDGET)
      expect(gotEverything).toBe(true)

      // Every record exactly once, in order: a leader move is transparent to delivery.
      expect(received.map(({ offset }) => offset)).toEqual(
        Array.from({ length: total }, (_, i) => i)
      )

      // No rejoin between the move and the deliveries that followed it. "After the move" is keyed on
      // the hint count, so a record in this window was demonstrably delivered after this client
      // learned of the new leader, and the whole second batch is in it by construction.
      // Before this change a partition-level NOT_LEADER_OR_FOLLOWER discarded the whole node's
      // response and forced a joinAndSync, which would show up as a higher count on these records.
      const deliveredAfterMove = received.filter(({ hintsSoFar }) => hintsSoFar > hintsBeforeMove)
      expect(deliveredAfterMove.length).toBeGreaterThanOrEqual(secondBatch.length)
      for (const { groupJoinsSoFar } of deliveredAfterMove) {
        expect(groupJoinsSoFar).toEqual(groupJoinsBeforeMove)
      }

      // The changed leader epoch re-arms validation, and the request goes to the new leader.
      const validation = requests.find(
        ({ apiName, observedAt }) =>
          apiName === 'OffsetForLeaderEpoch' && observedAt >= electionTriggeredAt
      )
      expect(validation).toBeDefined()
      expect(validation.broker).toEqual(`${newLeaderNode.host}:${newLeaderNode.port}`)

      expect(crashes).toEqual([])

      console.log('leader move during consumption observed', {
        topic,
        previousLeaderId: before.leader,
        previousLeaderEpoch: before.leaderEpoch,
        newLeaderId,
        hintedLeaderEpoch: applyLeaderHintSpy.mock.calls[hintCallIndex][0].leaderEpoch,
        validationBroker: validation.broker,
        msFromElectionTriggerToHint: Date.now() - electionTriggeredAt,
        msFromElectionTriggerToValidation: validation.observedAt - electionTriggeredAt,
        deliveredBeforeTheHint: deliveredAtHint,
        deliveredAfterMove: deliveredAfterMove.length,
        groupJoins: groupJoins.length,
      })
    }),
    caseTimeout(240000)
  )
})

// `replica.lag.time.max.ms` is unset in `docker-compose.yml`, so the broker default applies: a
// stopped replica stays in the ISR until it has been silent for this long.
// @see https://kafka.apache.org/43/documentation.html#brokerconfigs_replica.lag.time.max.ms
const REPLICA_LAG_TIME_MAX_MS = 30000

const TRUNCATION_EVIDENCE = 'log-truncation'
const TRUNCATION_BUDGET_MS = 420000
/** What one awaited broker or client operation of a truncation arm may take. */
const TRUNCATION_STEP_MS = 60000
/** What a truncation arm's cleanup steps may take together: one dead-broker disconnect each. */
const TRUNCATION_CLEANUP_BUDGET_MS = 4 * DISCONNECT_BUDGET_MS
/** What each broker read of a truncation arm's evidence may take. */
const TRUTH_MS = 5000

/**
 * @param {Array<{topic: string, partition: number, value: string}>} acknowledged one batch, in
 *   production order
 * @param {number} firstOffset the offset the batch's first record is expected at
 * @returns {string[]} the complete identity of each record at its expected offset
 */
const expectedPositions = (acknowledged, firstOffset) =>
  acknowledged.map((record, index) => positionOf({ ...record, offset: firstOffset + index }))

/**
 * One unclean-election arm: its clients, its broker restarts, everything it retains for its
 * evidence row, and the steps that await anything. Every awaited operation goes through the case
 * scope, and every resource has its own cleanup step, registered before the work that creates it.
 * The broker restarts run first, because a client with in-flight requests to a stopped broker
 * only fails them after its own request timeout.
 */
class TruncationArm {
  /**
   * @param {AbortSignal} scope the case's scope
   * @param {object} subscription the options of `subscribe` besides the topic
   */
  constructor(scope, subscription) {
    this.scope = scope
    this.subscription = subscription
    this.topic = `test-topic-${secureRandom()}`
    this.cleanup = new CleanupStack()
    this.kafka = new Kafka({
      clientId: 'consumer-unclean-election',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      retry: { retries: 30, initialRetryTime: 300, maxRetryTime: 1000 },
    })
    this.admin = this.kafka.admin()
    this.cluster = createCluster()
    this.producer = null
    this.consumer = null
    this.groupId = null
    /** @type {string} the step the arm reached */
    this.stage = 'created'
    /** @type {Record<string, Array<{topic: string, partition: number, value: string}>>} */
    this.acknowledged = { first: [], second: [], third: [] }
    /** @type {Array<{topic: string, partition: number, offset: string, value: string}>} */
    this.received = []
    this.truncations = []
    this.crashes = []
    this.joins = []
    /** @type {Array<{ at: number, action: string }>} the broker stops and starts, in order */
    this.brokerActions = []
    /** @type {Record<string, unknown>} what the arm learned as it went */
    this.facts = {}
    this.startedAt = Date.now()
  }

  /**
   * @param {string} stage the step the arm is about to await
   * @returns {void}
   */
  at(stage) {
    this.stage = stage
  }

  /**
   * @template T
   * @param {string} label names the operation in its time-out message
   * @param {() => Promise<T>} op
   * @returns {Promise<T>}
   */
  step(label, op) {
    return bounded(this.scope, TRUNCATION_STEP_MS, op, { label })
  }

  /**
   * @param {string} label names the wait in its time-out message
   * @param {{attempts: number, delay: number}} budget
   * @param {() => boolean|Promise<boolean>} check a failure it throws is the caller's
   * @returns {Promise<void>}
   * @throws {Error} `<label> did not settle within <ms> ms`, or what `check` threw
   */
  wait(label, budget, check) {
    return pollUntil(this.scope, budget.attempts * budget.delay, check, {
      delayMs: budget.delay,
      label,
    })
  }

  /**
   * Stops or starts a broker container; the call itself cannot be bounded, so it is recorded
   * before it is made.
   * @param {'stop'|'start'} action
   * @param {string} service
   * @returns {void}
   */
  broker(action, service) {
    this.brokerActions.push({ at: Date.now() - this.startedAt, action: `${action} ${service}` })
    this.scope.throwIfAborted()
    if (action === 'stop') stopBroker(service)
    else startBroker(service)
  }

  /**
   * Registers every close, the broker restarts last so they run first, and creates the topic and
   * the group that the cluster's node 1 coordinates.
   * @param {(context: {admin: object, topic: string, groupId: string}) => Promise<void>} [prepare]
   *   runs before the consumer joins, with the group still empty
   * @returns {Promise<void>}
   */
  async open(prepare) {
    const { cleanup } = this
    cleanup.add('cluster', () => this.cluster.disconnect())
    cleanup.add('admin', () => this.admin.disconnect())
    cleanup.add('producer', () => this.producer?.disconnect())
    cleanup.add('consumer', () => this.consumer?.disconnect())
    for (const service of ['kafka3', 'kafka2']) {
      cleanup.add(`restart ${service}`, () => {
        try {
          startBroker(service)
        } catch (e) {
          // Already running, or compose is down - neither is a reason to skip the other service.
        }
      })
    }

    this.at('connect')
    await this.step('The admin connect', () => this.admin.connect())
    await this.step('The cluster connect', () => this.cluster.connect())
    this.at('create the topic')
    await this.step('The topic creation', () =>
      this.admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: this.topic,
            replicaAssignment: [{ partition: 0, replicas: [2, 3] }],
            configEntries: [{ name: 'unclean.leader.election.enable', value: 'true' }],
          },
        ],
      })
    )
    this.groupId = await this.pinnedGroup()
    this.producer = this.kafka.producer()
    await this.step('The producer connect', () => this.producer.connect())
    if (prepare) {
      this.at('prepare')
      await this.step('The preparation', () =>
        prepare({ admin: this.admin, topic: this.topic, groupId: this.groupId })
      )
    }
  }

  /**
   * Reads the initial leader, which either replica may hold, and returns a group id whose
   * coordinator is node 1, the surviving voter, so the group is not stranded while the other two
   * nodes are stopped and started.
   * @returns {Promise<string>}
   */
  async pinnedGroup() {
    const { cluster, topic } = this
    // `cluster` is the client this arm drives metadata through by hand, because it is the only
    // one whose cache it can refresh on demand: `admin`'s own cache is governed by a 5-minute
    // `metadataMaxAge`, so after the election it would keep routing to the stopped node 2.
    await this.step('The target topic', () => cluster.addTargetTopic(topic))
    await this.step('The metadata refresh', () => cluster.refreshMetadata())
    this.facts.initialLeader = this.leaderOf()
    expect([2, 3]).toContain(this.facts.initialLeader)
    for (let attempt = 0; attempt < 60; attempt++) {
      const candidate = `test-group-${secureRandom()}`
      const coordinator = await this.step('The coordinator lookup', () =>
        cluster.findGroupCoordinator({ groupId: candidate })
      )
      if (coordinator.nodeId === 1) return candidate
    }
    throw new Error('No group id is coordinated by node 1')
  }

  /** @returns {number} the node that leads partition 0 of the topic, from `cluster`'s cache */
  leaderOf() {
    return Number(Object.keys(this.cluster.findLeaderForPartitions(this.topic, [0]))[0])
  }

  /**
   * Sends one batch and, once the send is confirmed, records it as acknowledged.
   * @param {'first'|'second'|'third'} name
   * @param {number} number
   * @returns {Promise<Array<{key: string, value: string}>>} the messages sent
   */
  async produce(name, number) {
    const messages = generateMessages({ number })
    await this.step(`The ${name} send`, () => this.producer.send({ topic: this.topic, messages }))
    this.acknowledged[name] = messages.map(({ value }) => ({
      topic: this.topic,
      partition: 0,
      value,
    }))
    return messages
  }

  /**
   * Replaces the producer, which still has the stopped node cached as the leader for the whole of
   * its own `metadataMaxAge`, longer than the arm.
   * @returns {Promise<void>}
   */
  async rebuildProducer() {
    const old = this.producer
    this.producer = null
    await bounded(this.scope, DISCONNECT_BUDGET_MS, () => old.disconnect().catch(() => {}), {
      label: 'The producer disconnect',
    })
    this.producer = this.kafka.producer()
    await this.step('The producer connect', () => this.producer.connect())
  }

  /**
   * Subscribes and starts the consumer, retaining every delivery, truncation and crash.
   * @returns {Promise<void>}
   */
  async start() {
    this.consumer = this.kafka.consumer({
      groupId: this.groupId,
      // Only affects how quickly the consumer notices the new leader after its old one goes away;
      // the truncation itself is detected by validation, not by the refresh.
      metadataMaxAge: 5000,
    })
    const { consumer } = this
    consumer.on(consumer.events.LOG_TRUNCATION, (event) =>
      this.truncations.push({ ...event.payload, receivedCount: this.received.length })
    )
    consumer.on(consumer.events.CRASH, (event) => this.crashes.push(event.payload))
    consumer.on(consumer.events.GROUP_JOIN, (event) => this.joins.push(event.payload))
    await this.step('The consumer connect', () => consumer.connect())
    await this.step('The subscription', () =>
      consumer.subscribe({ topic: this.topic, ...this.subscription })
    )
    await this.step('The consumer run', () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          this.received.push({
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString(),
          })
        },
      })
    )
  }

  /**
   * Reads what the brokers hold, each read bounded and its failure recorded, never thrown. Brokers
   * that are stopped at the failure make the reads fail, which is itself a fact.
   * @returns {Promise<{ committed: unknown, endOffsets: unknown }>}
   */
  async readTruth() {
    const read = (label, op) =>
      bounded(this.scope, TRUTH_MS, op, { label }).catch((error) => ({
        unavailable: describeError(error),
      }))
    return {
      committed: await read('The committed read', () =>
        this.admin.fetchOffsets({ groupId: this.groupId, topics: [this.topic] })
      ),
      endOffsets: await read('The end offsets read', () =>
        this.admin.fetchTopicOffsets(this.topic)
      ),
    }
  }

  /**
   * Writes the evidence row; the write is synchronous after the broker reads, so an aborted case
   * scope cannot stop it.
   * @param {Error|null} failure the arm's own failure, when it has one
   * @returns {Promise<Error|null>} the write's failure, which never throws
   */
  async write(failure) {
    try {
      const truth = await this.readTruth()
      writeEvidence(TRUNCATION_EVIDENCE, {
        scenario: TRUNCATION_EVIDENCE,
        case: this.subscription.offsetReset === 'none' ? 'none-policy' : 'from-beginning',
        stage: this.stage,
        error: describeError(failure),
        topic: this.topic,
        groupId: this.groupId,
        acknowledged: this.acknowledged,
        received: this.received,
        truncations: this.truncations,
        crashes: this.crashes.map((crash) => describeError(crash.error ?? crash)),
        joins: this.joins.length,
        brokerActions: this.brokerActions,
        brokerTruth: truth,
        ...this.facts,
      })
      return null
    } catch (error) {
      return error
    }
  }

  /**
   * Runs the arm's body, writes its row however it ended, and only then runs the cleanups.
   * @param {() => Promise<void>} body
   * @returns {Promise<void>}
   * @throws {Error} see `runWithEvidence`
   */
  guarded(body) {
    return runWithEvidence(
      this.cleanup,
      (failure) => this.write(failure),
      body,
      TRUNCATION_CLEANUP_BUDGET_MS
    )
  }
}

/**
 * Live confirmation that an unclean leader election - a leader whose log is genuinely shorter than
 * what the consumer already read - is detected through OffsetForLeaderEpoch validation rather than
 * silently skipping or redelivering records.
 *
 * The election is **scripted, not waited for**: `unclean.leader.election.enable` set on a running
 * topic only takes effect when the broker's periodic unclean-election thread next runs (five
 * minutes by default), so the topic is created with the config and the election is triggered
 * explicitly.
 * @see clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java (Apache Kafka 4.3.1)
 *
 * Three facts about the compose cluster this procedure depends on, stated so a failure here is
 * diagnosable rather than mysterious:
 *
 * 1. `replica.lag.time.max.ms` is unset, so the default 30 s applies (see the constant above):
 *    after stopping a broker the test must wait that out before the replica leaves the ISR. A
 *    shorter step timeout turns this into an unexplainable flake.
 * 2. `min.insync.replicas` is unset, so the default 1 applies. The compose file sets
 *    `KAFKA_DEFAULT_REPLICATION_FACTOR: 2` and `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2` but no
 *    min-ISR; raising it to 2 would turn the produce that follows the stop into a hang.
 * 3. The topic is created through `admin.createTopics` with an explicit replica assignment and
 *    config - JS, like the rest of the spec, not a `kafka-topics.sh` shell-out.
 *
 * kafka1 stays out of the replica set so it survives as the bootstrap broker and as a
 * controller-quorum voter, and the group coordinator is pinned to it so the group is not stranded
 * during the window. The controller quorum is 1-of-3 only between "stop kafka2" and "kafka3 is
 * up", which is expected.
 */
describeLogTruncation('Consumer > unclean leader election (live)', () => {
  // The arm restarts its own brokers and closes its own clients; this is the net for a case whose
  // cleanup was cut short, because a stopped broker would otherwise degrade the shared cluster.
  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    for (const service of ['kafka2', 'kafka3']) {
      try {
        startBroker(service)
      } catch (e) {
        // Already running, or compose is down - neither is a reason to skip the other service.
      }
    }
  }, 120000)

  /**
   * The unclean-election scenario as one case. The subscription and an optional step before the
   * consumer joins are the only things an arm varies; everything it asserts is shared.
   * @param {object} subscription the options of `subscribe` besides the topic
   * @param {(context: {admin: object, topic: string, groupId: string}) => Promise<void>} [prepare]
   *   runs before the consumer joins, with the group still empty
   * @returns {() => Promise<void>} the case for `it`
   */
  const truncationCase = (subscription, prepare) =>
    liveCase(TRUNCATION_BUDGET_MS, (scope) => {
      const arm = new TruncationArm(scope, subscription)
      return arm.guarded(async () => {
        const { topic } = arm
        const phase = phaseLogger(arm.startedAt)
        await arm.open(prepare)
        phase('topic created', { initialLeader: arm.facts.initialLeader })

        arm.at('produce the first batch')
        const firstBatch = await arm.produce('first', 10)
        arm.at('start the consumer')
        await arm.start()
        arm.at('wait for the first batch')
        await arm.wait(
          'The first batch',
          DELIVERY_BUDGET,
          () => arm.received.length >= firstBatch.length
        )
        phase('first batch consumed')

        // kafka3 goes away and is waited out of the ISR, so what follows exists only on kafka2.
        arm.at('stop kafka3')
        arm.broker('stop', 'kafka3')
        await delay(scope, REPLICA_LAG_TIME_MAX_MS + 5000)
        phase('kafka3 stopped and waited out of the ISR')

        arm.at('produce the second batch')
        const secondBatch = await arm.produce('second', 10)
        const consumedBeforeTruncation = firstBatch.length + secondBatch.length
        arm.at('wait for the second batch')
        await arm.wait(
          'The second batch',
          DELIVERY_BUDGET,
          () => arm.received.length >= consumedBeforeTruncation
        )
        // The runner resolves an offset after `eachMessage` returns, so give it a moment: the
        // position the truncation is asserted against is the resolved one, not the delivered one.
        await delay(scope, 2000)
        phase('second batch consumed', { received: arm.received.length })

        // The only replica holding the second batch is stopped, and the stale one comes back.
        arm.at('swap the brokers')
        arm.broker('stop', 'kafka2')
        arm.broker('start', 'kafka3')
        arm.at('wait for kafka3')
        await arm.wait('Broker 3', BROKER_RESTART_BUDGET, async () => {
          try {
            await arm.cluster.findBroker({ nodeId: 3 })
            return true
          } catch (e) {
            return false
          }
        })
        phase('kafka2 stopped, kafka3 back and reachable')

        // `kafka-leader-election.sh` exits 0 in more than one situation, so the exit status alone
        // proves nothing - the printed outcome is what says which one happened. Two of them are
        // expected here: the command performed the election ("Successfully completed leader
        // election"), or the controller had already performed it ("Valid replica already
        // elected"), which is the common case because the topic carries
        // `unclean.leader.election.enable=true` and the controller may unclean-elect the returning
        // replica the moment it registers, before this line runs. A failed election prints "Error
        // completing leader election" and throws. Which of the two ran is not the point; the
        // leader and end offset below are what prove the replica that won is the one with the
        // shorter log.
        // @see tools/src/main/java/org/apache/kafka/tools/LeaderElectionCommand.java (Apache Kafka 4.3)
        arm.at('trigger the election')
        arm.scope.throwIfAborted()
        const electionOutput = triggerUncleanElection(topic).toString()
        arm.facts.electionOutput = electionOutput.trim()
        expect(electionOutput).toMatch(
          /(Successfully completed leader election|Valid replica already elected)/i
        )

        // Read through `cluster`, refreshed by hand on every attempt: any client that still has
        // node 2 cached as the leader would route this to a stopped container until its own
        // `metadataMaxAge` expires.
        arm.at('wait for the election to settle')
        let endOffsetAfterElection = null
        let leaderAfterElection = null
        await arm.wait('The election', METADATA_BUDGET, async () => {
          try {
            await arm.step('The metadata refresh', () => arm.cluster.refreshMetadata())
            leaderAfterElection = arm.leaderOf()
            if (leaderAfterElection !== 3) {
              return false
            }
            const [{ partitions }] = await arm.step('The offsets read', () =>
              arm.cluster.fetchTopicsOffset([
                { topic, fromBeginning: false, partitions: [{ partition: 0 }] },
              ])
            )
            endOffsetAfterElection = partitions[0].offset
            return true
          } catch (e) {
            return false
          }
        })
        arm.facts.leaderAfterElection = leaderAfterElection
        arm.facts.endOffsetAfterElection = endOffsetAfterElection
        phase('election settled', { leaderAfterElection, endOffsetAfterElection })

        // The stale replica won, and it won with the log it had: everything produced while it was
        // stopped is gone from the leader.
        expect(leaderAfterElection).toEqual(3)
        expect(endOffsetAfterElection).toEqual(String(firstBatch.length))

        arm.at('wait for the truncation')
        await arm.wait('The truncation', DELIVERY_BUDGET, () => arm.truncations.length > 0)
        phase('LOG_TRUNCATION observed', arm.truncations[0])

        // Before this change there is no validation and no event at all: the consumer's position is
        // simply out of range on the new leader and the partition is reset per `fromBeginning`, so
        // both the event and the resume-point assertions below fail.
        const [truncation] = arm.truncations
        expect(truncation.topic).toEqual(topic)
        expect(truncation.partition).toEqual(0)
        expect(truncation.offset).toEqual(String(consumedBeforeTruncation))
        expect(truncation.divergingOffset).toEqual(String(firstBatch.length))
        expect(typeof truncation.leaderEpoch).toBe('number')

        // It resumes at the diverging offset and keeps consuming: the records written after the
        // election reuse the offsets the truncation gave back.
        arm.at('rebuild the producer')
        await arm.rebuildProducer()
        arm.at('produce the third batch')
        const thirdBatch = await arm.produce('third', 5)
        phase('third batch produced')

        const resumed = () => arm.received.slice(truncation.receivedCount)
        arm.at('wait for the resumed batch')
        await arm.wait(
          'The resumed batch',
          DELIVERY_BUDGET,
          () => resumed().length >= thirdBatch.length
        )

        arm.at('assert')
        expect(
          resumed()
            .slice(0, thirdBatch.length)
            .map(({ offset }) => Number(offset))
        ).toEqual(Array.from({ length: thirdBatch.length }, (_, i) => firstBatch.length + i))
        expect(resumed().slice(0, thirdBatch.length).map(positionOf)).toEqual(
          expectedPositions(arm.acknowledged.third, firstBatch.length)
        )
        expect(arm.received.slice(0, truncation.receivedCount).map(positionOf)).toEqual([
          ...expectedPositions(arm.acknowledged.first, 0),
          ...expectedPositions(arm.acknowledged.second, firstBatch.length),
        ])
        expect(arm.crashes).toEqual([])

        console.log('unclean-election truncation observed', {
          topic,
          groupId: arm.groupId,
          initialLeader: arm.facts.initialLeader,
          electionOutput: arm.facts.electionOutput,
          positionBeforeTruncation: consumedBeforeTruncation,
          newLeaderEndOffset: endOffsetAfterElection,
          truncation,
        })
      })
    })

  it(
    'detects the truncation, emits LOG_TRUNCATION, resumes at the diverging offset and keeps consuming',
    truncationCase({ fromBeginning: true }),
    caseTimeout(TRUNCATION_BUDGET_MS)
  )

  it(
    'with the none policy a known divergence still resumes at the diverging offset and halts nothing',
    truncationCase({ offsetReset: 'none' }, ({ admin, topic, groupId }) =>
      admin.setOffsets({ groupId, topic, partitions: [{ partition: 0, offset: '0' }] })
    ),
    caseTimeout(TRUNCATION_BUDGET_MS)
  )
})
