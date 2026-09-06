const { Kafka, logLevel } = require('../../../index')
const Cluster = require('../../cluster')
const {
  secureRandom,
  plainTextBrokers,
  createCluster,
  createTopic,
  generateMessages,
  waitForConsumerToJoinGroup,
  reassignPreferredReplica,
  waitForReassignmentToComplete,
  triggerPreferredElection,
  triggerUncleanElection,
  stopBroker,
  startBroker,
} = require('testHelpers')

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
const DISCONNECT_BUDGET_MS = 15000 // one dead-broker request timeout plus slack

/**
 * Awaits `promise` but gives up after `ms`, so a teardown can never outlive its hook budget: a
 * client with in-flight requests to a stopped container only fails them after its own
 * `requestTimeout`, and several such disconnects in a row can exceed Jest's hook timeout.
 * @param {Promise<unknown>} promise
 * @param {number} ms
 * @returns {Promise<void>}
 */
const withTimeout = async (promise, ms) => {
  await Promise.race([promise, sleep(ms)])
}

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
    applyLeaderHintSpy && applyLeaderHintSpy.mockRestore()
    applyLeaderHintSpy = consumer = producer = admin = kafka = adminKafka = null
  })

  it('keeps consuming through a leader move, takes the hint path, and revalidates', async () => {
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
      hint.topic === topic && hint.leaderId === newLeaderId && hint.leaderEpoch > before.leaderEpoch
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
    expect(received.map(({ offset }) => offset)).toEqual(Array.from({ length: total }, (_, i) => i))

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
  }, 240000)
})

// `replica.lag.time.max.ms` is unset in `docker-compose.yml`, so the broker default applies: a
// stopped replica stays in the ISR until it has been silent for this long.
// @see https://kafka.apache.org/43/documentation.html#brokerconfigs_replica.lag.time.max.ms
const REPLICA_LAG_TIME_MAX_MS = 30000

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
  let kafka
  let cluster
  let admin
  let producer
  let consumer

  // The restarts come first and each is isolated: at this point kafka2 is stopped, so every
  // client below has in-flight requests to a dead container that only fail after their own
  // request timeout - putting the restarts last would let a hook timeout skip them and leave the
  // shared cluster degraded for the next spec. The disconnects that follow are individually
  // bounded for the same reason.
  afterEach(async () => {
    for (const service of ['kafka2', 'kafka3']) {
      try {
        startBroker(service)
      } catch (e) {
        // Already running, or compose is down - neither is a reason to skip the other service.
      }
    }

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
    cluster &&
      (await withTimeout(
        cluster.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      ))
    consumer = producer = admin = cluster = kafka = null
  }, 120000)

  it('detects the truncation, emits LOG_TRUNCATION, resumes at the diverging offset and keeps consuming', async () => {
    const topic = `test-topic-${secureRandom()}`
    const phase = phaseLogger(Date.now())

    kafka = new Kafka({
      clientId: 'consumer-unclean-election',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      retry: { retries: 30, initialRetryTime: 300, maxRetryTime: 1000 },
    })

    admin = kafka.admin()
    await admin.connect()

    cluster = createCluster()
    await cluster.connect()

    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic,
          replicaAssignment: [{ partition: 0, replicas: [2, 3] }],
          configEntries: [{ name: 'unclean.leader.election.enable', value: 'true' }],
        },
      ],
    })

    // Read rather than assumed: either replica may lead the fresh assignment, and both orders
    // reach the same end state (kafka3 is stopped first, so leadership ends up on kafka2 either
    // way). Logged so a later assertion failure is diagnosable.
    // `cluster` is the client this test drives metadata through by hand, because it is the only
    // one whose cache it can refresh on demand: `admin`'s own cache is governed by a 5-minute
    // `metadataMaxAge`, so after the election it would keep routing to the stopped node 2.
    await cluster.addTargetTopic(topic)
    await cluster.refreshMetadata()
    const leaderOf = () => Number(Object.keys(cluster.findLeaderForPartitions(topic, [0]))[0])

    const initialLeader = leaderOf()
    expect([2, 3]).toContain(initialLeader)
    phase('topic created', { initialLeader })

    // Pin the group coordinator to node 1, the surviving voter, so the group is not stranded
    // during the window where the other two nodes are being stopped and started.
    let groupId = null
    for (let attempt = 0; attempt < 60 && groupId == null; attempt++) {
      const candidate = `test-group-${secureRandom()}`
      const coordinator = await cluster.findGroupCoordinator({ groupId: candidate })
      if (coordinator.nodeId === 1) {
        groupId = candidate
      }
    }
    expect(groupId).not.toBeNull()

    producer = kafka.producer()
    await producer.connect()

    const firstBatch = generateMessages({ number: 10 })
    await producer.send({ topic, messages: firstBatch })

    consumer = kafka.consumer({
      groupId,
      // Only affects how quickly the consumer notices the new leader after its old one goes away;
      // the truncation itself is detected by validation, not by the refresh.
      metadataMaxAge: 5000,
    })

    const received = []
    const truncations = []
    const crashes = []
    consumer.on(consumer.events.LOG_TRUNCATION, (event) =>
      truncations.push({ ...event.payload, receivedCount: received.length })
    )
    consumer.on(consumer.events.CRASH, (event) => crashes.push(event.payload))

    await consumer.subscribe({ topic, fromBeginning: true })
    consumer
      .run({ eachMessage: async ({ message }) => received.push(Number(message.offset)) })
      .catch((e) => crashes.push(e))
    await waitForConsumerToJoinGroup(consumer)

    const gotFirstBatch = await waitForCondition(() => received.length >= firstBatch.length)
    expect(gotFirstBatch).toBe(true)
    phase('first batch consumed')

    // kafka3 goes away and is waited out of the ISR, so what follows exists only on kafka2.
    stopBroker('kafka3')
    await sleep(REPLICA_LAG_TIME_MAX_MS + 5000)
    phase('kafka3 stopped and waited out of the ISR')

    const secondBatch = generateMessages({ number: 10 })
    await producer.send({ topic, messages: secondBatch })

    const consumedBeforeTruncation = firstBatch.length + secondBatch.length
    const gotSecondBatch = await waitForCondition(
      () => received.length >= consumedBeforeTruncation,
      DELIVERY_BUDGET
    )
    expect(gotSecondBatch).toBe(true)
    // The runner resolves an offset after `eachMessage` returns, so give it a moment: the
    // position the truncation is asserted against is the resolved one, not the delivered one.
    await sleep(2000)
    phase('second batch consumed', { received: received.length })

    // The only replica holding the second batch is stopped, and the stale one comes back.
    stopBroker('kafka2')
    startBroker('kafka3')

    const reachable = await waitForCondition(async () => {
      try {
        await cluster.findBroker({ nodeId: 3 })
        return true
      } catch (e) {
        return false
      }
    }, BROKER_RESTART_BUDGET)
    expect(reachable).toBe(true)
    phase('kafka2 stopped, kafka3 back and reachable')

    // `kafka-leader-election.sh` exits 0 in more than one situation, so the exit status alone
    // proves nothing - the printed outcome is what says which one happened. Two of them are
    // expected here: the command performed the election ("Successfully completed leader
    // election"), or the controller had already performed it ("Valid replica already elected"),
    // which is the common case because the topic carries `unclean.leader.election.enable=true`
    // and the controller may unclean-elect the returning replica the moment it registers, before
    // this line runs. A failed election prints "Error completing leader election" and throws.
    // Which of the two ran is not the point; the leader and end offset below are what prove the
    // replica that won is the one with the shorter log.
    // @see tools/src/main/java/org/apache/kafka/tools/LeaderElectionCommand.java (Apache Kafka 4.3)
    const electionOutput = triggerUncleanElection(topic).toString()
    expect(electionOutput).toMatch(
      /(Successfully completed leader election|Valid replica already elected)/i
    )

    // Read through `cluster`, refreshed by hand on every attempt: any client that still has node
    // 2 cached as the leader would route this to a stopped container until its own
    // `metadataMaxAge` expires.
    let endOffsetAfterElection = null
    let leaderAfterElection = null
    const gotEndOffset = await waitForCondition(async () => {
      try {
        await cluster.refreshMetadata()
        leaderAfterElection = leaderOf()
        if (leaderAfterElection !== 3) {
          return false
        }
        const [{ partitions }] = await cluster.fetchTopicsOffset([
          { topic, fromBeginning: false, partitions: [{ partition: 0 }] },
        ])
        endOffsetAfterElection = partitions[0].offset
        return true
      } catch (e) {
        return false
      }
    }, METADATA_BUDGET)
    expect(gotEndOffset).toBe(true)
    phase('election settled', { leaderAfterElection, endOffsetAfterElection })

    // The stale replica won, and it won with the log it had: everything produced while it was
    // stopped is gone from the leader.
    expect(leaderAfterElection).toEqual(3)
    expect(endOffsetAfterElection).toEqual(String(firstBatch.length))

    const gotTruncation = await waitForCondition(() => truncations.length > 0, DELIVERY_BUDGET)
    expect(gotTruncation).toBe(true)
    phase('LOG_TRUNCATION observed', truncations[0])

    // Before this change there is no validation and no event at all: the consumer's position is
    // simply out of range on the new leader and the partition is reset per `fromBeginning`, so
    // both the event and the resume-point assertions below fail.
    const [truncation] = truncations
    expect(truncation.topic).toEqual(topic)
    expect(truncation.partition).toEqual(0)
    expect(truncation.offset).toEqual(String(consumedBeforeTruncation))
    expect(truncation.divergingOffset).toEqual(String(firstBatch.length))
    expect(typeof truncation.leaderEpoch).toBe('number')

    // It resumes at the diverging offset and keeps consuming: the records written after the
    // election reuse the offsets the truncation gave back. The producer is rebuilt first - the
    // old one still has the stopped node 2 cached as the leader for the whole of its own
    // `metadataMaxAge`, which is longer than this test.
    await withTimeout(
      producer.disconnect().catch(() => {}),
      DISCONNECT_BUDGET_MS
    )
    producer = kafka.producer()
    await producer.connect()

    const thirdBatch = generateMessages({ number: 5 })
    await producer.send({ topic, messages: thirdBatch })
    phase('third batch produced')

    const resumed = () => received.slice(truncation.receivedCount)
    const gotThirdBatch = await waitForCondition(
      () => resumed().length >= thirdBatch.length,
      DELIVERY_BUDGET
    )
    expect(gotThirdBatch).toBe(true)

    expect(resumed().slice(0, thirdBatch.length)).toEqual(
      Array.from({ length: thirdBatch.length }, (_, i) => firstBatch.length + i)
    )
    expect(crashes).toEqual([])

    console.log('unclean-election truncation observed', {
      topic,
      groupId,
      initialLeader,
      electionOutput: electionOutput.trim(),
      positionBeforeTruncation: consumedBeforeTruncation,
      newLeaderEndOffset: endOffsetAfterElection,
      truncation,
    })
  }, 420000)
})
