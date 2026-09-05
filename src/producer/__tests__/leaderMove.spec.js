const { execFileSync } = require('child_process')
const { Kafka, logLevel } = require('../../../index')
const Cluster = require('../../cluster')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')

const enabled = process.env.LEADER_MOVE_E2E === '1'
const describeLive = enabled ? describe : describe.skip

const BOOTSTRAP_SERVER = 'kafka1:29092'
// Suffixed with this process's pid so two overlapping runs (there should never be one, but the
// file is otherwise never cleaned up) cannot clobber each other's reassignment plan.
const REASSIGNMENT_FILE = `/tmp/leader-move-reassignment-${process.pid}.json`
const KAFKA_BIN = '/opt/kafka/bin'

/** Runs a command inside the kafka1 container, piping `input` to its stdin when given. */
const execInKafka1 = (args, input) =>
  execFileSync('docker', ['compose', 'exec', '-T', 'kafka1', ...args], {
    stdio: 'pipe',
    input,
  })

/**
 * Reassigns a partition's replica set so `newLeaderId` is first (the preferred leader), without
 * moving any replica off or onto a broker that doesn't already host it - this is a *reordering*,
 * not a real data move, so `--verify` completes almost immediately.
 */
const reassignPreferredReplica = (topic, replicas, newLeaderId) => {
  const reorderedReplicas = [
    newLeaderId,
    ...replicas.filter((replicaId) => replicaId !== newLeaderId),
  ]
  const reassignmentJson = {
    version: 1,
    partitions: [{ topic, partition: 0, replicas: reorderedReplicas }],
  }

  execInKafka1(['sh', '-c', `cat > ${REASSIGNMENT_FILE}`], JSON.stringify(reassignmentJson))
  execInKafka1([
    `${KAFKA_BIN}/kafka-reassign-partitions.sh`,
    '--bootstrap-server',
    BOOTSTRAP_SERVER,
    '--reassignment-json-file',
    REASSIGNMENT_FILE,
    '--execute',
  ])
}

/** True once `kafka-reassign-partitions.sh --verify` reports the reassignment finished. */
const reassignmentIsComplete = () => {
  const output = execInKafka1([
    `${KAFKA_BIN}/kafka-reassign-partitions.sh`,
    '--bootstrap-server',
    BOOTSTRAP_SERVER,
    '--reassignment-json-file',
    REASSIGNMENT_FILE,
    '--verify',
  ]).toString()

  // Kafka 4.3.1's actual wording, confirmed against a live broker: "Reassignment of partition
  // <topic>-<partition> is completed." - note the trailing "d", which a bare `is complete\b`
  // word-boundary check misses entirely.
  return /reassignment of partition .* is completed/i.test(output)
}

const waitForReassignmentToComplete = async (deadlineMs = 60000) => {
  const deadline = Date.now() + deadlineMs
  while (Date.now() < deadline) {
    if (reassignmentIsComplete()) return
    await new Promise((resolve) => setTimeout(resolve, 500))
  }
  throw new Error('Partition reassignment did not complete in time')
}

const triggerPreferredElection = (topic) =>
  execInKafka1([
    `${KAFKA_BIN}/kafka-leader-election.sh`,
    '--bootstrap-server',
    BOOTSTRAP_SERVER,
    '--election-type',
    'PREFERRED',
    '--topic',
    topic,
    '--partition',
    '0',
  ])

/**
 * Live confirmation of the KIP-951 produce path (spec §3.3).
 *
 * A stopped broker can never answer with a hint - its socket is gone before it could respond, so
 * the client only ever recovers through the ordinary connection-error/refresh path, never through
 * `NOT_LEADER_OR_FOLLOWER`'s `CurrentLeader` field. The mechanism that actually produces a live
 * `NOT_LEADER_OR_FOLLOWER` response *with* a usable hint is a leadership move where the **old**
 * leader stays alive and keeps answering requests: a partition reassignment reorders the replica
 * list so a different broker is preferred, then `kafka-leader-election.sh --election-type
 * PREFERRED` hands leadership to it - the old leader is still up, still serving the client's
 * in-flight `acks: -1` produce requests, and correctly reports the new leader once it no longer
 * owns the partition.
 *
 * The producer's `metadataMaxAge` is set far longer than the test, so the only way this client
 * can learn of the leadership change is the hint itself: an ordinary background metadata refresh
 * racing in first would let the client route the very next request straight to the new leader,
 * never observing a `NOT_LEADER_OR_FOLLOWER` response at all.
 *
 * The fake-socket harness in `leaderHint.spec.js` is the primary evidence for the hint path; this
 * test only confirms it survives a real leadership change end to end.
 *
 * Reassigning/electing on a shared cluster is not safe alongside the other producer specs running
 * in parallel workers, so this suite is gated exactly like the rebootstrap e2e: skipped unless
 * `LEADER_MOVE_E2E=1`, and run in its own foreground `docker compose` invocation.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
 */
describeLive('Producer > leader move (live)', () => {
  let kafka
  let producer
  let admin
  let applyLeaderHintSpy
  let refreshMetadataSpy

  beforeAll(() => {
    applyLeaderHintSpy = jest.spyOn(Cluster.prototype, 'applyLeaderHint')
    refreshMetadataSpy = jest.spyOn(Cluster.prototype, 'refreshMetadata')
  })

  afterAll(async () => {
    applyLeaderHintSpy && applyLeaderHintSpy.mockRestore()
    refreshMetadataSpy && refreshMetadataSpy.mockRestore()
    producer && (await producer.disconnect().catch(() => {}))
    admin && (await admin.disconnect().catch(() => {}))
  })

  test('a leadership change is followed on the next attempt, using the hint the broker sent', async () => {
    const topic = `test-topic-${secureRandom()}`
    await createTopic({ topic, partitions: 1, replicas: 3 })

    // A separate client (and Cluster) for admin work, so reading metadata and driving the CLI
    // tools never touches the producer's own cached routing.
    const adminKafka = new Kafka({
      clientId: 'leader-move-e2e-admin',
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
    const newLeader = before.replicas.find((replicaId) => replicaId !== before.leader)

    kafka = new Kafka({
      clientId: 'leader-move-e2e',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      retry: { retries: 30, initialRetryTime: 300, maxRetryTime: 1000 },
    })

    producer = kafka.producer({
      // Far longer than this test: the client must learn of the new leader from the hint, not
      // from an ordinary background refresh racing in first. (`metadataMaxAge` is a
      // producer/consumer option, not a `Kafka` constructor one - it belongs here.)
      metadataMaxAge: 600000,
    })
    await producer.connect()

    const total = 120
    const intervalMs = 50
    const results = []
    // Snapshot of `refreshMetadataSpy`'s call count (all instances - only the producer's own
    // Cluster is active during this loop) taken immediately before each top-level `send()`, so
    // the refreshes any one send's own retry caused can be isolated afterwards.
    const refreshCountBeforeSend = []

    const produceLoop = (async () => {
      for (let i = 0; i < total; i++) {
        refreshCountBeforeSend.push(refreshMetadataSpy.mock.calls.length)
        results.push(await producer.send({ topic, acks: -1, messages: [{ value: `record-${i}` }] }))
        await new Promise((resolve) => setTimeout(resolve, intervalMs))
      }
    })()

    // Let a handful of sends land on the current (still preferred) leader before moving it.
    await new Promise((resolve) => setTimeout(resolve, intervalMs * 4))

    const electionTriggeredAt = Date.now()
    reassignPreferredReplica(topic, before.replicas, newLeader)
    await waitForReassignmentToComplete()
    triggerPreferredElection(topic)

    await produceLoop
    // Sentinel so `refreshCountBeforeSend[i + 1]` is defined for the very last iteration too.
    refreshCountBeforeSend.push(refreshMetadataSpy.mock.calls.length)

    expect(results).toHaveLength(total)

    const hintCallIndex = applyLeaderHintSpy.mock.calls.findIndex(
      ([hint]) =>
        hint.topic === topic && hint.leaderId === newLeader && hint.leaderEpoch > before.leaderEpoch
    )
    expect(hintCallIndex).toBeGreaterThanOrEqual(0)

    const [observedHint] = applyLeaderHintSpy.mock.calls[hintCallIndex]
    console.log('KIP-951 leader-move hint observed', {
      topic,
      previousLeaderId: before.leader,
      previousLeaderEpoch: before.leaderEpoch,
      hintedLeaderId: observedHint.leaderId,
      hintedLeaderEpoch: observedHint.leaderEpoch,
      msFromElectionTriggerToHint: Date.now() - electionTriggeredAt,
    })

    // How many refreshMetadata calls (any Cluster - only the producer's is active while the loop
    // runs) happened during the top-level send() at index `i`.
    const refreshesDuringSend = (i) => refreshCountBeforeSend[i + 1] - refreshCountBeforeSend[i]

    // Which send() picked up the hint: the first one, after the very first send's unrelated
    // initial metadata bootstrap (`Cluster#addMultipleTargetTopics` always refreshes once, the
    // first time a topic is added), whose own retry needed a refresh at all.
    const hintSendIndex = results.findIndex((_, i) => i > 0 && refreshesDuringSend(i) > 0)
    expect(hintSendIndex).toBeGreaterThan(0)

    // The hint's routing fix is immediately durable: the very next, independent send() resolves
    // without a metadata refresh of its own completing in between - proof the fix came from the
    // hint's synchronous cache write, not from something that needs re-discovering on every
    // subsequent attempt (which an ordinary refresh racing in to reveal the same leader would
    // also satisfy, making the hint irrelevant to how the client actually recovered).
    expect(refreshesDuringSend(hintSendIndex + 1)).toEqual(0)

    const after = await readLeader()
    expect(after.leader).toEqual(newLeader)

    await admin.disconnect()
    admin = null
  }, 180000)
})
