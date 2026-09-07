/**
 * Post-rebalance latency probe: measures how long a member that keeps a partition across a
 * rebalance waits for its next batch on that partition.
 *
 * Usage: node scripts/benchmarks/rebalanceLatency.js [--partitions 6] [--runs 7]
 *          [--timeout <ms>] [--protocol eager]
 * Prints one JSON line:
 *   { protocol, partitions, members, runs, groupInitialRebalanceDelayMs, samplesMs, medianMs }
 *
 * `--protocol` is a LABEL ONLY in this task: every run uses the default eager assigner. Task 15
 * threads `partitionAssigners` through so the cooperative arm measures what it says.
 *
 * Only a partition held BOTH before and after the round is measured. A partition that moves
 * measures reassignment, and a first-ever assignment measures group formation; neither is the
 * cost this phase changes. The clock starts when the second member's consumer joins the group
 * and stops on the first batch the first member receives on a kept partition afterwards.
 *
 * A producer feeds the topic continuously, so "no batch" always means "the fetch path was not
 * running", never "there was nothing to send".
 *
 * Seven runs, first discarded, median reported - the interleaving and one-cluster-user rules are
 * the caller's. A run that never sees a kept-partition batch fails on the deadline rather than
 * hanging.
 */
const { Kafka, logLevel } = require('../../index')

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : Number(process.argv[i + 1])
}

const stringArg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : process.argv[i + 1]
}

const PARTITIONS = arg('partitions', 6)
const RUNS = arg('runs', 7)
const PROTOCOL = stringArg('protocol', 'eager')
const TIMEOUT_MS = arg('timeout', 60000)
const MEMBERS = 2
const PRODUCE_INTERVAL_MS = 50

// docker-compose.yml pins KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS to '0'; Kafka's own default is
// 3000, which would dominate every sample. Recorded so a number measured elsewhere is comparable.
// @see https://kafka.apache.org/documentation/#brokerconfigs_group.initial.rebalance.delay.ms
const GROUP_INITIAL_REBALANCE_DELAY_MS = arg('initial-rebalance-delay', 0)

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * Creates the run's topic and waits for its leaders, so the first fetch is not a metadata retry.
 * @param {import('../../index').Kafka} kafka
 * @param {string} topic
 * @returns {Promise<void>}
 */
const createTopic = async (kafka, topic) => {
  const admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({
    topics: [{ topic, numPartitions: PARTITIONS, replicationFactor: 1 }],
    waitForLeaders: true,
  })
  await admin.disconnect()
}

/**
 * Deletes the run's topic; a failed deletion is reported without masking the run's own error.
 * @param {import('../../index').Kafka} kafka
 * @param {string} topic
 * @returns {Promise<void>}
 */
const deleteTopic = async (kafka, topic) => {
  const admin = kafka.admin()
  try {
    await admin.connect()
    await admin.deleteTopics({ topics: [topic] })
    await admin.disconnect()
  } catch (e) {
    console.error(`could not delete ${topic}: ${e.message}`)
  }
}

/**
 * Feeds the topic until stopped, so a missing batch is never "nothing to read".
 * @param {import('../../index').Producer} producer
 * @param {string} topic
 * @returns {{ stop: () => void }}
 */
const startFeeding = (producer, topic) => {
  let running = true
  const loop = async () => {
    // eslint-disable-next-line no-unmodified-loop-condition -- flipped by stop(), called from outside this loop
    while (running) {
      await producer
        .send({
          topic,
          messages: Array.from({ length: PARTITIONS }, (_, partition) => ({
            partition,
            value: Buffer.alloc(64, 1),
          })),
        })
        .catch(() => {})
      await sleep(PRODUCE_INTERVAL_MS)
    }
  }
  loop()
  return { stop: () => (running = false) }
}

/**
 * One measurement. Starts member 1 alone, records what it holds, starts member 2, and returns
 * the milliseconds between member 2's GROUP_JOIN and member 1's next batch on a kept partition.
 * @param {import('../../index').Kafka} kafka
 * @param {string} topic
 * @param {number} run
 * @returns {Promise<number>}
 */
const measureOnce = async (kafka, topic, run) => {
  const groupId = `bench-rebalance-latency-${Date.now()}-${run}`
  const consumers = Array.from({ length: MEMBERS }, () => kafka.consumer({ groupId }))
  const [first, second] = consumers

  let heldBefore = new Set()
  let joinedAgainAt
  let firstKeptBatchAt
  let assignmentsSeen = 0

  first.on(first.events.GROUP_JOIN, ({ payload }) => {
    assignmentsSeen += 1
    if (assignmentsSeen === 1) {
      heldBefore = new Set(payload.memberAssignment[topic] || [])
    }
  })

  // The clock starts on member 2's own GROUP_JOIN, not on its connect: TCP, ApiVersions,
  // metadata and FindCoordinator are noisy and are not what this phase changes.
  let joinedAgainResolve
  const secondJoined = new Promise((resolve) => (joinedAgainResolve = resolve))
  second.on(second.events.GROUP_JOIN, () => {
    if (joinedAgainAt === undefined) {
      joinedAgainAt = process.hrtime.bigint()
      joinedAgainResolve()
    }
  })

  await first.connect()
  await first.subscribe({ topic, fromBeginning: false })
  await first.run({
    eachBatch: async ({ batch }) => {
      const kept = assignmentsSeen > 1 && heldBefore.has(batch.partition)
      if (kept && joinedAgainAt !== undefined && firstKeptBatchAt === undefined) {
        firstKeptBatchAt = process.hrtime.bigint()
      }
    },
  })

  // eslint-disable-next-line no-unmodified-loop-condition -- set by the GROUP_JOIN listener above
  while (assignmentsSeen === 0) {
    await sleep(50)
  }
  // Let the single member settle into a steady fetch loop before the round under measurement.
  await sleep(2000)

  await second.connect()
  await second.subscribe({ topic, fromBeginning: false })
  await second.run({ eachBatch: async () => {} })
  await secondJoined

  const deadline = Date.now() + TIMEOUT_MS
  // eslint-disable-next-line no-unmodified-loop-condition -- set by the eachBatch callback above
  while (firstKeptBatchAt === undefined) {
    if (Date.now() > deadline) {
      throw new Error(`run ${run} saw no batch on a kept partition within ${TIMEOUT_MS} ms`)
    }
    await sleep(25)
  }

  const elapsedMs = Number(firstKeptBatchAt - joinedAgainAt) / 1e6

  for (const consumer of consumers) {
    await consumer.stop()
    await consumer.disconnect()
  }

  return elapsedMs
}

;(async () => {
  const kafka = new Kafka({
    clientId: 'bench-rebalance-latency',
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    logLevel: logLevel.NOTHING,
  })

  const topic = `bench-rebalance-latency-${Date.now()}`
  await createTopic(kafka, topic)

  const producer = kafka.producer()
  await producer.connect()
  const feed = startFeeding(producer, topic)

  try {
    const samplesMs = []
    for (let run = 0; run < RUNS + 1; run++) {
      const elapsedMs = await measureOnce(kafka, topic, run)
      // The first run pays for JIT warm-up and the very first group formation on this topic.
      if (run > 0) {
        samplesMs.push(elapsedMs)
      }
    }

    console.log(
      JSON.stringify({
        protocol: PROTOCOL,
        partitions: PARTITIONS,
        members: MEMBERS,
        runs: RUNS,
        groupInitialRebalanceDelayMs: GROUP_INITIAL_REBALANCE_DELAY_MS,
        samplesMs,
        medianMs: median(samplesMs),
      })
    )
  } finally {
    feed.stop()
    await producer.disconnect()
    await deleteTopic(kafka, topic)
  }
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
