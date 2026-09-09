/**
 * Retained-heap detector for the cooperative rebalance path.
 *
 * Drives N cooperative rounds against a live cluster and samples the heap five times, each after
 * two forced garbage collections, so what is measured is what is RETAINED rather than what has
 * merely not been collected yet. The incremental apply keeps per-partition state across rounds by
 * design; this is what proves it does not keep it forever.
 *
 * Usage: node --expose-gc scripts/benchmarks/rebalanceStorm.js [--rounds 200] [--partitions 6]
 *          [--group-protocol classic|consumer]
 * Prints one JSON line on stdout:
 *   { groupProtocol, rounds, partitions, samples: [{ round, heapUsedBytes }], deltaBytes,
 *     slopeBytesPerRound, ceilingBytes, withinCeiling, slopeGateBytesPerRound, withinSlopeGate }
 * and one progress line per 20 rounds on stderr, so a 200-round run is never silent for the whole
 * of its wall time while the single result line on stdout stays parseable.
 *
 * `--group-protocol` selects the membership protocol: `classic` keeps the cooperative-sticky
 * assigner the storm was written for, `consumer` is the KIP-848 ConsumerGroupHeartbeat
 * reconciliation, where the coordinator assigns.
 *
 * Gate: the LAST-THIRD slope must be at most 32 KB per round. The total delta is recorded against
 * an 8 MB ceiling but is not the gate: a one-off allocation early in the run is not a leak, and a
 * steady climb is.
 */
const { Kafka, logLevel, PartitionAssigners } = require('../../index')

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : Number(process.argv[i + 1])
}

const stringArg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : process.argv[i + 1]
}

const ROUNDS = arg('rounds', 200)
const PARTITIONS = arg('partitions', 6)
const GROUP_PROTOCOL = stringArg('group-protocol', 'classic')
const SAMPLES = 5
const CEILING_BYTES = 8 * 1024 * 1024
const SLOPE_GATE_BYTES_PER_ROUND = 32 * 1024
const ROUND_TIMEOUT_MS = 60000
const PROGRESS_EVERY_ROUNDS = 20

if (typeof global.gc !== 'function') {
  console.error('run with: node --expose-gc scripts/benchmarks/rebalanceStorm.js')
  process.exit(1)
}

if (GROUP_PROTOCOL !== 'classic' && GROUP_PROTOCOL !== 'consumer') {
  console.error(`--group-protocol must be classic or consumer, got ${GROUP_PROTOCOL}`)
  process.exit(1)
}

/**
 * The classic arm's membership options: the storm is a cooperative scenario by construction.
 * @returns {object} consumer options
 */
const classicMembershipOptions = () => ({
  groupProtocol: 'classic',
  partitionAssigners: [PartitionAssigners.cooperativeSticky],
})

// The assigner choice only means anything under the classic protocol: a KIP-848 group is assigned
// by the coordinator, and passing `partitionAssigners` alongside it throws at construction.
// @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
const membershipOptions =
  GROUP_PROTOCOL === 'consumer' ? { groupProtocol: 'consumer' } : classicMembershipOptions()

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
 * One cooperative round: a second member joins, the group rebalances, it leaves, the group
 * rebalances back. Both halves are awaited on the first member's GROUP_JOIN, so a "round" is a
 * completed rebalance and not a sleep.
 * @param {import('../../index').Kafka} kafka
 * @param {string} groupId
 * @param {string} topic
 * @param {() => Promise<void>} nextJoin resolves on the first member's next GROUP_JOIN
 * @returns {Promise<void>}
 */
const oneRound = async (kafka, groupId, topic, nextJoin) => {
  const second = kafka.consumer({ groupId, ...membershipOptions })
  await second.connect()
  await second.subscribe({ topic, fromBeginning: false })
  const joined = nextJoin()
  await second.run({ eachBatch: async () => {} })
  await joined

  const left = nextJoin()
  await second.stop()
  await second.disconnect()
  await left
}

/**
 * Heap retained after two forced collections. Two, not one: the first can leave objects the
 * second's mark phase then finds unreachable.
 * @returns {number}
 */
const sampleHeap = () => {
  global.gc()
  global.gc()
  return process.memoryUsage().heapUsed
}

/**
 * Ordinary least-squares slope over the last three samples, in bytes per round. Three, not "the
 * last third of five", which would be a line through two points rather than a fit.
 * @param {Array<[number, number]>} points
 * @returns {number}
 */
const slope = (points) => {
  const n = points.length
  const meanX = points.reduce((sum, [x]) => sum + x, 0) / n
  const meanY = points.reduce((sum, [, y]) => sum + y, 0) / n
  const num = points.reduce((sum, [x, y]) => sum + (x - meanX) * (y - meanY), 0)
  const den = points.reduce((sum, [x]) => sum + (x - meanX) ** 2, 0)
  return den === 0 ? 0 : num / den
}

;(async () => {
  const kafka = new Kafka({
    clientId: 'bench-rebalance-storm',
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    logLevel: logLevel.NOTHING,
  })

  const topic = `bench-rebalance-storm-${Date.now()}`
  const groupId = `bench-rebalance-storm-${Date.now()}`
  await createTopic(kafka, topic)

  const first = kafka.consumer({ groupId, ...membershipOptions })

  // The first member's GROUP_JOIN is the round's clock: each half of a round waits for the next
  // one, so a round ends when the group has actually rebalanced.
  let onJoin = () => {}
  let crash = null
  first.on(first.events.GROUP_JOIN, () => onJoin())
  first.on(first.events.CRASH, ({ payload }) => {
    crash = payload.error
    onJoin()
  })

  /**
   * Resolves on the first member's next GROUP_JOIN and rejects if that member crashes or the round
   * stalls, so a broken rebalance costs one error line rather than a hung cluster slot.
   * @returns {Promise<void>}
   */
  const nextJoin = () =>
    new Promise((resolve, reject) => {
      if (crash) {
        reject(crash)
        return
      }

      const timer = setTimeout(
        () => reject(new Error(`no rebalance within ${ROUND_TIMEOUT_MS} ms`)),
        ROUND_TIMEOUT_MS
      )

      onJoin = () => {
        onJoin = () => {}
        clearTimeout(timer)
        return crash ? reject(crash) : resolve()
      }
    })

  const samples = []
  const sampleAt = new Set(
    Array.from({ length: SAMPLES - 1 }, (_, i) => Math.round((ROUNDS * (i + 1)) / (SAMPLES - 1)))
  )

  try {
    const joined = nextJoin()
    await first.connect()
    await first.subscribe({ topic, fromBeginning: false })
    await first.run({ eachBatch: async () => {} })
    await joined

    samples.push({ round: 0, heapUsedBytes: sampleHeap() })

    for (let round = 1; round <= ROUNDS; round++) {
      await oneRound(kafka, groupId, topic, nextJoin)
      if (sampleAt.has(round)) {
        samples.push({ round, heapUsedBytes: sampleHeap() })
      }
      if (round % PROGRESS_EVERY_ROUNDS === 0) {
        console.error(
          JSON.stringify({
            progress: { round, rounds: ROUNDS, heapUsedBytes: process.memoryUsage().heapUsed },
          })
        )
      }
    }

    const deltaBytes = samples[samples.length - 1].heapUsedBytes - samples[0].heapUsedBytes
    const slopeBytesPerRound = slope(
      samples.slice(-3).map(({ round, heapUsedBytes }) => [round, heapUsedBytes])
    )
    const withinSlopeGate = slopeBytesPerRound <= SLOPE_GATE_BYTES_PER_ROUND

    console.log(
      JSON.stringify({
        groupProtocol: GROUP_PROTOCOL,
        rounds: ROUNDS,
        partitions: PARTITIONS,
        samples,
        deltaBytes,
        slopeBytesPerRound,
        ceilingBytes: CEILING_BYTES,
        withinCeiling: deltaBytes <= CEILING_BYTES,
        slopeGateBytesPerRound: SLOPE_GATE_BYTES_PER_ROUND,
        withinSlopeGate,
      })
    )

    process.exitCode = withinSlopeGate ? 0 : 1
  } finally {
    // A crashed member can fail its own shutdown; that must not mask the round's error.
    await first.stop().catch(() => {})
    await first.disconnect().catch(() => {})
    await deleteTopic(kafka, topic)
  }
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
