/**
 * Produce-latency/throughput probe: one message per send (no batching), the
 * case Nagle's algorithm can delay by coalescing small frames.
 * Usage: node scripts/benchmarks/produceLatency.js [--messages 20000] [--size 200] [--acks -1]
 *          [--linger <ms>] [--idempotent 0|1]
 * `--linger` and `--idempotent` are passed to the producer only when given, so the default
 * producer is measured unless a flag says otherwise; a tree that has no `linger` ignores it.
 * Prints p50/p90/p99 per-send latency in ms and messages/sec as one JSON line.
 */
const { Kafka, logLevel } = require('../../index')

/**
 * Reads the numeric flag `--<name>`; a value that is not a number exits 2.
 * @param {string} name
 * @param {number|undefined} fallback returned when the flag is absent
 * @returns {number|undefined}
 */
const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  if (i === -1) {
    return fallback
  }
  const value = Number(process.argv[i + 1])
  if (!Number.isFinite(value)) {
    console.error(`--${name} needs a number`)
    process.exit(2)
  }
  return value
}

const MESSAGES = arg('messages', 20000)
const SIZE = arg('size', 200)
const ACKS = arg('acks', -1)
const LINGER = arg('linger', undefined)
const IDEMPOTENT = arg('idempotent', undefined)
if (IDEMPOTENT !== undefined && IDEMPOTENT !== 0 && IDEMPOTENT !== 1) {
  console.error('--idempotent must be 0 or 1')
  process.exit(2)
}
const TOPIC = process.env.BENCH_TOPIC || 'bench-produce-latency'

/**
 * @param {number[]} sorted values in ascending order
 * @param {number} p fraction between 0 and 1
 * @returns {number} the value at that fraction of the sorted list
 */
const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]

/**
 * Creates the topic, connects the producer, times `MESSAGES` awaited sends one after another and
 * prints their latency percentiles and throughput as one JSON line.
 * @returns {Promise<void>}
 */
const main = async () => {
  const kafka = new Kafka({
    clientId: 'bench-produce',
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    logLevel: logLevel.NOTHING,
  })
  // Created up front, so no run, the first of a session included, pays for its auto-creation.
  const admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({ topics: [{ topic: TOPIC }], waitForLeaders: true })
  await admin.disconnect()

  const producer = kafka.producer({
    ...(LINGER === undefined ? {} : { linger: LINGER }),
    ...(IDEMPOTENT === undefined ? {} : { idempotent: IDEMPOTENT === 1 }),
  })
  await producer.connect()

  const value = Buffer.alloc(SIZE, 'x')
  const latencies = new Array(MESSAGES)

  const startedAll = process.hrtime.bigint()
  for (let i = 0; i < MESSAGES; i++) {
    const started = process.hrtime.bigint()
    // one message per send: this is the case Nagle can delay
    await producer.send({ topic: TOPIC, acks: ACKS, messages: [{ value }] })
    latencies[i] = Number(process.hrtime.bigint() - started) / 1e6
  }
  const elapsedMs = Number(process.hrtime.bigint() - startedAll) / 1e6

  latencies.sort((a, b) => a - b)
  console.log(
    JSON.stringify({
      messages: MESSAGES,
      size: SIZE,
      acks: ACKS,
      linger: LINGER ?? null,
      idempotent: IDEMPOTENT ?? null,
      p50: percentile(latencies, 0.5),
      p90: percentile(latencies, 0.9),
      p99: percentile(latencies, 0.99),
      messagesPerSecond: (MESSAGES / elapsedMs) * 1000,
    })
  )

  await producer.disconnect()
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
