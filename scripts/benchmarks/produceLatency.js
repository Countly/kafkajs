/**
 * Produce-latency/throughput probe: one message per send (no batching), the
 * case Nagle's algorithm can delay by coalescing small frames.
 * Usage: node scripts/benchmarks/produceLatency.js [--messages 20000] [--size 200] [--acks -1]
 * Prints p50/p90/p99 per-send latency in ms and messages/sec as one JSON line.
 */
const { Kafka, logLevel } = require('../../index')

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : Number(process.argv[i + 1])
}

const MESSAGES = arg('messages', 20000)
const SIZE = arg('size', 200)
const ACKS = arg('acks', -1)
const TOPIC = process.env.BENCH_TOPIC || 'bench-produce-latency'

const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]

;(async () => {
  const kafka = new Kafka({
    clientId: 'bench-produce',
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    logLevel: logLevel.NOTHING,
  })
  const producer = kafka.producer()
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
      p50: percentile(latencies, 0.5),
      p90: percentile(latencies, 0.9),
      p99: percentile(latencies, 0.99),
      messagesPerSecond: (MESSAGES / elapsedMs) * 1000,
    })
  )

  await producer.disconnect()
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
