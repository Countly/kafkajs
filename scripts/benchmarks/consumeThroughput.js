/**
 * Consume-throughput probe: produces a fixed corpus, consumes it, and reports messages/sec plus
 * p50/p90/p99 handling latency.
 * Usage: node scripts/benchmarks/consumeThroughput.js [--messages 200000] [--size 200]
 *          [--partitions 6] [--timeout <ms>] [--handler eachBatch|eachMessage]
 *          [--subscribe-channels 0|1]
 * Prints one JSON line: { messages, size, partitions, handler, subscribeChannels,
 *   messagesPerSecond, p50, p90, p99 }
 *
 * The corpus is fixed on purpose: 200000 messages of 200 B across one topic of 6 partitions, no
 * compression, `autoCommit: false`, one discarded warm-up run, and a fresh consumer group per run.
 *
 * Two arms, selected by `--handler`. `eachBatch` (the default, so earlier numbers stay comparable)
 * uses `eachBatchAutoResolve` and times each batch hand-off; `eachMessage` times each record and so
 * exercises the per-record path, which the batch arm never touches.
 *
 * A run that never receives the whole corpus fails on the deadline instead of hanging, so a lost
 * partition or a wrong `--messages` costs one error line rather than a cluster slot.
 *
 * `--subscribe-channels 1` (this tree only) subscribes a function that does nothing to the
 * diagnostics channel of every public event, so the run measures what subscribing costs.
 */
const { Kafka, logLevel } = require('../../index')
const subscribeChannels = require('./subscribeChannels')

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : Number(process.argv[i + 1])
}

const stringArg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : process.argv[i + 1]
}

const HANDLER = stringArg('handler', 'eachBatch')
const SUBSCRIBE_CHANNELS = arg('subscribe-channels', 0)
const MESSAGES = arg('messages', 200000)
const SIZE = arg('size', 200)
const PARTITIONS = arg('partitions', 6)
const PRODUCE_BATCH = 1000
const MS_PER_MESSAGE_BUDGET = 2
const TIMEOUT_MS = arg('timeout', Math.max(60000, MESSAGES * MS_PER_MESSAGE_BUDGET))
const GENERATED_TOPIC = `bench-consume-throughput-${Date.now()}`
const TOPIC = process.env.BENCH_TOPIC || GENERATED_TOPIC

if (HANDLER !== 'eachBatch' && HANDLER !== 'eachMessage') {
  console.error(`--handler must be eachBatch or eachMessage, got ${HANDLER}`)
  process.exit(1)
}
if (SUBSCRIBE_CHANNELS !== 0 && SUBSCRIBE_CHANNELS !== 1) {
  console.error(`--subscribe-channels must be 0 or 1, got ${SUBSCRIBE_CHANNELS}`)
  process.exit(1)
}

const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]

/**
 * Creates the benchmark topic and waits for its leaders, so the first fetch is not a metadata retry.
 * @param {import('../../index').Kafka} kafka the client to administer with
 * @returns {Promise<void>}
 */
const createTopic = async (kafka) => {
  const admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: PARTITIONS, replicationFactor: 1 }],
    waitForLeaders: true,
  })
  await admin.disconnect()
}

/**
 * Deletes the generated benchmark topic so repeated runs do not leave topics behind. A topic named
 * by `BENCH_TOPIC` is left alone, and a failed deletion is reported without masking the run's own
 * error.
 * @param {import('../../index').Kafka} kafka the client to administer with
 * @returns {Promise<void>}
 */
const deleteTopic = async (kafka) => {
  if (TOPIC !== GENERATED_TOPIC) {
    return
  }

  const admin = kafka.admin()
  try {
    await admin.connect()
    await admin.deleteTopics({ topics: [TOPIC] })
    await admin.disconnect()
  } catch (e) {
    console.error(`could not delete ${TOPIC}: ${e.message}`)
  }
}

/**
 * Produces the fixed corpus: `MESSAGES` messages of `SIZE` bytes, uncompressed, round-robined over
 * the topic's partitions by the default partitioner.
 * @param {import('../../index').Kafka} kafka the client to produce with
 * @returns {Promise<void>}
 */
const produceCorpus = async (kafka) => {
  const producer = kafka.producer()
  await producer.connect()

  const value = Buffer.alloc(SIZE, 'x')
  for (let sent = 0; sent < MESSAGES; sent += PRODUCE_BATCH) {
    const count = Math.min(PRODUCE_BATCH, MESSAGES - sent)
    await producer.send({
      topic: TOPIC,
      messages: Array.from({ length: count }, () => ({ value })),
    })
  }

  await producer.disconnect()
}

/**
 * Consumes the whole corpus once with a fresh group, timing every handler call: per batch under
 * `--handler eachBatch`, per record under `--handler eachMessage`.
 * @param {import('../../index').Kafka} kafka the client to consume with
 * @param {number} run run number, used to keep each run on its own consumer group
 * @returns {Promise<{ messagesPerSecond: number, p50: number, p90: number, p99: number }>}
 *   throughput over the consume phase and handling latency in ms
 */
const consumeOnce = async (kafka, run) => {
  const consumer = kafka.consumer({ groupId: `${TOPIC}-run-${run}` })
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })

  const latencies = []
  let consumed = 0
  let bytes = 0
  let startedAt
  let resolveWhenDrained
  const drained = new Promise((resolve) => {
    resolveWhenDrained = resolve
  })

  let deadlineTimer
  const deadline = new Promise((_, reject) => {
    deadlineTimer = setTimeout(
      () =>
        reject(
          new Error(
            `run ${run} timed out after ${TIMEOUT_MS} ms with ${consumed} of ${MESSAGES} messages consumed`
          )
        ),
      TIMEOUT_MS
    )
  })

  const eachBatchArm = {
    eachBatchAutoResolve: true,
    autoCommit: false,
    eachBatch: async ({ batch }) => {
      const batchStartedAt = process.hrtime.bigint()
      if (startedAt === undefined) {
        startedAt = batchStartedAt
      }

      for (const message of batch.messages) {
        bytes += message.value.length
      }
      consumed += batch.messages.length

      latencies.push(Number(process.hrtime.bigint() - batchStartedAt) / 1e6)

      if (consumed >= MESSAGES) {
        resolveWhenDrained()
      }
    },
  }

  const eachMessageArm = {
    autoCommit: false,
    eachMessage: async ({ message }) => {
      const messageStartedAt = process.hrtime.bigint()
      if (startedAt === undefined) {
        startedAt = messageStartedAt
      }

      bytes += message.value.length
      consumed += 1

      latencies.push(Number(process.hrtime.bigint() - messageStartedAt) / 1e6)

      if (consumed >= MESSAGES) {
        resolveWhenDrained()
      }
    },
  }

  await consumer.run(HANDLER === 'eachMessage' ? eachMessageArm : eachBatchArm)

  try {
    await Promise.race([drained, deadline])
  } finally {
    clearTimeout(deadlineTimer)
    await consumer.stop()
    await consumer.disconnect()
  }

  if (startedAt === undefined || (SIZE > 0 && bytes === 0)) {
    throw new Error(
      `run ${run} received nothing: ${consumed} of ${MESSAGES} messages, ${bytes} bytes`
    )
  }

  const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6

  latencies.sort((a, b) => a - b)
  return {
    messagesPerSecond: (consumed / elapsedMs) * 1000,
    p50: percentile(latencies, 0.5),
    p90: percentile(latencies, 0.9),
    p99: percentile(latencies, 0.99),
  }
}

;(async () => {
  const kafka = new Kafka({
    clientId: 'bench-consume',
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    logLevel: logLevel.NOTHING,
  })
  if (SUBSCRIBE_CHANNELS === 1) {
    subscribeChannels(kafka)
  }

  await createTopic(kafka)

  try {
    await produceCorpus(kafka)

    // The first run pays for page cache, JIT warm-up and group formation; its numbers are discarded.
    await consumeOnce(kafka, 0)
    const measured = await consumeOnce(kafka, 1)

    console.log(
      JSON.stringify({
        messages: MESSAGES,
        size: SIZE,
        partitions: PARTITIONS,
        handler: HANDLER,
        subscribeChannels: SUBSCRIBE_CHANNELS === 1,
        ...measured,
      })
    )
  } finally {
    await deleteTopic(kafka)
  }
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
