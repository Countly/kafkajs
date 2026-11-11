/**
 * The same workload through the public API of any KafkaJS-compatible client, so the fork and
 * kafkajs@2.2.4 are measured by identical code. It loads the client only from `--client` with
 * `require.resolve` with a real-path prefix check and uses `Kafka`, `producer().send` and
 * `consumer().run` only: no fork module, no helper and no instrumentation is loaded.
 *
 * Usage: node scripts/benchmarks/clientCompare.js --client <dir> --package <@countly/kafkajs|kafkajs>
 *          --workload <produce-latency|produce-throughput|consume-throughput> [--messages N]
 *          [--size N] [--brokers host:port,...]
 * `--client` is an npm install prefix that holds `node_modules/<package>`. Defaults: 20000 messages
 * for `produce-latency`, 200000 for the others, 200 bytes per message, the three brokers of
 * docker-compose.yml.
 * Prints one JSON line: { package, version, workload, messages, size, ...metrics } with the
 * package name and version it loaded, so a caller can reject a result from the wrong client.
 * Metrics: `produce-latency` p50, p90, p99 (ms) of one awaited single-message send and
 * messagesPerSecond; `produce-throughput` messagesPerSecond of sends of 500 messages;
 * `consume-throughput` messagesPerSecond between the first and the last `eachMessage`. Each also
 * reports elapsedMs. Exit codes: 0 done, 1 the client or the workload failed, 2 a bad argument.
 */
const fs = require('node:fs')
const path = require('node:path')

const WORKLOADS = ['produce-latency', 'produce-throughput', 'consume-throughput']
const DEFAULT_MESSAGES = {
  'produce-latency': 20000,
  'produce-throughput': 200000,
  'consume-throughput': 200000,
}
const DEFAULT_BROKERS = 'localhost:9092,localhost:9095,localhost:9098'
const DEFAULT_SIZE = 200
const PARTITIONS = 3
const BATCH = 500
const CONSUME_TIMEOUT_MS = 300000
const USAGE =
  'usage: node scripts/benchmarks/clientCompare.js --client <dir> --package <name> --workload <produce-latency|produce-throughput|consume-throughput> [--messages N] [--size N] [--brokers host:port,...]'

/** A command line the script cannot act on; the exit code is 2. */
class UsageError extends Error {}

/**
 * @param {string[]} argv
 * @returns {{ client: string, package: string, workload: string, messages: number, size: number,
 *   brokers: string[] }}
 * @throws {UsageError} for a missing, repeated or unknown flag, an unknown workload or a count that
 *   is not a positive integer
 */
const parseArgs = (argv) => {
  const flags = {}
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i]
    const known = ['--client', '--package', '--workload', '--messages', '--size', '--brokers']
    if (!known.includes(name) || argv[i + 1] === undefined || Object.hasOwn(flags, name)) {
      throw new UsageError(USAGE)
    }
    flags[name] = argv[i + 1]
  }
  const workload = flags['--workload']
  if (!flags['--client'] || !flags['--package'] || !WORKLOADS.includes(workload)) {
    throw new UsageError(USAGE)
  }
  const messages = Number(flags['--messages'] ?? DEFAULT_MESSAGES[workload])
  const size = Number(flags['--size'] ?? DEFAULT_SIZE)
  if (!Number.isInteger(messages) || messages < 1 || !Number.isInteger(size) || size < 1) {
    throw new UsageError(USAGE)
  }
  return {
    client: path.resolve(flags['--client']),
    package: flags['--package'],
    workload,
    messages,
    size,
    brokers: (flags['--brokers'] ?? DEFAULT_BROKERS).split(','),
  }
}

/**
 * @param {string} dir an install prefix that holds `node_modules/<pkg>`
 * @param {string} pkg the package name to load
 * @returns {{ Kafka: Function, logLevel: object, name: string, version: string }} the client and the
 *   name and version its own package.json states
 * @throws {Error} when either the entry point or package metadata resolves outside `dir` or is absent
 */
const loadClient = (dir, pkg) => {
  const prefix = fs.realpathSync(dir)
  const resolve = (id) => {
    const resolved = fs.realpathSync(require.resolve(id, { paths: [prefix] }))
    const relative = path.relative(prefix, resolved)
    if (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
      throw new Error(`${id} resolved outside client prefix: ${dir}`)
    }
    return resolved
  }
  const entry = resolve(pkg)
  const metadata = resolve(`${pkg}/package.json`)
  const { Kafka, logLevel } = require(entry)
  const { name, version } = require(metadata)
  return { Kafka, logLevel, name, version }
}

/**
 * @param {number[]} sorted values in ascending order
 * @param {number} p fraction between 0 and 1
 * @returns {number} the value at that fraction of the list
 */
const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]

/** @returns {bigint} a monotonic clock reading in nanoseconds */
const now = () => process.hrtime.bigint()

/** @typedef {{ now: () => bigint, setTimeout: Function, clearTimeout: Function }} Timing */
const DEFAULT_TIMING = { now, setTimeout, clearTimeout }

/**
 * Sends `messages` messages in sends of at most `BATCH`, one after another.
 * @param {object} producer a connected producer
 * @param {string} topic
 * @param {number} messages
 * @param {Buffer} value
 * @returns {Promise<void>}
 */
const sendAll = async (producer, topic, messages, value) => {
  const batch = Array.from({ length: Math.min(BATCH, messages) }, () => ({ value }))
  for (let sent = 0; sent < messages; sent += BATCH) {
    await producer.send({ topic, messages: batch.slice(0, Math.min(BATCH, messages - sent)) })
  }
}

/**
 * Runs `work` with a connected producer and always disconnects it.
 * @param {object} kafka
 * @param {(producer: object) => Promise<*>} work
 * @returns {Promise<*>} what `work` returns
 */
const withProducer = async (kafka, work) => {
  const producer = kafka.producer()
  try {
    await producer.connect()
    return await work(producer)
  } finally {
    await producer.disconnect()
  }
}

/**
 * One awaited single-message send per record, after one untimed send that pays for metadata.
 * @param {object} kafka
 * @param {string} topic
 * @param {{ messages: number, size: number }} options
 * @param {Timing} timing monotonic clock and timeout scheduling
 * @returns {Promise<object>} p50, p90, p99, messagesPerSecond and elapsedMs
 */
const produceLatency = (kafka, topic, { messages, size }, { now }) =>
  withProducer(kafka, async (producer) => {
    const value = Buffer.alloc(size, 'x')
    await producer.send({ topic, messages: [{ value }] })
    const latencies = new Array(messages)
    const started = now()
    for (let i = 0; i < messages; i++) {
      const sentAt = now()
      await producer.send({ topic, messages: [{ value }] })
      latencies[i] = Number(now() - sentAt) / 1e6
    }
    const elapsedMs = Number(now() - started) / 1e6
    latencies.sort((a, b) => a - b)
    return {
      p50: percentile(latencies, 0.5),
      p90: percentile(latencies, 0.9),
      p99: percentile(latencies, 0.99),
      messagesPerSecond: (messages / elapsedMs) * 1000,
      elapsedMs,
    }
  })

/**
 * Batched sends of `BATCH` messages, after one untimed send that pays for metadata.
 * @param {object} kafka
 * @param {string} topic
 * @param {{ messages: number, size: number }} options
 * @param {Timing} timing monotonic clock and timeout scheduling
 * @returns {Promise<object>} messagesPerSecond and elapsedMs
 */
const produceThroughput = (kafka, topic, { messages, size }, { now }) =>
  withProducer(kafka, async (producer) => {
    const value = Buffer.alloc(size, 'x')
    await producer.send({ topic, messages: [{ value }] })
    const started = now()
    await sendAll(producer, topic, messages, value)
    const elapsedMs = Number(now() - started) / 1e6
    return { messagesPerSecond: (messages / elapsedMs) * 1000, elapsedMs }
  })

/**
 * Produces the corpus untimed, then times `eachMessage` from the first delivery to the last, so
 * group formation is outside the interval. Gives up after `CONSUME_TIMEOUT_MS`.
 * @param {object} kafka
 * @param {string} topic
 * @param {{ messages: number, size: number }} options
 * @param {Timing} timing monotonic clock and timeout scheduling
 * @returns {Promise<object>} messagesPerSecond and elapsedMs
 * @throws {Error} when the corpus is not delivered in time
 */
const consumeThroughput = async (
  kafka,
  topic,
  { messages, size },
  { now, setTimeout, clearTimeout }
) => {
  await withProducer(kafka, (producer) =>
    sendAll(producer, topic, messages, Buffer.alloc(size, 'x'))
  )
  const consumer = kafka.consumer({ groupId: `${topic}-group` })
  let timer
  try {
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    let received = 0
    let first
    let last
    const delivered = new Promise((resolve, reject) => {
      timer = setTimeout(
        () => reject(new Error(`received ${received} of ${messages} messages in time`)),
        CONSUME_TIMEOUT_MS
      )
      consumer
        .run({
          eachMessage: async () => {
            first ??= now()
            if (++received === messages) {
              last = now()
              resolve()
            }
          },
        })
        .catch(reject)
    })
    await delivered
    const elapsedMs = Number(last - first) / 1e6
    return { messagesPerSecond: ((messages - 1) / elapsedMs) * 1000, elapsedMs }
  } finally {
    clearTimeout(timer)
    await consumer.disconnect()
  }
}

const RUNNERS = {
  'produce-latency': produceLatency,
  'produce-throughput': produceThroughput,
  'consume-throughput': consumeThroughput,
}

/**
 * Runs one workload on a fresh topic, then deletes its group (if any) and topic.
 * @param {{ Kafka: Function, logLevel: object }} client
 * @param {string} workload one of `WORKLOADS`
 * @param {{ messages: number, size: number, brokers: string[] }} options
 * @param {Timing} [timing] monotonic clock and timeout scheduling
 * @returns {Promise<object>} the workload's metrics
 * @throws {Error} for an unknown workload, or whatever the client throws
 */
const runWorkload = async ({ Kafka, logLevel }, workload, options, timing = DEFAULT_TIMING) => {
  if (!RUNNERS[workload]) throw new Error(`unknown workload ${workload}`)
  const kafka = new Kafka({
    clientId: `bench-compare-${workload}`,
    brokers: options.brokers,
    logLevel: logLevel.NOTHING,
  })
  const topic = `bench-compare-${workload}-${process.pid}-${Date.now()}`
  const admin = kafka.admin()
  try {
    await admin.connect()
    try {
      await admin.createTopics({
        topics: [{ topic, numPartitions: PARTITIONS }],
        waitForLeaders: true,
      })
      return await RUNNERS[workload](kafka, topic, options, timing)
    } finally {
      if (workload === 'consume-throughput') {
        await admin
          .deleteGroups([`${topic}-group`])
          .catch((error) =>
            process.stderr.write(`could not delete ${topic}-group: ${error.message}\n`)
          )
      }
      await admin
        .deleteTopics({ topics: [topic] })
        .catch((error) => process.stderr.write(`could not delete ${topic}: ${error.message}\n`))
    }
  } finally {
    await admin.disconnect()
  }
}

/**
 * @param {string[]} argv
 * @param {(line: string) => void} [log] receives the result line
 * @returns {Promise<void>}
 * @throws {UsageError} for a bad argument
 * @throws {Error} when the client cannot be loaded or the workload fails
 */
const main = async (argv, log = console.log) => {
  const options = parseArgs(argv)
  const client = loadClient(options.client, options.package)
  const metrics = await runWorkload(client, options.workload, options)
  const { workload, messages, size } = options
  log(
    JSON.stringify({
      package: client.name,
      version: client.version,
      workload,
      messages,
      size,
      ...metrics,
    })
  )
}

module.exports = { parseArgs, loadClient, runWorkload, main, WORKLOADS }

if (require.main === module) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error instanceof UsageError ? error.message : error.stack}\n`)
    process.exitCode = error instanceof UsageError ? 2 : 1
  })
}
