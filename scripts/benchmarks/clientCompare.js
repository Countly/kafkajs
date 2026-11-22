/**
 * The same workload through the public API of any KafkaJS-compatible client, so the fork and
 * kafkajs@2.2.4 are measured by identical code. It loads the client only from `--client` with
 * `createRequire` rooted there with a real-path prefix check and uses `Kafka`, `producer().send`,
 * `consumer().run`, `consumer.on` and `CompressionCodecs` only: no fork module and no helper is
 * loaded.
 *
 * Usage: node scripts/benchmarks/clientCompare.js --client <dir> --package <@countly/kafkajs|kafkajs>
 *          --workload <produce-latency|produce-throughput|consume-throughput> [--messages N]
 *          [--duration-ms N] [--size N] [--partitions N] [--compression none|lz4]
 *          [--settings defaults|same] [--brokers host:port,...]
 * `--client` is an npm install prefix that holds `node_modules/<package>`. Defaults: 20000 messages
 * for `produce-latency`, 200000 for the others, 200 bytes per message, 3 partitions, no
 * compression, each package's own consumer defaults, the three brokers of docker-compose.yml.
 * `--duration-ms` (produce workloads only, never with `--messages`) sends for a fixed measured
 * window after a 5-second warm-up that is not measured. A consume run excludes the first tenth of
 * the corpus as its warm-up. `--settings same` gives either package one explicit consumer
 * configuration (`SAME_SETTINGS`) instead of its defaults. `--compression lz4` uses the fork's
 * built-in codec; kafkajs 2.2.4 has none, so for it the run registers an `lz4-napi` frame codec,
 * loaded from this repository's install, through `CompressionCodecs`.
 * Prints one JSON line: { package, version, workload, messages, size, partitions, compression,
 * ...metrics } with the package name and version it loaded, so a caller can reject a result from
 * the wrong client.
 * Metrics: `produce-latency` p50, p90, p99 (ms) of one awaited single-message send and
 * messagesPerSecond; `produce-throughput` messagesPerSecond and cpuMicrosPerMessage of sends of 500
 * messages; `consume-throughput` messagesPerSecond, cpuMicrosPerMessage (user plus system),
 * event-loop delay p50, p99 and max (ms), Fetch requests, bytes and messages per Fetch, all over the
 * measured window, plus the Fetch version, the consumer settings in force and the LZ4 codec. Each
 * also reports elapsedMs and measuredMessages. Exit codes: 0 done, 1 the client or the workload
 * failed, 2 a bad argument.
 */
const fs = require('node:fs')
const { createRequire } = require('node:module')
const path = require('node:path')
const { monitorEventLoopDelay } = require('node:perf_hooks')

const WORKLOADS = ['produce-latency', 'produce-throughput', 'consume-throughput']
const DEFAULT_MESSAGES = {
  'produce-latency': 20000,
  'produce-throughput': 200000,
  'consume-throughput': 200000,
}
const DEFAULT_BROKERS = 'localhost:9092,localhost:9095,localhost:9098'
const DEFAULT_SIZE = 200
const DEFAULT_PARTITIONS = 3
const COMPRESSIONS = ['none', 'lz4']
const SETTINGS = ['defaults', 'same']
const BATCH = 500
const CORPUS_CONCURRENCY = 8
const WARMUP_MS = 5000
const WARMUP_FRACTION = 0.1
const POOL_SIZE = 1024
const CONSUME_TIMEOUT_MS = 300000
const ROOT = path.resolve(__dirname, '..', '..')
const FLAGS = [
  '--client',
  '--package',
  '--workload',
  '--messages',
  '--duration-ms',
  '--size',
  '--partitions',
  '--compression',
  '--settings',
  '--brokers',
]
const USAGE =
  'usage: node scripts/benchmarks/clientCompare.js --client <dir> --package <name> --workload <produce-latency|produce-throughput|consume-throughput> [--messages N] [--duration-ms N] [--size N] [--partitions N] [--compression none|lz4] [--settings defaults|same] [--brokers host:port,...]'

/**
 * The one explicit consumer configuration of `--settings same`: kafkajs 2.2.4's defaults, with the
 * fork's own options set to what 2.2.4 does (no CRC check, no prefetch, no fetch session). 2.2.4
 * ignores the options it does not have.
 */
const SAME_SETTINGS = Object.freeze({
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxBytesPerPartition: 1048576,
  minBytes: 1,
  maxBytes: 10485760,
  maxWaitTimeInMs: 5000,
  maxInFlightRequests: null,
  readUncommitted: false,
  checkCrcs: false,
  maxPrefetchBatches: 0,
  fetchSessions: false,
})

/**
 * Each package's consumer defaults that can bear on consume rate, as its source sets them: the
 * fork's `src/consumer/index.js` and `Kafka#consumer` in `src/index.js` (in-flight limit 5, Java's
 * default); kafkajs 2.2.4's `src/consumer/index.js` and `src/network/connection.js` (in-flight
 * unbounded). `pollTimeout` is the fork's `maxPollInterval` and 2.2.4's `rebalanceTimeout`. 2.2.4
 * has no CRC check, no prefetch (each fetcher waits for its batches to be processed) and no fetch
 * sessions, so those are listed as false, 0 and false for it.
 */
const PACKAGE_DEFAULTS = {
  '@countly/kafkajs': Object.freeze({
    sessionTimeout: 45000,
    pollTimeout: 300000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 10485760,
    maxWaitTimeInMs: 5000,
    maxInFlightRequests: 5,
    readUncommitted: false,
    checkCrcs: true,
    maxPrefetchBatches: 0,
    fetchSessions: false,
  }),
  kafkajs: Object.freeze({
    sessionTimeout: 30000,
    pollTimeout: 60000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 1048576,
    minBytes: 1,
    maxBytes: 10485760,
    maxWaitTimeInMs: 5000,
    maxInFlightRequests: null,
    readUncommitted: false,
    checkCrcs: false,
    maxPrefetchBatches: 0,
    fetchSessions: false,
  }),
}

/** A command line the script cannot act on; the exit code is 2. */
class UsageError extends Error {}

/**
 * @param {string|undefined} text
 * @returns {number|undefined} the positive integer `text` holds, undefined when absent
 * @throws {UsageError} when present and not a positive integer
 */
const positive = (text) => {
  if (text === undefined) return undefined
  const n = Number(text)
  if (!Number.isInteger(n) || n < 1) throw new UsageError(USAGE)
  return n
}

/**
 * @param {string[]} argv
 * @returns {{ client: string, package: string, workload: string, messages: number|null,
 *   durationMs: number|null, size: number, partitions: number, compression: 'none'|'lz4',
 *   settings: 'defaults'|'same', brokers: string[] }}
 * @throws {UsageError} for a missing, repeated or unknown flag, an unknown workload, compression or
 *   settings, a count that is not a positive integer, or `--duration-ms` with `--messages` or on
 *   the consume workload
 */
const parseArgs = (argv) => {
  const flags = {}
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i]
    if (!FLAGS.includes(name) || argv[i + 1] === undefined || Object.hasOwn(flags, name)) {
      throw new UsageError(USAGE)
    }
    flags[name] = argv[i + 1]
  }
  const workload = flags['--workload']
  const compression = flags['--compression'] ?? 'none'
  const settings = flags['--settings'] ?? 'defaults'
  const known =
    WORKLOADS.includes(workload) &&
    COMPRESSIONS.includes(compression) &&
    SETTINGS.includes(settings)
  if (!flags['--client'] || !flags['--package'] || !known) throw new UsageError(USAGE)
  const durationMs = positive(flags['--duration-ms']) ?? null
  const timedConsume = durationMs !== null && workload === 'consume-throughput'
  if (timedConsume || (durationMs !== null && flags['--messages'] !== undefined)) {
    throw new UsageError(USAGE)
  }
  return {
    client: path.resolve(flags['--client']),
    package: flags['--package'],
    workload,
    messages:
      durationMs === null ? (positive(flags['--messages']) ?? DEFAULT_MESSAGES[workload]) : null,
    durationMs,
    size: positive(flags['--size']) ?? DEFAULT_SIZE,
    partitions: positive(flags['--partitions']) ?? DEFAULT_PARTITIONS,
    compression,
    settings,
    brokers: (flags['--brokers'] ?? DEFAULT_BROKERS).split(','),
  }
}

/**
 * @param {(id: string) => string} resolveId resolves a module id to a file
 * @param {string} id a package name
 * @returns {string|null} the version in the package.json above the package's entry point (packages
 *   whose `exports` hide their package.json included), null when the package does not resolve
 */
const packageVersion = (resolveId, id) => {
  let dir
  try {
    dir = path.dirname(resolveId(id))
  } catch {
    return null
  }
  for (; dir !== path.dirname(dir); dir = path.dirname(dir)) {
    const file = path.join(dir, 'package.json')
    if (!fs.existsSync(file)) continue
    const metadata = JSON.parse(fs.readFileSync(file, 'utf8'))
    if (metadata.name === id) return metadata.version
  }
  return null
}

/**
 * @param {string} dir an install prefix that holds `node_modules/<pkg>`
 * @param {string} pkg the package name to load
 * @returns {{ Kafka: Function, logLevel: object, CompressionTypes: object,
 *   CompressionCodecs: object, name: string, version: string, lz4Version: string|null }} the
 *   client, the name and version its own package.json states, and the version of the `lz4-napi`
 *   the package itself resolves inside `dir` (null when none)
 * @throws {Error} when either the entry point or package metadata resolves outside `dir` or is absent
 */
const loadClient = (dir, pkg) => {
  const prefix = fs.realpathSync(dir)
  const clientRequire = createRequire(path.join(prefix, 'package.json'))
  const resolve = (id) => {
    const resolved = fs.realpathSync(clientRequire.resolve(id))
    const relative = path.relative(prefix, resolved)
    if (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
      throw new Error(`${id} resolved outside client prefix: ${dir}`)
    }
    return resolved
  }
  const entry = resolve(pkg)
  const metadata = resolve(`${pkg}/package.json`)
  const { Kafka, logLevel, CompressionTypes, CompressionCodecs } = require(entry)
  const { name, version } = require(metadata)
  const packageRequire = createRequire(entry)
  const lz4Version = packageVersion((id) => {
    const resolved = fs.realpathSync(packageRequire.resolve(id))
    if (path.relative(prefix, resolved).startsWith('..')) throw new Error('outside prefix')
    return resolved
  }, 'lz4-napi')
  return { Kafka, logLevel, CompressionTypes, CompressionCodecs, name, version, lz4Version }
}

/**
 * Gives a client without a built-in LZ4 codec (kafkajs 2.2.4) an `lz4-napi` frame codec, loaded from
 * this repository's install, which is the package and version the fork's built-in codec uses.
 * @param {{ CompressionTypes: object, CompressionCodecs: object, lz4Version: string|null }} client
 * @param {NodeJS.Require} [load] loads a package from the repository root
 * @returns {{ codec: 'built-in'|'registered', package: 'lz4-napi', version: string|null }} what
 *   the run compresses with
 */
const useLz4 = (client, load = createRequire(path.join(ROOT, 'package.json'))) => {
  if (client.lz4Version !== null) {
    return { codec: 'built-in', package: 'lz4-napi', version: client.lz4Version }
  }
  const lz4 = load('lz4-napi')
  const codec = {
    compress: async (encoder) => lz4.compressFrame(encoder.buffer),
    decompress: async (buffer) => lz4.decompressFrame(buffer),
  }
  client.CompressionCodecs[client.CompressionTypes.LZ4] = () => codec
  return {
    codec: 'registered',
    package: 'lz4-napi',
    version: packageVersion(load.resolve, 'lz4-napi'),
  }
}

/**
 * @param {number[]} sorted values in ascending order
 * @param {number} p fraction between 0 and 1
 * @returns {number} the value at that fraction of the list
 */
const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]

/** @returns {bigint} a monotonic clock reading in nanoseconds */
const now = () => process.hrtime.bigint()

/**
 * @typedef {{ now: () => bigint, setTimeout: Function, clearTimeout: Function,
 *   cpuUsage: (previous?: object) => { user: number, system: number },
 *   eventLoopDelay: () => { enable: Function, disable: Function, percentile: (p: number) => number,
 *     max: number } }} Timing clocks, timers, process CPU and an event-loop delay histogram (ns)
 */
const DEFAULT_TIMING = {
  now,
  setTimeout,
  clearTimeout,
  cpuUsage: (previous) => process.cpuUsage(previous),
  eventLoopDelay: () => monitorEventLoopDelay({ resolution: 10 }),
}

/**
 * Corpus values: `POOL_SIZE` distinct records of `size` bytes, words from a fixed vocabulary in a
 * seeded order, so a compressed batch holds text that compresses the way logs do rather than one
 * repeated byte.
 * @param {number} size bytes per value
 * @returns {Buffer[]}
 */
const valuePool = (size) => {
  const words = ['event', 'user', 'session', 'view', 'click', 'app', 'id', 'ts', 'ok', 'error']
  let seed = 1
  return Array.from({ length: POOL_SIZE }, (_, i) => {
    let text = `{"n":${i}`
    while (text.length < size) {
      seed = (seed * 1103515245 + 12345) % 2147483648
      text += `,"${words[seed % words.length]}":${seed % 100000}`
    }
    return Buffer.from(text.slice(0, size))
  })
}

/**
 * @param {Buffer[]} values the corpus values
 * @param {number} from index of the first message
 * @param {number} count
 * @returns {{ value: Buffer }[]} `count` messages cycling through `values`
 */
const messagesOf = (values, from, count) =>
  Array.from({ length: count }, (_, i) => ({ value: values[(from + i) % values.length] }))

/**
 * Sends `messages` messages in sends of at most `BATCH`, `concurrency` sends at a time.
 * @param {object} producer a connected producer
 * @param {{ topic: string, compression?: number }} target topic and codec of every send
 * @param {number} messages
 * @param {Buffer[]} values
 * @param {number} [concurrency]
 * @returns {Promise<void>}
 */
const sendAll = async (producer, target, messages, values, concurrency = 1) => {
  let next = 0
  const lane = async () => {
    while (next < messages) {
      const from = next
      next += Math.min(BATCH, messages - from)
      await producer.send({ ...target, messages: messagesOf(values, from, next - from) })
    }
  }
  await Promise.all(Array.from({ length: concurrency }, lane))
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
 * Repeats `send` until `ms` milliseconds of the monotonic clock have passed since `started`.
 * @param {() => bigint} clock
 * @param {bigint} started
 * @param {number} ms
 * @param {() => Promise<number>} send one send; resolves to the messages it carried
 * @returns {Promise<number>} the messages sent
 */
const sendFor = async (clock, started, ms, send) => {
  let sent = 0
  while (Number(clock() - started) / 1e6 < ms) sent += await send()
  return sent
}

/**
 * One awaited single-message send per record, after an untimed warm-up: one send that pays for
 * metadata, or `WARMUP_MS` of sends with `durationMs`. With `durationMs` the measured window is
 * that long; otherwise it is `messages` sends.
 * @param {object} kafka
 * @param {object} target topic and codec of every send
 * @param {{ messages: number, durationMs: number|null, values: Buffer[] }} options
 * @param {Timing} timing
 * @returns {Promise<object>} p50, p90, p99, messagesPerSecond, measuredMessages and elapsedMs
 */
const produceLatency = (kafka, target, { messages, durationMs, values }, { now }) =>
  withProducer(kafka, async (producer) => {
    const latencies = []
    const one = async () => {
      const sentAt = now()
      await producer.send({ ...target, messages: messagesOf(values, latencies.length, 1) })
      latencies.push(Number(now() - sentAt) / 1e6)
      return 1
    }
    await one()
    if (durationMs !== null) await sendFor(now, now(), WARMUP_MS, one)
    latencies.length = 0
    const started = now()
    if (durationMs !== null) await sendFor(now, started, durationMs, one)
    else for (let i = 0; i < messages; i++) await one()
    const elapsedMs = Number(now() - started) / 1e6
    const measuredMessages = latencies.length
    latencies.sort((a, b) => a - b)
    return {
      p50: percentile(latencies, 0.5),
      p90: percentile(latencies, 0.9),
      p99: percentile(latencies, 0.99),
      messagesPerSecond: (measuredMessages / elapsedMs) * 1000,
      measuredMessages,
      elapsedMs,
    }
  })

/**
 * Batched sends of `BATCH` messages, after an untimed warm-up: one send that pays for metadata, or
 * `WARMUP_MS` of batches with `durationMs`. With `durationMs` the measured window is that long;
 * otherwise it is `messages` messages.
 * @param {object} kafka
 * @param {object} target topic and codec of every send
 * @param {{ messages: number, durationMs: number|null, values: Buffer[] }} options
 * @param {Timing} timing
 * @returns {Promise<object>} messagesPerSecond, cpuMicrosPerMessage, measuredMessages and elapsedMs
 */
const produceThroughput = (kafka, target, { messages, durationMs, values }, timing) =>
  withProducer(kafka, async (producer) => {
    const { now, cpuUsage } = timing
    const batch = async () => {
      await producer.send({ ...target, messages: messagesOf(values, 0, BATCH) })
      return BATCH
    }
    await producer.send({ ...target, messages: messagesOf(values, 0, 1) })
    if (durationMs !== null) await sendFor(now, now(), WARMUP_MS, batch)
    const started = now()
    const cpu = cpuUsage()
    let measuredMessages = messages
    if (durationMs !== null) measuredMessages = await sendFor(now, started, durationMs, batch)
    else await sendAll(producer, target, messages, values)
    const elapsedMs = Number(now() - started) / 1e6
    const { user, system } = cpuUsage(cpu)
    return {
      messagesPerSecond: (measuredMessages / elapsedMs) * 1000,
      cpuMicrosPerMessage: (user + system) / measuredMessages,
      measuredMessages,
      elapsedMs,
    }
  })

/**
 * Produces the corpus untimed, then times `eachMessage` from the end of the warm-up (the first
 * tenth of the corpus) to the last delivery, so group formation and warm-up are outside the
 * window. Over that window it also takes process CPU, an event-loop delay histogram and the
 * consumer's Fetch requests (`consumer.events.REQUEST`, whose `size` is the response's bytes).
 * Gives up after `CONSUME_TIMEOUT_MS`.
 * @param {object} kafka
 * @param {object} target topic and codec of the corpus sends
 * @param {{ messages: number, values: Buffer[], consumerSettings: object }} options
 * @param {Timing} timing
 * @returns {Promise<object>} messagesPerSecond, cpuMicrosPerMessage, eventLoopDelayP50,
 *   eventLoopDelayP99, eventLoopDelayMax (ms), fetchRequests, bytesPerFetch, messagesPerFetch,
 *   fetchVersion, measuredMessages and elapsedMs
 * @throws {Error} when the corpus is not delivered in time
 */
const consumeThroughput = async (kafka, target, { messages, values, consumerSettings }, timing) => {
  const { now, setTimeout, clearTimeout, cpuUsage, eventLoopDelay } = timing
  await withProducer(kafka, (producer) =>
    sendAll(producer, target, messages, values, CORPUS_CONCURRENCY)
  )
  const consumer = kafka.consumer({ groupId: `${target.topic}-group`, ...consumerSettings })
  const warmup = Math.max(1, Math.floor(messages * WARMUP_FRACTION))
  const delay = eventLoopDelay()
  const fetches = { requests: 0, bytes: 0, version: null }
  let measuring = false
  let timer
  try {
    consumer.on(consumer.events.REQUEST, ({ payload }) => {
      if (!measuring || payload.apiName !== 'Fetch') return
      fetches.requests++
      fetches.bytes += payload.size
      fetches.version = payload.apiVersion
    })
    await consumer.connect()
    await consumer.subscribe({ topic: target.topic, fromBeginning: true })
    let received = 0
    let first
    let last
    let cpu
    const delivered = new Promise((resolve, reject) => {
      timer = setTimeout(
        () => reject(new Error(`received ${received} of ${messages} messages in time`)),
        CONSUME_TIMEOUT_MS
      )
      consumer
        .run({
          eachMessage: async () => {
            if (++received === warmup) {
              measuring = true
              delay.enable()
              cpu = cpuUsage()
              first = now()
            }
            if (received === messages) {
              last = now()
              cpu = cpuUsage(cpu)
              delay.disable()
              measuring = false
              resolve()
            }
          },
        })
        .catch(reject)
    })
    await delivered
    const elapsedMs = Number(last - first) / 1e6
    const measuredMessages = messages - warmup
    return {
      messagesPerSecond: (measuredMessages / elapsedMs) * 1000,
      cpuMicrosPerMessage: (cpu.user + cpu.system) / measuredMessages,
      eventLoopDelayP50: delay.percentile(50) / 1e6,
      eventLoopDelayP99: delay.percentile(99) / 1e6,
      eventLoopDelayMax: delay.max / 1e6,
      fetchRequests: fetches.requests,
      bytesPerFetch: fetches.bytes / fetches.requests,
      messagesPerFetch: measuredMessages / fetches.requests,
      fetchVersion: fetches.version,
      measuredMessages,
      elapsedMs,
    }
  } finally {
    measuring = false
    delay.disable()
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
 * Runs one workload on a fresh topic, then deletes its group (if any) and topic. The consume
 * corpus topic has one replica, since only the fetch path is measured; every topic's files are
 * removed one second after it is deleted, so corpora do not pile up on the runner's disk.
 * @param {{ Kafka: Function, logLevel: object, name?: string, CompressionTypes?: object,
 *   CompressionCodecs?: object, lz4Version?: string|null }} client
 * @param {string} workload one of `WORKLOADS`
 * @param {{ messages: number, durationMs?: number|null, size: number, partitions?: number,
 *   compression?: 'none'|'lz4', settings?: 'defaults'|'same', brokers: string[] }} options
 * @param {Partial<Timing>} [timing]
 * @returns {Promise<object>} the workload's metrics; a consume run adds the consumer `settings`
 *   in force and an LZ4 run its `codec`
 * @throws {Error} for an unknown workload, or whatever the client throws
 */
const runWorkload = async (client, workload, options, timing = {}) => {
  if (!RUNNERS[workload]) throw new Error(`unknown workload ${workload}`)
  const { Kafka, logLevel } = client
  const { partitions = DEFAULT_PARTITIONS, compression = 'none', settings = 'defaults' } = options
  const consume = workload === 'consume-throughput'
  const codec = compression === 'lz4' ? useLz4(client) : undefined
  const kafka = new Kafka({
    clientId: `bench-compare-${workload}`,
    brokers: options.brokers,
    logLevel: logLevel.NOTHING,
  })
  const topic = `bench-compare-${workload}-${process.pid}-${Date.now()}`
  const target = codec ? { topic, compression: client.CompressionTypes.LZ4 } : { topic }
  const consumerSettings = settings === 'same' ? SAME_SETTINGS : {}
  const admin = kafka.admin()
  try {
    await admin.connect()
    try {
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: partitions,
            ...(consume && { replicationFactor: 1 }),
            configEntries: [{ name: 'file.delete.delay.ms', value: '1000' }],
          },
        ],
        waitForLeaders: true,
      })
      const values = valuePool(options.size)
      const run = { durationMs: null, ...options, values, consumerSettings }
      const metrics = await RUNNERS[workload](kafka, target, run, { ...DEFAULT_TIMING, ...timing })
      const inForce = consume && {
        settings:
          settings === 'same'
            ? { same: SAME_SETTINGS }
            : { defaults: PACKAGE_DEFAULTS[client.name] ?? null },
      }
      return { ...metrics, ...inForce, ...(codec && { codec }) }
    } finally {
      if (consume) {
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
  const { workload, messages, size, partitions, compression } = options
  log(
    JSON.stringify({
      package: client.name,
      version: client.version,
      workload,
      messages,
      size,
      partitions,
      compression,
      ...metrics,
    })
  )
}

module.exports = {
  parseArgs,
  loadClient,
  useLz4,
  valuePool,
  runWorkload,
  main,
  WORKLOADS,
  SAME_SETTINGS,
  PACKAGE_DEFAULTS,
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error instanceof UsageError ? error.message : error.stack}\n`)
    process.exitCode = error instanceof UsageError ? 2 : 1
  })
}
