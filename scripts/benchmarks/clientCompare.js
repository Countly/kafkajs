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
 *          [--settings defaults|same] [--passes N] [--brokers host:port,...]
 * `--client` is an npm install prefix that holds `node_modules/<package>`. Defaults: 20000 messages
 * for `produce-latency`, 200000 for the others, 200 bytes per message, 3 partitions, no
 * compression, each package's own consumer defaults, the three brokers of docker-compose.yml.
 * `--duration-ms` (produce workloads only, never with `--messages`) sends for a fixed measured
 * window after a 5-second warm-up that is not measured. A consume run excludes the first tenth of
 * the corpus as its warm-up; `--passes N` (consume only) reads the corpus N times, seeking each
 * partition back to its first offset once it delivered its last record, so a corpus the runner's
 * disk can hold still gives a long enough window. `--settings same` gives either package one
 * explicit consumer configuration (`SAME_SETTINGS`) and, on the produce workloads, one explicit
 * producer configuration (`SAME_PRODUCER_SETTINGS`) instead of its defaults. A run waits for its
 * new topic's leaders itself, through unknown-topic answers: kafkajs 2.2.4's `createTopics`
 * `waitForLeaders` gives up on the first one, which a KRaft broker can give right after the topic
 * is created. `--compression lz4` uses the fork's
 * built-in codec; kafkajs 2.2.4 has none, so for it the run registers an `lz4-napi` frame codec,
 * loaded from this repository's install, through `CompressionCodecs`.
 * Prints one JSON line: { package, version, workload, messages, size, partitions, compression,
 * ...metrics } with the package name and version it loaded, so a caller can reject a result from
 * the wrong client.
 * Metrics: `produce-latency` p50, p90, p99 (ms) of one awaited single-message send and
 * messagesPerSecond; `produce-throughput` messagesPerSecond and cpuMicrosPerMessage of sends of 500
 * messages; `consume-throughput` messagesPerSecond, cpuMicrosPerMessage (user plus system),
 * event-loop delay p50, p99 and max (ms), Fetch requests, bytes and messages per Fetch, all over the
 * measured window, plus the Fetch version, the passes and the LZ4 codec. A consume run also
 * reports mainThreadCpuMicrosPerMessage (the JavaScript thread only, from `process.threadCpuUsage`;
 * absent on a Node without it), fullGcCount (major collections in the measured window) and the
 * corpus layout. Each also reports elapsedMs, measuredMessages and the producer or consumer
 * settings in force. Exit codes: 0 done, 1 the client or the workload failed, 2 a bad argument.
 *
 * The consume corpus is laid out the same on both arms, whatever `--settings` says: each arm's own
 * client writes it (loading the other package would break the arm's isolation) with
 * `CORPUS_PRODUCER_SETTINGS`, in sends of `BATCH` records where record `i` goes to partition
 * `i % partitions`, so each send leaves one batch per partition and neither package's
 * partitioner or batching defaults shape the log.
 */
const fs = require('node:fs')
const { createRequire } = require('node:module')
const path = require('node:path')
const { monitorEventLoopDelay, PerformanceObserver, constants } = require('node:perf_hooks')
const { setTimeout: sleep } = require('node:timers/promises')

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
const LEADER_WAIT_MS = 30000
const LEADER_POLL_MS = 100
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
  '--passes',
  '--brokers',
]
const USAGE =
  'usage: node scripts/benchmarks/clientCompare.js --client <dir> --package <name> --workload <produce-latency|produce-throughput|consume-throughput> [--messages N] [--duration-ms N] [--size N] [--partitions N] [--compression none|lz4] [--settings defaults|same] [--passes N] [--brokers host:port,...]'

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

/**
 * The one explicit producer configuration of `--settings same`: what kafkajs 2.2.4 does, set on the
 * fork's own options (not idempotent, unbounded in-flight requests, five retries, no linger, and a
 * batch large enough that one send of 500 records leaves as one batch per partition, as 2.2.4
 * sends it), plus `Partitioners.DefaultPartitioner`, 2.2.4's default, on both arms. 2.2.4 ignores
 * `linger` and `batchSize`, which it does not have.
 */
const SAME_PRODUCER_SETTINGS = Object.freeze({
  idempotent: false,
  maxInFlightRequests: null,
  retry: Object.freeze({ retries: 5 }),
  linger: 0,
  batchSize: 1048576,
})

/**
 * The producer configuration that writes every consume corpus, on both arms and under either
 * `--settings`: not idempotent, unbounded in-flight requests, five retries, no linger, and a
 * one-byte `batchSize`, so on the fork every send's records for a partition fill a batch of their
 * own at once and never join another send's. kafkajs 2.2.4 ignores `linger` and `batchSize` and
 * sends each `send()` as its own request, which gives it the same batches.
 */
const CORPUS_PRODUCER_SETTINGS = Object.freeze({
  idempotent: false,
  maxInFlightRequests: null,
  retry: Object.freeze({ retries: 5 }),
  linger: 0,
  batchSize: 1,
})

/** The consume corpus layout each consume run reports. */
const CORPUS_LAYOUT = Object.freeze({
  partitioning: 'record i to partition i % partitions',
  recordsPerSend: BATCH,
  concurrentSends: CORPUS_CONCURRENCY,
  producer: CORPUS_PRODUCER_SETTINGS,
})

/**
 * Each package's producer defaults that can bear on produce latency or rate, as its source sets
 * them: the fork's `DEFAULTS` and `DELIVERY_RETRY` in `src/producer/index.js` and `Kafka#producer`
 * in `src/index.js`; kafkajs 2.2.4's `src/producer/index.js` and `src/network/connection.js`.
 * `none` marks a setting the package does not have: 2.2.4 sends each `send()` as its own request.
 * Both send with `acks: -1` and no compression unless told otherwise.
 */
const PRODUCER_DEFAULTS = {
  '@countly/kafkajs': Object.freeze({
    idempotent: true,
    maxInFlightRequests: 5,
    retries: 2147483647,
    linger: 5,
    batchSize: 16384,
    partitioner: 'UniformStickyPartitioner (adaptive)',
    acks: -1,
  }),
  kafkajs: Object.freeze({
    idempotent: false,
    maxInFlightRequests: null,
    retries: 5,
    linger: 'none',
    batchSize: 'none',
    partitioner: 'DefaultPartitioner',
    acks: -1,
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
 *   settings: 'defaults'|'same', passes: number, brokers: string[] }}
 * @throws {UsageError} for a missing, repeated or unknown flag, an unknown workload, compression or
 *   settings, a count that is not a positive integer, or `--duration-ms` with `--messages`, or
 *   `--duration-ms` or `--passes` on a workload they do not apply to
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
  const consume = workload === 'consume-throughput'
  const passes = positive(flags['--passes']) ?? 1
  const misplaced = consume ? durationMs !== null : flags['--passes'] !== undefined
  if (misplaced || (durationMs !== null && flags['--messages'] !== undefined)) {
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
    passes,
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
 *   CompressionCodecs: object, Partitioners: object, name: string, version: string,
 *   lz4Version: string|null }} the
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
  const { Kafka, logLevel, CompressionTypes, CompressionCodecs, Partitioners } = require(entry)
  const { name, version } = require(metadata)
  const packageRequire = createRequire(entry)
  const lz4Version = packageVersion((id) => {
    const resolved = fs.realpathSync(packageRequire.resolve(id))
    if (path.relative(prefix, resolved).startsWith('..')) throw new Error('outside prefix')
    return resolved
  }, 'lz4-napi')
  return {
    Kafka,
    logLevel,
    CompressionTypes,
    CompressionCodecs,
    Partitioners,
    name,
    version,
    lz4Version,
  }
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
 * Starts counting major (full) garbage collections through a `gc` PerformanceObserver. A
 * collection whose entry Node has not delivered yet when the count stops is not counted.
 * @returns {() => number} stops observing and returns the count; later calls return it again
 */
const countFullGcs = () => {
  let count = 0
  let stopped = false
  const add = (entries) => {
    for (const { detail } of entries) {
      if (detail?.kind === constants.NODE_PERFORMANCE_GC_MAJOR) count++
    }
  }
  const observer = new PerformanceObserver((list) => add(list.getEntries()))
  observer.observe({ entryTypes: ['gc'] })
  return () => {
    if (!stopped) {
      stopped = true
      add(observer.takeRecords())
      observer.disconnect()
    }
    return count
  }
}

/**
 * @typedef {{ now: () => bigint, setTimeout: Function, clearTimeout: Function,
 *   sleep: (ms: number) => Promise<void>,
 *   cpuUsage: (previous?: object) => { user: number, system: number },
 *   threadCpuUsage: ((previous?: object) => { user: number, system: number })|null,
 *   countFullGcs: () => () => number,
 *   eventLoopDelay: () => { enable: Function, disable: Function, percentile: (p: number) => number,
 *     max: number } }} Timing clocks, timers, process CPU, the JavaScript thread's CPU (null on a
 *   Node without `process.threadCpuUsage`), a full-GC counter and an event-loop delay histogram (ns)
 */
const DEFAULT_TIMING = {
  now,
  setTimeout,
  clearTimeout,
  sleep: (ms) => sleep(ms),
  cpuUsage: (previous) => process.cpuUsage(previous),
  threadCpuUsage:
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection
    typeof process.threadCpuUsage === 'function'
      ? // eslint-disable-next-line n/no-unsupported-features/node-builtins -- guarded above
        (previous) => process.threadCpuUsage(previous)
      : null,
  countFullGcs,
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
 * @param {number} [partitions] when given, message `n` names partition `n % partitions`
 * @returns {{ value: Buffer, partition?: number }[]} `count` messages cycling through `values`
 */
const messagesOf = (values, from, count, partitions) =>
  Array.from({ length: count }, (_, i) =>
    partitions === undefined
      ? { value: values[(from + i) % values.length] }
      : { value: values[(from + i) % values.length], partition: (from + i) % partitions }
  )

/**
 * Sends `messages` messages in sends of at most `BATCH`, `concurrency` sends at a time.
 * @param {object} producer a connected producer
 * @param {{ topic: string, compression?: number }} target topic and codec of every send
 * @param {number} messages
 * @param {Buffer[]} values
 * @param {number} [concurrency]
 * @param {number} [partitions] when given, message `n` goes to partition `n % partitions`
 * @returns {Promise<void>}
 */
const sendAll = async (producer, target, messages, values, concurrency = 1, partitions) => {
  let next = 0
  const lane = async () => {
    while (next < messages) {
      const from = next
      next += Math.min(BATCH, messages - from)
      await producer.send({
        ...target,
        messages: messagesOf(values, from, next - from, partitions),
      })
    }
  }
  await Promise.all(Array.from({ length: concurrency }, lane))
}

/**
 * Runs `work` with a connected producer and always disconnects it.
 * @param {object} kafka
 * @param {(producer: object) => Promise<*>} work
 * @param {object} [config] the producer's configuration
 * @returns {Promise<*>} what `work` returns
 */
const withProducer = async (kafka, work, config = {}) => {
  const producer = kafka.producer(config)
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
 * @param {{ messages: number, durationMs: number|null, values: Buffer[],
 *   producerSettings: object }} options
 * @param {Timing} timing
 * @returns {Promise<object>} p50, p90, p99, messagesPerSecond, measuredMessages and elapsedMs
 */
const produceLatency = (
  kafka,
  target,
  { messages, durationMs, values, producerSettings },
  { now }
) =>
  withProducer(
    kafka,
    async (producer) => {
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
    },
    producerSettings
  )

/**
 * Batched sends of `BATCH` messages, after an untimed warm-up: one send that pays for metadata, or
 * `WARMUP_MS` of batches with `durationMs`. With `durationMs` the measured window is that long;
 * otherwise it is `messages` messages.
 * @param {object} kafka
 * @param {object} target topic and codec of every send
 * @param {{ messages: number, durationMs: number|null, values: Buffer[],
 *   producerSettings: object }} options
 * @param {Timing} timing
 * @returns {Promise<object>} messagesPerSecond, cpuMicrosPerMessage, measuredMessages and elapsedMs
 */
const produceThroughput = (kafka, target, options, timing) =>
  withProducer(
    kafka,
    async (producer) => {
      const { messages, durationMs, values } = options
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
    },
    options.producerSettings
  )

/**
 * Produces the corpus untimed, laid out as `CORPUS_LAYOUT` says, then times `eachMessage` from the end of the warm-up (the first
 * tenth of the corpus) to the last delivery, so group formation and warm-up are outside the
 * window. With `passes` above one, each partition is sought back to its first offset when it
 * delivers its last record, until it has been read `passes` times. Only a partition's next offset in
 * its current pass counts, so a duplicate or out-of-order delivery is not counted and the run ends
 * only once every partition delivered each pass in order. Over the window it also takes
 * process CPU, the JavaScript thread's CPU when the timing has it, major GCs, an event-loop delay
 * histogram and the consumer's Fetch requests (`consumer.events.REQUEST`, whose `size` is the
 * response's bytes). Gives up after `CONSUME_TIMEOUT_MS`.
 * @param {object} kafka
 * @param {object} target topic and codec of the corpus sends
 * @param {{ messages: number, values: Buffer[], consumerSettings: object, passes?: number,
 *   partitions: number, admin: object }} options `admin` reads each partition's corpus offsets
 * @param {Timing} timing
 * @returns {Promise<object>} messagesPerSecond, cpuMicrosPerMessage,
 *   mainThreadCpuMicrosPerMessage (absent without `timing.threadCpuUsage`), fullGcCount,
 *   eventLoopDelayP50, eventLoopDelayP99, eventLoopDelayMax (ms), fetchRequests, bytesPerFetch,
 *   messagesPerFetch, fetchVersion, passes, corpus, measuredMessages and elapsedMs
 * @throws {Error} when the corpus is not delivered in time
 */
const consumeThroughput = async (kafka, target, options, timing) => {
  const { messages, values, consumerSettings, passes = 1, partitions, admin } = options
  const { now, setTimeout, clearTimeout, cpuUsage, threadCpuUsage, countFullGcs, eventLoopDelay } =
    timing
  await withProducer(
    kafka,
    (producer) => sendAll(producer, target, messages, values, CORPUS_CONCURRENCY, partitions),
    CORPUS_PRODUCER_SETTINGS
  )
  const parts = new Map()
  for (const { partition, high, low } of await admin.fetchTopicOffsets(target.topic)) {
    if (high !== low) {
      const first = Number(low)
      parts.set(partition, { low, first, last: Number(high) - 1, next: first, laps: 0 })
    }
  }
  const total = messages * passes
  const consumer = kafka.consumer({ groupId: `${target.topic}-group`, ...consumerSettings })
  const warmup = Math.max(1, Math.floor(messages * WARMUP_FRACTION))
  const delay = eventLoopDelay()
  const fetches = { requests: 0, bytes: 0, version: null }
  let measuring = false
  let timer
  let stopGcCount = null
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
    let thread = null
    let fullGcCount
    const delivered = new Promise((resolve, reject) => {
      timer = setTimeout(
        () => reject(new Error(`received ${received} of ${total} messages in time`)),
        CONSUME_TIMEOUT_MS
      )
      consumer
        .run({
          eachMessage: async ({ partition, message }) => {
            const part = parts.get(partition)
            if (!part || part.laps === passes || Number(message.offset) !== part.next) return
            if (part.next === part.last) {
              part.next = part.first
              if (++part.laps < passes)
                consumer.seek({ topic: target.topic, partition, offset: part.low })
            } else {
              part.next++
            }
            if (++received === warmup) {
              measuring = true
              delay.enable()
              cpu = cpuUsage()
              thread = threadCpuUsage ? threadCpuUsage() : null
              stopGcCount = countFullGcs()
              first = now()
            }
            if (received === total) {
              last = now()
              cpu = cpuUsage(cpu)
              if (thread) thread = threadCpuUsage(thread)
              fullGcCount = stopGcCount()
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
    const measuredMessages = total - warmup
    return {
      messagesPerSecond: (measuredMessages / elapsedMs) * 1000,
      cpuMicrosPerMessage: (cpu.user + cpu.system) / measuredMessages,
      ...(thread && {
        mainThreadCpuMicrosPerMessage: (thread.user + thread.system) / measuredMessages,
      }),
      fullGcCount,
      eventLoopDelayP50: delay.percentile(50) / 1e6,
      eventLoopDelayP99: delay.percentile(99) / 1e6,
      eventLoopDelayMax: delay.max / 1e6,
      fetchRequests: fetches.requests,
      bytesPerFetch: fetches.bytes / fetches.requests,
      messagesPerFetch: measuredMessages / fetches.requests,
      fetchVersion: fetches.version,
      passes,
      corpus: CORPUS_LAYOUT,
      measuredMessages,
      elapsedMs,
    }
  } finally {
    measuring = false
    stopGcCount?.()
    delay.disable()
    clearTimeout(timer)
    await consumer.disconnect()
  }
}

/**
 * Waits until the broker answers `topic`'s metadata with a leader for each of its `partitions`,
 * reading it every `LEADER_POLL_MS` through any error (a KRaft broker may not know a topic yet
 * right after CreateTopics).
 * @param {object} admin a connected admin client
 * @param {string} topic
 * @param {number} partitions
 * @param {Timing} timing
 * @returns {Promise<void>}
 * @throws {Error} the last error, or a leaderless answer, once `LEADER_WAIT_MS` have passed
 */
const waitForLeaders = async (admin, topic, partitions, { now, sleep }) => {
  const started = now()
  for (;;) {
    let failure
    try {
      const [metadata] = (await admin.fetchTopicMetadata({ topics: [topic] })).topics
      const led = metadata.partitions.filter(({ leader }) => leader >= 0).length
      if (led === partitions) return
      failure = new Error(`${topic} has leaders for ${led} of ${partitions} partitions`)
    } catch (error) {
      failure = error
    }
    if (Number(now() - started) / 1e6 >= LEADER_WAIT_MS) throw failure
    await sleep(LEADER_POLL_MS)
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
 *   CompressionCodecs?: object, Partitioners?: object, lz4Version?: string|null }} client
 * @param {string} workload one of `WORKLOADS`
 * @param {{ messages: number, durationMs?: number|null, size: number, partitions?: number,
 *   compression?: 'none'|'lz4', settings?: 'defaults'|'same', passes?: number,
 *   brokers: string[] }} options
 * @param {Partial<Timing>} [timing]
 * @returns {Promise<object>} the workload's metrics, with the consumer (consume) or producer
 *   (produce) `settings` in force, and an LZ4 run's `codec`
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
  const same = settings === 'same'
  const consumerSettings = same ? SAME_SETTINGS : {}
  const producerSettings =
    same && !consume
      ? { ...SAME_PRODUCER_SETTINGS, createPartitioner: client.Partitioners.DefaultPartitioner }
      : {}
  const inForce = same
    ? {
        same: consume
          ? SAME_SETTINGS
          : { ...SAME_PRODUCER_SETTINGS, partitioner: 'DefaultPartitioner' },
      }
    : { defaults: (consume ? PACKAGE_DEFAULTS : PRODUCER_DEFAULTS)[client.name] ?? null }
  const clock = { ...DEFAULT_TIMING, ...timing }
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
      })
      await waitForLeaders(admin, topic, partitions, clock)
      const values = valuePool(options.size)
      const run = {
        durationMs: null,
        ...options,
        values,
        partitions,
        consumerSettings,
        producerSettings,
        admin,
      }
      const metrics = await RUNNERS[workload](kafka, target, run, clock)
      return { ...metrics, settings: inForce, ...(codec && { codec }) }
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
  SAME_PRODUCER_SETTINGS,
  PRODUCER_DEFAULTS,
  CORPUS_PRODUCER_SETTINGS,
  CORPUS_LAYOUT,
  countFullGcs,
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error instanceof UsageError ? error.message : error.stack}\n`)
    process.exitCode = error instanceof UsageError ? 2 : 1
  })
}
