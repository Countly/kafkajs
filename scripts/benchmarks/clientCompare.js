/**
 * The same workload through the public API of any KafkaJS-compatible client, so the fork and
 * kafkajs@2.2.4 are measured by identical code. It loads the client only from `--client` with
 * `createRequire` rooted there with a real-path prefix check and uses `Kafka`, `producer().send`,
 * `consumer().run`, `consumer.on` and `CompressionCodecs` only: no fork module and no helper is
 * loaded.
 *
 * Usage: node scripts/benchmarks/clientCompare.js --client <dir> --package <@countly/kafkajs|kafkajs>
 *          --workload <produce-latency|produce-throughput|consume-throughput> [--messages N]
 *          [--duration-ms N] [--size N] [--partitions N] [--compression none|lz4|gzip]
 *          [--settings defaults|same] [--passes N] [--batch N] [--unawaited N] [--keys index]
 *          [--headers N] [--partitioner default|custom] [--value-type buffer|string]
 *          [--fork-linger N] [--fork-idempotent true|false] [--probe on] [--brokers host:port,...]
 * `--client` is an npm install prefix that holds `node_modules/<package>`. Defaults: 20000 messages
 * for `produce-latency`, 200000 for the others, 200 bytes per message, 3 partitions, no
 * compression, each package's own consumer defaults, the three brokers of docker-compose.yml.
 * Shape flags of `produce-throughput`: `--batch N` puts N records in each send (500 by default);
 * `--unawaited N` instead issues N one-record sends without awaiting any, then awaits them all (it
 * excludes `--batch`), so a producer that batches sends over time joins them; `--keys index` gives
 * record `n` of the measured window a key that is `n` written in seven digits, so the partitioner
 * places it by key and every key is as long as every other, and without it records have no key;
 * `--headers N` gives every record, the first send included, N string headers. `--partitioner
 * custom` (with the default settings only) gives both packages the same plain function that places
 * record `n` in partition `n % partitions`. `--value-type string` (both produce workloads) sends
 * each value as a string instead of a Buffer. A produce run reports the shape flags it was given as
 * `shape`. A run with none of them and a `--duration-ms` window sends the first values of the
 * corpus on every send, as the produce rows always did, so their figures stay comparable over time;
 * every other produce-throughput run walks the corpus, the walk starting again at the first record
 * when the measured window starts. `--compression gzip` uses the built-in codec both packages ship
 * (`node:zlib`).
 * `--fork-linger N` and `--fork-idempotent true|false` (produce-throughput only, not with
 * `--settings same`) replace the fork's default `linger` (ms) or `idempotent` and nothing else; the
 * run of kafkajs 2.2.4 ignores them and says so, since it has no linger and is not idempotent by
 * default. The run reports what each arm applied or ignored as `settings.overrides` or
 * `settings.ignoredOverrides`.
 * `--probe on` (produce-throughput only) adds what costs the run something to measure: the time of
 * each `send()` call, the peak memory and the garbage collections of the measured window (a
 * collection counts, with its whole pause, when it started inside the window), and the Produce
 * requests each round (one awaited round of sends) took, counted from the producer's own
 * `producer.network.request` event, which both packages emit and which names no partitions, so the
 * partition batches per request are not counted. A run without it adds no clock read, timer,
 * observer or event listener of its own; the timed window still reads its clock once per round, as
 * it always did. `--partitioner custom` starts its count again at 0 when the measured
 * window starts, like the record numbering.
 * `--duration-ms` (produce workloads only, never with `--messages`) sends for a fixed measured
 * window after a 5-second warm-up that is not measured. A consume run excludes the first tenth of
 * the corpus as its warm-up; `--passes N` (consume only) reads the corpus N times, seeking each
 * partition back to its first offset once it delivered its last record, so a corpus the runner's
 * disk can hold still gives a long enough window. `--settings same` gives either package one
 * explicit consumer configuration (`SAME_SETTINGS`) and, on the produce workloads, one explicit
 * producer configuration (`SAME_PRODUCER_SETTINGS`) instead of its defaults. A run creates its
 * topic with `waitForLeaders: false` and waits for the leaders itself, through unknown-topic
 * answers: both packages wait by default, and kafkajs 2.2.4's wait gives up on the first error
 * other than `LEADER_NOT_AVAILABLE`, which a KRaft broker can give right after the topic is
 * created. `--compression lz4` uses the fork's
 * built-in codec; kafkajs 2.2.4 has none, so for it the run registers an `lz4-napi` frame codec,
 * loaded from this repository's install, through `CompressionCodecs`.
 * Prints one JSON line: { package, version, workload, messages, size, partitions, compression,
 * ...metrics } with the package name and version it loaded, so a caller can reject a result from
 * the wrong client.
 * Metrics: `produce-latency` p50, p90, p99 (ms) of one awaited single-message send and
 * messagesPerSecond; `produce-throughput` messagesPerSecond and cpuMicrosPerMessage of sends of 500
 * messages (or the shape's), and with `--probe on` also, over the measured window,
 * `sendCallMicrosP50` and `P99` (the time from calling `send()` to its return of a promise, with
 * the request already built and the promise not awaited), `peakRssBytes` and `peakArrayBuffers`
 * (the largest of the readings taken every 100 ms and at the window's two ends) and the garbage
 * collections of any kind that started in the window, as a count and a total pause (ms) per
 * 10,000 messages, and `produceRequestsPerRoundP50` and `produceRequestsPerRoundMax` (the median
 * and the most Produce requests completed in one round); `consume-throughput` messagesPerSecond,
 * cpuMicrosPerMessage (user plus system), event-loop delay p50, p99 and max (ms), Fetch requests,
 * bytes and messages per Fetch, all over the measured window, plus the Fetch version, the passes
 * and the LZ4 codec. A consume run also
 * reports mainThreadCpuMicrosPerMessage (the JavaScript thread only, from `process.threadCpuUsage`;
 * absent on a Node without it, which threadCpuUsageSupported says), fullGcCount (major
 * collections that started in the measured window) and the corpus layout. Each also reports elapsedMs, measuredMessages and the producer or consumer
 * settings in force. Exit codes: 0 done, 1 the client or the workload failed, 2 a bad argument.
 *
 * The consume corpus is laid out the same on both arms, whatever `--settings` says: each arm's own
 * client writes it (loading the other package would break the arm's isolation) with
 * `CORPUS_PRODUCER_SETTINGS`, in sends of `BATCH` records where record `i` goes to partition
 * `i % partitions`, so each send leaves one batch per partition and neither package's
 * partitioner or batching defaults shape the log. Before measuring, the run checks that each
 * partition holds exactly the records the layout puts there, so a retried or lost send fails the
 * run; the order of the batches inside a partition is not checked.
 */
const fs = require('node:fs')
const { createRequire } = require('node:module')
const path = require('node:path')
const {
  createHistogram,
  monitorEventLoopDelay,
  performance,
  PerformanceObserver,
  constants,
} = require('node:perf_hooks')
const { setTimeout: sleep, setImmediate: nextTurn } = require('node:timers/promises')

const WORKLOADS = ['produce-latency', 'produce-throughput', 'consume-throughput']
const DEFAULT_MESSAGES = {
  'produce-latency': 20000,
  'produce-throughput': 200000,
  'consume-throughput': 200000,
}
const DEFAULT_BROKERS = 'localhost:9092,localhost:9095,localhost:9098'
const DEFAULT_SIZE = 200
const DEFAULT_PARTITIONS = 3
const COMPRESSIONS = ['none', 'lz4', 'gzip']
const SETTINGS = ['defaults', 'same']
const VALUE_TYPES = ['buffer', 'string']
const KEY_MODES = ['index']
const PARTITIONERS = ['default', 'custom']
const PROBE_MODES = ['on']
const IDEMPOTENT_MODES = ['true', 'false']
/** The package name of the fork, the only arm the `--fork-*` flags apply to. */
const FORK_PACKAGE = '@countly/kafkajs'
/** Digits of a `--keys index` key, so every key has the same length. */
const KEY_DIGITS = 7
/** The `CompressionTypes` member each compression names, for the codecs a send can ask for. */
const COMPRESSION_TYPE = { lz4: 'LZ4', gzip: 'GZIP' }
const BATCH = 500
const MEMORY_SAMPLE_MS = 100
const CORPUS_CONCURRENCY = 8
const WARMUP_MS = 5000
const WARMUP_FRACTION = 0.1
const POOL_SIZE = 1024
const POOL_BYTES = 64 * 1024 * 1024
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
  '--batch',
  '--unawaited',
  '--keys',
  '--headers',
  '--partitioner',
  '--value-type',
  '--fork-linger',
  '--fork-idempotent',
  '--probe',
  '--brokers',
]
/** The workloads each optional flag applies to; the flag is a usage error on any other. */
const APPLIES_TO = {
  '--batch': ['produce-throughput'],
  '--unawaited': ['produce-throughput'],
  '--keys': ['produce-throughput'],
  '--headers': ['produce-throughput'],
  '--partitioner': ['produce-throughput'],
  '--value-type': ['produce-latency', 'produce-throughput'],
  '--fork-linger': ['produce-throughput'],
  '--fork-idempotent': ['produce-throughput'],
  '--probe': ['produce-throughput'],
}
const USAGE =
  'usage: node scripts/benchmarks/clientCompare.js --client <dir> --package <name> --workload <produce-latency|produce-throughput|consume-throughput> [--messages N] [--duration-ms N] [--size N] [--partitions N] [--compression none|lz4|gzip] [--settings defaults|same] [--passes N] [--batch N] [--unawaited N] [--keys index] [--headers N] [--partitioner default|custom] [--value-type buffer|string] [--fork-linger N] [--fork-idempotent true|false] [--probe on] [--brokers host:port,...]'

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
 * @param {string|undefined} text
 * @returns {number|undefined} the integer (0 included) `text` holds, undefined when absent
 * @throws {UsageError} when present and not written as a non-negative integer
 */
const nonNegative = (text) => {
  if (text === undefined) return undefined
  if (!/^\d+$/.test(text)) throw new UsageError(USAGE)
  return Number(text)
}

/**
 * @param {string[]} argv
 * @returns {{ client: string, package: string, workload: string, messages: number|null,
 *   durationMs: number|null, size: number, partitions: number, compression: 'none'|'lz4'|'gzip',
 *   settings: 'defaults'|'same', passes: number, batch?: number, unawaited?: number,
 *   keys?: 'index', headers?: number, partitioner?: 'default'|'custom',
 *   valueType?: 'buffer'|'string', forkLinger?: number, forkIdempotent?: boolean, probe?: 'on',
 *   brokers: string[] }} the optional fields are absent unless their flag was given
 * @throws {UsageError} for a missing, repeated or unknown flag, an unknown workload, compression,
 *   settings, key mode, partitioner, value type, idempotence or probe mode, a count that is not a
 *   positive integer (a linger that is not a non-negative one), `--duration-ms` with `--messages`,
 *   a flag on a workload it does not apply to, `--batch` with `--unawaited`, or `--partitioner
 *   custom`, `--fork-linger` or `--fork-idempotent` with `--settings same`
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
  const valueType = flags['--value-type']
  const keys = flags['--keys']
  const partitioner = flags['--partitioner']
  const probe = flags['--probe']
  const idempotent = flags['--fork-idempotent']
  const oneOf = (choices, value) => value === undefined || choices.includes(value)
  const known =
    WORKLOADS.includes(workload) &&
    COMPRESSIONS.includes(compression) &&
    SETTINGS.includes(settings) &&
    oneOf(VALUE_TYPES, valueType) &&
    oneOf(KEY_MODES, keys) &&
    oneOf(PARTITIONERS, partitioner) &&
    oneOf(IDEMPOTENT_MODES, idempotent) &&
    oneOf(PROBE_MODES, probe)
  if (!flags['--client'] || !flags['--package'] || !known) throw new UsageError(USAGE)
  const durationMs = positive(flags['--duration-ms']) ?? null
  const consume = workload === 'consume-throughput'
  const passes = positive(flags['--passes']) ?? 1
  const misplaced = consume ? durationMs !== null : flags['--passes'] !== undefined
  const misapplied = Object.entries(APPLIES_TO).some(
    ([flag, workloads]) => flags[flag] !== undefined && !workloads.includes(workload)
  )
  const forkFlagged = flags['--fork-linger'] !== undefined || idempotent !== undefined
  const conflicting =
    (flags['--batch'] !== undefined && flags['--unawaited'] !== undefined) ||
    (partitioner === 'custom' && settings === 'same') ||
    (forkFlagged && settings === 'same')
  if (
    misplaced ||
    misapplied ||
    conflicting ||
    (durationMs !== null && flags['--messages'] !== undefined)
  ) {
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
    batch: positive(flags['--batch']),
    unawaited: positive(flags['--unawaited']),
    keys,
    headers: positive(flags['--headers']),
    partitioner,
    valueType,
    forkLinger: nonNegative(flags['--fork-linger']),
    forkIdempotent: idempotent === undefined ? undefined : idempotent === 'true',
    probe,
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
 * @param {Parameters<typeof useLz4>[0]} client
 * @param {'none'|'lz4'|'gzip'} compression
 * @returns {object|undefined} what the run compresses with, for its report: the LZ4 codec's
 *   package (see `useLz4`), or for gzip the `node:zlib` both packages ship, which needs nothing
 *   registered; undefined without compression
 */
const codecOf = (client, compression) => {
  if (compression === 'lz4') return useLz4(client)
  if (compression === 'gzip') {
    return { name: 'gzip', codec: 'built-in', package: 'zlib', version: process.versions.zlib }
  }
  return undefined
}

/**
 * @param {number[]} sorted values in ascending order
 * @param {number} p fraction between 0 and 1
 * @returns {number} the value at that fraction of the list
 */
const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]

/** @returns {bigint} a monotonic clock reading in nanoseconds */
const now = () => process.hrtime.bigint()

/** Lets Node publish the `gc` entries of collections that already ended: two event-loop turns. */
const drainGcEntries = async () => {
  await nextTurn()
  await nextTurn()
}

/**
 * Starts counting major (full) garbage collections through a `gc` PerformanceObserver, by the
 * entries' start on the performance clock: a collection counts when it started between this call
 * and `stop()`, even if Node delivers its entry later, during `count()`'s drain.
 * @param {{ Observer?: Function, now?: () => number, drain?: () => Promise<void> }} [seams] the
 *   observer class, the performance clock and the wait for late entries, replaced in tests
 * @returns {{ stop: () => void, count: () => Promise<number>, close: () => void }} `stop` ends the
 *   window (the first call counts); `count` drains late entries, closes the observer and returns
 *   the window's count; `close` disconnects the observer once, without counting
 */
const countFullGcs = ({
  Observer = PerformanceObserver,
  now = () => performance.now(),
  drain = drainGcEntries,
} = {}) => {
  const starts = []
  const keep = (entries) => {
    for (const { startTime, detail } of entries) {
      if (detail?.kind === constants.NODE_PERFORMANCE_GC_MAJOR) starts.push(startTime)
    }
  }
  const observer = new Observer((list) => keep(list.getEntries()))
  observer.observe({ entryTypes: ['gc'] })
  const from = now()
  let to = null
  let closed = false
  const close = () => {
    if (closed) return
    closed = true
    observer.disconnect()
  }
  return {
    stop: () => {
      to ??= now()
    },
    count: async () => {
      await drain()
      if (!closed) keep(observer.takeRecords())
      close()
      return starts.filter((start) => start >= from && start <= to).length
    },
    close,
  }
}

/**
 * Registers a `gc` PerformanceObserver that records every garbage collection (any kind) inside a
 * window the caller marks with the readings of its own monotonic clock: `open(at)` and `stop(at)`
 * take the `process.hrtime.bigint()` readings that start and end the rate's window, and a
 * collection counts, with its whole pause, when it started between them (entries carry their start
 * on the performance clock, which this converts with one offset read at registration), even if
 * Node delivers its entry later, during `result()`'s drain. Entries delivered before `open` all
 * started before it, since the marks are taken in the same synchronous step as the call, and are
 * ignored. Only the count and the total pause are kept, so a window with many collections holds no
 * entries. Register before the window and open it at its first reading.
 * @param {{ Observer?: Function, now?: () => number, hrtime?: () => bigint,
 *   drain?: () => Promise<void> }} [seams] the observer class, the performance clock, the
 *   monotonic clock the marks come from and the wait for late entries, replaced in tests
 * @returns {{ open: (at: bigint) => void, stop: (at: bigint) => void,
 *   result: () => Promise<{ count: number, pauseMs: number }>, close: () => void }} `open` and
 *   `stop` mark the window's two ends (the first call of each counts); `result` drains late
 *   entries, closes the observer and returns the window's collections and their total duration in
 *   ms (all collections after `open` when `stop` was not called); `close` disconnects the observer
 *   once, without a result
 */
const trackGcs = ({
  Observer = PerformanceObserver,
  now: performanceNow = () => performance.now(),
  hrtime = now,
  drain = drainGcEntries,
} = {}) => {
  let from = null
  let to = null
  let count = 0
  let pauseMs = 0
  const keep = (entries) => {
    for (const { startTime, duration } of entries) {
      if (from !== null && startTime >= from && (to === null || startTime <= to)) {
        count++
        pauseMs += duration
      }
    }
  }
  const observer = new Observer((list) => keep(list.getEntries()))
  observer.observe({ entryTypes: ['gc'] })
  const offsetMs = performanceNow() - Number(hrtime()) / 1e6
  const toPerformance = (at) => Number(at) / 1e6 + offsetMs
  let closed = false
  const close = () => {
    if (closed) return
    closed = true
    observer.disconnect()
  }
  return {
    open: (at) => {
      from ??= toPerformance(at)
    },
    stop: (at) => {
      to ??= toPerformance(at)
    },
    result: async () => {
      await drain()
      if (!closed) keep(observer.takeRecords())
      close()
      return { count, pauseMs }
    },
    close,
  }
}

/**
 * @typedef {{ now: () => bigint, setTimeout: Function, clearTimeout: Function,
 *   setInterval: Function, clearInterval: Function,
 *   sleep: (ms: number) => Promise<void>,
 *   cpuUsage: (previous?: object) => { user: number, system: number },
 *   threadCpuUsage: ((previous?: object) => { user: number, system: number })|null,
 *   memoryUsage: () => { rss: number, arrayBuffers: number },
 *   countFullGcs: () => { stop: () => void, count: () => Promise<number>, close: () => void },
 *   trackGcs: () => { open: (at: bigint) => void, stop: (at: bigint) => void,
 *     result: () => Promise<{ count: number, pauseMs: number }>, close: () => void },
 *   eventLoopDelay: () => { enable: Function, disable: Function, percentile: (p: number) => number,
 *     max: number } }} Timing clocks, timers, process CPU, the JavaScript thread's CPU (null on a
 *   Node without `process.threadCpuUsage`), the resident and `ArrayBuffer` memory in bytes, a
 *   full-GC counter, an any-GC recorder and an event-loop delay histogram (ns)
 */
const DEFAULT_TIMING = {
  now,
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval,
  sleep: (ms) => sleep(ms),
  memoryUsage: () => {
    const { rss, arrayBuffers } = process.memoryUsage()
    return { rss, arrayBuffers }
  },
  trackGcs,
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
 * Corpus values: `POOL_SIZE` distinct records of `size` bytes (fewer when that would hold more than
 * `POOL_BYTES`, so a large record size does not fill the process with its corpus), words from a
 * fixed vocabulary in a seeded order, so a compressed batch holds text that compresses the way logs
 * do rather than one repeated byte.
 * @param {number} size bytes per value
 * @returns {Buffer[]}
 */
const valuePool = (size) => {
  const words = ['event', 'user', 'session', 'view', 'click', 'app', 'id', 'ts', 'ok', 'error']
  const count = Math.min(POOL_SIZE, Math.max(1, Math.floor(POOL_BYTES / size)))
  let seed = 1
  return Array.from({ length: count }, (_, i) => {
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
 * @param {number} count
 * @returns {Record<string, string>} `count` string headers, `h0: 'v0'` to `h<count - 1>`
 */
const headerPool = (count) =>
  Object.fromEntries(Array.from({ length: count }, (_, i) => [`h${i}`, `v${i}`]))

/**
 * @param {number} n a message's number in the window
 * @returns {string} `n` in `KEY_DIGITS` digits (counting again from 0 after 10^`KEY_DIGITS`
 *   messages), so every key is as long as every other
 */
const keyOf = (n) => String(n % 10 ** KEY_DIGITS).padStart(KEY_DIGITS, '0')

/**
 * @param {(Buffer|string)[]} values the corpus values
 * @param {number} from number of the first message in the window
 * @param {number} count
 * @param {{ keyed: boolean, headers?: Record<string, string> }} shape whether message `n` carries
 *   the key `keyOf(n)`, and the headers every message carries
 * @returns {{ value: Buffer|string, key?: string, headers?: object }[]} `count` messages cycling
 *   through `values`
 */
const shapedMessagesOf = (values, from, count, { keyed, headers }) =>
  Array.from({ length: count }, (_, i) => {
    const n = from + i
    const message = { value: values[n % values.length] }
    if (keyed) message.key = keyOf(n)
    if (headers) message.headers = headers
    return message
  })

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
 * What a `--probe on` run measures beside its rate: the time of each `send()` call, the peak
 * memory, and the garbage collections, all over one window. `start()` and `stop()` are called
 * next to the reads of the rate clock, so the window is the one the rate is measured over; the
 * collection observer takes its start last in `start()` and its end first in `stop()`. Nothing is
 * kept per call (the times go into a fixed-size histogram) and `close()` releases the sampling
 * timer and the collection observer when a run fails before `finish()`. It also counts the Produce
 * requests the producer completed in each round (`watch`, `beginRound`, `endRound`), keeping one
 * counter per distinct count rather than one entry per round.
 */
class ProduceProbe {
  #timing
  #calls = createHistogram()
  #measuring = false
  #peakRss = 0
  #peakArrayBuffers = 0
  #timer = null
  #gcs = null
  #requests = 0
  #roundFrom = null
  /** @type {Map<number, number>} rounds by the number of Produce requests they took */
  #rounds = new Map()

  /** @param {Timing} timing the clocks, memory reading, interval timers and GC recorder */
  constructor(timing) {
    this.#timing = timing
  }

  /** @returns {boolean} whether the window is open: only then is a call worth timing */
  get measuring() {
    return this.#measuring
  }

  /**
   * Starts counting the Produce requests the producer completes (its `request` instrumentation
   * event, which kafkajs 2.2.4 and the fork both emit) while the window is open.
   * @param {{ on: Function, events: { REQUEST: string } }} producer
   */
  watch(producer) {
    producer.on(producer.events.REQUEST, ({ payload }) => {
      if (this.#measuring && payload.apiName === 'Produce') this.#requests++
    })
  }

  /** Marks the start of a round of sends; nothing counts outside an open window. */
  beginRound() {
    this.#roundFrom = this.#measuring ? this.#requests : null
  }

  /** Closes the round `beginRound` opened and files the Produce requests it completed. */
  endRound() {
    if (this.#roundFrom === null) return
    const requests = this.#requests - this.#roundFrom
    this.#roundFrom = null
    this.#rounds.set(requests, (this.#rounds.get(requests) ?? 0) + 1)
  }

  /**
   * Sets up what the window needs, before it: reads the memory now and every `MEMORY_SAMPLE_MS`
   * and registers the collection observer. No call is timed and no collection counts until `open`.
   */
  start() {
    this.#sample()
    this.#timer = this.#timing.setInterval(() => this.#sample(), MEMORY_SAMPLE_MS)
    this.#gcs = this.#timing.trackGcs()
  }

  /** @param {bigint} at the reading of the rate's clock that starts the rate's window */
  open(at) {
    this.#gcs.open(at)
    this.#measuring = true
  }

  /** @param {bigint} ns how long one `send()` call ran before it returned its promise */
  recordCall(ns) {
    this.#calls.record(Math.max(1, Number(ns)))
  }

  /**
   * Closes the window at the reading of the rate's clock that ends the rate's window: no call is
   * timed after this, and a collection that starts later is not counted.
   * @param {bigint} at
   */
  stop(at) {
    this.#measuring = false
    this.#gcs.stop(at)
  }

  /**
   * Reads the memory once more, stops the sampling and waits for the window's late collection
   * entries; the window itself ended at `stop`.
   * @param {number} messages the messages sent in the window
   * @returns {Promise<{ sendCallMicrosP50: number, sendCallMicrosP99: number, peakRssBytes: number,
   *   peakArrayBuffers: number, gcCountPer10kMessages: number, gcPauseMsPer10kMessages: number,
   *   produceRequestsPerRoundP50?: number, produceRequestsPerRoundMax?: number }>} the two request
   *   figures are absent when no round was counted
   */
  async finish(messages) {
    this.#stopSampling()
    this.#sample()
    const { count, pauseMs } = await this.#gcs.result()
    const perRound = this.#requestsPerRound()
    return {
      sendCallMicrosP50: this.#calls.percentile(50) / 1000,
      sendCallMicrosP99: this.#calls.percentile(99) / 1000,
      peakRssBytes: this.#peakRss,
      peakArrayBuffers: this.#peakArrayBuffers,
      gcCountPer10kMessages: (count * 10000) / messages,
      gcPauseMsPer10kMessages: (pauseMs * 10000) / messages,
      ...perRound,
    }
  }

  /** Stops sampling and recording without a result. */
  close() {
    this.#measuring = false
    this.#stopSampling()
    this.#gcs?.close()
  }

  #stopSampling() {
    if (this.#timer === null) return
    this.#timing.clearInterval(this.#timer)
    this.#timer = null
  }

  /**
   * @returns {{ produceRequestsPerRoundP50?: number, produceRequestsPerRoundMax?: number }} the
   *   median (the value at half the rounds in ascending order, as `percentile` takes it) and the
   *   largest Produce request count of a round; empty when no round was counted
   */
  #requestsPerRound() {
    const counts = [...this.#rounds].sort(([a], [b]) => a - b)
    if (counts.length === 0) return {}
    const middle = Math.floor(counts.reduce((sum, [, rounds]) => sum + rounds, 0) / 2)
    let seen = 0
    let median
    for (const [requests, rounds] of counts) {
      seen += rounds
      if (seen > middle) {
        median = requests
        break
      }
    }
    return {
      produceRequestsPerRoundP50: median,
      produceRequestsPerRoundMax: counts[counts.length - 1][0],
    }
  }

  #sample() {
    const { rss, arrayBuffers } = this.#timing.memoryUsage()
    this.#peakRss = Math.max(this.#peakRss, rss)
    this.#peakArrayBuffers = Math.max(this.#peakArrayBuffers, arrayBuffers)
  }
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
 * Batched sends of `batch` messages (`BATCH` by default) one after the other, or with `unawaited`
 * rounds of that many one-record sends issued without awaiting any and then all awaited, after an
 * untimed warm-up: one send that pays for metadata, or `WARMUP_MS` of batches with `durationMs`.
 * With `durationMs` the measured window is that long; otherwise it is `messages` messages. With
 * `keys` message `n` of the window carries the key `keyOf(n)`, and with `headers` every message,
 * the first send included, carries that many string headers; the numbering starts at 0 when the
 * window starts, so both packages send the same records in the window whatever their warm-up
 * managed. A run with none of the shape flags and a `durationMs` window sends the first values of
 * the corpus on every send, as it always did; any other run walks the corpus. With `probe` the
 * window also reports the time of each `send()` call, the peak memory, the garbage collections and
 * the Produce requests each round took, over the window the rate is measured over, whose two clock
 * readings the probe is given (`ProduceProbe`); without it the run adds no clock read, timer,
 * observer or event listener of its own. With
 * `routing` the custom partitioner's count starts again at 0 when the window starts, like the
 * record numbering.
 * @param {object} kafka
 * @param {object} target topic and codec of every send
 * @param {{ messages: number, durationMs: number|null, values: (Buffer|string)[],
 *   producerSettings: object, batch?: number, unawaited?: number, keys?: 'index',
 *   headers?: number, partitioner?: 'default'|'custom', valueType?: 'buffer'|'string',
 *   probe?: 'on', routing?: { reset: () => void } }} options
 * @param {Timing} timing
 * @returns {Promise<object>} messagesPerSecond, cpuMicrosPerMessage, measuredMessages, elapsedMs
 *   and, with `probe`, the probe's metrics
 */
const produceThroughput = (kafka, target, options, timing) =>
  withProducer(
    kafka,
    async (producer) => {
      const { messages, durationMs, values, batch = BATCH, unawaited } = options
      const { now, cpuUsage } = timing
      const shape = {
        keyed: options.keys !== undefined,
        headers: options.headers === undefined ? undefined : headerPool(options.headers),
      }
      const shaped = shape.keyed || shape.headers !== undefined
      const flagged = [
        options.batch,
        options.unawaited,
        options.keys,
        options.headers,
        options.partitioner,
        options.valueType,
      ].some((flag) => flag !== undefined)
      const repeatsFirstValues = durationMs !== null && !flagged
      let issued = 0
      const nextMessages = (count) => {
        const from = issued
        issued += count
        return shaped
          ? shapedMessagesOf(values, from, count, shape)
          : messagesOf(values, repeatsFirstValues ? 0 : from, count)
      }
      const probe = options.probe === undefined ? null : new ProduceProbe(timing)
      const call = (count) => {
        const request = { ...target, messages: nextMessages(count) }
        if (!probe?.measuring) return producer.send(request)
        const calledAt = now()
        const sending = producer.send(request)
        probe.recordCall(now() - calledAt)
        return sending
      }
      const perRound = unawaited ?? batch
      const send = async (limit = Infinity) => {
        const count = Math.min(perRound, limit)
        probe?.beginRound()
        if (unawaited) await Promise.all(Array.from({ length: count }, () => call(1)))
        else await call(count)
        probe?.endRound()
        return count
      }
      try {
        probe?.watch(producer)
        await call(1)
        if (durationMs !== null) await sendFor(now, now(), WARMUP_MS, send)
        issued = 0
        options.routing?.reset()
        probe?.start()
        const started = now()
        probe?.open(started)
        const cpu = cpuUsage()
        let measuredMessages = messages
        if (durationMs !== null) {
          measuredMessages = await sendFor(now, started, durationMs, send)
        } else {
          let left = messages
          while (left > 0) left -= await send(left)
        }
        const ended = now()
        probe?.stop(ended)
        const elapsedMs = Number(ended - started) / 1e6
        const { user, system } = cpuUsage(cpu)
        return {
          messagesPerSecond: (measuredMessages / elapsedMs) * 1000,
          cpuMicrosPerMessage: (user + system) / measuredMessages,
          measuredMessages,
          elapsedMs,
          ...(await probe?.finish(measuredMessages)),
        }
      } finally {
        probe?.close()
      }
    },
    options.producerSettings
  )

/**
 * Produces the corpus untimed, laid out as `CORPUS_LAYOUT` says, checks each partition's record
 * count against that layout, then times `eachMessage` from the end of the warm-up (the first
 * tenth of the corpus) to the last delivery, so group formation and warm-up are outside the
 * window. With `passes` above one, each partition is sought back to its first offset when it
 * delivers its last record, until it has been read `passes` times. Only a partition's next offset in
 * its current pass counts, so a duplicate or out-of-order delivery is not counted and the run ends
 * only once every partition delivered each pass in order. Over the window it also takes
 * process CPU, the JavaScript thread's CPU when the timing has it, major GCs, an event-loop delay
 * histogram and the consumer's Fetch requests (`consumer.events.REQUEST`, whose `size` is the
 * response's bytes). The full-GC count waits for late `gc` entries after the window has closed.
 * Gives up after `CONSUME_TIMEOUT_MS`.
 * @param {object} kafka
 * @param {object} target topic and codec of the corpus sends
 * @param {{ messages: number, values: Buffer[], consumerSettings: object, passes?: number,
 *   partitions: number, admin: object }} options `admin` reads each partition's corpus offsets
 * @param {Timing} timing
 * @returns {Promise<object>} messagesPerSecond, cpuMicrosPerMessage,
 *   threadCpuUsageSupported, mainThreadCpuMicrosPerMessage (absent without
 *   `timing.threadCpuUsage`), fullGcCount,
 *   eventLoopDelayP50, eventLoopDelayP99, eventLoopDelayMax (ms), fetchRequests, bytesPerFetch,
 *   messagesPerFetch, fetchVersion, passes, corpus, measuredMessages and elapsedMs
 * @throws {Error} when a partition does not hold the records the layout puts there, or the corpus
 *   is not delivered in time
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
  const answered = await admin.fetchTopicOffsets(target.topic)
  for (let partition = 0; partition < partitions; partition++) {
    const offsets = answered.find((answer) => answer.partition === partition)
    const held = offsets ? Number(offsets.high) - Number(offsets.low) : 0
    const laid = Math.max(0, Math.ceil((messages - partition) / partitions))
    if (held !== laid) {
      throw new Error(
        `corpus partition ${partition} holds ${held} records where its layout puts ${laid}: a send was retried or lost, so the arms would not read the same corpus`
      )
    }
  }
  for (const { partition, high, low } of answered) {
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
  let gc = null
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
              gc = countFullGcs()
              delay.enable()
              cpu = cpuUsage()
              thread = threadCpuUsage ? threadCpuUsage() : null
              first = now()
            }
            if (received === total) {
              last = now()
              cpu = cpuUsage(cpu)
              if (thread) thread = threadCpuUsage(thread)
              gc.stop()
              delay.disable()
              measuring = false
              resolve()
            }
          },
        })
        .catch(reject)
    })
    await delivered
    const fullGcCount = await gc.count()
    const elapsedMs = Number(last - first) / 1e6
    const measuredMessages = total - warmup
    return {
      messagesPerSecond: (measuredMessages / elapsedMs) * 1000,
      cpuMicrosPerMessage: (cpu.user + cpu.system) / measuredMessages,
      threadCpuUsageSupported: thread !== null,
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
    gc?.close()
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

/**
 * The `--partitioner custom` routing, a plain function both packages call the same way, with one
 * count shared by every partitioner it creates, which the run resets when its window starts so the
 * routing of the window does not depend on how much the warm-up sent.
 * @returns {{ createPartitioner: () => ({ partitionMetadata }: { partitionMetadata: object[] })
 *   => number, reset: () => void }} `createPartitioner` gives a partitioner that places the `n`th
 *   record any of them was asked about since the last `reset` in partition `n % partitions`
 */
const customRouting = () => {
  let n = 0
  const place = ({ partitionMetadata }) => n++ % partitionMetadata.length
  return {
    createPartitioner: () => place,
    reset: () => {
      n = 0
    },
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
 *   compression?: 'none'|'lz4'|'gzip', settings?: 'defaults'|'same', passes?: number,
 *   batch?: number, unawaited?: number, keys?: 'index', headers?: number,
 *   partitioner?: 'default'|'custom', valueType?: 'buffer'|'string', forkLinger?: number,
 *   forkIdempotent?: boolean, probe?: 'on', brokers: string[] }} options `forkLinger` and
 *   `forkIdempotent` become the producer's `linger` and `idempotent` on the fork (client name
 *   `@countly/kafkajs`) only
 * @param {Partial<Timing>} [timing]
 * @returns {Promise<object>} the workload's metrics, with the consumer (consume) or producer
 *   (produce) `settings` in force (the fork's `forkLinger` and `forkIdempotent` as
 *   `settings.overrides`, any other package's as `settings.ignoredOverrides`), an LZ4 or gzip
 *   run's `codec`, and, when any of `batch`,
 *   `unawaited`, `keys`, `headers`, `partitioner` or `valueType` was given, those as `shape`
 * @throws {Error} for an unknown workload, or whatever the client throws
 */
const runWorkload = async (client, workload, options, timing = {}) => {
  if (!RUNNERS[workload]) throw new Error(`unknown workload ${workload}`)
  const { Kafka, logLevel } = client
  const { partitions = DEFAULT_PARTITIONS, compression = 'none', settings = 'defaults' } = options
  const { batch, unawaited, keys, headers, partitioner, valueType } = options
  const consume = workload === 'consume-throughput'
  const codec = codecOf(client, compression)
  const kafka = new Kafka({
    clientId: `bench-compare-${workload}`,
    brokers: options.brokers,
    logLevel: logLevel.NOTHING,
  })
  const topic = `bench-compare-${workload}-${process.pid}-${Date.now()}`
  const target =
    compression === 'none'
      ? { topic }
      : { topic, compression: client.CompressionTypes[COMPRESSION_TYPE[compression]] }
  const same = settings === 'same'
  const consumerSettings = same ? SAME_SETTINGS : {}
  const routing = partitioner === 'custom' ? customRouting() : undefined
  const requested = Object.fromEntries(
    Object.entries({ linger: options.forkLinger, idempotent: options.forkIdempotent }).filter(
      ([, value]) => value !== undefined
    )
  )
  const forked = client.name === FORK_PACKAGE
  const producerSettings = {
    ...(same &&
      !consume && {
        ...SAME_PRODUCER_SETTINGS,
        createPartitioner: client.Partitioners.DefaultPartitioner,
      }),
    ...(routing && { createPartitioner: routing.createPartitioner }),
    ...(forked && requested),
  }
  const shape = Object.fromEntries(
    Object.entries({ batch, unawaited, keys, headers, partitioner, valueType }).filter(
      ([, value]) => value !== undefined
    )
  )
  const inForce = same
    ? {
        same: consume
          ? SAME_SETTINGS
          : { ...SAME_PRODUCER_SETTINGS, partitioner: 'DefaultPartitioner' },
      }
    : {
        defaults: (consume ? PACKAGE_DEFAULTS : PRODUCER_DEFAULTS)[client.name] ?? null,
        ...(Object.keys(requested).length > 0 && {
          [forked ? 'overrides' : 'ignoredOverrides']: requested,
        }),
      }
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
        waitForLeaders: false,
      })
      await waitForLeaders(admin, topic, partitions, clock)
      const pool = valuePool(options.size)
      const values = valueType === 'string' ? pool.map((value) => value.toString()) : pool
      const run = {
        durationMs: null,
        ...options,
        values,
        partitions,
        consumerSettings,
        producerSettings,
        routing,
        admin,
      }
      const metrics = await RUNNERS[workload](kafka, target, run, clock)
      return {
        ...metrics,
        settings: inForce,
        ...(codec && { codec }),
        ...(Object.keys(shape).length > 0 && { shape }),
      }
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
  trackGcs,
  customRouting,
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error instanceof UsageError ? error.message : error.stack}\n`)
    process.exitCode = error instanceof UsageError ? 2 : 1
  })
}
