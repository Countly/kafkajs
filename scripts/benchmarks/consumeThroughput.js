/**
 * Consume-throughput probe: produces a fixed corpus, consumes it, and reports messages/sec plus
 * handling latency and the gap between consecutive handler entries.
 * Usage: node [--expose-gc] scripts/benchmarks/consumeThroughput.js [--messages 200000]
 *          [--size 200] [--partitions 6] [--timeout <ms>] [--handler eachBatch|eachMessage]
 *          [--subscribe-channels 0|1] [--max-bytes-per-partition <n>]
 *          [--autocommit-threshold <n>] [--otel 0|1] [--intern-offsets 0|1] [--control 0|1]
 *          [--listener 0|1]
 * Prints one JSON line: { messages, size, partitions, handler, subscribeChannels,
 *   messagesPerSecond, p50, p90, p99, handlerP99, deliveryGapP50, deliveryGapP90, deliveryGapP99,
 *   batches, deliveredMessages, deliveredBatches, handlerEntries, gapSamples, deliveredBytes,
 *   heapRetainedBytes }
 *
 * `deliveryGap*` is the time between consecutive handler entries, so decode, queue wait, runner
 * work and a commit between two entries are inside it. `p50`, `p90`, `p99` and `handlerP99` (the
 * same figure as `p99`) time the handler body only and are never a client-path tail. `batches` is
 * the `eachBatch` calls, 0 for `eachMessage`. `heapRetainedBytes` is the heap in use after the
 * measured run's consumer is gone and two collections; it needs `--expose-gc`.
 *
 * `--max-bytes-per-partition` sets the consumer option: 32768 gives the `eachBatch` arm about
 * 1 380 batches from 200 000 messages of 200 bytes, where the 1 MiB default gives about 45.
 * `--listener 1` registers a rebalance listener with an assigned and a revoked callback on the same
 * `run` call. It counts the initial assignment and the graceful-teardown revoke, outside the timed
 * interval, and fails the run unless each happened once; the eager startup revoke does not count.
 * `--autocommit-threshold n` turns `autoCommit` on with that threshold. `--otel 1` registers the
 * instrumentation of this tree's `otel/` with a tracer and a meter provider that export nothing.
 * `--intern-offsets 1` keeps every offset string in a Map: a deliberate leak, the control that
 * shows the heap reading sees a regression of that kind.
 *
 * Declared before any run (session.js, medians of seven, the first run of each arm discarded, an
 * A/A arm, a quiet host): messages per second within -3 % of the previous tree's; `deliveryGapP99`
 * within +5 %, gated only with 200 000 actual entry gaps (`eachMessage`) or at least 1 000
 * actual entry gaps (`eachBatch`), else inconclusive; `--otel 1` within +3 % of the previous tree's own `otel/`;
 * `heapRetainedBytes` within +2 MiB, and `--intern-offsets 1` must exceed it: an arm that does not
 * fail means the measurement is blind and the session is invalid.
 *
 * `--control 1` is not a measurement. It verifies finite record, callback and gap totals through
 * the same handlers, then runs the measured consume once with
 * `OffsetOperations#begin` counted from the first delivered batch on and prints `{ control: true,
 * accounting, observed, arm, beginCalls, expected, pass }`, exit code 1 unless the count is what the arm should give:
 * none without `--autocommit-threshold`, some with it. It loads this tree's own module, so it
 * refuses on a tree without it, and a timed run never loads it.
 *
 * The corpus is fixed on purpose: 200000 messages of 200 B across one topic of 6 partitions, no
 * compression, `autoCommit: false`, one discarded warm-up run, and a fresh consumer group per run.
 * `--handler eachBatch` (the default) uses
 * `eachBatchAutoResolve` and times each batch; `eachMessage` times each record. A run that never
 * receives the whole corpus fails on the deadline instead of hanging: `--timeout` bounds the drain,
 * from `run` until the last message, and startup is bounded by the client's connection and request
 * timeouts.
 *
 * `--subscribe-channels 1` (this tree only) subscribes a function that does nothing to the
 * diagnostics channel of every public event, so the run measures what subscribing costs.
 */
const path = require('path')
const { createRequire } = require('module')
const { Kafka, logLevel } = require('../../index')
const { Clients } = require('../clients')
const subscribeChannels = require('./subscribeChannels')
const { DeliveryAccounting } = require('./fetchFrameCounter')

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
const OTEL = arg('otel', 0)
const INTERN_OFFSETS = arg('intern-offsets', 0)
const CONTROL = arg('control', 0)
const LISTENER = arg('listener', 0)
const MAX_BYTES_PER_PARTITION = process.argv.includes('--max-bytes-per-partition')
  ? arg('max-bytes-per-partition')
  : undefined
const AUTOCOMMIT_THRESHOLD = process.argv.includes('--autocommit-threshold')
  ? arg('autocommit-threshold')
  : undefined
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
for (const [name, value] of [
  ['subscribe-channels', SUBSCRIBE_CHANNELS],
  ['otel', OTEL],
  ['intern-offsets', INTERN_OFFSETS],
  ['control', CONTROL],
  ['listener', LISTENER],
]) {
  if (value !== 0 && value !== 1) {
    console.error(`--${name} must be 0 or 1, got ${value}`)
    process.exit(1)
  }
}
for (const [name, value] of [
  ['max-bytes-per-partition', MAX_BYTES_PER_PARTITION],
  ['autocommit-threshold', AUTOCOMMIT_THRESHOLD],
]) {
  if (value !== undefined && !(Number.isInteger(value) && value >= 1)) {
    console.error(`--${name} must be an integer of at least 1, got ${value}`)
    process.exit(1)
  }
}

const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]

/**
 * @param {number[]} values milliseconds
 * @returns {{ p50: number, p90: number, p99: number }} the percentiles of the values
 */
const percentiles = (values) => {
  const sorted = [...values].sort((a, b) => a - b)
  return {
    p50: percentile(sorted, 0.5),
    p90: percentile(sorted, 0.9),
    p99: percentile(sorted, 0.99),
  }
}

/**
 * The gaps between consecutive handler entries.
 * @param {bigint[]} entries `process.hrtime.bigint()` at each entry, in order
 * @returns {number[]} the gap before each entry after the first, in milliseconds
 */
const deliveryGaps = (entries) =>
  entries.slice(1).map((entered, index) => Number(entered - entries[index]) / 1e6)

/** Counts the two required listener stages independently from record and callback totals. */
class ListenerAccounting {
  /** Initializes one consume arm before setup. */
  constructor() {
    this.stage = 'startup'
    this.assigned = 0
    this.teardownRevoked = 0
  }

  /** @returns {void} records actual initial assignment callback entry */
  onAssigned() {
    if (this.stage === 'startup') this.assigned++
  }

  /** @returns {void} ignores eager startup revoke and records graceful teardown only */
  onRevoked() {
    if (this.stage === 'teardown') this.teardownRevoked++
  }

  /** @returns {void} called after finishedAt, immediately before consumer.stop */
  beginTeardown() {
    this.stage = 'teardown'
  }

  /**
   * Requires the fixed stable one-member benchmark's actual callbacks.
   * @returns {void} no performance claim
   * @throws {Error} a missing, duplicate or misattributed invocation
   */
  verify() {
    if (this.assigned !== 1 || this.teardownRevoked !== 1) {
      throw new Error(
        'Listener control requires initial assigned and graceful teardown revoked once'
      )
    }
  }
}

/**
 * Checks observed counts against a separately declared finite source transcript.
 * @param {object} actual driver result
 * @param {object} expected source totals, never derived from the observed handler counters
 * @returns {void}
 * @throws {Error} naming the first count that disagrees
 */
const verifyAccounting = (actual, expected) => {
  for (const [key, value] of Object.entries(expected)) {
    if (actual[key] !== value) {
      throw new Error(`consume accounting ${key}: expected ${value}, received ${actual[key]}`)
    }
  }
}

/**
 * Runs the real public-handler wiring over three fixed finite transcripts, outside any timed
 * sample. Callback counts are declared from the source layout, separately from delivered records.
 * @param {'eachBatch'|'eachMessage'} handler selected public callback
 * @returns {Promise<object[]>} checked driver results
 * @throws {Error} if a callback, record or actual-gap count is lost or fabricated
 */
const accountingControl = async (handler) => {
  const rows = []
  for (const sizes of [[12], [3, 3, 3, 3], [2, 2, 2, 2, 2, 2]]) {
    const consumer = {
      connect: async () => {},
      subscribe: async () => {},
      stop: async () => {},
      disconnect: async () => {},
      run: async (options) => {
        let offset = 0
        for (const count of sizes) {
          const messages = Array.from({ length: count }, () => ({
            offset: String(offset++),
            value: Buffer.alloc(2),
          }))
          if (options.eachBatch) await options.eachBatch({ batch: { partition: 0, messages } })
          else for (const message of messages) await options.eachMessage({ partition: 0, message })
        }
      },
    }
    const result = await consumeOnce({ consumer: () => consumer }, 1, undefined, {
      handler,
      messages: 12,
      size: 2,
      timeout: 1000,
      listener: false,
    })
    const entries = handler === 'eachBatch' ? sizes.length : 12
    verifyAccounting(result, {
      deliveredMessages: 12,
      deliveredBatches: handler === 'eachBatch' ? sizes.length : 0,
      handlerEntries: entries,
      gapSamples: entries - 1,
      deliveredBytes: 24,
    })
    rows.push(result)
  }
  return rows
}

/**
 * Registers the OpenTelemetry instrumentation of this tree's `otel/` directory on this tree's
 * client, with a tracer and a meter provider that record and export nothing: what is measured is
 * the wrappers' cost. Its dependencies are resolved from that directory.
 * @returns {void}
 */
const registerOtel = () => {
  const otelDir = path.join(__dirname, '..', '..', 'otel')
  const otelRequire = createRequire(path.join(otelDir, 'package.json'))
  const { KafkaJsInstrumentation } = otelRequire('./src')
  const { TracerProvider, NoopSpanProcessor } = otelRequire('@opentelemetry/sdk-trace')
  const { MeterProvider } = otelRequire('@opentelemetry/sdk-metrics')
  const { AsyncLocalStorageContextManager } = otelRequire('@opentelemetry/context-async-hooks')
  const { context } = otelRequire('@opentelemetry/api')

  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable())
  const instrumentation = new KafkaJsInstrumentation()
  instrumentation.setTracerProvider(
    new TracerProvider({ spanProcessors: [new NoopSpanProcessor()] })
  )
  instrumentation.setMeterProvider(new MeterProvider())
  instrumentation.init()[0].patch(require('../../index'))
}

/**
 * Counts the calls of `OffsetOperations#begin` made after `delivered()` is called. The module is
 * loaded here and nowhere else, so a timed run on a tree that lacks it has no unresolved import.
 * @param {(request: string) => object} [load] how the module is loaded, `require` by default
 * @returns {{ delivered: () => void, count: () => number, restore: () => void }}
 * @throws {Error} naming the mode, when the tree has no `OffsetOperations`
 */
const countLedgerCalls = (load = require) => {
  let OffsetOperations
  try {
    OffsetOperations = load('../../src/consumer/offsetManager/offsetOperations')
  } catch (e) {
    if (e.code === 'MODULE_NOT_FOUND') {
      throw new Error('--control 1 counts OffsetOperations#begin, which this tree does not have')
    }
    throw e
  }

  const original = OffsetOperations.prototype.begin
  let counting = false
  let calls = 0
  OffsetOperations.prototype.begin = function begin(...args) {
    if (counting) {
      calls += 1
    }
    return original.apply(this, args)
  }

  return {
    delivered: () => {
      counting = true
    },
    count: () => calls,
    restore: () => {
      OffsetOperations.prototype.begin = original
    },
  }
}

/** Offsets kept for the whole run by `--intern-offsets 1`. */
const interned = new Map()

/**
 * @returns {Promise<number|undefined>} the heap in use after two collections 100 ms apart, when
 *   node runs with `--expose-gc`
 */
const retainedHeap = async () => {
  if (typeof global.gc !== 'function') {
    return undefined
  }

  global.gc()
  await new Promise((resolve) => setTimeout(resolve, 100))
  global.gc()
  return process.memoryUsage().heapUsed
}

/**
 * Creates the benchmark topic and waits for its leaders, so the first fetch is not a metadata retry.
 * @param {import('../../index').Kafka} kafka the client to administer with
 * @returns {Promise<void>}
 * @throws {Error} the failure of the creation, after the admin is disconnected
 */
const createTopic = (kafka) =>
  Clients.use(kafka, async (clients) => {
    const admin = clients.admin()
    await admin.connect()
    await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: PARTITIONS, replicationFactor: 1 }],
      waitForLeaders: true,
    })
  })

/**
 * Deletes the generated benchmark topic so repeated runs do not leave topics behind. A topic named
 * by `BENCH_TOPIC` is left alone, and a failed deletion is reported without masking the run's own
 * error. The admin is disconnected either way.
 * @param {import('../../index').Kafka} kafka the client to administer with
 * @returns {Promise<void>}
 */
const deleteTopic = async (kafka) => {
  if (TOPIC !== GENERATED_TOPIC) {
    return
  }

  try {
    await Clients.use(kafka, async (clients) => {
      const admin = clients.admin()
      await admin.connect()
      await admin.deleteTopics({ topics: [TOPIC] })
    })
  } catch (e) {
    console.error(`could not delete ${TOPIC}: ${e.message}`)
  }
}

/**
 * Produces the fixed corpus: `MESSAGES` messages of `SIZE` bytes, uncompressed, round-robined over
 * the topic's partitions by the default partitioner.
 * @param {import('../../index').Kafka} kafka the client to produce with
 * @returns {Promise<void>}
 * @throws {Error} the failure of a connect or send, after the producer is disconnected
 */
const produceCorpus = (kafka) =>
  Clients.use(kafka, async (clients) => {
    const producer = clients.producer()
    await producer.connect()

    const value = Buffer.alloc(SIZE, 'x')
    for (let sent = 0; sent < MESSAGES; sent += PRODUCE_BATCH) {
      const count = Math.min(PRODUCE_BATCH, MESSAGES - sent)
      await producer.send({
        topic: TOPIC,
        messages: Array.from({ length: count }, () => ({ value })),
      })
    }
  })

/**
 * Consumes the whole corpus once with a fresh group, timing every handler call: per batch under
 * `--handler eachBatch`, per record under `--handler eachMessage`. `--timeout` bounds the drain,
 * from `run` until the last message; startup is bounded by the client's connection and request
 * timeouts.
 * @param {import('../../index').Kafka} kafka the client to consume with
 * @param {number} run run number, used to keep each run on its own consumer group
 * @param {{ delivered: () => void }} [ledger] told when the first batch has been delivered
 * @param {object} [options] finite source settings for untimed controls; CLI defaults otherwise
 * @param {'eachBatch'|'eachMessage'} [options.handler] selected callback
 * @param {number} [options.messages] independently declared delivered records
 * @param {number} [options.size] corpus record bytes
 * @param {number} [options.timeout] drain deadline in milliseconds, armed after run resolves
 * @param {() => bigint} [options.now] monotonic measurement clock
 * @param {boolean} [options.listener] registers the counted rebalance listener, as `--listener 1`
 * @returns {Promise<{ messagesPerSecond: number, p50: number, p90: number, p99: number,
 *   handlerP99: number, deliveryGapP50: number|null, deliveryGapP90: number|null, deliveryGapP99: number|null,
 *   batches: number, deliveredMessages: number, deliveredBatches: number, handlerEntries: number,
 *   gapSamples: number, deliveredBytes: number }>} throughput over the consume phase, handling latency in ms and the gaps
 *   between handler entries in ms
 * @throws {Error} the failure of connect, subscribe, run or stop, or the drain deadline;
 *   after owner cleanup and timer clearing; a run with no bytes or a mismatched record total
 */
const consumeOnce = async (
  kafka,
  run,
  ledger,
  {
    handler = HANDLER,
    messages = MESSAGES,
    size = SIZE,
    timeout = TIMEOUT_MS,
    now = () => process.hrtime.bigint(),
    listener = LISTENER === 1,
  } = {}
) => {
  const latencies = []
  const accounting = new DeliveryAccounting(handler)
  const listenerAccounting = listener ? new ListenerAccounting() : undefined
  const rebalanceListener = listener
    ? {
        onPartitionsAssigned: () => listenerAccounting.onAssigned(),
        onPartitionsRevoked: () => listenerAccounting.onRevoked(),
      }
    : undefined
  let bytes = 0
  let finishedAt
  let startedAt
  let resolveWhenDrained
  const drained = new Promise((resolve) => {
    resolveWhenDrained = resolve
  })

  const commits =
    AUTOCOMMIT_THRESHOLD === undefined
      ? { autoCommit: false }
      : { autoCommit: true, autoCommitThreshold: AUTOCOMMIT_THRESHOLD }

  const eachBatchArm = {
    eachBatchAutoResolve: true,
    ...commits,
    eachBatch: async ({ batch }) => {
      const batchStartedAt = now()
      accounting.enter(batch.messages.length, batchStartedAt)
      if (startedAt === undefined) {
        startedAt = batchStartedAt
      }

      for (const message of batch.messages) {
        bytes += message.value.length
        if (INTERN_OFFSETS === 1) {
          interned.set(`${batch.partition}:${message.offset}`, message.offset)
        }
      }

      latencies.push(Number(now() - batchStartedAt) / 1e6)
      ledger?.delivered()

      if (accounting.messages >= messages) {
        resolveWhenDrained()
      }
    },
  }

  const eachMessageArm = {
    ...commits,
    eachMessage: async ({ partition, message }) => {
      const messageStartedAt = now()
      accounting.enter(1, messageStartedAt)
      if (startedAt === undefined) {
        startedAt = messageStartedAt
      }

      bytes += message.value.length
      if (INTERN_OFFSETS === 1) {
        interned.set(`${partition}:${message.offset}`, message.offset)
      }

      latencies.push(Number(now() - messageStartedAt) / 1e6)
      ledger?.delivered()

      if (accounting.messages >= messages) {
        resolveWhenDrained()
      }
    },
  }

  await Clients.use(kafka, async (clients) => {
    const consumer = clients.consumer({
      groupId: `${TOPIC}-run-${run}`,
      ...(MAX_BYTES_PER_PARTITION !== undefined && {
        maxBytesPerPartition: MAX_BYTES_PER_PARTITION,
      }),
    })

    let deadlineTimer
    try {
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({
        ...(handler === 'eachMessage' ? eachMessageArm : eachBatchArm),
        ...(rebalanceListener && { rebalanceListener }),
      })
      const deadline = new Promise((_, reject) => {
        deadlineTimer = setTimeout(
          () =>
            reject(
              new Error(
                `run ${run} timed out after ${timeout} ms with ${accounting.messages} of ${messages} messages consumed`
              )
            ),
          timeout
        )
      })
      await Promise.race([drained, deadline])
      finishedAt = now()
    } finally {
      clearTimeout(deadlineTimer)
    }
    listenerAccounting?.beginTeardown()
    await consumer.stop()
    listenerAccounting?.verify()
  })

  if (startedAt === undefined || (size > 0 && bytes === 0)) {
    throw new Error(
      `run ${run} received nothing: ${accounting.messages} of ${messages} messages, ${bytes} bytes`
    )
  }

  verifyAccounting({ deliveredMessages: accounting.messages }, { deliveredMessages: messages })
  const elapsedMs = Number(finishedAt - startedAt) / 1e6

  const latency = percentiles(latencies)
  const counts = accounting.result()
  return {
    messagesPerSecond: (accounting.messages / elapsedMs) * 1000,
    ...latency,
    handlerP99: latency.p99,
    ...counts,
    deliveredBytes: bytes,
    batches: counts.deliveredBatches,
  }
}

;(async () => {
  if (require.main !== module) {
    return
  }

  if (OTEL === 1) {
    registerOtel()
  }

  const kafka = new Kafka({
    clientId: 'bench-consume',
    brokers: ['localhost:9092', 'localhost:9095', 'localhost:9098'],
    logLevel: logLevel.NOTHING,
  })
  if (SUBSCRIBE_CHANNELS === 1) {
    subscribeChannels(kafka)
  }

  const ledger = CONTROL === 1 ? countLedgerCalls() : undefined
  try {
    await createTopic(kafka)
    await produceCorpus(kafka)

    if (CONTROL === 1) {
      const accounting = await accountingControl(HANDLER)
      const observed = await consumeOnce(kafka, 1, ledger)

      const expected = AUTOCOMMIT_THRESHOLD === undefined ? 'none' : 'some'
      const pass = expected === 'some' ? ledger.count() > 0 : ledger.count() === 0
      console.log(
        JSON.stringify({
          control: true,
          accounting,
          observed,
          arm: { handler: HANDLER, autocommitThreshold: AUTOCOMMIT_THRESHOLD ?? null },
          beginCalls: ledger.count(),
          expected,
          pass,
        })
      )
      process.exitCode = pass ? 0 : 1
      return
    }

    // The first run pays for page cache, JIT warm-up and group formation; its numbers are discarded.
    await consumeOnce(kafka, 0)
    const measured = await consumeOnce(kafka, 1)
    const heapRetainedBytes = await retainedHeap()

    console.log(
      JSON.stringify({
        messages: MESSAGES,
        size: SIZE,
        partitions: PARTITIONS,
        handler: HANDLER,
        subscribeChannels: SUBSCRIBE_CHANNELS === 1,
        ...measured,
        ...(heapRetainedBytes !== undefined && { heapRetainedBytes }),
      })
    )
  } finally {
    try {
      await deleteTopic(kafka)
    } finally {
      ledger?.restore()
    }
  }
})().catch((e) => {
  console.error(e)
  process.exit(1)
})

module.exports = {
  percentiles,
  deliveryGaps,
  countLedgerCalls,
  registerOtel,
  DeliveryAccounting,
  ListenerAccounting,
  verifyAccounting,
  accountingControl,
  createTopic,
  deleteTopic,
  produceCorpus,
  consumeOnce,
}
