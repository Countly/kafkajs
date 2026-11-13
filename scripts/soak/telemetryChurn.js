'use strict'

/**
 * The telemetry churn profile: one parent that supervises four one-client worker processes, one per
 * public client kind. Each worker holds the descriptors and instance ID of its client through
 * subscription changes, telemetry reconnects and public close/reopen cycles, and is judged on its
 * own memory and exact resource identities. The parent owns the broker resource, the parent-
 * coordinated tick catalogue, the independent record oracle, the independent protobuf decoding and
 * the natural-exit observations of the children.
 *
 * Development-only and public-API-only: nothing here reads a private member of the client.
 * `soak.js` injects its `Soak`, `instrument` and verdict so this module never requires it back.
 */
const { SINK_BYTES: CHURN_SINK_BYTES } = require('./churnReporter')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const zlib = require('node:zlib')
// eslint-disable-next-line n/no-unsupported-features/node-builtins -- development driver; only the resource lifetime hooks are used
const { AsyncLocalStorage, createHook } = require('node:async_hooks')
const { fork } = require('node:child_process')
const v8 = require('node:v8')
const { createChaosSocketFactory, FrameFilter } = require('../../testHelpers/chaosSocket')
const {
  DecoderGate,
  DecoderMemory,
  TelemetryWireObserver,
  OBSERVER_LIMITS,
  checkedResource,
  readExports,
} = require('../../testHelpers/telemetryFixtures')
const { pollUntil } = require('../../src/producer/__tests__/fixtures/bounded')
const { setShareGroupConfigs, resetShareGroupConfigs } = require('../../testHelpers/shareFixtures')
const {
  TelemetrySegments,
  checkSegmentFiles,
  FILE_BOUND,
  scheduleOf,
} = require('../../testHelpers/telemetrySegments')
const {
  NativeJournal,
  PendingSampler,
  ProcessMemory,
  readProcStatus,
} = require('../../testHelpers/processMemory')
const {
  FixedJournal,
  HandlerJournal,
  JournalConstancy,
  SYMBOL_BYTES,
  journalState,
} = require('../../testHelpers/fixedJournal')
const gzipCodec = require('../../src/protocol/message/compression/gzip')
const zstdCodec = require('../../src/protocol/message/compression/zstd')
const { slope } = require('./bounds')
const { LIVE_SEED, LiveSeed } = require('./liveSeed')
const { LONG_WINDOW, HeapSpaceJournal, selected: longWindowSelected } = require('./longWindow')
const {
  ShareTrace,
  acknowledgementResponse,
  confirmedDecisions,
  heldAcquisitions,
} = require('../e2e/telemetry/shareEvidence')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')
const shareFetchWire = require('../../testHelpers/wire/shareFetchV2')
const shareAckWire = require('../../testHelpers/wire/shareAcknowledgeV2')

const SCENARIO = longWindowSelected() ? LONG_WINDOW.scenario : 'telemetry-churn'
const KINDS = Object.freeze(['producer', 'consumer', 'admin', 'shareConsumer'])
const STEPS = Object.freeze({ subscriptionMs: 10000, retireMs: 30000, reopenMs: 60000 })
const ORDER = Object.freeze(['reopen', 'retire', 'subscription'])
const STAGE_OF = Object.freeze({ reopen: 'lifecycle', retire: 'retire', subscription: 'confirm' })
const MINIMUMS = Object.freeze({ subscription: 30, retire: 10, reopen: 5 })
const LATE_MS = 5000
const DESCRIPTORS = 128
const CORPUS = Object.freeze({
  records: longWindowSelected() ? LONG_WINDOW.corpusRecords : 10000,
  batch: 100,
  batchMs: 3000,
  bytes: 200,
})
const SETUP_MS = 48000
const NATURAL_EXIT_MS = 10000
/** How long a killed worker's close and drained pipes are awaited before its row is final. */
const KILL_JOIN_MS = 5000
/**
 * The part of the 120 s cleanup kept back from the wait for worker results: joining or killing the
 * workers (10 s + 5 s), the parent's terminal decodes (4 x 8 s) and 3 s for its own releases.
 */
const RESULT_RESERVE_MS = 50000
const STAGE_MS = Object.freeze({ lifecycle: 12000, retire: 8000, confirm: 10000 })
const DECODER_SLOTS = 4
const DECODE_BUDGET_MS = 6000
const MESSAGE_BYTES = 8 * 1024 * 1024
const TOTAL_BYTES = 48 * 1024 * 1024
const MAX_STDERR = 65536
const WORKER_ENV = 'TELEMETRY_WORKER_KIND'
const WORKER_TOKEN = 'TELEMETRY_WORKER_TOKEN'
const SUBSCRIPTIONS = Object.freeze(['com.example.', '', 'org.apache.kafka.'])
const INITIAL_SELECTION = SUBSCRIPTIONS[2]
const CUSTOM_PREFIX = SUBSCRIPTIONS[0]
const PUSH_INTERVAL_MS = 1000
const TICK_PROOF_MS = 5000
const MIB = 1048576
const ROOT = path.resolve(__dirname, '../..')
/** Native compression rows one worker journals: one per Push of a 360 s run at 1 s, with room. */
const NATIVE_ROWS = 1024
/** Engine code readings one worker journals: every settled reading of a run, with room. */
const ENGINE_ROWS = 64
/** Five-minute telemetry worker flags, also reused by live-flags journal controls. */
const WORKER_FLAGS = Object.freeze([
  '--expose-gc',
  '--min-semi-space-size=1',
  '--max-semi-space-size=1',
])

/**
 * The proved worst-case encoded bytes of each share row in the `share-packed-1` layout, rounded up
 * to a multiple of 16: an exchange header (167, with int32 identifiers, Float64 clocks, two raw
 * 32-byte digests and every label in a slot), a request/response partition pair with its own
 * topic wrappers (72), the acquired range plus acknowledgement batch of one record with int64
 * offsets of 8 bytes each (63) and a public acknowledgement row with its error labels (67).
 * Digests, offsets and labels that cannot be packed exactly cost more and overflow fatally.
 */
const SHARE_ROW_BYTES = Object.freeze({
  header: 176,
  partitionPair: 80,
  record: 64,
  acknowledgement: 80,
})

/**
 * Bounds the unchanged corpus, three-partition topology and session catalogue, not average traffic.
 * Empty/error rounds owe 50 ms per node (shareFetchLoop); 500 ms is only a maximum broker wait.
 * A productive fetch or a separate flush may carry just one record, not a full 100-record batch.
 * The 10% corpus margin covers extra deliveries; further traffic or shapes overflow fatally.
 * Rows reserve their `SHARE_ROW_BYTES`; field columns and byte-only label slots bound repeated names.
 * @param {object} [limits] the unchanged telemetry schedule constants @returns {object} capacities
 */
const journalLimits = (
  limits = { caseMs: 540000, sampleAt: Array(11), warmupMs: 60000, measureMs: 300000 }
) => {
  const batches = Math.ceil(CORPUS.records / CORPUS.batch)
  const records = Math.ceil((CORPUS.records * 11) / 10)
  const sessions = 3 * (Math.ceil(limits.caseMs / STEPS.reopenMs) + 2)
  const emptyRounds = 3 * Math.ceil(limits.caseMs / 50)
  const lifecycleExchanges = sessions * 4
  const exchanges = emptyRounds + records * 2 + lifecycleExchanges
  const partitionPairs = emptyRounds + 3 * (records * 2 + lifecycleExchanges)
  const receipts = 2 * (Math.ceil(limits.caseMs / STEPS.reopenMs) + 3)
  return {
    records,
    batches: Math.ceil((batches * 11) / 10),
    exchanges,
    exchangeBytes:
      exchanges * SHARE_ROW_BYTES.header +
      partitionPairs * SHARE_ROW_BYTES.partitionPair +
      records * SHARE_ROW_BYTES.record,
    acknowledgements: records,
    acknowledgementBytes: records * SHARE_ROW_BYTES.acknowledgement,
    receipts,
    samples: limits.sampleAt.length + 4,
    sampleBytes: (limits.sampleAt.length + 4) * 65536,
  }
}

/**
 * The exact bytes a worker of `kind` allocates and fills before its disabled control, derived only
 * from the journal limits: ordinary work (two 16-byte id ledgers, 32-byte handler rows with their
 * completeness bitmap, 256-byte receipts, 16-byte producer slots), the native journal (2,048-byte
 * rows) with its pending windows (256 bytes), the memory samples, the engine code journal (64-byte
 * rows), the long-window heap space journal when the limits name one and, for the share consumer,
 * both share archives with their label slots. `nativeRows`, `engineRows` and `heapSpaceRows` of the
 * limits replace the defaults of the existing lane. The sizes mirror
 * the owners that allocate them; a spec compares the sum with the storage the real owners hold.
 * @param {string} kind a client kind
 * @param {object} [limits] the telemetry schedule constants, as for `journalLimits`
 * @returns {number} declared journal bytes
 */
const declaredJournalBytes = (kind, limits) => {
  const storage = journalLimits(limits)
  const ordinary =
    2 * storage.records * 16 +
    (storage.records * 32 + CORPUS.records) +
    2 * storage.receipts * 256 +
    storage.batches * 16
  const native = (limits?.nativeRows ?? NATIVE_ROWS) * (2048 + 256)
  const engine = (limits?.engineRows ?? ENGINE_ROWS) * 64
  const heapSpaces = (limits?.heapSpaceRows ?? 0) * LONG_WINDOW.heapSpaceRowBytes
  const share =
    kind === 'shareConsumer'
      ? storage.exchangeBytes + storage.acknowledgementBytes + 2 * SYMBOL_BYTES
      : 0
  return ordinary + native + storage.sampleBytes + engine + heapSpaces + share
}

const CONTROL = Object.freeze({
  rssDriftBytes: 2 * MIB,
  retainedDriftBytes: 262144,
  idleMs: 2000,
  setupPeakBytes: 96 * MIB,
  catalogue: Object.freeze({ producer: 20, consumer: 0, admin: 10, shareConsumer: 0 }),
  valueBytes: 7,
})
const TRACKED = new Set([
  'Timeout',
  'TCPWRAP',
  'TCPSERVERWRAP',
  'PIPEWRAP',
  'TTYWRAP',
  'UDPWRAP',
  'MESSAGEPORT',
])
const MESSAGE_TYPES = Object.freeze({
  worker: [
    'hello',
    'ready',
    'phase',
    'stage-done',
    'confirm-fenced',
    'decode',
    'result',
    'failure',
    'receipt',
    'unhandled',
    'progress',
  ],
  once: ['hello', 'ready', 'phase', 'result', 'receipt'],
})

/**
 * @param {number} ms delay
 * @returns {Promise<void>} resolves after the delay; the timer is not unreferenced
 */
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * @param {string} kind a client kind @returns {string} its topic suffix and group label
 */
const labelOf = (kind) => (kind === 'shareConsumer' ? 'share' : kind)

/**
 * The fixed identity of one corpus record. Records are unique across kinds and positions.
 * @param {string} stem run prefix @param {string} kind client kind @param {number} index position
 * @returns {string} the record key
 */
const recordId = (stem, kind, index) => `${stem}:${labelOf(kind)}:${String(index).padStart(5, '0')}`

/**
 * @param {string} key a corpus record key @returns {number} its position in the corpus, or -1
 */
const indexOf = (key) => {
  const match = /:(\d{5})$/.exec(String(key))
  return match ? Number(match[1]) : -1
}

/**
 * The fixed 200 byte value of a record, carrying its identity.
 * @param {string} id record identity @returns {Buffer} exactly `CORPUS.bytes` bytes
 */
const valueOf = (id) => Buffer.from(id.padEnd(CORPUS.bytes, '.'))

/**
 * @param {Buffer|string} bytes data @returns {string} lower-case SHA-256 hex
 */
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/**
 * The driver source a run is bound to: this module and the forked driver, each by its
 * repository-relative path and digest, so a verifier at the run's tree can join both.
 * @param {string} driver absolute path of the driver the workers are forked from
 * @returns {{churn:{file:string, sha256:string}, driver:{file:string, sha256:string}}} the stamp
 */
const sourceStamp = (driver) => ({
  churn: { file: path.relative(ROOT, __filename), sha256: sha(fs.readFileSync(__filename)) },
  driver: { file: path.relative(ROOT, driver), sha256: sha(fs.readFileSync(driver)) },
})

/**
 * @param {number} type the Push compression type: 0 none, 1 GZIP, 4 ZSTD
 * @param {Buffer} blob the transmitted bytes
 * @returns {Buffer} the raw MetricsData, at most 4096 bytes @throws {Error} for another type
 */
const decompressBounded = (type, blob) => {
  const options = { maxOutputLength: 4096 }
  if (type === 0) return blob
  if (type === 1) return zlib.gunzipSync(blob, options)
  // eslint-disable-next-line n/no-unsupported-features/node-builtins -- required live Node lines provide the built-in codec
  if (type === 4) return zlib.zstdDecompressSync(blob, options)
  throw new Error('Unsupported captured compression')
}

/**
 * @param {string[]|number[]} ids identities @returns {string} hash of the sorted identity list
 */
const hashIds = (ids) => sha(JSON.stringify([...ids].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))))

/**
 * Rejects the first arrival of a promise that was not settled within `ms`; the timer never lingers.
 * @template T
 * @param {Promise<T>|function():Promise<T>} work the awaited work @param {number} ms bound
 * @param {string} label failure description @returns {Promise<T>} the work's own outcome
 * @throws {Error} naming the label when the bound passed first
 */
const within = async (work, ms, label) => {
  let timer
  try {
    return await Promise.race([
      typeof work === 'function' ? work() : work,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} did not finish within ${ms} ms`)), ms)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Reads the kernel's lifetime peak resident size of this executable image.
 * @returns {number|null} bytes, or null where the source does not exist or cannot be parsed
 */
const readHighWater = () => readProcStatus()?.vmhwm ?? null

/**
 * One unsettled memory reading: current and peak sizes in bytes, never rebased or clamped.
 * @param {string} phase the reading's place in the fixed catalogue
 * @param {function():number} [now] monotonic milliseconds
 * @returns {object} `phase`, `at`, rss, heapUsed, external, arrayBuffers, retained, hwm, rawMaxRss
 */
const readMemory = (phase, now = () => performance.now()) => {
  const usage = process.memoryUsage()
  return {
    phase,
    at: now(),
    rss: usage.rss,
    heapUsed: usage.heapUsed,
    external: usage.external,
    arrayBuffers: usage.arrayBuffers,
    retained: usage.heapUsed + usage.external,
    hwm: readHighWater(),
    rawMaxRss: process.resourceUsage().maxRSS * 1024,
  }
}

/**
 * Two collections 100 ms apart with the collection callbacks left to settle, then a reading.
 * @param {string} phase the reading's place in the fixed catalogue
 * @param {object} io `{gc, pause, now, collections, engine}`; the pause must let timers and immediates
 *   run, `collections` counts the collections the engine reported so far, and an `engine` journal
 *   (shadow only) takes the engine's code sizes beside the reading
 * @returns {Promise<object>} the settled reading with `gc: {admittedAt, completedAt, observed}`
 */
const settledReading = async (phase, { gc, pause, now, collections = () => 0, engine = null }) => {
  const admittedAt = now()
  const before = collections()
  gc()
  await pause(100)
  gc()
  await new Promise((resolve) => setImmediate(resolve))
  await new Promise((resolve) => setImmediate(resolve))
  const reading = readMemory(phase, now)
  engine?.record(reading)
  return {
    ...reading,
    gc: { admittedAt, completedAt: reading.at, observed: collections() - before },
  }
}

/**
 * The V8 code and bytecode bytes beside every reading of one worker, kept as fixed scalar rows. It
 * is shadow evidence of the code-accounting question and never enters a verdict. Its storage is
 * reserved before the first control reading and held to the end; an overflow throws and nothing is
 * truncated or overwritten.
 */
class EngineCodeJournal {
  /** @param {{capacity?:number}} [options] the most readings one worker takes */
  constructor({ capacity = ENGINE_ROWS } = {}) {
    this.rows = new FixedJournal({ name: 'engine code', capacity, bytes: capacity * 64 })
  }

  /**
   * Appends the engine's code sizes for a reading just taken; nothing about the reading changes.
   * @param {{phase:string, at:number}} reading the reading's catalogue name and monotonic time
   * @returns {void}
   * @throws {Error} when the journal is full
   */
  record({ phase, at }) {
    const code = v8.getHeapCodeStatistics()
    this.rows.push([phase, at, code.code_and_metadata_size, code.bytecode_and_metadata_size])
  }

  /** @returns {object} fixed storage and append cursor, never the rows */
  journalState() {
    return this.rows.journalState()
  }

  /** @returns {Array<{phase:string, at:number, code:number, bytecode:number}>} rows, after the run */
  values() {
    return this.rows.values().map(([phase, at, code, bytecode]) => ({ phase, at, code, bytecode }))
  }

  /**
   * Reports the raw, code and data series of a run beside its acceptance verdict, never into it.
   * Code is `code_and_metadata_size + bytecode_and_metadata_size`; data is raw minus code, in bytes.
   * @param {Array<{minutes:number, raw:number, code:number}>} points the settled readings in order
   * @returns {object} per series the least-squares slope per minute and the first-to-last change,
   *   or `complete: false` when fewer than two readings or two distinct times exist; it never throws
   */
  static shadow(points) {
    const report = { scope: 'shadow', acceptance: false, readings: points.length }
    if (
      points.length < 2 ||
      points.some((p) => ![p.minutes, p.raw, p.code, p.raw - p.code].every(Number.isFinite))
    )
      return { ...report, complete: false }
    const series = (pick) => {
      const values = points.map((p) => [p.minutes, pick(p)])
      return {
        slopeBytesPerMinute: slope(values),
        endpointBytes: values.at(-1)[1] - values[0][1],
      }
    }
    try {
      return {
        ...report,
        complete: true,
        raw: series((p) => p.raw),
        code: series((p) => p.code),
        data: series((p) => p.raw - p.code),
      }
    } catch (error) {
      return { ...report, complete: false, error: error.message }
    }
  }

  /**
   * Validates an archived journal against the original raw samples without sorting or dropping rows.
   * Reading indices name the full journal; sample indices name the original verdict input array.
   * Both clocks must match exactly: absolute memory `at` and phase-relative `completedAtMs`.
   * @param {object[]} samples original `{completedAtMs, snapshot.memory}` samples in order
   * @param {object} engine version 1 archive with `readingCount`, `readings` and `samples` references
   * @returns {object[]} joined sample identity, both clocks, raw retained bytes, code and bytecode
   * @throws {Error} for missing, duplicate, reordered, mismatched or non-finite facts
   */
  static join(samples, engine) {
    if (
      engine?.version !== 1 ||
      !Array.isArray(samples) ||
      !Array.isArray(engine.readings) ||
      !Array.isArray(engine.samples) ||
      engine.readings.length !== engine.readingCount ||
      engine.samples.length !== samples.length
    )
      throw new Error('Engine journal join needs complete version 1 arrays')
    let previousAt = -Infinity
    for (const [index, row] of engine.readings.entries()) {
      if (
        row?.readingIndex !== index ||
        typeof row.phase !== 'string' ||
        row.phase.length === 0 ||
        ![row.at, row.code, row.bytecode].every(Number.isFinite) ||
        row.at <= previousAt
      )
        throw new Error(`Engine journal has an invalid or reordered reading ${index}`)
      previousAt = row.at
    }
    let previousReading = -1
    let previousCompletion = -Infinity
    return samples.map((sample, sampleIndex) => {
      const reference = engine.samples[sampleIndex]
      const reading = engine.readings[reference?.readingIndex]
      const memory = sample?.snapshot?.memory
      if (
        reference?.sampleIndex !== sampleIndex ||
        !Number.isSafeInteger(reference.readingIndex) ||
        reference.readingIndex <= previousReading ||
        !reading ||
        !memory ||
        ![memory.at, memory.retainedBytes, sample.completedAtMs].every(Number.isFinite) ||
        reading.at !== memory.at ||
        reading.phase !== memory.phase ||
        reference.completedAtMs !== sample.completedAtMs ||
        sample.completedAtMs <= previousCompletion
      )
        throw new Error(`Engine journal join mismatch at sample ${sampleIndex}`)
      previousReading = reference.readingIndex
      previousCompletion = sample.completedAtMs
      return {
        sampleIndex,
        readingIndex: reference.readingIndex,
        phase: reading.phase,
        at: reading.at,
        completedAtMs: sample.completedAtMs,
        raw: memory.retainedBytes,
        code: reading.code,
        bytecode: reading.bytecode,
      }
    })
  }

  /**
   * Materialises the full engine journal and exact sample references only after the closed reading.
   * Extra control, baseline and cleanup readings stay in the archive, outside the sample fit.
   * The summary uses the original OLS arithmetic and phase-relative sample completion times.
   * @param {object[]} samples original verdict input samples, each `{completedAtMs, snapshot.memory}`
   * @returns {object} diagnostic-only summary and versioned `engine` archive; an invalid join or
   *   overflow is `complete: false`, retains the evidence and publishes no partial series
   */
  report(samples) {
    const readings = this.values().map((row, readingIndex) => ({ readingIndex, ...row }))
    const byTime = new Map(readings.map((row) => [row.at, row.readingIndex]))
    const engine = {
      version: 1,
      readingCount: this.journalState().length,
      readings,
      samples: samples.map((sample, sampleIndex) => ({
        sampleIndex,
        readingIndex: byTime.get(sample?.snapshot?.memory?.at) ?? null,
        completedAtMs: sample?.completedAtMs,
      })),
    }
    try {
      if (this.journalState().overflow) throw new Error('Engine journal overflow')
      const points = EngineCodeJournal.join(samples, engine).map((row) => ({
        minutes: row.completedAtMs / 60000,
        raw: row.raw,
        code: row.code + row.bytecode,
      }))
      return { ...EngineCodeJournal.shadow(points), engine }
    } catch (error) {
      return { scope: 'shadow', acceptance: false, complete: false, error: error.message, engine }
    }
  }
}

/**
 * @param {function():number} [now] monotonic milliseconds
 * @returns {object} one-minute load per CPU, free memory and memory pressure, with unavailable
 *   capabilities named rather than reported as quiet
 */
const hostContext = (now = () => performance.now()) => {
  let pressure = { available: false }
  try {
    const found = /^some avg10=([\d.]+)/m.exec(fs.readFileSync('/proc/pressure/memory', 'utf8'))
    if (found) pressure = { available: true, someAvg10: Number(found[1]) }
  } catch {
    // an unreadable pressure file is reported as unavailable
  }
  const cpus = os.cpus().length
  return {
    at: now(),
    cpus,
    load1: os.loadavg()[0],
    loadPerCpu: os.loadavg()[0] / Math.max(1, cpus),
    freeBytes: os.freemem(),
    pressure,
  }
}

/**
 * @param {Array<{kind:string,type:string,token:object}>} facts compressible per-offset facts
 * @returns {object[]} the same facts as inclusive offset ranges where neighbours agree in every
 *   other field, in the original order
 */
const compactRuns = (facts) => {
  const runs = []
  for (const fact of facts) {
    const last = runs.at(-1)
    const joins =
      last &&
      last.topicId === fact.topicId &&
      last.partition === fact.partition &&
      last.type === fact.type &&
      last.at === fact.at &&
      last.errorCode === fact.errorCode &&
      last.exchange === fact.exchange &&
      BigInt(last.lastOffset) + 1n === BigInt(fact.offset)
    if (joins) last.lastOffset = fact.offset
    else
      runs.push({ ...fact, firstOffset: fact.offset, lastOffset: fact.offset, offset: undefined })
  }
  return runs.map(({ offset, ...run }) => run)
}

/**
 * The write fence of one registry call: the observer cursor of every segment that can still record a
 * request. A request recorded after the cursor of its own segment, or in a segment created after all
 * of them, was written after the call returned.
 * @param {TelemetrySegments} segments the owner of the client's observer segments
 * @returns {{cursors:Object<string,number>}} cursor by segment ID
 */
const writeFence = (segments) => ({
  cursors: Object.fromEntries(
    segments
      .undischarged()
      .filter((segment) => segment.observer)
      .map((segment) => [segment.id, segment.observer.checkpoint()])
  ),
})

/**
 * The deterministic action table of one run, derived from the fixed schedule alone: what happens at
 * which millisecond after the epoch. The five-minute lane batches the original ordered selections
 * into five disconnected public boundaries: the first follows the 60 s slot so its final selection
 * is custom, and the last follows all retirements so an empty selection owes no recovery Push.
 * The long-window lane retains its original three stages.
 */
class TelemetrySchedule {
  /**
   * @param {{warmupMs:number, measureMs:number, corpusStartMs?:number}} window warm-up and
   *   measurement lengths, and when the corpus starts: with the measurement unless the long-window
   *   candidate starts traffic with the warm-up
   */
  constructor({ warmupMs, measureMs, corpusStartMs = warmupMs }) {
    this.warmupMs = warmupMs
    this.corpusStartMs = corpusStartMs
    this.endMs = warmupMs + measureMs
    this.betweenLifecycles = SCENARIO === 'telemetry-churn' && measureMs <= 300000
    const at = new Map()
    for (const [action, step] of [
      ['reopen', STEPS.reopenMs],
      ['retire', STEPS.retireMs],
      ['subscription', STEPS.subscriptionMs],
    ])
      for (let ms = step; ms < this.endMs; ms += step) at.set(ms, [...(at.get(ms) ?? []), action])
    this.ticks = [...at]
      .sort((a, b) => a[0] - b[0])
      .map(([ms, actions], number) => ({
        number,
        at: ms,
        index: ms / STEPS.subscriptionMs,
        actions: ORDER.filter((a) => actions.includes(a)),
      }))
    if (this.betweenLifecycles) {
      const cycles = this.ticks.filter((tick) => tick.actions.includes('reopen'))
      const gaps = new Set(
        cycles.map((tick, index) =>
          index === cycles.length - 1
            ? this.endMs - STEPS.subscriptionMs
            : tick.at + (index === 0 ? STEPS.subscriptionMs : 0)
        )
      )
      const pending = []
      this.ticks = this.ticks
        .flatMap((tick) => {
          pending.push(tick.index)
          const transitions = gaps.has(tick.at) ? pending.splice(0) : []
          const actions = [
            ...(transitions.length ? ['reopen'] : []),
            ...(tick.actions.includes('retire') ? ['retire'] : []),
            ...(transitions.length ? ['subscription'] : []),
          ]
          return actions.length ? [{ ...tick, actions, transitions }] : []
        })
        .map((tick, number) => ({ ...tick, number }))
    }
  }

  /**
   * @param {{at:number}} tick one table row @returns {boolean} whether it falls in the measurement
   */
  measured(tick) {
    return tick.at >= this.warmupMs
  }

  /**
   * @param {{actions:string[]}} tick one table row @returns {string[]} its stages, in order
   */
  stages(tick) {
    if (this.betweenLifecycles)
      return [
        ...(tick.transitions.length ? ['confirm'] : []),
        ...(tick.actions.includes('retire') ? ['retire'] : []),
      ]
    return tick.actions.map((action) => STAGE_OF[action])
  }

  /**
   * @param {number} index the 10 s transition number, from 1
   * @returns {string} the metrics prefix list the resource holds after that transition
   */
  selection(index) {
    return SUBSCRIPTIONS[(index - 1) % SUBSCRIPTIONS.length]
  }

  /**
   * @returns {{subscription:number, retire:number, reopen:number}} the measured action counts the
   *   table holds, which a completed run must reach
   */
  required() {
    const counts = { subscription: 0, retire: 0, reopen: 0 }
    for (const tick of this.ticks)
      if (this.measured(tick))
        for (const action of tick.actions)
          counts[action] += action === 'subscription' ? (tick.transitions?.length ?? 1) : 1
    return counts
  }

  /**
   * @returns {number[]} the fixed slots, in ms after the epoch, at which the corpus batches are
   *   offered; none falls in the warm-up unless the corpus starts with it
   */
  corpusSlots() {
    return Array.from(
      { length: CORPUS.records / CORPUS.batch },
      (_, batch) => this.corpusStartMs + batch * CORPUS.batchMs
    )
  }
}

/**
 * Tracks the live timers and handles of this process from before the target is built, with stable
 * issued tokens, a finite live map and the role of the code that created each one. A resource is
 * `target` when created inside `as('target', ...)`, `fixture` inside `as('fixture:<name>', ...)`
 * and `unknown` otherwise. Every token also names the resource that triggered it. Resources created
 * before `start` (the IPC channel) are not seen.
 */
class ProcessResources {
  #roles = new AsyncLocalStorage()

  /** Builds a disabled tracker. */
  constructor() {
    this.live = new Map()
    this.hook = createHook({
      init: (id, type, trigger) => {
        if (!TRACKED.has(type)) return
        const role = this.#roles.getStore() ?? 'unknown'
        const parent = this.live.get(trigger)
        this.live.set(id, {
          group: role === 'target' ? 'target' : role.startsWith('fixture') ? 'fixture' : 'unknown',
          token: { id, type, role, trigger, triggerRole: parent?.token.role ?? null },
        })
      },
      destroy: (id) => {
        this.live.delete(id)
      },
    })
  }

  /** @returns {void} begins tracking */
  start() {
    this.hook.enable()
  }

  /** @returns {void} stops tracking and forgets every resource */
  stop() {
    this.hook.disable()
    this.live.clear()
  }

  /**
   * @template T
   * @param {string} role `target` or `fixture:<name>` @param {function():T} work
   * @returns {T} the work's own result; every resource it creates carries the role
   */
  as(role, work) {
    return this.#roles.run(role, work)
  }

  /**
   * @returns {Record<string, object[]>} the three fixed role groups, each possibly empty, holding
   *   the token of every live resource. Tokens are the same object for the same resource, so a
   *   probe tells a replacement from the original, and a missing group is never mistaken for none.
   */
  identities() {
    const roles = { 'resources:target': [], 'resources:fixture': [], 'resources:unknown': [] }
    for (const { group, token } of this.live.values()) roles[`resources:${group}`].push(token)
    return roles
  }
}

/**
 * The retained metric descriptors of one client: the explicit registrations whose bounded
 * retention is the baseline of the memory screen. One slot can be replaced by an identity of the
 * same charge to exercise registration churn; the journal changes only after a registration or
 * removal succeeded.
 */
class Registry {
  /**
   * @param {object} client a public client with the additive registration methods
   * @param {string} kind its kind, part of every name
   * @param {number} [count] canonical descriptors
   * @param {{now:function():number}} [clock] monotonic time for the replacement journal
   */
  constructor(
    client,
    kind,
    count = DESCRIPTORS,
    clock = { now: () => performance.now() },
    fence = () => null
  ) {
    this.client = client
    this.kind = kind
    this.count = count
    this.clock = clock
    this.fence = fence
    this.variant = 'a'
    this.registered = []
    this.bytes = 0
    this.replacements = []
  }

  /**
   * @param {number} index descriptor number @param {string} [variant] identity variant of slot 0
   * @returns {object} the legal numeric descriptor
   */
  descriptor(index, variant = 'a') {
    return {
      name: `com.example.soak.${this.kind}.${String(index).padStart(3, '0')}`,
      type: 'sum',
      unit: '1',
      value: BigInt(index + 1),
      attributes: { slot: String(index), variant: index === 0 ? variant : 'a' },
    }
  }

  /**
   * @param {object} descriptor a descriptor @returns {number} its metadata charge in bytes
   */
  charge(descriptor) {
    return (
      Buffer.byteLength(descriptor.name) +
      Buffer.byteLength(descriptor.unit) +
      Object.entries(descriptor.attributes).reduce(
        (sum, [key, value]) => sum + Buffer.byteLength(key) + Buffer.byteLength(value),
        0
      )
    )
  }

  /** @returns {void} registers the canonical population @throws {Error} the first refusal */
  fill() {
    for (let i = 0; i < this.count; i++) this.add(this.descriptor(i))
  }

  /**
   * @param {object} descriptor to register @returns {void} journals it only on success
   */
  add(descriptor) {
    this.client.registerMetricForSubscription(descriptor)
    this.registered.push(descriptor)
    this.bytes += this.charge(descriptor)
  }

  /**
   * @param {object} descriptor to remove @returns {void} journals the removal only on success
   */
  remove(descriptor) {
    this.client.unregisterMetricFromSubscription({
      name: descriptor.name,
      attributes: descriptor.attributes,
    })
    this.registered = this.registered.filter((entry) => entry !== descriptor)
    this.bytes -= this.charge(descriptor)
  }

  /**
   * Swaps slot 0 for another attribute identity of the same charge: an unregistration followed by a
   * registration, never a same-identity update. Both write fences are journalled, each the
   * observer cursors of every live segment at the moment the call returned, so a replay places
   * every later Push on the correct side of each call without any elapsed-time grace.
   * @param {number} [tick] the tick that asked for the swap
   * @returns {{type:'replace', tick:number|null, removedVariant:string, addedVariant:string,
   *   unregisteredAt:number, registeredAt:number, unregisterFence:object, registerFence:object,
   *   at:number}} the journal entry
   */
  replace(tick = null) {
    const next = this.variant === 'a' ? 'b' : 'a'
    const entry = { type: 'replace', tick, removedVariant: this.variant, addedVariant: next }
    this.remove(this.registered.find((d) => d.name === this.descriptor(0).name))
    entry.unregisteredAt = this.clock.now()
    entry.unregisterFence = this.fence()
    this.add(this.descriptor(0, next))
    entry.registeredAt = this.clock.now()
    entry.registerFence = this.fence()
    this.variant = next
    entry.at = entry.registeredAt
    this.replacements.push(entry)
    return entry
  }

  /** @returns {void} puts the canonical identity back, so every checkpoint compares one registry */
  restore() {
    if (this.variant !== 'a') this.replace(null)
  }

  /**
   * @param {string} uuid the retained client instance ID
   * @returns {string} a fingerprint of the canonical descriptors, their charge and the UUID
   */
  digest(uuid) {
    const canon = this.registered
      .map((d) => [d.name, d.type, d.unit, d.attributes])
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
    return crypto
      .createHash('sha256')
      .update(JSON.stringify({ kind: this.kind, uuid, bytes: this.bytes, canon }))
      .digest('hex')
  }
}

/**
 * Pure predicates over one segment's wire records (the observer's own transcript, live or
 * archived). The worker uses them to claim proofs while the run is live; the verifier uses the very
 * same predicates on the sealed archive, so a claim is only ever as good as the bytes it names.
 */
class TelemetryProofs {
  /**
   * @param {object[]} records one segment's transcript records
   * @param {string} uuid the client's assigned instance ID
   * @returns {object[]} successful Gets that carried and returned that UUID, in order
   */
  static gets(records, uuid) {
    return records.filter(
      (r) =>
        r.type === 'request' &&
        r.apiKey === 71 &&
        r.uuid === uuid &&
        r.response?.errorCode === 0 &&
        r.response.uuid === uuid
    )
  }

  /**
   * @param {object[]} records one segment's transcript records
   * @returns {object[]} accepted, nonempty, nonterminating Pushes, in order
   */
  static accepted(records) {
    return records.filter(
      (r) =>
        r.type === 'request' &&
        r.apiKey === 72 &&
        !r.terminating &&
        r.response?.errorCode === 0 &&
        r.metricsBytes > 0
    )
  }

  /**
   * @param {object[]} records one segment's transcript records
   * @param {object} push a Push of those records
   * @returns {object|undefined} the last successful Get on the Push's own socket before it was written
   */
  static governing(records, push) {
    return records
      .filter(
        (r) =>
          r.type === 'request' &&
          r.apiKey === 71 &&
          r.socketId === push.socketId &&
          r.response?.errorCode === 0 &&
          r.response.responseAt <= push.writeAt
      )
      .at(-1)
  }

  /**
   * @param {object[]} records one segment's transcript records @param {number} at monotonic time
   * @returns {object|undefined} the Get whose selection is in force then: the last successful Get
   *   answered at or before that time
   */
  static inForce(records, at) {
    return records
      .filter(
        (r) =>
          r.type === 'request' &&
          r.apiKey === 71 &&
          r.response?.errorCode === 0 &&
          r.response.responseAt <= at
      )
      .at(-1)
  }

  /**
   * @param {{records:object[], closures:object[]}} transcript one segment's transcript
   * @returns {boolean} whether its one public close owes a final: the selection in force when the
   *   close began is nonempty
   */
  static owesFinal({ records, closures }) {
    const [closure] = closures ?? []
    if (!closure) return false
    return (
      (TelemetryProofs.inForce(records, closure.at)?.response.requestedMetrics?.length ?? 0) > 0
    )
  }

  /**
   * The first accepted nonempty nonterminating Push on the socket of a replacement Get, written
   * strictly after that Get's response, later in request order, under a successful subscription of
   * the same client.
   * @param {object[]} records one segment's transcript records
   * @param {object} get the replacement Get @returns {object|undefined} the proving Push
   */
  static provingPush(records, get) {
    return TelemetryProofs.accepted(records).find(
      (push) =>
        push.socketId === get.socketId &&
        push.index > get.index &&
        push.writeAt > get.response.responseAt &&
        push.uuid === get.response.uuid &&
        TelemetryProofs.governing(records, push)?.response.uuid === push.uuid
    )
  }

  /**
   * @param {object[]} records one segment's transcript records @param {string} uuid assigned UUID
   * @param {{after?:number, selection:string[]}} want the earliest write time and the exact list
   * @returns {object|undefined} the first successful Get that proves the selection: written at or
   *   after `after`, carrying and returning the UUID, with exactly that requested list and 1000 ms
   */
  static selectionGet(records, uuid, { after = -Infinity, selection }) {
    return TelemetryProofs.gets(records, uuid).find(
      (get) =>
        get.writeAt >= after &&
        get.response.pushIntervalMs === PUSH_INTERVAL_MS &&
        JSON.stringify(get.response.requestedMetrics) === JSON.stringify(selection)
    )
  }

  /**
   * The declared proof selection: a pure function of one segment's transcript. It names the first
   * accepted nonempty Push of every telemetry socket that had a successful Get, the first accepted
   * Push governed by each successful custom-prefix Get, and the segment's accepted nonempty final,
   * the terminal segment's included. Every other accepted Push keeps its bytes, response, node,
   * subscription and sink join but is not fully decoded.
   * @param {object[]} records one segment's transcript records
   * @returns {string[]} the request keys to decode, sorted
   */
  static selection(records) {
    const keys = new Set()
    const pushes = TelemetryProofs.accepted(records)
    const seenSockets = new Set()
    const seenCustom = new Set()
    for (const push of pushes) {
      const get = TelemetryProofs.governing(records, push)
      if (!get) continue
      if (!seenSockets.has(push.socketId)) {
        seenSockets.add(push.socketId)
        keys.add(push.requestKey)
      }
      if (
        get.response.requestedMetrics.some((p) => p.startsWith(CUSTOM_PREFIX)) &&
        push.subscriptionId === get.response.subscriptionId &&
        !seenCustom.has(get.requestKey)
      ) {
        seenCustom.add(get.requestKey)
        keys.add(push.requestKey)
      }
    }
    for (const r of records)
      if (
        r.type === 'request' &&
        r.apiKey === 72 &&
        r.terminating &&
        r.response?.errorCode === 0 &&
        r.metricsBytes > 0
      )
        keys.add(r.requestKey)
    return [...keys].sort()
  }
}

/**
 * @param {number[]} indexes record positions @returns {string[]} violations of "exactly 0..records-1"
 */
const identityViolations = (indexes, label) => {
  const seen = new Set()
  const out = []
  for (const index of indexes) {
    if (!Number.isInteger(index) || index < 0 || index >= CORPUS.records)
      out.push(`${label} holds an identity outside the corpus`)
    else if (seen.has(index)) out.push(`${label} repeats an identity`)
    seen.add(index)
  }
  if (seen.size !== CORPUS.records) out.push(`${label} identities are incomplete`)
  return [...new Set(out)]
}

/**
 * The independent raw-log reading of one corpus topic in its archived, replayable shape.
 * @param {Map<number,{records:object[]}>} log the raw log by partition, as `readLog` returns it
 * @param {string} stem run prefix @param {string} kind the corpus kind
 * @returns {{partitions:Object<string,number[][]>, valuesSha256:Object<string,string>,
 *   ends:Object<string,string>}} `[offset, index]` rows, the hash of each partition's value bytes
 *   in offset order, and the log end of each partition
 */
const logInputs = (log, stem, kind) => {
  const partitions = {}
  const valuesSha256 = {}
  const ends = {}
  for (const [partition, { records }] of log) {
    const rows = records.map((r) => [Number(r.offset), indexOf(r.key)]).sort((a, b) => a[0] - b[0])
    const hash = crypto.createHash('sha256')
    for (const r of [...records].sort((a, b) => Number(a.offset) - Number(b.offset)))
      hash.update(Buffer.from(r.value ?? ''))
    partitions[partition] = rows
    valuesSha256[partition] = hash.digest('hex')
    ends[partition] = String(rows.length ? rows.at(-1)[0] + 1 : 0)
  }
  return { partitions, valuesSha256, ends }
}

/**
 * Recomputes one corpus topic's durable-identity conclusions from archived inputs alone: the log
 * holds each of the 10000 identities once, from offset 0 without gaps, with the exact value bytes.
 * @param {object} inputs `logInputs` output plus `stem` and `kind`
 * @returns {{violations:string[], indexes:number[], byOffset:Map<string,number>}} the conclusions
 */
const checkLog = ({ partitions, valuesSha256, ends, stem, kind }) => {
  const violations = []
  const indexes = []
  const byOffset = new Map()
  for (const [partition, rows] of Object.entries(partitions)) {
    const hash = crypto.createHash('sha256')
    rows.forEach(([offset, index], position) => {
      if (offset !== position) violations.push(`log ${partition} has a gap or reorder at ${offset}`)
      indexes.push(index)
      byOffset.set(`${partition}:${offset}`, index)
      hash.update(valueOf(recordId(stem, kind, index)))
    })
    if (hash.digest('hex') !== valuesSha256?.[partition])
      violations.push(`log ${partition} value bytes differ from the corpus`)
    if (ends?.[partition] !== String(rows.length))
      violations.push(`log ${partition} end differs from its records`)
  }
  violations.push(...identityViolations(indexes, 'durable'))
  return { violations: [...new Set(violations)], indexes, byOffset }
}

/**
 * Compares the independent identity sets of the ordinary work: what was offered, what the broker
 * resolved, what the raw log holds, what the handler entered and what was committed. Every set
 * must equal its expectation exactly and nothing may be replayed.
 */
class OrdinaryOracle {
  /**
   * @param {object} input independent readings, all archived by the caller
   * @param {'producer'|'consumer'} input.kind the ordinary kind
   * @param {string} input.stem run prefix
   * @param {object} input.log `logInputs` output
   * @param {number[]} [input.offered] producer offers, by corpus position
   * @param {number[]} [input.resolved] producer resolutions, by corpus position
   * @param {number[][]} [input.handled] `[partition, offset, index, enteredAt, exitedAt]` entries
   * @param {Object<string,string>} [input.processed] the handler's next offset per partition
   * @param {Object<string,string>} [input.committed] raw broker committed next offsets
   * @returns {{ok:boolean, violations:string[], hashes:object}} the verdict with set hashes
   */
  static check({ kind, stem, log, offered, resolved, handled, processed, committed }) {
    const durable = checkLog({ ...log, stem, kind })
    const violations = [...durable.violations]
    const hashes = { durable: hashIds(durable.indexes.map(String)) }
    if (kind === 'producer') {
      violations.push(...identityViolations(offered ?? [], 'offered'))
      violations.push(...identityViolations(resolved ?? [], 'resolved'))
      hashes.offered = hashIds((offered ?? []).map(String))
      hashes.resolved = hashIds((resolved ?? []).map(String))
    } else {
      const entries = handled ?? []
      violations.push(
        ...identityViolations(
          entries.map((e) => e[2]),
          'handler'
        )
      )
      for (const [partition, offset, index] of entries)
        if (durable.byOffset.get(`${partition}:${offset}`) !== index)
          violations.push('a handler entry differs from the raw log at its offset')
      hashes.handled = hashIds(entries.map((e) => String(e[2])))
      const keys = new Set([
        ...Object.keys(log.ends ?? {}),
        ...Object.keys(processed ?? {}),
        ...Object.keys(committed ?? {}),
      ])
      for (const key of keys) {
        if (processed?.[key] !== log.ends?.[key])
          violations.push(`processed end differs from the log end on ${key}`)
        if (committed?.[key] !== processed?.[key])
          violations.push(`committed end differs on ${key}`)
      }
    }
    return { ok: violations.length === 0, violations: [...new Set(violations)], hashes }
  }
}

/**
 * Pure re-derivations over archived share exchange facts: the lossless protocol facts of every
 * captured ShareFetch and ShareAcknowledge exchange (socket, correlation, request batches, response
 * and per-partition fetch and acknowledgement statuses, acquired ranges, frame hashes). Fetch
 * status and acknowledgement status stay separate: only a successful fetch partition acquires and
 * only a successful acknowledgement partition settles.
 */
class ShareExchanges {
  /**
   * Reads one completed or unanswered captured exchange into its protocol facts.
   * @param {{requestHex:string, responseHex?:string, at:number, responseAt?:number}} frame the
   *   captured frame pair @param {{segment:string|null, socket:number|null}} owner its socket
   * @param {number} n the exchange's ordinal in this worker
   * @returns {object} the exchange facts, never the record bytes
   * @throws {Error} for a correlation mismatch or an unreadable frame
   */
  static fact(frame, owner, n) {
    const requestBytes = Buffer.from(frame.requestHex, 'hex')
    const header = readRequestHeader(requestBytes)
    const wire = header.apiKey === 78 ? shareFetchWire : shareAckWire
    const request = wire.readRequest(requestBytes.subarray(header.byteLength))
    const fact = {
      n,
      segment: owner.segment,
      socket: owner.socket,
      apiKey: header.apiKey,
      apiVersion: header.apiVersion,
      correlationId: header.correlationId,
      at: frame.at,
      responseAt: frame.responseAt ?? null,
      requestSha256: sha(requestBytes),
      responseSha256: null,
      request: {
        groupId: request.groupId,
        isRenewAck: request.isRenewAck === true,
        topics: request.topics.map((topic) => ({
          topicId: topic.topicId,
          partitions: topic.partitions.map((p) => ({
            partition: p.partitionIndex,
            batches: (p.acknowledgementBatches ?? []).map((b) => ({
              firstOffset: String(b.firstOffset),
              lastOffset: String(b.lastOffset),
              types: [...b.acknowledgeTypes],
            })),
          })),
        })),
      },
      response: null,
    }
    if (!frame.responseHex) return fact
    const responseBytes = Buffer.from(frame.responseHex, 'hex')
    if (responseBytes.readInt32BE(4) !== header.correlationId)
      throw new Error('A share response answers another request')
    const response = acknowledgementResponse(header.apiKey, responseBytes)
    fact.responseSha256 = sha(responseBytes)
    fact.response = {
      errorCode: response.errorCode,
      lockMs: response.acquisitionLockTimeoutMs,
      topics: response.topics.map((topic) => ({
        topicId: topic.topicId,
        partitions: topic.partitions.map((p) => ({
          partition: p.partitionIndex,
          fetchErrorCode: p.fetchErrorCode,
          acknowledgeErrorCode: p.acknowledgeErrorCode,
          acquired: p.acquired.map((a) => ({ ...a })),
        })),
      })),
    }
    return fact
  }

  /**
   * @param {object[]} exchanges archived exchange facts @param {string} group the share group
   * @returns {object[]} every range acquired by a successful fetch partition, with the request and
   *   response times and the exchange it belongs to
   */
  static acquisitions(exchanges, group) {
    return exchanges
      .filter((e) => e.apiKey === 78 && e.request.groupId === group && e.response?.errorCode === 0)
      .flatMap((e) =>
        e.response.topics.flatMap((topic) =>
          topic.partitions
            .filter((p) => p.fetchErrorCode === 0)
            .flatMap((p) =>
              p.acquired.map((range) => ({
                ...range,
                topicId: topic.topicId,
                partition: p.partition,
                at: e.at,
                responseAt: e.responseAt,
                exchange: e.n,
              }))
            )
        )
      )
  }

  /**
   * @param {object[]} exchanges archived exchange facts @param {string} group the share group
   * @returns {{settled:object[], failed:object[]}} per-offset decisions the broker confirmed, as
   *   ranges, and the acknowledgement batches whose partition status or exchange failed
   */
  static decisions(exchanges, group) {
    const settled = []
    const failed = []
    for (const e of exchanges) {
      if (e.request.groupId !== group || e.request.isRenewAck) continue
      for (const topic of e.request.topics)
        for (const p of topic.partitions) {
          if (!p.batches.length) continue
          const replies = (e.response?.topics ?? [])
            .filter((t) => t.topicId === topic.topicId)
            .flatMap((t) => t.partitions)
            .filter((r) => r.partition === p.partition)
          const ok = e.response?.errorCode === 0 && replies.length === 1
          if (!ok || replies[0].acknowledgeErrorCode !== 0) {
            failed.push({
              exchange: e.n,
              topicId: topic.topicId,
              partition: p.partition,
              errorCode: ok ? replies[0].acknowledgeErrorCode : (e.response?.errorCode ?? null),
            })
            continue
          }
          const facts = []
          for (const b of p.batches)
            for (let o = BigInt(b.firstOffset); o <= BigInt(b.lastOffset); o++) {
              if (facts.length >= CORPUS.records) throw new Error('Unbounded share range')
              facts.push({
                topicId: topic.topicId,
                partition: p.partition,
                offset: String(o),
                type:
                  b.types.length === 1 ? b.types[0] : b.types[Number(o - BigInt(b.firstOffset))],
                errorCode: 0,
                at: e.responseAt,
                exchange: e.n,
              })
            }
          settled.push(...compactRuns(facts))
        }
    }
    return { settled, failed }
  }
}

/**
 * Recomputes the share conclusions from archived exchange facts: the delivered accepted-prefix and
 * rest-release behaviour. Acquisitions and broker settlements are re-derived, never copied. Every
 * handler entry joins the raw log at its own offset and the one acquisition it ran under; every
 * decision settles the acquisition it follows and precedes the next; every offset is accepted once
 * at the broker and publicly; releases are the only reason for a second delivery; no accepted work
 * is delivered again; at every public close each unsettled offset ends in the decision its handler
 * progress demands; and nothing names work outside the corpus.
 */
class ShareOracle {
  /**
   * @param {object} input archived share inputs
   * @param {string} input.stem run prefix @param {object} input.log `logInputs` output
   * @param {string} input.topicId the corpus topic's ID @param {string} input.group the share group
   * @param {number[][]} input.handled `[partition, offset, index, enteredAt, exitedAt]`
   * @param {object[]} input.exchanges the archived exchange facts
   * @param {object[]} input.settled public acknowledge events with their time
   * @param {Array<{at:number,end:number}>} input.closures captured public close boundaries
   * @returns {{ok:boolean, violations:string[], hashes:object}} the verdict with set hashes
   */
  static check({ stem, log, topicId, group, handled, exchanges, settled, closures }) {
    const durable = checkLog({ ...log, stem, kind: 'shareConsumer' })
    const violations = [...durable.violations]
    const offsets = [...durable.byOffset.keys()]
    const corpus = new Set(offsets)
    const each = (ranges, work) => {
      for (const range of ranges) {
        let count = 0
        for (let n = BigInt(range.firstOffset); n <= BigInt(range.lastOffset); n++) {
          if (++count > CORPUS.records) throw new Error('Unbounded share range')
          const key = `${range.partition}:${n}`
          if (!corpus.has(key)) violations.push('a share fact names an offset outside the corpus')
          work(key, range)
        }
      }
    }
    const { settled: settlements, failed } = ShareExchanges.decisions(exchanges, group)
    const acquired = new Map()
    const accepted = new Map()
    const released = new Map()
    const decided = new Map()
    const publics = new Map()
    const push = (map, key, value) => map.set(key, [...(map.get(key) ?? []), value])
    each(ShareExchanges.acquisitions(exchanges, group), (key, r) => {
      if (r.topicId !== topicId) violations.push('an acquisition names a foreign topic')
      push(acquired, key, r)
    })
    each(settlements, (key, r) => {
      if (r.topicId !== topicId) violations.push('a settlement names a foreign topic')
      push(decided, key, r)
      if (r.type === 1) push(accepted, key, r)
      else if (r.type === 2) push(released, key, r)
      else violations.push('an offset was rejected or settled with an unknown type')
    })
    each(
      settled.filter((r) => ['accept', 'release'].includes(r.type)),
      (key, r) => {
        if (r.error !== null) violations.push('a public acknowledgement carried an error')
        push(publics, key, r)
      }
    )
    const entries = new Map()
    for (const [partition, offset, index, enteredAt, exitedAt] of handled) {
      const key = `${partition}:${offset}`
      if (!corpus.has(key)) violations.push('a handler entry names foreign work')
      else if (durable.byOffset.get(key) !== index)
        violations.push('a handler entry differs from the raw log at its offset')
      push(entries, key, { index, enteredAt, exitedAt })
    }
    for (const key of offsets) {
      const deliveries = (acquired.get(key) ?? []).sort((a, b) => a.at - b.at)
      const decisions = (decided.get(key) ?? []).sort((a, b) => a.at - b.at)
      const accepts = accepted.get(key) ?? []
      const releases = released.get(key) ?? []
      const events = publics.get(key) ?? []
      const seen = entries.get(key) ?? []
      if (accepts.length !== 1) violations.push('an offset lacks exactly one broker accept')
      if (decisions.length && decisions.at(-1).type !== 1)
        violations.push('an offset was settled again after its accept')
      if (deliveries.length !== 1 + releases.length)
        violations.push('deliveries differ from one plus the releases')
      deliveries.forEach((r, position) => {
        if (r.deliveryCount !== position + 1) violations.push('a delivery count is not sequential')
        const decision = decisions[position]
        const next = deliveries[position + 1]
        const crosses = next && decision?.at > next.at && decision.exchange !== next.exchange
        if (decision && (decision.at < r.responseAt || crosses))
          violations.push('a decision is attached to another acquisition')
        const window = seen.filter(
          (e) => e.enteredAt >= r.responseAt && (!decision || e.enteredAt <= decision.at)
        )
        if (window.length > 1) violations.push('an acquisition was handled twice')
      })
      for (const e of seen)
        if (
          !deliveries.some(
            (r, position) =>
              e.enteredAt >= r.responseAt &&
              (!decisions[position] || e.enteredAt <= decisions[position].at)
          )
        )
          violations.push('a handler entry is not joined to its acquisition')
      if (events.filter((e) => e.type === 'accept').length !== 1)
        violations.push('an offset lacks exactly one public accept')
      if (events.filter((e) => e.type === 'release').length !== releases.length)
        violations.push('public releases differ from broker releases')
      if (!seen.length || seen.length > 1 + releases.length)
        violations.push('handler entries differ from one plus the releases')
      if (accepts.length === 1 && seen.some((e) => e.enteredAt > accepts[0].at))
        violations.push('accepted work was delivered again')
      for (const closure of closures) {
        const first = decisions.find((d) => d.at > closure.at)
        if (!first || !deliveries.some((d) => d.at <= closure.end)) continue
        const entered = seen.filter((e) => e.enteredAt <= closure.end)
        const prefix =
          entered.length > 0 &&
          entered.every((e) => e.exitedAt !== null && e.exitedAt <= closure.end)
        if (decisions.some((d) => d.at <= closure.at)) continue
        if (first.at > closure.end) violations.push('an offset was unsettled when its close ended')
        else if (first.type !== (prefix ? 1 : 2))
          violations.push('the close did not accept the handled prefix and release the rest')
      }
    }
    return {
      ok: violations.length === 0,
      violations: [...new Set(violations)],
      hashes: {
        durable: hashIds(durable.indexes.map(String)),
        handled: hashIds(handled.map((e) => `${e[0]}:${e[1]}:${e[2]}`)),
        exchanges: sha(JSON.stringify(exchanges)),
      },
      failedAcknowledgements: failed.length,
    }
  }
}

/**
 * The classifying socket factory of one worker. Every socket is a chaos socket over the socket of
 * the observer segment the owner has selected. The first request frame of API 71 or 72 marks that
 * very socket as the telemetry transport; one such open socket can be retired, leaving core sockets
 * untouched. A closed socket is released at once, so only scalar facts outlive it. Socket history
 * is bounded by the churn segment cap times the observer socket cap; overflow fails before dialing.
 */
class TelemetryTap {
  /**
   * @param {TelemetrySegments} segments the owner whose selected segment receives every socket
   * @param {{share?:ShareLedger, clock?:{now:function():number}}} [options] a ledger that also
   *   wraps every socket of a share consumer
   */
  constructor(segments, { share = null, clock = { now: () => performance.now() } } = {}) {
    this.chaos = createChaosSocketFactory((options) => segments.socketFactory(options), {
      retainClosed: false,
    })
    this.segments = segments
    this.share = share
    this.clock = clock
    this.records = []
    this.factory = (options) => {
      if (this.records.length >= scheduleOf(SCENARIO).cap * OBSERVER_LIMITS.sockets)
        throw new Error('Telemetry tap socket history bound exceeded')
      return this.track(this.chaos(options), segments.current())
    }
  }

  /**
   * @param {object} socket the chaos socket just issued @param {object} segment the segment whose
   *   observer received it @returns {object} the same socket, classified
   */
  track(socket, segment) {
    const record = {
      segment: segment.id,
      ordinal: segment.observer.socketOwnership().length,
      socket,
      telemetry: false,
      closed: false,
      closedAt: null,
    }
    const classifier = new FrameFilter({
      headerBytes: 12,
      decide: (header) => {
        if (header.length >= 12 && [71, 72].includes(header.readInt16BE(4))) {
          record.telemetry = true
          if (record.armed) record.hit = true
        }
        return false
      },
    })
    const write = socket.write
    const tap = this
    socket.write = function (data, encoding) {
      classifier.push(Buffer.from(data, encoding))
      const result = write.call(this, data, encoding)
      if (record.armed && record.hit) {
        record.armed = false
        record.hit = false
        const at = tap.clock.now()
        const faulted = record.faulted
        record.faulted = null
        this.destroy(new Error('telemetry transport retired'))
        faulted(at)
      }
      return result
    }
    record.restore = () => {
      delete socket.write
    }
    if (this.share) this.share.wrap(socket, { segment: segment.id, ordinal: record.ordinal })
    socket.on('close', () => {
      record.closed = true
      record.closedAt = this.clock.now()
      record.restore?.()
      record.restore = null
      record.socket = null
      record.faulted = null
    })
    this.records.push(record)
    return socket
  }

  /** @returns {object[]} open sockets proven by an actual 71/72 frame to be the telemetry transport */
  telemetry() {
    return this.records.filter((r) => r.telemetry && !r.closed)
  }

  /** @returns {number} how many sockets are still open, of any kind */
  open() {
    return this.records.filter((r) => !r.closed).length
  }

  /**
   * Declares the one captured telemetry socket as faulted and destroys it right after the next
   * telemetry request it carries is written, so exactly one exchange is cut off. Core sockets are
   * never a target.
   * @returns {{segment:string, ordinal:number, declaredAt:number, hit:Promise<number>}} what was
   *   declared; `hit` resolves with the time the socket was destroyed
   * @throws {Error} unless exactly one telemetry socket is open in the selected segment
   */
  retire() {
    const open = this.telemetry()
    const current = this.segments.current()
    if (open.length !== 1 || open[0].segment !== current.id)
      throw new Error('Expected exactly one open telemetry socket in the selected segment')
    const [target] = open
    const hit = new Promise((resolve) => {
      target.faulted = resolve
    })
    const declaredAt = this.clock.now()
    current.observer.declareFault(target.ordinal)
    target.hit = false
    target.armed = true
    return { segment: target.segment, ordinal: target.ordinal, declaredAt, hit }
  }

  /** @returns {void} removes every classifier patch still installed */
  restore() {
    for (const record of this.records) {
      record.restore?.()
      record.restore = null
    }
  }
}

/**
 * The share consumer's raw exchange facts. Every socket gets its own existing trace; each
 * completed exchange is read into lossless protocol facts (see `ShareExchanges.fact`), checked
 * against the existing independent 9E readers, and its frames are forgotten, so the trace never
 * grows with the corpus. An exchange still unanswered when its socket closed is kept with no
 * response. The public acknowledge events and the handler progress are separate facts.
 */
class ShareLedger {
  /**
   * @param {object} options
   * @param {object} [options.storage] schedule-derived journal capacities
   * @param {string} options.group the share group @param {{now:function():number}} options.clock
   */
  constructor({ group, clock, storage = journalLimits() }) {
    this.group = group
    this.clock = clock
    this.traces = []
    this.exchanges = new FixedJournal({
      name: 'share exchanges',
      packed: true,
      capacity: storage.exchanges,
      bytes: storage.exchangeBytes,
    })
    this.settled = new FixedJournal({
      name: 'share acknowledgements',
      packed: true,
      capacity: storage.acknowledgements,
      bytes: storage.acknowledgementBytes,
    })
  }

  /**
   * @param {object} socket one socket of the share consumer
   * @param {{segment?:string, ordinal?:number}} [owner] the segment and socket it belongs to
   * @returns {object} the same socket
   */
  wrap(socket, owner = {}) {
    const entry = {
      segment: owner.segment ?? null,
      socket: owner.ordinal ?? null,
      trace: new ShareTrace(),
      closed: false,
    }
    entry.trace.wrap(socket)
    socket.once('close', () => {
      entry.closed = true
    })
    this.traces.push(entry)
    return socket
  }

  /**
   * Reads every completed exchange into its facts and forgets its frames; a closed socket's
   * unanswered exchanges are kept without a response and its trace is released.
   * @returns {void}
   * @throws {Error} for a capture failure, an unreadable frame, or facts that differ from what the
   *   existing readers derive from the same frame
   */
  drain() {
    for (const entry of this.traces) {
      if (entry.trace.failures.length) throw new Error(entry.trace.failures[0])
      const done = entry.trace.frames.filter((frame) => frame.responseHex || entry.closed)
      entry.trace.frames = entry.trace.frames.filter((frame) => !done.includes(frame))
      for (const frame of done) {
        const fact = ShareExchanges.fact(frame, entry, this.exchanges.length + 1)
        if (frame.responseHex) this.agree(frame, fact)
        this.exchanges.push(fact)
      }
    }
    this.traces = this.traces.filter((entry) => !entry.closed)
  }

  /**
   * @param {object} frame a completed frame pair @param {object} fact its exchange facts
   * @returns {void} @throws {Error} unless the facts re-derive exactly what the 9E readers read
   */
  agree(frame, fact) {
    const strip = (rows) =>
      rows.map(({ topicId, partition, firstOffset, lastOffset, deliveryCount, offset, type }) => ({
        topicId,
        partition,
        firstOffset,
        lastOffset,
        deliveryCount,
        offset,
        type,
      }))
    const held = strip(heldAcquisitions([frame], this.group))
    const derived = strip(ShareExchanges.acquisitions([fact], this.group))
    const confirmed = strip(confirmedDecisions([frame], this.group))
    const settled = strip(
      ShareExchanges.decisions([fact], this.group).settled.flatMap((range) => {
        const out = []
        for (let o = BigInt(range.firstOffset); o <= BigInt(range.lastOffset); o++)
          out.push({ ...range, offset: String(o), firstOffset: undefined, lastOffset: undefined })
        return out
      })
    )
    if (JSON.stringify(held) !== JSON.stringify(derived))
      throw new Error('Share acquisition facts differ from the raw reader')
    if (JSON.stringify(confirmed) !== JSON.stringify(settled))
      throw new Error('Share settlement facts differ from the raw reader')
  }

  /**
   * @param {object} event a public acknowledge event @returns {void} retained with its time
   */
  record(event) {
    this.settled.push({
      topic: event.payload.topic,
      partition: event.payload.partition,
      firstOffset: event.payload.firstOffset,
      lastOffset: event.payload.lastOffset,
      type: event.payload.type,
      error: event.payload.error
        ? { name: event.payload.error.name, reason: event.payload.error.reason }
        : null,
      at: this.clock.now(),
    })
  }

  /** @returns {object} fixed storage and append cursors; never materialises the facts */
  journalState() {
    return journalState([this.exchanges, this.settled])
  }

  /** @returns {object} the archived facts, after a final drain, with the hash of the exchanges */
  facts() {
    this.drain()
    const exchanges = this.exchanges.values()
    return {
      group: this.group,
      exchanges,
      exchangesSha256: sha(JSON.stringify(exchanges)),
      settled: this.settled.values(),
    }
  }
}

/**
 * The ordinary public work of one worker kind, independent of telemetry, confined to the
 * measurement. The producer offers its finite corpus in fixed slots, one batch per slot; the
 * consumers only handle what the parent offers. Admission is paused, in-flight work finished and
 * commits confirmed before a public close, and resumed only after the reconnect. Every identity
 * offered, resolved, handled and committed is kept as an independent ledger.
 */
class OrdinaryWork {
  /**
   * @param {object} options
   * @param {string} options.kind the client kind @param {object} options.client the target client
   * @param {string} options.stem run prefix @param {string} options.topic the corpus topic
   * @param {string} options.ctl the control topic for work that is not corpus
   * @param {{now:()=>number}} options.clock monotonic time
   * @param {(ms:number)=>Promise<void>} [options.sleep] real delay
   * @param {ShareLedger} [options.ledger] the share ledger of a share consumer
   * @param {number[]} [options.slots] producer batch slots, in ms after the corpus start
   * @param {object} [options.storage] schedule-derived journal capacities
   */
  constructor({
    kind,
    client,
    stem,
    topic,
    ctl,
    clock,
    sleep = wait,
    ledger = null,
    slots = [],
    storage = journalLimits(),
  }) {
    Object.assign(this, { kind, client, stem, topic, ctl, clock, sleep, ledger, slots })
    this.running = false
    this.paused = false
    this.loop = null
    this.inflight = null
    this.active = 0
    this.lastExit = -Infinity
    const ids = (name) =>
      new FixedJournal({ name, capacity: storage.records, bytes: storage.records * 16 })
    this.offered = ids('offered')
    this.resolved = ids('resolved')
    this.handled = new HandlerJournal(CORPUS.records, storage.records)
    this.processed = new Map()
    this.committed = new Map()
    this.commits = new FixedJournal({
      name: 'commits',
      capacity: storage.receipts,
      bytes: storage.receipts * 256,
    })
    this.pauses = new FixedJournal({
      name: 'pauses',
      capacity: storage.receipts,
      bytes: storage.receipts * 256,
    })
    this.late = new FixedJournal({
      name: 'producer slots',
      capacity: storage.batches,
      bytes: storage.batches * 16,
    })
    this.failures = []
    this.listening = false
  }

  /**
   * @returns {Promise<void>} subscribes and runs the consuming kinds, again after each reopen
   * @throws {Error} a refusal of subscribe or run
   */
  async attach() {
    const handle = async ({ partition, message }) => {
      this.active += 1
      const row = this.handled.enter(
        partition,
        message.offset,
        indexOf(String(message.key)),
        this.clock.now()
      )
      if (this.kind === 'consumer')
        this.processed.set(String(partition), String(BigInt(message.offset) + 1n))
      this.lastExit = this.clock.now()
      this.handled.exit(row, this.lastExit)
      this.active -= 1
    }
    if (this.kind === 'shareConsumer') {
      if (!this.listening) {
        this.client.on(this.client.events.ACKNOWLEDGE, (event) => {
          this.ledger.record(event)
          this.ledger.drain()
        })
        this.client.on(this.client.events.FETCH, () => this.ledger.drain())
        this.listening = true
      }
      await this.client.subscribe({ topics: [this.topic] })
      await this.client.run({ eachMessage: handle })
    } else {
      await this.client.subscribe({ topic: this.topic, fromBeginning: true })
      await this.client.run({ autoCommit: false, eachMessage: handle })
    }
  }

  /**
   * Begins the kind's idle work: the consumers attach, the admin starts its paced describes.
   * @returns {Promise<void>} after the first attach
   */
  async start() {
    this.running = true
    if (this.kind === 'consumer' || this.kind === 'shareConsumer') await this.attach()
    if (this.kind === 'admin') {
      this.loop = this.poll()
      this.loop.catch(() => {})
    }
  }

  /**
   * Starts the measured corpus: only the producer offers records here.
   * @returns {void}
   */
  corpus() {
    if (this.kind !== 'producer' || this.loop) return
    this.loop = this.pace()
    this.loop.catch(() => {})
  }

  /** @returns {Promise<void>} the admin's paced metadata requests until `stop` */
  async poll() {
    while (this.running) {
      if (!this.paused) {
        this.inflight = this.client.describeCluster({ timeout: 3000 })
        try {
          await this.inflight
        } catch (error) {
          this.failures.push(error)
          this.running = false
          return
        }
      }
      await this.sleep(CORPUS.batchMs)
    }
  }

  /**
   * Offers the corpus batch by batch, each no earlier than its fixed slot and none while paused.
   * A batch the pause delayed is sent as soon as admission resumes, one batch at a time.
   * @returns {Promise<void>} after the last batch or at `stop`
   */
  async pace() {
    const start = this.clock.now()
    for (let batch = 0; batch < this.slots.length && this.running; batch++) {
      const due = start + (this.slots[batch] - this.slots[0])
      while (this.running && (this.clock.now() < due || this.paused))
        await this.sleep(Math.min(100, Math.max(1, due - this.clock.now())))
      if (!this.running) return
      const ids = Array.from({ length: CORPUS.batch }, (_, i) => batch * CORPUS.batch + i)
      this.late.push(Math.max(0, this.clock.now() - due))
      this.offered.push(...ids)
      this.inflight = this.client.send({
        topic: this.topic,
        acks: -1,
        messages: ids.map((i) => {
          const id = recordId(this.stem, 'producer', i)
          return { key: id, value: valueOf(id) }
        }),
      })
      try {
        await this.inflight
      } catch (error) {
        this.failures.push(error)
        this.running = false
        return
      }
      this.resolved.push(...ids)
    }
  }

  /**
   * Work that is not corpus, used while a telemetry socket is faulted: it must still succeed.
   * @param {number} n a unique number for this call
   * @returns {Promise<void>} after the kind's own ordinary operation, if it has one
   */
  async core(n) {
    if (this.kind === 'admin') await this.client.describeCluster({ timeout: 3000 })
    else if (this.kind === 'producer')
      await this.client.send({
        topic: this.ctl,
        acks: -1,
        messages: [{ key: `${this.stem}:core:${n}`, value: Buffer.from('core') }],
      })
  }

  /** @returns {Promise<void>} commits the processed next offsets of the classic consumer */
  async commit() {
    const offsets = [...this.processed]
      .filter(([partition, offset]) => this.committed.get(partition) !== offset)
      .map(([partition, offset]) => ({
        topic: this.topic,
        partition: Number(partition),
        offset,
      }))
    if (!offsets.length) return
    await this.client.commitOffsets(offsets)
    for (const { partition, offset } of offsets) this.committed.set(String(partition), offset)
    this.commits.push({ at: this.clock.now(), count: offsets.length })
  }

  /**
   * Pauses admission, finishes in-flight work and confirms every commit, ahead of a public close.
   * @returns {Promise<{pausedAt:number, idleAt:number, confirmedAt:number}>} the barrier's receipt
   * @throws {Error} a failure of in-flight work or of the commit, or work that does not go idle
   */
  async pause() {
    const receipt = { pausedAt: this.clock.now(), idleAt: null, confirmedAt: null }
    this.paused = true
    if (this.inflight) await this.inflight
    if (this.kind === 'consumer') {
      await this.client.pause([{ topic: this.topic }])
      const deadline = this.clock.now() + 8000
      while (this.active > 0 || this.clock.now() - this.lastExit < 200) {
        if (this.clock.now() > deadline) throw new Error('The consumer handler did not go idle')
        await this.sleep(50)
      }
      receipt.idleAt = this.clock.now()
      await this.commit()
      receipt.confirmedAt = this.clock.now()
    } else {
      receipt.idleAt = this.clock.now()
      receipt.confirmedAt = receipt.idleAt
    }
    this.pauses.push(receipt)
    return receipt
  }

  /**
   * Resumes admission after the reconnect; a consuming kind runs again on the reconnected object.
   * @returns {Promise<void>} when admission is open again
   */
  async resume() {
    if (this.running && (this.kind === 'consumer' || this.kind === 'shareConsumer'))
      await this.attach()
    if (this.kind === 'consumer') await this.client.resume([{ topic: this.topic }])
    this.paused = false
  }

  /**
   * Stops admission, joins the loop and, for a consuming kind, waits until the whole corpus was
   * handled before confirming the last commit.
   * @param {number} ms how long the consumer may take to finish the corpus
   * @returns {Promise<void>} @throws {Error} the first ordinary failure, or an incomplete corpus
   */
  async stop(ms = 40000) {
    if (this.loop && this.kind === 'producer') await this.loop
    this.running = false
    if (this.loop) await this.loop
    if (this.failures.length) throw this.failures[0]
    if (this.kind === 'consumer' || this.kind === 'shareConsumer') {
      const deadline = this.clock.now() + ms
      while (this.handled.unique < CORPUS.records && this.clock.now() < deadline)
        await this.sleep(200)
      if (this.handled.unique < CORPUS.records)
        throw new Error('The corpus was not fully handled before the close')
      if (this.kind === 'consumer') await this.commit()
    }
  }

  /** @returns {object} allocated bytes and cursors without rebuilding the oracle */
  journalState() {
    return journalState([
      this.offered,
      this.resolved,
      this.handled,
      this.commits,
      this.pauses,
      this.late,
    ])
  }

  /**
   * @returns {object} the archived identity ledgers of this worker: offered and resolved positions,
   *   every handler entry, processed and committed ends, commit and pause receipts
   */
  ids() {
    return {
      kind: this.kind,
      offered: this.offered.values(),
      resolved: this.resolved.values(),
      handled: this.handled.values(),
      processed: Object.fromEntries(this.processed),
      committed: Object.fromEntries(this.committed),
      commits: this.commits.values(),
      pauses: this.pauses.values(),
      late: this.late.values(),
    }
  }
}

/**
 * The parent's own telemetry-disabled clients: the topics, the broker resource, the paced
 * producer of the consumer and share corpora, and the independent raw-log and offset readers.
 */
class ParentFixture {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses @param {string} options.stem run prefix
   * @param {object} options.kafka a `Kafka` whose clients opt out of telemetry
   * @param {Function} options.readLog the raw log reader @param {Function} options.withResource
   *   the owner of one initially absent broker resource
   * @param {{now:function():number}} [options.clock] monotonic time for the rows
   */
  constructor({
    brokers,
    stem,
    kafka,
    readLog,
    withResource,
    clock = { now: () => performance.now() },
  }) {
    Object.assign(this, { brokers, stem, kafka, readLog, withResource, clock })
    this.admin = kafka.admin({ enableMetricsPush: false })
    this.producer = kafka.producer({ enableMetricsPush: false, idempotent: true })
    this.rotationEntries = { 'interval.ms': '1000', match: `client_id=^${stem}-.*$` }
    this.shareConfigOwned = false
  }

  /** @param {string} kind the client kind @returns {string} its corpus topic */
  topic(kind) {
    return `${this.stem}-${labelOf(kind)}`
  }

  /** @param {string} kind @returns {string} its consumer group */
  group(kind) {
    return `${this.stem}-${labelOf(kind)}-group`
  }

  /** @returns {string} the topic of work that is not corpus: control and core probes */
  get ctl() {
    return `${this.stem}-ctl`
  }

  /**
   * Creates the corpus topics and reads back earliest share offsets before any worker joins.
   * @returns {Promise<void>} Connected clients, 3 partitions RF3, and an applied group override.
   * @throws {Error} On topic creation or group configuration failure.
   */
  async open() {
    await this.admin.connect()
    await this.producer.connect()
    await this.admin.createTopics({
      waitForLeaders: true,
      topics: [
        this.topic('producer'),
        this.topic('consumer'),
        this.topic('shareConsumer'),
        this.ctl,
      ].map((topic) => ({
        topic,
        numPartitions: 3,
        replicationFactor: 3,
        configEntries: [{ name: 'min.insync.replicas', value: '2' }],
      })),
    })
    this.shareConfigOwned = true
    await setShareGroupConfigs(this.admin, this.group('shareConsumer'), {
      'share.auto.offset.reset': 'earliest',
    })
  }

  /**
   * @param {function():Promise<void>} work runs while the resource exists
   * @returns {Promise<void>} after the resource was created, read back, and deleted again
   */
  inResource(work) {
    return this.withResource(
      this.admin,
      { name: this.stem, entries: { metrics: INITIAL_SELECTION, ...this.rotationEntries } },
      work
    )
  }

  /**
   * Sets the next subscription and polls successful stale readbacks until every value is exact.
   * The set, all reads and pauses share the parent's remaining rotation budget; API errors fail
   * immediately, and expiry stops polling with the last expected/observed mismatch.
   * @param {number} index the 10 s transition number, from 1 @param {string} metrics the new value
   * @param {number} [budgetMs] remaining time within the existing confirmation stage, at most 9 s
   * @returns {Promise<{index:number, selection:string, setAt:number, readbackAt:number,
   *   readback:string, readbackAttempts:number}>} the transition row, times on this process's clock
   * @throws {Error} on an API failure or when the budget expires without an exact readback
   */
  async rotate(index, metrics, budgetMs = STAGE_MS.confirm - 1000) {
    const resource = { type: 16, name: this.stem }
    const expected = { metrics, ...this.rotationEntries }
    const names = Object.keys(expected)
    const deadline = this.clock.now() + budgetMs
    const timeout = () => Math.max(1, Math.min(3000, Math.ceil(deadline - this.clock.now())))
    const label = 'The subscription set and readback'
    const owner = new AbortController()
    let entries
    let readbackAttempts = 0
    try {
      return await within(
        async () => {
          const set = await this.admin.incrementalAlterConfigs({
            timeout: timeout(),
            resources: [
              { ...resource, configEntries: [{ name: 'metrics', operation: 0, value: metrics }] },
            ],
          })
          owner.signal.throwIfAborted()
          const setAt = this.clock.now()
          const rows = set?.resources
          if (!Array.isArray(rows) || rows.length !== 1 || rows[0].errorCode !== 0)
            throw new Error('The subscription set was not accepted')
          let readbackAt
          await pollUntil(
            owner.signal,
            Infinity,
            async (scope) => {
              readbackAttempts += 1
              const answer = await this.admin.describeConfigs({
                timeout: timeout(),
                resources: [{ ...resource, configNames: names }],
              })
              scope.throwIfAborted()
              const read = checkedResource(answer, resource.name)
              entries = Object.fromEntries(
                read.configEntries.map((e) => [e.configName, e.configValue])
              )
              readbackAt = this.clock.now()
              return names.every((name) => entries[name] === expected[name])
            },
            { delayMs: 50 }
          )
          return {
            index,
            selection: metrics,
            setAt,
            readbackAt,
            readback: entries.metrics,
            readbackAttempts,
          }
        },
        budgetMs,
        label
      )
    } catch (error) {
      if (!entries || error?.message !== `${label} did not finish within ${budgetMs} ms`)
        throw error
      throw new Error(
        `The subscription readback differs: ${JSON.stringify({
          index,
          expected,
          observed: Object.fromEntries(
            names.map((name) => [
              name,
              entries[name] === undefined ? null : String(entries[name]).slice(0, 256),
            ])
          ),
        })}`
      )
    } finally {
      owner.abort()
    }
  }

  /**
   * Produces records `from`..`from + count` of one parent-owned corpus.
   * @param {string} kind consumer or shareConsumer @param {number} from first position
   * @param {number} count how many @returns {Promise<number[]>} the produced positions
   */
  async produce(kind, from, count) {
    const ids = Array.from({ length: count }, (_, i) => from + i)
    await this.producer.send({
      topic: this.topic(kind),
      acks: -1,
      messages: ids.map((i) => {
        const id = recordId(this.stem, kind, i)
        return { key: id, value: valueOf(id) }
      }),
    })
    return ids
  }

  /** @param {string} kind @returns {Promise<object>} `logInputs` of the topic's raw log */
  async durable(kind) {
    const log = await this.readLog({ brokers: this.brokers, topic: this.topic(kind) })
    return logInputs(log, this.stem, kind)
  }

  /** @returns {Promise<Object<string,string>>} the group's committed next offsets, unresolved */
  async committed() {
    const answer = await this.admin.fetchOffsets({
      groupId: this.group('consumer'),
      topics: [this.topic('consumer')],
      resolveOffsets: false,
    })
    return Object.fromEntries(
      answer.flatMap(({ partitions }) =>
        partitions.map((p) => [String(p.partition), String(p.offset)])
      )
    )
  }

  /** @returns {Promise<string>} the topic ID of the share corpus topic, from public metadata */
  async topicId() {
    const found = await this.admin.fetchTopicMetadata({ topics: [this.topic('shareConsumer')] })
    const topic = found.topics.find((t) => t.name === this.topic('shareConsumer'))
    if (!topic?.topicId) throw new Error('The share topic has no topic ID')
    return topic.topicId
  }

  /**
   * Restores the owned group override before disconnecting both clients, even after failed setup.
   * @returns {Promise<void>} Every cleanup attempted.
   * @throws {AggregateError} All restoration and disconnect failures.
   */
  async close() {
    const errors = []
    if (this.shareConfigOwned) {
      try {
        await resetShareGroupConfigs(this.admin, this.group('shareConsumer'), [
          'share.auto.offset.reset',
        ])
        this.shareConfigOwned = false
      } catch (error) {
        errors.push(error)
      }
    }
    const outcomes = await Promise.allSettled([this.producer.disconnect(), this.admin.disconnect()])
    errors.push(...outcomes.filter((o) => o.status === 'rejected').map((o) => o.reason))
    if (errors.length) throw new AggregateError(errors, 'Fixture cleanup failed')
  }
}

/**
 * The parent's independent decoding of the workers' selected Pushes. A worker forwards bounded raw
 * input and receives the real pinned Java result; the parent keeps the real receipts, outside any
 * target worker's memory, and reduces them to the facts a replay joins by raw hash and broker line.
 */
class DecoderCoordinator {
  /**
   * @param {object} options
   * @param {string} options.directory absolute run directory @param {string} options.line broker line
   * @param {{now:function():number}} options.clock monotonic time
   * @param {object} [options.env] environment of the decoder child
   * @param {DecoderGate} [options.gate] the concurrency limiter, four by default
   * @param {Function} [options.make] builds the observer; a spec supplies a fake
   * @param {function():object[]} [options.exports] reads the broker sink rows
   * @param {number} [options.sinkBytes] reporter sink cap; the five-minute cap by default
   */
  constructor({
    directory,
    line,
    clock,
    env = process.env,
    gate = new DecoderGate(DECODER_SLOTS),
    make = (options) => new TelemetryWireObserver(options),
    exports,
    sinkBytes = CHURN_SINK_BYTES,
  }) {
    this.exports = exports ?? (() => readExports({ fileBytes: sinkBytes }))
    this.observer = make({
      directory,
      line,
      clock,
      mode: 'configured',
      owner: 'parent-decoder',
      sinkBytes,
      gate,
      env,
    })
    this.calls = []
    this.closed = null
  }

  /**
   * @param {string} kind the worker that asked @param {string} rawB64 the raw MetricsData, base64
   * @returns {Promise<object>} the pinned Java decode of exactly those bytes
   * @throws {Error} for oversized input or any decoder failure
   */
  async decode(kind, rawB64) {
    const raw = Buffer.from(rawB64, 'base64')
    if (raw.length === 0 || raw.length > 4096) throw new Error('Decoder input is outside its bound')
    this.calls.push({ kind, rawSha256: sha(raw), bytes: raw.length })
    return this.observer.decodeRaw(raw, { budgetMs: DECODE_BUDGET_MS })
  }

  /**
   * Decodes the actual terminal final from a closed worker's immutable capture. The worker's
   * receipt-to-exit interval has already ended; this work belongs to parent cleanup and memory. A
   * close under a nonempty selection must hold exactly one accepted final to decode.
   * @param {string} directory scenario directory @param {string} kind worker kind
   * @param {object} pack terminal segment receipt, updated after the proof is persisted
   * @returns {Promise<void>} after bounded independent decode, sink join and bundle validation
   * @throws {Error} for a missing owed final, missing bytes, decoder/sink mismatch or a bundle
   *   bound violation
   */
  async terminal(directory, kind, pack) {
    const prefix = `${directory}/segments/`
    const transcript = fs.readFileSync(prefix + pack.transcript.name)
    const bundle = fs.readFileSync(prefix + pack.bundle.name, 'utf8')
    if (sha(transcript) !== pack.transcript.sha256 || sha(bundle) !== pack.bundle.sha256)
      throw new Error(`The sealed terminal segment ${pack.id} changed before its final was decoded`)
    const parsed = JSON.parse(transcript)
    const required = TelemetryProofs.selection(parsed.records)
    const finals = parsed.records.filter(
      (r) =>
        r.type === 'request' &&
        r.apiKey === 72 &&
        r.terminating &&
        r.response?.errorCode === 0 &&
        r.metricsBytes > 0
    )
    if (TelemetryProofs.owesFinal(parsed) && finals.length !== 1)
      throw new Error(`The terminal segment ${pack.id} owes one accepted final`)
    const keys = new Set(finals.map((r) => r.requestKey))
    const checked = checkSegmentFiles({
      id: pack.id,
      transcript,
      bundle,
      proofs: required.filter((key) => !keys.has(key)),
    })
    const lines = []
    for (const request of finals) {
      const blob = checked.pushes.get(request.requestKey).blob
      const raw = decompressBounded(request.compressionType, blob)
      const rawSha256 = sha(raw)
      const decoded = await this.decode(kind, raw.toString('base64'))
      if (decoded.dataSha256 !== rawSha256 || decoded.dataBytes !== raw.length)
        throw new Error('Terminal decode differs from captured bytes')
      const row = await this.sinkRow(request, raw.length, rawSha256)
      const receipts = this.receipts().filter((receipt) => receipt.rawSha256 === rawSha256)
      if (!receipts.length) throw new Error('Missing original terminal decoder receipt')
      lines.push({
        t: 'proof',
        key: request.requestKey,
        rawSha256,
        rawB64: raw.toString('base64'),
        receipts,
        decoded: {
          requestKey: request.requestKey,
          uuid: request.uuid,
          node: request.node,
          compressionType: request.compressionType,
          rawBytes: raw.length,
          transmittedBytes: blob.length,
          transmittedSha256: sha(blob),
          rawSha256,
          decoded,
          exportIdentity: { node: row.node, seq: row.seq },
        },
      })
    }
    const complete = bundle + lines.map((line) => JSON.stringify(line) + '\n').join('')
    if (Buffer.byteLength(complete) > FILE_BOUND)
      throw new Error('Terminal proof bundle exceeds bound')
    checkSegmentFiles({ id: pack.id, transcript, bundle: complete, proofs: required })
    fs.writeFileSync(prefix + pack.bundle.name, complete, { mode: 0o600 })
    pack.bundle.bytes = Buffer.byteLength(complete)
    pack.bundle.sha256 = sha(complete)
    pack.terminalProofs = lines.map((line) => line.key)
  }

  /**
   * @param {object} request the accepted final @param {number} bytes its raw length
   * @param {string} rawSha256 its raw hash
   * @returns {Promise<object>} the one sink row of exactly that final, waiting at most 2000 ms for
   *   the broker to flush it @throws {Error} unless exactly one row matches
   */
  async sinkRow(request, bytes, rawSha256) {
    for (let waited = 0; ; waited += 50) {
      const rows = this.exports().filter(
        (r) =>
          r.terminating &&
          r.node === request.node &&
          r.clientId === request.clientId &&
          r.clientInstanceId === request.uuid &&
          r.dataSha256 === rawSha256 &&
          r.dataBytes === bytes &&
          r.parse === 'ok'
      )
      if (rows.length === 1) return rows[0]
      if (rows.length > 1 || waited >= 2000)
        throw new Error('Terminal final lacks a unique sink join')
      await wait(50)
    }
  }

  /** @returns {object[]} the receipts of every decoder child this coordinator ran so far */
  receipts() {
    return fs
      .readdirSync(this.observer.directory)
      .filter((name) => /^decoder-\d+\.json$/.test(name))
      .map((name) => JSON.parse(fs.readFileSync(`${this.observer.directory}/${name}`, 'utf8')))
  }

  /**
   * Joins the decoder children and reduces their receipts, each with the child's separately
   * reported memory. Calling it again returns the first result.
   * @returns {Promise<{receipts:object[], calls:object[]}>} one compact receipt per decoder child
   * @throws {Error} when the observer is not clean
   */
  close() {
    this.closed ??= (async () => {
      await this.observer.dispose()
      this.observer.assertClean()
      const receipts = fs
        .readdirSync(this.observer.directory)
        .filter((name) => /^decoder-\d+\.json$/.test(name))
        .map((name) => {
          const receipt = JSON.parse(fs.readFileSync(`${this.observer.directory}/${name}`, 'utf8'))
          const [identity, body] = String(receipt.stdout ?? '')
            .trim()
            .split('\n')
          return {
            pid: Number(/\d+/.exec(name)[0]),
            code: receipt.code,
            signal: receipt.signal ?? null,
            line: receipt.line,
            rawSha256: receipt.rawSha256,
            identity: identity ? JSON.parse(identity) : null,
            bodySha256: body ? sha(JSON.stringify(JSON.parse(body))) : null,
            stderrBytes: Buffer.byteLength(receipt.stderr ?? ''),
            memory: receipt.memory ?? DecoderMemory.unavailable('not recorded by the observer'),
          }
        })
        .sort((a, b) => a.pid - b.pid)
      return { receipts, calls: this.calls }
    })()
    return this.closed
  }
}

/**
 * Supervises the four worker processes: one forked driver per kind, a fixed handshake and message
 * catalogue, bounded output, and a natural-exit observation that starts at the one explicit-
 * disconnect receipt. A violation of the catalogue, a duplicate, a substituted or oversized
 * message fails the worker instead of being ignored or clipped.
 */
class TelemetryProcesses {
  /**
   * @param {object} options
   * @param {string} options.script the driver to fork
   * @param {Function} [options.spawn] the fork function; a spec supplies a fake
   * @param {Function} [options.now] a monotonic millisecond clock
   * @param {string[]} [options.args] the driver arguments
   * @param {(kind:string, error:Error)=>void} [options.onFailure] told once per failed worker
   * @param {(kind:string, message:object)=>void} [options.onDecode] serves a worker's decode request
   * @param {number} [options.messageBytes] the largest IPC message a worker may send
   * @param {number} [options.totalBytes] the most IPC bytes a worker may send in all
   */
  constructor({
    script,
    spawn = fork,
    now = () => performance.now(),
    args = [],
    onFailure = () => {},
    onDecode = () => {},
    messageBytes = MESSAGE_BYTES,
    totalBytes = TOTAL_BYTES,
  }) {
    Object.assign(this, { script, spawn, now, args, onFailure, onDecode, messageBytes, totalBytes })
    this.workers = new Map()
  }

  /**
   * Forks one worker and registers it.
   * @param {string} kind exactly one of the four kinds
   * @returns {object} the supervised record
   * @throws {Error} for an unknown or already started kind
   */
  start(kind) {
    if (!KINDS.includes(kind)) throw new Error(`Unknown telemetry worker kind ${kind}`)
    if (this.workers.has(kind)) throw new Error(`Telemetry worker ${kind} already started`)
    const token = crypto.randomBytes(8).toString('hex')
    const child = this.spawn(this.script, [...this.args, '--profile', 'telemetry'], {
      execPath: process.execPath,
      execArgv: longWindowSelected() ? ['--expose-gc', ...LONG_WINDOW.v8Flags] : [...WORKER_FLAGS],
      env: { ...process.env, [WORKER_ENV]: kind, [WORKER_TOKEN]: token },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    })
    const worker = {
      kind,
      token,
      child,
      messages: [],
      counts: new Map(),
      bytes: 0,
      stderr: '',
      stdout: '',
      stdoutBytes: 0,
      receiptAt: null,
      disconnectedAt: null,
      exitedAt: null,
      lastMessage: null,
      exit: null,
      error: null,
      aborted: false,
      killed: false,
      waiters: [],
    }
    const capture = (name) => (chunk) => {
      const size = Buffer.byteLength(chunk)
      worker[`${name}Bytes`] = (worker[`${name}Bytes`] ?? 0) + size
      const tail = Buffer.concat([Buffer.from(worker[name]), Buffer.from(chunk)])
      worker[name] = tail.subarray(Math.max(0, tail.length - MAX_STDERR)).toString('utf8')
      while (Buffer.byteLength(worker[name]) > MAX_STDERR) worker[name] = worker[name].slice(1)
      if (worker[`${name}Bytes`] > MAX_STDERR)
        this.fail(worker, new Error(`Worker ${name} overflow`))
    }
    worker.stderrBytes = 0
    child.stdout?.on('data', capture('stdout'))
    child.stderr?.on('data', capture('stderr'))
    child.on('message', (message) => this.receive(kind, message))
    child.once('disconnect', () => {
      worker.disconnectedAt = this.now()
    })
    child.once('exit', () => {
      worker.exitedAt = this.now()
    })
    worker.closed = new Promise((resolve) => {
      child.once('close', (code, signal) => {
        worker.exit = { event: 'close', code, signal, closedAt: this.now() }
        if (!worker.messages.some((m) => m.type === 'result') && !worker.aborted)
          this.fail(worker, new Error(`Telemetry worker ${kind} exited before its result`))
        this.wake(worker)
        resolve(worker.exit)
      })
    })
    child.once('error', (error) => this.fail(worker, error))
    this.workers.set(kind, worker)
    return worker
  }

  /**
   * @param {object} worker the supervised record @param {Error} error its first failure
   * @returns {void} retained once and reported to the owner
   */
  fail(worker, error) {
    if (worker.error) return
    worker.error = error
    this.wake(worker)
    this.onFailure(worker.kind, error)
  }

  /** @param {object} worker the supervised record @returns {void} wakes every waiter */
  wake(worker) {
    for (const wake of worker.waiters.splice(0)) wake()
  }

  /**
   * Accepts one IPC message from a worker. Only the fixed catalogue is accepted: each of hello,
   * ready, phase, result and receipt at most once, every message bound to the handshake token, and
   * every message and the total within their byte bounds.
   * @param {string} kind the sending worker @param {object} message the message
   * @returns {void}
   */
  receive(kind, message) {
    const worker = this.workers.get(kind)
    if (!worker) return
    const bad = (why) => this.fail(worker, new Error(`Telemetry worker ${kind}: ${why}`))
    if (typeof message !== 'object' || message === null) return bad('a non-object message')
    if (!MESSAGE_TYPES.worker.includes(message.type)) return bad(`unknown message ${message.type}`)
    if (message.kind !== kind || message.token !== worker.token)
      return bad('a substituted handshake or message')
    const size = Buffer.byteLength(JSON.stringify(message))
    worker.bytes += size
    if (size > this.messageBytes || worker.bytes > this.totalBytes) return bad('IPC overflow')
    const seen = (worker.counts.get(message.type) ?? 0) + 1
    worker.counts.set(message.type, seen)
    if (MESSAGE_TYPES.once.includes(message.type) && seen > 1)
      return bad(`a duplicate ${message.type}`)
    if (message.type !== 'hello' && !worker.counts.has('hello'))
      return bad('a message before hello')
    worker.lastMessage = {
      type: message.type,
      bytes: size,
      parentAt: this.now(),
      ...(message.tick !== undefined && { tick: message.tick }),
      ...(message.stage !== undefined && { stage: message.stage }),
      ...(message.row && { rowKind: message.row.kind }),
      ...(message.message && { message: String(message.message).slice(0, 2048) }),
    }
    if (
      message.type === 'hello' &&
      longWindowSelected() &&
      (!Array.isArray(message.execArgv) ||
        !LONG_WINDOW.v8Flags.every((flag) => message.execArgv.includes(flag)))
    )
      return bad('missing long-window worker flag --no-flush-bytecode')
    if (message.type === 'receipt') worker.receiptAt = this.now()
    if (message.type === 'decode') {
      if (!worker.error) this.onDecode(kind, message)
      return
    }
    worker.messages.push({ ...message, parentAt: this.now() })
    if (message.type === 'failure')
      this.fail(worker, new Error(`Telemetry worker ${kind} failed: ${message.message}`))
    if (message.type === 'unhandled')
      this.fail(worker, new Error(`Telemetry worker ${kind} had an unhandled rejection`))
    this.wake(worker)
  }

  /**
   * @param {string} kind the worker @param {function(object):boolean} match which message
   * @param {number} ms bound @param {string} label what is awaited
   * @returns {Promise<object>} the first matching message
   * @throws {Error} on timeout, a protocol failure or an early exit
   */
  async until(kind, match, ms, label) {
    const worker = this.workers.get(kind)
    const deadline = this.now() + ms
    for (;;) {
      if (worker.error) throw worker.error
      const found = worker.messages.find(match)
      if (found) return found
      if (worker.exit) throw new Error(`Telemetry worker ${kind} exited before ${label}`)
      const left = deadline - this.now()
      if (left <= 0) throw new Error(`Telemetry worker ${kind} sent no ${label} within ${ms} ms`)
      await new Promise((resolve) => {
        worker.waiters.push(resolve)
        setTimeout(resolve, Math.min(left, 250)).unref?.()
      })
    }
  }

  /**
   * @param {string} kind the worker @param {string} type the message type to wait for
   * @param {number} ms bound @returns {Promise<object>} the first such message
   */
  message(kind, type, ms) {
    return this.until(kind, (m) => m.type === type, ms, type)
  }

  /**
   * @param {string} kind the worker @param {object} message the parent message to deliver
   * @returns {void} @throws {Error} when the worker is gone
   */
  send(kind, message) {
    const worker = this.workers.get(kind)
    if (!worker || worker.exit || worker.error) throw new Error(`Telemetry worker ${kind} is gone`)
    worker.child.send(message)
  }

  /**
   * The exit facts of one worker, all of them raw so a replay recomputes the interval itself.
   * @param {object} worker the supervised record
   * @param {object|null} closed its close facts, or null when it did not close within the bound
   * @returns {object} the exit row; a missed bound or a kill is reported, never turned into success
   */
  facts(worker, closed) {
    const exit = closed ?? { event: 'timeout', code: null, signal: null }
    const receipt = worker.receiptAt
    return {
      kind: worker.kind,
      event: exit.event,
      code: exit.code,
      signal: exit.signal,
      stderrBytes: worker.stderrBytes,
      stdoutBytes: worker.stdoutBytes,
      stderrTail: worker.stderr,
      stdoutTail: worker.stdout,
      lastMessage: worker.lastMessage,
      messages: Object.fromEntries(worker.counts),
      error: worker.error && { name: worker.error.name, message: worker.error.message },
      disconnectedAt: worker.disconnectedAt,
      exitedAt: worker.exitedAt,
      unhandled: worker.messages.filter((m) => m.type === 'unhandled').length,
      receipts: worker.counts.get('receipt') ?? 0,
      watchdogExpired: closed === null || worker.killed,
      killed: worker.killed,
      aborted: worker.aborted,
      receiptAt: receipt,
      closedAt: exit.closedAt ?? null,
      afterDisconnectMs: receipt === null || !exit.closedAt ? null : exit.closedAt - receipt,
    }
  }

  /**
   * @param {object} worker the supervised record @param {number} ms the bound from now
   * @returns {Promise<object|null>} its close facts, or null when it did not close in time
   */
  async settle(worker, ms) {
    let timer
    const timeout = new Promise((resolve) => {
      timer = setTimeout(() => resolve(null), ms)
    })
    try {
      return await Promise.race([worker.closed, timeout])
    } finally {
      clearTimeout(timer)
    }
  }

  /**
   * Waits for the natural close of one worker, at most the bound after its explicit-disconnect
   * receipt (or after the call, without a receipt).
   * @param {string} kind the worker @param {number} [ms] the bound after the receipt
   * @returns {Promise<object>} the natural-exit row; a timeout is reported, never turned into success
   */
  async join(kind, ms = NATURAL_EXIT_MS) {
    const worker = this.workers.get(kind)
    const receipt = worker.receiptAt
    const limit = receipt === null ? ms : Math.max(0, ms - (this.now() - receipt))
    return this.facts(worker, await this.settle(worker, limit))
  }

  /**
   * Asks a worker to stop admission and release its public client, then joins its process and
   * drained pipes within the remaining natural-exit bound. A worker still alive after that bound is
   * killed and joined for a further bounded time; its row stays failed with `killed` and
   * `watchdogExpired`, and a worker that never closes even then is reported as a timeout, so
   * cancellation always ends. The abort is sent only over a connected channel, and a send that
   * still finds it closed is discarded rather than reported as the worker's failure.
   * @param {string} kind the worker @returns {Promise<object>} the actual exit facts
   */
  async cancel(kind) {
    const worker = this.workers.get(kind)
    if (worker.killed) return this.facts(worker, worker.exit)
    if (!worker.exit && !worker.aborted) {
      worker.aborted = true
      if (worker.child.connected) worker.child.send({ type: 'abort' }, () => {})
    }
    const exit = await this.join(kind)
    if (exit.event === 'close') return exit
    worker.killed = true
    worker.child.kill('SIGKILL')
    return this.facts(worker, await this.settle(worker, KILL_JOIN_MS))
  }
}

/** A FIFO of parent messages that a worker takes by predicate; nothing is dropped. */
class Inbox {
  /** Builds an empty inbox. */
  constructor() {
    this.items = []
    this.waiting = []
    this.cancelled = false
  }

  /** @param {object} message a parent message @returns {void} */
  push(message) {
    if (message.type === 'abort') this.cancelled = true
    else this.items.push(message)
    this.waiting = this.waiting.filter((take) => !take())
  }

  /**
   * @param {function(object):boolean} match which message @returns {Promise<object>} the first one
   */
  take(match) {
    return new Promise((resolve, reject) => {
      const take = () => {
        if (this.cancelled) {
          reject(new Error('Telemetry worker cancelled'))
          return true
        }
        const index = this.items.findIndex(match)
        if (index < 0) return false
        resolve(this.items.splice(index, 1)[0])
        return true
      }
      if (!take()) this.waiting.push(take)
    })
  }
}

/**
 * One worker: a single public client of one kind, its retained registry, the observer segments of
 * its lifecycles, the ordinary work and the parent-coordinated churn, run under the inherited
 * `Soak` sampler. The parent owns the tick catalogue and sends one command per stage; the worker
 * executes it with public operations only, claims what proves it and replies with a receipt. It
 * never closes a client it did not create and never exits the process.
 */
class TelemetryWorker {
  /**
   * @param {object} options
   * @param {string} options.kind the one target client kind
   * @param {object} options.client the target public client
   * @param {TelemetryTap} options.tap its classifying socket tap
   * @param {TelemetrySegments} options.segments the owner of its observer segments
   * @param {Registry} options.registry its retained descriptors
   * @param {{now: () => number}} options.clock monotonic milliseconds
   * @param {(ms:number) => Promise<void>} [options.sleep] real delay
   * @param {OrdinaryWork} options.ordinary kind-specific workload
   * @param {{warmupMs:number, measureMs:number}} options.limits the fixed window
   * @param {Inbox} options.inbox the parent's messages @param {function(object):void} options.send
   *   delivers a message to the parent
   * @param {function(Function):*} [options.target] runs a public client call under the target role
   * @param {ShareLedger} [options.ledger] the share ledger of a share consumer
   * @param {object} [options.peak] diagnostic-only share phase recorder
   * @param {object} [options.schedule] a `TelemetrySchedule`
   * @param {{manifest:object, prefix:string}} [options.foreground] the control's frozen catalogue,
   *   replayed once by the enabled client during setup
   */
  constructor({
    kind,
    client,
    tap,
    segments,
    registry,
    clock,
    sleep = wait,
    ordinary,
    limits,
    inbox,
    send,
    target = (work) => work(),
    ledger = null,
    schedule,
    foreground = null,
    peak = null,
  }) {
    Object.assign(this, {
      kind,
      client,
      tap,
      segments,
      registry,
      clock,
      sleep,
      ordinary,
      limits,
      inbox,
      send,
      target,
      ledger,
      foreground,
      peak,
    })
    this.schedule = schedule ?? new TelemetrySchedule(limits)
    this.uuid = null
    this.actions = {
      warm: { subscription: 0, retire: 0, reopen: 0 },
      measured: { subscription: 0, retire: 0, reopen: 0 },
    }
    this.claims = {
      setup: null,
      foreground: null,
      initial: null,
      reopens: [],
      retirements: [],
      transitions: [],
      proofs: [],
    }
    this.stages = []
    this.owed = new Set()
    this.coreN = 0
    this.sealing = null
    this.confirmation = null
    this.timing = {
      phaseAt: null,
      goAt: null,
      boundaryAt: null,
      measurementMs: null,
      cleanupMs: null,
      receiptAt: null,
    }
    /** The supervising Soak, set by the owner before `work` runs. */
    this.sampler = null
  }

  /** @returns {{timeout: number}} the same finite close budget for every kind, admin included */
  closeOptions() {
    return { timeout: 3000 }
  }

  /**
   * Polls a lookup until it returns a truthy value.
   * @param {string} label what is awaited @param {number} ms bound @param {function():*} lookup
   * @returns {Promise<*>} the truthy result @throws {Error} when the bound passes first
   */
  async find(label, ms, lookup) {
    const deadline = this.clock.now() + ms
    for (;;) {
      const found = lookup()
      if (found) return found
      if (this.clock.now() > deadline) throw new Error(`${this.kind}: no ${label} within ${ms} ms`)
      await this.sleep(50)
    }
  }

  /**
   * Registers the descriptors, obtains the UUID in the setup lifecycle, replays the control's frozen
   * foreground catalogue with telemetry on, disconnects and leaves the client disconnected with
   * exactly that state retained, the next lifecycle's segment selected and the setup segment sealed.
   * @returns {Promise<string>} the retained instance ID
   * @throws {Error} a failed registration, connect, UUID wait or seal
   */
  async prepare() {
    this.registry.fill()
    const first = this.segments.current()
    await this.target(() => this.client.connect())
    this.uuid = await this.target(() => this.client.clientInstanceId({ timeout: 15000 }))
    if (this.foreground)
      this.claims.foreground = await this.target(() =>
        runCatalogue(this.client, this.foreground.manifest, this.foreground.prefix, () =>
          this.clock.now()
        )
      )
    const get = await this.find('setup Get', 15000, () =>
      first.observer
        .subscriptions({ uuid: this.uuid })
        .find((r) => r.response?.errorCode === 0 && r.response.uuid === this.uuid)
    )
    await this.target(() => this.segments.close(this.client, this.closeOptions()))
    this.segments.advance()
    this.claims.setup = { segment: first.id, uuid: this.uuid, get: get.requestKey }
    await this.seal(first, false)
    return this.uuid
  }

  /**
   * Seals one closed segment: decodes exactly its declared proof selection through the parent, then
   * discharges it. The previous seal must have finished by the next boundary. The terminal
   * segment's final is the one exception decoded here: it is sent by the explicit public close,
   * after which the process must exit within its natural-exit bound, so the parent decodes it from
   * the sealed bytes once this process has exited and completes the same bundle.
   * @param {object} segment a closed segment @param {boolean} terminal whether it is the last one
   * @returns {Promise<object>} its sealed summary
   * @throws {Error} for a claimed proof outside the declared selection, a decode failure or a seal failure
   */
  async seal(segment, terminal) {
    const { observer } = segment
    await within(observer.socketsClosed(), 5000, `${segment.id} sockets closing`)
    const records = observer.readTranscript()
    const isFinal = (key) => records.find((r) => r.requestKey === key).terminating
    const required = TelemetryProofs.selection(records)
    const keys = terminal ? required.filter((key) => !isFinal(key)) : required
    const claimed = this.claims.proofs.filter((p) => p.segment === segment.id).map((p) => p.push)
    for (const key of claimed)
      if (!keys.includes(key))
        throw new Error(`${segment.id}: claimed proof ${key} is not in the declared selection`)
    const outcomes = await Promise.allSettled(
      keys.map((key) =>
        observer.decodeAccepted({ requestKey: key }, { budgetMs: DECODE_BUDGET_MS })
      )
    )
    const failed = outcomes.find((o) => o.status === 'rejected')
    if (failed) throw failed.reason
    const pack = await this.segments.discharge(segment, { proofs: keys })
    this.send({ type: 'progress', sealed: pack })
    return pack
  }

  /**
   * Starts sealing a closed segment in the background while the next lifecycle runs.
   * @param {object} segment the closed segment @returns {void}
   */
  drain(segment) {
    const draining = { settled: false, error: null, id: segment.id }
    draining.promise = this.seal(segment, false).then(
      () => {
        draining.settled = true
      },
      (error) => {
        draining.settled = true
        draining.error = error
      }
    )
    this.sealing = draining
  }

  /**
   * @returns {Promise<void>} once the previous segment is sealed
   * @throws {Error} when it is still draining at a boundary or failed to seal; nothing queues
   */
  drained() {
    const previous = this.sealing
    if (!previous) return Promise.resolve()
    if (!previous.settled) throw new Error(`The previous ${this.kind} segment has not drained`)
    if (previous.error) throw previous.error
    return Promise.resolve()
  }

  /**
   * Opens the initial active lifecycle on the already selected segment and proves its Get and a
   * current normal Push.
   * @returns {Promise<void>} after the first accepted Push under the initial subscription
   */
  async begin() {
    const to = this.segments.current()
    await this.target(() => this.client.connect())
    await this.target(() => this.ordinary.start())
    const cached = await this.target(() => this.client.clientInstanceId({ timeout: 0 }))
    if (cached !== this.uuid)
      throw new Error('The instance ID changed before the first measurement')
    const get = await this.find(
      'initial Get',
      15000,
      () => TelemetryProofs.gets(to.observer.readTranscript(), this.uuid)[0]
    )
    const push = await this.find('initial proving Push', 15000, () =>
      TelemetryProofs.provingPush(to.observer.readTranscript({ after: get.index - 1 }), get)
    )
    this.claims.initial = {
      segment: to.id,
      cachedUuid: cached,
      get: get.requestKey,
      push: push.requestKey,
    }
    this.claims.proofs.push({
      tick: null,
      segment: to.id,
      push: push.requestKey,
      owedBy: ['initial'],
    })
  }

  /**
   * One public close then, as the next public action, an immediate connect on the same object, with
   * admission paused and drained before and resumed after. Proves the cached UUID and a successful
   * replacement Get; the current normal Push is proved after the tick's configuration.
   * @param {{tick:number}} command the stage command
   * @returns {Promise<object>} the facts of the stage
   * @throws {Error} a changed UUID, a missing replacement Get or an undrained previous segment
   */
  async reopen(command) {
    await this.drained()
    const pause = await this.target(() => this.ordinary.pause())
    const observedAt = this.clock.now()
    const cycle = this.target(() => this.segments.cycle(this.client, this.closeOptions()))
    await within(cycle.closed, 8000, `The ${this.kind} close`)
    const closedAt = this.clock.now()
    this.drain(cycle.from)
    await within(cycle.reopened, 10000, `The ${this.kind} reconnect`)
    const cached = await this.target(() => this.client.clientInstanceId({ timeout: 0 }))
    const get = await this.find(
      'replacement Get',
      15000,
      () => TelemetryProofs.gets(cycle.to.observer.readTranscript(), this.uuid)[0]
    )
    await this.target(() => this.ordinary.resume())
    const notes = this.segments.journal().slice(-3)
    if (
      notes.map((n) => n.type).join() !== 'select,disconnect-resolved,connect-called' ||
      notes[2].seq !== notes[1].seq + 1
    )
      throw new Error('The connect was not the next public action after the close')
    if (cached !== this.uuid) throw new Error('The instance ID changed across a public reopen')
    this.owed.add('reopen')
    const claim = {
      tick: command.tick,
      from: cycle.from.id,
      to: cycle.to.id,
      observedAt,
      closedAt,
      cachedUuid: cached,
      get: get.requestKey,
      pause,
    }
    this.claims.reopens.push(claim)
    return claim
  }

  /**
   * Faults the one captured telemetry socket while ordinary work continues, and proves the
   * replacement Get on a new socket of the same lifecycle.
   * @param {{tick:number}} command the stage command
   * @returns {Promise<object>} the facts of the stage
   * @throws {Error} without an open telemetry socket, a failed core operation or no replacement Get
   */
  async retire(command) {
    const segment = this.segments.current()
    const after = segment.observer.checkpoint()
    const fault = this.tap.retire()
    const working = this.target(() => this.ordinary.core(this.coreN++)).then(
      () => ({ status: 'fulfilled' }),
      (reason) => ({ status: 'rejected', reason })
    )
    const faultedAt = await within(fault.hit, 8000, `The ${this.kind} telemetry fault`)
    const coreResult = await working
    if (coreResult.status === 'rejected') throw coreResult.reason
    const core = await this.find('core request beside the fault', 8000, () =>
      segment.observer
        .normalRequests({ after })
        .find((r) => r.writeAt >= fault.declaredAt && r.response?.responseAt !== undefined)
    )
    const get = await this.find('replacement Get after the retirement', 12000, () =>
      TelemetryProofs.gets(segment.observer.readTranscript({ after }), this.uuid).find(
        (r) => r.socketId !== fault.ordinal
      )
    )
    this.owed.add('retire')
    const claim = {
      tick: command.tick,
      segment: segment.id,
      faulted: fault.ordinal,
      declaredAt: fault.declaredAt,
      faultedAt,
      core: core.requestKey,
      get: get.requestKey,
    }
    this.claims.retirements.push(claim)
    return claim
  }

  /**
   * In the five-minute lane, pauses admission and closes the public lifecycle before acknowledging
   * the fence; reconnect waits for the parent's batch readbacks. The frozen long-window fence
   * only captures its original write cursor.
   * @param {{tick:number, index:number, selection:string, transitions?:number[]}} command pending batch
   * @returns {Promise<void>} the receipt contains only scalar observer and clock facts
   * @throws {Error} for overlapping fences, a failed close or an undrained previous segment
   */
  async fenceConfirmation(command) {
    if (this.confirmation) throw new Error('A confirmation fence is already pending')
    if (command.transitions) {
      await this.drained()
      const pause = await this.target(() => this.ordinary.pause())
      const observedAt = this.clock.now()
      const cycle = this.target(() =>
        this.segments.cycle(this.client, this.closeOptions(), () => {
          this.confirmation.closedAt = this.clock.now()
          this.confirmation.after = cycle.to.observer.checkpoint()
          this.send({
            type: 'confirm-fenced',
            ...this.confirmation,
            cycle: undefined,
            resume: undefined,
          })
          return new Promise((resolve) => {
            this.confirmation.resume = resolve
          })
        })
      )
      this.confirmation = {
        tick: command.tick,
        index: command.index,
        selection: command.selection,
        segment: cycle.to.id,
        receivedAt: observedAt,
        after: 0,
        pause,
        observedAt,
        cycle,
      }
      await within(cycle.closed, 8000, `The ${this.kind} close`)
      this.drain(cycle.from)
      return
    }
    const segment = this.segments.current()
    this.confirmation = {
      tick: command.tick,
      index: command.index,
      selection: command.selection,
      segment: segment.id,
      receivedAt: this.clock.now(),
      after: segment.observer.checkpoint(),
    }
    this.send({ type: 'confirm-fenced', ...this.confirmation })
  }

  /**
   * After the parent's successful readback, swaps one custom identity and proves the selection
   * with a Get written after the acknowledged fence. The five-minute lane first releases the held
   * reconnect, so its handshake follows all readbacks; the long-window lane can use a Get already
   * received before this command. Discharges every owed current normal Push under unchanged bounds.
   * @param {{tick:number, index:number, selection:string}} command the stage release
   * @returns {Promise<object>} the facts of the stage
   * @throws {Error} for a missing or mismatched fence, no matching Get or no owed Push
   */
  async confirm(command) {
    const fence = this.confirmation
    const segment = this.segments.current()
    if (
      !fence ||
      fence.tick !== command.tick ||
      fence.index !== command.index ||
      fence.selection !== command.selection ||
      fence.segment !== segment.id
    )
      throw new Error('The confirmation has no matching pre-rotation fence')
    this.confirmation = null
    const { receivedAt, after } = fence
    const releasedAt = this.clock.now()
    if (fence.cycle) {
      fence.resume()
      await within(fence.cycle.reopened, 10000, `The ${this.kind} reconnect`)
      const cached = await this.target(() => this.client.clientInstanceId({ timeout: 0 }))
      if (cached !== this.uuid) throw new Error('The instance ID changed across a public reopen')
      await this.target(() => this.ordinary.resume())
      this.claims.reopens.push({
        tick: command.tick,
        from: fence.cycle.from.id,
        to: segment.id,
        observedAt: fence.observedAt,
        closedAt: fence.closedAt,
        cachedUuid: cached,
        pause: fence.pause,
      })
      if (command.selection !== '') this.owed.add('reopen')
    }
    const replacement = this.registry.replace(command.tick)
    const list = command.selection === '' ? [] : [command.selection]
    const get = await this.find('selection Get', STAGE_MS.confirm - 2000, () =>
      TelemetryProofs.selectionGet(segment.observer.readTranscript({ after }), this.uuid, {
        after: receivedAt,
        selection: list,
      })
    )
    if (fence.cycle) this.claims.reopens.at(-1).get = get.requestKey
    const claim = {
      tick: command.tick,
      index: command.index,
      selection: command.selection,
      segment: segment.id,
      receivedAt,
      releasedAt,
      after,
      get: get.requestKey,
      subscriptionId: get.response.subscriptionId,
      replacement,
      proof: null,
    }
    claim.proof = await this.proveOwed(command.tick)
    this.claims.transitions.push(claim)
    return claim
  }

  /**
   * Proves every pending lifecycle or retirement obligation on the current socket.
   * @param {number} tick schedule row @returns {Promise<object|null>} current Push proof, if owed
   * @throws {Error} when the current socket has no successful Get or normal Push
   */
  async proveOwed(tick) {
    const segment = this.segments.current()
    if (!this.owed.size) return null
    const open = this.tap.telemetry()
    if (open.length !== 1 || open[0].segment !== segment.id)
      throw new Error(`${this.kind}: expected one open telemetry socket for the owed proof`)
    const records = () => segment.observer.readTranscript({ after: 0 })
    const current = TelemetryProofs.gets(records(), this.uuid)
      .filter((g) => g.socketId === open[0].ordinal)
      .at(0)
    if (!current) throw new Error(`${this.kind}: the current socket has no replacement Get`)
    const push = await this.find('owed current Push', TICK_PROOF_MS, () =>
      TelemetryProofs.provingPush(
        segment.observer.readTranscript({ after: current.index - 1 }),
        current
      )
    )
    const proof = {
      socketId: open[0].ordinal,
      get: current.requestKey,
      push: push.requestKey,
      owedBy: [...this.owed],
    }
    this.claims.proofs.push({
      tick,
      segment: segment.id,
      push: push.requestKey,
      owedBy: [...this.owed],
    })
    this.owed.clear()
    return proof
  }

  /**
   * Executes one parent command and replies with a receipt carrying this worker's own times.
   * @param {object} command `{type:'stage', tick, stage, ...}` or `{type:'corpus'}`
   * @returns {Promise<void>} after the receipt was sent
   * @throws {Error} a failed stage; the parent has the failure message and no later stage runs
   */
  async handle(command) {
    if (command.type === 'corpus') {
      this.ordinary.corpus()
      return
    }
    const tick = this.schedule.ticks[command.tick]
    if (!tick || !this.schedule.stages(tick).includes(command.stage))
      throw new Error(`A stage the fixed table does not hold: ${command.tick}/${command.stage}`)
    if (command.type === 'confirm-fence') {
      if (command.stage !== 'confirm') throw new Error('Only confirmation stages have a fence')
      await this.fenceConfirmation(command)
      return
    }
    this.peak?.phase(
      { lifecycle: 'reopen', retire: 'retire', confirm: 'subscription' }[command.stage],
      command.tick
    )
    const startedAt = command.stage === 'confirm' ? this.confirmation?.receivedAt : this.clock.now()
    if (command.stage === 'lifecycle') await this.reopen(command)
    else if (command.stage === 'retire') {
      await this.retire(command)
      if (this.schedule.betweenLifecycles) await this.proveOwed(command.tick)
    } else await this.confirm(command)
    const bucket = this.schedule.measured(tick) ? this.actions.measured : this.actions.warm
    const action = { lifecycle: 'reopen', retire: 'retire', confirm: 'subscription' }[command.stage]
    bucket[action] += command.stage === 'confirm' ? (command.transitions?.length ?? 1) : 1
    if (command.stage === 'confirm' && command.transitions) bucket.reopen += 1
    const doneAt = this.clock.now()
    this.stages.push({ tick: command.tick, stage: command.stage, startedAt, doneAt })
    this.send({ type: 'stage-done', tick: command.tick, stage: command.stage, startedAt, doneAt })
    this.peak?.phase('active')
  }

  /**
   * Runs the warm-up and measurement under the supervising Soak, then the cleanup. The sampler is
   * stopped and joined at the measurement boundary, before any cleanup await; after a failure no
   * later stage runs, and every owned release is still attempted.
   * @param {number} ms the fixed warm-up plus measurement
   * @returns {Promise<void>} after cleanup; a failure keeps the primary and every cleanup error
   * @throws {Error|AggregateError} the first workload failure with the cleanup failures
   */
  async work(ms) {
    const { warmupMs, measureMs } = this.limits
    if (ms !== warmupMs + measureMs) throw new Error('Telemetry churn runs its fixed 360000 ms')
    const errors = []
    this.timing.phaseAt = this.sampler.phaseStartedAt
    this.peak?.window(this.timing.phaseAt, this.limits.warmupMs)
    this.peak?.phase('await-go')
    this.send({ type: 'phase', at: this.timing.phaseAt, uuid: this.uuid })
    try {
      await this.inbox.take((m) => m.type === 'go')
      this.timing.goAt = this.clock.now()
      this.peak?.phase('begin')
      await this.begin()
      this.peak?.phase('active')
      for (;;) {
        const command = await this.inbox.take((m) =>
          ['confirm-fence', 'stage', 'corpus', 'finish'].includes(m.type)
        )
        if (command.type === 'finish') break
        await this.handle(command)
      }
    } catch (error) {
      errors.push(error)
    }
    await this.cleanup(errors)
    if (errors.length === 1) throw errors[0]
    if (errors.length) throw new AggregateError(errors, 'Telemetry worker failed')
  }

  /**
   * The cleanup after the common end-of-measurement boundary, every release attempted and all
   * failures kept: the sampler stops first, ordinary work is finished and confirmed, the registry is
   * restored, the terminal segment's declared proofs are decoded, the one explicit public close
   * sends its receipt the moment it resolves, and the remaining segments are sealed.
   * @param {Error[]} errors the failures so far, appended to
   * @returns {Promise<void>}
   */
  async cleanup(errors) {
    const attempt = async (work) => {
      try {
        await work()
      } catch (error) {
        errors.push(error)
      }
    }
    this.peak?.phase('cleanup')
    this.timing.boundaryAt = this.clock.now()
    this.timing.measurementMs = this.timing.boundaryAt - this.timing.phaseAt - this.limits.warmupMs
    await attempt(() => this.sampler.endSampling())
    const cleanupAt = this.clock.now()
    // A failed run cannot finish a corpus whose parent has already stopped producing it.
    await attempt(() => this.target(() => this.ordinary.stop(errors.length ? 1 : undefined)))
    await attempt(() => this.registry.restore())
    const last = this.segments.current()
    await attempt(async () => {
      if (errors.length) return
      await this.drained()
      const keys = TelemetryProofs.selection(last.observer.readTranscript())
      const outcomes = await Promise.allSettled(
        keys.map((key) =>
          last.observer.decodeAccepted({ requestKey: key }, { budgetMs: DECODE_BUDGET_MS })
        )
      )
      const failed = outcomes.find((o) => o.status === 'rejected')
      if (failed) throw failed.reason
    })
    let closing = null
    await attempt(async () => {
      closing = this.target(() => this.segments.close(this.client, this.closeOptions()))
      closing.then(
        () => {
          this.timing.receiptAt = this.clock.now()
          this.send({ type: 'receipt', at: this.timing.receiptAt })
        },
        () => {}
      )
      await closing
    })
    await attempt(async () => {
      await this.sealing?.promise
      if (this.sealing?.error) throw this.sealing.error
      await this.seal(last, true)
    })
    await attempt(() => this.ledger?.drain())
    this.tap.restore()
    this.timing.cleanupMs = this.clock.now() - cleanupAt
  }

  /**
   * Closes whatever a failed setup left open, attempting every release.
   * @returns {Promise<void>} @throws {AggregateError} the cleanup failures
   */
  async abort() {
    const errors = []
    for (const work of [
      () => this.ordinary.stop(1),
      () => this.client.disconnect(),
      () => this.sampler?.endSampling(),
      () => this.tap.restore(),
    ])
      try {
        await work()
      } catch (error) {
        errors.push(error)
      }
    if (errors.length) throw new AggregateError(errors, 'Telemetry worker abort failed')
  }

  /**
   * @returns {{ok:boolean, violations:string[]}} whether every required measured action completed
   */
  requirements() {
    const violations = []
    const need = this.schedule.required()
    for (const action of Object.keys(need))
      if (this.actions.measured[action] < need[action] || need[action] < MINIMUMS[action])
        violations.push(`measured ${action} actions ${this.actions.measured[action]}`)
    return { ok: violations.length === 0, violations }
  }

  /**
   * @returns {object} everything the parent archives for this worker: claims, receipts, action
   *   counts, the ordering journal, the replacements, the sealed segments and the share facts
   */
  facts() {
    return {
      kind: this.kind,
      uuid: this.uuid,
      claims: this.claims,
      stages: this.stages,
      actions: this.actions,
      journal: this.segments.journal(),
      replacements: this.registry.replacements,
      sealed: this.segments.sealed(),
      timing: this.timing,
      requirements: this.requirements(),
      ordinary: this.ordinary.ids(),
      share: this.ledger ? this.ledger.facts() : null,
    }
  }
}

/** Counts the telemetry request frames of the sockets it issues, to prove a control sent none. */
class FrameCounter {
  /** @param {Function} inner the underlying socket factory */
  constructor(inner) {
    const chaos = createChaosSocketFactory(inner, { retainClosed: false })
    this.frames = 0
    this.open = 0
    this.factory = (options) => this.track(chaos(options))
  }

  /** @param {object} socket a chaos socket @returns {object} the same socket, counted */
  track(socket) {
    const classifier = new FrameFilter({
      headerBytes: 12,
      decide: (header) => {
        if (header.length >= 12 && [71, 72].includes(header.readInt16BE(4))) this.frames += 1
        return false
      },
    })
    const write = socket.write
    socket.write = function (data, encoding) {
      classifier.push(Buffer.from(data, encoding))
      return write.call(this, data, encoding)
    }
    this.open += 1
    socket.on('close', () => {
      this.open -= 1
      delete socket.write
    })
    return socket
  }
}

/**
 * The fixed foreground of one kind, frozen before either mode runs: the client options, the topic
 * and group it joins, the bounded operations with their input sizes, the one public lifecycle and
 * the expected outcome of every operation. The disabled control and the enabled worker run the
 * same catalogue, each with its own record identities.
 * @param {string} kind the client kind @param {object} config the parent's configuration
 * @returns {object} the manifest, plain data
 */
const controlManifest = (kind, config) => ({
  kind,
  topic: config.topic,
  joins: kind === 'consumer' || kind === 'shareConsumer',
  operations: Array.from({ length: CONTROL.catalogue[kind] }, (_, n) => ({
    n,
    op: kind === 'producer' ? 'send' : 'describeCluster',
    topic: kind === 'producer' ? config.ctl : null,
    valueBytes: kind === 'producer' ? CONTROL.valueBytes : 0,
    expected: 'fulfilled',
  })),
  lifecycles: 1,
})

/**
 * Runs the frozen foreground operations of a manifest on one client and records every outcome.
 * @param {object} client the public client @param {object} manifest `controlManifest` output
 * @param {string} prefix the record identity prefix of this mode
 * @param {function():number} now monotonic milliseconds
 * @returns {Promise<object[]>} `{n, status, at}` per operation, in order
 * @throws {*} the first operation's own rejection, after its outcome was recorded
 */
const runCatalogue = async (client, manifest, prefix, now) => {
  const outcomes = []
  for (const { n, op, topic } of manifest.operations) {
    try {
      if (op === 'send')
        await client.send({
          topic,
          acks: -1,
          messages: [{ key: `${prefix}:${n}`, value: Buffer.alloc(CONTROL.valueBytes, 99) }],
        })
      else await client.describeCluster({ timeout: 3000 })
      outcomes.push({ n, status: 'fulfilled', at: now() })
    } catch (error) {
      outcomes.push({ n, status: 'rejected', at: now() })
      throw error
    }
  }
  return outcomes
}

/**
 * The telemetry-disabled reference of one worker: the same kind, process, loaded modules, topic
 * and resident subscription as the enabled client, running the frozen foreground catalogue between
 * two settled readings that bracket it at the same idle resident state, then a complete release
 * before the enabled client exists. The five-minute lane warms the catalogue and its idle window
 * before c0, recording those outcomes separately. It performs no telemetry work and proves that.
 */
class ReferenceControl {
  /**
   * @param {object} options
   * @param {string} options.kind the client kind @param {object} options.config the parent's configuration
   * @param {Function} options.Kafka the client constructor @param {Function} options.socketFactory
   * @param {object} options.logLevel the log levels @param {ProcessResources} options.resources
   * @param {{now:function():number}} options.clock monotonic time
   * @param {(ms:number)=>Promise<void>} [options.sleep] real delay
   * @param {function():void} [options.gc] a full collection, `global.gc` by default
   * @param {function():number} [options.collections] collections the engine reported so far
   * @param {EngineCodeJournal|null} [options.engine] takes the code sizes of each settled reading
   */
  constructor({
    kind,
    config,
    Kafka,
    socketFactory,
    logLevel,
    resources,
    clock,
    sleep = wait,
    gc = globalThis.gc,
    collections = () => 0,
    engine = null,
  }) {
    Object.assign(this, {
      kind,
      config,
      Kafka,
      socketFactory,
      logLevel,
      resources,
      clock,
      sleep,
      gc,
      collections,
      engine,
    })
    this.manifest = controlManifest(kind, config)
  }

  /**
   * @returns {Promise<object>} the control facts: the frozen manifest and every outcome, both
   *   settled readings bracketing the catalogue, the five-minute warm-up outcomes and idle times,
   *   the release reading, the telemetry frames it sent
   *   (none), the sockets and handles it left (none)
   * @throws {Error} a failed foreground operation or a control that is still holding something
   */
  async run() {
    const { kind, config, clock, manifest } = this
    const io = {
      gc: this.gc,
      pause: (ms) => this.sleep(ms),
      now: () => clock.now(),
      collections: this.collections,
      engine: this.engine,
    }
    const counter = new FrameCounter(this.socketFactory)
    return this.resources.as('fixture:control', async () => {
      const kafka = new this.Kafka({
        clientId: `${config.stem}-ctl-${kind}`,
        brokers: config.brokers,
        logLevel: this.logLevel.NOTHING,
        socketFactory: counter.factory,
      })
      const off = { enableMetricsPush: false }
      const group = `${config.group}-ctl`
      const client =
        kind === 'producer'
          ? kafka.producer({ ...off, idempotent: true })
          : kind === 'admin'
            ? kafka.admin(off)
            : kind === 'shareConsumer'
              ? kafka.shareConsumer({ ...off, groupId: group })
              : kafka.consumer({
                  ...off,
                  groupId: group,
                  sessionTimeout: 30000,
                  heartbeatInterval: 3000,
                  maxWaitTimeInMs: 500,
                  readUncommitted: false,
                })
      let c0
      let c1
      let warmup
      let outcomes = []
      try {
        await client.connect()
        if (kind === 'consumer') {
          await client.subscribe({ topic: manifest.topic, fromBeginning: true })
          await client.run({ autoCommit: false, eachMessage: async () => {} })
        }
        if (kind === 'shareConsumer') {
          await client.subscribe({ topics: [manifest.topic] })
          await client.run({ eachMessage: async () => {} })
        }
        if (SCENARIO === 'telemetry-churn') {
          warmup = { startedAt: clock.now() }
          warmup.outcomes = await runCatalogue(
            client,
            manifest,
            `${config.stem}:ctl-warmup`,
            io.now
          )
          warmup.idleStartedAt = clock.now()
        }
        const warmupIdleMs = CONTROL.idleMs * (warmup && kind === 'shareConsumer' ? 5 : 1)
        await this.sleep(warmupIdleMs)
        if (warmup) {
          // A timer may fire a fraction of a millisecond early against this clock.
          while (clock.now() - warmup.idleStartedAt < warmupIdleMs) await this.sleep(1)
          warmup.completedAt = clock.now()
        }
        c0 = await settledReading('c0', io)
        outcomes = await runCatalogue(client, manifest, `${config.stem}:ctl-off`, io.now)
        await this.sleep(CONTROL.idleMs)
        c1 = await settledReading('c1', io)
      } catch (error) {
        await client.disconnect().catch(() => {})
        throw error
      }
      await client.disconnect()
      const deadline = clock.now() + 5000
      while (counter.open > 0 && clock.now() < deadline) await this.sleep(50)
      const released = await settledReading('control-released', io)
      const leftover = [...this.resources.live.values()].filter(
        (entry) => entry.token.role === 'fixture:control'
      ).length
      return {
        kind,
        manifest,
        outcomes,
        ...(warmup && { warmup }),
        catalogue: manifest.operations.length,
        telemetryFrames: counter.frames,
        openSockets: counter.open,
        leftoverHandles: leftover,
        c0,
        c1,
        released,
      }
    })
  }
}

/**
 * The parent of a run: configures the broker resource, starts four workers, coordinates the fixed
 * tick catalogue in joined stages, offers the parent-owned corpora, decodes the workers' selected
 * Pushes independently, and writes one oracle, four natural-exit rows, four resource verdicts and
 * one final verdict, stamped only after every cleanup. A failure taints the run: no later lazy work
 * starts, every owned release is still attempted and joined, and no cleanup failure is swallowed.
 */
class TelemetryChurn {
  /**
   * @param {object} options
   * @param {object} options.fixture a `ParentFixture` @param {TelemetryProcesses} options.processes
   *   the supervised workers @param {DecoderCoordinator} options.decoder the independent decoder
   * @param {(row:object)=>void} options.record evidence sink
   * @param {{now:()=>number}} options.clock monotonic time
   * @param {(ms:number)=>Promise<void>} [options.sleep] real delay
   * @param {object} options.limits the `TELEMETRY` limits
   * @param {Function} options.verdict `TelemetryResourceVerdict.evaluate`
   * @param {string[]} options.brokers seed addresses @param {string} options.stem run prefix
   * @param {string} options.line broker line @param {string} options.directory absolute run directory
   * @param {object|null} [options.source] the `sourceStamp` of the driver the workers fork from,
   *   archived so a verifier joins every worker's own stamp to the run's tree
   */
  constructor({
    fixture,
    processes,
    decoder,
    record,
    clock,
    sleep = wait,
    limits,
    verdict,
    brokers,
    stem,
    line,
    directory,
    source = null,
  }) {
    Object.assign(this, {
      fixture,
      processes,
      decoder,
      record,
      clock,
      sleep,
      limits,
      verdict,
      brokers,
      stem,
      line,
      directory,
      source,
    })
    this.closed = null
    this.failed = false
    this.cause = undefined
    this.stage = 'setup'
    this.failureStage = null
    this.stages = []
    this.rotations = []
    this.tickStarts = []
    this.missed = []
    this.offered = { consumer: [], shareConsumer: [] }
    this.outstanding = new Set()
    this.abort = new Promise((_, reject) => {
      this.rejectAbort = reject
    })
    this.abort.catch(() => {})
    processes.onFailure = (kind, error) => this.fail(error)
    processes.onDecode = (kind, message) => this.serve(kind, message)
  }

  /**
   * @param {Error} error the first failure of the run @returns {void} later lazy work now refuses
   */
  fail(error) {
    if (!this.failed) {
      this.failed = true
      this.cause = error
      this.failureStage = this.stage
    }
    this.rejectAbort(error)
  }

  /** @returns {void} @throws {*} the run's first failure, even a falsy one, so no later work starts */
  guard() {
    if (this.failed) throw this.cause
  }

  /**
   * Retains one concurrently started driver operation until it settles, so a failure path can join
   * it instead of unwinding past it.
   * @template T
   * @param {Promise<T>} work the started operation @returns {Promise<T>} the same operation
   */
  own(work) {
    this.outstanding.add(work)
    const forget = () => this.outstanding.delete(work)
    work.then(forget, forget)
    return work
  }

  /**
   * Stops every worker after a failure and joins them with every outstanding driver operation.
   * @param {*} primary the failure that stopped the run
   * @returns {Promise<void>} once nothing owned is outstanding
   * @throws {*} the primary failure alone, or with every failure the joins reported
   */
  async contain(primary) {
    this.fail(primary)
    const joined = await Promise.allSettled([
      ...this.outstanding,
      ...[...this.processes.workers.keys()].map((kind) => this.processes.cancel(kind)),
    ])
    const failures = joined
      .filter((r) => r.status === 'rejected' && r.reason !== primary)
      .map((r) => r.reason)
    if (failures.length)
      throw new AggregateError([primary, ...failures], 'Churn containment failed')
    throw primary
  }

  /**
   * Answers one worker's request for an independent decode, never blocking the other workers.
   * @param {string} kind the asking worker @param {object} message `{id, rawB64}` @returns {void}
   */
  serve(kind, message) {
    this.decoder.decode(kind, message.rawB64).then(
      (body) => this.sendTo(kind, { type: 'decoded', id: message.id, body }),
      (error) =>
        this.sendTo(kind, {
          type: 'decoded',
          id: message.id,
          error: String(error?.message ?? error).slice(0, 1024),
        })
    )
  }

  /**
   * @param {string} kind the worker @param {object} message what to send
   * @returns {void} a worker that is already gone is not an error of the decoder
   */
  sendTo(kind, message) {
    try {
      this.processes.send(kind, message)
    } catch {
      // the worker's own failure is already the run's failure
    }
  }

  /**
   * @param {number} target a monotonic reading @returns {Promise<void>} resolves once it is reached
   * @throws {Error} the run's failure, as soon as it exists
   */
  async until(target) {
    for (;;) {
      this.guard()
      const left = target - this.clock.now()
      if (left <= 0) return
      await this.sleep(Math.min(left, 250))
    }
  }

  /**
   * Runs the whole fixed case and judges it.
   * @returns {Promise<{ok:boolean, violations:string[], clientKinds:string[], workers:object[],
   *   oracle:object, timeline:object, setupMs:number, cleanupMs:number, totalMs:number,
   *   archive:object}>}
   * @throws {Error} a failure that left no verdict; cleanup is still attempted and joined
   */
  async run() {
    const timeline = { t0: this.clock.now(), epochAt: null, boundaryAt: null, cleanupEndAt: null }
    this.parentMemory = [readMemory('parent-start', () => this.clock.now())]
    const schedule = new TelemetrySchedule(this.limits)
    let results = []
    let primary
    let failed = false
    try {
      results = await this.measure(timeline, schedule)
    } catch (error) {
      primary = error
      failed = true
      this.fail(error)
    }
    timeline.boundaryAt ??= this.clock.now()
    this.stage = 'cleanup'
    const shutdown = await this.shutdown(results)
    timeline.cleanupEndAt = this.clock.now()
    this.stage = 'verification'
    try {
      this.lastResult = this.judge(results, shutdown, timeline, schedule)
    } catch (error) {
      shutdown.errors.push(error)
    }
    if (failed || shutdown.errors.length) {
      const errors = [...(failed ? [primary] : []), ...shutdown.errors]
      throw errors.length === 1 ? errors[0] : new AggregateError(errors, 'Telemetry churn failed')
    }
    return this.lastResult
  }

  /**
   * Setup, then the measurement, in the fixed order: start the workers, join their handshakes,
   * ready receipts and phase starts inside the setup budget, fix one epoch, run the corpus and
   * the tick catalogue in joined stages, then take every worker's result.
   * @param {object} timeline the run's raw monotonic stamps, filled in as it goes
   * @param {TelemetrySchedule} schedule the fixed catalogue @returns {Promise<object[]>} the results
   * @throws {Error} a setup over its budget, a missed measured tick, or any stage failure
   */
  async measure(timeline, schedule) {
    const { warmupMs, measureMs, cleanupMs: cleanupBound } = this.limits
    const left = () => Math.max(1, SETUP_MS - (this.clock.now() - timeline.t0))
    let results = []
    await this.fixture.open()
    await this.fixture.inResource(async () => {
      try {
        for (const kind of KINDS) this.processes.start(kind)
        const hellos = await Promise.all(
          KINDS.map((kind) => this.processes.message(kind, 'hello', left()))
        )
        this.handshakes = hellos.map(
          ({
            kind,
            origin,
            pid,
            node,
            v8,
            platform,
            arch,
            execArgv,
            modules,
            codecs,
            source,
            parentAt,
          }) => ({
            kind,
            origin,
            pid,
            node,
            v8,
            platform,
            arch,
            execArgv,
            modules,
            codecs,
            source,
            parentAt,
          })
        )
        for (const kind of KINDS)
          this.processes.send(kind, {
            type: 'config',
            stem: this.stem,
            brokers: this.brokers,
            topic: this.fixture.topic(kind),
            group: this.fixture.group(kind),
            ctl: this.fixture.ctl,
            line: this.line,
            directory: this.directory,
          })
        await Promise.all(KINDS.map((kind) => this.processes.message(kind, 'ready', left())))
        for (const kind of KINDS) this.processes.send(kind, { type: 'start' })
        await Promise.all(KINDS.map((kind) => this.processes.message(kind, 'phase', left())))
        timeline.epochAt = this.clock.now()
        timeline.setupMs = timeline.epochAt - timeline.t0
        if (timeline.setupMs > SETUP_MS) throw new Error('The setup exceeded its budget')
        this.stage = 'warmup'
        for (const kind of KINDS) this.processes.send(kind, { type: 'go' })
        const corpus = this.own(this.offerCorpus(timeline.epochAt, schedule))
        corpus.catch((error) => this.fail(error))
        await this.ticks(timeline.epochAt, schedule)
        await this.until(timeline.epochAt + warmupMs + measureMs)
        timeline.boundaryAt = this.clock.now()
        this.stage = 'cleanup'
        await corpus
        for (const kind of KINDS) this.processes.send(kind, { type: 'finish' })
        const resultMs = cleanupBound - RESULT_RESERVE_MS
        results = await Promise.race([
          Promise.all(KINDS.map((kind) => this.processes.message(kind, 'result', resultMs))),
          this.abort,
        ])
      } catch (error) {
        await this.contain(error)
      }
    })
    return results
  }

  /**
   * Offers the parent-owned corpora in their fixed measured slots, one batch per slot, and starts
   * the workers' own producer pacing at the first slot. Nothing is offered in the warm-up.
   * @param {number} epoch the monotonic reading at which the schedule began
   * @param {TelemetrySchedule} schedule the fixed catalogue @returns {Promise<void>} after the last batch
   * @throws {Error} the run's failure at any slot, or a failed produce
   */
  async offerCorpus(epoch, schedule) {
    const slots = schedule.corpusSlots()
    for (let batch = 0; batch < slots.length; batch++) {
      await this.until(epoch + slots[batch])
      if (batch === 0) for (const kind of KINDS) this.processes.send(kind, { type: 'corpus' })
      const from = batch * CORPUS.batch
      for (const kind of ['consumer', 'shareConsumer']) {
        this.guard()
        this.offered[kind].push(...(await this.own(this.fixture.produce(kind, from, CORPUS.batch))))
      }
    }
  }

  /**
   * The fixed tick catalogue. At every tick the stages run in order and are joined across all four
   * workers. In the five-minute lane, each confirmation fence closes its public lifecycle; every
   * original set/readback in the batch then precedes the reconnect and confirmation, followed by
   * any scheduled telemetry retirement. The long-window lane retains its original stage order. A tick
   * more than the late bound behind is a missed tick and is never caught up.
   * @param {number} epoch the monotonic reading at which the schedule began
   * @param {TelemetrySchedule} schedule the fixed catalogue @returns {Promise<void>} after the last tick
   * @throws {Error} a missed measured tick, a stage over its bound or a failed set
   */
  async ticks(epoch, schedule) {
    for (const tick of schedule.ticks) {
      await this.until(epoch + tick.at)
      if (schedule.measured(tick)) this.stage = 'measurement'
      const beganAt = this.clock.now()
      const late = beganAt - (epoch + tick.at)
      if (late > LATE_MS) {
        this.missed.push({
          tick: tick.number,
          at: tick.at,
          late,
          measured: schedule.measured(tick),
        })
        if (schedule.measured(tick))
          throw new Error(`The measured tick at ${tick.at} ms was missed`)
        continue
      }
      this.tickStarts.push({ tick: tick.number, at: tick.at, beganAt })
      for (const stage of schedule.stages(tick)) {
        this.guard()
        const sentAt = this.clock.now()
        const remaining = () => Math.max(0, STAGE_MS[stage] - (this.clock.now() - sentAt))
        const command = {
          tick: tick.number,
          stage,
          index: tick.index,
          selection: schedule.selection(tick.index),
          ...(tick.transitions && { transitions: tick.transitions }),
        }
        let fences = null
        if (stage === 'confirm') {
          for (const kind of KINDS) this.processes.send(kind, { type: 'confirm-fence', ...command })
          fences = await Promise.race([
            Promise.all(
              KINDS.map((kind) =>
                this.processes.until(
                  kind,
                  (m) => m.type === 'confirm-fenced' && m.tick === tick.number,
                  remaining(),
                  `confirmation fence of tick ${tick.number}`
                )
              )
            ),
            this.abort,
          ])
          for (const index of tick.transitions ?? [tick.index]) {
            const requestedAt = this.clock.now()
            const rotation = await this.own(
              this.fixture.rotate(
                index,
                schedule.selection(index),
                Math.min(STAGE_MS.confirm - 1000, remaining())
              )
            )
            this.rotations.push({ tick: tick.number, requestedAt, ...rotation })
          }
        }
        const releasedAt = this.clock.now()
        for (const kind of KINDS) this.processes.send(kind, { type: 'stage', ...command })
        const done = await Promise.race([
          Promise.all(
            KINDS.map((kind) =>
              this.processes.until(
                kind,
                (m) => m.type === 'stage-done' && m.tick === tick.number && m.stage === stage,
                remaining(),
                `${stage} of tick ${tick.number}`
              )
            )
          ),
          this.abort,
        ])
        done.forEach((message, position) =>
          this.stages.push({
            tick: tick.number,
            stage,
            kind: KINDS[position],
            sentAt,
            ...(fences && {
              releasedAt,
              fence: {
                parentAt: fences[position].parentAt,
                receivedAt: fences[position].receivedAt,
                after: fences[position].after,
                segment: fences[position].segment,
              },
            }),
            parentAt: message.parentAt,
            workerStartedAt: message.startedAt,
            workerDoneAt: message.doneAt,
          })
        )
      }
    }
  }

  /**
   * Joins every owned resource, all-settled: every worker's natural close, all workers at once (a
   * worker that does not close naturally is killed within a bound and stays failed), the oracle
   * readings while the parent clients are open, the independent decoder, and the parent's own
   * clients. Nothing here starts new churn.
   * @param {object[]} results the worker results, possibly empty after a failure
   * @returns {Promise<{exits:object, oracle:object|null, decoder:object|null, errors:Error[]}>}
   */
  async shutdown(results) {
    const errors = []
    const attempt = async (work) => {
      try {
        return await work()
      } catch (error) {
        errors.push(error)
        return null
      }
    }
    const joined = await Promise.all(
      KINDS.filter((kind) => this.processes.workers.has(kind)).map(async (kind) => {
        const exit = await attempt(() =>
          this.failed ? this.processes.cancel(kind) : this.processes.join(kind)
        )
        if (!exit || exit.event === 'close') return [kind, exit]
        const cancelled = await attempt(() => this.processes.cancel(kind))
        return [kind, cancelled && { ...cancelled, watchdogExpired: true }]
      })
    )
    const exits = {}
    for (const [kind, exit] of joined) {
      if (exit) exits[kind] = exit
      if (!exit || exit.event !== 'close' || exit.watchdogExpired || exit.killed)
        errors.push(new Error(`Telemetry worker ${kind} did not close naturally`))
    }
    const parentMemory = [
      ...(this.parentMemory ?? []),
      readMemory('parent-after-workers', () => this.clock.now()),
    ]
    for (const result of results)
      if (exits[result.kind]?.event === 'close')
        await attempt(() =>
          this.decoder.terminal(this.directory, result.kind, result.facts.sealed.at(-1))
        )
    parentMemory.push(readMemory('parent-after-terminal-decodes', () => this.clock.now()))
    const oracle = results.length && !this.failed ? await attempt(() => this.oracle(results)) : null
    const decoder = await attempt(() => this.decoder.close())
    await attempt(() => this.fixture.close())
    return { exits, oracle, decoder, parentMemory, errors }
  }

  /**
   * Compares the independent raw logs, raw committed offsets and the share acknowledgements with
   * each worker's own ledgers, and keeps every input so a replay recomputes the same conclusions.
   * The share exchanges stay only in the share worker's archived facts; the inputs name them by
   * their capture hash.
   * @param {object[]} results the four worker results @returns {Promise<object>} the oracle row and inputs
   */
  async oracle(results) {
    const byKind = Object.fromEntries(results.map((r) => [r.kind, r.facts]))
    const logs = {}
    for (const kind of ['producer', 'consumer', 'shareConsumer'])
      logs[kind] = await this.fixture.durable(kind)
    const committed = await this.fixture.committed()
    const topicId = await this.fixture.topicId()
    const closures = this.closuresOf(byKind.shareConsumer.sealed)
    const inputs = {
      stem: this.stem,
      producer: {
        log: logs.producer,
        offered: byKind.producer.ordinary.offered,
        resolved: byKind.producer.ordinary.resolved,
      },
      consumer: {
        log: logs.consumer,
        handled: byKind.consumer.ordinary.handled,
        processed: byKind.consumer.ordinary.processed,
        committed,
      },
      shareConsumer: {
        log: logs.shareConsumer,
        topicId,
        group: this.fixture.group('shareConsumer'),
        handled: byKind.shareConsumer.ordinary.handled,
        exchangesSha256: byKind.shareConsumer.share.exchangesSha256,
        settled: byKind.shareConsumer.share.settled,
        closures,
      },
      parentOffered: this.offered,
    }
    const checks = {
      producer: OrdinaryOracle.check({ kind: 'producer', stem: this.stem, ...inputs.producer }),
      consumer: OrdinaryOracle.check({ kind: 'consumer', stem: this.stem, ...inputs.consumer }),
      shareConsumer: ShareOracle.check({
        stem: this.stem,
        ...inputs.shareConsumer,
        exchanges: byKind.shareConsumer.share.exchanges,
      }),
    }
    const violations = Object.entries(checks).flatMap(([kind, check]) =>
      check.violations.map((v) => `${kind}: ${v}`)
    )
    for (const kind of ['consumer', 'shareConsumer'])
      violations.push(
        ...identityViolations(this.offered[kind], `the parent's ${kind} offers`).map(
          (v) => `${kind}: ${v}`
        )
      )
    return {
      ok: violations.length === 0,
      violations,
      independentSetHashes: Object.fromEntries(
        Object.entries(checks).map(([kind, check]) => [kind, check.hashes])
      ),
      recordCounts: {
        producer: {
          offered: byKind.producer.ordinary.offered.length,
          handled: byKind.producer.ordinary.resolved.length,
        },
        consumer: {
          offered: this.offered.consumer.length,
          handled: byKind.consumer.ordinary.handled.length,
        },
        shareConsumer: {
          offered: this.offered.shareConsumer.length,
          handled: byKind.shareConsumer.ordinary.handled.length,
        },
      },
      inputs,
    }
  }

  /**
   * @param {object[]} sealed the sealed summaries of the share consumer's segments
   * @returns {Array<{at:number,end:number}>} the public close boundaries its observers captured
   */
  closuresOf(sealed) {
    return sealed.flatMap((pack) =>
      JSON.parse(
        fs.readFileSync(`${this.directory}/segments/${pack.transcript.name}`, 'utf8')
      ).closures.map(({ at, end }) => ({ at, end }))
    )
  }

  /**
   * Judges the finished run from raw facts and stamps the verdict. Everything it states was
   * produced after the last cleanup step, so a pass never precedes a release.
   * @param {object[]} results worker results @param {object} shutdown the joined outcomes
   * @param {object} timeline the raw stamps @param {TelemetrySchedule} schedule the catalogue
   * @returns {object} the run's result, with the heavy replay inputs under `archive`
   */
  judge(results, shutdown, timeline, schedule) {
    const { warmupMs, measureMs, cleanupMs: cleanupBound, caseMs } = this.limits
    const violations = this.failed ? [String(this.cause?.message ?? this.cause)] : []
    violations.push(...shutdown.errors.map((error) => String(error?.message ?? error)))
    const workers = []
    for (const kind of KINDS) {
      const messages = this.processes.workers.get(kind)?.messages ?? []
      const result =
        results.find((r) => r.kind === kind) ?? messages.find((m) => m.type === 'result')
      if (!result) {
        const partial = this.partial(kind, messages, shutdown.exits[kind] ?? null)
        workers.push(partial)
        this.record({
          kind: 'resource-verdict',
          clientKind: kind,
          partial: true,
          rows: partial.rows,
          failures: partial.failures,
          result: { ok: false, outcome: 'failed', violations: ['incomplete worker evidence'] },
        })
        this.record({ ...(partial.exit ?? {}), kind: 'natural-exit', clientKind: kind })
        violations.push(`${kind}: incomplete worker evidence`)
        continue
      }
      const exit = shutdown.exits[result.kind] ?? {}
      const input = { ...result.input, clientCount: 1 }
      const resourceVerdict = this.verdict(input)
      const evaluated = JournalConstancy.apply(resourceVerdict, input)
      const beside = {
        ...(result.shadow && { shadow: result.shadow }),
        ...(result.seed && { seed: result.seed }),
      }
      workers.push({
        kind: result.kind,
        input,
        result: evaluated,
        exit,
        uuid: result.uuid,
        facts: result.facts,
        ...beside,
      })
      this.record({
        kind: 'resource-verdict',
        clientKind: result.kind,
        clientId: result.clientId,
        uuid: result.uuid,
        input,
        result: evaluated,
        ...beside,
      })
      this.record({ ...exit, kind: 'natural-exit', clientKind: result.kind })
      if (!evaluated.ok)
        violations.push(
          `${result.kind}: ${[...evaluated.violations, ...(evaluated.inconclusive ?? [])].join(', ')}`
        )
      if (!result.facts.requirements.ok)
        violations.push(`${result.kind}: ${result.facts.requirements.violations}`)
      if (
        exit.event !== 'close' ||
        exit.code !== 0 ||
        exit.signal !== null ||
        exit.stderrBytes !== 0 ||
        exit.unhandled !== 0 ||
        exit.receipts !== 1 ||
        exit.killed ||
        exit.aborted ||
        exit.watchdogExpired ||
        !(Number.isFinite(exit.afterDisconnectMs) && exit.afterDisconnectMs >= 0) ||
        exit.afterDisconnectMs > this.limits.exitMs
      )
        violations.push(`${result.kind}: the child did not exit naturally`)
    }
    const need = schedule.required()
    const measured = { subscription: 0, retire: 0, reopen: 0 }
    const unit = { lifecycle: 'reopen', retire: 'retire', confirm: 'subscription' }
    for (const tick of schedule.ticks)
      if (schedule.measured(tick))
        for (const stage of schedule.stages(tick))
          if (
            KINDS.every((kind) =>
              this.stages.some(
                (s) => s.tick === tick.number && s.stage === stage && s.kind === kind
              )
            )
          ) {
            measured[unit[stage]] += stage === 'confirm' ? (tick.transitions?.length ?? 1) : 1
            if (stage === 'confirm' && tick.transitions) measured.reopen += 1
          }
    for (const action of Object.keys(need))
      if (measured[action] < need[action] || measured[action] < MINIMUMS[action])
        violations.push(`measured ${action} stages ${measured[action]} of ${need[action]}`)
    if (this.missed.some((m) => m.measured)) violations.push('a measured tick was missed')
    const oracle = shutdown.oracle
    if (!oracle) violations.push('the oracle was not computed')
    else if (!oracle.ok) violations.push(...oracle.violations)
    const setupMs = timeline.epochAt - timeline.t0
    const cleanupMs = timeline.cleanupEndAt - timeline.boundaryAt
    const totalMs = timeline.cleanupEndAt - timeline.t0
    if (setupMs > SETUP_MS) violations.push('the setup exceeded its budget')
    if (cleanupMs > cleanupBound) violations.push('the cleanup exceeded its budget')
    if (totalMs > caseMs - 2000) violations.push('the case exceeded its total budget')
    const verdict = { ok: violations.length === 0, violations, setupMs, cleanupMs, totalMs }
    const { inputs, ...oracleRow } = oracle ?? { ok: false, violations: [] }
    this.record({ kind: 'oracle', ...oracleRow })
    this.record({
      kind: 'verdict',
      ...verdict,
      warmupMs,
      measurementMs: measureMs,
      timeline,
      measured,
    })
    return {
      ...verdict,
      clientKinds: [...KINDS],
      workers,
      oracle: oracleRow,
      timeline,
      archive: {
        stem: this.stem,
        inputs,
        stages: this.stages,
        rotations: this.rotations,
        tickStarts: this.tickStarts,
        missed: this.missed,
        handshakes: this.handshakes ?? [],
        source: this.source,
        decoder: shutdown.decoder,
        parentMemory: shutdown.parentMemory,
        failureStage: this.failureStage,
        processes: shutdown.exits,
        workers: Object.fromEntries(workers.map((w) => [w.kind, w.facts ?? w])),
      },
    }
  }

  /**
   * The evidence a worker published before it failed without a result: its raw progress rows, its
   * sealed segment receipts, its failure facts and its exit, nothing summarised away.
   * @param {string} kind the worker @param {object[]} messages its received messages
   * @param {object|null} exit its joined exit facts
   * @returns {{kind:string, facts:object|null, sealed:object[], rows:object[], failures:object[],
   *   exit:object|null}} the partial worker entry
   */
  partial(kind, messages, exit) {
    const progress = messages.filter((m) => m.type === 'progress')
    const failure = messages.find((m) => m.type === 'failure')
    return {
      kind,
      facts: failure?.facts ?? null,
      sealed: failure?.facts?.sealed ?? progress.filter((m) => m.sealed).map((m) => m.sealed),
      rows: progress.filter((m) => m.row).map((m) => m.row),
      failures: failure?.failures ?? [],
      ...(failure?.shadow && { shadow: failure.shadow }),
      exit,
    }
  }

  /**
   * What the run can show at this moment, for an owner whose own bound expired before `run`
   * returned: the judged result once it exists, otherwise a failed result built from every message
   * the workers sent so far (progress rows, sealed segment receipts, failure facts) and every exit
   * already observed. Nothing is awaited, released or recorded here.
   * @returns {object} a result of the shape `run` returns, with the partial worker entries under
   *   `archive.workers`
   */
  evidence() {
    if (this.lastResult) return this.lastResult
    const workers = KINDS.map((kind) => {
      const worker = this.processes.workers.get(kind)
      const exit = worker?.exit ? this.processes.facts(worker, worker.exit) : null
      return this.partial(kind, worker?.messages ?? [], exit)
    })
    return {
      ok: false,
      violations: ['the run ended before its verdict'],
      clientKinds: [...KINDS],
      workers,
      archive: {
        stem: this.stem,
        stages: this.stages,
        rotations: this.rotations,
        tickStarts: this.tickStarts,
        missed: this.missed,
        handshakes: this.handshakes ?? [],
        source: this.source,
        parentMemory: this.parentMemory ?? [],
        failureStage: this.failureStage,
        processes: Object.fromEntries(workers.map((w) => [w.kind, w.exit])),
        workers: Object.fromEntries(workers.map((w) => [w.kind, w])),
      },
    }
  }

  /**
   * Idempotent safety net for what `run` already released: cancels every child still running, all
   * at once (one that ignores the abort is killed within a bound), and closes the parent's clients
   * and decoder if a failure stopped `run` early.
   * @returns {Promise<void>} @throws {Error|AggregateError} every cleanup failure, first one first
   */
  close() {
    this.closed ??= (async () => {
      const cancelled = await Promise.allSettled(
        KINDS.filter((kind) => {
          const worker = this.processes.workers.get(kind)
          return worker && !worker.exit
        }).map((kind) => this.processes.cancel(kind))
      )
      const errors = cancelled.filter((r) => r.status === 'rejected').map((r) => r.reason)
      for (const work of [() => this.decoder.close(), () => this.fixture.close()])
        try {
          await work()
        } catch (error) {
          errors.push(error)
        }
      if (errors.length === 1) throw errors[0]
      if (errors.length) throw new AggregateError(errors, 'Telemetry cleanup failed')
    })()
    return this.closed
  }
}

/**
 * The child side of one worker: handshake, the disabled reference control, one public target
 * client with its observer segments, setup, the supervised soak, and the one explicit-disconnect
 * receipt, after which the process is left to exit by itself. Every message names the worker and
 * carries the handshake token.
 */
class TelemetryWorkerProcess {
  /**
   * @param {object} options
   * @param {string} options.kind the one client kind
   * @param {object} options.soak injected `{ Soak, WORKLOADS, PROFILES, instrument, TELEMETRY,
   *   TelemetryResourceVerdict, logLevel, Kafka, socketFactory, startup }`
   * @param {object} [options.proc] the process, for its IPC channel; a spec supplies a fake
   * @param {{now:()=>number}} [options.clock] monotonic time
   * @param {Function} [options.sleep] real delay
   * @param {object} [options.env] where the handshake token is read, `process.env` by default
   */
  constructor({
    kind,
    soak,
    proc = process,
    clock = { now: () => performance.now() },
    sleep = wait,
    env = process.env,
  }) {
    Object.assign(this, { kind, soak, proc, clock, sleep, env })
    this.inbox = new Inbox()
    this.pending = new Map()
    this.nextDecode = 0
    this.worker = null
  }

  /**
   * @param {object} message what to tell the parent
   * @param {object} [options] delivery options
   * @param {boolean} [options.flush] wait for the IPC write before disconnecting the channel
   * @returns {boolean|Promise<void>} the send status, or the completion of a requested flush
   * @throws {Error} when the IPC channel cannot accept the message
   */
  send(message, { flush = false } = {}) {
    const envelope = { ...message, kind: this.kind, token: this.env[WORKER_TOKEN] }
    if (!flush) return this.proc.send(envelope)
    return new Promise((resolve, reject) => {
      this.proc.send(envelope, (error) => (error ? reject(error) : resolve()))
    })
  }

  /** @returns {void} routes parent messages: decode answers settle their request, the rest wait */
  listen() {
    this.proc.on('message', (message) => {
      if (message?.type === 'decoded') {
        const pending = this.pending.get(message.id)
        this.pending.delete(message.id)
        if (!pending) return
        if (message.error) pending.reject(new Error(message.error))
        else pending.resolve(message.body)
      } else this.inbox.push(message)
    })
    this.proc.on('unhandledRejection', () => this.send({ type: 'unhandled' }))
  }

  /**
   * The decoder seam of the observer: forwards bounded raw input to the parent and returns the real
   * pinned Java result.
   * @param {Buffer} raw the decompressed MetricsData, at most 4096 bytes
   * @returns {Promise<object>} the decoded body @throws {Error} when the parent could not decode
   */
  askDecode(raw) {
    if (this.pending.size >= 16)
      return Promise.reject(new Error('Too many decode requests pending'))
    const id = ++this.nextDecode
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      this.send({ type: 'decode', id, rawB64: raw.toString('base64') })
    })
  }

  /**
   * The seeded control of this worker, selected by the environment.
   * @returns {LiveSeed|null} the seed that retains through the product `Kafka` class, or null
   * @throws {Error} for an unknown seed mode
   */
  seeded() {
    return LiveSeed.of(this.env[LIVE_SEED.env], {
      root: this.soak.Kafka,
      limits: this.soak.TELEMETRY,
    })
  }

  /**
   * Forwards one soak row to the parent. The row of a scheduled sample also retains the seeded
   * control's buffer for the interval that follows it, so the next reading carries it.
   * @param {object} row a soak row @param {LiveSeed|null} seed the seeded control, if any
   * @returns {void}
   */
  progress(row, seed) {
    if (row.kind === 'baseline') this.peak?.phase('M')
    if (row.kind === 'teardown') this.peak?.phase('closed')
    this.send({ type: 'progress', row })
    if (row.kind === 'sample') seed?.retain()
  }

  /**
   * Builds the one public client of this kind with telemetry on.
   * @param {object} kafka the instrumented `Kafka` @param {object} config the parent's configuration
   * @returns {object} the public client
   */
  create(kafka, config) {
    const common = { enableMetricsPush: true }
    if (this.kind === 'producer') return kafka.producer({ ...common, idempotent: true })
    if (this.kind === 'admin') return kafka.admin(common)
    if (this.kind === 'shareConsumer')
      return kafka.shareConsumer({ ...common, groupId: config.group })
    return kafka.consumer({
      ...common,
      groupId: config.group,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTimeInMs: 500,
      readUncommitted: false,
    })
  }

  /**
   * @param {object} parts what the reference holds when the run is judged
   * @param {EngineCodeJournal} [parts.engine] takes the code sizes of the final reading
   * @returns {object} the replayable reference facts: source and capability, every reading, the
   *   control with its frozen catalogue, the enabled replay of that catalogue, the native work
   *   journal and its sampler, and the host before the control, at the boundary and after the run
   */
  reference({ loaded, control, host, memory, journal, sampler, foreground, allocation, engine }) {
    const reference = {
      version: 'rss-reference-2',
      source: memory.source ? 'proc-status-vmhwm' : null,
      units: 'bytes',
      capability: { gc: memory.gcCapability().native, peak: readHighWater() !== null },
      startup: this.soak.startup ?? null,
      loaded,
      control,
      foreground,
      journals: {
        version: 2,
        allocationBeforeControl: [
          'native',
          'pending-windows',
          'memory-samples',
          'engine-code',
          ...(this.soak.TELEMETRY?.heapSpaceRows ? ['heap-spaces'] : []),
          'ordinary',
          ...(this.kind === 'shareConsumer' ? ['share'] : []),
        ],
        allocationAfterControlBeforeM: [],
        allocation,
        capacities: journalLimits(this.soak.TELEMETRY),
        setupAllowanceBytes: this.soak.TelemetryResourceVerdict.setupAllowance(this.kind),
        rssEffect:
          'All journal backing storage is touched before c0 and held through closed. It is included in the disabled RSS baseline, the setup allowance and the 160 MiB absolute peak. The setup allowance is 96 MiB, plus for the share consumer its exact declared journal bytes; no bytes are subtracted from any reading.',
      },
      native: { journal: journal.summary(), sampler: sampler.summary() },
      final: readMemory('final', () => this.clock.now()),
      host: { ...host, after: hostContext(() => this.clock.now()) },
    }
    engine?.record(reference.final)
    return reference
  }

  /**
   * The stamps a parent binds this worker to: its clock origin, process, Node build and flags, the
   * loaded module count, codec capability and the path and digest of this module and its driver.
   * @returns {object} plain data sent with `hello`
   */
  stamps() {
    return {
      origin: performance.timeOrigin,
      pid: process.pid,
      node: process.version,
      v8: process.versions.v8,
      platform: process.platform,
      arch: process.arch,
      execArgv: process.execArgv,
      modules: Object.keys(require.cache).length,
      codecs: { gzip: true, zstd: zstdCodec.isSupported === true },
      source: sourceStamp(require.main?.filename ?? __filename),
    }
  }

  /**
   * Runs the worker to its receipt and result. An `abort` from the parent rejects whatever message
   * the worker waits for, so a setup or run in progress unwinds through its own cleanup.
   * @returns {Promise<{ok:boolean, violations:string[]}>} the resource verdict of this one client
   * @throws {Error} a failed setup or run, after the failure message was still sent
   */
  async run() {
    const { soak, kind, proc } = this
    if (typeof proc.send !== 'function')
      throw new Error('A telemetry worker needs its parent channel')
    this.listen()
    this.send({ type: 'hello', ...this.stamps() })
    const resources = new ProcessResources()
    const memory = new ProcessMemory({ now: () => this.clock.now() })
    const storage = journalLimits(soak.TELEMETRY)
    const sampleJournal = new FixedJournal({
      name: 'memory samples',
      capacity: storage.samples,
      bytes: storage.sampleBytes,
    })
    const engine = new EngineCodeJournal({ capacity: soak.TELEMETRY?.engineRows })
    const heapSpaces = soak.TELEMETRY?.heapSpaceRows
      ? new HeapSpaceJournal({ capacity: soak.TELEMETRY.heapSpaceRows })
      : null
    const nativeRows = soak.TELEMETRY?.nativeRows ?? NATIVE_ROWS
    const sampler = new PendingSampler(memory, {
      capacity: nativeRows,
      set: (fn, ms) => resources.as('fixture:native-sampler', () => setInterval(fn, ms)),
    })
    const peak = soak.sharePeak?.({ sampler, limits: soak.TELEMETRY }) ?? null
    this.peak = peak
    let segments = null
    const journal = new NativeJournal({
      memory,
      sampler,
      capacity: nativeRows,
      fixed: true,
      epoch: () => segments?.current().index ?? 0,
    })
    const target = (work) => resources.as('target', work)
    let verdict
    let failure
    let failed = false
    let worker = null
    let seed = null
    try {
      const config = await this.inbox.take((m) => m.type === 'config')
      seed = this.seeded()
      const schedule = new TelemetrySchedule(soak.TELEMETRY)
      const ledger =
        kind === 'shareConsumer'
          ? new ShareLedger({ group: config.group, clock: this.clock, storage })
          : null
      const ordinary = new OrdinaryWork({
        kind,
        client: null,
        stem: config.stem,
        topic: config.topic,
        ctl: config.ctl,
        clock: this.clock,
        sleep: this.sleep,
        ledger,
        slots: schedule.corpusSlots(),
        storage,
      })
      const allocation = journalState([
        ordinary,
        ...(ledger ? [ledger] : []),
        journal,
        sampler,
        sampleJournal,
        engine,
        ...(heapSpaces ? [heapSpaces] : []),
      ])
      resources.start()
      if (peak) resources.as('fixture:share-peak', () => peak.start())
      peak?.phase('loaded')
      const loaded = readMemory('loaded', () => this.clock.now())
      engine.record(loaded)
      const host = { before: hostContext(() => this.clock.now()) }
      peak?.phase('control')
      const control = await new ReferenceControl({
        kind,
        config,
        Kafka: soak.Kafka,
        socketFactory: soak.socketFactory,
        logLevel: soak.logLevel,
        resources,
        clock: this.clock,
        sleep: this.sleep,
        collections: () => memory.collections,
        engine,
      }).run()
      peak?.phase('setup')
      host.boundary = hostContext(() => this.clock.now())
      journal.observe('gzip', gzipCodec)
      journal.observe('zstd', zstdCodec)
      segments = TelemetrySegments.ordinary(SCENARIO, kind, {
        directory: config.directory,
        clock: this.clock,
        observer: {
          line: config.line,
          mode: 'configured',
          decoder: (raw) => this.askDecode(raw),
          completeStream: false,
          sinkBytes: soak.TELEMETRY.sinkBytes ?? CHURN_SINK_BYTES,
          consumed: new Set(),
        },
      })
      const tap = new TelemetryTap(segments, { share: ledger, clock: this.clock })
      const { kafka, probe } = soak.instrument(
        {
          clientId: `${config.stem}-${kind}`,
          brokers: config.brokers,
          logLevel: soak.logLevel.NOTHING,
          socketFactory: tap.factory,
        },
        { publicOnly: true, memory: true, identities: () => resources.identities() }
      )
      const client = this.create(kafka, config)
      ordinary.client = client
      const owned = segments
      const WorkerRegistry = soak.SharePeakRegistry ?? Registry
      const Worker = soak.SharePeakWorker ?? TelemetryWorker
      worker = new Worker({
        kind,
        client,
        tap,
        segments,
        registry: new WorkerRegistry(client, kind, DESCRIPTORS, this.clock, () =>
          writeFence(owned)
        ),
        clock: this.clock,
        sleep: this.sleep,
        limits: soak.TELEMETRY,
        schedule,
        inbox: this.inbox,
        send: (message) => this.send(message),
        target,
        ledger,
        foreground: { manifest: control.manifest, prefix: `${config.stem}:ctl-on` },
        ordinary,
        peak,
      })
      const uuid = await worker.prepare()
      const measured = {
        snapshot: async () => {
          worker.registry.restore()
          const admittedAt = this.clock.now()
          const before = memory.collections
          const snapshot = await probe.snapshot()
          await new Promise((resolve) => setImmediate(resolve))
          await new Promise((resolve) => setImmediate(resolve))
          const reading = readMemory('settled', () => this.clock.now())
          engine.record(reading)
          heapSpaces?.record(reading, memory.collections)
          return {
            ...snapshot,
            memory: snapshot.memory && {
              ...reading,
              retainedBytes: reading.retained,
              probe: snapshot.memory,
              gc: { admittedAt, completedAt: reading.at, observed: memory.collections - before },
            },
            registryDigest: worker.registry.digest(uuid),
            journals: {
              ...journalState([
                worker.ordinary,
                ...(ledger ? [ledger] : []),
                journal,
                sampler,
                sampleJournal,
                engine,
                ...(heapSpaces ? [heapSpaces] : []),
              ]),
              owners: {
                ordinary: worker.ordinary.journalState(),
                ...(ledger ? { share: ledger.journalState() } : {}),
                native: journal.journalState(),
                pendingWindows: sampler.journalState(),
                samples: sampleJournal.journalState(),
                engine: engine.journalState(),
                ...(heapSpaces ? { heapSpaces: heapSpaces.journalState() } : {}),
              },
            },
          }
        },
      }
      let input
      let shadow = null
      const judge = (run) => {
        sampler.stop()
        journal.restoreAll()
        input = {
          clientCount: 1,
          baseline: run.baseline,
          samples: run.samples.map((s) => ({
            admittedAtMs: s.admittedPhaseMs,
            completedAtMs: s.phaseMs,
            atMs: s.phaseMs,
            snapshot: s.snapshot,
          })),
          teardown: run.teardown,
          measurementMs: worker.timing.measurementMs,
          cleanupMs: worker.timing.cleanupMs,
          reference: this.reference({
            loaded,
            control,
            host,
            memory,
            journal,
            sampler,
            foreground: worker.claims.foreground,
            allocation,
            engine,
          }),
          sampler: { skipped: run.skipped, readFailures: run.readFailures },
          ...((peak || soak.TELEMETRY?.diagnostic) && {
            diagnostic: true,
            acceptanceEligible: false,
          }),
        }
        const judged = JournalConstancy.apply(soak.TelemetryResourceVerdict.evaluate(input), input)
        if (peak) {
          peak.phase('final')
          peak.stop()
        }
        shadow = {
          ...engine.report(input.samples),
          ...(peak && { sharePeak: peak.report() }),
          ...(heapSpaces && { heapSpaces: heapSpaces.values() }),
        }
        return judged
      }
      const soakRun = new soak.Soak({
        kafka,
        probe: measured,
        phases: soak.PROFILES.telemetry,
        context: { telemetryChurn: worker },
        sampleMs: soak.TELEMETRY.sampleMs,
        sampleAt: soak.TELEMETRY.sampleAt,
        sampleJournal,
        warmupMs: soak.TELEMETRY.warmupMs,
        judge: (run) =>
          judge({ ...run, skipped: soakRun.skipped, readFailures: soakRun.readFailures }),
        record: (row) => this.progress(row, seed),
        workloads: soak.WORKLOADS,
        clock: this.clock,
      })
      worker.sampler = soakRun
      this.send({ type: 'ready', uuid, clientId: `${config.stem}-${kind}` })
      await this.inbox.take((m) => m.type === 'start')
      verdict = await resources.as('fixture:sampler', () => soakRun.run())
      await this.send(
        {
          type: 'result',
          clientId: `${config.stem}-${kind}`,
          uuid,
          verdict,
          input,
          facts: worker.facts(),
          shadow,
          seed: seed?.summary() ?? null,
        },
        { flush: true }
      )
    } catch (error) {
      failed = true
      failure = error
      const causes = [error]
      try {
        await worker?.abort()
      } catch (abortError) {
        causes.push(abortError)
      }
      let facts = null
      try {
        facts = worker?.facts() ?? null
      } catch (factError) {
        causes.push(factError)
      }
      peak?.stop()
      await this.send(
        {
          type: 'failure',
          facts,
          ...(peak && { shadow: { sharePeak: peak.report() } }),
          native: { journal: journal.summary(), sampler: sampler.summary() },
          failures: causes.map((cause) => ({
            name: cause?.name ?? typeof cause,
            message: String(cause?.message ?? cause).slice(0, 2048),
          })),
          message: causes
            .map((c) => String(c?.message ?? c))
            .join('; ')
            .slice(0, 2048),
        },
        { flush: true }
      )
    } finally {
      peak?.stop()
      sampler.stop()
      journal.restoreAll()
      memory.stop()
      resources.stop()
      if (proc.connected) proc.disconnect?.()
    }
    if (failed) throw failure
    return verdict
  }
}

module.exports = {
  FixedJournal,
  JournalConstancy,
  declaredJournalBytes,
  journalLimits,
  KINDS,
  NATIVE_ROWS,
  SHARE_ROW_BYTES,
  SCENARIO,
  SCHEDULE_STEPS: STEPS,
  MINIMUMS,
  CORPUS,
  CONTROL,
  SETUP_MS,
  NATURAL_EXIT_MS,
  KILL_JOIN_MS,
  WORKER_ENV,
  WORKER_TOKEN,
  SUBSCRIPTIONS,
  DecoderCoordinator,
  EngineCodeJournal,
  FrameCounter,
  Inbox,
  OrdinaryOracle,
  OrdinaryWork,
  ParentFixture,
  ProcessResources,
  ReferenceControl,
  Registry,
  ShareExchanges,
  ShareLedger,
  ShareOracle,
  TelemetryChurn,
  TelemetryProcesses,
  TelemetryProofs,
  TelemetrySchedule,
  TelemetryTap,
  TelemetryWorker,
  TelemetryWorkerProcess,
  WORKER_FLAGS,
  checkLog,
  compactRuns,
  controlManifest,
  runCatalogue,
  hostContext,
  indexOf,
  labelOf,
  logInputs,
  readHighWater,
  readMemory,
  recordId,
  settledReading,
  sourceStamp,
  valueOf,
  wait,
  within,
  writeFence,
}
