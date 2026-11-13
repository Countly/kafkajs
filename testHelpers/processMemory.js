'use strict'

/**
 * Process memory measurement for the single-client resource arms. Its dependencies load no product, so
 * an arm can take its startup reading before any application module is imported.
 *
 * Absolute current-image high water comes from `/proc/self/status` `VmHWM` (kibibytes, scaled to
 * bytes). Its approximate readings can decrease, so judges retain the maximum across all reads
 * and separately check current-reading coverage. The raw `process.resourceUsage().maxRSS * 1024` is
 * recorded beside it, can also decrease, and is never substituted for it. Nothing here writes
 * `/proc/self/clear_refs` or resets a high water.
 */
const fs = require('node:fs')
const { FixedJournal } = require('./fixedJournal')
const os = require('node:os')
const v8 = require('node:v8')
const { performance, PerformanceObserver } = require('node:perf_hooks')

const KIB = 1024

/**
 * Pin both semi-space sizes to 1 MiB and request eager bytecode and feedback allocation.
 * Execution can still grow compiled code and metadata; these flags do not establish retained stability.
 */
const MEMORY_FLAGS = Object.freeze([
  '--expose-gc',
  '--no-lazy',
  '--no-lazy-feedback-allocation',
  '--min-semi-space-size=1',
  '--max-semi-space-size=1',
])

/** Collection convergence is stricter than any retained gate; it never changes an acceptance value. */
const SETTLE = Object.freeze({ rounds: 6, bytes: 16384 })
/** Only the resource control c0/c1 may spend this many collection pairs reaching convergence. */
const CONTROL_SETTLE_ROUNDS = 12

/**
 * @param {object|undefined} before preceding post-GC reading
 * @param {object} after latest post-GC reading
 * @returns {boolean} whether heap and backing-store accounting have stopped changing materially
 */
const collectionsSettled = (before, after) =>
  before !== undefined &&
  ['heapUsed', 'external', 'arrayBuffers'].every(
    (field) => Math.abs(after[field] - before[field]) <= SETTLE.bytes
  )

/**
 * The quantity the closed-ceiling, first-use and control-stability gates judge: the bytes the
 * heap and external memory hold, without what V8 spends on bytecode and compiled code. It is read
 * from the final checkpoint of a settled reading, so it is the same moment as `retained`. It never
 * feeds the RSS, external or peak gates.
 * @param {{heapUsed: number, external: number, gc: {checkpoints: Array<{engine: {bytecode: number,
 *   code: number}}>}}} reading a settled reading with its collection checkpoints
 * @returns {number} `heapUsed + external - (bytecode + code)` at the final checkpoint
 */
const dataBytes = (reading) => {
  const { bytecode, code } = reading.gc.checkpoints.at(-1).engine
  return reading.heapUsed + reading.external - (bytecode + code)
}

/**
 * Records adjacent decreases of valid peak counters without changing the readings. A rebound
 * below an earlier maximum is not another decrease; missing or invalid counters are left to the
 * caller's validation. The signed delta is `after - before`.
 * @param {object[]} readings raw readings in catalogue order
 * @param {string[]} fields peak counters to inspect
 * @returns {Array<{index:number, phase:string, field:string, before:number, after:number,
 *   delta:number}>} every observed decrease, in reading and field order
 */
const peakReadingDecreases = (readings, fields) => {
  const decreases = []
  for (let index = 1; index < readings.length; index++) {
    for (const field of fields) {
      const before = readings[index - 1]?.[field]
      const after = readings[index]?.[field]
      if (
        Number.isSafeInteger(before) &&
        before >= 0 &&
        Number.isSafeInteger(after) &&
        after >= 0 &&
        after < before
      )
        decreases.push({
          index,
          phase: readings[index].phase,
          field,
          before,
          after,
          delta: after - before,
        })
    }
  }
  return decreases
}

/** What the selected absolute peak source is called and measures. */
const PEAK_SOURCE = Object.freeze({ name: 'proc-self-status-vmhwm', units: 'bytes' })

/** The sampling period while native work is pending, in milliseconds. */
const SAMPLE_MS = 20

/** The pause between the two collections of a settled reading, in milliseconds. */
const GC_PAUSE_MS = 100

/** The most pending windows a sampler describes one by one; a further window sets `overflow`. */
const WINDOW_CAPACITY = 32

/**
 * @param {string} text the contents of `/proc/self/status`
 * @param {string} name a field such as `VmHWM`
 * @returns {number|null} the field in bytes, or null when it is absent or unparseable
 */
const statusBytes = (text, name) => {
  const match = new RegExp(`^${name}:\\s+(\\d+) kB$`, 'm').exec(text)
  return match ? Number(match[1]) * KIB : null
}

/**
 * @returns {{ vmhwm: number, vmrss: number }|null} the kernel's high-water and current resident
 *   bytes of this process, or null where `/proc/self/status` is unavailable or lacks either field
 */
const readProcStatus = () => {
  try {
    const text = fs.readFileSync('/proc/self/status', 'utf8')
    const vmhwm = statusBytes(text, 'VmHWM')
    const vmrss = statusBytes(text, 'VmRSS')
    return vmhwm === null || vmrss === null ? null : { vmhwm, vmrss }
  } catch {
    return null
  }
}

/**
 * @param {() => ({vmhwm:number,vmrss:number}|null)} [read] reads the kernel status
 * @returns {{ name: string, units: string }|null} the selected absolute peak source, or null when
 *   this platform offers no usable nonresetting kernel peak (sampled RSS alone is insufficient)
 */
const selectPeakSource = (read = readProcStatus) => (read() === null ? null : { ...PEAK_SOURCE })

/** @returns {{memorySomeAvg10: number}|null} the kernel's memory pressure, when it reports one */
const memoryPressure = () => {
  try {
    const match = /some avg10=([\d.]+)/.exec(fs.readFileSync('/proc/pressure/memory', 'utf8'))
    return match ? { memorySomeAvg10: Number(match[1]) } : null
  } catch {
    return null
  }
}

/** @returns {object} the CPU, load and memory context of this host right now */
const hostContext = () => ({
  cpus: os.cpus().length,
  load: os.loadavg(),
  freeBytes: os.freemem(),
  totalBytes: os.totalmem(),
  psi: memoryPressure(),
})

/**
 * @param {number} ms the pause @returns {Promise<void>} resolves after a real timer of that length
 */
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/** @returns {Promise<void>} resolves after one macrotask, so queued callbacks can run */
const turn = () => new Promise((resolve) => setImmediate(resolve))

/**
 * Reads current memory and the kernel high water, tracking what each mark has to cover. A mark
 * records the high water at a phase boundary together with the largest current RSS and external
 * reading taken since the previous mark, so a replay can check the kernel maximum covers them.
 */
class ProcessMemory {
  /**
   * @param {object} [sources] replaceable readings for tests
   * @param {() => ({vmhwm:number,vmrss:number}|null)} [sources.read] kernel status
   * @param {(() => void)|undefined} [sources.gc] a full collection; defaults to `globalThis.gc`
   * @param {() => object} [sources.usage] `process.memoryUsage`
   * @param {() => number} [sources.maxRss] raw lifetime `maxRSS` in bytes
   * @param {() => string[]} [sources.modules] loaded CommonJS module filenames
   * @param {() => number} [sources.now] monotonic milliseconds
   * @param {(ms: number) => Promise<void>} [sources.sleep] the pause between collections
   * @param {() => Promise<void>} [sources.settleTurn] one turn after the second collection, in
   *   which the engine's collection callbacks run; a settled reading waits for two
   * @returns {ProcessMemory} an observer with no marks yet
   */
  constructor({
    read = readProcStatus,
    gc = globalThis.gc,
    usage = () => process.memoryUsage(),
    maxRss = () => process.resourceUsage().maxRSS * KIB,
    now = () => performance.now(),
    modules = () => Object.keys(require.cache),
    sleep = pause,
    settleTurn = turn,
  } = {}) {
    Object.assign(this, { read, gc, usage, maxRss, now, modules, sleep, settleTurn })
    this.loadedModules = null
    this.source = selectPeakSource(read)
    this.marks = []
    this.covered = 0
    this.coveredExternal = 0
    this.collections = 0
    this.observer = new PerformanceObserver((list) => {
      this.collections += list.getEntries().length
    })
    this.observer.observe({ entryTypes: ['gc'] })
  }

  /**
   * @returns {{ available: boolean, native: boolean }} whether a collection function exists and is
   *   the engine's own rather than a replacement
   */
  gcCapability() {
    const available = typeof this.gc === 'function'
    const native =
      available && /\{\s*\[native code\]\s*\}/.test(Function.prototype.toString.call(this.gc))
    return { available, native }
  }

  /**
   * Takes one current reading and folds it into what the next mark covers. `arrayBuffers` is part
   * of `external`, so `retained` does not add it again.
   * @returns {{at:number, rss:number, heapUsed:number, external:number, arrayBuffers:number,
   *   retained:number}} the reading
   */
  current() {
    const usage = this.usage()
    const rss = usage.rss
    this.covered = Math.max(this.covered, rss)
    this.coveredExternal = Math.max(this.coveredExternal, usage.external)
    return {
      at: this.now(),
      rss,
      heapUsed: usage.heapUsed,
      external: usage.external,
      arrayBuffers: usage.arrayBuffers,
      retained: usage.heapUsed + usage.external,
    }
  }

  /**
   * Records the absolute high water at a phase boundary and starts a new covered interval. The
   * current reading is taken before the kernel's; the judge checks whether the peak covers it.
   * @param {string} phase the fixed catalogue name of the boundary
   * @returns {{phase:string, at:number, rss:number, vmhwm:number|null, maxRss:number,
   *   covered:number, coveredExternal:number, imports:{count:number,late:string[]}|null}} the mark, also kept in `marks`; `vmhwm` is null
   *   when no peak source was selected. `imports` freezes the loaded module filenames at `loaded`
   *   and records every later addition without keeping module exports alive.
   */
  mark(phase) {
    const files = this.modules()
    if (phase === 'loaded' && this.loadedModules === null) this.loadedModules = new Set(files)
    const imports = this.loadedModules
      ? { count: files.length, late: files.filter((file) => !this.loadedModules.has(file)).sort() }
      : null
    const usage = this.usage()
    const status = this.source ? this.read() : null
    const mark = {
      phase,
      at: this.now(),
      rss: usage.rss,
      vmhwm: status ? status.vmhwm : null,
      maxRss: this.maxRss(),
      covered: Math.max(this.covered, usage.rss),
      coveredExternal: Math.max(this.coveredExternal, usage.external),
      imports,
    }
    this.covered = 0
    this.coveredExternal = 0
    this.marks.push(mark)
    return mark
  }

  /**
   * Repeats the two-collection/100 ms protocol until two successive readings converge, with at
   * most six pairs by default, or twelve for resource control c0/c1. Every reading remains in the
   * evidence and contributes to peak coverage. The last reading is returned even when convergence
   * fails; no minimum or favourable endpoint is
   * selected. Every checkpoint records the engine's bytecode and compiled-code sizes; only
   * `dataBytes` subtracts them, and only for the gates that name it.
   * @param {Function|null} [observe] diagnostic-only synchronous (round, stage) scalar observer;
   *   omitted in acceptance, with no additional reads, collections or turns
   * @param {Function|null} [completed] diagnostic-only synchronous observer of the final reading
   * @param {number} [rounds=SETTLE.rounds] collection-pair budget; only resource control c0/c1 use
   *   `CONTROL_SETTLE_ROUNDS`, with the same convergence rule
   * @returns {Promise<object>} the final reading and all GC admission/completion/engine facts
   * @throws {Error} when no collection function exists
   */
  async settled(observe = null, completed = null, rounds = SETTLE.rounds) {
    if (typeof this.gc !== 'function') throw new Error('A settled reading needs node --expose-gc')
    const admittedAt = this.now()
    const before = this.collections
    const checkpoints = []
    for (let round = 0; round < rounds; round++) {
      const started = this.now()
      const observed = this.collections
      this.gc()
      await this.sleep(GC_PAUSE_MS)
      this.gc()
      if (observe) observe(round, 'after-second-gc')
      if (observe) observe(round, 'before-turn-1')
      await this.settleTurn()
      if (observe) observe(round, 'after-turn-1')
      if (observe) observe(round, 'before-turn-2')
      await this.settleTurn()
      if (observe) observe(round, 'after-turn-2')
      const code = v8.getHeapCodeStatistics()
      const reading = this.current()
      const checkpoint = {
        ...reading,
        admittedAt: started,
        observed: this.collections - observed,
        engine: { bytecode: code.bytecode_and_metadata_size, code: code.code_and_metadata_size },
      }
      const previous = checkpoints.at(-1)
      checkpoints.push(checkpoint)
      if (collectionsSettled(previous, checkpoint)) break
    }
    const last = checkpoints.at(-1)
    const reading = Object.fromEntries(
      ['at', 'rss', 'heapUsed', 'external', 'arrayBuffers', 'retained'].map((key) => [
        key,
        last[key],
      ])
    )
    const result = {
      ...reading,
      gc: { admittedAt, completedAt: reading.at, observed: this.collections - before, checkpoints },
    }
    if (completed) completed(result)
    return result
  }

  /** @returns {void} stops watching collections */
  stop() {
    this.observer.disconnect()
  }
}

/**
 * Reads current memory every 20 ms while at least one native job is pending and keeps a bounded
 * summary: the count, the largest gap between readings of one pending window, the peaks, and per
 * pending window its first and last reading time, reading count and RSS and external peaks.
 */
class PendingSampler {
  /**
   * @param {ProcessMemory} memory takes the readings
   * @param {object} [timers] replaceable timers for tests
   * @param {(fn: () => void, ms: number) => *} [timers.set] `setInterval`
   * @param {(handle: *) => void} [timers.clear] `clearInterval`
   * @param {number|null} [timers.capacity] fixed scalar window archive; null preserves the default
   * @returns {PendingSampler} a sampler with nothing pending
   */
  constructor(memory, { set = setInterval, clear = clearInterval, capacity = null } = {}) {
    Object.assign(this, { memory, set, clear })
    this.pending = 0
    this.handle = null
    this.windowLast = null
    this.count = 0
    this.windows = 0
    this.windowRows =
      capacity === null
        ? []
        : new FixedJournal({ name: 'pending windows', capacity, bytes: capacity * 256 })
    this.window = null
    this.overflow = false
    this.maxGapMs = 0
    this.peak = { rss: 0, heapUsed: 0, external: 0, arrayBuffers: 0 }
  }

  /**
   * Takes one reading and folds it into the summary.
   * @returns {object} the reading
   */
  take() {
    const reading = this.memory.current()
    this.count += 1
    if (this.window) {
      this.window.count += 1
      this.window.end = reading.at
      this.window.rss = Math.max(this.window.rss, reading.rss)
      this.window.external = Math.max(this.window.external, reading.external)
    }
    for (const key of Object.keys(this.peak))
      this.peak[key] = Math.max(this.peak[key], reading[key])
    if (this.windowLast !== null)
      this.maxGapMs = Math.max(this.maxGapMs, reading.at - this.windowLast)
    this.windowLast = reading.at
    return reading
  }

  /**
   * One more piece of native work is pending: reads now and starts the 20 ms interval if idle.
   * Once `stop` has dropped work still pending, a submission can find no window; it is still read.
   * @returns {object} the reading taken at submission
   */
  begin() {
    if (this.pending === 0) {
      this.windows += 1
      this.window = { begin: null, end: null, count: 0, rss: 0, external: 0 }
      if (!(this.windowRows instanceof FixedJournal)) {
        if (this.windowRows.length < WINDOW_CAPACITY) this.windowRows.push(this.window)
        else this.overflow = true
      }
      this.windowLast = null
      this.handle = this.set(() => this.take(), SAMPLE_MS)
    }
    this.pending += 1
    const reading = this.take()
    if (this.window) this.window.begin ??= reading.at
    return reading
  }

  /**
   * One piece of native work settled: reads now and stops the interval when none remains.
   * @returns {object} the reading taken at settlement
   */
  end() {
    this.pending -= 1
    const reading = this.take()
    if (this.pending === 0) {
      this.clear(this.handle)
      this.handle = null
      if (this.windowRows instanceof FixedJournal) this.windowRows.push(this.window)
      this.window = null
    }
    return reading
  }

  /**
   * Requests cleanup without discarding a pending window. An outstanding job's `end` owns the
   * interval until settlement, including work shorter than the 20 ms sampling period.
   * @returns {void}
   */
  stop() {
    if (this.pending > 0) return
    if (this.handle !== null) this.clear(this.handle)
    this.handle = null
    this.window = null
    this.pending = 0
  }

  /** @returns {object} fixed window capacity and append cursor, without materialising rows */
  journalState() {
    return this.windowRows.journalState()
  }

  /**
   * @returns {{count:number, windows:number, pendingWindows:Array<{begin:number, end:number,
   *   count:number, rss:number, external:number}>, overflow:boolean, maxGapMs:number,
   *   intervalMs:number, peak:{rss:number,heapUsed:number,external:number,arrayBuffers:number}}}
   *   bounded facts; `overflow` says more windows ran than are described
   */
  summary() {
    return {
      count: this.count,
      windows: this.windows,
      pendingWindows:
        this.windowRows instanceof FixedJournal
          ? [...this.windowRows.values(), ...(this.window ? [{ ...this.window }] : [])]
          : this.windowRows.map((window) => ({ ...window })),
      overflow: this.overflow,
      maxGapMs: this.maxGapMs,
      intervalMs: SAMPLE_MS,
      peak: { ...this.peak },
    }
  }
}

/**
 * Journals the production native compression calls an arm makes, without replacing them. A wrapper
 * calls the original, returns exactly what it returned and only observes: weak-issued IDs of the
 * job, input and result, byte lengths, the distinct backing stores they sit on (views of one store
 * are counted once) and a reading at submission and at settlement.
 */
class NativeJournal {
  /**
   * @param {object} options
   * @param {ProcessMemory} options.memory takes the readings
   * @param {PendingSampler} options.sampler samples while a job is pending
   * @param {number} [options.capacity=32] the most rows kept; a further job sets `overflow`
   * @param {() => number} [options.epoch] the arm's lifecycle counter; a job that settles in a
   *   later epoch than it was submitted in is a retired completion
   * @param {(row: object) => void} [options.onSubmit] called synchronously after a submission
   * @param {boolean} [options.fixed] reserve and touch scalar byte storage before M; overflow throws
   * @returns {NativeJournal} a journal observing nothing yet
   */
  constructor({
    memory,
    sampler,
    capacity = 32,
    epoch = () => 0,
    onSubmit = () => {},
    fixed = false,
  }) {
    Object.assign(this, { memory, sampler, capacity, epoch, onSubmit })
    this.rows = fixed
      ? new FixedJournal({ name: 'native work', capacity, bytes: capacity * 2048 })
      : []
    this.submitted = 0
    this.live = new Set()
    this.maxLive = 0
    this.overflow = false
    this.issued = new WeakMap()
    this.nextId = 0
    this.restores = []
  }

  /**
   * @param {object|Function|null|undefined} object a job, buffer or backing store
   * @returns {number|null} its stable issued ID, kept without keeping the object alive
   */
  idOf(object) {
    if (object === null || (typeof object !== 'object' && typeof object !== 'function')) return null
    if (!this.issued.has(object)) this.issued.set(object, ++this.nextId)
    return this.issued.get(object)
  }

  /**
   * Wraps `module.compress` for the life of the journal.
   * @param {string} codec `gzip` or `zstd`
   * @param {{ compress: Function }} module the production codec module the payload calls
   * @returns {void}
   */
  observe(codec, module) {
    const original = module.compress
    const journal = this
    module.compress = function (...args) {
      const row = journal.begin(codec, args[0])
      let result
      try {
        result = original.apply(this, args)
      } catch (error) {
        journal.end(row, undefined)
        throw error
      }
      Promise.resolve(result).then(
        (value) => journal.end(row, value),
        () => journal.end(row, undefined)
      )
      if (row) journal.onSubmit(row)
      return result
    }
    this.restores.push(() => {
      module.compress = original
    })
  }

  /**
   * Starts a row for one submission.
   * @param {string} codec the codec
   * @param {{buffer: Buffer}} job the argument object the payload passes
   * @returns {object|null} the row, or null once the capacity is exceeded
   */
  begin(codec, job) {
    if (this.submitted >= this.capacity) {
      this.overflow = true
      if (this.rows instanceof FixedJournal) throw new Error('native work journal overflow')
      return null
    }
    const input = job?.buffer
    const row = {
      id: ++this.submitted,
      codec,
      jobId: this.idOf(job),
      inputId: this.idOf(input),
      inputBytes: input?.length ?? 0,
      inputBackingId: this.idOf(input?.buffer),
      inputBackingBytes: input?.buffer?.byteLength ?? 0,
      epoch: this.epoch(),
      submitted: null,
      settled: null,
      outcome: null,
      resultId: null,
      resultBytes: 0,
      resultBackingId: null,
      resultBackingBytes: 0,
      distinctBackingBytes: 0,
      retired: false,
    }
    if (!(this.rows instanceof FixedJournal)) this.rows.push(row)
    this.live.add(row)
    row.submitted = this.sampler.begin()
    this.maxLive = Math.max(this.maxLive, this.live.size)
    return row
  }

  /**
   * Completes a row at settlement.
   * @param {object|null} row the row `begin` returned
   * @param {Buffer|undefined} value the resolved result, or undefined for a rejection
   * @returns {void}
   */
  end(row, value) {
    if (!row || !this.live.has(row)) return
    row.settled = this.sampler.end()
    this.live.delete(row)
    row.outcome = Buffer.isBuffer(value) ? 'resolved' : 'rejected'
    if (row.outcome === 'resolved') {
      row.resultId = this.idOf(value)
      row.resultBytes = value.length
      row.resultBackingId = this.idOf(value.buffer)
      row.resultBackingBytes = value.buffer.byteLength
    }
    const distinct = new Map([[row.inputBackingId, row.inputBackingBytes]])
    if (row.resultBackingId !== null) distinct.set(row.resultBackingId, row.resultBackingBytes)
    row.distinctBackingBytes = [...distinct.values()].reduce((sum, bytes) => sum + bytes, 0)
    row.retired = this.epoch() !== row.epoch
    if (this.rows instanceof FixedJournal) this.rows.push(row)
  }

  /** @returns {void} restores every wrapped module */
  restoreAll() {
    while (this.restores.length) this.restores.pop()()
  }

  /** @returns {object} reserved native-row capacity and cursor, with no product references */
  journalState() {
    const state = this.rows.journalState()
    return { ...state, overflow: state.overflow || this.overflow }
  }

  /**
   * @returns {{rows: object[], maxLive: number, live: number, overflow: boolean}} bounded facts
   */
  summary() {
    return {
      rows:
        this.rows instanceof FixedJournal
          ? [...this.rows.values(), ...this.live].sort((a, b) => a.id - b.id)
          : this.rows.map((row) => ({ ...row })),
      maxLive: this.maxLive,
      live: this.live.size,
      overflow: this.overflow,
    }
  }
}

/**
 * Witnesses what a public disconnect's resolution continuation does before it first yields. Call
 * `resolved` first thing in that continuation and `call` for each action it then takes: a
 * microtask queued at resolution records `yield`, so an action taken after any await is recorded
 * after `yield`, and one taken synchronously before it.
 */
class BoundaryWitness {
  /**
   * @param {() => number} now monotonic milliseconds
   * @returns {BoundaryWitness} a witness with no events
   */
  constructor(now) {
    this.now = now
    this.events = []
  }

  /** @returns {void} records the resolution and queues the record of the first yield */
  resolved() {
    this.call('disconnect')
    queueMicrotask(() => this.call('yield'))
  }

  /** @param {string} action the call the continuation makes now @returns {void} */
  call(action) {
    this.events.push({ action, at: this.now() })
  }

  /** @returns {Array<{action: string, at: number}>} a copy of the events, in order */
  summary() {
    return this.events.map((event) => ({ ...event }))
  }
}

module.exports = {
  MEMORY_FLAGS,
  SETTLE,
  CONTROL_SETTLE_ROUNDS,
  collectionsSettled,
  dataBytes,
  peakReadingDecreases,
  PEAK_SOURCE,
  SAMPLE_MS,
  GC_PAUSE_MS,
  WINDOW_CAPACITY,
  BoundaryWitness,
  ProcessMemory,
  PendingSampler,
  NativeJournal,
  readProcStatus,
  selectPeakSource,
  memoryPressure,
  hostContext,
}
