'use strict'

const v8 = require('node:v8')
const { readProcStatus } = require('../../testHelpers/processMemory')
const {
  TelemetryWorker,
  Registry,
  declaredJournalBytes,
  runCatalogue,
  within,
} = require('./telemetryChurn')

const PHASES = Object.freeze([
  'allocation',
  'loaded',
  'control',
  'setup',
  'M',
  'await-go',
  'begin',
  'active',
  'reopen',
  'retire',
  'subscription',
  'cleanup',
  'closed',
  'final',
  'failure',
])
const POLL_MS = 20
const GRID_MS = 1000
const ROW_MARGIN = 8
const HEADER = 18

/** Fixed scalar-only observations of the original worker; never an acceptance input. */
class SharePeak {
  /**
   * @param {object} options diagnostic sources; injectable for broker-free proofs
   * @param {number} options.reserveBytes original declared reserve, without any adjustment
   * @param {Function} [options.now] monotonic milliseconds
   * @param {Function} [options.proc] raw kernel counters
   * @param {Function} [options.usage] process memory counters
   * @param {Function} [options.spaces] V8 per-space counters
   * @param {Function} [options.pending] pending native compression count
   * @param {number} [options.durationMs] unchanged total case budget
   * @param {number} [options.capacity] fixed row bound; overflow is fatal
   */
  constructor({
    reserveBytes,
    now = () => performance.now(),
    proc = readProcStatus,
    usage = () => process.memoryUsage(),
    spaces = () => v8.getHeapSpaceStatistics(),
    pending = () => 0,
    durationMs = 540000,
    capacity = Math.ceil(durationMs / GRID_MS) * ROW_MARGIN,
  }) {
    Object.assign(this, { reserveBytes, now, proc, usage, spaces, pending, capacity })
    this.names = spaces().map((space) => space.space_name)
    this.stride = HEADER + this.names.length * 3
    this.storage = new Float64Array(capacity * this.stride)
    new Uint8Array(this.storage.buffer).fill(0xa5)
    this.length = 0
    this.errors = new Set()
    this.currentPhase = 0
    this.tick = -1
    this.lastAt = NaN
    this.lastHwm = NaN
    this.lastPhase = 0
    this.origin = now()
    this.nextGrid = this.origin
    this.phaseAt = null
    this.warmupMs = 60000
    this.handle = null
  }

  /** @returns {void} begins fixed polling; no GC, disk archive writes or IPC are added */
  start() {
    this.poll()
    this.handle = setInterval(() => {
      try {
        this.poll()
      } catch (error) {
        this.errors.add(error.message)
        clearInterval(this.handle)
        this.handle = null
      }
    }, POLL_MS)
  }

  /**
   * @param {string} phase fixed operational phase @param {number} [tick] original parent tick
   * @returns {void} records the boundary without waiting or collecting
   */
  phase(phase, tick = -1) {
    const index = PHASES.indexOf(phase)
    if (index < 0) throw new Error('unknown share peak phase')
    this.currentPhase = index
    this.tick = tick
    this.poll(true)
  }

  /**
   * @param {number} at original Soak phase origin @param {number} warmupMs original warm-up
   * @returns {void} aligns all later facts to the existing phase clock
   */
  window(at, warmupMs) {
    this.phaseAt = at
    this.warmupMs = warmupMs
  }

  /**
   * Reads the kernel on the 20 ms poll and captures all scalars on the 1 s grid, an observed
   * VmHWM change or a phase boundary. The interval since the previous poll brackets discovery,
   * not the exact kernel peak instant; multiple steps during that interval cannot be separated.
   * @param {boolean} [boundary=false] record even without a grid or counter change
   * @returns {void} @throws {Error} on fixed storage overflow
   */
  poll(boundary = false) {
    const at = this.now()
    const kernel = this.proc()
    const hwm = kernel?.vmhwm ?? NaN
    if (!kernel) this.errors.add('proc status unavailable')
    const step = Number.isFinite(this.lastHwm) && hwm > this.lastHwm
    const changed = !Number.isFinite(hwm) || hwm !== this.lastHwm
    const gridAt = at >= this.nextGrid ? this.nextGrid - this.origin : NaN
    if (Number.isFinite(gridAt)) {
      if (at >= this.nextGrid + GRID_MS) this.errors.add('fixed grid missed')
      this.nextGrid += (Math.floor((at - this.nextGrid) / GRID_MS) + 1) * GRID_MS
    }
    if (boundary || changed || Number.isFinite(gridAt)) {
      if (this.length === this.capacity) {
        this.errors.add('share peak overflow')
        throw new Error('share peak overflow')
      }
      const usage = this.usage()
      const spaces = this.spaces()
      const offset = this.length * this.stride
      const values = [
        at,
        this.lastAt,
        this.currentPhase,
        this.tick,
        gridAt,
        Number(step),
        kernel?.vmrss ?? NaN,
        hwm,
        usage.rss,
        usage.heapTotal,
        usage.heapUsed,
        usage.external,
        usage.arrayBuffers,
        this.pending(),
        this.phaseAt ?? NaN,
        this.warmupMs,
        NaN,
        this.lastPhase,
      ]
      this.storage.set(values, offset)
      if (spaces.length !== this.names.length) this.errors.add('heap space catalogue changed')
      this.names.forEach((name, index) => {
        const space = spaces.find((space) => space.space_name === name)
        const sizes = [space?.space_size, space?.space_used_size, space?.physical_space_size]
        if (!sizes.every(Number.isFinite)) this.errors.add('heap space source unavailable')
        this.storage.set(
          sizes.map((value) => value ?? NaN),
          offset + HEADER + index * 3
        )
      })
      this.storage[offset + 16] = this.now()
      this.length++
    }
    this.lastAt = at
    this.lastHwm = hwm
    this.lastPhase = this.currentPhase
  }

  /** @returns {void} stops polling before any archive is materialised */
  stop() {
    clearInterval(this.handle)
    this.handle = null
  }

  /**
   * @returns {object} raw replayable facts, including VmHWM decreases and the first maximum.
   *   Decreases do not invalidate the diagnostic; RSS categories overlap and are never subtracted.
   */
  report() {
    const readings = Array.from({ length: this.length }, (_, index) => {
      const row = this.storage.subarray(index * this.stride, (index + 1) * this.stride)
      const phaseMs = Number.isFinite(row[14]) ? row[0] - row[14] : null
      return {
        index,
        at: row[0],
        previousPollAt: Number.isFinite(row[1]) ? row[1] : null,
        phase: PHASES[row[2]],
        previousPollPhase: PHASES[row[17]],
        completedAt: row[16],
        tick: row[3] < 0 ? null : row[3],
        phaseMs,
        window:
          phaseMs === null
            ? 'setup'
            : ['cleanup', 'closed', 'final', 'failure'].includes(PHASES[row[2]])
              ? 'cleanup'
              : phaseMs < row[15]
                ? 'warmup'
                : 'measurement',
        gridAt: Number.isFinite(row[4]) ? row[4] : null,
        hwmStep: Boolean(row[5]),
        VmRSS: Number.isFinite(row[6]) ? row[6] : null,
        VmHWM: Number.isFinite(row[7]) ? row[7] : null,
        memoryUsage: {
          rss: row[8],
          heapTotal: row[9],
          heapUsed: row[10],
          external: row[11],
          arrayBuffers: row[12],
        },
        pendingNativeJobs: row[13],
        reserveBytes: this.reserveBytes,
        heapSpaces: this.names.map((name, i) => ({
          name,
          committedBytes: row[HEADER + i * 3],
          usedBytes: row[HEADER + i * 3 + 1],
          physicalBytes: row[HEADER + i * 3 + 2],
        })),
      }
    })
    const hwmDecreases = []
    for (let i = 1; i < readings.length; i++) {
      const before = readings[i - 1].VmHWM
      const { index, phase, VmHWM: after, VmRSS } = readings[i]
      if (before !== null && after !== null && after < before)
        hwmDecreases.push({ index, phase, before, after, VmRSS })
    }
    const peak = readings.reduce(
      (best, row) => (row.VmHWM !== null && (!best || row.VmHWM > best.VmHWM) ? row : best),
      null
    )
    return {
      version: 1,
      scope: 'shadow',
      acceptance: false,
      acceptanceEligible: false,
      attributionComplete: false,
      complete: this.errors.size === 0 && readings.length > 0,
      pollMs: POLL_MS,
      gridMs: GRID_MS,
      origin: this.origin,
      reserveBytes: this.reserveBytes,
      diagnosticReserveBytes: this.storage.byteLength,
      capacity: this.capacity,
      expectedPeakPhase:
        'Hypothesis: measured reopen, where the previous segment seal overlaps the next public lifecycle. Setup, native work, active subscription/retirement and terminal cleanup remain alternative phases; peak.phase tests this without excusing any bytes.',
      errors: [...this.errors],
      hwmDecreases,
      peak,
      readings,
      interpretation:
        'peak is the first observation of the largest VmHWM. Inspect phase, tick, phaseMs and previousPollAt. Reopen/retire/subscription delimit churn; cleanup includes terminal decode and disconnect. Committed V8 spaces, heap, external and arrayBuffers overlap and do not partition RSS. Allocator/native/code residency remains unattributed. Diagnostic storage is additional observer overhead, identical in both arms; nothing is subtracted.',
    }
  }
}

/** Keeps the harness descriptor population when the disabled public API refuses registration. */
class DisabledShareRegistry extends Registry {
  /** @param {object} descriptor same harness fact; no telemetry registration @returns {void} */
  add(descriptor) {
    this.registered.push(descriptor)
    this.bytes += this.charge(descriptor)
  }

  /** @param {object} descriptor same harness fact; no telemetry unregistration @returns {void} */
  remove(descriptor) {
    this.registered = this.registered.filter((entry) => entry !== descriptor)
    this.bytes -= this.charge(descriptor)
  }
}

/** Same share corpus and public lifecycle; telemetry-only requests and proof waits are absent. */
class DisabledShareWorker extends TelemetryWorker {
  /** @returns {Promise<null>} prepares the same descriptors and foreground with telemetry disabled */
  async prepare() {
    this.registry.fill()
    const first = this.segments.current()
    await this.target(() => this.client.connect())
    if (this.foreground)
      this.claims.foreground = await this.target(() =>
        runCatalogue(this.client, this.foreground.manifest, this.foreground.prefix, () =>
          this.clock.now()
        )
      )
    await this.target(() => this.segments.close(this.client, this.closeOptions()))
    this.segments.advance()
    this.claims.setup = { segment: first.id, telemetry: false }
    await this.seal(first, false)
    return null
  }

  /** @returns {Promise<void>} starts the unchanged ordinary workload */
  async begin() {
    await this.target(() => this.client.connect())
    await this.target(() => this.ordinary.start())
  }

  /** @param {object} command original parent tick @returns {Promise<void>} same pause/close/open/resume */
  async reopen(command) {
    await this.drained()
    const pause = await this.target(() => this.ordinary.pause())
    const observedAt = this.clock.now()
    const cycle = this.target(() => this.segments.cycle(this.client, this.closeOptions()))
    await within(cycle.closed, 8000, 'The shareConsumer close')
    const closedAt = this.clock.now()
    this.drain(cycle.from)
    await within(cycle.reopened, 10000, 'The shareConsumer reconnect')
    await this.target(() => this.ordinary.resume())
    this.claims.reopens.push({
      tick: command.tick,
      from: cycle.from.id,
      to: cycle.to.id,
      observedAt,
      closedAt,
      pause,
    })
  }

  /** @param {object} command original tick @returns {Promise<void>} keeps foreground core work */
  async retire(command) {
    await this.target(() => this.ordinary.core(this.coreN++))
    this.claims.retirements.push({ tick: command.tick, telemetry: false })
  }

  /** @param {object} command original tick @returns {Promise<void>} keeps descriptor replacement */
  async confirm(command) {
    if (this.confirmation?.tick !== command.tick)
      throw new Error('missing disabled confirmation fence')
    this.confirmation = null
    const replacement = this.registry.replace(command.tick)
    this.claims.transitions.push({ tick: command.tick, replacement, telemetry: false })
  }
}

/**
 * Installs explicit diagnostic seams in a child owner; normal and non-share owners stay untouched.
 * @param {object} owner original TelemetryWorkerProcess @param {string} arm enabled or disabled
 * @returns {void} @throws {Error} for an unknown arm
 */
const installSharePeak = (owner, arm) => {
  if (!['enabled', 'disabled'].includes(arm)) throw new Error('unknown share peak arm')
  if (owner.kind !== 'shareConsumer') return
  owner.soak.sharePeak = ({ sampler, limits }) =>
    new SharePeak({
      reserveBytes: declaredJournalBytes('shareConsumer', limits),
      durationMs: limits.caseMs,
      pending: () => sampler.pending,
      now: () => owner.clock.now(),
    })
  if (arm === 'disabled') {
    owner.soak.SharePeakWorker = DisabledShareWorker
    owner.soak.SharePeakRegistry = DisabledShareRegistry
    owner.create = (kafka, config) =>
      kafka.shareConsumer({ enableMetricsPush: false, groupId: config.group })
  }
}

module.exports = { SharePeak, DisabledShareWorker, DisabledShareRegistry, installSharePeak }
