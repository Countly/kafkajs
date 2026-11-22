'use strict'

/**
 * Measurements the load run takes beside its oracle: per-key counts in one-second buckets (the
 * per-leader flow), the clients' own instrumentation events (Metadata requests, fetch rounds,
 * rebalances) and the event loop's worst lag in each second. Each holds one number per second of
 * the run, or a fixed-size histogram, never one entry per record.
 */

const os = require('node:os')
const { LatencyHistogram } = require('./loadProfile')

/**
 * Counts per series and key in whole seconds since `begin`. Spans are in ms from that origin; a
 * span covers only the seconds wholly inside it.
 */
class FlowMeter {
  constructor() {
    this.origin = null
    /** @type {Map<string, Map<string, number[]>>} */
    this.series = new Map()
  }

  /** @param {number} origin the clock time second 0 starts at @returns {void} */
  begin(origin) {
    this.origin = origin
  }

  /**
   * @param {string} series @param {string} key @param {number} count @param {number} now clock time
   * @returns {void} nothing is counted before `begin`
   */
  add(series, key, count, now) {
    if (this.origin === null || now < this.origin) return
    if (!this.series.has(series)) this.series.set(series, new Map())
    const keys = this.series.get(series)
    if (!keys.has(key)) keys.set(key, [])
    const buckets = keys.get(key)
    const s = Math.floor((now - this.origin) / 1000)
    buckets[s] = (buckets[s] ?? 0) + count
  }

  /** @param {number} fromMs @param {number} toMs @returns {number} whole seconds in the span */
  wholeSeconds(fromMs, toMs) {
    return Math.max(0, Math.floor(toMs / 1000) - Math.ceil(fromMs / 1000))
  }

  /** @param {string} series @param {string} key @param {number} fromMs @param {number} toMs @returns {number} */
  sum(series, key, fromMs, toMs) {
    let total = 0
    this.#each(series, key, fromMs, toMs, (n) => {
      total += n
    })
    return total
  }

  /**
   * @param {string} series @param {string} key @param {number} fromMs @param {number} toMs
   * @returns {number} the longest run of seconds in the span with nothing counted
   */
  longestStall(series, key, fromMs, toMs) {
    let longest = 0
    let run = 0
    this.#each(series, key, fromMs, toMs, (n) => {
      run = n > 0 ? 0 : run + 1
      longest = Math.max(longest, run)
    })
    return longest
  }

  #each(series, key, fromMs, toMs, fn) {
    const buckets = this.series.get(series)?.get(key) ?? []
    for (let s = Math.ceil(fromMs / 1000); s < Math.floor(toMs / 1000); s++) fn(buckets[s] ?? 0)
  }
}

/**
 * The event loop's worst lag in each second: a timer every `intervalMs` measures how late it ran.
 */
class LagProbe {
  /**
   * @param {object} [options]
   * @param {{ now: () => number }} [options.clock]
   * @param {number} [options.intervalMs=20]
   * @param {{ setInterval: Function, clearInterval: Function }} [options.timers]
   */
  constructor({ clock = performance, intervalMs = 20, timers = globalThis } = {}) {
    Object.assign(this, { clock, intervalMs, timers })
    this.maxBySecond = []
    this.timer = null
  }

  /** @param {number} origin the clock time second 0 starts at @returns {void} */
  start(origin) {
    let expected = this.clock.now() + this.intervalMs
    this.timer = this.timers.setInterval(() => {
      const now = this.clock.now()
      const lag = Math.max(0, now - expected)
      expected = now + this.intervalMs
      const s = Math.floor((now - origin) / 1000)
      if (s >= 0 && !(this.maxBySecond[s] >= lag)) this.maxBySecond[s] = lag
    }, this.intervalMs)
    this.timer.unref?.()
  }

  /** @returns {void} */
  stop() {
    if (this.timer) this.timers.clearInterval(this.timer)
    this.timer = null
  }

  /**
   * @param {number} fromMs @param {number} toMs from the origin, inclusive
   * @returns {number|null} the worst lag in the seconds the span touches; null with no sample
   */
  maxBetween(fromMs, toMs) {
    let max = null
    for (let s = Math.max(0, Math.floor(fromMs / 1000)); s <= Math.floor(toMs / 1000); s++) {
      const lag = this.maxBySecond[s]
      if (lag !== undefined && (max === null || lag > max)) max = lag
    }
    return max
  }
}

/** Rebalances the report keeps; later ones are counted only. */
const MAX_REBALANCES = 500

/**
 * What the clients under test report through their instrumentation events: Metadata requests per
 * minute and their response sizes (`REQUEST`), fetch round durations (`FETCH`), each rebalance
 * (`GROUP_JOIN`) with the worst event-loop lag around it, and each member's time from `subscribe`
 * to its first assignment.
 */
class ClientMeter {
  /** @param {{ clock: { now: () => number }, lag: { maxBetween: Function } }} options */
  constructor({ clock, lag }) {
    Object.assign(this, { clock, lag })
    this.origin = clock.now()
    this.metadata = { requests: 0, perMinute: [], totalBytes: 0, maxBytes: 0 }
    this.fetchRoundMs = new LatencyHistogram()
    this.rebalances = []
    this.rebalanceCount = 0
    this.subscribedAt = new Map()
    this.firstAssignmentMs = {}
  }

  /**
   * Listens to one client. A consumer is named by its member; its fetch and join events count too.
   * @param {{ on: Function, events: object }} client
   * @param {string|null} [member]
   * @returns {void}
   */
  observe(client, member = null) {
    client.on(client.events.REQUEST, ({ payload }) => {
      if (payload.apiName !== 'Metadata') return
      const minute = Math.floor(this.#at() / 60000)
      const m = this.metadata
      m.requests++
      m.perMinute[minute] = (m.perMinute[minute] ?? 0) + 1
      m.totalBytes += payload.size
      m.maxBytes = Math.max(m.maxBytes, payload.size)
    })
    if (member === null) return
    client.on(client.events.FETCH, ({ payload }) => this.fetchRoundMs.record(payload.duration))
    client.on(client.events.GROUP_JOIN, ({ payload }) => {
      const atMs = this.#at()
      if (!(member in this.firstAssignmentMs) && this.subscribedAt.has(member))
        this.firstAssignmentMs[member] = atMs - this.subscribedAt.get(member)
      this.rebalanceCount++
      if (this.rebalances.length >= MAX_REBALANCES) return
      const partitions = Object.values(payload.memberAssignment).reduce((n, p) => n + p.length, 0)
      this.rebalances.push({ member, atMs, joinMs: payload.duration, partitions })
    })
  }

  /** @param {string} member about to call `subscribe` @returns {void} */
  subscribing(member) {
    this.subscribedAt.set(member, this.#at())
  }

  /** @returns {object} the measurements, times in ms from the meter's creation */
  toJSON() {
    const { requests, perMinute, totalBytes, maxBytes } = this.metadata
    const minutes = Array.from(perMinute, (n) => n ?? 0)
    return {
      metadata: {
        requests,
        perMinute: minutes,
        maxPerMinute: Math.max(0, ...minutes),
        meanBytes: requests ? Math.round(totalBytes / requests) : null,
        maxBytes,
      },
      fetchRoundMs: this.fetchRoundMs.toJSON(),
      firstAssignmentMs: this.firstAssignmentMs,
      rebalanceCount: this.rebalanceCount,
      rebalances: this.rebalances.map((r) => ({
        ...r,
        loopDelayMaxMs: this.lag.maxBetween(r.atMs - r.joinMs, r.atMs + 5000),
      })),
    }
  }

  #at() {
    return this.clock.now() - this.origin
  }
}

/**
 * Reads the load process's CPU and the machine's, cumulative, so two reads give the share of a
 * span. The machine's share counts every core, so the brokers sharing the machine count too.
 */
class HostMeter {
  /**
   * @param {object} [sources] the outside world; a unit spec replaces it
   * @param {() => { user: number, system: number }} [sources.cpuUsage] this process, µs
   * @param {() => { user: number, nice: number, sys: number, irq: number, idle: number }[]} [sources.cpuTimes]
   *   per core, ms
   * @param {() => number[]} [sources.loadavg] 1, 5 and 15 minute load averages
   * @param {number} [sources.cpus] cores the process may use
   */
  constructor({
    cpuUsage = () => process.cpuUsage(),
    cpuTimes = () => os.cpus().map((c) => c.times),
    loadavg = () => os.loadavg(),
    cpus = os.availableParallelism(),
  } = {}) {
    Object.assign(this, { cpuUsage, cpuTimes, loadavg, cpus })
  }

  /** @returns {{ processUs: number, machineBusyMs: number, machineTotalMs: number, load1: number }} */
  read() {
    const { user, system } = this.cpuUsage()
    let busy = 0
    let total = 0
    for (const t of this.cpuTimes()) {
      const all = t.user + t.nice + t.sys + t.irq + t.idle
      total += all
      busy += all - t.idle
    }
    return {
      processUs: user + system,
      machineBusyMs: busy,
      machineTotalMs: total,
      load1: this.loadavg()[0],
    }
  }

  /**
   * @param {object} from a `read` @param {object} to a later `read` @param {number} elapsedMs
   * @returns {{ coreShare: number|null, machineBusyShare: number|null }} this process's CPU as a
   *   share of one core, and the machine's busy share of all its cores; null over an empty span
   */
  static between(from, to, elapsedMs) {
    const total = to.machineTotalMs - from.machineTotalMs
    return {
      coreShare:
        elapsedMs > 0 ? round3((to.processUs - from.processUs) / (elapsedMs * 1000)) : null,
      machineBusyShare: total > 0 ? round3((to.machineBusyMs - from.machineBusyMs) / total) : null,
    }
  }
}

/** @param {number} n @returns {number} */
const round3 = (n) => Math.round(n * 1000) / 1000

/**
 * The broker containers' CPU while the window is open: `docker stats --no-stream` every
 * `intervalMs`, one call at a time. Keeps a count, sum and maximum per container, never the
 * samples. A failed call is counted and the probe goes on.
 */
class BrokerCpuProbe {
  /**
   * @param {object} options
   * @param {() => Promise<string>} options.stats the output of `docker stats --no-stream --format
   *   '{{.Name}}\t{{.CPUPerc}}'`
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} options.sleep
   * @param {number} [options.intervalMs=10000]
   */
  constructor({ stats, sleep, intervalMs = 10000 }) {
    Object.assign(this, { stats, sleep, intervalMs })
    /** @type {Map<string, { samples: number, sum: number, max: number }>} */
    this.containers = new Map()
    this.failures = 0
    this.lastFailure = null
    this.done = null
  }

  /**
   * Parses `docker stats` lines of `<name>\t<percent>%`, where the percent is of one core.
   * @param {string} text
   * @returns {{ name: string, percent: number }[]} lines that are not one are left out
   */
  static parse(text) {
    const rows = []
    for (const line of text.split('\n')) {
      const m = /^(\S+)\t(\d+(?:\.\d+)?)%$/.exec(line.trim())
      if (m) rows.push({ name: m[1], percent: Number(m[2]) })
    }
    return rows
  }

  /** @param {AbortSignal} signal the window; the probe stops when it aborts @returns {void} */
  start(signal) {
    this.done = this.#loop(signal)
  }

  /** @returns {Promise<void>} once the call under way, if any, has returned */
  async stop() {
    await this.done
  }

  async #loop(signal) {
    while (!signal.aborted) {
      try {
        for (const { name, percent } of BrokerCpuProbe.parse(await this.stats())) {
          const c = this.containers.get(name) ?? { samples: 0, sum: 0, max: 0 }
          c.samples++
          c.sum += percent
          c.max = Math.max(c.max, percent)
          this.containers.set(name, c)
        }
      } catch (error) {
        this.failures++
        this.lastFailure = String(error?.message ?? error).slice(0, 200)
      }
      await this.sleep(this.intervalMs, signal)
    }
  }

  /**
   * @returns {{ containers: Record<string, { samples: number, meanPercent: number, maxPercent: number }>,
   *   failures: number, lastFailure: string|null }} percents of one core
   */
  toJSON() {
    const containers = {}
    for (const [name, { samples, sum, max }] of [...this.containers].sort(([a], [b]) =>
      a < b ? -1 : 1
    )) {
      containers[name] = { samples, meanPercent: Math.round(sum / samples), maxPercent: max }
    }
    return { containers, failures: this.failures, lastFailure: this.lastFailure }
  }
}

/** The counts a `ThroughputSeries` keeps per interval. */
const SERIES_COUNTS = Object.freeze(['sent', 'acked', 'failed', 'consumed'])

/**
 * Records per fixed interval over a run's planned span (offer window, join and drain): sent,
 * acknowledged, failed and consumed, and per interval the first and last acknowledgement and the
 * longest stretch inside it with none. Kept for `all` and for each named group (a partition
 * leader). Its memory is fixed when a group first counts: one typed-array cell per interval and
 * field, never one entry per record; a record past the span is only counted in `beyond`. Counting
 * is an array increment per group, plus two comparisons for an acknowledgement.
 */
class ThroughputSeries {
  /** @param {{ intervalMs?: number, spanMs: number }} options the span is rounded up to whole intervals */
  constructor({ intervalMs = 5000, spanMs }) {
    this.intervalMs = intervalMs
    this.intervals = Math.max(1, Math.ceil(spanMs / intervalMs))
    this.origin = null
    this.beyond = 0
    /** @type {Map<string, object>} */
    this.groups = new Map()
  }

  /**
   * @param {{ seriesIntervalMs: number, durationSeconds: number, joinSeconds: number, drainSeconds: number }} profile
   * @returns {ThroughputSeries} sized for the profile's window, join and drain
   */
  static forProfile({ seriesIntervalMs, durationSeconds, joinSeconds, drainSeconds }) {
    const spanMs = (durationSeconds + joinSeconds + drainSeconds) * 1000
    return new ThroughputSeries({ intervalMs: seriesIntervalMs, spanMs })
  }

  /** @param {number} origin the clock time the first interval starts at @returns {void} */
  begin(origin) {
    this.origin = origin
  }

  /**
   * Counts `n` records in `all` and, when it is another name, in `group`. Nothing is counted
   * before `begin`.
   * @param {'sent'|'acked'|'failed'|'consumed'} kind
   * @param {number} n @param {number} now clock time @param {string} [group='all']
   * @returns {void}
   */
  count(kind, n, now, group = 'all') {
    if (this.origin === null) return
    const at = now - this.origin
    const i = Math.floor(at / this.intervalMs)
    if (i < 0 || i >= this.intervals) {
      this.beyond += n
      return
    }
    this.#record(this.#group('all'), kind, n, at, i)
    if (group !== 'all') this.#record(this.#group(group), kind, n, at, i)
  }

  /**
   * Adds another process's series, taken from the same profile, after its window and join. Counts
   * add; first and last acknowledgement times widen. An interval's longest stretch without an
   * acknowledgement becomes the shortest any process saw: an upper bound on the merged one, since
   * one process's acknowledgements may fall inside another's stretch.
   * @param {object} other a `snapshot`
   * @returns {void}
   * @throws {Error} when the intervals differ
   */
  merge(other) {
    if (other.intervalMs !== this.intervalMs || other.intervals !== this.intervals)
      throw new Error(
        `a series of ${other.intervals} intervals of ${other.intervalMs} ms cannot merge into ${this.intervals} of ${this.intervalMs} ms`
      )
    this.beyond += other.beyond
    for (const [name, cells] of Object.entries(other.groups)) {
      const g = this.#group(name)
      g.gaps ??= this.#gaps(g)
      for (let i = 0; i < this.intervals; i++) {
        for (const kind of SERIES_COUNTS) g[kind][i] += cells[kind][i]
        if (cells.firstAckMs[i] !== null) {
          if (!(g.firstAt[i] <= cells.firstAckMs[i])) g.firstAt[i] = cells.firstAckMs[i]
          if (!(g.lastAt[i] >= cells.lastAckMs[i])) g.lastAt[i] = cells.lastAckMs[i]
        }
        g.gaps[i] = Math.min(g.gaps[i], cells.maxGapMs[i])
      }
    }
  }

  /**
   * @returns {{ intervalMs: number, intervals: number, beyond: number, groups: Record<string, object> }}
   *   per group arrays of one cell per interval: the counts, `firstAckMs` and `lastAckMs` (ms from
   *   the origin, null with none) and `maxGapMs` (an interval with no acknowledgement has its whole
   *   length); safe to send to another process
   */
  snapshot() {
    const groups = {}
    for (const [name, g] of this.groups) {
      const cells = {}
      for (const kind of SERIES_COUNTS) cells[kind] = Array.from(g[kind])
      const orNull = (t) => (Number.isNaN(t) ? null : t)
      cells.firstAckMs = Array.from(g.firstAt, orNull)
      cells.lastAckMs = Array.from(g.lastAt, orNull)
      cells.maxGapMs = Array.from(g.gaps ?? this.#gaps(g))
      groups[name] = cells
    }
    const { intervalMs, intervals, beyond } = this
    return { intervalMs, intervals, beyond, groups }
  }

  #group(name) {
    let g = this.groups.get(name)
    if (!g) {
      const cells = () => new Float64Array(this.intervals)
      g = { firstAt: cells().fill(NaN), lastAt: cells().fill(NaN), maxGap: cells(), lastAckMs: 0 }
      for (const kind of SERIES_COUNTS) g[kind] = cells()
      g.gaps = null
      this.groups.set(name, g)
    }
    return g
  }

  #record(g, kind, n, at, i) {
    g[kind][i] += n
    if (kind !== 'acked') return
    const gap = at - Math.max(g.lastAckMs, i * this.intervalMs)
    if (gap > g.maxGap[i]) g.maxGap[i] = gap
    if (Number.isNaN(g.firstAt[i])) g.firstAt[i] = at
    g.lastAt[i] = at
    g.lastAckMs = at
  }

  /** Each interval's longest stretch without an acknowledgement, its tail after the last one included. */
  #gaps(g) {
    return Float64Array.from(g.maxGap, (inner, i) => {
      const end = (i + 1) * this.intervalMs
      if (Number.isNaN(g.lastAt[i])) return this.intervalMs
      return Math.max(inner, end - g.lastAt[i])
    })
  }
}

module.exports = { BrokerCpuProbe, ClientMeter, FlowMeter, HostMeter, LagProbe, ThroughputSeries }
