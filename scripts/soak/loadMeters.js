'use strict'

/**
 * Measurements the load run takes beside its oracle: per-key counts in one-second buckets (the
 * per-leader flow), the clients' own instrumentation events (Metadata requests, fetch rounds,
 * rebalances) and the event loop's worst lag in each second. Each holds one number per second of
 * the run, or a fixed-size histogram, never one entry per record.
 */

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

module.exports = { ClientMeter, FlowMeter, LagProbe }
