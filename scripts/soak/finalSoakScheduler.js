'use strict'
/**
 * Final soak timing: the run clock, the plan expanded from the reviewed matrix schedule, the
 * scheduler that runs it without ever moving a step, and the gate that keeps at most two processes
 * collecting at once.
 */
const { SHAPES } = require('./finalSoakSchema')

/** @param {object} setting a matrix `{ value, unit, source }` setting @returns {number} */
const valueOf = (setting) => setting.value

/** The timers the harness armed for its own waits and bounds, until each fires or is cleared. */
const harnessTimers = new Set()

/** @param {Function} fn @param {number} realMs @param {unknown[]} args @returns {NodeJS.Timeout} */
const arm = (fn, realMs, args) => {
  const timer = setTimeout(() => {
    harnessTimers.delete(timer)
    fn(...args)
  }, realMs)
  harnessTimers.add(timer)
  return timer
}

/**
 * Arms a harness WAIT: how long the harness sleeps on its own clock (a schedule step, a poll
 * interval), shortened when a rehearsal runs that clock faster. A resource sample subtracts every
 * harness timer, so it never counts one the harness made for the measurement or the teardown.
 * @param {(...args: unknown[]) => void} fn @param {number} ms harness-clock ms @param {...unknown} args
 * @returns {NodeJS.Timeout} the timer, cleared with `clearHarnessTimeout`
 */
const harnessWait = (fn, ms, ...args) => arm(fn, (Number(ms) || 0) / MonotonicClock.rate, args)

/**
 * Arms a harness BOUND: a limit that catches a hang, counted in real time whatever the harness
 * clock's rate, so a faster clock never turns it into a race against the CPU or the disk.
 * @param {(...args: unknown[]) => void} fn @param {number} ms real ms @param {...unknown} args
 * @returns {NodeJS.Timeout} the timer, cleared with `clearHarnessTimeout`
 */
const harnessBound = (fn, ms, ...args) => arm(fn, Number(ms) || 0, args)

/**
 * The harness-clock span of a real-time bound, for a bound kept as a run-clock deadline
 * (`clock.now() + realSpan(ms)`): equal to `ms` at the real rate.
 * @param {number} ms real ms @returns {number} harness-clock ms
 */
const realSpan = (ms) => ms * MonotonicClock.rate

/** @param {NodeJS.Timeout|null} timer a `harnessWait` or `harnessBound` timer @returns {void} */
const clearHarnessTimeout = (timer) => {
  harnessTimers.delete(timer)
  clearTimeout(timer)
}

/** @returns {number} the harness timers that keep the event loop alive now */
const liveHarnessTimers = () => [...harnessTimers].filter((t) => t.hasRef()).length

/**
 * The host's monotonic clock, relative to an origin captured once per run. A rehearsal may run it
 * `rate` times faster than real time from the shared real instant `anchor` (monotonic ns); the
 * harness's waits (`harnessWait`) follow it, while its bounds (`harnessBound`, `realSpan`) and the
 * clients' timers stay on real time.
 */
class MonotonicClock {
  /** How many times faster than real time the harness clock runs. */
  static rate = 1
  /** The real monotonic instant, in ns, the faster clock is counted from. */
  static anchor = 0n

  /** @param {number} [originMs] the run origin in monotonic ms; now by default */
  constructor(originMs = MonotonicClock.raw()) {
    this.originMs = originMs
  }

  /** @returns {number} monotonic milliseconds since boot */
  static raw() {
    const { rate, anchor } = MonotonicClock
    return Number(anchor + (process.hrtime.bigint() - anchor) * BigInt(rate)) / 1e6
  }

  /** @returns {number} milliseconds since the origin */
  now() {
    return MonotonicClock.raw() - this.originMs
  }

  /**
   * @param {number} at origin-relative ms
   * @param {AbortSignal} [signal] resolves early, clearing the timer, once aborted
   * @returns {Promise<void>}
   */
  sleepUntil(at, signal) {
    if (at <= this.now() || signal?.aborted) return Promise.resolve()
    return new Promise((resolve) => {
      let timer = null
      const done = () => {
        clearHarnessTimeout(timer)
        signal?.removeEventListener('abort', done)
        resolve()
      }
      // Timers fire on a millisecond loop clock, so one can wake a fraction early: check again.
      const check = () => {
        const wait = at - this.now()
        if (wait <= 0) done()
        else timer = harnessWait(check, Math.ceil(wait))
      }
      signal?.addEventListener('abort', done, { once: true })
      check()
    })
  }
}

/**
 * Expands the matrix schedule into timed steps, in start order. Each hour of the day holds the
 * matrix's one outage window on even hours; a rehearsal is the day's plan cut at its length (its
 * first hours, unchanged); the smoke holds as many windows as fit between its
 * first outage and its recovery deadline, so it exercises the two down times (two seconds, then
 * twelve) in the order the day does. Work steps (`lane: 'work'`) run one
 * at a time; timer steps (periodic and quiet samples, idle windows, seed and teardown marks) start
 * at their own time regardless of work. Each execution names the client kinds that must act in it.
 * @param {object} matrix the reviewed matrix
 * @param {'smoke'|'rehearsal'|'acceptance'} mode
 * @returns {{ mode: string, requestedMs: number, steps: object[] }}
 */
function planSchedule(matrix, mode) {
  const s = matrix.schedule
  const kinds = Object.keys(matrix.clientKinds)
  const where = (test) => kinds.filter((k) => test(matrix.clientKinds[k]))
  const recipients = (role) => matrix.topology.topics.find((t) => t.role === role).recipients
  const execution = (shape) => s.executions.find((e) => e.shape === shape)
  const membership = execution('membership-churn')
  const churn = where((k) => k.churn)
  const ordinary = where((k) => k.role === 'consumer' && k.churn)
  const statics = where((k) => k.static)
  const recreation = [...new Set([...recipients('recreate'), 'producer-idempotent', 'admin'])]
  const requestedMs = valueOf(matrix.acceptance.modes[mode].requestedMs)
  const period = valueOf(matrix.sampling.periodMs)
  const smoke = mode === 'smoke'
  const hours = smoke ? 1 : requestedMs / s.hourMs
  const outageWindowMs =
    (execution('broker-outage').recoveredByMinute - execution('broker-outage').minutes[0]) * 60000
  const smokeOutages = Math.floor((s.smoke.recoveredByMs - s.smoke.outageAtMs) / outageWindowMs)
  const steps = []
  const add = (step) => steps.push({ order: steps.length, ...step })
  const work = (id, shape, at, maxMs, participants, extra = {}) =>
    add({
      id,
      type: 'execution',
      lane: 'work',
      shape,
      at,
      deadline: at + maxMs,
      participants,
      ...extra,
    })

  add({
    id: 'traffic',
    type: 'execution',
    lane: 'work',
    shape: 'traffic',
    at: 0,
    deadline: requestedMs,
    participants: kinds,
  })
  for (let k = 0; k * period <= requestedMs; k++)
    add({
      id: `k${k}`,
      type: 'periodic',
      lane: 'timer',
      slot: k,
      at: k * period,
      deadline: k * period + valueOf(matrix.sampling.jitterMs),
    })
  let outage = 0
  for (let h = 0; h < hours; h++) {
    const H = h * s.hourMs
    const hh = String(h).padStart(2, '0')
    const at = (minutes) => minutes.map((m) => H + m * 60000)
    const times = smoke
      ? {
          membership: [s.smoke.membershipAtMs],
          regex: [s.smoke.regexAtMs],
          recreation: [s.smoke.recreationAtMs],
          outage: Array.from(
            { length: smokeOutages },
            (_, i) => s.smoke.outageAtMs + i * outageWindowMs
          ),
        }
      : {
          membership: at(membership.minutes),
          regex: at(execution('regex-churn').minutes),
          recreation: at(execution('topic-recreation').minutes),
          outage: h % 2 === 0 ? at(execution('broker-outage').minutes) : [],
        }
    times.membership.forEach((t, i) => {
      const id = `h${hh}m${i}`
      work(id, 'membership-churn', t, membership.maxMs, churn, {
        waves: membership.waves,
        restart: i === 0 ? statics : [],
      })
      for (let w = 0; w < membership.waves; w++) {
        const waveAt = t + w * membership.waveMaxMs
        add({
          id: `${id}w${w}`,
          type: 'execution',
          lane: 'within',
          parent: id,
          shape: 'pause-resume',
          at: waveAt,
          deadline: waveAt + membership.waveMaxMs,
          participants: ordinary,
        })
      }
    })
    times.regex.forEach((t, i) =>
      work(`h${hh}r${i}`, 'regex-churn', t, execution('regex-churn').maxMs, recipients('regex'))
    )
    times.recreation.forEach((t, i) =>
      work(`h${hh}t${i}`, 'topic-recreation', t, execution('topic-recreation').maxMs, recreation, {
        topic: (h * times.recreation.length + i) % 2,
      })
    )
    times.outage.forEach((t, i) => {
      work(`h${hh}o${i}`, 'broker-outage', t, outageWindowMs, kinds, {
        node: (outage % 3) + 1,
        stop: outage % 2 === 0 ? 'clean' : 'hard',
        downMs: outage % 2 === 0 ? 2000 : 12000,
      })
      outage++
    })
    const idle = smoke ? s.smoke.idleMs : { from: H + s.idleMs.from, to: H + s.idleMs.to }
    const drainMs = s.auxiliaryDrainMs.to - s.auxiliaryDrainMs.from
    work(`h${hh}d`, 'drain', idle.from - drainMs, drainMs, churn)
    add({
      id: `h${hh}i`,
      type: 'idle',
      lane: 'timer',
      shape: 'idle',
      at: idle.from,
      until: idle.to,
      participants: kinds,
    })
    const quiet = smoke ? s.smoke.quietAtMs : s.quietAtMs.map((q) => H + q)
    const quietDeadline = quiet[0] + (s.quietDeadlineMs - s.quietAtMs[0])
    quiet.forEach((q, reading) =>
      add({
        id: `h${hh}q${reading}`,
        type: 'quiet',
        lane: 'timer',
        boundary: `h${hh}`,
        reading,
        at: q,
        deadline: quietDeadline,
      })
    )
    if (h === 0 && !smoke)
      add({ id: 'seed-start', type: 'seed-start', lane: 'work', at: quietDeadline })
  }
  if (smoke) add({ id: 'seed-start', type: 'seed-start', lane: 'work', at: 0 })
  add({ id: 'seed-clear', type: 'seed-clear', lane: 'work', at: requestedMs })
  add({ id: 'disconnect', type: 'disconnect', lane: 'work', at: requestedMs })
  const rank = { 'seed-start': -1 }
  steps.sort(
    (a, b) => a.at - b.at || (rank[a.type] ?? 0) - (rank[b.type] ?? 0) || a.order - b.order
  )
  return { mode, requestedMs, steps }
}

/**
 * Counts what a plan schedules, per shape and per kind, for comparison with the matrix totals.
 * @param {{ steps: object[] }} plan
 * @returns {object} idle windows, executions by shape, pauses per ordinary kind, restarts per static
 *   kind, periodic slots, quiet readings and the shapes and kinds that act
 */
function scheduleTotals({ steps }) {
  const executions = steps.filter((s) => s.type === 'execution' || s.type === 'idle')
  const count = (shape) => executions.filter((s) => s.shape === shape).length
  const perKind = (list) => {
    const out = {}
    for (const kind of list) out[kind] = (out[kind] ?? 0) + 1
    return out
  }
  return {
    idleWindows: count('idle'),
    regexExecutions: count('regex-churn'),
    recreations: count('topic-recreation'),
    membershipExecutions: count('membership-churn'),
    outages: count('broker-outage'),
    pauseSequences: perKind(
      executions.filter((s) => s.shape === 'pause-resume').flatMap((s) => s.participants)
    ),
    staticRestarts: perKind(executions.flatMap((s) => s.restart ?? [])),
    periodicSlots: steps.filter((s) => s.type === 'periodic').length,
    quietReadings: steps.filter((s) => s.type === 'quiet').length,
    shapes: SHAPES.filter((shape) => count(shape) > 0).sort(),
    kinds: [...new Set(executions.flatMap((s) => s.participants))].sort(),
  }
}

/** The steps that still run after an abort, in plan order. */
const TEARDOWN = Object.freeze(['seed-clear', 'disconnect'])

/**
 * Runs a plan against a clock. Work steps run one at a time in plan order, and so do the timer
 * steps of each type (so only one timer per type is pending at any moment). A step that cannot
 * start before its deadline is not performed, and one that finishes after its deadline is late.
 * Either way it is reported `late` and never moved: later steps, and idle windows above all, keep
 * their times. Steps with `lane: 'within'` are performed by their parent work step and reported by
 * it. After `abort()`, every step that has not started is reported `aborted` without being
 * performed, except the teardown steps (`seed-clear`, then `disconnect`), which then run at once so
 * the run is torn down naturally and the seed's clear is still recorded.
 *
 * A work step still running `graceMs` after its deadline is reported `error` at that moment, so
 * its lane goes on and the failure aborts the run; its work cannot be cancelled, so it stays in
 * `inflight` until it settles. A sample that must see the run quiet asks `quiescent()`. While a
 * quiet sample runs no work step starts: a work step due then waits until the sample settled (and
 * is late if that passes its deadline), and an idle window ends only after it (`quietSettled`).
 */
class Scheduler {
  /**
   * @param {object} options
   * @param {{ now: () => number, sleepUntil: (at: number) => Promise<void> }} options.clock
   * @param {(step: object) => Promise<void>} options.perform does the step's work
   * @param {(outcome: object) => void} [options.onOutcome] receives each settled step
   * @param {number} [options.graceMs] how long past its deadline a work step may run, a real-time
   *   bound
   */
  constructor({ clock, perform, onOutcome = () => {}, graceMs = Infinity }) {
    this.clock = clock
    this.perform = perform
    this.onOutcome = onOutcome
    this.graceMs = graceMs
    this.summary = { ok: 0, late: 0, error: 0, aborted: 0 }
    this.aborted = null
    this.controller = new AbortController()
    this.inflight = new Set()
    this.quieting = new Set()
  }

  /**
   * Holds work steps and the end of an idle window until `work` settles, as a running quiet sample
   * does.
   * @param {Promise<unknown>} work @returns {void}
   */
  hold(work) {
    const held = Promise.resolve(work)
    this.quieting.add(held)
    const release = () => this.quieting.delete(held)
    held.then(release, release)
  }

  /** @returns {Promise<void>} resolves once no quiet sample or held work is running */
  async quietSettled() {
    while (this.quieting.size > 0) await Promise.allSettled([...this.quieting])
  }

  /** @returns {boolean} whether no work step is running, including one already reported late */
  quiescent() {
    return this.inflight.size === 0
  }

  /**
   * @param {object[]} steps the plan's steps, in start order
   * @returns {Promise<{ ok: number, late: number, error: number, aborted: number }>} counts by outcome
   */
  async run(steps) {
    const lanes = new Map()
    for (const step of steps) {
      if (step.lane === 'within') continue
      const lane = step.lane === 'work' ? 'work' : `timer:${step.type}`
      lanes.set(
        lane,
        (lanes.get(lane) ?? Promise.resolve()).then(() => this.runStep(step))
      )
    }
    await Promise.all(lanes.values())
    return this.summary
  }

  /**
   * Stops admitting steps; the run's teardown step still runs.
   * @param {string} reason why the run is aborted
   * @returns {void}
   */
  abort(reason) {
    this.aborted ??= reason
    this.controller.abort()
  }

  /** @param {object} step @returns {Promise<void>} */
  async runStep(step) {
    await this.clock.sleepUntil(step.at, this.controller.signal)
    if (step.lane === 'work') await this.quietSettled()
    const startedAt = this.clock.now()
    const deadline = step.deadline ?? Infinity
    let outcome = 'ok'
    let error = null
    if (this.aborted && !TEARDOWN.includes(step.type)) outcome = 'aborted'
    else if (startedAt >= deadline && !this.aborted) outcome = 'late'
    else {
      try {
        await this.bounded(step, deadline)
      } catch (e) {
        outcome = 'error'
        error = e.message
      }
      if (outcome === 'ok' && this.clock.now() > deadline) outcome = 'late'
    }
    this.summary[outcome]++
    this.onOutcome({
      id: step.id,
      type: step.type,
      outcome,
      at: step.at,
      startedAt,
      endedAt: this.clock.now(),
      error,
    })
  }

  /**
   * Performs one step, tracking a work step in `inflight` until its work settles and giving up
   * waiting `graceMs` after its deadline.
   * @param {object} step @param {number} deadline
   * @returns {Promise<void>}
   * @throws {Error} the step's own error, or that it still ran past its grace
   */
  async bounded(step, deadline) {
    const running = Promise.resolve().then(() => this.perform(step))
    if (step.type === 'quiet') {
      this.quieting.add(running)
      running.then(
        () => this.quieting.delete(running),
        () => this.quieting.delete(running)
      )
    }
    if (step.lane !== 'work') return running
    this.inflight.add(step.id)
    running.then(
      () => this.inflight.delete(step.id),
      () => this.inflight.delete(step.id)
    )
    const limit = deadline + realSpan(this.graceMs)
    if (limit === Infinity) return running
    const timer = new AbortController()
    try {
      const over = Symbol('over')
      const result = await Promise.race([
        running,
        this.clock.sleepUntil(limit, timer.signal).then(() => over),
      ])
      if (result === over)
        throw new Error(`${step.id} still runs ${this.graceMs} ms after its deadline`)
    } finally {
      timer.abort()
    }
  }
}

/**
 * Admits at most `limit` tasks at once, in call order. The supervisor runs each process's sample
 * through it, so no more than two processes force collections at the same moment and the others'
 * admissions are staggered inside the slot jitter.
 */
class SamplingGate {
  /** @param {{ limit?: number }} [options] the concurrent task bound, two by default */
  constructor({ limit = 2 } = {}) {
    this.limit = limit
    this.active = 0
    this.peak = 0
    this.waiting = []
    this.idlers = []
  }

  /**
   * Runs one task once a place is free.
   * @template T
   * @param {() => Promise<T>} task
   * @returns {Promise<T>} the task's own result or error
   */
  async run(task) {
    if (this.active < this.limit) this.active++
    else await new Promise((resolve) => this.waiting.push(resolve))
    this.peak = Math.max(this.peak, this.active)
    try {
      return await task()
    } finally {
      const next = this.waiting.shift()
      if (next) next()
      else this.active--
      if (this.active === 0) for (const resolve of this.idlers.splice(0)) resolve()
    }
  }

  /** @returns {Promise<void>} resolves once no task is running or waiting */
  idle() {
    if (this.active === 0) return Promise.resolve()
    return new Promise((resolve) => this.idlers.push(resolve))
  }
}

module.exports = {
  valueOf,
  harnessWait,
  harnessBound,
  realSpan,
  clearHarnessTimeout,
  liveHarnessTimers,
  MonotonicClock,
  planSchedule,
  scheduleTotals,
  Scheduler,
  SamplingGate,
}
