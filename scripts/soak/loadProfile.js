'use strict'

/**
 * The load profile: its parameters, its offer schedule, its latency and rate report, its fault
 * schedules and its verdict. Everything here is pure; `loadRun.js` drives a cluster with it.
 *
 * The verdict keeps two questions apart. The product verdict is the sequence oracle's: no loss, no
 * duplicate or reordering beyond the producer kind's guarantee, no skipped commit. The profile
 * verdict says whether the run offered the load it was asked for: a run that did not reach its
 * target rate fails as a profile, and says so, but proves nothing against the product. The
 * slow-broker schedule adds a flow verdict: whether the partitions led by the other brokers kept
 * flowing while one broker was slow, then frozen, and whether the slow broker's own partitions
 * flowed again after it (`flowVerdict`).
 */

const CODECS = Object.freeze(['none', 'gzip', 'snappy', 'lz4', 'zstd'])
const KINDS = Object.freeze(['idempotent', 'plain', 'transactional'])

/**
 * The parameters of a run with their defaults: the profile a hosted 4-core runner is dispatched
 * with. `producerKinds` is assigned to producers in turn.
 */
const LOAD_DEFAULTS = Object.freeze({
  rate: 10000,
  recordBytes: 1024,
  topics: 12,
  partitions: 16,
  producers: 3,
  /** Processes the producers are spread over; 1 keeps them in the run's own process. */
  producerProcesses: 1,
  producerKinds: Object.freeze(['idempotent', 'plain', 'transactional']),
  consumers: 3,
  durationSeconds: 600,
  compression: 'none',
  faults: 'none',
  replicationFactor: 3,
  minInsyncReplicas: 2,
  /** The least share of the offered records that must be sent and acknowledged. */
  targetFraction: 0.95,
  /** How long the run waits, after the offer window, for the sends still unsettled. */
  joinSeconds: 150,
  /** How long consumers may stay silent before the drain counts as done. */
  drainQuietSeconds: 15,
  drainSeconds: 180,
  /** Offer ticks a second; each tick sends what the schedule made due. */
  ticksPerSecond: 100,
  /** The most records a producer may have unsettled, as seconds of its share of the rate. */
  unsettledSeconds: 2,
  transactionMs: 500,
  abortEvery: 10,
  /** Send each tick's records as one batch per partition leader (`slow-broker` sets it). */
  leaderLanes: false,
  /** Give every record a `topic` header, so one read of all topics can tell them apart. */
  topicHeader: false,
  /** The interval of the throughput series (`ThroughputSeries`). */
  seriesIntervalMs: 5000,
})

const INTEGER_FLAGS = Object.freeze({
  '--rate': 'rate',
  '--record-bytes': 'recordBytes',
  '--topics': 'topics',
  '--partitions': 'partitions',
  '--producers': 'producers',
  '--producer-processes': 'producerProcesses',
  '--consumers': 'consumers',
  '--duration': 'durationSeconds',
  '--replication-factor': 'replicationFactor',
})

/**
 * The fault schedules a profile can run alongside its load. `start` runs concurrently with the offer
 * window and returns what it did; a schedule that is only an extension point has no `start` and is
 * refused by `parseLoadArgs`.
 *
 *   none              the plain profile
 *   rolling-restarts  from a third of the window: every broker stopped and restarted in turn, then
 *                     every broker killed and restarted in turn, `betweenMs` apart (BrokerBouncer)
 *   slow-broker       in the middle third of the window: kafka2's answers to clients delayed
 *                     `delayMs` (`NetemDelay`), then kafka2 paused `freezeMs` (a `BrokerBouncer`
 *                     pause step), then a recovery phase; judged by `flowVerdict`
 *   many-topics       thousands of single-partition topics at a modest rate, one clean rolling
 *                     restart from a third of the window, read back in one pass
 *
 * A schedule may also carry `profile` (parameters it sets), `validate` (refuses a profile it
 * cannot run) and `check` (proves its fault mechanism before anything is created). Every `start`
 * gets `{ bouncer, netem, admin, topics, windowMs, windowStart, clock, signal, sleep }`; a record
 * with `phase`, and a broker outage that ran, has `startMs` and `endMs` in ms from the window's
 * start. The window's end aborts `signal` and stops a schedule without failing it: a fault it
 * stopped is recorded with `skipped: 'window ended'`, so only a fault mechanism's own failure is a
 * schedule error; `verdictOf` makes such a run inconclusive.
 */
/**
 * Waits on the run's clock until `ms` after the window started.
 * @param {{ clock: { now: () => number }, windowStart: number, sleep: Function, signal: AbortSignal }} context
 * @param {number} ms
 * @returns {Promise<void>}
 */
const untilWindowMs = ({ clock, windowStart, sleep, signal }, ms) =>
  sleep(Math.max(0, ms - (clock.now() - windowStart)), signal)

/** @param {{ clock: { now: () => number }, windowStart: number }} context @returns {number} */
const windowMsOf = ({ clock, windowStart }) => Math.round(clock.now() - windowStart)

/** The brokers of the run's Compose cluster, in the order a rolling restart takes them. */
const BROKER_SERVICES = Object.freeze(['kafka1', 'kafka2', 'kafka3'])

/** What a fault step the window's end stopped from running records as `skipped`. */
const WINDOW_ENDED = 'window ended'

/**
 * Whether `error` is the offer window ending: the driver aborts the schedule's signal then, and a
 * `BrokerBouncer` step asked for after that rejects with the signal's reason.
 * @param {*} error @param {AbortSignal} signal
 * @returns {boolean}
 */
const windowEnded = (error, signal) => signal.aborted && error === signal.reason

/**
 * Restarts each broker in turn, `between` ms apart, while the window is open. The window's end
 * stops it without failing it: a node already down is restored by its own step, no later node is
 * taken down, and each restart not run is recorded with `skipped`, so a schedule longer than its
 * window says so in the report.
 * @param {object} context see `FAULT_SCHEDULES`
 * @param {{ rollingBounce: Function }} context.bouncer
 * @param {boolean} [context.hard=false] kill rather than stop
 * @param {number} context.between @param {object} context.admin @param {string[]} context.topics
 * @param {AbortSignal} context.signal the window
 * @param {(ms: number, signal?: AbortSignal) => Promise<void>} context.sleep
 * @returns {Promise<object[]>} per broker in order, its outage with `startMs` and `endMs` (the
 *   step's span in ms from the window's start, on the run's clock), or `{ service, kind, skipped }`
 * @throws {Error} a restart's own failure, after its node was restored
 */
const restartEach = async (context) => {
  const { bouncer, hard = false, between, admin, topics, signal, sleep } = context
  const records = []
  for (const [i, service] of BROKER_SERVICES.entries()) {
    if (i > 0) await sleep(between, signal)
    if (!signal.aborted) {
      try {
        const startMs = windowMsOf(context)
        const outages = await bouncer.rollingBounce({
          hard,
          admin,
          topics,
          services: [service],
          signal,
        })
        const endMs = windowMsOf(context)
        records.push(...outages.map((outage) => ({ ...outage, startMs, endMs })))
        continue
      } catch (error) {
        if (!windowEnded(error, signal)) throw error
      }
    }
    records.push({ service, kind: hard ? 'kill' : 'stop', skipped: WINDOW_ENDED })
  }
  return records
}

/**
 * A rolling restart phase: one `restartEach` round per entry of `hards` (kill when true), `between`
 * ms apart. The phase record, first, spans the rounds; it is left out when the window had already
 * ended, and every restart is then recorded as not run.
 * @param {object} context see `FAULT_SCHEDULES` @param {number} between
 * @param {boolean[]} hards
 * @returns {Promise<object[]>} the phase, then each restart's record in order
 * @throws {Error} a restart's own failure (`restartEach`)
 */
const rollingRestart = async (context, between, hards) => {
  const { signal, sleep } = context
  const kind = hards.map((hard) => (hard ? 'kill' : 'stop')).join('+')
  const phase = signal.aborted
    ? null
    : { phase: 'rolling-restart', kind, startMs: windowMsOf(context) }
  const records = []
  for (const [i, hard] of hards.entries()) {
    if (i > 0) await sleep(between, signal)
    records.push(...(await restartEach({ ...context, hard, between })))
  }
  if (!phase) return records
  phase.endMs = windowMsOf(context)
  return [phase, ...records]
}

/** The fault kind of each slow-broker phase. */
const SLOW_KINDS = Object.freeze({ slow: 'delay', frozen: 'pause', recovery: 'none' })

const FAULT_SCHEDULES = Object.freeze({
  none: Object.freeze({ start: async () => [] }),
  'rolling-restarts': Object.freeze({
    betweenMs: 20000,
    /**
     * @param {object} context
     * @param {{ rollingBounce: Function }} context.bouncer a `BrokerBouncer`
     * @param {object} context.admin @param {string[]} context.topics
     * @param {number} context.windowMs @param {AbortSignal} context.signal
     * @param {(ms: number, signal?: AbortSignal) => Promise<void>} context.sleep
     * @returns {Promise<object[]>} the `rolling-restart` phase, then every restart's outage, clean
     *   ones first; a restart the window's end stopped is recorded with `skipped` (`rollingRestart`)
     */
    async start(context) {
      const { windowMs, signal, sleep } = context
      await sleep(Math.floor(windowMs / 3), signal)
      return rollingRestart(context, FAULT_SCHEDULES['rolling-restarts'].betweenMs, [false, true])
    },
  }),
  'slow-broker': Object.freeze({
    service: 'kafka2',
    nodeId: 2,
    /** A slow broker, not a dead one: well below the 30 s request timeout. */
    delayMs: 300,
    /** Longer than the client's 30 s default request timeout, so in-flight requests time out. */
    freezeMs: 45000,
    /** Each phase needs at least 20 s around the freeze: the middle third holds 45 + 20 s. */
    minWindowMs: 3 * 65000,
    /**
     * 10 ticks a second keeps each producer at 10 requests a second per broker, under the 5 in
     * flight / 300 ms ≈ 16 a second that one connection to the delayed broker can complete, so
     * the slow lane is limited by the delay and not by a request queue the harness overfills.
     * One lane per leader keeps a slow partition's acknowledgement out of another leader's send:
     * the flow verdict then sees the client, not the harness's batching.
     */
    profile: Object.freeze({ ticksPerSecond: 10, leaderLanes: true }),
    /** @param {object} profile @returns {string|null} why the profile cannot run it */
    validate: (profile) =>
      profile.durationSeconds * 1000 >= FAULT_SCHEDULES['slow-broker'].minWindowMs
        ? null
        : `fault schedule slow-broker needs --duration of at least ${FAULT_SCHEDULES['slow-broker'].minWindowMs / 1000}`,
    /**
     * @param {{ netem: { check: () => Promise<void> }|null }} context
     * @returns {Promise<void>}
     * @throws {Error} when there is no delay tool or it does not work on this host
     */
    async check({ netem }) {
      if (!netem) throw new Error('slow-broker needs a network delay tool')
      await netem.check()
    },
    /**
     * @param {object} context see `FAULT_SCHEDULES`
     * @returns {Promise<object[]>} the slow, frozen and recovery phases as they ran; the delay is
     *   always removed, and `BrokerBouncer#step` always unpauses; each phase the window's end
     *   stopped is recorded with `skipped`, so a run that did not hold all three is incomplete
     */
    async start(context) {
      const { netem, bouncer, admin, topics, windowMs, signal } = context
      const { service, delayMs, freezeMs } = FAULT_SCHEDULES['slow-broker']
      const third = windowMs / 3
      const records = []
      const notRun = (...phases) => {
        for (const phase of phases)
          records.push({ phase, service, kind: SLOW_KINDS[phase], skipped: WINDOW_ENDED })
        return records
      }
      await untilWindowMs(context, third)
      if (signal.aborted) return notRun('slow', 'frozen', 'recovery')
      const slow = { phase: 'slow', service, kind: 'delay', delayMs, startMs: windowMsOf(context) }
      records.push(slow)
      await netem.apply()
      try {
        await untilWindowMs(context, third + (third - freezeMs) / 2)
      } finally {
        await netem.clear()
        slow.endMs = windowMsOf(context)
      }
      if (signal.aborted) return notRun('frozen', 'recovery')
      const frozen = { phase: 'frozen', service, kind: 'pause', requestedMs: windowMsOf(context) }
      records.push(frozen)
      let outage
      try {
        outage = await bouncer.step(
          { service, kind: 'pause', holdMs: freezeMs },
          { admin, topics, signal }
        )
      } catch (error) {
        if (!windowEnded(error, signal)) throw error
        frozen.skipped = WINDOW_ENDED
        return notRun('recovery')
      }
      frozen.endMs = windowMsOf(context)
      frozen.startMs = frozen.endMs - outage.outageMs
      frozen.outageMs = outage.outageMs
      const recovery = { phase: 'recovery', service, kind: 'none', startMs: frozen.endMs }
      records.push(recovery)
      await untilWindowMs(context, 2 * third)
      recovery.endMs = windowMsOf(context)
      return records
    },
  }),
  'many-topics': Object.freeze({
    betweenMs: 20000,
    /** Below this many partitions in total the run is not a many-topics run. */
    minPartitions: 1000,
    /** One console consumer reads every topic; per-topic reads cost a JVM and an idle timeout each. */
    profile: Object.freeze({ topicHeader: true }),
    /** @param {object} profile @returns {string|null} why the profile cannot run it */
    validate: (profile) =>
      profile.topics * profile.partitions >= FAULT_SCHEDULES['many-topics'].minPartitions
        ? null
        : `fault schedule many-topics needs at least ${FAULT_SCHEDULES['many-topics'].minPartitions} partitions (--topics × --partitions)`,
    /**
     * @param {object} context see `FAULT_SCHEDULES`
     * @returns {Promise<object[]>} the rolling restart's phase, then each restart's outage or,
     *   when the window's end stopped it, `skipped` (`restartEach`)
     */
    async start(context) {
      await untilWindowMs(context, context.windowMs / 3)
      return rollingRestart(context, FAULT_SCHEDULES['many-topics'].betweenMs, [false])
    },
  }),
})

const USAGE = `usage: node scripts/soak/loadRun.js --out <dir> [--rate <n>] [--record-bytes <n>]
  [--topics <n>] [--partitions <n>] [--producers <n>] [--producer-kinds <${KINDS.join('|')},...>]
  [--consumers <n>] [--duration <seconds>] [--compression <${CODECS.join('|')}>]
  [--faults <${Object.keys(FAULT_SCHEDULES).join('|')}>] [--replication-factor <n>]
  [--producer-processes <n>]
  slow-broker needs --duration of at least 195 and one producer process; many-topics needs at least
  1000 partitions; --producer-processes is at most --producers`

/**
 * @param {string[]} argv arguments after the script name
 * @returns {object} `LOAD_DEFAULTS` with the given flags applied, plus `out`
 * @throws {Error} the usage line, with the reason, for an unknown flag, a value out of range, a
 *   fault schedule the profile cannot hold or a missing `--out`
 */
const parseLoadArgs = (argv) => {
  const profile = { ...LOAD_DEFAULTS, out: null }
  const fail = (why) => {
    throw new Error(`${why}\n${USAGE}`)
  }
  for (let i = 0; i < argv.length; i += 2) {
    const [flag, value] = [argv[i], argv[i + 1]]
    if (value === undefined) fail(`${flag} needs a value`)
    if (INTEGER_FLAGS[flag]) {
      const n = Number(value)
      if (!Number.isInteger(n) || n < 1) fail(`${flag} must be a positive integer`)
      profile[INTEGER_FLAGS[flag]] = n
    } else if (flag === '--out') profile.out = value
    else if (flag === '--compression') {
      if (!CODECS.includes(value)) fail(`unknown codec ${value}`)
      profile.compression = value
    } else if (flag === '--faults') {
      const schedule = FAULT_SCHEDULES[value]
      if (!schedule) fail(`unknown fault schedule ${value}`)
      profile.faults = value
    } else if (flag === '--producer-kinds') {
      const kinds = value.split(',')
      if (!kinds.every((k) => KINDS.includes(k))) fail(`unknown producer kind in ${value}`)
      profile.producerKinds = kinds
    } else fail(`unknown flag ${flag}`)
  }
  if (!profile.out) fail('--out is required')
  const schedule = FAULT_SCHEDULES[profile.faults]
  Object.assign(profile, schedule.profile)
  const refusal = schedule.validate?.(profile)
  if (refusal) fail(refusal)
  if (profile.producerProcesses > profile.producers)
    fail('--producer-processes must be at most --producers')
  if (profile.leaderLanes && profile.producerProcesses > 1)
    fail(`fault schedule ${profile.faults} needs --producer-processes 1`)
  if (profile.minInsyncReplicas > profile.replicationFactor)
    profile.minInsyncReplicas = profile.replicationFactor
  return profile
}

/**
 * A deadline schedule: record n of a producer is due `n / rate` seconds after the start, whether or
 * not the producer kept up, so latency measured from the due time includes the wait a slow client
 * imposes (no coordinated omission).
 */
class OfferSchedule {
  /** @param {{ rate: number, start: number }} options records a second, start time in ms */
  constructor({ rate, start }) {
    Object.assign(this, { rate, start })
    this.taken = 0
  }

  /** @param {number} now @returns {number} records due by `now` and not yet taken */
  due(now) {
    return Math.max(0, Math.floor(((now - this.start) * this.rate) / 1000) - this.taken)
  }

  /**
   * Takes the next `count` records.
   * @param {number} count
   * @returns {{ first: number, dueAt: (i: number) => number }} the due time of the i-th
   */
  take(count) {
    const first = this.taken
    this.taken += count
    return { first, dueAt: (i) => this.start + ((first + i + 1) * 1000) / this.rate }
  }
}

/**
 * A latency histogram of fixed size: logarithmic buckets 2 % wide from 1 µs to beyond an hour.
 */
class LatencyHistogram {
  static #BASE = Math.log(1.02)
  static #BUCKETS = 1200

  constructor() {
    this.counts = new Float64Array(LatencyHistogram.#BUCKETS)
    this.total = 0
    this.maxMs = 0
  }

  /** @param {number} ms @param {number} [weight=1] @returns {void} */
  record(ms, weight = 1) {
    const us = Math.max(1, ms * 1000)
    const i = Math.min(
      LatencyHistogram.#BUCKETS - 1,
      Math.ceil(Math.log(us) / LatencyHistogram.#BASE)
    )
    this.counts[i] += weight
    this.total += weight
    if (ms > this.maxMs) this.maxMs = ms
  }

  /**
   * @param {number} p a percentile, 0 < p ≤ 100
   * @returns {number|null} the upper edge of the bucket holding it, in ms; null with no samples
   */
  percentile(p) {
    if (this.total === 0) return null
    const rank = (p / 100) * this.total
    let seen = 0
    for (let i = 0; i < this.counts.length; i++) {
      seen += this.counts[i]
      if (seen >= rank) return Math.min(this.maxMs, Math.exp(i * LatencyHistogram.#BASE) / 1000)
    }
    return this.maxMs
  }

  /** @returns {{ counts: number[], total: number, maxMs: number }} for `merge` in another process */
  snapshot() {
    return { counts: Array.from(this.counts), total: this.total, maxMs: this.maxMs }
  }

  /** @param {{ counts: number[], total: number, maxMs: number }} other a `snapshot` @returns {void} */
  merge({ counts, total, maxMs }) {
    for (let i = 0; i < counts.length; i++) this.counts[i] += counts[i]
    this.total += total
    this.maxMs = Math.max(this.maxMs, maxMs)
  }

  /** @returns {{ samples: number, p50: ?number, p90: ?number, p99: ?number, p999: ?number, max: number }} */
  toJSON() {
    const at = (p) => this.percentile(p)
    return {
      samples: this.total,
      p50: at(50),
      p90: at(90),
      p99: at(99),
      p999: at(99.9),
      max: this.maxMs,
    }
  }
}

/**
 * Offered against achieved.
 * @param {object} totals
 * @param {number} totals.rate the target, records a second
 * @param {number} totals.windowMs the offer window
 * @param {number} totals.offered records the schedule made due in the window
 * @param {number} totals.sent records handed to the client in the window
 * @param {number} totals.acked records acknowledged, or deliberately aborted in a transaction, by the
 *   end of the join
 * @param {number} [totals.failed=0] records whose send the client rejected: a shortfall they cause
 *   is the client refusing, which the report names so it is not read as a slow runner
 * @param {number} totals.targetFraction
 * @returns {{ reached: boolean, target: number, offeredRate: number, sentRate: number, ackedRate: number, shortfall: string|null }}
 */
const rateReport = ({ rate, windowMs, offered, sent, acked, failed = 0, targetFraction }) => {
  const perSecond = (n) => Math.round((n * 1000) / windowMs)
  const floor = targetFraction * offered
  const short = []
  if (sent < floor) short.push(`sent ${sent} of ${offered} offered`)
  if (acked < floor)
    short.push(
      `acknowledged ${acked} of ${offered} offered` + (failed > 0 ? ` (${failed} rejected)` : '')
    )
  return {
    reached: offered > 0 && short.length === 0,
    target: rate,
    offeredRate: perSecond(offered),
    sentRate: perSecond(sent),
    ackedRate: perSecond(acked),
    shortfall: offered === 0 ? 'nothing was offered' : short.join('; ') || null,
  }
}

/** Exit codes of a run. */
const EXIT = Object.freeze({
  pass: 0,
  product: 1,
  inconclusive: 2,
  rateNotReached: 3,
  flowStalled: 4,
})

/** Oracle findings that say the read, not the product, went wrong. */
const READ_FINDINGS = Object.freeze(['incompleteRead', 'offsetOrder', 'unparseable'])

/**
 * The run's verdict.
 * @param {object} result
 * @param {{ complete: boolean, findings: Record<string, { count: number }> }|null} result.oracle
 *   `SequenceChecker#finish`, null when the read never happened
 * @param {{ reached: boolean }} result.rate `rateReport`
 * @param {string|null} [result.error] a driver failure
 * @param {'KEPT_FLOWING'|'STALLED'|'INCONCLUSIVE'|null} [result.flow] `flowVerdict`, for a
 *   schedule that has one; the verdict carries it only then
 * @param {object[]} [result.faults] the fault schedule's records: one with `skipped` is a fault
 *   that did not run
 * @returns {{ ok: boolean, product: 'PASS'|'FAIL'|'INCONCLUSIVE', profile: 'REACHED'|'RATE_NOT_REACHED',
 *   flow?: string, faults?: 'INCOMPLETE', reasons?: string[], exitCode: number }}
 *   any product finding fails the product, except a loss seen by an incomplete read; a read
 *   finding or a driver error without a product finding is inconclusive; with the product passing,
 *   faults not run (`faults` INCOMPLETE, each named in `reasons`) or an inconclusive flow exit 2, a
 *   stalled flow 4, and the rate not reached 3, never 0
 */
const verdictOf = ({ oracle, rate, error = null, flow = null, faults = [] }) => {
  const counts = oracle ? oracle.findings : {}
  const productFinding = Object.entries(counts).some(
    ([kind, { count }]) =>
      count > 0 && !READ_FINDINGS.includes(kind) && (kind !== 'loss' || oracle.complete)
  )
  const readFinding = READ_FINDINGS.some((kind) => counts[kind]?.count > 0)
  const product = productFinding
    ? 'FAIL'
    : !oracle || error || readFinding || !oracle.complete
      ? 'INCONCLUSIVE'
      : 'PASS'
  const profile = rate.reached ? 'REACHED' : 'RATE_NOT_REACHED'
  const notRun = faults
    .filter((f) => f.skipped)
    .map((f) => `${f.phase ?? f.kind} ${f.service}: ${f.skipped}`)
  const exitCode =
    product === 'FAIL'
      ? EXIT.product
      : product === 'INCONCLUSIVE' || flow === 'INCONCLUSIVE' || notRun.length
        ? EXIT.inconclusive
        : flow === 'STALLED'
          ? EXIT.flowStalled
          : profile === 'RATE_NOT_REACHED'
            ? EXIT.rateNotReached
            : EXIT.pass
  return {
    ok: exitCode === EXIT.pass,
    product,
    profile,
    ...(flow && { flow }),
    ...(notRun.length && { faults: 'INCOMPLETE', reasons: notRun }),
    exitCode,
  }
}

/**
 * The phases of a window: `baseline` before the first recorded phase, each record that has a
 * `phase` in start order, and `after` from the last one's end to the window's end.
 * @param {object[]} records a fault schedule's records
 * @param {number} windowMs
 * @returns {{ phase: string, startMs: number, endMs: number }[]}
 */
const phaseList = (records, windowMs) => {
  const phases = records
    .filter((r) => r.phase && Number.isFinite(r.startMs) && Number.isFinite(r.endMs))
    .map(({ phase, startMs, endMs }) => ({ phase, startMs, endMs }))
    .sort((a, b) => a.startMs - b.startMs)
  const first = phases[0]?.startMs ?? windowMs
  const last = phases[phases.length - 1]?.endMs ?? windowMs
  return [
    { phase: 'baseline', startMs: 0, endMs: first },
    ...phases,
    { phase: 'after', startMs: last, endMs: windowMs },
  ]
}

/**
 * Memory per phase, from the run's samples: the peak and the last sample in each phase (a sample
 * at `startMs < atMs ≤ endMs` belongs to it).
 * @param {{ atMs: number, heapUsed: number, rss: number }[]} samples
 * @param {{ phase: string, startMs: number, endMs: number }[]} phases `phaseList`
 * @returns {object[]} per phase `peakHeapUsed`, `peakRss`, `endHeapUsed`, `endRss`; null with no sample
 */
const phaseMemory = (samples, phases) =>
  phases.map(({ phase, startMs, endMs }) => {
    const inside = samples.filter((s) => s.atMs > startMs && s.atMs <= endMs)
    const last = inside[inside.length - 1]
    return {
      phase,
      startMs,
      endMs,
      peakHeapUsed: inside.length ? Math.max(...inside.map((s) => s.heapUsed)) : null,
      peakRss: inside.length ? Math.max(...inside.map((s) => s.rss)) : null,
      endHeapUsed: last?.heapUsed ?? null,
      endRss: last?.rss ?? null,
    }
  })

/**
 * CPU per phase, from the run's samples: this process's CPU as a share of one core and the
 * machine's busy share of all its cores, between the last sample at or before the phase's start
 * and the last one at or before its end.
 * @param {{ atMs: number, processUs: number, machineBusyMs: number, machineTotalMs: number }[]} samples
 *   in time order, the first at the window's start
 * @param {{ phase: string, startMs: number, endMs: number }[]} phases `phaseList`
 * @returns {{ phase: string, startMs: number, endMs: number, coreShare: number|null, machineBusyShare: number|null }[]}
 *   null where no two samples bound the phase
 */
const phaseCpu = (samples, phases) =>
  phases.map(({ phase, startMs, endMs }) => {
    const lastAt = (ms) => samples.findLast((s) => s.atMs <= ms)
    const from = lastAt(startMs)
    const to = lastAt(endMs)
    const spanned = from && to && to.atMs > from.atMs
    const share = (n, d) => (spanned && d > 0 ? Math.round((n / d) * 1000) / 1000 : null)
    return {
      phase,
      startMs,
      endMs,
      coreShare: share(to?.processUs - from?.processUs, (to?.atMs - from?.atMs) * 1000),
      machineBusyShare: share(
        to?.machineBusyMs - from?.machineBusyMs,
        to?.machineTotalMs - from?.machineTotalMs
      ),
    }
  })

/**
 * When a rate shortfall is put down to the CPU, from the offer window's measurements.
 *
 *   clientCoreShare   a load process at 90 % of one core or more is limited by the client: its
 *                     JavaScript runs on one thread, so the rest of a core is all it can gain
 *   machineBusyShare  every core of the machine 90 % busy or more: the brokers, the clients and
 *                     the console reads share the machine, and none of them can go faster
 */
const LIMIT_RULES = Object.freeze({ clientCoreShare: 0.9, machineBusyShare: 0.9 })

/**
 * What limited a run that did not reach its rate. A client at one core is named first: when the
 * machine is saturated too, the client is still the part that cannot use another core.
 * @param {object} input
 * @param {boolean} input.reached `rateReport`
 * @param {(number|null)[]} input.coreShares each load process's CPU over the offer window, as a
 *   share of one core
 * @param {number|null} input.machineBusyShare the machine's busy share over the offer window
 * @returns {'client-cpu'|'machine'|'unknown'|null} null when the rate was reached; `unknown` when
 *   neither rule holds: look at the brokers, the unsettled cap and the rejected sends
 */
const rateLimitedBy = ({ reached, coreShares, machineBusyShare }) => {
  if (reached) return null
  if (coreShares.some((share) => (share ?? 0) >= LIMIT_RULES.clientCoreShare)) return 'client-cpu'
  if ((machineBusyShare ?? 0) >= LIMIT_RULES.machineBusyShare) return 'machine'
  return 'unknown'
}

/**
 * The flow rules of the slow-broker schedule, applied to the idempotent and plain producers'
 * acknowledgements per partition leader (a transaction waits for every partition it wrote, so a
 * transactional producer stalls with the slowest one by design and is left out).
 *
 *   slow      the other leaders acknowledge at least 90 % of what they were offered: their brokers
 *             are untouched, so only the client can slow them; 10 % covers the 100 ms offer tick
 *             and the unsettled cap's ramp
 *   frozen    the other leaders stall at most 20 s and acknowledge at least 50 %: the broker side
 *             holds them about 11 s (the frozen broker stays in their ISR until the controller
 *             fences it after broker.session.timeout.ms, 9 s, plus a 2 s heartbeat), while a client
 *             that waits on the frozen broker's requests holds them a full 30 s request timeout, so
 *             20 s separates the two; flowing at the offered rate after a 20 s stall is 25 of 45 s,
 *             above 50 %
 *   recovery  every leader, the slow broker's too, acknowledges at least 90 %: the client
 *             recovered without a restart
 */
const FLOW_RULES = Object.freeze({
  slow: Object.freeze({ minRatio: 0.9, maxStallSeconds: null, slowLeaderToo: false }),
  frozen: Object.freeze({ minRatio: 0.5, maxStallSeconds: 20, slowLeaderToo: false }),
  recovery: Object.freeze({ minRatio: 0.9, maxStallSeconds: null, slowLeaderToo: true }),
})

/** A judged phase shorter than this many whole seconds is too short to give a rate. */
const MIN_JUDGED_SECONDS = 5

/**
 * The per-leader flow verdict of the slow-broker schedule.
 * @param {object} input
 * @param {{ phase: string, startMs: number, endMs: number }[]} input.phases `phaseList`
 * @param {{ sum: Function, longestStall: Function, wholeSeconds: Function }} input.flow a
 *   `FlowMeter` with the `acked` series per leader (and `delivered`, reported only)
 * @param {Record<string, number>} input.rates per leader, records a second offered by the producers
 *   the rules judge; its keys are the leaders
 * @param {string} input.slowNode the slowed broker's node id
 * @returns {{ verdict: 'KEPT_FLOWING'|'STALLED'|'INCONCLUSIVE', reasons: string[], table: object[] }}
 *   one table row per phase and leader; STALLED when a rule is broken, else INCONCLUSIVE when a
 *   judged phase was too short or the slowed broker led nothing
 */
const flowVerdict = ({ phases, flow, rates, slowNode }) => {
  const stalled = []
  const unjudged = []
  const table = []
  const leaders = Object.keys(rates).sort()
  if (!leaders.includes(slowNode))
    unjudged.push(`leader ${slowNode} led no partition: nothing was slowed`)
  for (const { phase, startMs, endMs } of phases) {
    const seconds = flow.wholeSeconds(startMs, endMs)
    const rule = FLOW_RULES[phase]
    if (rule && seconds < MIN_JUDGED_SECONDS)
      unjudged.push(
        `${phase}: ${seconds} whole seconds, too short to judge (least ${MIN_JUDGED_SECONDS})`
      )
    for (const leader of leaders) {
      const perSecond = (series) =>
        seconds ? Math.round(flow.sum(series, leader, startMs, endMs) / seconds) : null
      const row = {
        phase,
        leader,
        offeredRate: Math.round(rates[leader]),
        ackedRate: perSecond('acked'),
        deliveredRate: perSecond('delivered'),
        longestStallSeconds: flow.longestStall('acked', leader, startMs, endMs),
      }
      table.push(row)
      if (!rule || seconds < MIN_JUDGED_SECONDS) continue
      if (leader === slowNode && !rule.slowLeaderToo) continue
      if (rule.maxStallSeconds !== null && row.longestStallSeconds > rule.maxStallSeconds)
        stalled.push(
          `${phase}: leader ${leader} stalled ${row.longestStallSeconds} s (most ${rule.maxStallSeconds} s)`
        )
      const ratio = flow.sum('acked', leader, startMs, endMs) / (rates[leader] * seconds)
      if (ratio < rule.minRatio)
        stalled.push(
          `${phase}: leader ${leader} acknowledged ${Math.round(ratio * 100)} % of its offered rate (least ${rule.minRatio * 100} %)`
        )
    }
  }
  const verdict = stalled.length ? 'STALLED' : unjudged.length ? 'INCONCLUSIVE' : 'KEPT_FLOWING'
  return { verdict, reasons: [...stalled, ...unjudged], table }
}

/**
 * The stall rule of the throughput series: a stretch longer than `maxSilentMs` with no
 * acknowledgement while records were offered, outside every fault on the group's brokers, is
 * flagged. 20 s is the frozen-phase bound of `FLOW_RULES` and for the same reason: a broker that
 * stops answering holds its partitions about 11 s before the controller fences it, while a client
 * that waits on a refresh or a request holds them for a 30 s request timeout or longer. It must
 * stay above the series interval, since a silence shorter than one interval is not a candidate.
 */
const STALL_RULE = Object.freeze({ maxSilentMs: 20000 })

/**
 * Records offered in each interval by a deadline schedule of `rate` a second: the schedule's own
 * count, `floor(t * rate / 1000)`, at the interval's ends, none after the window.
 * @param {number} rate @param {number} intervalMs @param {number} intervals @param {number} windowMs
 * @returns {number[]}
 */
const offeredPerInterval = (rate, intervalMs, intervals, windowMs) =>
  Array.from({ length: intervals }, (_, i) => {
    const due = (ms) => Math.floor((Math.min(ms, windowMs) * rate) / 1000)
    return due((i + 1) * intervalMs) - due(i * intervalMs)
  })

/**
 * The longest piece of `[from, to]` outside every one of `windows`.
 * @param {number} from @param {number} to @param {{ startMs: number, endMs: number }[]} windows
 * @returns {{ startMs: number, lengthMs: number }|null} null when the windows cover it
 */
const longestOutside = (from, to, windows) => {
  let pieces = [[from, to]]
  for (const { startMs, endMs } of windows)
    pieces = pieces.flatMap(([a, b]) =>
      [
        [a, Math.min(b, startMs)],
        [Math.max(a, endMs), b],
      ].filter(([x, y]) => y > x)
    )
  const best = pieces.reduce((m, p) => (!m || p[1] - p[0] > m[1] - m[0] ? p : m), null)
  return best && { startMs: best[0], lengthMs: best[1] - best[0] }
}

/**
 * The throughput series with the records offered in each interval, and its stall finding.
 *
 * Per group with a rate (`all`, and each original partition leader when the run has leader lanes)
 * the silences are the stretches between consecutive acknowledgements inside the offer window,
 * from its start to its end, taken from each interval's first and last acknowledgement. Only a
 * silence of at least one interval is a candidate: a shorter one can stay inside an interval,
 * where only its length (`maxGapMs`) is kept, so it is not measured everywhere.
 * A silence is excused where it overlaps a fault on the group's brokers: an outage, delay or pause
 * record (with `service`, `startMs` and `endMs`) of that leader's broker, and for `all` any such
 * record. A phase record without a service, and `recovery`, excuse nothing. The group is flagged
 * when the longest piece of a silence outside its excuses is longer than the rule.
 *
 * What it cannot tell: without leader lanes (every schedule but slow-broker) only `all` exists, so
 * one stopped leader whose partitions others outweigh is not a silence, and any outage excuses the
 * whole run; leaders are taken before the window, so after a failover a partition counts under its
 * first leader. It is a report, not part of the verdict.
 * @param {object} input
 * @param {object} input.series `ThroughputSeries#snapshot`, merged across processes
 * @param {Record<string, number>} input.rates records a second offered per group; a group without
 *   one is not judged
 * @param {number} input.windowMs @param {object[]} input.faults the fault schedule's records
 * @returns {{ series: object, summary: object }} `series` with `offered` per group; `summary`
 *   per judged group its longest silence, its longest piece outside the excuses (`unexcused`),
 *   each null with no silence of an interval or more, and `flagged`, then `stalled` and `reasons`
 */
const throughputReport = ({ series, rates, windowMs, faults }) => {
  const { intervalMs, intervals } = series
  const excuses = faults.filter(
    (f) => f.service && f.kind !== 'none' && Number.isFinite(f.startMs) && Number.isFinite(f.endMs)
  )
  const nodeOf = (service) => String(BROKER_SERVICES.indexOf(service) + 1)
  const groups = {}
  const summary = {}
  const reasons = []
  const seconds = (ms) => (ms / 1000).toFixed(1)
  for (const [name, cells] of Object.entries(series.groups)) {
    const rate = rates[name]
    groups[name] = {
      ...cells,
      offered:
        rate === undefined ? null : offeredPerInterval(rate, intervalMs, intervals, windowMs),
    }
    if (rate === undefined) continue
    const windows = excuses.filter((f) => name === 'all' || nodeOf(f.service) === name)
    let longestSilence = null
    let unexcused = null
    const silence = (from, to) => {
      if (to - from < intervalMs) return
      if (to - from > (longestSilence?.lengthMs ?? 0))
        longestSilence = { startMs: from, lengthMs: to - from }
      const piece = longestOutside(from, to, windows)
      if (piece && piece.lengthMs > (unexcused?.lengthMs ?? 0)) unexcused = piece
    }
    let last = 0
    for (let i = 0; i < intervals && i * intervalMs < windowMs; i++) {
      if (cells.firstAckMs[i] === null) continue
      silence(last, Math.min(cells.firstAckMs[i], windowMs))
      last = cells.lastAckMs[i]
    }
    if (last < windowMs) silence(last, windowMs)
    const flagged = (unexcused?.lengthMs ?? 0) > STALL_RULE.maxSilentMs
    summary[name] = { longestSilence, unexcused, flagged }
    if (flagged)
      reasons.push(
        `${name === 'all' ? 'all' : `leader ${name}`}: no acknowledgement for ${seconds(unexcused.lengthMs)} s from ${seconds(unexcused.startMs)} s, outside any fault on its brokers (most ${STALL_RULE.maxSilentMs / 1000} s)`
      )
  }
  return {
    series: { ...series, groups },
    summary: {
      intervalMs,
      intervals,
      beyond: series.beyond,
      stallThresholdMs: STALL_RULE.maxSilentMs,
      groups: summary,
      stalled: reasons.length > 0,
      reasons,
    },
  }
}

module.exports = {
  BROKER_SERVICES,
  CODECS,
  EXIT,
  FAULT_SCHEDULES,
  FLOW_RULES,
  KINDS,
  LIMIT_RULES,
  LOAD_DEFAULTS,
  LatencyHistogram,
  OfferSchedule,
  USAGE,
  flowVerdict,
  parseLoadArgs,
  phaseCpu,
  phaseList,
  phaseMemory,
  rateLimitedBy,
  rateReport,
  STALL_RULE,
  throughputReport,
  verdictOf,
}
