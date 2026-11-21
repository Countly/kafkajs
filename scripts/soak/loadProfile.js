'use strict'

/**
 * The load profile: its parameters, its offer schedule, its latency and rate report, its fault
 * schedules and its verdict. Everything here is pure; `loadRun.js` drives a cluster with it.
 *
 * The verdict keeps two questions apart. The product verdict is the sequence oracle's: no loss, no
 * duplicate or reordering beyond the producer kind's guarantee, no skipped commit. The profile
 * verdict says whether the run offered the load it was asked for: a run that did not reach its
 * target rate fails as a profile, and says so, but proves nothing against the product.
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
})

const INTEGER_FLAGS = Object.freeze({
  '--rate': 'rate',
  '--record-bytes': 'recordBytes',
  '--topics': 'topics',
  '--partitions': 'partitions',
  '--producers': 'producers',
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
 *   slow-broker       extension point: one broker delayed (`chaosSocket`) and then frozen
 *                     (`pauseBroker`) under load, for producers and consumers
 *   many-topics       extension point: thousands of topics with a trickle on each, a rolling
 *                     restart and a regex subscriber over all of them
 */
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
     * @returns {Promise<object[]>} every bounce step's outage, clean steps first
     */
    async start({ bouncer, admin, topics, windowMs, signal, sleep }) {
      await sleep(Math.floor(windowMs / 3), signal)
      if (signal.aborted) return []
      const between = FAULT_SCHEDULES['rolling-restarts'].betweenMs
      const clean = await bouncer.rollingBounce({ between, admin, topics, signal })
      await sleep(between, signal)
      if (signal.aborted) return clean
      const hard = await bouncer.rollingBounce({ hard: true, between, admin, topics, signal })
      return [...clean, ...hard]
    },
  }),
  'slow-broker': Object.freeze({ extensionPoint: true }),
  'many-topics': Object.freeze({ extensionPoint: true }),
})

const USAGE = `usage: node scripts/soak/loadRun.js --out <dir> [--rate <n>] [--record-bytes <n>]
  [--topics <n>] [--partitions <n>] [--producers <n>] [--producer-kinds <${KINDS.join('|')},...>]
  [--consumers <n>] [--duration <seconds>] [--compression <${CODECS.join('|')}>]
  [--faults <${Object.keys(FAULT_SCHEDULES).join('|')}>] [--replication-factor <n>]`

/**
 * @param {string[]} argv arguments after the script name
 * @returns {object} `LOAD_DEFAULTS` with the given flags applied, plus `out`
 * @throws {Error} the usage line, with the reason, for an unknown flag, a value out of range, an
 *   extension-point fault schedule or a missing `--out`
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
      if (schedule.extensionPoint) fail(`fault schedule ${value} is not implemented yet`)
      profile.faults = value
    } else if (flag === '--producer-kinds') {
      const kinds = value.split(',')
      if (!kinds.every((k) => KINDS.includes(k))) fail(`unknown producer kind in ${value}`)
      profile.producerKinds = kinds
    } else fail(`unknown flag ${flag}`)
  }
  if (!profile.out) fail('--out is required')
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
const EXIT = Object.freeze({ pass: 0, product: 1, inconclusive: 2, rateNotReached: 3 })

/** Oracle findings that say the read, not the product, went wrong. */
const READ_FINDINGS = Object.freeze(['incompleteRead', 'offsetOrder', 'unparseable'])

/**
 * The run's verdict.
 * @param {object} result
 * @param {{ complete: boolean, findings: Record<string, { count: number }> }|null} result.oracle
 *   `SequenceChecker#finish`, null when the read never happened
 * @param {{ reached: boolean }} result.rate `rateReport`
 * @param {string|null} [result.error] a driver failure
 * @returns {{ ok: boolean, product: 'PASS'|'FAIL'|'INCONCLUSIVE', profile: 'REACHED'|'RATE_NOT_REACHED', exitCode: number }}
 *   any product finding fails the product, except a loss seen by an incomplete read; a read
 *   finding or a driver error without a product finding is inconclusive; a product pass with the
 *   rate not reached exits 3, never 0
 */
const verdictOf = ({ oracle, rate, error = null }) => {
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
  const exitCode =
    product === 'FAIL'
      ? EXIT.product
      : product === 'INCONCLUSIVE'
        ? EXIT.inconclusive
        : profile === 'RATE_NOT_REACHED'
          ? EXIT.rateNotReached
          : EXIT.pass
  return { ok: exitCode === EXIT.pass, product, profile, exitCode }
}

module.exports = {
  CODECS,
  EXIT,
  FAULT_SCHEDULES,
  KINDS,
  LOAD_DEFAULTS,
  LatencyHistogram,
  OfferSchedule,
  USAGE,
  parseLoadArgs,
  rateReport,
  verdictOf,
}
