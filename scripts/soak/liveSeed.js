'use strict'

/**
 * The seeded control of the live telemetry churn. Each worker retains one fresh, touched buffer for
 * every scheduled sample, through the product's own `Kafka` class, at twice the slope limit. The
 * control proves the live retained-slope verdict and the shadow data slope both see real growth.
 * A seeded run is never acceptance evidence: its records live only under `controls/` and the churn
 * archive verifier refuses any row that carries a seed.
 */
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')

/**
 * The control's switch, mode and rate, and how far the shadow data slope may fall short of it.
 * `toleranceFraction` is of the seeded rate: a quarter, so the data slope must reach 96 KiB/min of
 * the 128 KiB/min seeded, beside natural drift. `allowed` is every violation a seeded run may
 * report: the slope, and the endpoint and closed ceilings that retained bytes can also cross.
 */
const LIVE_SEED = Object.freeze({
  env: 'TELEMETRY_SEED',
  mode: 'slope2x',
  factor: 2,
  root: Symbol.for('telemetry.seed.root'),
  fill: 0x5a,
  toleranceFraction: 0.25,
  allowed: Object.freeze(['retained slope', 'retained endpoint', 'closed transient ceiling']),
})

/**
 * The warm-up-only seed of the long-window lane: 128 KiB retained after each of the first twelve
 * samples (the 60 s to 390 s readings), 1.5 MiB in all, and nothing once the window opens. It is the
 * positive control of the first-use bound and, like every seed, never acceptance evidence.
 */
const WARMUP_SEED = Object.freeze({ mode: 'warmup1p5mib', intervals: 12, bytesPerInterval: 131072 })

const MINUTE_MS = 60000
const RECORDS = 'telemetry-seeded-rows.jsonl'
const VERDICT = 'telemetry-seeded-verdict.json'

/** Retains the seeded buffers of one worker through a root the product keeps alive. */
class LiveSeed {
  /**
   * @param {string|undefined} mode the value of `TELEMETRY_SEED`; empty means no seed
   * @param {{root: object, limits: {slopeBytesPerMinute: number, sampleMs: number}}} options the
   *   product object that holds the buffers, and the `TELEMETRY` limits the rate derives from
   * @returns {LiveSeed|null} the seed, or null when none is requested
   * @throws {Error} for a mode other than `slope2x` or the long-window `warmup1p5mib`
   */
  static of(mode, { root, limits } = {}) {
    if (!mode) return null
    if (mode === WARMUP_SEED.mode) return new WarmupLiveSeed(root)
    if (mode !== LIVE_SEED.mode) throw new Error(`Unknown telemetry seed mode ${mode}`)
    return new LiveSeed(root, limits)
  }

  /**
   * @param {object} root the product object that holds the buffers
   * @param {{slopeBytesPerMinute: number, sampleMs: number}} limits the slope limit and sample interval
   */
  constructor(root, { slopeBytesPerMinute, sampleMs }) {
    this.root = root
    this.rateBytesPerMinute = slopeBytesPerMinute * LIVE_SEED.factor
    this.bytesPerInterval = Math.round((this.rateBytesPerMinute * sampleMs) / MINUTE_MS)
    this.intervals = 0
  }

  /** @returns {void} retains one fresh buffer, filled so every page is touched, for one interval */
  retain() {
    this.root[LIVE_SEED.root] ??= []
    this.root[LIVE_SEED.root].push(Buffer.alloc(this.bytesPerInterval, LIVE_SEED.fill))
    this.intervals += 1
  }

  /**
   * @returns {{mode: string, rateBytesPerMinute: number, bytesPerInterval: number, intervals: number,
   *   buffers: number, bytes: number}} what was asked for and what the root actually holds
   */
  summary() {
    const held = this.root[LIVE_SEED.root] ?? []
    return {
      mode: LIVE_SEED.mode,
      rateBytesPerMinute: this.rateBytesPerMinute,
      bytesPerInterval: this.bytesPerInterval,
      intervals: this.intervals,
      buffers: held.length,
      bytes: held.reduce((sum, buffer) => sum + buffer.length, 0),
    }
  }

  /** @returns {void} drops every retained buffer from the root */
  release() {
    delete this.root[LIVE_SEED.root]
  }
}

/** The warm-up-only seed: it retains for the first `WARMUP_SEED.intervals` samples and then counts on. */
class WarmupLiveSeed extends LiveSeed {
  /** @param {object} root the product object that holds the buffers */
  constructor(root) {
    super(root, { slopeBytesPerMinute: 0, sampleMs: 0 })
    this.bytesPerInterval = WARMUP_SEED.bytesPerInterval
  }

  /** @returns {void} retains one buffer while the warm-up lasts; later samples only advance the count */
  retain() {
    if (this.intervals < WARMUP_SEED.intervals) super.retain()
    else this.intervals += 1
  }

  /** @returns {object} the base summary under the warm-up mode */
  summary() {
    return { ...super.summary(), mode: WARMUP_SEED.mode }
  }
}

/**
 * The compact facts of one worker that the control is judged on and archived with.
 * @param {object} worker a worker entry of the churn result
 * @returns {object} its verdict violations and measured values, sample count, shadow report, seed
 *   summary and exit facts; absent parts are null so the judge names them
 */
const factsOf = (worker) => ({
  kind: worker.kind,
  violations: worker.result?.violations ?? null,
  measured: worker.result?.measured ?? null,
  samples: worker.input?.samples?.length ?? null,
  shadow: worker.shadow ?? null,
  seed: worker.seed ?? null,
  exit: worker.exit ?? null,
})

/**
 * Holds a seeded live run to its role: every worker's own verdict reports the retained slope and no
 * violation the seed cannot explain, its shadow data slope is at least the seeded rate minus the
 * tolerance, the seed retained exactly one configured buffer for every judged sample, and the
 * worker exited naturally with its own verdict failed. The run's overall verdict is not consulted.
 * @param {object[]} workers the facts of every worker, from `factsOf`, in kind order
 * @param {{slopeBytesPerMinute: number, sampleMs: number, exitMs: number}} limits the `TELEMETRY` limits
 * @returns {void}
 * @throws {assert.AssertionError} naming the first worker and the part that is off its role
 */
const judgeSeededRun = (workers, limits) => {
  const { KINDS } = require('./telemetryChurn')
  assert.deepEqual(
    workers.map((worker) => worker.kind),
    [...KINDS],
    'one worker of every kind, in order'
  )
  const rate = limits.slopeBytesPerMinute * LIVE_SEED.factor
  const bytesPerInterval = Math.round((rate * limits.sampleMs) / MINUTE_MS)
  for (const { kind, violations, measured, samples, shadow, seed, exit } of workers) {
    assert(Array.isArray(violations), `${kind}: violations list`)
    assert(
      violations.includes('retained slope'),
      `${kind}: the seeded retained slope was not detected`
    )
    assert(
      Number.isFinite(measured?.slopeBytesPerMinute) &&
        measured.slopeBytesPerMinute > limits.slopeBytesPerMinute,
      `${kind}: the slope label agrees with its measured value`
    )
    const stray = violations.filter((label) => !LIVE_SEED.allowed.includes(label))
    assert.equal(stray.length, 0, `${kind}: violations the seed does not explain: ${stray}`)
    assert.equal(seed?.mode, LIVE_SEED.mode, `${kind}: seed mode`)
    assert.equal(seed.bytesPerInterval, bytesPerInterval, `${kind}: seed bytes per interval`)
    assert(
      Number.isInteger(samples) && seed.intervals === samples && seed.buffers === samples,
      `${kind}: one buffer for every sample`
    )
    assert.equal(seed.bytes, samples * bytesPerInterval, `${kind}: retained bytes`)
    assert(
      shadow?.scope === 'shadow' && shadow.acceptance === false && shadow.complete === true,
      `${kind}: a complete shadow report`
    )
    assert(
      shadow.data?.slopeBytesPerMinute >= rate * (1 - LIVE_SEED.toleranceFraction),
      `${kind}: shadow data slope ${shadow.data?.slopeBytesPerMinute} is below the seeded rate ${rate} less ${LIVE_SEED.toleranceFraction * 100}%`
    )
    assert(
      exit?.event === 'close' &&
        exit.code === 1 &&
        exit.signal === null &&
        !exit.killed &&
        !exit.aborted &&
        !exit.watchdogExpired &&
        exit.stderrBytes === 0 &&
        exit.unhandled === 0 &&
        exit.receipts === 1 &&
        Number.isFinite(exit.afterDisconnectMs) &&
        exit.afterDisconnectMs >= 0 &&
        exit.afterDisconnectMs <= limits.exitMs,
      `${kind}: natural exit on its own failed verdict (code ${exit?.code}, signal ${exit?.signal}, stderr ${exit?.stderrBytes} bytes)`
    )
  }
}

/** Writes the records of one seeded run, only under `controls/` of its run directory. */
class ControlRecords {
  /**
   * @param {string} directory the absolute run directory
   * @throws {Error} when an earlier run left its records there
   */
  constructor(directory) {
    this.directory = path.join(directory, 'controls')
    fs.mkdirSync(this.directory, { recursive: true })
    fs.writeFileSync(path.join(this.directory, RECORDS), '', { flag: 'wx' })
  }

  /** @param {object} row one row of the churn run @returns {void} appended as one JSON line */
  record(row) {
    fs.appendFileSync(path.join(this.directory, RECORDS), `${JSON.stringify(row)}\n`)
  }

  /**
   * @param {{ok: boolean, failure: string|null, workers: object[]}} result whether the control passed,
   *   why not, and the `factsOf` of every worker
   * @returns {void}
   * @throws {Error} when the verdict was already written
   */
  finish(result) {
    const record = {
      control: 'telemetry-seeded',
      mode: LIVE_SEED.mode,
      acceptance: false,
      node: process.version,
      broker: process.env.KAFKA_VERSION ?? null,
      ...result,
    }
    fs.writeFileSync(path.join(this.directory, VERDICT), JSON.stringify(record, null, 2), {
      flag: 'wx',
    })
  }
}

/**
 * Requires the recorded control of this run to have passed, judging its workers again rather than
 * trusting the recorded flag, so a run that skipped the case cannot pass.
 * @param {string} directory the absolute run directory
 * @param {object} [limits] the `TELEMETRY` limits; read from the soak module when omitted
 * @returns {void}
 * @throws {Error} when no record exists, it is not a passed seeded control of this Node, or its
 *   workers no longer satisfy the judge
 */
const requireControl = (directory, limits = require('./soak').TELEMETRY) => {
  const record = JSON.parse(fs.readFileSync(path.join(directory, 'controls', VERDICT), 'utf8'))
  assert.equal(record.control, 'telemetry-seeded', 'the control record')
  assert.equal(record.mode, LIVE_SEED.mode, 'the seed mode')
  assert.equal(record.acceptance, false, 'a control is never acceptance evidence')
  assert.equal(record.node, process.version, 'the control ran on another node')
  assert.equal(record.ok, true, record.failure ?? 'the control did not pass')
  judgeSeededRun(record.workers, limits)
}

module.exports = {
  LIVE_SEED,
  WARMUP_SEED,
  LiveSeed,
  ControlRecords,
  factsOf,
  judgeSeededRun,
  requireControl,
}
