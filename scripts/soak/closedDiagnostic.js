'use strict'

const fs = require('node:fs')
const path = require('node:path')
const v8 = require('node:v8')

const FLAGS = Object.freeze({ diagnostic: true, acceptanceEligible: false })
/** Engine code journal rows: the warm capture's extra settles overflow the 64-row default. */
const ENGINE_ROWS = 128

/** Three endpoint captures, written synchronously only AFTER the original settled heap reading. */
class ClosedCapture {
  /**
   * @param {object} options
   * @param {string} options.directory fresh worker directory
   * @param {string} options.kind client kind
   * @param {object} [options.engine] V8 capture functions, injectable in broker-free tests
   */
  constructor({ directory, kind, engine = v8 }) {
    Object.assign(this, { directory, kind, engine })
    fs.mkdirSync(directory, { recursive: true })
    this.seen = new Set()
  }

  /**
   * Does not collect or settle before recording statistics. Snapshot creation itself perturbs V8;
   * these observations can never substitute for acceptance readings.
   * @param {'baseline'|'warm'|'teardown'} checkpoint original endpoint or experimental baseline
   * @param {object} snapshot already collected and settled probe reading
   * @returns {void} writes statistics before taking the corresponding snapshot
   * @throws {Error} on duplicate checkpoints, missing GC evidence or file/capture failure
   */
  record(checkpoint, snapshot) {
    if (this.seen.has(checkpoint)) throw new Error(`duplicate closed checkpoint ${checkpoint}`)
    if (!(snapshot.memory?.gc?.observed >= 2)) throw new Error('closed capture needs settled GCs')
    this.seen.add(checkpoint)
    const row = {
      ...FLAGS,
      kind: this.kind,
      checkpoint,
      node: process.version,
      v8: process.versions.v8,
      memory: snapshot.memory,
      heapSpaces: this.engine.getHeapSpaceStatistics(),
      code: this.engine.getHeapCodeStatistics(),
    }
    fs.writeFileSync(path.join(this.directory, `${checkpoint}.json`), JSON.stringify(row), {
      flag: 'wx',
    })
    this.engine.writeHeapSnapshot(path.join(this.directory, `${checkpoint}.heapsnapshot`))
  }
}

/**
 * Installs a private Soak subclass in the diagnostic child only. With the switch off this returns
 * before allocating observers, changing options or reading memory. No production module is patched.
 * @param {object} worker TelemetryWorkerProcess with injectable soak dependencies
 * @param {object} [env] explicit CI switch and absolute output root
 * @param {object} [ports] injected capture dependencies for broker-free controls
 * @returns {void}
 * @throws {Error} for an enabled invocation outside isolated diagnostic CI
 */
const installClosedDiagnostic = (worker, env = process.env, ports = {}) => {
  if (env.TELEMETRY_CLOSED_DIAGNOSTIC !== '1') return
  if (
    env.CI !== 'true' ||
    !path.isAbsolute(env.TELEMETRY_CLOSED_ROOT || '') ||
    env.TELEMETRY_LONG_WINDOW ||
    env.TELEMETRY_SEED ||
    env.LIVE_CENSUS_IDENTITY ||
    env.SHARE_PEAK_DIAGNOSTIC
  )
    throw new Error('closed capture requires explicit diagnostic CI and an absolute output root')
  const capture = new ClosedCapture({
    directory: path.join(env.TELEMETRY_CLOSED_ROOT, worker.kind),
    kind: worker.kind,
    ...ports,
  })
  const Original = worker.soak.Soak
  /** Opt-in capture seam; ordinary workers never load or instantiate it. */
  class ClosedSoak extends Original {
    /** @param {object} options unchanged Soak options @returns {ClosedSoak} capture-enabled run */
    constructor(options) {
      super({
        ...options,
        record: (row) => {
          if (['baseline', 'teardown'].includes(row.kind)) capture.record(row.kind, row.snapshot)
          options.record(row)
        },
      })
      const churn = options.context.telemetryChurn
      // Named aliases expose existing owners, not copies of their growing collections.
      globalThis.__telemetryClosedRoots = {
        client: churn.client,
        registry: churn.registry,
        harness: churn,
      }
      this.warmCaptured = false
      const snapshot = options.probe.snapshot.bind(options.probe)
      this.probe = {
        snapshot: async () => {
          const value = await snapshot()
          if (
            !this.warmCaptured &&
            this.phaseStartedAt !== null &&
            this.clock.now() - this.phaseStartedAt >= this.warmupMs
          ) {
            if (this.clock.now() - this.phaseStartedAt > this.warmupMs + 5000)
              throw new Error('closed diagnostic missed the warm baseline')
            this.warmCaptured = true
            await this.settle()
            capture.record('warm', await snapshot())
          }
          return value
        },
      }
    }
  }
  worker.soak = {
    ...worker.soak,
    Soak: ClosedSoak,
    TELEMETRY: { ...worker.soak.TELEMETRY, ...FLAGS, engineRows: ENGINE_ROWS },
  }
}

module.exports = { FLAGS, ClosedCapture, installClosedDiagnostic }
