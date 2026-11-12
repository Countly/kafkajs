#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const Module = require('node:module')
const { execFileSync } = require('node:child_process')
const root = path.resolve(__dirname, '../..')

/**
 * One job of the long-window shadow lane: it runs the existing telemetry churn once with the frozen
 * candidate (420 s warm-up, 900 s window, traffic at the existing rate from the start) on an isolated
 * CI runner and archives what the candidate asks for. Every file carries `diagnostic: true` and
 * `acceptanceEligible: false`, the frozen parameters and the commit; the embedded verdicts of the run
 * are by construction not acceptance and are never consulted.
 */
class LongWindowDiagnostic {
  /**
   * @param {object} options
   * @param {string} options.directory the fresh output directory
   * @param {'unseeded'|'seeded'|'warmup-seed'} options.arm the arm of this job
   * @param {object} options.identity the shared record facts from `identityOf`
   * @param {object} options.modules the loaded `{ buildTelemetryChurn, PROFILES, ShadowReport, LONG_WINDOW }`
   */
  constructor({ directory, arm, identity, modules }) {
    Object.assign(this, { directory, arm, identity, modules })
  }

  /**
   * Validates the invocation, selects the candidate and its seed for this process and the workers it
   * forks, runs the job and writes its files.
   * @param {string[]} argv `['--diagnostic-only', <fresh-directory>]`
   * @param {object} [env] the environment, `process.env` by default
   * @returns {Promise<void>}
   * @throws {Error} outside diagnostic CI, for a stale directory or switch, an incomplete run, or a
   *   warm-up seed that did not trip the first-use bound; the files are written first
   */
  static async main([selector, directory], env = process.env) {
    const { LONG_WINDOW, identityOf, seedModeOf } = require('./longWindow')
    const arm = env.LONG_WINDOW_ARM
    if (
      selector !== '--diagnostic-only' ||
      !directory ||
      env.CI !== 'true' ||
      !LONG_WINDOW.arms.includes(arm) ||
      env.LIVE_CENSUS_IDENTITY ||
      env.SHARE_PEAK_DIAGNOSTIC ||
      env.TELEMETRY_SEED ||
      env[LONG_WINDOW.env]
    )
      throw new Error(
        'Use --diagnostic-only <fresh-directory> with an explicit long-window arm in CI'
      )
    fs.mkdirSync(directory, { recursive: true })
    if (fs.readdirSync(directory).length) throw new Error('long-window directory must be fresh')
    const identity = identityOf({
      arm,
      replicate: env.LONG_WINDOW_REPLICATE,
      tree: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(),
      env,
    })
    env[LONG_WINDOW.env] = '1'
    const seed = seedModeOf(arm)
    if (seed) env.TELEMETRY_SEED = seed
    LongWindowDiagnostic.aliasTestHelpers()
    const { buildTelemetryChurn, PROFILES } = require('./soak')
    const { ShadowReport } = require('./longWindow')
    const run = new LongWindowDiagnostic({
      directory,
      arm,
      identity,
      modules: { buildTelemetryChurn, PROFILES, ShadowReport, LONG_WINDOW },
    })
    await run.execute()
  }

  /** @returns {void} lets plain `node` resolve the `testHelpers` name that Jest maps */
  static aliasTestHelpers() {
    const resolve = Module._resolveFilename
    Module._resolveFilename = function (request, ...rest) {
      return resolve.call(
        this,
        request === 'testHelpers' ? path.join(root, 'testHelpers/index.js') : request,
        ...rest
      )
    }
  }

  /**
   * @param {string} name a file name in the output directory @param {object} value what it holds,
   *   with the identity first so the flags lead every file
   * @returns {void}
   */
  write(name, value) {
    fs.writeFileSync(
      path.join(this.directory, name),
      JSON.stringify({ ...this.identity, ...value })
    )
  }

  /**
   * Runs the churn, archives its failure or its result, and always closes it.
   * @returns {Promise<void>}
   * @throws {Error} the failure of the run, or an incomplete or unexpected shadow outcome
   */
  async execute() {
    const { buildTelemetryChurn, PROFILES } = this.modules
    this.write('identity.json', {})
    const owner = buildTelemetryChurn({
      brokers: ['localhost:19092', 'localhost:19095', 'localhost:19098'],
      phases: PROFILES.telemetry,
      record: (row) =>
        fs.appendFileSync(
          path.join(this.directory, 'parent.jsonl'),
          JSON.stringify({ ...this.identity, row }) + '\n'
        ),
    })
    let result
    try {
      result = await owner.run()
    } catch (error) {
      this.write('failure.json', { error: error.stack, partial: owner.evidence() })
      throw error
    } finally {
      await owner.close()
    }
    const reports = result.workers.map((worker) => this.reportOf(worker))
    this.write('long-window.json', { workers: reports })
    this.write('workers.json', { workers: result.workers.map((worker) => this.archiveOf(worker)) })
    this.write('result.json', {
      ok: result.ok,
      violations: result.violations,
      setupMs: result.setupMs,
      cleanupMs: result.cleanupMs,
      totalMs: result.totalMs,
      timeline: result.timeline,
      oracle: result.oracle,
      processes: result.archive.processes,
      handshakes: result.archive.handshakes,
    })
    this.summarise(reports)
    this.verify(reports)
  }

  /**
   * @param {object} worker a worker entry of the churn result
   * @returns {object} its shadow report; a worker without a result is an incomplete report
   */
  reportOf(worker) {
    const { ShadowReport, LONG_WINDOW } = this.modules
    const { input, facts, shadow, seed } = worker
    if (!input || !facts?.timing)
      return {
        scope: 'shadow',
        acceptance: false,
        diagnostic: true,
        kind: worker.kind,
        arm: this.arm,
        complete: false,
        error: 'no worker result',
      }
    return ShadowReport.of({
      kind: worker.kind,
      arm: this.arm,
      samples: input.samples,
      engine: shadow?.engine,
      heapSpaces: shadow?.heapSpaces,
      stages: facts.stages,
      endAt: facts.timing.phaseAt + LONG_WINDOW.warmupMs + LONG_WINDOW.measureMs,
      seed: seed ?? null,
    })
  }

  /**
   * @param {object} worker a worker entry of the churn result
   * @returns {object} what is archived of it beside its report: its judged input and embedded
   *   verdict, its exit, its engine and heap-space rows and its stage times, without the id ledgers
   */
  archiveOf(worker) {
    const { kind, uuid, exit, result, input, shadow, seed, facts } = worker
    return {
      kind,
      uuid,
      exit,
      result,
      input,
      shadow,
      seed: seed ?? null,
      facts: facts && {
        stages: facts.stages,
        timing: facts.timing,
        actions: facts.actions,
        requirements: facts.requirements,
      },
    }
  }

  /** @param {object[]} reports the worker reports @returns {void} one line per worker on stdout */
  summarise(reports) {
    const kib = (bytes) => (bytes / 1024).toFixed(1)
    for (const report of reports)
      process.stdout.write(
        report.complete
          ? `${report.kind} ${this.arm}: mid data slope ${kib(report.mid.data.slopeBytesPerMinute)} KiB/min, ` +
              `endpoint ${kib(report.mid.data.endpointBytes)} KiB, first-use ${kib(report.firstUse.dataBytes)} KiB` +
              `${report.firstUse.tripped ? ' (tripped)' : ''}${report.masked ? ', masked' : ''}\n`
          : `${report.kind} ${this.arm}: incomplete (${report.error})\n`
      )
  }

  /**
   * @param {object[]} reports the worker reports
   * @returns {void}
   * @throws {Error} for a missing or incomplete report, or, in the warm-up-seed arm, a worker whose
   *   first-use growth stayed within the bound
   */
  verify(reports) {
    const incomplete = reports.filter((report) => !report.complete)
    if (reports.length !== 4 || incomplete.length)
      throw new Error(
        `incomplete long-window reports: ${incomplete.map((r) => `${r.kind}: ${r.error}`).join('; ')}`
      )
    const quiet = reports.filter((report) => !report.firstUse.tripped)
    if (this.arm === 'warmup-seed' && quiet.length)
      throw new Error(
        `the warm-up seed did not trip the first-use bound for ${quiet.map((r) => r.kind).join(', ')}`
      )
  }
}

module.exports = { LongWindowDiagnostic }
if (require.main === module)
  LongWindowDiagnostic.main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
