#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { execFileSync } = require('node:child_process')
const { FLAGS } = require('./closedDiagnostic')

/**
 * Runs the unchanged five-minute churn on Kafka 4.3.1 with a diagnostic-only child. Embedded
 * acceptance failures are archived unchanged. Functional failures remain fatal to this job.
 * @param {string[]} argv explicit diagnostic selector and fresh absolute output directory
 * @param {object} [env] CI environment with explicit capture switch
 * @returns {Promise<void>} writes compact run facts for offline analysis, never scenario evidence
 * @throws {Error} outside diagnostic CI, on mixed experiments, stale output or failed execution
 */
const main = async ([selector, directory], env = process.env) => {
  if (
    selector !== '--diagnostic-only' ||
    !path.isAbsolute(directory || '') ||
    env.CI !== 'true' ||
    env.TELEMETRY_CLOSED_DIAGNOSTIC !== '1' ||
    env.KAFKA_VERSION !== '4.3.1' ||
    env.TELEMETRY_LONG_WINDOW ||
    env.TELEMETRY_SEED ||
    env.LIVE_CENSUS_IDENTITY ||
    env.SHARE_PEAK_DIAGNOSTIC
  )
    throw new Error(
      'Use --diagnostic-only <fresh absolute directory> in explicit closed diagnostic CI'
    )
  fs.mkdirSync(directory, { recursive: true })
  if (fs.readdirSync(directory).length) throw new Error('closed diagnostic directory must be fresh')
  env.TELEMETRY_CLOSED_ROOT = directory
  const identity = {
    ...FLAGS,
    tree: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
    runId: env.GITHUB_RUN_ID,
    attempt: env.GITHUB_RUN_ATTEMPT,
    node: process.version,
    v8: process.versions.v8,
    kafka: env.KAFKA_VERSION,
    workerFlags: ['--expose-gc'],
    warmupMs: 60000,
    measurementMs: 300000,
  }
  fs.writeFileSync(path.join(directory, 'identity.json'), JSON.stringify(identity))
  const { LongWindowDiagnostic } = require('./longWindowDiagnostic')
  LongWindowDiagnostic.aliasTestHelpers()
  const { buildTelemetryChurn, PROFILES } = require('./soak')
  const { sourceStamp, KINDS } = require('./telemetryChurn')
  const owner = buildTelemetryChurn({
    brokers: ['localhost:19092', 'localhost:19095', 'localhost:19098'],
    phases: PROFILES.telemetry,
    record: () => {},
  })
  owner.processes.script = path.join(__dirname, 'closedChild.js')
  owner.source = sourceStamp(owner.processes.script)
  let result
  try {
    result = await owner.run()
  } catch (error) {
    fs.writeFileSync(
      path.join(directory, 'summary.json'),
      JSON.stringify({ ...identity, complete: false, error: error.stack })
    )
    throw error
  } finally {
    await owner.close()
  }
  const workers = result.workers.map(({ kind, input, result: verdict, facts, exit }) => ({
    kind,
    diagnostic: input?.diagnostic,
    acceptanceEligible: input?.acceptanceEligible,
    verdict,
    requirements: facts?.requirements,
    timing: facts?.timing,
    sampler: input?.sampler,
    exit,
  }))
  const complete =
    KINDS.every((kind) =>
      workers.some(
        (worker) =>
          worker.kind === kind &&
          worker.diagnostic === true &&
          worker.acceptanceEligible === false &&
          worker.requirements?.ok === true &&
          worker.requirements?.violations?.length === 0 &&
          worker.sampler?.skipped?.length === 0 &&
          worker.sampler?.readFailures?.length === 0 &&
          worker.exit?.code === 0 &&
          worker.exit?.event === 'close' &&
          worker.exit?.receipts === 1 &&
          worker.exit?.signal === null &&
          worker.exit?.stderrBytes === 0 &&
          worker.exit?.unhandled === 0 &&
          Number.isFinite(worker.exit?.afterDisconnectMs) &&
          worker.exit.afterDisconnectMs >= 0 &&
          worker.exit.afterDisconnectMs <= 10000 &&
          !worker.exit?.killed &&
          !worker.exit?.aborted &&
          !worker.exit?.watchdogExpired
      )
    ) &&
    result.oracle?.ok === true &&
    result.oracle?.violations?.length === 0
  fs.writeFileSync(
    path.join(directory, 'summary.json'),
    JSON.stringify({
      ...identity,
      complete,
      originalVerdict: { ok: result.ok, violations: result.violations },
      oracle: result.oracle,
      workers,
    })
  )
  if (!complete) throw new Error('closed diagnostic has incomplete or failed functional evidence')
}

module.exports = { main }
if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    console.error(error)
    process.exitCode = 1
  })
