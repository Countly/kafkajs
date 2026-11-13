#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const Module = require('node:module')
const { execFileSync } = require('node:child_process')
const root = path.resolve(__dirname, '../..')

/**
 * Runs the diagnostic cohort on an isolated CI runner. The enabled share worker also samples
 * allocations around reopens and cleanup; those profiled peaks include extra observer overhead.
 * The parent corpus, public reopens, reserve and GC stay fixed. Reopen completion joins the seal
 * before stopping the profile, so this diagnostic can delay stage acknowledgements.
 * Telemetry proof failures in the disabled arm remain archived, never promoted to a passing gate.
 * @param {string[]} argv explicit selector and fresh output directory
 * @returns {Promise<void>} diagnostic-only parent records, original verdict and share shadow facts
 * @throws {Error} outside diagnostic CI or on incomplete execution
 */
const main = async ([selector, directory]) => {
  const arm = process.env.SHARE_PEAK_ARM
  if (
    selector !== '--diagnostic-only' ||
    !directory ||
    process.env.CI !== 'true' ||
    !['enabled', 'disabled'].includes(arm) ||
    process.env.LIVE_CENSUS_IDENTITY ||
    process.env.TELEMETRY_SEED
  )
    throw new Error('Use --diagnostic-only <fresh-directory> with an explicit share peak arm in CI')
  fs.mkdirSync(directory, { recursive: true })
  if (fs.readdirSync(directory).length) throw new Error('share peak directory must be fresh')
  const resolve = Module._resolveFilename
  Module._resolveFilename = function (request, ...rest) {
    return resolve.call(
      this,
      request === 'testHelpers' ? path.join(root, 'testHelpers/index.js') : request,
      ...rest
    )
  }
  const { buildTelemetryChurn, PROFILES } = require('./soak')
  const { sourceStamp } = require('./telemetryChurn')
  const identity = {
    diagnostic: true,
    scope: 'shadow',
    acceptanceEligible: false,
    attributionComplete: false,
    arm,
    tree: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(),
    runId: process.env.GITHUB_RUN_ID,
    attempt: process.env.GITHUB_RUN_ATTEMPT,
    node: process.version,
    v8: process.versions.v8,
    kafka: process.env.KAFKA_VERSION,
    flags: ['--expose-gc'],
    allocationSampling: arm === 'enabled',
  }
  fs.writeFileSync(path.join(directory, 'identity.json'), JSON.stringify(identity))
  process.env.SHARE_PEAK_DIAGNOSTIC = '1'
  process.env.SHARE_PEAK_ROOT = path.resolve(directory)
  const owner = buildTelemetryChurn({
    brokers: ['localhost:19092', 'localhost:19095', 'localhost:19098'],
    phases: PROFILES.telemetry,
    record: (row) =>
      fs.appendFileSync(
        path.join(directory, 'parent.jsonl'),
        JSON.stringify({ ...row, ...identity }) + '\n'
      ),
  })
  owner.processes.script = path.join(__dirname, 'sharePeakChild.js')
  owner.source = sourceStamp(owner.processes.script)
  let result
  try {
    result = await owner.run()
    fs.writeFileSync(path.join(directory, 'result.json'), JSON.stringify({ ...identity, result }))
  } catch (error) {
    fs.writeFileSync(
      path.join(directory, 'failure.json'),
      JSON.stringify({ ...identity, error: error.stack, partial: owner.evidence() })
    )
    throw error
  } finally {
    await owner.close()
  }
  const share = result.workers.find((worker) => worker.kind === 'shareConsumer')
  const peak = share?.shadow?.sharePeak
  if (!peak || !share?.facts)
    throw new Error('missing share peak diagnostic; inspect original result')
  const telemetryFrames = share.facts.sealed.reduce((sum, pack) => {
    const transcript = JSON.parse(
      fs.readFileSync(
        path.join(process.env.TELEMETRY_EVIDENCE_DIR, 'segments', pack.transcript.name),
        'utf8'
      )
    )
    return (
      sum +
      transcript.records.filter((row) => row.type === 'request' && [71, 72].includes(row.apiKey))
        .length
    )
  }, 0)
  fs.writeFileSync(
    path.join(directory, 'share-peak.json'),
    JSON.stringify({
      ...identity,
      shadow: { sharePeak: peak },
      originalVerdict: share.result,
      acceptancePeak: {
        bytes: share.result.measured?.peak ?? null,
        firstObserved:
          peak.readings.find((row) => row.VmHWM === share.result.measured?.peak) ?? null,
        note: 'Original judged peak is unchanged; shadow.peak can be later or larger. A null join means no exact VmHWM equality, not an attributed peak.',
      },
      disabledDifferences:
        arm === 'disabled'
          ? [
              'enableMetricsPush=false; clientInstanceId and telemetry proof waits are absent',
              'no telemetry socket exists to retire; the same ordinary core operation still runs',
              'harness descriptors/replacements remain; registration into the disabled product is refused and omitted',
            ]
          : [],
      telemetryProofs: share.facts.claims,
      telemetryFrames,
      handshakes: result.archive.handshakes,
      workload: {
        actions: share.facts.actions,
        ordinary: share.facts.ordinary,
        timing: share.facts.timing,
      },
    })
  )
  if ((arm === 'disabled' && telemetryFrames !== 0) || (arm === 'enabled' && telemetryFrames === 0))
    throw new Error('share peak telemetry switch does not match the archived wire requests')
  if (arm === 'enabled') {
    const allocations = peak.allocationProfiles
    const reopens = share.facts.actions.warm.reopen + share.facts.actions.measured.reopen
    if (
      !allocations?.complete ||
      allocations.stages.filter((entry) => entry.stage === 'reopen').length !== reopens
    )
      throw new Error('incomplete share allocation profiles; inspect original result and artifacts')
  }
  if (
    !peak.complete ||
    !share.facts.requirements.ok ||
    !result.oracle?.ok ||
    Object.values(result.archive.processes).some(
      (exit) => exit.code !== 0 || exit.killed || exit.aborted || exit.watchdogExpired
    )
  )
    throw new Error('incomplete share peak diagnostic; inspect original result and shadow errors')
}

module.exports = { main }
if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
