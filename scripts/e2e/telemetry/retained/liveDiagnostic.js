#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const Module = require('node:module')
const crypto = require('node:crypto')
const { execFileSync } = require('node:child_process')
const { archiveSources, verifySources } = require('../retainedDiagnostic')
const root = path.resolve(__dirname, '../../../..')

/**
 * Runs one four-worker cohort; capture and scalar companions use separate CI jobs.
 * Uses the original workload, phase, sample grid, GC and supervision; scalar failures survive.
 * @param {string[]} argv explicit diagnostic selector and fresh output directory
 * @returns {Promise<void>} archived identities, checkpoint records and parent results
 * @throws {Error} outside CI or on incomplete supervised work; memory verdicts are evidence only
 */
const main = async ([selector, directory, variant]) => {
  if (
    selector !== '--diagnostic-only' ||
    !directory ||
    !['capture', 'scalar'].includes(variant) ||
    process.env.CI !== 'true' ||
    process.env.TELEMETRY_RESOURCES_ARTIFACT_DIR
  )
    throw new Error('Use --diagnostic-only <new-directory> <capture|scalar> in isolated CI')
  fs.mkdirSync(directory, { recursive: true })
  if (fs.readdirSync(directory).length) throw new Error('live census directory must be fresh')
  const sources = archiveSources(directory)
  verifySources(directory, sources)
  const tree = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim()
  const resolve = Module._resolveFilename
  Module._resolveFilename = function (request, ...rest) {
    return resolve.call(
      this,
      request === 'testHelpers' ? path.join(root, 'testHelpers/index.js') : request,
      ...rest
    )
  }
  const { buildTelemetryChurn, PROFILES } = require('../../../soak/soak')
  const { sourceStamp } = require('../../../soak/telemetryChurn')
  const results = []
  const target = path.join(directory, variant)
  fs.mkdirSync(target)
  const identity = {
    schema: 1,
    diagnostic: true,
    acceptanceEligible: false,
    attributionComplete: false,
    childId: crypto.randomUUID(),
    tree,
    runId: process.env.GITHUB_RUN_ID,
    attempt: process.env.GITHUB_RUN_ATTEMPT,
    node: process.version,
    v8: process.versions.v8,
    flags: censusFlags(),
    kafka: process.env.KAFKA_VERSION,
    variant,
    sources,
  }
  const identityFile = path.join(target, 'identity.json')
  fs.writeFileSync(identityFile, JSON.stringify(identity))
  process.env.LIVE_CENSUS_IDENTITY = identityFile
  process.env.LIVE_CENSUS_DIRECTORY = target
  const owner = buildTelemetryChurn({
    brokers: ['localhost:19092', 'localhost:19095', 'localhost:19098'],
    phases: PROFILES.telemetry,
    record: (row) =>
      fs.appendFileSync(
        path.join(target, 'parent.jsonl'),
        JSON.stringify({ diagnostic: true, acceptanceEligible: false, ...row }) + '\n'
      ),
  })
  owner.processes.script = path.join(__dirname, 'liveChild.js')
  owner.source = sourceStamp(owner.processes.script)
  try {
    const result = await owner.run()
    fs.writeFileSync(path.join(target, 'result.json'), JSON.stringify({ ...identity, result }))
    results.push({ variant, completed: true, scalarVerdict: result.ok })
  } catch (error) {
    fs.writeFileSync(
      path.join(target, 'failure.json'),
      JSON.stringify({ ...identity, message: error.stack })
    )
    results.push({ variant, completed: false, error: error.message })
  } finally {
    await owner.close()
  }
  fs.writeFileSync(
    path.join(directory, 'live-summary.json'),
    JSON.stringify({
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      results,
    })
  )
  if (results.some((row) => !row.completed)) process.exitCode = 1
}

/**
 * The V8 flags the census workers are spawned with: the five-minute telemetry worker flags.
 * @returns {string[]} a copy of `WORKER_FLAGS`
 */
const censusFlags = () => [...require('../../../soak/telemetryChurn').WORKER_FLAGS]

module.exports = { main, censusFlags }
if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
