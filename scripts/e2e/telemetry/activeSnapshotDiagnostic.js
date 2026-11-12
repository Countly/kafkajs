#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { execFileSync } = require('node:child_process')
const { MEMORY_FLAGS } = require('../../../testHelpers/processMemory')
const { KINDS } = require('./resourceArtifact')
const { archiveSources, verifySources } = require('./retainedDiagnostic')
const { supervise } = require('./activeDiagnostic')

const ROOT = path.resolve(__dirname, '../../..')
const REPETITIONS = 3

/**
 * Runs twelve serial public scalar children with post-reading triggered snapshots in isolated CI.
 * @param {string[]} argv explicit selector and fresh evidence directory
 * @returns {Promise<void>} original scalar evidence, at most two snapshots per child, source archive
 * @throws {Error} outside isolated CI, on source mutation or incomplete natural exit
 */
const main = async ([selector, directory]) => {
  if (
    selector !== '--diagnostic-only' ||
    !directory ||
    process.env.CI !== 'true' ||
    process.env.TELEMETRY_RESOURCES_ARTIFACT_DIR
  )
    throw new Error('Use --diagnostic-only <new-directory> in isolated CI')
  fs.mkdirSync(directory, { recursive: true })
  if (fs.readdirSync(directory).length) throw new Error('active snapshot directory must be fresh')
  const sources = archiveSources(directory)
  verifySources(directory, sources)
  const identity = {
    schema: 1,
    diagnostic: true,
    acceptanceEligible: false,
    attributionComplete: false,
    variant: 'snapshot',
    tree: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8' }).trim(),
    runId: process.env.GITHUB_RUN_ID,
    attempt: process.env.GITHUB_RUN_ATTEMPT,
    node: process.version,
    v8: process.versions.v8,
    flags: MEMORY_FLAGS,
  }
  const summary = { ...identity, sources, repetitions: REPETITIONS, results: [], complete: false }
  const save = () =>
    fs.writeFileSync(path.join(directory, 'snapshot-summary.json'), JSON.stringify(summary))
  save()
  for (const kind of KINDS) {
    for (let repetition = 1; repetition <= REPETITIONS; repetition++) {
      const child = `${kind}-${repetition}`
      const result = await supervise(path.join(directory, child), {
        ...identity,
        arm: 'public',
        kind,
        repetition,
        childId: crypto.randomUUID(),
      })
      summary.results.push({ ...result, child, repetition })
      verifySources(directory, sources)
      save()
    }
  }
  summary.complete = summary.results.every((row) => row.complete)
  save()
  if (!summary.complete)
    throw new Error('incomplete active snapshot diagnostic; evidence preserved')
}

module.exports = { main, REPETITIONS }
if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
