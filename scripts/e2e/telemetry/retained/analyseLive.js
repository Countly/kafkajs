#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { read } = require('./analyse')
const { verifySources } = require('../retainedDiagnostic')
const { censusSnapshot, censusComparison } = require('./census')
const { KINDS } = require('../../../soak/telemetryChurn')

/**
 * Authenticates four checkpoints per same-worker capture and preserves scalar companions.
 * @param {string} directory live diagnostic root @returns {Promise<void>} additive ledgers
 * @throws {Error} for foreign/missing identities, sources or capture bytes
 */
const main = async (directory) => {
  const summary = {
    schema: 1,
    diagnostic: true,
    acceptanceEligible: false,
    attributionComplete: false,
    analysisComplete: false,
    workers: [],
  }
  const save = () =>
    fs.writeFileSync(path.join(directory, 'census-summary.json'), JSON.stringify(summary))
  save()
  const run = JSON.parse(fs.readFileSync(path.join(directory, 'live-summary.json')))
  for (const { variant } of run.results) {
    if (!['capture', 'scalar'].includes(variant)) throw new Error('foreign census variant')
    const identity = JSON.parse(fs.readFileSync(path.join(directory, variant, 'identity.json')))
    verifySources(directory, identity.sources)
    for (const kind of KINDS) {
      const child = path.join(directory, variant, kind)
      const actual = JSON.parse(fs.readFileSync(path.join(child, 'census-identity.json')))
      if (
        actual.childId !== identity.childId ||
        actual.kind !== kind ||
        actual.variant !== variant ||
        actual.tree !== identity.tree ||
        actual.node !== identity.node ||
        actual.v8 !== identity.v8 ||
        JSON.stringify(actual.flags) !== JSON.stringify(identity.flags) ||
        actual.diagnostic !== true ||
        actual.acceptanceEligible !== false ||
        actual.attributionComplete !== false ||
        !Number.isSafeInteger(actual.pid) ||
        actual.pid <= 0
      )
        throw new Error('foreign live census identity')
      const events = fs
        .readFileSync(path.join(child, 'census-events.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .map(JSON.parse)
      if (JSON.stringify(events.map((row) => row.name)) !== '["M","first","last","closed"]')
        throw new Error('missing/reordered live census checkpoints')
      let matched
      let previous
      const checkpoints = []
      for (const event of events) {
        if (
          event.diagnostic !== true ||
          event.acceptanceEligible !== false ||
          event.attributionComplete !== false ||
          !event.journals ||
          event.processRetainedBytes !== event.reading.heapUsed + event.reading.external ||
          !Number.isFinite(event.reading.gc.observed) ||
          event.reading.gc.observed < 2 ||
          event.startedAt < event.reading.at ||
          event.reading.gc.completedAt !== event.reading.at ||
          event.endedAt < event.startedAt
        )
          throw new Error('invalid live census collection/scalar facts')
        if (variant === 'scalar') {
          if (event.file !== null || event.bytes !== 0 || event.sha256 !== null)
            throw new Error('scalar companion has capture')
          checkpoints.push(event)
          continue
        }
        if (event.file !== `${event.name}.heapsnapshot`)
          throw new Error('foreign census snapshot path')
        const current = read(path.join(child, event.file))
        if (current.bytes !== event.bytes || current.sha256 !== event.sha256)
          throw new Error('live census snapshot digest mismatch')
        const census = censusSnapshot(current)
        if (event.name === 'M') matched = census
        const output = `census-${event.name}.json`
        fs.writeFileSync(
          path.join(child, output),
          JSON.stringify({
            ...census,
            checkpoint: event,
            graphMinusProcessBytes: census.totalSelfBytes - event.processRetainedBytes,
            originalM: censusComparison(matched, census),
            consecutive: previous ? censusComparison(previous, census) : null,
          })
        )
        previous = census
        checkpoints.push({ name: event.name, output, unresolvedBytes: census.totals.unresolved })
      }
      summary.workers.push({ kind, variant, pid: actual.pid, identity: actual, checkpoints })
      save()
    }
  }
  summary.analysisComplete = run.results.length === 1 && run.results.every((row) => row.completed)
  if (!summary.analysisComplete) process.exitCode = 1
  save()
}

module.exports = { main }
if (require.main === module)
  main(process.argv[2]).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
