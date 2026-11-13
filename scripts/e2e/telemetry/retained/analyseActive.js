#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { verifySources } = require('../retainedDiagnostic')
const { KINDS } = require('../resourceArtifact')
const { CAPACITY, digest } = require('./contract')
const { readSnapshot, analyseSnapshot } = require('./heapDiff')
const { censusSnapshot, censusComparison } = require('./census')
const { topOwners, validatePoint } = require('./activeCensus')

/**
 * Authenticates bounded same-process captures and reuses the retained graph/census tooling offline.
 * @param {string} directory diagnostic root @returns {void} full ledgers and top retaining paths
 * @throws {Error} on incomplete, foreign, reordered or changed evidence; never emits acceptance
 */
const main = (directory) => {
  const run = JSON.parse(fs.readFileSync(path.join(directory, 'active-summary.json')))
  const summary = {
    schema: 1,
    diagnostic: true,
    acceptanceEligible: false,
    attributionComplete: false,
    analysisComplete: false,
    children: [],
  }
  const save = () =>
    fs.writeFileSync(path.join(directory, 'active-ownership.json'), JSON.stringify(summary))
  save()
  verifySources(directory, run.sources)
  if (
    run.diagnostic !== true ||
    run.acceptanceEligible !== false ||
    run.attributionComplete !== false ||
    !run.complete ||
    !['capture', 'scalar'].includes(run.variant) ||
    run.results.length !== KINDS.length
  )
    throw new Error('incomplete active diagnostic catalogue')
  for (const [index, kind] of KINDS.entries()) {
    const child = path.join(directory, kind)
    const result = JSON.parse(fs.readFileSync(path.join(child, 'result.json')))
    const identity = JSON.parse(fs.readFileSync(path.join(child, 'census-identity.json')))
    for (const key of [
      'diagnostic',
      'acceptanceEligible',
      'attributionComplete',
      'tree',
      'runId',
      'node',
      'v8',
      'variant',
    ])
      if (identity[key] !== run[key]) throw new Error(`foreign active census ${key}`)
    if (
      !result.complete ||
      run.results[index].kind !== kind ||
      identity.kind !== kind ||
      identity.childId !== run.results[index].childId ||
      identity.childId !== result.identity.childId ||
      identity.pid !== result.pid ||
      JSON.stringify(identity.flags) !== JSON.stringify(run.flags)
    )
      throw new Error('foreign active child identity')
    const file = path.join(child, 'census-events.jsonl')
    if (fs.statSync(file).size > CAPACITY.eventBytes) throw new Error('active event overflow')
    const events = fs.readFileSync(file, 'utf8').trim().split('\n').map(JSON.parse)
    const coverage = result.readings.activeCensus?.checkpointCoverage
    const codecs = ['gzip', ...(result.readings.manifest.codecs.zstd ? ['zstd'] : [])]
    if (
      !Array.isArray(coverage) ||
      JSON.stringify(coverage.map((row) => row.codec)) !== JSON.stringify(codecs) ||
      coverage.some((row) => typeof row.pendingCheckpointCaptured !== 'boolean')
    )
      throw new Error('missing original active checkpoint coverage')
    const expected = [
      'M',
      ...coverage.flatMap(({ codec, pendingCheckpointCaptured }) => [
        codec,
        ...(pendingCheckpointCaptured ? [`${codec}-checkpoint`] : []),
      ]),
    ]
    if (
      events[0]?.name !== 'M' ||
      JSON.stringify(events.map((row) => row.name).sort()) !== JSON.stringify(expected.sort())
    )
      throw new Error('missing/reordered active checkpoints')
    let matched
    const checkpoints = []
    let previousEnd = -1
    let previousCodec = -1
    for (const event of events) {
      const codecIndex = codecs.indexOf(event.name.replace(/-checkpoint$/, ''))
      if (event.reading.at < previousEnd || codecIndex < previousCodec)
        throw new Error('active checkpoint chronology changed')
      previousEnd = event.endedAt
      previousCodec = codecIndex
      validatePoint(event, {
        variant: run.variant,
        matched: result.readings.matched,
        native: result.readings.native.rows.find((row) => row.id === event.pending?.[0]?.id),
      })
      if (run.variant === 'scalar') {
        if (event.file !== null || event.bytes !== 0 || event.sha256 !== null)
          throw new Error('scalar companion has capture')
        checkpoints.push(event)
        continue
      }
      if (
        event.file !== `${event.name}.heapsnapshot` ||
        event.bytes <= 0 ||
        event.bytes > CAPACITY.snapshotBytes
      )
        throw new Error('invalid active snapshot bounds/path')
      const target = path.join(child, event.file)
      if (
        fs.lstatSync(target).isSymbolicLink() ||
        fs.statSync(target).size !== event.bytes ||
        digest(fs.readFileSync(target)) !== event.sha256
      )
        throw new Error('active snapshot digest mismatch')
      const raw = readSnapshot(target)
      const graph = analyseSnapshot(raw)
      const census = censusSnapshot({ ...raw, graph })
      if (event.name === 'M') matched = census
      const owners = topOwners(census, graph)
      const originalM = censusComparison(matched, census)
      const newOwners = topOwners(
        census,
        graph,
        new Set(originalM.allocated.map((node) => node.id))
      )
      const output = `census-${event.name}.json`
      fs.writeFileSync(
        path.join(child, output),
        JSON.stringify({
          ...census,
          checkpoint: event,
          originalM,
          topOwners: owners,
          newOwners,
        })
      )
      checkpoints.push({
        name: event.name,
        output,
        raw: event.raw ?? null,
        afterMajorGc: event.afterMajorGc ?? null,
        rawMinusM: event.rawMinusM ?? 0,
        afterMajorGcMinusM: event.afterMajorGcMinusM ?? null,
        totals: census.totals,
        topOwners: owners,
        newOwners,
      })
    }
    summary.children.push({ kind, identity, checkpointCoverage: coverage, checkpoints })
    save()
  }
  summary.analysisComplete = true
  save()
}

module.exports = { main }
if (require.main === module)
  try {
    main(process.argv[2])
  } catch (error) {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  }
