#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { readSnapshot, analyseSnapshot } = require('./heapDiff')
const { censusSnapshot, censusComparison } = require('./census')
const { topOwners } = require('./activeCensus')
const { CAPACITY, digest } = require('./contract')
const { FLAGS } = require('./closeSnapshot')
const { verifySources } = require('../retainedDiagnostic')
const { dataBytes } = require('../../../../testHelpers/processMemory')

/**
 * Reconciles self bytes and traces disappeared/shrunk nodes in M, growth in closed.
 * @param {object} before M census @param {object} after closed census
 * @param {object} beforeGraph M graph @param {object} afterGraph closed graph
 * @returns {object} disjoint category ledger and representative constructor/owner paths
 * @throws {Error} on unstable or unreconciled self-byte accounting
 */
const analysePair = (before, after, beforeGraph, afterGraph) => {
  const comparison = censusComparison(before, after)
  const old = new Map(before.nodes.map((node) => [node.id, node.selfBytes]))
  const next = new Map(after.nodes.map((node) => [node.id, node.selfBytes]))
  const disappearedIds = new Set(comparison.released.map((node) => node.id))
  const shrunk = {
    nodes: before.nodes.map((node) => ({
      ...node,
      selfBytes: next.has(node.id) ? Math.max(0, node.selfBytes - next.get(node.id)) : 0,
    })),
  }
  const grown = {
    nodes: after.nodes.map((node) => ({
      ...node,
      selfBytes: Math.max(0, node.selfBytes - (old.get(node.id) ?? 0)),
    })),
  }
  const owners = (point, graph, ids) => {
    const constructors = topOwners(point, graph, ids)
    const labelled = { nodes: point.nodes.map((node) => ({ ...node, name: node.label })) }
    return { constructors, owners: topOwners(labelled, graph, ids) }
  }
  const disappeared = owners(before, beforeGraph, disappearedIds)
  const shrinkage = owners(
    shrunk,
    beforeGraph,
    new Set(shrunk.nodes.filter((node) => node.selfBytes > 0).map((node) => node.id))
  )
  const growth = owners(
    grown,
    afterGraph,
    new Set(grown.nodes.filter((node) => node.selfBytes > 0).map((node) => node.id))
  )
  return {
    ...FLAGS,
    delta: comparison.delta,
    categories: comparison.categories,
    releasedSelfBytes: comparison.released.reduce((sum, node) => sum + node.selfBytes, 0),
    shrunkSelfBytes: shrunk.nodes.reduce((sum, node) => sum + node.selfBytes, 0),
    grownSelfBytes: grown.nodes.reduce((sum, node) => sum + node.selfBytes, 0),
    disappeared: disappeared.constructors,
    disappearedOwners: disappeared.owners,
    shrunk: shrinkage.constructors,
    shrunkOwners: shrinkage.owners,
    grown: growth.constructors,
    grownOwners: growth.owners,
    interpretation:
      'Stable IDs reconcile additive self bytes, including resizing and category transfers. Runtime is the engine bucket. Buffers and unresolved data stay separate. Disappeared/shrunk paths are from M; growth paths are from closed. Paths prove reachability at snapshot time, not allocation stacks or exclusive ownership. Snapshots collect and allocate, so this pair cannot identify garbage removed by the M capture itself or explain every byte of the original scalar decrease. No diagnostic bytes are subtracted from acceptance.',
  }
}

/**
 * Authenticates the exact original reading seams, identity, order and snapshot digests.
 * @param {string} directory child directory @param {object} result joined child output
 * @returns {object[]} M and closed events for capture or scalar mode
 * @throws {Error} on incomplete, foreign, reordered or modified evidence
 */
const validateChild = (directory, result) => {
  const points = result.readings?.closeSnapshot
  if (
    !result.complete ||
    result.readings?.diagnostic !== true ||
    result.readings?.acceptanceEligible !== false ||
    points?.diagnostic !== true ||
    points.acceptanceEligible !== false ||
    points.attributionComplete !== false ||
    points.complete !== true ||
    points.records?.length !== 2
  )
    throw new Error('incomplete closed snapshot child')
  const identity = JSON.parse(fs.readFileSync(path.join(directory, 'census-identity.json')))
  for (const key of ['childId', 'tree', 'runId', 'kind', 'node', 'v8', 'variant'])
    if (identity[key] !== result.identity[key]) throw new Error(`foreign closed snapshot ${key}`)
  if (
    !['capture', 'scalar'].includes(identity.variant) ||
    identity.seed !== (result.identity.seed === 'close' ? 'close' : null) ||
    identity.diagnostic !== true ||
    identity.acceptanceEligible !== false ||
    identity.attributionComplete !== false ||
    identity.pid !== result.pid ||
    JSON.stringify(identity.flags) !== JSON.stringify(result.readings.manifest.execArgv)
  )
    throw new Error('foreign closed snapshot identity')
  const filename = path.join(directory, 'census-events.jsonl')
  if (fs.statSync(filename).size > CAPACITY.eventBytes) throw new Error('close event overflow')
  const events = fs.readFileSync(filename, 'utf8').trim().split('\n').map(JSON.parse)
  if (events.length !== 2) throw new Error('missing closed snapshot events')
  let previousEnd = -1
  for (const [index, event] of events.entries()) {
    const name = ['M', 'closed'][index]
    const record = points.records[index]
    const { registry, ...original } = result.readings[index === 0 ? 'matched' : 'closed']
    if (
      event.name !== name ||
      event.diagnostic !== true ||
      event.acceptanceEligible !== false ||
      event.attributionComplete !== false ||
      JSON.stringify(event.reading) !== JSON.stringify(original) ||
      ['name', 'reading', 'countersBefore', 'processRetainedBytes'].some(
        (key) => JSON.stringify(event[key]) !== JSON.stringify(record[key])
      ) ||
      !Number.isFinite(event.startedAt) ||
      !Number.isFinite(event.endedAt) ||
      event.startedAt < original.at ||
      event.startedAt < previousEnd ||
      event.endedAt < event.startedAt ||
      !Number.isFinite(record.afterSnapshot?.at) ||
      record.afterSnapshot.at < event.endedAt ||
      event.processRetainedBytes !== original.retained ||
      !Array.isArray(event.countersBefore?.spaces) ||
      !record.countersAfter?.heap
    )
      throw new Error('closed snapshot does not follow original reading')
    previousEnd = record.afterSnapshot.at
    if (identity.variant === 'capture') {
      if (
        event.file !== `${name}.heapsnapshot` ||
        event.bytes <= 0 ||
        event.bytes > CAPACITY.snapshotBytes
      )
        throw new Error('invalid closed snapshot path/bounds')
      const target = path.join(directory, event.file)
      if (
        fs.lstatSync(target).isSymbolicLink() ||
        fs.statSync(target).size !== event.bytes ||
        digest(fs.readFileSync(target)) !== event.sha256
      )
        throw new Error('closed snapshot digest mismatch')
    } else if (event.file !== null || event.bytes !== 0 || event.sha256 !== null)
      throw new Error('scalar companion contains a snapshot')
  }
  return events
}

/**
 * @param {string} filename summary destination @param {object} value diagnostic results
 * @returns {void} bounded display paths and labels; full graphs remain in snapshots
 */
const writeSummary = (filename, value) => {
  fs.writeFileSync(
    filename,
    JSON.stringify(value, (key, entry) => {
      if (key === 'rootPath' && entry.length > 32)
        return [...entry.slice(0, 4), { omittedPathNodes: entry.length - 32 }, ...entry.slice(-28)]
      if (typeof entry === 'string' && entry.length > 512)
        return `${entry.slice(0, 480)}…[${entry.length - 480} characters omitted]`
      return entry
    })
  )
}

/**
 * @param {string} directory diagnostic root @returns {void} authenticated per-child and aggregate analysis
 * @throws {Error} after preserving partial evidence if any child or source is incomplete
 */
const main = (directory) => {
  const run = JSON.parse(fs.readFileSync(path.join(directory, 'close-summary.json')))
  verifySources(directory, run.sources)
  if (
    run.diagnostic !== true ||
    run.acceptanceEligible !== false ||
    run.attributionComplete !== false
  )
    throw new Error('foreign closed snapshot run')
  const summary = { schema: 1, ...FLAGS, analysisComplete: false, children: [] }
  for (const kind of ['producer', 'consumer']) {
    for (const seed of ['unseeded', 'close']) {
      const name = `${kind}-${seed}`
      const child = path.join(directory, name)
      const row = { ...FLAGS, child: name, analysisComplete: false }
      try {
        const result = JSON.parse(fs.readFileSync(path.join(child, 'result.json')))
        const entry = run.results.find((item) => item.kind === kind && item.seed === seed)
        if (!entry?.complete || entry.childId !== result.identity.childId)
          throw new Error('foreign close child catalogue')
        for (const key of ['tree', 'runId', 'node', 'v8', 'variant'])
          if (result.identity[key] !== run[key]) throw new Error(`foreign close run ${key}`)
        if (result.identity.kind !== kind || result.identity.seed !== seed)
          throw new Error('foreign closed snapshot arm')
        const events = validateChild(child, result)
        const { matched, closed } = result.readings
        row.scalarDelta = {
          data: dataBytes(closed) - dataBytes(matched),
          heapUsed: closed.heapUsed - matched.heapUsed,
          external: closed.external - matched.external,
          retained: closed.retained - matched.retained,
          bytecode:
            closed.gc.checkpoints.at(-1).engine.bytecode -
            matched.gc.checkpoints.at(-1).engine.bytecode,
          code:
            closed.gc.checkpoints.at(-1).engine.code - matched.gc.checkpoints.at(-1).engine.code,
        }
        row.plantedAfterM = result.readings.seeds.measured?.afterMatched ?? 0
        row.observations = result.readings.closeSnapshot.records
        if (run.variant === 'capture') {
          const beforeRaw = readSnapshot(path.join(child, events[0].file))
          const beforeGraph = analyseSnapshot(beforeRaw)
          const before = censusSnapshot({ ...beforeRaw, graph: beforeGraph })
          const afterRaw = readSnapshot(path.join(child, events[1].file))
          const afterGraph = analyseSnapshot(afterRaw)
          const after = censusSnapshot({ ...afterRaw, graph: afterGraph })
          row.attribution = analysePair(before, after, beforeGraph, afterGraph)
        }
        row.analysisComplete = true
      } catch (error) {
        row.failure = error.message
      }
      if (fs.existsSync(child)) writeSummary(path.join(child, 'close-analysis.json'), row)
      summary.children.push(row)
    }
  }
  summary.analysisComplete = run.complete && summary.children.every((row) => row.analysisComplete)
  writeSummary(path.join(directory, 'close-analysis.json'), summary)
  console.table(
    summary.children.map((row) => ({
      child: row.child,
      complete: row.analysisComplete,
      dataDelta: row.scalarDelta?.data,
      selfDelta: row.attribution?.delta,
      released: row.attribution?.releasedSelfBytes,
      failure: row.failure,
    }))
  )
  if (!summary.analysisComplete)
    throw new Error('incomplete closed snapshot analysis; partial evidence preserved')
}

module.exports = { analysePair, validateChild, main }
if (require.main === module)
  try {
    main(process.argv[2])
  } catch (error) {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  }
