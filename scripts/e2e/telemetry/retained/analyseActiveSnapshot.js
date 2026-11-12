#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { readSnapshot, analyseSnapshot } = require('./heapDiff')
const { censusSnapshot, censusComparison } = require('./census')
const { topOwners } = require('./activeCensus')
const { CAPACITY, digest } = require('./contract')
const { verifySources } = require('../retainedDiagnostic')
const { KINDS } = require('../resourceArtifact')
const { REPETITIONS } = require('../activeSnapshotDiagnostic')
const { THRESHOLD } = require('./activeSnapshot')

const FLAGS = { diagnostic: true, acceptanceEligible: false, attributionComplete: false }
const ROOT = path.resolve(__dirname, '../../../..')
const PRODUCT_FILES = {
  TelemetryPayload: 'src/telemetry/payload.js',
  TelemetryMetrics: 'src/telemetry/metrics.js',
  ClientTelemetry: 'src/telemetry/client.js',
  TelemetryFactory: 'src/telemetry/factory.js',
  ProtoWriter: 'src/telemetry/otlp.js',
  MetricsDataEncoding: 'src/telemetry/otlp.js',
  Connection: 'src/network/connection.js',
  RequestQueue: 'src/network/requestQueue/index.js',
  Broker: 'src/broker/index.js',
  Cluster: 'src/cluster/index.js',
  ShareRun: 'src/consumer/share/shareRun.js',
  ShareSession: 'src/consumer/share/shareSession.js',
  ShareFetchLoop: 'src/consumer/share/shareFetchLoop.js',
  ShareMembership: 'src/consumer/share/shareMembership.js',
}

/**
 * Resolves a named product owner field against the archived executed source, never an allocation stack.
 * @param {object[]} rootPath ordinary strong path @param {string} sourceRoot archived repository root
 * @returns {object[]} observed product fields; unknown or optimized fields keep a null source
 */
const productFields = (rootPath, sourceRoot) => {
  const owners = []
  for (let index = 0; index < rootPath.length - 1; index++) {
    const owner = rootPath[index].node
    const file = PRODUCT_FILES[owner.name]
    if (!file) continue
    const field = String(rootPath[index + 1].edge?.name ?? '')
    const filename = path.join(sourceRoot, file)
    let line = -1
    if (fs.existsSync(filename)) {
      const lines = fs.readFileSync(filename, 'utf8').split('\n')
      line = lines.findIndex((text) => text.includes(`this.${field}`))
      if (line < 0) line = lines.findIndex((text) => text.trim().startsWith(`${field} =`))
    }
    owners.push({ owner, field, source: line < 0 ? null : { file, line: line + 1 } })
  }
  return owners
}

/**
 * Compares additive self bytes, including positive resizes, without summing retained subtrees.
 * @param {object} before matched census @param {object} after triggered census
 * @param {object} graph triggered graph @param {object} observation original scalar and post-snapshot
 * @param {string} [sourceRoot] executed archived sources (repository root in synthetic tests)
 * @returns {object} compact growth ledgers, representative paths and explicit attribution limits
 */
const analysePair = (before, after, graph, observation, sourceRoot = ROOT) => {
  const comparison = censusComparison(before, after)
  const old = new Map(before.nodes.map((node) => [node.id, node.selfBytes]))
  const growth = {
    nodes: after.nodes.map((node) => ({
      ...node,
      selfBytes: Math.max(0, node.selfBytes - (old.get(node.id) ?? 0)),
    })),
  }
  const ids = new Set(growth.nodes.filter((node) => node.selfBytes > 0).map((node) => node.id))
  const constructors = topOwners(growth, graph, ids)
  const split = { product: 0, harness: 0, engine: 0, buffers: 0, unresolved: 0 }
  const liveness = { ordinaryReachable: 0, conditionalReachable: 0, nonSemanticLive: 0 }
  for (const node of growth.nodes) {
    split[node.category === 'runtime' ? 'engine' : node.category] += node.selfBytes
    liveness[
      node.ordinaryLive
        ? 'ordinaryReachable'
        : node.semanticLive
          ? 'conditionalReachable'
          : 'nonSemanticLive'
    ] += node.selfBytes
    node.name = node.label || 'unresolved'
    node.type = 'owner-label'
  }
  const owners = topOwners(growth, graph, ids)
  const productOwners = constructors.flatMap((group) =>
    group.examples.flatMap((example) =>
      productFields(example.rootPath, sourceRoot).map((owner) => ({
        ...owner,
        growthNodeId: example.id,
        growthSelfBytes: example.selfBytes,
        rootPath: example.rootPath,
      }))
    )
  )
  const gcSensitiveRetainedBytes = observation.reading.retained - observation.afterSnapshot.retained
  const populationAssessment =
    liveness.ordinaryReachable + liveness.conditionalReachable > THRESHOLD
      ? 'Reachable additive growth survives snapshot collection; inspect the named owners.'
      : gcSensitiveRetainedBytes > THRESHOLD
        ? 'The overage shrank during collecting capture; garbage-in-waiting is a candidate.'
        : 'The original scalar block is not isolated by this graph pair.'
  return {
    ...FLAGS,
    populationAssessment,
    netSelfBytes: comparison.delta,
    positiveSelfBytes: Object.values(split).reduce((sum, bytes) => sum + bytes, 0),
    categories: comparison.categories,
    split,
    liveness,
    constructors,
    owners,
    productOwners,
    productPathObserved: productOwners.length > 0,
    blockAttribution: 'unresolved',
    postCaptureReachability: liveness,
    garbageInWaiting:
      'candidate only; snapshot collection cannot identify the original dead cohort',
    gcSensitiveRetainedBytes,
    interpretation:
      'Snapshot creation collects and allocates. Original scalar, post-snapshot counters and graph self bytes have different time/accounting scopes. Positive growth includes new IDs and positive resizes; net categories preserve releases and transfers. Paths prove reachability of their named growth nodes at snapshot time, not ownership of the original scalar block. Product field locations are source references, not allocation stacks. Missing product paths in bounded examples do not prove absence. A GC-sensitive decrease supports garbage-in-waiting as a candidate; the original block remains unresolved and no bytes are subtracted from acceptance.',
  }
}

/**
 * Authenticates a child's two-event census archive against its original scalar readings.
 * @param {string} directory child directory @param {object} result supervisor result
 * @returns {object[]} zero-trigger M or authenticated M/trigger events
 * @throws {Error} on foreign, incomplete, reordered or changed diagnostic evidence
 */
const validateChild = (directory, result) => {
  const points = result.readings?.activeSnapshot
  if (
    !result.complete ||
    points?.diagnostic !== true ||
    points.acceptanceEligible !== false ||
    points.attributionComplete !== false ||
    points.complete !== true ||
    points.threshold !== THRESHOLD ||
    typeof points.triggered !== 'boolean' ||
    points.records?.length !== (points.triggered ? 2 : 1)
  )
    throw new Error('incomplete active snapshot child')
  const identity = JSON.parse(fs.readFileSync(path.join(directory, 'census-identity.json')))
  for (const key of ['childId', 'tree', 'runId', 'kind', 'node', 'v8'])
    if (identity[key] !== result.identity[key]) throw new Error(`foreign snapshot ${key}`)
  if (
    identity.diagnostic !== true ||
    identity.acceptanceEligible !== false ||
    identity.attributionComplete !== false ||
    identity.variant !== 'capture' ||
    identity.pid !== result.pid ||
    JSON.stringify(identity.flags) !== JSON.stringify(result.readings.manifest.execArgv)
  )
    throw new Error('foreign active snapshot identity')
  const eventsFile = path.join(directory, 'census-events.jsonl')
  if (fs.statSync(eventsFile).size > CAPACITY.eventBytes) throw new Error('snapshot event overflow')
  const events = fs.readFileSync(eventsFile, 'utf8').trim().split('\n').map(JSON.parse)
  if (events.length !== points.records.length) throw new Error('missing active snapshot events')
  const firstOverage = result.readings.actives
    .flatMap((active) => [
      ...active.gc.checkpoints.map((reading, round) => ({ phase: active.phase, round, reading })),
      { phase: active.phase, round: null, reading: active },
    ])
    .find((point) => point.reading.retained - result.readings.matched.retained > THRESHOLD)
  const trigger = events[1]
  if (
    Boolean(firstOverage) !== points.triggered ||
    (trigger &&
      ['phase', 'round', 'reading'].some(
        (key) => JSON.stringify(trigger[key]) !== JSON.stringify(firstOverage[key])
      ))
  )
    throw new Error('snapshot skipped the first original overage')
  let previousEnd = -1
  for (const [index, event] of events.entries()) {
    const record = points.records[index]
    const original =
      index === 0
        ? result.readings.matched
        : result.readings.actives.find((row) => row.phase === event.phase)
    const scalar =
      index === 0 || event.round === null ? original : original?.gc.checkpoints[event.round]
    if (
      event.name !== (index === 0 ? 'M' : 'trigger') ||
      event.diagnostic !== true ||
      event.acceptanceEligible !== false ||
      event.attributionComplete !== false ||
      !scalar ||
      JSON.stringify(event.reading) !== JSON.stringify(scalar) ||
      ['name', 'phase', 'round', 'delta', 'reading'].some(
        (key) => JSON.stringify(event[key]) !== JSON.stringify(record[key])
      ) ||
      !Number.isFinite(event.startedAt) ||
      !Number.isFinite(event.endedAt) ||
      event.startedAt < scalar.at ||
      event.startedAt < previousEnd ||
      event.endedAt < event.startedAt ||
      !Number.isFinite(record.afterSnapshot?.at) ||
      record.afterSnapshot.at < event.endedAt ||
      !Number.isSafeInteger(record.afterSnapshot.retained) ||
      record.afterSnapshot.retained !==
        record.afterSnapshot.heapUsed + record.afterSnapshot.external ||
      event.processRetainedBytes !== scalar.retained ||
      (index === 0
        ? event.delta !== 0
        : event.delta !== scalar.retained - result.readings.matched.retained ||
          event.delta <= THRESHOLD)
    )
      throw new Error('snapshot does not follow original reading')
    previousEnd = record.afterSnapshot.at
    if (
      event.file !== `${event.name}.heapsnapshot` ||
      event.bytes <= 0 ||
      event.bytes > CAPACITY.snapshotBytes
    )
      throw new Error('invalid active snapshot path/bounds')
    const target = path.join(directory, event.file)
    if (
      fs.lstatSync(target).isSymbolicLink() ||
      fs.statSync(target).size !== event.bytes ||
      digest(fs.readFileSync(target)) !== event.sha256
    )
      throw new Error('active snapshot digest mismatch')
  }
  return events
}

/**
 * Writes bounded display labels and paths; raw snapshots retain every ID and complete graph.
 * @param {string} filename JSON destination @param {object} value diagnostic summary
 * @returns {void} compact JSON with explicit truncation markers
 * @throws {Error} on write failure
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
 * Analyses each completed child's pair offline, retaining partial and no-trigger outcomes.
 * @param {string} directory diagnostic root @returns {void} per-child JSON, aggregate JSON and log table
 * @throws {Error} after saving partial results if any child or source authentication is incomplete
 */
const main = (directory) => {
  const run = JSON.parse(fs.readFileSync(path.join(directory, 'snapshot-summary.json')))
  verifySources(directory, run.sources)
  if (
    run.diagnostic !== true ||
    run.acceptanceEligible !== false ||
    run.variant !== 'snapshot' ||
    run.repetitions !== REPETITIONS
  )
    throw new Error('foreign active snapshot run')
  const summary = { schema: 1, ...FLAGS, analysisComplete: false, children: [] }
  for (const kind of KINDS) {
    for (let repetition = 1; repetition <= REPETITIONS; repetition++) {
      const name = `${kind}-${repetition}`
      const child = path.join(directory, name)
      const row = { ...FLAGS, child: name, analysisComplete: false }
      try {
        const result = JSON.parse(fs.readFileSync(path.join(child, 'result.json')))
        const entry = run.results.find((item) => item.child === name)
        if (
          result.identity.kind !== kind ||
          result.identity.repetition !== repetition ||
          !entry?.complete
        )
          throw new Error('foreign snapshot child catalogue')
        for (const key of ['tree', 'runId', 'node', 'v8', 'variant'])
          if (result.identity[key] !== run[key]) throw new Error(`foreign snapshot run ${key}`)
        if (entry.childId !== result.identity.childId) throw new Error('foreign snapshot child ID')
        const events = validateChild(child, result)
        row.triggered = events.length === 2
        row.trigger = events[1] ?? null
        row.originalReadings = {
          matched: result.readings.matched,
          actives: result.readings.actives,
        }
        if (row.triggered) {
          let before
          for (const event of events) {
            const raw = readSnapshot(path.join(child, event.file))
            const graph = analyseSnapshot(raw)
            const census = censusSnapshot({ ...raw, graph })
            if (event.name === 'M') before = census
            else
              row.growth = analysePair(
                before,
                census,
                graph,
                result.readings.activeSnapshot.records[1],
                path.join(directory, 'sources')
              )
          }
        } else
          row.outcome =
            'No original active reading exceeded four MiB; block attribution unavailable.'
        row.analysisComplete = true
      } catch (error) {
        row.failure = error.message
      }
      if (fs.existsSync(child)) writeSummary(path.join(child, 'snapshot-analysis.json'), row)
      summary.children.push(row)
    }
  }
  summary.analysisComplete = run.complete && summary.children.every((row) => row.analysisComplete)
  writeSummary(path.join(directory, 'snapshot-analysis.json'), summary)
  console.table(
    summary.children.map((row) => ({
      child: row.child,
      complete: row.analysisComplete,
      triggered: row.triggered ?? null,
      gcSensitiveRetainedBytes: row.growth?.gcSensitiveRetainedBytes ?? null,
      triggerDelta: row.trigger?.delta ?? null,
      positiveSelfBytes: row.growth?.positiveSelfBytes ?? null,
      productSelfBytes: row.growth?.split.product ?? null,
      harnessSelfBytes: row.growth?.split.harness ?? null,
      engineSelfBytes: row.growth?.split.engine ?? null,
      productPathObserved: row.growth?.productPathObserved ?? null,
      population: row.growth?.populationAssessment ?? row.outcome ?? row.failure,
    }))
  )
  if (!summary.analysisComplete)
    throw new Error('incomplete active snapshot analysis; partial evidence preserved')
}

module.exports = { analysePair, validateChild, main }
if (require.main === module)
  try {
    main(process.argv[2])
  } catch (error) {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  }
