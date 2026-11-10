#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { analyseSnapshot, ownershipIndex, auditReferences } = require('./heapDiff')
const { ephemeronLiveness, refineOwnership, selfLedger } = require('./ownership')
const { replay, verifySources } = require('../retainedDiagnostic')
const { CAPTURES, CATALOGUE, validateCatalogue, digest } = require('./contract')
const { backingInventory, backingComparison } = require('./coldOwnership')

/**
 * Reads one authenticated graph and records semantic liveness independently of ordinary dominators.
 * @param {string} file raw V8 heap capture @returns {object} graph, labels and ephemeron facts
 */
const read = (file) => {
  const bytes = fs.readFileSync(file)
  const raw = JSON.parse(bytes)
  const graph = analyseSnapshot(raw)
  const semantic = ephemeronLiveness(raw)
  const labels = refineOwnership(graph).labels
  const conservativeOnly = []
  for (let v = 0; v < graph.n; v++)
    if (!semantic.reachableIds.has(graph.describe(v).id)) {
      conservativeOnly.push(graph.describe(v))
      labels[v] = 'unclassified:ordinary-approximation-without-semantic-liveness'
    }
  return {
    file: path.basename(file),
    sha256: digest(bytes),
    bytes: bytes.length,
    graph,
    labels,
    semantic,
    conservativeOnly,
  }
}

/** Named source ownership expectations, to be reviewed against actual graph paths. */
const LIFETIMES = Object.freeze({
  'MemoryForeground.registry/registered/client': {
    source: 'src/telemetry/__tests__/fixtures/memoryForeground.js',
    expected:
      'Same returned client, 128 descriptors and assigned UUID at M, block 0 and block 5; absent after all-released. Closed factory, lifetime counters and resolved close state are intentional while that client is rooted.',
  },
  TelemetryPayload: {
    source: 'src/telemetry/payload.js',
    expected:
      'One persistent builder at M and all closed ends; zero pending native shell after settlement; fresh transport authority for every repeated collector block; absent after all-released.',
  },
  TelemetryMetrics: {
    source: 'src/telemetry/metrics.js',
    expected:
      '128 custom descriptors and lifetime connection counter persist through disconnected ends; 64 node slots and 257-point snapshots retire to 129 disconnected points; snapshots, points, raw/result views and native contexts must be attributed, never exempted by a code label.',
  },
  'WeakOwners.rows': {
    source: 'scripts/e2e/telemetry/retained/weakOwners.js',
    expected:
      'Scalar slots are reserved before C0. Issued IDs/kinds remain through all-released; dead WeakRef wrappers retire at read, live targets remain observed. Labels cover only row storage, kind strings and WeakRefs, never arbitrary strong row descendants or weak targets.',
  },
  'NativeJournal/DiagnosticLog/ProcessMemory': {
    source: 'scripts/e2e/telemetry/retained/observer.js',
    expected:
      'Scalar-only observer and journal population remains intentionally rooted; issue tables are weak. Observer/code growth is a diagnostic effect, never subtracted from original M.',
  },
  'Connection/group/share/close': {
    source: 'src/telemetry/client.js',
    expected:
      'Inspect named retaining properties and factory/close continuations; classify reusable closed state separately from retired connections, authentication contexts and cancelled work.',
  },
})

/**
 * Enumerates every new or enlarged material data owner and all backing stores, without constructor rank caps.
 * @param {object|null} before preceding capture @param {object} after current capture
 * @returns {object} named paths, aliases, sources and uncapped consequential reference audits
 */
const details = (before, after) => {
  const graph = after.graph
  const old = new Map()
  if (before)
    for (let v = 0; v < before.graph.n; v++)
      old.set(before.graph.describe(v).id, before.graph.describe(v))
  const material = []
  const backingStores = []
  const audits = []
  const index = ownershipIndex(graph)
  for (let v = 0; v < graph.n; v++) {
    if (graph.groupIndex[v] < 0) continue
    const node = graph.describe(v)
    const previous = old.get(node.id)
    const store = /ArrayBuffer|JSArrayBufferData|Buffer|Uint8Array|zlib|Zstd/.test(node.name)
    const changed =
      !previous ||
      previous.selfBytes !== node.selfBytes ||
      previous.retainedBytes < node.retainedBytes
    const data = ['object', 'array', 'closure', 'native'].includes(node.type)
    if (store || (changed && data && (node.selfBytes >= 1024 || node.retainedBytes >= 16384))) {
      const trace = {
        ...node,
        previous: previous ?? null,
        category: after.labels[v],
        rootPath: graph.rootPath(v),
        incoming: graph.incoming(v),
        properties: graph
          .outgoing(v, true)
          .map(({ target, edge }) => ({ edge, target: graph.describe(target) })),
        sourceExpectations: LIFETIMES,
      }
      material.push(trace)
      if (store) backingStores.push(trace)
    }
    if (
      changed &&
      node.retainedBytes >= 16384 &&
      (/FeedbackVector|FeedbackCell/.test(node.name) ||
        (node.type === 'closure' && /Telemetry|Connection|Share|MemoryForeground/.test(node.name)))
    )
      audits.push(
        auditReferences(graph, v, {
          maxNodes: Infinity,
          maxDepth: Infinity,
          maxReferences: Infinity,
          includePaths: false,
        })
      )
  }
  return {
    material,
    backingStores,
    anchors: index.anchors.map(({ index: ignored, ...anchor }) => anchor),
    audits,
  }
}

/**
 * Analyses each completed capture child serially; pairwise and fixed C0/M comparisons use additive self bytes.
 * @param {string} directory diagnostic run root @returns {void} offline ownership artifacts; no acceptance claim
 */
const main = async (directory) => {
  fs.writeFileSync(
    path.join(directory, 'ownership-summary.json'),
    JSON.stringify({
      schema: 1,
      diagnostic: true,
      acceptanceEligible: false,
      analysisComplete: false,
      attributionComplete: false,
      status: 'analysis started; partial evidence remains on error or timeout',
    })
  )
  const summary = JSON.parse(fs.readFileSync(path.join(directory, 'diagnostic-summary.json')))
  const runProblems = validateCatalogue(summary.results)
  const replays = []
  for (const tuple of CATALOGUE) {
    const child = path.join(directory, `${tuple.arm}-${tuple.kind}-${tuple.variant}`)
    try {
      const record = JSON.parse(fs.readFileSync(path.join(child, 'record.json')))
      verifySources(directory, record.identity.sources)
      const reviewed = await replay(child, record.identity, record.exit)
      replays.push({
        ...tuple,
        childId: record.identity.childId,
        complete: reviewed.complete,
        problems: reviewed.problems,
      })
    } catch (error) {
      replays.push({ ...tuple, complete: false, problems: [error.message] })
    }
  }
  const analyses = []
  for (const result of replays.filter((row) => row.variant === 'capture')) {
    const child = path.join(directory, `${result.arm}-${result.kind}-capture`)
    if (!fs.existsSync(path.join(child, 'events.jsonl'))) {
      analyses.push({ ...result, analysisComplete: false })
      continue
    }
    const events = fs
      .readFileSync(path.join(child, 'events.jsonl'), 'utf8')
      .trim()
      .split('\n')
      .map(JSON.parse)
    const captures = events.filter((event) => event.type === 'checkpoint' && event.file)
    const comparisons = []
    let baseline
    let matched
    let previous
    const snapshots = []
    const cold = []
    const coldOwners = []
    for (const name of CAPTURES) {
      const capture = captures.find((row) => row.name === name)
      if (!capture) continue
      const current = read(path.join(child, capture.file))
      if (current.bytes !== capture.bytes || current.sha256 !== capture.sha256)
        throw new Error('offline capture authentication failed')
      const report = details(previous, current)
      if (['M', 'closed-0', 'closed-5', 'all-released'].includes(name)) {
        cold.push({ name, sha256: current.sha256, backingStores: backingInventory(current) })
        if (name === 'closed-0')
          for (const owner of report.material)
            if (!owner.previous || owner.selfBytes !== owner.previous.selfBytes)
              coldOwners.push({ id: owner.id, first: owner, endpoints: [] })
        if (coldOwners.length) {
          const ids = new Map()
          for (let v = 0; v < current.graph.n; v++) ids.set(current.graph.describe(v).id, v)
          for (const owner of coldOwners) {
            const v = ids.get(owner.id)
            owner.endpoints.push({
              name,
              present: v !== undefined,
              semanticLive: current.semantic.reachableIds.has(owner.id),
              node: v === undefined ? null : current.graph.describe(v),
              category: v === undefined ? null : current.labels[v],
              rootPath:
                v === undefined || current.graph.groupIndex[v] < 0 ? [] : current.graph.rootPath(v),
            })
          }
        }
      }
      const semantic = current.semantic
      const conditional = semantic.pairs.filter(
        (pair) => pair.active && !pair.independentlyReachableValue
      )
      const dependencies = new Set(conditional.flatMap((pair) => [pair.tableId, pair.keyId]))
      const raw = dependencies.size
        ? JSON.parse(fs.readFileSync(path.join(child, capture.file)))
        : null
      const dependencyRemoval = [...dependencies].map((id) => {
        const removed = ephemeronLiveness(raw, [id])
        return {
          id,
          lostConditionalValues: conditional
            .filter((pair) => !removed.reachableIds.has(pair.valueId))
            .map((pair) => pair.valueId),
        }
      })
      const detailFile = `ownership-${name}.json`
      fs.writeFileSync(
        path.join(child, detailFile),
        JSON.stringify({
          diagnostic: true,
          acceptanceEligible: false,
          name,
          ...report,
          ephemerons: {
            pairs: semantic.pairs,
            conditionalOnly: [...semantic.reachableIds].filter(
              (id) => !semantic.ordinaryIds.has(id)
            ),
            dependencyRemoval,
            ordinaryApproximationOnly: current.conservativeOnly,
          },
        })
      )
      if (!baseline) baseline = current
      if (name === 'M') matched = current
      const ledger = (start) => {
        const selfBytes = selfLedger(start.graph, current.graph, start.labels, current.labels)
        const unknown = Object.entries(selfBytes)
          .filter(([label]) => /unclassified|unresolved|residual:shared/.test(label))
          .reduce(
            (sum, [, row]) => ({
              before: sum.before + row.before,
              after: sum.after + row.after,
              delta: sum.delta + row.delta,
            }),
            { before: 0, after: 0, delta: 0 }
          )
        return {
          before: start.file,
          after: current.file,
          selfBytes,
          unknown,
          unknownCouldContainMaterialGrowth: unknown.after > 262144,
        }
      }
      comparisons.push({
        name,
        consecutive: previous ? ledger(previous) : null,
        baseline: ledger(baseline),
        originalM: matched ? ledger(matched) : null,
      })
      snapshots.push({
        name,
        file: current.file,
        sha256: current.sha256,
        bytes: current.bytes,
        detailFile,
      })
      previous = current
    }
    const coldReport = {
      schema: 1,
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      captures: cold,
      stores: cold.slice(1).map((capture, index) => ({
        before: cold[index].name,
        after: capture.name,
        ...backingComparison(cold[index].backingStores, capture.backingStores),
      })),
      coldOwners,
      selfBytes: comparisons
        .filter((row) => ['closed-0', 'closed-5', 'all-released'].includes(row.name))
        .map((row) => ({ name: row.name, originalM: row.originalM, consecutive: row.consecutive })),
      limitations:
        'Cold owners are material new/resized nodes at closed-0 relative to M; retained sizes and root paths overlap and are not added. Self-byte ledgers retain unknown additions/removals and transfers. Store IDs describe snapshot-visible backing storage, not unobserved native allocator scratch. A reviewer must attribute material unknowns and confirm lifetimes; capture counters cannot be subtracted from pre-GC readings.',
    }
    fs.writeFileSync(path.join(child, 'cold-ownership.json'), JSON.stringify(coldReport))
    analyses.push({
      arm: result.arm,
      kind: result.kind,
      analysisComplete: snapshots.length === 7,
      coldReport: 'cold-ownership.json',
    })
    fs.writeFileSync(
      path.join(child, 'ownership.json'),
      JSON.stringify({
        schema: 1,
        diagnostic: true,
        acceptanceEligible: false,
        workloadComplete: result.complete,
        analysisComplete: snapshots.length === 7,
        attributionComplete: false,
        snapshots,
        comparisons,
        coldReport: 'cold-ownership.json',
        lifetimeExpectations: LIFETIMES,
        limitations:
          'Self bytes are additive; retained subtrees overlap and are never summed. Ordinary dominators conservatively include labelled pair edges; semantic AND(table,key) liveness and dependency-removal facts are separate. No subtraction from pre-capture heapUsed. Every residual/unclassified bucket and material named path requires owner review; flat later bytes do not establish cold ownership.',
      })
    )
  }
  fs.writeFileSync(
    path.join(directory, 'ownership-summary.json'),
    JSON.stringify({
      schema: 1,
      diagnostic: true,
      acceptanceEligible: false,
      workloadComplete: runProblems.length === 0 && replays.every((row) => row.complete),
      analysisComplete: analyses.length === 8 && analyses.every((row) => row.analysisComplete),
      attributionComplete: false,
      runProblems,
      replays,
      analyses,
    })
  )
  if (
    runProblems.length ||
    replays.some((row) => !row.complete) ||
    analyses.some((row) => !row.analysisComplete)
  )
    process.exitCode = 1
}
module.exports = { read, details, main, LIFETIMES }
if (require.main === module)
  main(process.argv[2]).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
