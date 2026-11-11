'use strict'

const { analyseSnapshot } = require('./heapDiff')
const { prepareEphemerons, refineOwnership, selfLedger } = require('./ownership')
const { backingInventory } = require('./coldOwnership')

const FLAGS = Object.freeze({
  diagnostic: true,
  acceptanceEligible: false,
  attributionComplete: false,
})
const CATEGORIES = Object.freeze(['product', 'runtime', 'harness', 'buffers', 'unresolved'])

/**
 * Assigns exclusive self-byte categories, keeping unknown application data explicit.
 * @param {object} node graph node @param {string} label existing conservative owner label
 * @returns {string} one ledger category; retained subtree sizes are never added
 */
const category = (node, label = '') => {
  if (
    (node.type === 'object' &&
      /^(Buffer|ArrayBuffer|SharedArrayBuffer|(?:\w+Array)|DataView)$/.test(node.name)) ||
    /JSArrayBufferData/.test(node.name)
  )
    return 'buffers'
  if (
    node.type === 'code' ||
    node.type === 'object shape' ||
    /^(module-source|script-storage|compiled-method-storage)$/.test(label)
  )
    return 'runtime'
  if (
    node.type === 'object' &&
    /^(FixedJournal|WeakOwners|WeakRef|NativeJournal|PendingSampler|ProcessMemory|DiagnosticMemory|DiagnosticLog|LiveCensus|TelemetryTap|TelemetrySegments|TelemetryWireObserver|TelemetryExportReader|TelemetrySchedule|TelemetryProcesses|TelemetryWorker|TelemetryWorkerProcess|TelemetryChurn|TelemetryProofs|ProcessResources|Registry|OrdinaryOracle|ShareExchanges|ShareOracle|ParentFixture|DecoderCoordinator|DecoderMemory|DecoderGate|WireCursor|FrameStream|FrameCounter|ReferenceControl|Inbox|OrdinaryWork|ShareLedger|SocketTape)$/.test(
      node.name
    )
  )
    return 'harness'
  if (
    node.type === 'object' &&
    /^(TelemetryFactory|TelemetryMetrics|ClientTelemetry|TelemetryPayload|MetricSnapshot|ProtoWriter|MetricsDataEncoding|Connection|RequestQueue|Request|Broker|Cluster|ShareRun|ShareSession|ShareFetchLoop|ShareMembership|Session|Zlib|Gzip|Zstd)$/.test(
      node.name
    )
  )
    return 'product'
  if (
    /^(intentional-descriptor-uuid|transient-telemetry|core-connection-group-share-state|connection-session-metadata)$/.test(
      label
    )
  )
    return 'product'
  if (node.type !== 'object' && (/^(runtime:|node:)/.test(label) || label === 'feedback-storage'))
    return 'runtime'
  if (
    /^harness:/.test(label) ||
    /^(measurement-journals|foreground-decoded-facts|diagnostic-capture-state|compact-samples|phase-end-summaries|timing-summaries|timer-observer|original-final-proof|acknowledged-identities)$/.test(
      label
    )
  )
    return 'harness'
  return 'unresolved'
}

/**
 * Produces an additive census, including graph nodes without semantic liveness.
 * Normalized parent links reconstruct all strong paths without repeating their prefixes.
 * @param {object} raw V8 snapshot or prepared {graph,labels,semantic} from the retained analyser
 * @returns {object} disjoint self bytes, stable IDs, weak edges and unique backing stores
 * @throws {Error} on duplicate IDs or malformed snapshot dependencies
 */
const censusSnapshot = (raw) => {
  const graph = raw.graph ?? analyseSnapshot(raw)
  const semantic = raw.semantic ?? prepareEphemerons(raw).liveness()
  const labels = raw.labels ?? refineOwnership(graph).labels
  const totals = Object.fromEntries(CATEGORIES.map((name) => [name, 0]))
  const nodes = []
  const ids = new Set()
  const paths = []
  const weakEdges = []
  for (let v = 0; v < graph.n; v++) {
    const node = graph.describe(v)
    if (ids.has(node.id)) throw new Error('duplicate census stable ID')
    ids.add(node.id)
    const semanticLive = semantic.reachableIds.has(node.id)
    const owner = semanticLive ? category(node, labels[v]) : 'unresolved'
    nodes.push({
      id: node.id,
      name: node.name,
      type: node.type,
      selfBytes: node.selfBytes,
      category: owner,
      label: labels[v] ?? 'unreachable',
      ordinaryLive: semantic.ordinaryIds.has(node.id),
      semanticLive,
    })
    totals[owner] += node.selfBytes
    paths.push({ id: node.id, ...graph.rootLink(v) })
    for (const { target, edge } of graph.outgoing(v, true))
      if (edge.type === 'weak')
        weakEdges.push({ ownerId: node.id, targetId: graph.describe(target).id, edge })
  }
  return {
    schema: 1,
    ...FLAGS,
    totals,
    totalSelfBytes: Object.values(totals).reduce((sum, bytes) => sum + bytes, 0),
    nodes,
    paths,
    weakEdges,
    ephemerons: semantic.pairs,
    backingStores: backingInventory({ graph, labels, semantic }),
    interpretation:
      'Every graph self byte occurs once, including unresolved/non-semantic-live nodes. Paths are ordinary strong paths; labelled ephemeron edges require both table and key, as recorded separately. Views and ArrayBuffer wrappers contribute only their own self bytes; store IDs count backing storage once. Process retained = heapUsed + external; arrayBuffers is inside external and is never added again. Snapshot capture collects and allocates; graph self bytes, pre-capture process counters, engine code counters and RSS cover different times and allocator scopes. Their difference remains unexplained, never subtracted from an acceptance reading.',
  }
}

/**
 * Reconciles new allocations, releases, resizing and category transfers independently.
 * @param {object} before earlier same-worker census @param {object} after later census
 * @returns {object} additive category rows and explicit allocated/released identities
 * @throws {Error} if the stable-ID self-byte ledger does not reconcile
 */
const censusComparison = (before, after) => {
  const adapter = (point) => ({
    n: point.nodes.length,
    groupIndex: new Int8Array(point.nodes.length),
    describe: (v) => point.nodes[v],
  })
  const categories = selfLedger(
    adapter(before),
    adapter(after),
    before.nodes.map((node) => node.category),
    after.nodes.map((node) => node.category)
  )
  for (const name of CATEGORIES)
    categories[name] ??= {
      before: 0,
      after: 0,
      added: 0,
      removed: 0,
      resized: 0,
      transfer: 0,
      delta: 0,
    }
  const delta = after.totalSelfBytes - before.totalSelfBytes
  if (Object.values(categories).reduce((sum, row) => sum + row.delta, 0) !== delta)
    throw new Error('census total reconciliation failed')
  const old = new Set(before.nodes.map((node) => node.id))
  const next = new Set(after.nodes.map((node) => node.id))
  return {
    ...FLAGS,
    categories,
    beforeTotalSelfBytes: before.totalSelfBytes,
    afterTotalSelfBytes: after.totalSelfBytes,
    delta,
    closedAbsoluteUnresolvedBytes: after.totals.unresolved,
    allocated: after.nodes.filter((node) => !old.has(node.id)),
    released: before.nodes.filter((node) => !next.has(node.id)),
  }
}

module.exports = { censusSnapshot, censusComparison }
