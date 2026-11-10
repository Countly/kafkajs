#!/usr/bin/env node
'use strict'

/**
 * Parses V8's flat graph, computes strong reachability and immediate dominators using
 * Lengauer-Tarjan, and totals each constructor's union of dominated subtrees.
 * Weak edges are excluded; conditional ephemeron liveness is not reconstructed.
 * @param {object} snapshot Parsed .heapsnapshot JSON.
 * @returns {object} Graph indices and constructor totals, for offline comparison only.
 */
const analyseSnapshot = ({ snapshot, nodes, edges, strings }) => {
  const meta = snapshot.meta
  const nf = meta.node_fields.length
  const ef = meta.edge_fields.length
  const field = (fields, key) => {
    const index = fields.indexOf(key)
    if (index < 0) throw new Error(`Missing snapshot field: ${key}`)
    return index
  }
  const nt = field(meta.node_fields, 'type')
  const nn = field(meta.node_fields, 'name')
  const ni = field(meta.node_fields, 'id')
  const ns = field(meta.node_fields, 'self_size')
  const ne = field(meta.node_fields, 'edge_count')
  const et = field(meta.edge_fields, 'type')
  const en = field(meta.edge_fields, 'name_or_index')
  const to = field(meta.edge_fields, 'to_node')
  const n = nodes.length / nf
  if (!Number.isSafeInteger(n) || n !== snapshot.node_count || edges.length % ef !== 0)
    throw new Error('Malformed snapshot dimensions')
  const nodeTypes = meta.node_types[nt]
  const edgeTypes = meta.edge_types[et]
  const root = (snapshot.root_index ?? 0) / nf
  if (!Number.isInteger(root) || root < 0 || root >= n) throw new Error('Invalid snapshot root')
  const starts = new Uint32Array(n + 1)
  const predStarts = new Uint32Array(n + 1)
  for (let v = 0; v < n; v++) {
    const count = nodes[v * nf + ne]
    if (!Number.isSafeInteger(count) || count < 0) throw new Error('Invalid edge count')
    starts[v + 1] = starts[v] + count * ef
  }
  if (starts[n] !== edges.length) throw new Error('Malformed snapshot edges')
  for (let e = 0; e < edges.length; e += ef) {
    const target = edges[e + to] / nf
    if (!Number.isInteger(target) || target < 0 || target >= n)
      throw new Error('Invalid snapshot edge target')
    if (!edgeTypes[edges[e + et]]) throw new Error('Invalid snapshot edge type')
    if (edgeTypes[edges[e + et]] !== 'weak') predStarts[target + 1]++
  }
  for (let v = 1; v <= n; v++) predStarts[v] += predStarts[v - 1]
  const predecessors = new Uint32Array(predStarts[n])
  const predecessorEdges = new Uint32Array(predStarts[n])
  const cursor = predStarts.slice()
  for (let v = 0; v < n; v++)
    for (let e = starts[v]; e < starts[v + 1]; e += ef)
      if (edgeTypes[edges[e + et]] !== 'weak') {
        const at = cursor[edges[e + to] / nf]++
        predecessors[at] = v
        predecessorEdges[at] = e
      }

  const parent = new Int32Array(n).fill(-1)
  const semi = new Uint32Array(n)
  const vertex = new Uint32Array(n + 1)
  const stack = new Uint32Array(n)
  const edgeCursor = starts.slice()
  let visited = 1
  let top = 0
  stack[0] = root
  vertex[1] = root
  semi[root] = 1
  while (top >= 0) {
    const v = stack[top]
    const e = edgeCursor[v]
    if (e >= starts[v + 1]) {
      top--
      continue
    }
    edgeCursor[v] += ef
    const w = edges[e + to] / nf
    if (edgeTypes[edges[e + et]] === 'weak' || semi[w]) continue
    parent[w] = v
    semi[w] = ++visited
    vertex[visited] = w
    stack[++top] = w
  }
  const ancestor = new Int32Array(n).fill(-1)
  const label = Int32Array.from({ length: n }, (_, v) => v)
  const idom = new Int32Array(n).fill(-1)
  const bucket = new Int32Array(n).fill(-1)
  const nextBucket = new Int32Array(n).fill(-1)
  // Iterative path compression avoids overflowing the JS call stack on deep heap graphs.
  const evaluate = (v) => {
    let depth = 0
    let u = v
    while (ancestor[u] !== -1 && ancestor[ancestor[u]] !== -1) {
      stack[depth++] = u
      u = ancestor[u]
    }
    while (depth > 0) {
      u = stack[--depth]
      const a = ancestor[u]
      if (semi[label[a]] < semi[label[u]]) label[u] = label[a]
      ancestor[u] = ancestor[a]
    }
    return label[v]
  }
  for (let i = visited; i > 1; i--) {
    const w = vertex[i]
    for (let j = predStarts[w]; j < predStarts[w + 1]; j++) {
      const v = predecessors[j]
      if (semi[v]) semi[w] = Math.min(semi[w], semi[evaluate(v)])
    }
    const s = vertex[semi[w]]
    nextBucket[w] = bucket[s]
    bucket[s] = w
    const p = parent[w]
    ancestor[w] = p
    for (let v = bucket[p]; v !== -1; v = nextBucket[v]) {
      const u = evaluate(v)
      idom[v] = semi[u] < semi[v] ? u : p
    }
    bucket[p] = -1
  }
  for (let i = 2; i <= visited; i++) {
    const w = vertex[i]
    if (idom[w] !== vertex[semi[w]]) idom[w] = idom[idom[w]]
  }
  const retained = new Float64Array(n)
  const subtreeCount = new Uint32Array(n)
  const firstChild = new Int32Array(n).fill(-1)
  const nextChild = new Int32Array(n).fill(-1)
  const groupIndex = new Int32Array(n).fill(-1)
  const groups = []
  const groupKeys = new Map()
  const unreachable = { count: 0, selfBytes: 0 }
  const describe = (v) => ({
    id: nodes[v * nf + ni],
    type: nodeTypes[nodes[v * nf + nt]],
    name: strings[nodes[v * nf + nn]],
    selfBytes: nodes[v * nf + ns],
    retainedBytes: retained[v],
  })
  for (let v = 0; v < n; v++) {
    const node = describe(v)
    if (!Number.isSafeInteger(node.selfBytes) || node.selfBytes < 0)
      throw new Error('Invalid node self size')
    if (!semi[v]) {
      unreachable.count++
      unreachable.selfBytes += node.selfBytes
      continue
    }
    retained[v] = node.selfBytes
    subtreeCount[v] = 1
    const name = /string/.test(node.type) ? `(${node.type})` : node.name
    const key = JSON.stringify([node.type, name])
    if (!groupKeys.has(key)) {
      groupKeys.set(key, groups.length)
      groups.push({ key, type: node.type, name, count: 0, selfBytes: 0, retainedBytes: 0 })
    }
    const g = groupKeys.get(key)
    groupIndex[v] = g
    groups[g].count++
    groups[g].selfBytes += node.selfBytes
    if (v !== root) {
      nextChild[v] = firstChild[idom[v]]
      firstChild[idom[v]] = v
    }
  }
  for (let i = visited; i > 1; i--) {
    const v = vertex[i]
    retained[idom[v]] += retained[v]
    subtreeCount[idom[v]] += subtreeCount[v]
  }
  // In dominator preorder, each subtree is one interval. Merge same-group nested intervals.
  const lastEnd = new Uint32Array(groups.length)
  const ordinalIndex = new Uint32Array(n)
  let ordinal = 0
  top = 0
  stack[0] = root
  while (top >= 0) {
    const v = stack[top--]
    const g = groupIndex[v]
    if (ordinal >= lastEnd[g]) {
      groups[g].retainedBytes += retained[v]
      lastEnd[g] = ordinal + subtreeCount[v]
    }
    ordinalIndex[v] = ordinal
    ordinal++
    for (let child = firstChild[v]; child !== -1; child = nextChild[child]) stack[++top] = child
  }
  // Breadth-first parents provide short, concrete strong paths independent of dominator paths.
  const rootParent = new Int32Array(n).fill(-1)
  const rootEdge = new Int32Array(n).fill(-1)
  let head = 0
  let tail = 1
  stack[0] = root
  rootParent[root] = root
  while (head < tail) {
    const v = stack[head++]
    for (let e = starts[v]; e < starts[v + 1]; e += ef) {
      const w = edges[e + to] / nf
      if (edgeTypes[edges[e + et]] === 'weak' || rootParent[w] !== -1) continue
      rootParent[w] = v
      rootEdge[w] = e
      stack[tail++] = w
    }
  }
  const rootPath = (v) => {
    const result = []
    while (true) {
      const e = rootEdge[v]
      const type = e === -1 ? null : edgeTypes[edges[e + et]]
      result.push({
        node: describe(v),
        edge:
          e === -1
            ? null
            : {
                type,
                name: ['element', 'hidden'].includes(type) ? edges[e + en] : strings[edges[e + en]],
              },
      })
      if (v === root) break
      v = rootParent[v]
    }
    return result.reverse()
  }
  const outgoing = (v, includeWeak = false) => {
    const result = []
    for (let e = starts[v]; e < starts[v + 1]; e += ef) {
      const type = edgeTypes[edges[e + et]]
      if (type === 'weak' && !includeWeak) continue
      result.push({
        target: edges[e + to] / nf,
        edge: {
          type,
          name: ['element', 'hidden'].includes(type) ? edges[e + en] : strings[edges[e + en]],
        },
      })
    }
    return result
  }
  const incoming = (v) => {
    const result = []
    for (let i = predStarts[v]; i < predStarts[v + 1]; i++) {
      const e = predecessorEdges[i]
      const type = edgeTypes[edges[e + et]]
      result.push({
        owner: describe(predecessors[i]),
        edge: {
          type,
          name: ['element', 'hidden'].includes(type) ? edges[e + en] : strings[edges[e + en]],
        },
      })
    }
    return result
  }
  const dominates = (owner, v) =>
    ordinalIndex[v] >= ordinalIndex[owner] &&
    ordinalIndex[v] < ordinalIndex[owner] + subtreeCount[owner]
  return {
    n,
    root,
    groups,
    unreachable,
    retained,
    idom,
    describe,
    rootPath,
    groupIndex,
    outgoing,
    incoming,
    dominates,
    vertex,
    visited,
  }
}

/**
 * Finds actual named owning properties and labels only dominated storage. Code/source labels
 * apply to their own bytes, never exempt application descendants reached through feedback.
 * @param {object} graph Analysed strong graph.
 * @returns {object} Disjoint per-node categories and explicit property anchors.
 */
const ownershipIndex = (graph) => {
  if (graph.ownership) return graph.ownership
  const anchors = []
  const anchorAt = new Map()
  const sourceTargets = new Set()
  const scriptTargets = new Set()
  const diagnosticNodes = new Set()
  const diagnosticEffects = []
  for (let v = 0; v < graph.n; v++) {
    if (graph.groupIndex[v] < 0) continue
    const owner = graph.describe(v)
    for (const { target, edge } of graph.outgoing(v)) {
      let category
      if (edge.type === 'internal' && edge.name === 'script') scriptTargets.add(target)
      if (
        edge.type === 'internal' &&
        edge.name === 'source' &&
        /string/.test(graph.describe(target).type)
      )
        sourceTargets.add(target)
      if (edge.type !== 'property') continue
      if (owner.type === 'object') {
        if (owner.name === 'ShareSoakDriver' && edge.name === 'acknowledged')
          category = 'acknowledged-identities'
        if (owner.name === 'ShareSoak' && edge.name === 'final') category = 'original-final-proof'
        if (owner.name === 'Soak' && edge.name === 'samples') category = 'compact-samples'
        if (owner.name === 'Soak' && edge.name === 'phaseEnds') category = 'phase-end-summaries'
        if (owner.name === 'ShareSoak' && ['timing', 'counts', 'spent'].includes(edge.name))
          category = 'timing-summaries'
        if (owner.name === 'ShareTimerObserver' && edge.name === 'owners')
          category = 'timer-observer'
        if (owner.name === 'Module') category = 'module-cache'
        if (
          [
            'ProcessMemory',
            'DiagnosticMemory',
            'NativeJournal',
            'PendingSampler',
            'DiagnosticSampler',
            'DiagnosticLog',
          ].includes(owner.name)
        )
          category = 'measurement-journals'
        if (
          owner.name === 'MemoryForeground' &&
          ['ops', 'pushes', 'registry', 'registered'].includes(edge.name)
        )
          category =
            edge.name === 'ops' || edge.name === 'pushes'
              ? 'foreground-decoded-facts'
              : 'intentional-descriptor-uuid'
        if (/Telemetry/.test(owner.name) && /registry|custom|uuid|instance/i.test(edge.name))
          category = 'intentional-descriptor-uuid'
        if (/Telemetry/.test(owner.name) && /snapshot|job|pending|episode/i.test(edge.name))
          category = 'transient-telemetry'
        if (
          /Connection|Broker|Cluster|Consumer|Share/.test(owner.name) &&
          edge.name !== '__proto__'
        )
          category = 'core-connection-group-share-state'
        if (['allocPool', 'allocBuffer'].includes(edge.name)) category = 'buffer-pool-codec-runtime'

        if (
          ['ShareSoak', 'ShareSoakDriver'].includes(owner.name) &&
          ['admin', 'producer', 'lastCycle', 'activeCycle'].includes(edge.name) &&
          graph.describe(target).type === 'object'
        )
          category = 'connection-session-metadata'
      }
      if (owner.name === 'Module' && edge.name === '_cache') category = 'module-cache'
      const prototype =
        owner.type === 'closure' &&
        ['ShareRun', 'ShareFetchLoop', 'ShareSoakDriver', 'ShareMember', 'ShareSession'].includes(
          owner.name
        ) &&
        edge.name === 'prototype'
      if (!category && !prototype) continue
      const anchor = {
        owner,
        property: edge.name,
        target: graph.describe(target),
        category: category ?? 'prototype-inspection',
        index: target,
      }
      anchors.push(anchor)
      if (category) {
        const existing = anchorAt.get(target)
        anchorAt.set(
          target,
          existing && existing !== category ? 'shared-owner-unclassified' : category
        )
      }
    }
  }
  for (let v = 0; v < graph.n; v++) {
    if (graph.groupIndex[v] < 0 || graph.describe(v).type !== 'closure') continue
    const outgoing = graph.outgoing(v)
    const shared = outgoing.find(({ edge }) => edge.type === 'internal' && edge.name === 'shared')
    if (!shared) continue
    const script = graph
      .outgoing(shared.target)
      .find(({ edge }) => edge.type === 'internal' && edge.name === 'script')
    if (
      !script ||
      !/[/\\]scripts[/\\]diag[/\\]shareHeap\.js$/.test(graph.describe(script.target).name)
    )
      continue
    const context = outgoing.find(({ edge }) => edge.type === 'internal' && edge.name === 'context')
    for (const node of [v, shared.target, script.target, context?.target])
      if (node !== undefined) diagnosticNodes.add(node)
    for (const { target, edge } of graph.outgoing(script.target))
      if (edge.type === 'internal' && edge.name === 'source') diagnosticNodes.add(target)
    diagnosticEffects.push({
      function: graph.describe(v),
      script: graph.describe(script.target),
      context: context ? graph.describe(context.target) : null,
    })
  }
  const inherited = new Array(graph.n)
  const categories = new Array(graph.n)
  for (let i = 1; i <= graph.visited; i++) {
    const v = graph.vertex[i]
    const node = graph.describe(v)
    // DFS order visits each dominator before its descendants.
    inherited[v] =
      anchorAt.get(v) ??
      (inherited[graph.idom[v]] === 'module-cache' ? undefined : inherited[graph.idom[v]])
    let category = inherited[v] ?? 'unclassified'
    if (sourceTargets.has(v)) category = 'module-source'
    else if (/Buffer|ArrayBuffer|JSArrayBufferData|zlib|Zstd/.test(node.name))
      category = 'buffer-pool-codec-runtime'
    else if (/FeedbackVector|FeedbackCell|AllocationSite|Literal|Map$/.test(node.name))
      category = 'feedback-storage'
    else if (scriptTargets.has(v)) category = 'script-storage'
    else if (node.type === 'code') category = 'compiled-method-storage'
    else if (/^(system \/ )?Script$/.test(node.name)) category = 'script-storage'
    else if (
      node.type === 'object' &&
      /^(ShareRun|ShareFetchLoop|ShareMember|ShareSession|Connection|Socket|Broker|Cluster|Client)$/.test(
        node.name
      )
    )
      category = 'connection-session-metadata'
    if (diagnosticNodes.has(v) && ['closure', 'object'].includes(node.type))
      category = 'diagnostic-capture-state'
    categories[v] = category
  }
  graph.ownership = { anchors, categories, sourceTargets, diagnosticNodes, diagnosticEffects }
  return graph.ownership
}

/**
 * Traces all direct retainers plus concrete root and dominator owning properties for one sample.
 * @param {object} graph Analysed snapshot.
 * @param {number} v Node index.
 * @returns {object} Paths are evidence, not exclusive ownership or retirement proofs.
 */
const traceOwner = (graph, v) => ({
  rootPath: graph.rootPath(v),
  incoming: graph.incoming(v),
  owningProperties: ownershipIndex(graph)
    .anchors.filter((anchor) => graph.dominates(anchor.index, v))
    .map(({ index, ...anchor }) => anchor),
})

/**
 * Inspects feedback/prototype descendants through contexts, arrays, Maps and internal links.
 * Default work is capped; callers can explicitly request an uncapped traversal. Weak edges,
 * prototype links, strings and synthetic roots are reported traversal boundaries.
 * @param {object} graph Analysed snapshot.
 * @param {number} start Feedback or prototype node.
 * @param {object} [options] Explicit traversal limits and optional path serialization.
 * @returns {object} Application targets and their labelled reference chains.
 */
const auditReferences = (graph, start, options = {}) => {
  const {
    maxNodes = Infinity,
    maxDepth = Infinity,
    maxReferences = Infinity,
    includePaths = true,
  } = options
  const queue = [{ v: start, path: [], depth: 0 }]
  const seen = new Set([start])
  const applicationReferences = []
  const referenced = new Set()
  let weakEdges = 0
  let truncated = false
  for (let cursor = 0; cursor < queue.length && cursor < maxNodes; cursor++) {
    const { v, path, depth } = queue[cursor]
    for (const { target, edge } of graph.outgoing(v, true)) {
      if (edge.type === 'weak') {
        weakEdges++
        continue
      }
      const node = graph.describe(target)
      const chain = includePaths ? [...path, { owner: graph.describe(v), edge, target: node }] : []
      if (
        node.type === 'object' &&
        !['Array', 'Map', 'WeakMap', 'Set'].includes(node.name) &&
        !referenced.has(target)
      )
        if (applicationReferences.length < maxReferences) {
          referenced.add(target)
          applicationReferences.push({ target: node, ...(includePaths ? { path: chain } : {}) })
        } else truncated = true
      if (
        seen.has(target) ||
        /string/.test(node.type) ||
        node.type === 'synthetic' ||
        edge.name === '__proto__'
      )
        continue
      seen.add(target)
      if (depth + 1 >= maxDepth || queue.length >= maxNodes) {
        truncated = true
        continue
      }
      queue.push({ v: target, path: chain, depth: depth + 1 })
    }
  }
  return {
    node: graph.describe(start),
    ownerTrace: traceOwner(graph, start),
    applicationReferences,
    weakEdges,
    visitedNodes: queue.length,
    boundaries: ['weak edges', 'strings', 'synthetic roots', '__proto__ edges'],
    truncated,
    retirementProven: false,
  }
}

module.exports = { analyseSnapshot, ownershipIndex, auditReferences }
