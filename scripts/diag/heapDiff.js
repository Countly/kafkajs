#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const zlib = require('node:zlib')

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
  const cursor = predStarts.slice()
  for (let v = 0; v < n; v++)
    for (let e = starts[v]; e < starts[v + 1]; e += ef)
      if (edgeTypes[edges[e + et]] !== 'weak') predecessors[cursor[edges[e + to] / nf]++] = v

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
    const visitedParents = new Set()
    for (let i = predStarts[v]; i < predStarts[v + 1]; i++) {
      const parent = predecessors[i]
      if (visitedParents.has(parent)) continue
      visitedParents.add(parent)
      for (const entry of outgoing(parent))
        if (entry.target === v) result.push({ owner: describe(parent), edge: entry.edge })
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
 * Suggests where a human should inspect ownership; names do not prove a product or VM leak.
 * @param {object[]} rootPath Strong root path.
 * @returns {string[]} Heuristic owner categories observed on the path.
 */
const ownerHints = (rootPath) => {
  const text = rootPath.map(({ node, edge }) => `${node.name} ${edge?.name ?? ''}`).join(' ')
  const hints = []
  if (/timer|timeout|immediate|observer|async.?hook/i.test(text))
    hints.push('timer/observer metadata')
  if (/corpus|sample|summary|journal|evidence|acknowledged|prefix|ShareSoak|Soak/i.test(text))
    hints.push('corpus/sample/summary storage')
  if (
    /Kafka|Broker|Connection|Socket|ShareSession|ShareMember|Cluster|Consumer|Producer|ShareRun/i.test(
      text
    )
  )
    hints.push('client metadata/connection/session owners')
  if (rootPath.some(({ node }) => ['hidden', 'code', 'synthetic'].includes(node.type)))
    hints.push('V8 internals on path (does not establish ultimate ownership)')
  return hints.length ? hints : ['unclassified; inspect strong path and dominators']
}

/**
 * Diffs constructor counts, self bytes and retained unions; samples the largest individual
 * retained growth, including surviving owners, with before/after dominators and strong paths.
 * @param {object} baseline Result of analyseSnapshot at the baseline.
 * @param {object} end Result of analyseSnapshot at a later matched quiet point.
 * @returns {object} Serializable comparison and rankings.
 */
const compareSnapshots = (baseline, end) => {
  const before = new Map(baseline.groups.map((g) => [g.key, g]))
  const after = new Map(end.groups.map((g) => [g.key, g]))
  const ids = new Map()
  for (let v = 0; v < baseline.n; v++) ids.set(baseline.describe(v).id, v)
  const groups = [...new Set([...before.keys(), ...after.keys()])].map((key) => {
    const b = before.get(key)
    const a = after.get(key)
    const metrics = (g) => ({
      count: g?.count ?? 0,
      selfBytes: g?.selfBytes ?? 0,
      retainedBytes: g?.retainedBytes ?? 0,
    })
    const start = metrics(b)
    const finish = metrics(a)
    return {
      key,
      type: (a ?? b).type,
      name: (a ?? b).name,
      baseline: start,
      end: finish,
      delta: Object.fromEntries(
        Object.keys(start).map((field) => [field, finish[field] - start[field]])
      ),
      samples: [],
    }
  })
  const top = {}
  const selected = new Map()
  for (const field of ['count', 'selfBytes', 'retainedBytes']) {
    top[field] = groups
      .filter((g) => g.delta[field] > 0)
      .sort((a, b) => b.delta[field] - a.delta[field])
      .slice(0, 20)
      .map((g) => g.key)
    for (const key of top[field]) selected.set(key, [])
  }
  for (let v = 0; v < end.n; v++) {
    if (end.groupIndex[v] < 0) continue
    const key = end.groups[end.groupIndex[v]].key
    const candidates = selected.get(key)
    if (!candidates) continue
    const node = end.describe(v)
    const old = ids.get(node.id)
    candidates.push({
      v,
      retainedDelta: node.retainedBytes - (old === undefined ? 0 : baseline.retained[old]),
      retainedBytes: node.retainedBytes,
    })
    candidates.sort(
      (a, b) => b.retainedDelta - a.retainedDelta || b.retainedBytes - a.retainedBytes
    )
    if (candidates.length > 3) candidates.pop()
  }
  for (const group of groups) {
    for (const { v } of selected.get(group.key) ?? []) {
      const dominators = []
      for (let d = end.idom[v]; d !== -1; d = end.idom[d]) dominators.push(end.describe(d))
      const rootPath = end.rootPath(v)
      const old = ids.get(end.describe(v).id)
      const baselineNode = old === undefined ? null : baseline.describe(old)
      const baselineDominators = []
      if (old !== undefined)
        for (let d = baseline.idom[old]; d !== -1; d = baseline.idom[d])
          baselineDominators.push(baseline.describe(d))
      group.samples.push({
        ...end.describe(v),
        presentAtBaseline: baselineNode !== null,
        baselineNode,
        baselineDominators,
        retainedDeltaBytes: end.retained[v] - (baselineNode?.retainedBytes ?? 0),
        rootPath,
        dominators,
        ownerHints: ownerHints(rootPath),
      })
    }
  }
  return { top, groups, ownership: compareOwnership(baseline, end) }
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
    inherited[v] = anchorAt.get(v) ?? inherited[graph.idom[v]]
    let category = inherited[v] ?? 'unclassified'
    if (sourceTargets.has(v)) category = 'module-source'
    else if (/FeedbackVector|FeedbackCell/.test(node.name)) category = 'feedback-storage'
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
 * Work is explicitly capped; truncated or weak/ephemeron paths keep the attribution gate open.
 * @param {object} graph Analysed snapshot.
 * @param {number} start Feedback or prototype node.
 * @returns {object} Application targets and their labelled reference chains.
 */
const auditReferences = (graph, start) => {
  const queue = [{ v: start, path: [] }]
  const seen = new Set([start])
  const applicationReferences = []
  let weakEdges = 0
  let truncated = false
  for (let cursor = 0; cursor < queue.length && cursor < 1000; cursor++) {
    const { v, path } = queue[cursor]
    for (const { target, edge } of graph.outgoing(v, true)) {
      if (edge.type === 'weak') {
        weakEdges++
        continue
      }
      const node = graph.describe(target)
      const chain = [...path, { owner: graph.describe(v), edge, target: node }]
      if (
        node.type === 'object' &&
        !['Array', 'Map', 'WeakMap', 'Set'].includes(node.name) &&
        applicationReferences.length < 128
      )
        applicationReferences.push({ target: node, path: chain })
      if (applicationReferences.length >= 128) truncated = true
      if (
        seen.has(target) ||
        /string/.test(node.type) ||
        node.type === 'synthetic' ||
        edge.name === '__proto__'
      )
        continue
      seen.add(target)
      if (chain.length >= 6 || queue.length >= 1000) {
        truncated = true
        continue
      }
      queue.push({ v: target, path: chain })
    }
  }
  return {
    node: graph.describe(start),
    ownerTrace: traceOwner(graph, start),
    applicationReferences,
    weakEdges,
    truncated,
    retirementProven: false,
  }
}

/**
 * Accounts every reachable node once, including releases and category transfers of stable IDs.
 * Samples are selected for each growing category/type, independently of constructor rankings.
 * @param {object} before Previous capture (or baseline for a cumulative comparison).
 * @param {object} after Current capture.
 * @returns {object} Additive self-byte ledger, property traces and feedback/prototype inspections.
 */
const compareOwnership = (before, after) => {
  const b = ownershipIndex(before)
  const a = ownershipIndex(after)
  const categories = {}
  const totals = (graph, index, side) => {
    for (let v = 0; v < graph.n; v++) {
      if (graph.groupIndex[v] < 0) continue
      const category = index.categories[v]
      categories[category] ??= {
        before: { count: 0, selfBytes: 0 },
        after: { count: 0, selfBytes: 0 },
        addedSelfBytes: 0,
        removedSelfBytes: 0,
        survivorSelfDelta: 0,
        transferSelfBytes: 0,
      }
      categories[category].diagnosticSelfBytes ??= { before: 0, after: 0 }
      if (index.diagnosticNodes.has(v))
        categories[category].diagnosticSelfBytes[side] += graph.describe(v).selfBytes
      categories[category][side].count++
      categories[category][side].selfBytes += graph.describe(v).selfBytes
      categories[category].types ??= { before: {}, after: {} }
      const type = graph.describe(v).type
      categories[category].types[side][type] ??= { count: 0, selfBytes: 0 }
      categories[category].types[side][type].count++
      categories[category].types[side][type].selfBytes += graph.describe(v).selfBytes
    }
  }
  totals(before, b, 'before')
  totals(after, a, 'after')
  const beforeIds = new Map()
  const afterIds = new Set()
  for (let v = 0; v < before.n; v++)
    if (before.groupIndex[v] >= 0) beforeIds.set(before.describe(v).id, v)
  const selected = new Map()
  const audit = new Set(
    a.anchors
      .filter(({ category }) => category === 'prototype-inspection')
      .map(({ index }) => index)
  )
  for (let v = 0; v < after.n; v++) {
    if (after.groupIndex[v] < 0) continue
    const node = after.describe(v)
    const old = beforeIds.get(node.id)
    afterIds.add(node.id)
    const category = a.categories[v]
    const ledger = categories[category]
    if (old === undefined) ledger.addedSelfBytes += node.selfBytes
    else {
      const oldCategory = b.categories[old]
      if (oldCategory === category)
        ledger.survivorSelfDelta += node.selfBytes - before.describe(old).selfBytes
      else {
        categories[oldCategory].transferSelfBytes -= before.describe(old).selfBytes
        ledger.transferSelfBytes += node.selfBytes
      }
    }
    const retainedDelta = node.retainedBytes - (old === undefined ? 0 : before.retained[old])
    if (old !== undefined && retainedDelta <= 0 && node.selfBytes <= before.describe(old).selfBytes)
      continue
    const kind = /Buffer|ArrayBuffer|Uint8Array/.test(node.name)
      ? 'buffer'
      : /Feedback/.test(node.name)
        ? 'feedback'
        : node.type === 'array' || node.name === 'Array'
          ? 'array'
          : /Map/.test(node.name)
            ? 'map'
            : node.type
    const key = `${category}/${kind}`
    const candidates = selected.get(key) ?? []
    candidates.push({ v, retainedDelta })
    candidates.sort((x, y) => y.retainedDelta - x.retainedDelta)
    if (candidates.length > 3) candidates.pop()
    selected.set(key, candidates)
  }
  for (const [id, v] of beforeIds)
    if (!afterIds.has(id))
      categories[b.categories[v]].removedSelfBytes += before.describe(v).selfBytes
  const incrementalSamples = []
  for (const candidates of selected.values())
    for (const { v, retainedDelta } of candidates) {
      const node = after.describe(v)
      const old = beforeIds.get(node.id)
      incrementalSamples.push({
        ...node,
        category: a.categories[v],
        previousNode: old === undefined ? null : before.describe(old),
        retainedDeltaBytes: retainedDelta,
        ownerTrace: traceOwner(after, v),
      })
      if (/Feedback|Map|Buffer|ArrayBuffer/.test(node.name) || node.type === 'closure') audit.add(v)
    }
  // The originally investigated vector is kept visible if it survives even without growth.
  for (let v = 0; v < after.n; v++)
    if (
      after.groupIndex[v] >= 0 &&
      after.describe(v).id === 274123 &&
      /Feedback/.test(after.describe(v).name)
    )
      audit.add(v)
  const total = {
    before: { count: 0, selfBytes: 0 },
    after: { count: 0, selfBytes: 0 },
    delta: { count: 0, selfBytes: 0 },
  }
  for (const ledger of Object.values(categories)) {
    ledger.delta = {
      count: ledger.after.count - ledger.before.count,
      selfBytes: ledger.after.selfBytes - ledger.before.selfBytes,
    }
    if (
      ledger.delta.selfBytes !==
      ledger.addedSelfBytes -
        ledger.removedSelfBytes +
        ledger.survivorSelfDelta +
        ledger.transferSelfBytes
    )
      throw new Error('Ownership byte ledger does not reconcile')
    for (const side of ['before', 'after', 'delta'])
      for (const metric of ['count', 'selfBytes']) total[side][metric] += ledger[side][metric]
  }
  return {
    total,
    categories,
    incrementalSamples,
    anchors: a.anchors.map(({ index, ...anchor }) => anchor),
    sourceTargets: { before: b.sourceTargets.size, after: a.sourceTargets.size },
    referenceAudits: [...audit].map((v) => auditReferences(after, v)),
    diagnosticEffects: a.diagnosticEffects,
    attributionComplete: false,
    openGate:
      'Review unclassified/shared bytes, all material incremental owners, truncated references and conditional ephemerons; sampled paths do not prove retired data absent.',
  }
}

/** Reads one raw or gzipped V8 snapshot. @param {string} file Snapshot path. */
const readSnapshot = (file) =>
  analyseSnapshot(
    JSON.parse(
      file.endsWith('.gz')
        ? zlib.gunzipSync(fs.readFileSync(file)).toString('utf8')
        : fs.readFileSync(file, 'utf8')
    )
  )

/**
 * Writes JSON with every constructor total and readable text with rankings and sampled paths.
 * @param {string[]} argv Baseline, one or more ends, then --out <output prefix>.
 * @returns {void}
 */
const main = (argv) => {
  const args = [...argv]
  const option = (name) => {
    const at = args.indexOf(name)
    if (at < 0) return null
    const value = args[at + 1]
    if (!value || value.startsWith('--')) throw new Error(`Missing ${name} value`)
    args.splice(at, 2)
    return value
  }
  const evidenceFile = option('--evidence')
  const manifestFile = option('--manifest')
  let status = {
    complete: false,
    violations: ['No full diagnostic evidence/manifest supplied; partial analysis only'],
  }
  let captures
  if (manifestFile) {
    const read = (file) => fs.readFileSync(file, 'utf8').trim().split('\n').map(JSON.parse)
    captures = read(manifestFile)
    if (evidenceFile) status = require('./evidence').evidenceStatus(read(evidenceFile), captures)
    const snapshotFiles = captures.map(({ file, sizeBytes }) => {
      const raw = path.join(path.dirname(manifestFile), file)
      const resolved = fs.existsSync(raw) ? raw : `${raw}.gz`
      const bytes = resolved.endsWith('.gz')
        ? zlib.gunzipSync(fs.readFileSync(resolved)).length
        : fs.statSync(resolved).size
      if (bytes !== sizeBytes) {
        status.complete = false
        status.violations.push(`Snapshot size mismatch: ${file}`)
      }
      return resolved
    })
    args.unshift(...snapshotFiles)
  }
  const outAt = args.indexOf('--out')
  if (outAt < 2 || outAt !== args.length - 2)
    throw new Error(
      'Usage: node heapDiff.js baseline.heapsnapshot end.heapsnapshot [...] --out output-prefix'
    )
  const files = args.slice(0, outAt)
  const output = args[outAt + 1]
  const baseline = readSnapshot(files[0])
  const notes = [
    'DIAGNOSTIC ONLY — not a soak acceptance receipt.',
    `Evidence status: ${status.complete ? 'COMPLETE diagnostic workload/captures/cleanup' : 'INCOMPLETE — partial analysis only'}.`,
    ...status.violations,
    'Retained bytes are strong-edge dominator subtree unions within each constructor/type; groups overlap and must not be summed.',
    'Self bytes are additive. Weak edges are excluded; conditional ephemeron liveness is not reconstructed.',
    'Stable IDs indicate survival only in this process. Names and owner hints are not attribution verdicts.',
    'Root paths include V8 internals; follow client-rooted graphs to their teardown/reuse code before classifying retention.',
  ]
  fs.mkdirSync(path.dirname(output), { recursive: true })
  const json = fs.openSync(`${output}.json`, 'w')
  const print = (line) => {
    fs.appendFileSync(`${output}.txt`, `${line}\n`)
    console.log(line)
  }
  fs.writeFileSync(`${output}.txt`, '')
  fs.writeSync(
    json,
    JSON.stringify({
      notes,
      evidence: status,
      analysisSource: {
        file: 'scripts/diag/heapDiff.js',
        sha256: require('node:crypto')
          .createHash('sha256')
          .update(fs.readFileSync(__filename))
          .digest('hex'),
      },
      baseline: { file: files[0], groups: baseline.groups, unreachable: baseline.unreachable },
    }).slice(0, -1) + ',"comparisons":['
  )
  try {
    for (const note of notes) print(note)
    let previousFile = files[0]
    let previous = baseline
    const firstSeen = new Map()
    for (let v = 0; v < baseline.n; v++) firstSeen.set(baseline.describe(v).id, files[0])
    for (const [index, file] of files.slice(1).entries()) {
      const end = readSnapshot(file)
      const comparison = {
        file,
        previousFile,
        unreachable: end.unreachable,
        ...compareSnapshots(baseline, end),
        sincePrevious: compareSnapshots(previous, end),
        comparability: {
          intentionalCorpus: /repeat-010/.test(file) ? { before: 1800, after: 2400 } : null,
          changingOwners: [
            'acknowledged-identities',
            'original-final-proof',
            'compact-samples',
            'phase-end-summaries',
            'timing-summaries',
            'timer-observer',
          ],
          conclusion:
            'Compare added/released stable-ID self bytes in each ledger. Total heap alone establishes neither saturation nor a product leak; material unclassified ownership remains open.',
        },
      }
      for (let v = 0; v < end.n; v++) {
        const id = end.describe(v).id
        if (!firstSeen.has(id)) firstSeen.set(id, file)
      }
      for (const group of comparison.groups) {
        const previousGroup = previous.groups.find((g) => g.key === group.key)
        group.sincePrevious = Object.fromEntries(
          Object.keys(group.end).map((field) => [
            field,
            group.end[field] - (previousGroup?.[field] ?? 0),
          ])
        )
        for (const sample of group.samples) sample.firstSeenCapture = firstSeen.get(sample.id)
      }
      fs.writeSync(json, (index ? ',' : '') + JSON.stringify(comparison))
      print(`PAIRWISE ${previousFile} -> ${file}`)
      print(
        `Disjoint self-byte ledger: ${JSON.stringify(comparison.sincePrevious.ownership.categories)}`
      )
      for (const sample of comparison.sincePrevious.ownership.incrementalSamples) {
        print(
          `OWNER id=${sample.id} category=${sample.category} previous=${sample.previousNode?.id ?? 'new'} self=${sample.selfBytes} retained delta=${sample.retainedDeltaBytes} ${JSON.stringify(sample.ownerTrace)}`
        )
      }
      for (const audit of comparison.sincePrevious.ownership.referenceAudits)
        print(`REFERENCE AUDIT ${JSON.stringify(audit)}`)
      print(`\nBASELINE ${files[0]} -> ${file}`)
      for (const field of ['count', 'selfBytes', 'retainedBytes']) {
        print(`Top growing groups by ${field}:`)
        for (const key of comparison.top[field]) {
          const g = comparison.groups.find((group) => group.key === key)
          print(
            `${g.type}/${g.name}: delta count=${g.delta.count}, self=${g.delta.selfBytes} B, retained=${g.delta.retainedBytes} B; end count=${g.end.count}, self=${g.end.selfBytes} B, retained=${g.end.retainedBytes} B; since previous count=${g.sincePrevious.count}, self=${g.sincePrevious.selfBytes} B, retained=${g.sincePrevious.retainedBytes} B`
          )
        }
      }
      for (const g of comparison.groups.filter((group) => group.samples.length)) {
        print(`Samples ${g.type}/${g.name}:`)
        for (const sample of g.samples) {
          print(
            `  id=${sample.id}, baseline=${sample.presentAtBaseline}, first seen=${sample.firstSeenCapture}, self=${sample.selfBytes}, retained=${sample.retainedBytes}, retained delta=${sample.retainedDeltaBytes}; ${sample.ownerHints.join('; ')}`
          )
          print(
            `  ROOT ${sample.rootPath.map(({ node, edge }) => `${edge ? `--${edge.type}:${edge.name}--> ` : ''}${node.type}/${node.name}#${node.id}`).join(' ')}`
          )
          if (sample.baselineNode)
            print(
              `  BASELINE retained=${sample.baselineNode.retainedBytes} B; DOMINATORS ${sample.baselineDominators.map((node) => `${node.type}/${node.name}#${node.id}(${node.retainedBytes} B)`).join(' <- ')}`
            )
          print(
            `  DOMINATORS ${sample.dominators.map((node) => `${node.type}/${node.name}#${node.id}(${node.retainedBytes} B)`).join(' <- ')}`
          )
        }
      }
      previousFile = file
      previous = end
    }
    fs.writeSync(json, ']}\n')
  } finally {
    fs.closeSync(json)
  }
  if ((manifestFile || evidenceFile) && !status.complete)
    throw new Error('Incomplete diagnostic evidence; partial analysis retained')
}

if (require.main === module) {
  try {
    main(process.argv.slice(2))
  } catch (error) {
    console.error(error)
    process.exitCode = 1
  }
}

module.exports = { analyseSnapshot, compareSnapshots, main }
