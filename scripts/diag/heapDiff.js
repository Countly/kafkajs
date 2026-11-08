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
  return { n, groups, unreachable, retained, idom, describe, rootPath, groupIndex }
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
  return { top, groups }
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
  const outAt = argv.indexOf('--out')
  if (outAt < 2 || outAt !== argv.length - 2)
    throw new Error(
      'Usage: node heapDiff.js baseline.heapsnapshot end.heapsnapshot [...] --out output-prefix'
    )
  const files = argv.slice(0, outAt)
  const output = argv[outAt + 1]
  const baseline = readSnapshot(files[0])
  const notes = [
    'DIAGNOSTIC ONLY — not a soak acceptance receipt.',
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
      baseline: { file: files[0], groups: baseline.groups, unreachable: baseline.unreachable },
    }).slice(0, -1) + ',"comparisons":['
  )
  try {
    for (const note of notes) print(note)
    let previousFile = files[0]
    let previousGroups = new Map(baseline.groups.map((g) => [g.key, g]))
    const firstSeen = new Map()
    for (let v = 0; v < baseline.n; v++) firstSeen.set(baseline.describe(v).id, files[0])
    for (const [index, file] of files.slice(1).entries()) {
      const end = readSnapshot(file)
      const comparison = {
        file,
        previousFile,
        unreachable: end.unreachable,
        ...compareSnapshots(baseline, end),
      }
      for (let v = 0; v < end.n; v++) {
        const id = end.describe(v).id
        if (!firstSeen.has(id)) firstSeen.set(id, file)
      }
      for (const group of comparison.groups) {
        const previous = previousGroups.get(group.key)
        group.sincePrevious = Object.fromEntries(
          Object.keys(group.end).map((field) => [
            field,
            group.end[field] - (previous?.[field] ?? 0),
          ])
        )
        for (const sample of group.samples) sample.firstSeenCapture = firstSeen.get(sample.id)
      }
      fs.writeSync(json, (index ? ',' : '') + JSON.stringify(comparison))
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
      previousGroups = new Map(end.groups.map((g) => [g.key, g]))
    }
    fs.writeSync(json, ']}\n')
  } finally {
    fs.closeSync(json)
  }
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
