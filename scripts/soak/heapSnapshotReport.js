#!/usr/bin/env node
'use strict'
/**
 * Offline heap-growth report over the V8 heap snapshots a final soak run wrote at its quiet
 * boundaries (`<run dir>/heap/<slot>.<boundary>.heapsnapshot`, the files the run's heap summary
 * kept). For
 * each process it compares consecutive boundaries and prints where the live heap grew, by owner:
 * every reachable node is owned by the script of its own constructor or closure where the snapshot
 * names one, else by its immediate dominator's owner, and V8 code is its own rows (bytecode,
 * feedback, other compiled code). Owners are grouped as product (`src/`), harness (`scripts/soak/`,
 * `testHelpers/`), node internals (`node:`), dependencies, other scripts and unattributed. It then
 * lists the twenty constructors or closures whose self bytes grew most, each with the retainer path
 * of one instance new at the later boundary. Diagnostic only; it judges nothing.
 *
 *   node scripts/soak/heapSnapshotReport.js <run dir> [<slot>...]
 *
 * Memory: two parsed snapshots are held at once, at about five times the file size each (measured:
 * a 342 MB snapshot beside a 5 MB one peaked at 1.7 GB RSS and took 21 s), so a pair of 100 MB
 * snapshots needs about 1 GB. For snapshots past 500 MB, run node with `--max-old-space-size=8192`.
 */
const fs = require('node:fs')
const path = require('node:path')
const { readSnapshot, analyseSnapshot } = require('../e2e/telemetry/retained/heapDiff')
const { selfLedger } = require('../e2e/telemetry/retained/ownership')

const FILE = /^(p\d\d)\.(h\d\d)\.heapsnapshot$/
const UNATTRIBUTED = '(unattributed)'
const TOP_GROUPS = 20
const TOP_FILES = 10
const PATH_HOPS = 10

/**
 * @param {string} owner a script path, `v8:<row>` or the unattributed owner
 * @returns {string} the owner's class: product, harness, node, dependency, other script, a V8 row
 *   or unattributed
 */
const classOf = (owner) => {
  if (owner.startsWith('v8:') || owner === UNATTRIBUTED) return owner
  if (owner.startsWith('node:')) return 'node'
  if (/[\\/]node_modules[\\/]/.test(owner)) return 'dependency'
  if (/[\\/](scripts[\\/]soak|testHelpers)[\\/]/.test(owner)) return 'harness'
  if (/[\\/]src[\\/]/.test(owner)) return 'product'
  return 'other script'
}

/**
 * Labels every reachable node of an analysed snapshot with its owner, in dominator order so a
 * dominator is labelled before what it dominates.
 * @param {object} graph an `analyseSnapshot` result
 * @returns {string[]} the owner of each node index; unreachable nodes are left undefined
 */
const ownersOf = (graph) => {
  const target = (v, type, name) =>
    graph.outgoing(v).find(({ edge }) => edge.type === type && edge.name === name)?.target
  const scripts = new Map()
  /** @param {number} closure @returns {string|null} the path of the script defining it */
  const scriptOf = (closure) => {
    if (!scripts.has(closure)) {
      const shared = target(closure, 'internal', 'shared')
      const script = shared === undefined ? undefined : target(shared, 'internal', 'script')
      scripts.set(closure, script === undefined ? null : graph.describe(script).name || null)
    }
    return scripts.get(closure)
  }
  const byMap = new Map()
  /** @param {number} object @returns {string|null} the script of its prototype's constructor */
  const constructorScript = (object) => {
    const map = target(object, 'internal', 'map')
    if (map === undefined) return null
    if (!byMap.has(map)) {
      const prototype = target(map, 'internal', 'prototype')
      const ctor =
        prototype === undefined ? undefined : target(prototype, 'property', 'constructor')
      byMap.set(
        map,
        ctor !== undefined && graph.describe(ctor).type === 'closure' ? scriptOf(ctor) : null
      )
    }
    return byMap.get(map)
  }
  const owners = new Array(graph.n)
  for (let i = 1; i <= graph.visited; i++) {
    const v = graph.vertex[i]
    const { type, name } = graph.describe(v)
    let owner = null
    if (type === 'code') {
      if (/Bytecode/.test(name)) owner = 'v8:bytecode'
      else if (/Feedback/.test(name)) owner = 'v8:feedback'
      else if (/^(node:|file:|\/|[A-Za-z]:\\)/.test(name)) owner = name
      else owner = 'v8:compiled-code'
    } else if (type === 'closure') owner = scriptOf(v)
    else if (type === 'object') owner = constructorScript(v)
    owners[v] = owner ?? (v === graph.root ? UNATTRIBUTED : owners[graph.idom[v]])
  }
  return owners
}

/**
 * @param {object} graph @param {string[]} owners @returns {Map<string, object>} self bytes and
 *   counts of each object or closure group, keyed by type, name and owner
 */
const groupsOf = (graph, owners) => {
  const groups = new Map()
  for (let v = 0; v < graph.n; v++) {
    if (owners[v] === undefined) continue
    const { type, name, selfBytes } = graph.describe(v)
    if (type !== 'object' && type !== 'closure') continue
    const key = `${type}\0${name}\0${owners[v]}`
    const group = groups.get(key) ?? { type, name, owner: owners[v], count: 0, selfBytes: 0 }
    group.count++
    group.selfBytes += selfBytes
    groups.set(key, group)
  }
  return groups
}

/**
 * @param {object} graph @param {number} v @returns {string} the last hops of a shortest strong
 *   path from the roots to `v`, as `edge` steps ending at the node
 */
const retainerPath = (graph, v) => {
  const hops = graph.rootPath(v)
  const step = ({ edge, node }) => {
    const label = (node.name || node.type).replace(/\s+/g, ' ').slice(0, 40)
    if (!edge) return label
    const via = ['element', 'hidden'].includes(edge.type) ? `[${edge.name}]` : `.${edge.name}`
    return `${via} ${label}`
  }
  const shown = hops.slice(-PATH_HOPS).map(step)
  return (hops.length > PATH_HOPS ? '… ' : '') + shown.join(' > ')
}

/**
 * Compares two snapshots of one process.
 * @param {{ graph: object, owners: string[] }} before @param {{ graph: object, owners: string[] }} after
 * @returns {object} totals, rows by owner class, the top growing files and groups
 */
const compare = (before, after) => {
  const ledger = selfLedger(before.graph, after.graph, before.owners, after.owners)
  const classes = {}
  const files = []
  for (const [owner, row] of Object.entries(ledger)) {
    const into = (classes[classOf(owner)] ??= {
      before: 0,
      after: 0,
      delta: 0,
      added: 0,
      removed: 0,
    })
    for (const k of ['before', 'after', 'delta', 'added', 'removed']) into[k] += row[k]
    if (!owner.startsWith('v8:') && owner !== UNATTRIBUTED && row.delta !== 0)
      files.push({ owner, ...row })
  }
  const ids = new Set()
  for (let v = 0; v < before.graph.n; v++)
    if (before.owners[v] !== undefined) ids.add(before.graph.describe(v).id)
  const old = groupsOf(before.graph, before.owners)
  const grown = [...groupsOf(after.graph, after.owners)]
    .map(([key, g]) => ({
      key,
      ...g,
      countDelta: g.count - (old.get(key)?.count ?? 0),
      delta: g.selfBytes - (old.get(key)?.selfBytes ?? 0),
    }))
    .filter((g) => g.delta > 0)
    .sort((a, b) => b.delta - a.delta)
    .slice(0, TOP_GROUPS)
  const wanted = new Map(grown.map((g) => [g.key, g]))
  for (let v = 0; v < after.graph.n && wanted.size > 0; v++) {
    if (after.owners[v] === undefined) continue
    const { type, name, id } = after.graph.describe(v)
    const key = `${type}\0${name}\0${after.owners[v]}`
    if (!wanted.has(key) || ids.has(id)) continue
    wanted.get(key).path = retainerPath(after.graph, v)
    wanted.delete(key)
  }
  const total = (side) => Object.values(classes).reduce((sum, row) => sum + row[side], 0)
  return {
    before: total('before'),
    after: total('after'),
    classes,
    files: files.sort((a, b) => b.delta - a.delta).slice(0, TOP_FILES),
    groups: grown.map(({ key, ...g }) => ({ path: null, ...g })),
  }
}

/**
 * Finds the snapshots of a run directory and compares each process's consecutive boundaries,
 * holding at most two parsed snapshots at a time.
 * @param {string} dir the run directory @param {string[]} [slots] only these processes
 * @returns {{ slot: string, kind: string|null, from: string, to: string, diff: object }[]}
 */
function report(dir, slots = []) {
  const heap = path.join(dir, 'heap')
  const bySlot = new Map()
  for (const file of fs.readdirSync(heap).sort()) {
    const match = FILE.exec(file)
    if (!match || !fs.statSync(path.join(heap, file)).isFile()) continue
    if (slots.length > 0 && !slots.includes(match[1])) continue
    bySlot.set(match[1], [...(bySlot.get(match[1]) ?? []), { boundary: match[2], file }])
  }
  let config = null
  try {
    config = JSON.parse(fs.readFileSync(path.join(dir, 'config.json'), 'utf8'))
  } catch {}
  const kindOf = (slot) =>
    config?.clients.find((c) => c.process === slot && c.purpose !== 'observer')?.clientKind ??
    config?.processes.find((p) => p.slot === slot)?.role ??
    null
  const load = (file) => {
    const graph = analyseSnapshot(readSnapshot(path.join(heap, file)))
    return { graph, owners: ownersOf(graph) }
  }
  const results = []
  for (const [slot, list] of bySlot) {
    let before = null
    for (const { boundary, file } of list) {
      const after = load(file)
      if (before)
        results.push({
          slot,
          kind: kindOf(slot),
          from: before.boundary,
          to: boundary,
          diff: compare(before, after),
        })
      before = { ...after, boundary }
    }
  }
  return results
}

/** @param {number} n bytes @returns {string} signed, grouped */
const bytes = (n) => `${n > 0 ? '+' : ''}${Math.round(n).toLocaleString('en-US')}`

/**
 * @param {ReturnType<typeof report>} results
 * @returns {string} the printed report
 */
function format(results) {
  const lines = []
  const mib = (n) => (n / 1048576).toFixed(2)
  for (const { slot, kind, from, to, diff } of results) {
    lines.push(
      `${slot}${kind ? ` (${kind})` : ''} ${from} -> ${to}: live self bytes ${mib(diff.before)} -> ${mib(diff.after)} MiB (${bytes(diff.after - diff.before)} B)`,
      `  ${'owner'.padEnd(18)}${['before', 'after', 'delta', 'added', 'removed'].map((h) => h.padStart(14)).join('')}`
    )
    for (const [owner, row] of Object.entries(diff.classes).sort((a, b) => b[1].delta - a[1].delta))
      lines.push(
        `  ${owner.padEnd(18)}${['before', 'after'].map((k) => row[k].toLocaleString('en-US').padStart(14)).join('')}${['delta', 'added', 'removed'].map((k) => bytes(row[k]).padStart(14)).join('')}`
      )
    lines.push('  files by growth:')
    for (const f of diff.files)
      lines.push(`    ${bytes(f.delta).padStart(12)} B  ${classOf(f.owner).padEnd(12)} ${f.owner}`)
    lines.push(`  top ${TOP_GROUPS} growing constructors and closures:`)
    for (const g of diff.groups)
      lines.push(
        `    ${bytes(g.delta).padStart(12)} B ${bytes(g.countDelta).padStart(9)}  ${g.type} ${g.name}  [${classOf(g.owner)}] ${g.owner}`,
        `        retained by: ${g.path ?? '(no new instance)'}`
      )
    lines.push('')
  }
  return lines.join('\n')
}

if (require.main === module) {
  const [dir, ...slots] = process.argv.slice(2)
  if (!dir) {
    console.error('usage: node scripts/soak/heapSnapshotReport.js <run dir> [<slot>...]')
    process.exit(2)
  }
  const results = report(path.resolve(dir), slots)
  if (results.length === 0) {
    console.error(`no two snapshots of one process under ${path.join(dir, 'heap')}`)
    process.exit(1)
  }
  process.stdout.write(format(results))
}

module.exports = { report, format, classOf, ownersOf, compare }
