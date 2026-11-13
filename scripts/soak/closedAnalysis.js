#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const zlib = require('node:zlib')
const { pipeline } = require('node:stream/promises')
const { Transform } = require('node:stream')
const {
  readSnapshot,
  analyseSnapshot,
  ownershipIndex,
  traceOwner,
} = require('../e2e/telemetry/retained/heapDiff')
const { selfLedger } = require('../e2e/telemetry/retained/ownership')
const { FLAGS } = require('./closedDiagnostic')

const SCOPES = ['product', 'harness', 'engine', 'unresolved']
const CHECKPOINTS = ['baseline', 'warm', 'teardown', 'c0', 'c1']
const MAX_COMPRESSED_BYTES = 256 * 1024 * 1024

/**
 * Resolves a closure's V8 script name through metadata links, never through source text or context.
 * @param {object} graph analysed snapshot @param {number} start closure index
 * @returns {string} repository-relative source path, or empty when V8 supplies no recognised path
 */
const sourceOf = (graph, start) => {
  const queue = [start]
  const seen = new Set(queue)
  for (let i = 0; i < queue.length && i < 64; i++)
    for (const { target, edge } of graph.outgoing(queue[i])) {
      if (!['shared', 'script', 'script_or_debug_info', 'name'].includes(edge.name)) continue
      const name = graph.describe(target).name
      const match = /(?:^|\/)((?:src|scripts|testHelpers)\/[^\n]*\.js)$/.exec(name)
      if (match) return match[1]
      if (!seen.has(target)) {
        seen.add(target)
        queue.push(target)
      }
    }
  return ''
}

/**
 * Assigns additive self-byte labels. Named aliases identify the public src client and the scripts
 * registry/worker. Only state edges are walked; prototypes, compiler metadata, weak links, module
 * caches and global/native contexts are boundaries. Closure contexts are followed only when their
 * script is in the matching source tree. Unproven and multiply owned nodes remain unresolved.
 * @param {object} graph analysed strong graph @param {object} raw snapshot with optional V8 locations
 * @returns {object} disjoint ledger labels and a state-path lookup for sampled growth
 */
const labelsOf = (graph, raw) => {
  const ownership = ownershipIndex(graph)
  const owners = new Array(graph.n)
  const parents = new Int32Array(graph.n).fill(-1)
  const sources = new Map()
  const source = (v) => {
    if (!sources.has(v)) sources.set(v, sourceOf(graph, v))
    return sources.get(v)
  }
  const walk = (start, scope, owner) => {
    const queue = [{ v: start, owner, parent: -1 }]
    const seen = new Set([start])
    for (let i = 0; i < queue.length; i++) {
      const entry = queue[i]
      const v = entry.v
      const node = graph.describe(v)
      if (
        node.type === 'synthetic' ||
        node.type === 'code' ||
        ['Module', 'global', 'system / NativeContext'].includes(node.name)
      )
        continue
      if (
        node.type === 'closure' &&
        !(scope === 'product' ? /^src\// : /^(scripts|testHelpers)\//).test(source(v))
      )
        continue
      const label = `${scope}\0${entry.owner}`
      const previous = owners[v]
      if (previous && previous.split('\0')[0] !== scope) owners[v] = 'unresolved\0shared-state'
      else if (!previous) {
        owners[v] = label
        parents[v] = entry.parent
      }
      for (const { target, edge } of graph.outgoing(v)) {
        if (
          seen.has(target) ||
          !['property', 'element', 'context', 'internal'].includes(edge.type) ||
          [
            '__proto__',
            'prototype',
            'constructor',
            'map',
            'shared',
            'script',
            'code',
            'native_context',
            'previous',
            'scope_info',
          ].includes(edge.name) ||
          (scope === 'harness' && edge.name === 'client') ||
          (edge.type === 'internal' &&
            !['context', 'elements', 'properties', 'table', 'buffer', 'backing_store'].includes(
              edge.name
            ))
        )
          continue
        seen.add(target)
        queue.push({
          v: target,
          owner: v === start ? `${owner}.${edge.name}` : entry.owner,
          parent: v,
        })
      }
    }
  }
  for (let v = 0; v < graph.n; v++)
    for (const link of graph.outgoing(v)) {
      if (link.edge.type !== 'property' || link.edge.name !== '__telemetryClosedRoots') continue
      for (const { target, edge } of graph.outgoing(link.target)) {
        if (edge.name === 'client') walk(target, 'product', 'client')
        if (edge.name === 'registry') walk(target, 'harness', 'registry')
        if (edge.name === 'harness') walk(target, 'harness', 'harness')
      }
    }
  const labels = Array.from({ length: graph.n }, (_, v) => {
    const node = graph.describe(v)
    const category = ownership.categories[v]
    let owner = owners[v] || 'unresolved\0unproven'
    if (
      ['compiled-method-storage', 'feedback-storage', 'script-storage', 'module-source'].includes(
        category
      ) &&
      (category !== 'feedback-storage' ||
        node.type !== 'object' ||
        /^system \/ (Feedback|AllocationSite)/.test(node.name))
    )
      owner = `engine\0${category}`
    // Compiler objects receive engine labels only for their own bytes, never for descendants.
    if (
      /BytecodeArray|SharedFunctionInfo|FeedbackVector|FeedbackCell|CompilationCache/.test(
        node.name
      )
    )
      owner = `engine\0${node.name}`
    return `${owner}\0${node.type}\0${/string/.test(node.type) ? '(string)' : node.name.slice(0, 160)}`
  })
  const positions = new Map()
  const fields = raw.snapshot.meta.location_fields || []
  if (fields.length)
    for (let i = 0; i < (raw.locations?.length || 0); i += fields.length) {
      const index =
        raw.locations[i + fields.indexOf('object_index')] / raw.snapshot.meta.node_fields.length
      const line = raw.locations[i + fields.indexOf('line')]
      if (
        Number.isInteger(index) &&
        index >= 0 &&
        index < graph.n &&
        Number.isInteger(line) &&
        line >= 0
      )
        positions.set(index, line + 1)
    }
  /**
   * Gives a concrete state-field chain from a named client or registry, when the walk proved one.
   * Source positions name the nearest owning closure, not an inferred allocation site.
   * @param {number} v growth node @returns {object} reachability, owner field and optional file:line
   */
  const ownerOf = (v) => {
    const unproven = { reachable: null, root: null, field: null, source: null }
    if (!owners[v] || owners[v].startsWith('unresolved')) return unproven
    const chain = []
    let location = null
    let current = v
    for (let depth = 0; depth < graph.n; depth++) {
      if (!location && graph.describe(current).type === 'closure') {
        const file = source(current)
        const line = positions.get(current)
        if (file && line) location = { file, line }
      }
      const parent = parents[current]
      if (parent === -1) break
      const link = graph.outgoing(parent).find(({ target }) => target === current)
      chain.push(String(link.edge.name))
      current = parent
    }
    const root = owners[current]?.split('\0')[1]
    if (!['client', 'registry'].includes(root)) return unproven
    return { reachable: true, root, field: [root, ...chain.reverse()].join('.'), source: location }
  }
  return { labels, ownerOf }
}

/**
 * Bounds graph labels (including source strings) without dropping stable node IDs.
 * @param {object} value graph node or trace @returns {object} compact serializable evidence
 */
const compact = (value) =>
  JSON.parse(
    JSON.stringify(value, (key, entry) =>
      typeof entry === 'string' && entry.length > 240 ? `${entry.slice(0, 240)}…` : entry
    )
  )

/**
 * Reuses the retained graph reader, dominators, ownership index and stable-ID additive ledger.
 * @param {object} beforeRaw earlier V8 snapshot @param {object} afterRaw later V8 snapshot
 * @returns {object} reconciled self bytes by scope/constructor/owner and bounded growth paths
 */
const compareSnapshots = (beforeRaw, afterRaw) => {
  const before = analyseSnapshot(beforeRaw)
  const after = analyseSnapshot(afterRaw)
  const { labels: beforeLabels } = labelsOf(before, beforeRaw)
  const { labels: afterLabels, ownerOf } = labelsOf(after, afterRaw)
  const ledger = selfLedger(before, after, beforeLabels, afterLabels)
  const byScope = Object.fromEntries(
    SCOPES.map((scope) => [scope, { before: 0, after: 0, delta: 0 }])
  )
  const groups = Object.entries(ledger).map(([key, row]) => {
    const [scope, owner, type, constructor] = key.split('\0')
    for (const field of ['before', 'after', 'delta']) byScope[scope][field] += row[field]
    return { scope, owner, type, constructor, ...row, paths: [], key }
  })
  const oldIds = new Map()
  for (let v = 0; v < before.n; v++)
    if (before.groupIndex[v] >= 0) oldIds.set(before.describe(v).id, v)
  const top = groups
    .filter((row) => row.delta > 0)
    .sort((a, b) => b.delta - a.delta)
    .slice(0, 20)
  const selected = new Map(top.map((row) => [row.key, row]))
  for (let v = 0; v < after.n; v++) {
    if (after.groupIndex[v] < 0) continue
    const group = selected.get(afterLabels[v])
    if (!group) continue
    const node = after.describe(v)
    const old = oldIds.get(node.id)
    const growth =
      node.selfBytes -
      (old === undefined || beforeLabels[old] !== afterLabels[v]
        ? 0
        : before.describe(old).selfBytes)
    if (growth <= 0) continue
    group.paths.push({ index: v, growth, node })
    group.paths.sort((a, b) => b.growth - a.growth)
    group.paths.length = Math.min(group.paths.length, 2)
  }
  for (const group of top)
    group.paths = group.paths.map(({ index, ...row }) => {
      const trace = compact(traceOwner(after, index))
      return {
        ...row,
        node: compact(row.node),
        owner: compact(ownerOf(index)),
        trace: {
          rootPath: trace.rootPath.slice(-24),
          incoming: trace.incoming.slice(0, 8),
          owningProperties: trace.owningProperties.slice(0, 8),
          truncated:
            trace.rootPath.length > 24 ||
            trace.incoming.length > 8 ||
            trace.owningProperties.length > 8,
        },
      }
    })
  return {
    selfDelta: Object.values(byScope).reduce((sum, row) => sum + row.delta, 0),
    byScope,
    groups: groups
      .filter((row) => row.delta !== 0)
      .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))
      .slice(0, 60)
      .map(({ key, ...row }) => row),
    groupsTotal: groups.length,
    limitations:
      'Self bytes only; strong graph excludes weak edges, not an ephemeron proof. State/source boundaries and shared owners remain unresolved. Owner reachability is proven only for walked state edges; null means unproven. Source positions identify owning closures, not allocation sites. Snapshot GC and capture overhead perturb the run; engine statistics and snapshot self bytes are distinct measurements.',
  }
}

/**
 * Compresses available snapshots with a streaming byte bound; a failed or oversized output is
 * removed before upload. Originals are removed only after successful compression.
 * @param {string} directory worker directory @param {number} maxBytes total compressed limit
 * @returns {Promise<number>} compressed bytes across the five unique snapshots
 * @throws {Error} on compression failure or size overflow
 */
const compressSnapshots = async (directory, maxBytes) => {
  let bytes = 0
  for (const name of CHECKPOINTS) {
    const raw = path.join(directory, `${name}.heapsnapshot`)
    if (!fs.existsSync(raw)) continue
    const compressed = `${raw}.gz`
    const limit = new Transform({
      transform(chunk, encoding, callback) {
        bytes += chunk.length
        callback(
          bytes > maxBytes ? new Error('closed snapshots exceed compressed byte limit') : null,
          chunk
        )
      },
    })
    try {
      await pipeline(
        fs.createReadStream(raw),
        zlib.createGzip(),
        limit,
        fs.createWriteStream(compressed, { flags: 'wx' })
      )
    } catch (error) {
      fs.rmSync(compressed, { force: true })
      throw error
    }
    fs.unlinkSync(raw)
  }
  return bytes
}

/**
 * Analyses endpoint and control pairs serially and always archives a summary, including incomplete evidence.
 * The per-worker compressed cap bounds the four-worker snapshot artifact to 1 GiB.
 * @param {string} directory one worker directory @param {object} identity shared CI identity
 * @param {object} [options] bounded compression options, smaller limits for broker-free controls
 * @param {number} [options.maxBytes] total compressed bytes, 256 MiB by default
 * @returns {Promise<object>} compact diagnostic summary, also written to summary.json
 * @throws {Error} on missing/ineligible captures, malformed graphs, or artifact-size overflow
 */
const analyseWorker = async (directory, identity, { maxBytes = MAX_COMPRESSED_BYTES } = {}) => {
  const summary = { ...identity, ...FLAGS, complete: false }
  fs.mkdirSync(directory, { recursive: true })
  let failure
  try {
    const rows = Object.fromEntries(
      CHECKPOINTS.map((name) => {
        const row = JSON.parse(fs.readFileSync(path.join(directory, `${name}.json`), 'utf8'))
        if (
          row.diagnostic !== true ||
          row.acceptanceEligible !== false ||
          row.kind !== identity.kind ||
          row.checkpoint !== name
        )
          throw new Error(`ineligible or mismatched closed capture ${name}`)
        return [name, row]
      })
    )
    summary.readings = rows
    const pair = (name, end = 'teardown') => ({
      ...compareSnapshots(
        readSnapshot(path.join(directory, `${name}.heapsnapshot`), { locations: true }),
        readSnapshot(path.join(directory, `${end}.heapsnapshot`), { locations: true })
      ),
      retainedDelta: rows[end].memory.retainedBytes - rows[name].memory.retainedBytes,
      codeDelta: Object.fromEntries(
        Object.keys(rows[end].code).map((field) => [
          field,
          rows[end].code[field] - rows[name].code[field],
        ])
      ),
      heapSpaces: rows[end].heapSpaces.map((space) => ({
        name: space.space_name,
        usedDelta:
          space.space_used_size -
          (rows[name].heapSpaces.find((row) => row.space_name === space.space_name)
            ?.space_used_size ?? 0),
      })),
    })
    summary.original = pair('baseline')
    summary.warm = pair('warm')
    summary.control = pair('c0', 'c1')
    summary.complete = true
  } catch (error) {
    failure = error
  }
  try {
    summary.compressedBytes = await compressSnapshots(directory, maxBytes)
  } catch (error) {
    failure = failure || error
  }
  if (failure) {
    summary.complete = false
    summary.error = failure.message
  }
  fs.writeFileSync(path.join(directory, 'summary.json'), JSON.stringify(summary))
  if (failure) throw failure
  return summary
}

/**
 * Serial offline analysis keeps only one worker pair in memory. Emits one table row per kind/pair.
 * @param {string} directory diagnostic root containing all four worker directories
 * @returns {Promise<void>}
 * @throws {Error} on incomplete captures or a failed worker analysis
 */
const main = async (directory) => {
  const identity = JSON.parse(fs.readFileSync(path.join(directory, 'identity.json'), 'utf8'))
  console.log('worker         pair      retainedKiB productKiB harnessKiB engineKiB unresolvedKiB')
  const failures = []
  for (const kind of ['producer', 'consumer', 'admin', 'shareConsumer']) {
    let summary
    try {
      summary = await analyseWorker(path.join(directory, kind), { ...identity, kind })
    } catch (error) {
      failures.push(`${kind}: ${error.message}`)
      continue
    }
    for (const pair of ['original', 'warm', 'control']) {
      const row = summary[pair]
      console.log(
        `${kind.padEnd(14)} ${pair.padEnd(9)} ${[row.retainedDelta, ...SCOPES.map((scope) => row.byScope[scope].delta)].map((bytes) => (bytes / 1024).toFixed(1).padStart(10)).join(' ')}`
      )
    }
  }
  if (failures.length) throw new Error(failures.join('; '))
}

module.exports = { compareSnapshots, analyseWorker, main }
if (require.main === module)
  main(process.argv[2]).catch((error) => {
    console.error(error)
    process.exitCode = 1
  })
