'use strict'
/**
 * Reduces the V8 heap snapshots of a final soak run to the per-boundary summary the core checker
 * judges in its heap row (K5), and keeps only the snapshot files a reader needs to explain that
 * verdict.
 *
 * Judged bytes: the self bytes of every node of a snapshot except the nodes of type `code`. V8
 * gives that type to compiled code and its bookkeeping: machine code and its instruction streams,
 * bytecode, feedback vectors, cells and metadata, inline-cache handlers, deoptimisation data and
 * source positions, scope and shared-function infos, scripts and the literal boilerplates of
 * compiled functions. Every other type is judged, whatever owns it, and so is a type this module
 * has never seen. A snapshot holds only live nodes (V8 collects before it writes one), so the
 * judged bytes are all live heap bytes but code.
 *
 * `summarizeRun` runs after the run, outside every measured process. Each record of
 * `heap/summary.json` holds the supervisor's receipt, the snapshot's size and sha256, its total,
 * judged and excluded bytes, the excluded bytes by name category, the judged bytes by node type
 * and by owner class, and the constructors and closures that grew most since the process's
 * previous usable snapshot. The snapshot files kept are the first and the last usable one of each
 * process, each whose judged bytes exceed the first by more than half the endpoint bound, and each
 * that could not be read; the others are deleted once summarised.
 *
 * Cost per snapshot: about five times its file size in memory while two are held (a 15 MB file
 * parsed, analysed and compared in under a second and 140 MB of RSS).
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { readSnapshot, analyseSnapshot } = require('../e2e/telemetry/retained/heapDiff')
const { ownersOf, compare, classOf } = require('./heapSnapshotReport')

/** The one snapshot node type the heap row does not judge. */
const EXCLUDED_TYPE = 'code'
/** A snapshot whose judged bytes exceed its process's first by this share of the bound is kept. */
const KEEP_FRACTION = 0.5
const SUMMARY_FILE = 'heap/summary.json'
const SNAPSHOT_FILE = /^heap\/p\d\d\.h\d\d\.heapsnapshot$/
const TOP_GROWING = 10

/**
 * @param {string} name the name of a `code` node
 * @returns {string} its category: `(…)` names without their function, `system / <type>`,
 *   `(builtin code)`, `(script)` for a script path, `(unnamed)` or `(function)` for a shared
 *   function info named by its function
 */
function categoryOf(name) {
  if (name === '') return '(unnamed)'
  const paren = /^\((.*?)(?: for .*)?\)$/.exec(name)
  if (paren) return / builtin code$/.test(paren[1]) ? '(builtin code)' : `(${paren[1]})`
  const system = /^system \/ (\S+)/.exec(name)
  if (system) return `system / ${system[1]}`
  return /^(node:|file:|\/|[A-Za-z]:\\)/.test(name) ? '(script)' : '(function)'
}

/** @param {object} into @param {string} key @param {number} bytes @returns {void} */
const add = (into, key, bytes) => {
  into[key] = (into[key] ?? 0) + bytes
}

/**
 * Totals the nodes of one parsed snapshot.
 * @param {{ snapshot: object, nodes: Float64Array, strings: string[] }} parsed a `readSnapshot` result
 * @returns {{ nodes: number, totalBytes: number, judgedBytes: number, excludedBytes: number,
 *   excluded: Record<string, number>, judgedByType: Record<string, number> }}
 * @throws {Error} when the snapshot has no type, name or self size field
 */
function totalsOf({ snapshot, nodes, strings }) {
  const fields = snapshot.meta.node_fields
  const [type, name, size] = ['type', 'name', 'self_size'].map((f) => fields.indexOf(f))
  if (Math.min(type, name, size) < 0) throw new Error('the snapshot lacks a node field')
  const types = snapshot.meta.node_types[type]
  const totals = {
    nodes: nodes.length / fields.length,
    totalBytes: 0,
    judgedBytes: 0,
    excludedBytes: 0,
    excluded: {},
    judgedByType: {},
  }
  for (let i = 0; i < nodes.length; i += fields.length) {
    const kind = String(types[nodes[i + type]])
    const bytes = nodes[i + size]
    totals.totalBytes += bytes
    if (kind === EXCLUDED_TYPE) {
      totals.excludedBytes += bytes
      add(totals.excluded, categoryOf(strings[nodes[i + name]]), bytes)
    } else {
      totals.judgedBytes += bytes
      add(totals.judgedByType, kind, bytes)
    }
  }
  return totals
}

/**
 * @param {object} graph an analysed snapshot @param {string[]} owners its node owners
 * @returns {Record<string, number>} the judged self bytes by owner class
 */
function judgedByOwner(graph, owners) {
  const classes = {}
  for (let v = 0; v < graph.n; v++) {
    const { type, selfBytes } = graph.describe(v)
    if (type !== EXCLUDED_TYPE) add(classes, classOf(owners[v] ?? '(unattributed)'), selfBytes)
  }
  return classes
}

/**
 * Summarises every snapshot receipt of a finished run into `heap/summary.json` and deletes the
 * snapshot files it does not keep. Each process's receipts are read in boundary order, holding at
 * most two parsed snapshots at a time.
 * @param {string} out the artifact directory
 * @param {{ slot: string, boundary: string, file: string, at: number|null, ms: number|null,
 *   error: string|null }[]} receipts the supervisor's heap snapshot receipts
 * @param {number} bound the heap endpoint bound in bytes
 * @returns {{ excludedType: string, keepFraction: number, records: object[] }} what was written
 */
function summarizeRun(out, receipts, bound) {
  const bySlot = new Map()
  for (const r of receipts) bySlot.set(r.slot, [...(bySlot.get(r.slot) ?? []), r])
  const records = []
  for (const list of bySlot.values()) {
    const usable = []
    let previous = null
    for (const receipt of [...list].sort((a, b) => a.boundary.localeCompare(b.boundary))) {
      const { slot, boundary, file, at, ms } = receipt
      const record = { slot, boundary, file, at, ms, error: receipt.error ?? null, kept: false }
      records.push(record)
      if (record.error !== null) continue
      try {
        if (!SNAPSHOT_FILE.test(file)) throw new Error(`${file} is not a heap snapshot path`)
        const full = path.join(out, file)
        const bytes = fs.readFileSync(full)
        const parsed = readSnapshot(full)
        const graph = analyseSnapshot(parsed)
        const current = { graph, owners: ownersOf(graph), boundary }
        Object.assign(record, {
          snapshotBytes: bytes.length,
          sha256: crypto.createHash('sha256').update(bytes).digest('hex'),
          ...totalsOf(parsed),
          judgedByOwner: judgedByOwner(graph, current.owners),
          growingSince: previous?.boundary ?? null,
          growing: previous
            ? compare(previous, current)
                .groups.slice(0, TOP_GROWING)
                .map(({ type, name, owner, count, countDelta, delta, path: retainer }) => ({
                  type,
                  name,
                  owner,
                  ownerClass: classOf(owner),
                  count,
                  countDelta,
                  delta,
                  path: retainer,
                }))
            : [],
        })
        previous = current
        if (usable.length > 0 && record.judgedBytes - usable[0].judgedBytes > KEEP_FRACTION * bound)
          record.kept = true
        usable.push(record)
      } catch (e) {
        record.error = `unreadable snapshot: ${e.message}`.slice(0, 200)
        record.kept = SNAPSHOT_FILE.test(file) && fs.existsSync(path.join(out, file))
      }
    }
    if (usable.length > 0) usable[0].kept = usable.at(-1).kept = true
  }
  for (const r of records)
    if (!r.kept && r.error === null) fs.rmSync(path.join(out, r.file), { force: true })
  const summary = { excludedType: EXCLUDED_TYPE, keepFraction: KEEP_FRACTION, records }
  fs.mkdirSync(path.join(out, 'heap'), { recursive: true })
  fs.writeFileSync(path.join(out, SUMMARY_FILE), JSON.stringify(summary) + '\n')
  return summary
}

module.exports = {
  EXCLUDED_TYPE,
  KEEP_FRACTION,
  SUMMARY_FILE,
  SNAPSHOT_FILE,
  categoryOf,
  totalsOf,
  summarizeRun,
}
