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
 * previous usable snapshot. A record is bound to its own snapshot: the receipt must name
 * `snapshotFile(slot, boundary)` and the size the worker recorded, else the record carries an
 * error and no totals. Every snapshot file stays, so the checker can re-derive each number it
 * judges from the file itself.
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
const SUMMARY_FILE = 'heap/summary.json'
/** Why finalize could not write `SUMMARY_FILE`, when it could not. */
const SUMMARY_ERROR_FILE = 'heap/summary-error.txt'
const TOP_GROWING = 10

/**
 * @param {string} slot a process slot @param {string} boundary a quiet boundary
 * @returns {string} the artifact path of that process's snapshot of that boundary
 */
const snapshotFile = (slot, boundary) => `heap/${slot}.${boundary}.heapsnapshot`

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
 * Summarises every snapshot receipt of a finished run into `heap/summary.json`, keeping every
 * snapshot file. Each process's receipts are read in boundary order, holding at most two parsed
 * snapshots at a time. A receipt that names another file than its own slot and boundary's, or a
 * size other than the file's, gets an error record and is never read as that snapshot.
 * @param {string} out the artifact directory
 * @param {{ slot: string, boundary: string, file: string, at: number|null, ms: number|null,
 *   bytes: number|null, error: string|null }[]} receipts the supervisor's heap snapshot receipts
 * @returns {{ excludedType: string, records: object[] }} what was written
 * @throws {Error} when the summary cannot be written
 */
function summarizeRun(out, receipts) {
  const bySlot = new Map()
  for (const r of receipts) bySlot.set(r.slot, [...(bySlot.get(r.slot) ?? []), r])
  const records = []
  for (const list of bySlot.values()) {
    let previous = null
    for (const receipt of [...list].sort((a, b) => a.boundary.localeCompare(b.boundary))) {
      const { slot, boundary, file, at, ms } = receipt
      const record = { slot, boundary, file, at, ms, error: receipt.error ?? null }
      records.push(record)
      if (record.error !== null) continue
      const own = snapshotFile(slot, boundary)
      if (file !== own) {
        record.error = `the receipt names ${file}, which is not ${own}`.slice(0, 200)
        continue
      }
      try {
        const full = path.join(out, file)
        const bytes = fs.readFileSync(full)
        if (bytes.length !== receipt.bytes) {
          record.error = `${file} has ${bytes.length} bytes, its receipt ${receipt.bytes}`
          continue
        }
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
      } catch (e) {
        record.error = `unreadable snapshot: ${e.message}`.slice(0, 200)
      }
    }
  }
  const summary = { excludedType: EXCLUDED_TYPE, records }
  fs.mkdirSync(path.join(out, 'heap'), { recursive: true })
  fs.writeFileSync(path.join(out, SUMMARY_FILE), JSON.stringify(summary) + '\n')
  return summary
}

module.exports = {
  EXCLUDED_TYPE,
  SUMMARY_FILE,
  SUMMARY_ERROR_FILE,
  snapshotFile,
  categoryOf,
  totalsOf,
  summarizeRun,
}
