'use strict'

/**
 * The core checker of a final-soak artifact: seven rows a person can review line by line.
 *
 *   K1 no lost record      K2 no lost delivery     K3 same-owner duplicates   K4 commits
 *   K5 heap                K6 resources            K7 exit and completeness
 *
 * Each row is PASS, FAIL or INCONCLUSIVE with the numbers behind it. The overall verdict is
 * ACCEPTED only for an acceptance run on which all seven PASS. Every other contract row is printed
 * as NOT JUDGED with the CI lane that holds its short-run evidence; no code path here can print
 * PASS for them, and the output says that it is a core verdict.
 *
 * Interface (the artifact directory is the runbook's `$out`):
 * - `config.json`, `header.json`, `supervisor.json`, `stream.ndjson` and the archived matrix as the
 *   driver writes them. The stream is read once, line by line, and never held.
 * - Ground truth taken inside the broker container by Apache's own tools, before finalize, in
 *   `apache/`: `<main|transactions>.dump` from `kafka-console-consumer.sh --partition 0 --offset
 *   earliest --isolation-level read_committed --timeout-ms 60000` with `print.partition`,
 *   `print.offset` and `print.key` set through `--formatter-property` (line
 *   `Partition:<p>\tOffset:<o>\t<key>\t<value>`); `groups/<group>.describe` from
 *   `kafka-consumer-groups.sh --describe --group <group>`. Each has `<file>.stderr` and
 *   `<file>.end` (`exit=<status> lines=<n>`, written after the command returned).
 * - A missing, truncated, unparseable or unbound ground-truth file makes the rows that need it
 *   INCONCLUSIVE, never PASS. The full interface is in the task's interface document.
 *
 * Memory: the journal keeps one Map entry plus about 50 bytes of typed arrays per offered id on
 * the two standing topics (at most `matrix.traffic.maxOffers`, about 150 MB at 1.6 million), one
 * entry per owned (client run, partition) key and a fixed sample grid per process. Handler entries
 * only update typed arrays, so state does not grow with traffic; an offer beyond the cap is a
 * reported problem, not a larger table.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const schema = require('../soak/finalSoakSchema')

const REVIEWED_MATRIX = path.join(__dirname, '../soak/finalSoakMatrix.json')
const ARCHIVED_MATRIX = 'archive/scripts/soak/finalSoakMatrix.json'
const ROLES = ['main', 'transactions']
const EXAMPLES = 3
const SHARE_SERIAL = 0xffffffff
const ACK = { none: 0, acknowledged: 1, unknown: 2, rejected: 3 }
const ROW_NAMES = {
  K1: 'no lost record',
  K2: 'no lost delivery',
  K3: 'same-owner duplicates',
  K4: 'commits',
  K5: 'heap',
  K6: 'resources',
  K7: 'exit and completeness',
}

/** Contract rows this checker does not judge, each with the existing CI lane that covers it in short runs. */
const NOT_JUDGED = Object.freeze([
  { name: 'share lock lifecycle and authority replay', lane: 'share-soak' },
  { name: 'ownership tables', lane: 'consumer-churn' },
  { name: 'seed sensitivity control', lane: 'telemetry' },
  { name: 'latency denominators', lane: 'consumer-churn' },
  { name: 'cooperative round counts', lane: 'consumer-churn' },
  { name: 'restart leave mode and stop mode', lane: 'consumer-churn' },
  { name: 'receipts identity', lane: 'share-soak' },
  { name: 'companions and prior checkpoints', lane: 'telemetry' },
  { name: 'recreate and regex topic contents', lane: 'consumer-churn' },
])

/** One row's findings: any failure makes it FAIL, else any gap (missing proof) makes it INCONCLUSIVE. */
class Row {
  /** @param {string} id `K1`..`K7` */
  constructor(id) {
    Object.assign(this, { id, name: ROW_NAMES[id], fails: [], gaps: [], numbers: {} })
  }

  /** @param {string} reason a violation @returns {void} */
  fail(reason) {
    this.fails.push(reason)
  }

  /** @param {string} reason missing evidence, or evidence that cannot prove the rule @returns {void} */
  gap(reason) {
    this.gaps.push(reason)
  }

  /** @returns {{ id: string, name: string, verdict: string, reasons: string[], numbers: object }} */
  done() {
    const verdict = this.fails.length ? 'FAIL' : this.gaps.length ? 'INCONCLUSIVE' : 'PASS'
    const { id, name, numbers } = this
    return { id, name, verdict, reasons: [...this.fails, ...this.gaps].slice(0, 30), numbers }
  }
}

/** @param {object} matrix @param {string} mode @returns {object} the sampling plan of the mode */
function planOf(matrix, mode) {
  const smoke = mode === 'smoke'
  const requestedMs = matrix.acceptance.modes[mode].requestedMs.value
  return {
    requestedMs,
    periodMs: matrix.sampling.periodMs.value,
    jitterMs: matrix.sampling.jitterMs.value,
    slots: smoke ? matrix.schedule.smoke.periodicSlots : matrix.sampling.periodicSlots.value,
    hours: smoke ? 1 : requestedMs / matrix.schedule.hourMs,
    readings: matrix.sampling.quietReadings.value,
  }
}

/**
 * The run-scoped groups that read a standing topic: the subject of each recipient kind, plus the
 * seed's control client on the main topic.
 * @param {object} matrix @param {object} config @param {string} role `main` or `transactions`
 * @returns {{ group: string, share: boolean }[]}
 */
function recipientsOf(matrix, config, role) {
  const entry = matrix.topology.topics.find((t) => t.role === role)
  const clients = entry.recipients.map((kind) =>
    config.clients.find((c) => c.clientKind === kind && c.purpose === 'subject')
  )
  if (entry.seed) clients.push(config.clients.find((c) => c.purpose === 'control'))
  return clients.map((c) => ({
    group: c.group,
    share: matrix.clientKinds[c.clientKind].role === 'share',
  }))
}

/**
 * Everything the rows need from the record stream, reduced as records arrive. Offered ids of the
 * two standing topics get an index; acknowledgement, per-group entry and sample facts live in
 * typed arrays or small per-process tables, so the journal never holds the stream.
 */
class Journal {
  /**
   * @param {{ matrix: object, config: object, mode: string, capacity?: number }} options
   *   `capacity` is the offered-id cap, the matrix's `traffic.maxOffers` by default
   */
  constructor({ matrix, config, mode, capacity = matrix.traffic.maxOffers.value }) {
    this.plan = planOf(matrix, mode)
    this.capacity = capacity
    this.problems = []
    this.topics = new Map(ROLES.map((role, n) => [config.topics?.[role], n]))
    if (this.topics.has(undefined)) this.problems.push('config.topics does not name the topics')
    this.groupIndex = new Map()
    this.recipients = ROLES.map((role) =>
      recipientsOf(matrix, config, role).map(({ group, share }) => {
        if (!this.groupIndex.has(group)) this.groupIndex.set(group, this.groupIndex.size)
        return { group, share, index: this.groupIndex.get(group) }
      })
    )
    this.entered = [...this.groupIndex.keys()].map(() => new Uint32Array(capacity))
    this.ids = new Map()
    this.names = []
    this.topic = new Uint8Array(capacity)
    this.ack = new Uint8Array(capacity)
    this.offset = new Float64Array(capacity).fill(NaN)
    this.txOf = []
    this.txs = new Map()
    this.owners = new Map()
    this.serial = 0
    this.sameOwner = { count: 0, examples: [] }
    this.repeatsAfterChange = 0
    this.shareRepeats = 0
    this.redundantAcquires = 0
    this.incidents = new Map()
    this.procs = new Map(config.processes.map(({ slot }) => [slot, this.newProcess()]))
  }

  /** @returns {object} the empty facts of one process */
  newProcess() {
    const periodic = new Float64Array(this.plan.slots).fill(NaN)
    return {
      started: false,
      exit: null,
      baseline: null,
      final: null,
      periodic,
      quiet: new Map(),
      repeats: 0,
    }
  }

  /** @returns {{ ids: number, transactions: number, ownedKeys: number }} the sizes of the growing tables */
  stateSize() {
    return { ids: this.names.length, transactions: this.txs.size, ownedKeys: this.owners.size }
  }

  /** @param {object} r one parsed, schema-valid record @returns {void} */
  add(r) {
    if (r.kind === 'identity') this.identity(r)
    else if (r.kind === 'boundary') this.boundary(r)
    else if (r.kind === 'sample') this.sample(r)
    else if (r.kind === 'process') this.process(r)
    else if (r.kind === 'diagnostic') {
      const kind = /^(missing-source|partial handler-enter|problem:)/.exec(r.message)?.[1]
      if (kind)
        this.incidents.set(kind, {
          count: (this.incidents.get(kind)?.count ?? 0) + 1,
          first: this.incidents.get(kind)?.first ?? r.message,
        })
    }
  }

  /** @param {object} r an identity record @returns {void} */
  identity(r) {
    if (r.stage === 'offered') this.offered(r)
    else if (r.stage === 'ack') this.acknowledge(r)
    else if (r.stage === 'transaction') this.transaction(r.transaction).outcome = r.outcome
    else if (r.stage === 'handler-enter') this.entry(r)
  }

  /** @param {string} id @returns {{ id: string, outcome: ?string, members: number[] }} the transaction */
  transaction(id) {
    if (!this.txs.has(id)) this.txs.set(id, { id, outcome: null, members: [] })
    return this.txs.get(id)
  }

  /** @param {object} r an `offered` record @returns {void} */
  offered(r) {
    const topic = this.topics.get(r.key.topic)
    if (topic === undefined) return
    if (r.key.partition !== 0) this.problems.push(`${r.id} is on partition ${r.key.partition}`)
    if (this.ids.has(r.id)) {
      this.problems.push(`${r.id} is offered twice`)
      return
    }
    const i = this.names.length
    if (i >= this.capacity) {
      this.over ??= !this.problems.push(`more than ${this.capacity} offered ids`)
      return
    }
    this.names.push(r.id)
    this.ids.set(r.id, i)
    this.topic[i] = topic
    if (r.transaction !== null) {
      this.txOf[i] = this.transaction(r.transaction)
      this.txOf[i].members.push(i)
    }
  }

  /** @param {object} r an `ack` record @returns {void} */
  acknowledge(r) {
    const i = this.ids.get(r.id)
    if (i === undefined) {
      if (this.topics.has(r.key.topic) && this.names.length < this.capacity)
        this.problems.push(`${r.id} is acknowledged but was never offered`)
      return
    }
    this.ack[i] = ACK[r.outcome]
    if (r.offset === null) return
    if (Number.isSafeInteger(Number(r.offset))) this.offset[i] = Number(r.offset)
    else this.problems.push(`${r.id} has an offset beyond 2^53`)
  }

  /** @returns {string} the identity of an owned key: client run and partition */
  static ownerKey({ client, run }, key) {
    return `${client}|${run}|${key.topic}|${key.partition}`
  }

  /**
   * An applied assignment change on one key: an acquire of a key not owned, or the end of a revoke
   * or a loss. An acquire of a key still owned changes nothing and is only counted.
   * @param {object} r a `boundary` record @returns {void}
   */
  boundary(r) {
    if (!this.topics.has(r.key.topic)) return
    const k = Journal.ownerKey(r, r.key)
    const owner = this.owners.get(k) ?? { owned: false, serial: 0 }
    this.owners.set(k, owner)
    if (r.event === 'acquire' && owner.owned) this.redundantAcquires++
    else if (r.event === 'acquire') Object.assign(owner, { owned: true, serial: ++this.serial })
    else if (r.event === 'revoke-end' || r.event === 'lost')
      Object.assign(owner, { owned: false, serial: ++this.serial })
  }

  /**
   * A handler entry. A repeat of an id in a group fails when it happens on the same client run and
   * key with no assignment change since the previous entry; any other repeat is only counted.
   * @param {object} r a `handler-enter` record @returns {void}
   */
  entry(r) {
    const group = this.groupIndex.get(r.group)
    const i = this.ids.get(r.id)
    if (!this.topics.has(r.key.topic) || group === undefined || i === undefined) return
    const share = r.authority === null
    let serial = SHARE_SERIAL
    if (!share) {
      const k = Journal.ownerKey(r.authority, r.key)
      this.owners.set(k, this.owners.get(k) ?? { owned: true, serial: ++this.serial })
      serial = this.owners.get(k).serial
    }
    const before = this.entered[group][i]
    this.entered[group][i] = serial
    if (before === 0) return
    if (share) this.shareRepeats++
    else if (before !== serial) this.repeatsAfterChange++
    else if (this.sameOwner.count++ < EXAMPLES)
      this.sameOwner.examples.push(`${r.id} in ${r.group}`)
  }

  /** @param {object} r a `process` record @returns {void} */
  process(r) {
    const p = this.procs.get(r.process)
    if (!p) return
    if (r.event === 'start') p.started = true
    else if (p.exit) p.repeats++
    else p.exit = { code: r.code, signal: r.signal }
  }

  /** @param {object} r a `sample` record @returns {void} */
  sample(r) {
    const p = this.procs.get(r.process)
    if (!p) return
    if (r.role === 'baseline' || r.role === 'final') {
      if (p[r.role]) p.repeats++
      p[r.role] = new Map(r.activeResources.map((x) => [x.type, x.count]))
    } else if (r.role === 'periodic') {
      if (!(r.slot >= 0 && r.slot < p.periodic.length) || !Number.isNaN(p.periodic[r.slot]))
        p.repeats++
      else p.periodic[r.slot] = r.admittedAt
    } else if (r.role === 'quiet') {
      const b = p.quiet.get(r.boundary) ?? new Map()
      p.quiet.set(r.boundary, b)
      if (b.has(r.reading)) p.repeats++
      b.set(r.reading, { heapUsed: r.heapUsed, completedAt: r.completedAt })
    }
  }
}

/**
 * Reads a file line by line without holding it, hashing the bytes as they pass.
 * @param {string} file @param {(line: string) => void} onLine
 * @returns {Promise<{ bytes: number, sha256: string, lines: number, unterminated: boolean }>}
 */
async function eachLine(file, onLine) {
  const hash = crypto.createHash('sha256')
  let [tail, lines, bytes] = ['', 0, 0]
  for await (const chunk of fs.createReadStream(file, {
    encoding: 'utf8',
    highWaterMark: 1 << 20,
  })) {
    hash.update(chunk)
    bytes += Buffer.byteLength(chunk)
    const parts = (tail + chunk).split('\n')
    tail = parts.pop()
    for (const line of parts) onLine(line, ++lines)
  }
  return { bytes, sha256: hash.digest('hex'), lines, unterminated: tail !== '' }
}

/** @param {Map<string, object>} manifest @param {string} file @param {{ bytes: number, sha256: string }} got @param {string[]} problems */
function checkBound(manifest, file, got, problems) {
  const entry = manifest.get(file)
  if (!entry) problems.push(`${file}: the header manifest does not bind it`)
  else if (entry.bytes !== got.bytes || entry.sha256 !== got.sha256)
    problems.push(`${file}: differs from the header manifest`)
}

/**
 * Reads one small ground-truth file whole and checks it against the header manifest.
 * @param {object} ctx `{ dir, manifest }` @param {string} name file below `apache/`
 * @param {string[]} problems receives every problem @param {string} [missing] the text for a missing file
 * @returns {string|null} the text, or null when it is missing
 */
function readBound({ dir, manifest }, name, problems, missing = 'file is missing') {
  let text
  try {
    text = fs.readFileSync(path.join(dir, 'apache', name), 'utf8')
  } catch {
    problems.push(`apache/${name}: ${missing}`)
    return null
  }
  const got = {
    bytes: Buffer.byteLength(text),
    sha256: crypto.createHash('sha256').update(text).digest('hex'),
  }
  checkBound(manifest, `apache/${name}`, got, problems)
  return text
}

/**
 * Reads a step's end marker (`exit=<status> lines=<n>`), which must report a zero exit status.
 * @param {object} ctx @param {string} name the step's main file @param {string[]} problems
 * @returns {number|null} the line count the step reports, or null
 */
function readEnd(ctx, name, problems) {
  const text = readBound(
    ctx,
    `${name}.end`,
    problems,
    'no end marker: the step was cut off or never ran'
  )
  const m = text === null ? null : /^exit=(-?\d+) lines=(\d+)\n$/.exec(text)
  if (text !== null && !m) problems.push(`apache/${name}.end: unparseable end marker`)
  if (m && m[1] !== '0') problems.push(`apache/${name}: the tool exit status was ${m[1]}`)
  return m ? Number(m[2]) : null
}

/**
 * Streams a topic dump and compares every record with what the journal says was offered.
 * @param {object} ctx `{ dir, manifest, journal, config, matrix, dumped }`
 * @param {string} role `main` or `transactions`
 * @returns {Promise<object>} problems (the file cannot be trusted), counts and findings
 */
async function readDump(ctx, role) {
  const { dir, journal, config, matrix, dumped } = ctx
  const name = `${role}.dump`
  const out = { problems: [], count: 0, maxOffset: -1, findings: {} }
  const note = (kind, text) => {
    const f = (out.findings[kind] ??= { count: 0, examples: [] })
    if (f.count++ < EXAMPLES) f.examples.push(text)
  }
  const end = readEnd(ctx, name, out.problems)
  const stderr = readBound(ctx, `${name}.stderr`, out.problems)
  const topic = ROLES.indexOf(role)
  const width = matrix.traffic.valueBytes.value
  let last = -1
  const read = await eachLine(path.join(dir, 'apache', name), (line, n) => {
    const m = /^Partition:0\tOffset:(\d+)\t([^\t]*)\t([^\t]*)$/.exec(line)
    if (!m) {
      if (out.problems.length < 5) out.problems.push(`apache/${name}: unparseable line ${n}`)
      return
    }
    const [offset, id, value] = [Number(m[1]), m[2], m[3]]
    if (offset <= last) out.problems.push(`apache/${name}: offsets are not increasing at line ${n}`)
    out.count++
    out.maxOffset = last = Math.max(last, offset)
    const i = journal.ids.get(id)
    if (i === undefined || journal.topic[i] !== topic)
      return note('foreign', `${id} at offset ${offset}`)
    if (dumped[i]) return note('duplicate', id)
    dumped[i] = 1
    const t = journal.txOf[i]
    if (t?.outcome === 'aborted') note('abortedPresent', id)
    if (journal.ack[i] === ACK.acknowledged && journal.offset[i] !== offset)
      note('wrongOffset', `${id} acknowledged at ${journal.offset[i]}, found at ${offset}`)
    if (value !== `${config.runId}|${id}|${t?.id ?? ''}|`.padEnd(width, '.')) note('differs', id)
  }).catch(() => null)
  if (read === null) {
    out.problems.push(`apache/${name}: file is missing`)
    return out
  }
  checkBound(ctx.manifest, `apache/${name}`, read, out.problems)
  if (read.unterminated) out.problems.push(`apache/${name}: the last line has no newline`)
  if (end !== null && end !== read.lines)
    out.problems.push(
      `apache/${name}: has ${read.lines} lines but its end marker counted ${end} lines`
    )
  const counted = [...(stderr ?? '').matchAll(/^Processed a total of (\d+) messages$/gm)]
  if (stderr !== null && (counted.length !== 1 || Number(counted[0][1]) !== read.lines))
    out.problems.push(
      `apache/${name}.stderr: Processed a total of line does not report ${read.lines}`
    )
  return out
}

/**
 * Parses one group's describe output.
 * @param {object} ctx `{ dir, manifest }` @param {string} group
 * @returns {{ problems: string[], rows: { topic: string, partition: number, current: ?number, logEnd: ?number }[] }}
 */
function readDescribe(ctx, group) {
  const name = `groups/${group}.describe`
  const out = { problems: [], rows: [] }
  if (!/^[A-Za-z0-9._-]+$/.test(group)) {
    out.problems.push(`${name}: unsafe group id`)
    return out
  }
  const end = readEnd(ctx, name, out.problems)
  const text = readBound(ctx, name, out.problems)
  readBound(ctx, `${name}.stderr`, out.problems)
  if (text === null) return out
  const lines = text.split('\n')
  if (end !== null && end !== lines.length - (text.endsWith('\n') ? 1 : 0))
    out.problems.push(`apache/${name}: has other than the ${end} lines its end marker counted`)
  const body = lines.filter((l) => l.trim() !== '')
  if (
    !/^GROUP\s+TOPIC\s+PARTITION\s+CURRENT-OFFSET\s+LOG-END-OFFSET\s+LAG(\s|$)/.test(body[0] ?? '')
  ) {
    out.problems.push(`apache/${name}: no describe header row`)
    return out
  }
  const num = (v) => (v === '-' ? null : Number(v))
  for (const line of body.slice(1)) {
    const m = /^(\S+)\s+(\S+)\s+(\d+)\s+(\d+|-)\s+(\d+|-)\s+(\d+|-)(\s.*)?$/.exec(line)
    if (!m || m[1] !== group)
      out.problems.push(`apache/${name}: unparseable line "${line.slice(0, 40)}"`)
    else out.rows.push({ topic: m[2], partition: +m[3], current: num(m[4]), logEnd: num(m[5]) })
  }
  return out
}

/** @returns {string} the first examples of a finding */
const first = (f) => ` (first ${f?.examples.join('; ')})`

/**
 * The offsets of the transactions partition that a read-committed reader cannot see but the
 * journal explains: one control marker per decided transaction that wrote there and every
 * acknowledged member of an aborted transaction the dump did not show. Uncertain sends and
 * outcomes widen the range instead of guessing.
 * @param {Journal} journal @param {Uint8Array} dumped @returns {[number, number]} lowest and highest
 */
function invisibleBounds(journal, dumped) {
  let [lo, hi] = [0, 0]
  const hidden = (list) => list.filter((i) => !dumped[i]).length
  for (const t of journal.txs.values()) {
    const acked = t.members.filter((i) => journal.ack[i] === ACK.acknowledged)
    const unsure = t.members.filter(
      (i) => journal.ack[i] === ACK.none || journal.ack[i] === ACK.unknown
    )
    const marker = acked.length > 0 ? 1 : 0
    const maybe = acked.length === 0 && unsure.length > 0 ? 1 : 0
    if (t.outcome === 'committed') {
      lo += marker
      hi += marker + maybe
    } else if (t.outcome === 'aborted') {
      lo += hidden(acked) + marker
      hi += hidden(acked) + marker + hidden(unsure) + maybe
    } else hi += hidden(acked) + hidden(unsure) + (t.members.length > 0 ? 1 : 0)
  }
  return [lo, hi]
}

/**
 * K1: every acknowledged id, and every non-rejected member of a committed transaction, is in the
 * read-committed dump once at its acknowledged offset, aborted members never, and the dump is
 * proved complete: the log end from Apache's describe equals the dumped records plus the offsets
 * the journal explains as invisible. Unknown results and outcomes are INCONCLUSIVE, never PASS.
 * @param {Row} k1 @param {object} ctx `{ journal, dumps, ends, dumped }`
 * @returns {boolean} whether the dump is usable and proved complete
 */
function judgeLog(k1, { journal, dumps, ends, dumped }) {
  const problems = dumps.flatMap((d) => d.problems)
  problems.forEach((p) => k1.gap(p))
  if (problems.length > 0) return false
  for (const [kind, text] of [
    ['foreign', 'dump records the journal never offered'],
    ['duplicate', 'ids present more than once in the dump'],
    ['abortedPresent', 'ids of aborted transactions present in the dump'],
    ['wrongOffset', 'ids found at an offset other than the acknowledged one'],
    ['differs', 'dump records whose bytes differ from what was offered'],
  ]) {
    const found = dumps.map((d) => d.findings[kind]).filter(Boolean)
    const total = found.reduce((n, f) => n + f.count, 0)
    if (total > 0) k1.fail(`${total} ${text}${first(found[0])}`)
  }
  const absent = { hard: [], soft: [] }
  const uncertain = []
  for (let i = 0; i < journal.names.length; i++) {
    const [t, ack] = [journal.txOf[i], journal.ack[i]]
    if (dumped[i] || t?.outcome === 'aborted' || ack === ACK.rejected) continue
    if (t ? t.outcome === 'committed' : ack === ACK.acknowledged) {
      const [at, d, end] = [journal.offset[i], dumps[journal.topic[i]], ends[journal.topic[i]]]
      absent[at < d.maxOffset || (end !== null && at >= end) ? 'hard' : 'soft'].push(
        journal.names[i]
      )
    } else if (!t) uncertain.push(journal.names[i])
  }
  const [lo, hi] = invisibleBounds(journal, dumped)
  const complete = dumps.every((d, n) => {
    if (ends[n] === null) {
      k1.gap(`no describe row gives the ${ROLES[n]} log end`)
      return false
    }
    const invisible = ends[n] - d.count
    if (n === 0 ? invisible === 0 : invisible >= lo && invisible <= hi) return true
    k1.gap(
      `the ${ROLES[n]} dump stops at offset ${d.maxOffset} but the log ends at ${ends[n]}: ` +
        `${d.count} records dumped, ${n === 0 ? 0 : `${lo}..${hi}`} invisible offsets explained, so it is not proved complete`
    )
    return false
  })
  const list = (ids) => `${ids.length} acknowledged or committed ids absent from the dump`
  const some = (ids) => ` (first ${ids.slice(0, EXAMPLES).join(', ')})`
  if (absent.hard.length > 0) k1.fail(`${list(absent.hard)} inside the log${some(absent.hard)}`)
  const text = `${list(absent.soft)}${some(absent.soft)}`
  if (absent.soft.length > 0 && complete) k1.fail(text)
  else if (absent.soft.length > 0) k1.gap(text)
  if (uncertain.length > 0)
    k1.gap(
      `${uncertain.length} sends with unknown results are absent from the log${some(uncertain)}`
    )
  const open = [...journal.txs.values()].filter(
    (t) => t.members.length > 0 && !/^(committed|aborted)$/.test(t.outcome)
  )
  if (open.length > 0)
    k1.gap(`${open.length} transactions have an unknown or missing outcome (first ${open[0].id})`)
  Object.assign(k1.numbers, {
    offeredIds: journal.names.length,
    dumpedMain: dumps[0].count,
    dumpedTransactions: dumps[1].count,
    logEndMain: ends[0],
    logEndTransactions: ends[1],
    unknownSendsAbsent: uncertain.length,
  })
  return complete
}

/**
 * K2: every visible record (in the proved-complete dump) entered a handler in every recipient group.
 * @param {Row} k2 @param {Journal} journal @param {Uint8Array} dumped @returns {void}
 */
function judgeDelivery(k2, journal, dumped) {
  const missing = new Map()
  let visible = 0
  for (let i = 0; i < journal.names.length; i++) {
    if (!dumped[i]) continue
    visible++
    for (const { group, index } of journal.recipients[journal.topic[i]]) {
      if (journal.entered[index][i] === 0) {
        const m = missing.get(group) ?? { count: 0, first: journal.names[i] }
        missing.set(group, { ...m, count: m.count + 1 })
      }
    }
  }
  for (const [group, m] of missing)
    k2.fail(`${group} never entered a handler for ${m.count} visible records (first ${m.first})`)
  Object.assign(k2.numbers, { visibleRecords: visible, groupsJudged: journal.groupIndex.size })
}

/**
 * K3: only a repeat on the same client run with no assignment change since the previous entry
 * fails; every other repeat is counted and printed, not judged.
 * @param {Row} row @param {Journal} journal @returns {void}
 */
function judgeDuplicates(row, journal) {
  const { sameOwner } = journal
  if (sameOwner.count > 0)
    row.fail(
      `${sameOwner.count} same-owner repeats with no assignment change (first ${sameOwner.examples.join('; ')})`
    )
  const { repeatsAfterChange, shareRepeats, redundantAcquires } = journal
  Object.assign(row.numbers, {
    sameOwnerRepeats: sameOwner.count,
    repeatsAfterChange,
    shareRepeats,
    redundantAcquires,
  })
}

/**
 * K4: the final committed offset of every committing group equals the log end, both read from the
 * describe output of Apache's own tool, never from the client.
 * @param {Row} row @param {object} ctx `{ journal, describes, config }`
 * @returns {(number|null)[]} the log end the describes agree on, per topic
 */
function judgeCommits(row, { journal, describes, config }) {
  const ends = [new Set(), new Set()]
  const groups = new Set()
  ROLES.forEach((role, n) => {
    for (const { group } of journal.recipients[n].filter((r) => !r.share)) {
      groups.add(group)
      const d = describes.get(group)
      if (d.problems.length > 0) continue
      const r = d.rows.find((x) => x.topic === config.topics[role] && x.partition === 0)
      if (!r) row.fail(`${group} has no row for ${role}: it committed nothing`)
      else if (r.current === null) row.fail(`${group} never committed on ${role}`)
      else if (r.logEnd === null) row.gap(`${group} has no log end for ${role}`)
      else {
        ends[n].add(r.logEnd)
        if (r.current !== r.logEnd)
          row.fail(`${group} committed ${r.current} on ${role} but the log ends at ${r.logEnd}`)
      }
    }
  })
  for (const group of groups) describes.get(group).problems.forEach((p) => row.gap(p))
  const agreed = ends.map((set, n) => {
    if (set.size > 1)
      row.gap(`the describes disagree about the ${ROLES[n]} log end: ${[...set].join(', ')}`)
    return set.size === 1 ? [...set][0] : null
  })
  Object.assign(row.numbers, {
    groupsJudged: groups.size,
    logEndMain: agreed[0],
    logEndTransactions: agreed[1],
  })
  return agreed
}

/** @param {number[]} values @returns {number} the middle value; the upper middle of an even count */
const upperMedian = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * The heap screen over matched quiet boundaries: Y[h] is the median heapUsed of boundary h, X[h]
 * the median completion time in minutes; the endpoint compares the medians of the last and first
 * three boundaries and the slope is the least-squares fit of Y over X.
 * @param {{ heapUsed: number, completedAt: number }[][]} boundaries readings per boundary, in order
 * @param {{ endpointBytes: number, slopeBytesPerMinute: number }} limits
 * @returns {{ endpoint: number, slope: number, ok: boolean }}
 */
function heapScreen(boundaries, limits) {
  const Y = boundaries.map((b) => upperMedian(b.map((r) => r.heapUsed)))
  const X = boundaries.map((b) => upperMedian(b.map((r) => r.completedAt)) / 60000)
  const n = Y.length
  const edge = Math.max(1, Math.min(3, Math.floor(n / 2)))
  const endpoint = upperMedian(Y.slice(n - edge)) - upperMedian(Y.slice(0, edge))
  const mean = (v) => v.reduce((s, x) => s + x, 0) / v.length
  const [mx, my] = [mean(X), mean(Y)]
  const den = X.reduce((s, x) => s + (x - mx) ** 2, 0)
  const slope = den === 0 ? 0 : X.reduce((s, x, i) => s + (x - mx) * (Y[i] - my), 0) / den
  return {
    endpoint,
    slope,
    ok: endpoint <= limits.endpointBytes && slope <= limits.slopeBytesPerMinute,
  }
}

/**
 * K5: the heap screen of each unseeded standing process on its quiet triplets, one boundary per
 * hour. The seed never substitutes for or fails an unseeded process; fewer than two boundaries
 * cannot be screened and are INCONCLUSIVE.
 * @param {Row} row @param {object} ctx `{ journal, matrix, config, plan }` @returns {void}
 */
function judgeHeap(row, { journal, matrix, config, plan }) {
  const limits = {
    endpointBytes: matrix.bounds['heap-endpoint'].value,
    slopeBytesPerMinute: matrix.bounds['heap-slope'].value,
  }
  for (const { slot } of config.processes.filter((p) => p.role === 'subject')) {
    const series = []
    for (let h = 0; h < plan.hours; h++) {
      const readings = journal.procs.get(slot).quiet.get(`h${String(h).padStart(2, '0')}`)
      if (readings && [...Array(plan.readings).keys()].every((n) => readings.has(n)))
        series.push([...readings.values()])
    }
    if (series.length < Math.max(plan.hours, 2))
      row.gap(
        `${slot} has ${series.length} of ${plan.hours} quiet boundaries with ${plan.readings} readings; the screen needs at least 2`
      )
    if (series.length < 2) continue
    const r = heapScreen(series, limits)
    row.numbers[slot] = { endpoint: r.endpoint, slopePerMinute: Math.round(r.slope) }
    if (r.endpoint > limits.endpointBytes)
      row.fail(`${slot} heap endpoint ${r.endpoint} bytes exceeds ${limits.endpointBytes}`)
    if (r.slope > limits.slopeBytesPerMinute)
      row.fail(
        `${slot} heap slope ${Math.round(r.slope)} bytes/min exceeds ${limits.slopeBytesPerMinute}`
      )
  }
}

/**
 * K6: every process's final resource counts are at or below its baseline; a type the baseline never
 * had counts against zero, so an unknown type fails.
 * @param {Row} row @param {{ journal: Journal, config: object }} ctx @returns {void}
 */
function judgeResources(row, { journal, config }) {
  let above = 0
  for (const { slot } of config.processes) {
    const { baseline, final } = journal.procs.get(slot)
    if (!baseline || !final) row.gap(`${slot} has no ${baseline ? 'final' : 'baseline'} sample`)
    for (const [type, count] of final ?? []) {
      if (count <= (baseline?.get(type) ?? 0) || !baseline) continue
      above++
      row.fail(
        `${slot} final ${type} count ${count} exceeds its baseline ${baseline.get(type) ?? 0}`
      )
    }
  }
  row.numbers.typesAboveBaseline = above
}

/**
 * K7: parent-observed exits (code 0, no signal), a run as long as its mode requires, a complete
 * sample grid, a clean supervisor receipt and a valid, digest-matching stream.
 * @param {Row} row @param {object} ctx `{ journal, config, header, supervisor, plan, problems, mode }` @returns {void}
 */
function judgeCompleteness(row, { journal, config, header, supervisor, plan, problems, mode }) {
  problems.slice(0, 10).forEach((p) => row.fail(`invalid evidence: ${p}`))
  if (problems.length > 10) row.fail(`${problems.length - 10} more invalid-evidence problems`)
  const w = header.window
  if (w.requestedMs !== plan.requestedMs)
    row.fail(`the run requested ${w.requestedMs} ms but ${mode} needs ${plan.requestedMs} ms`)
  if (w.runEndAt - w.runStartAt !== w.requestedMs)
    row.fail('the run window is not the requested time')
  if (header.exit.code !== 0 || header.exit.signal !== null)
    row.fail(`the supervisor exited with code ${header.exit.code} and signal ${header.exit.signal}`)
  for (const { slot } of config.processes) {
    const p = journal.procs.get(slot)
    if (!p.started) row.fail(`${slot} has no start record`)
    if (!p.exit) row.fail(`${slot} has no parent-observed exit`)
    else if (p.exit.code !== 0 || p.exit.signal !== null)
      row.fail(`${slot} exited with code ${p.exit.code} and signal ${p.exit.signal}`)
    for (const role of ['baseline', 'final'])
      if (!p[role]) row.fail(`${slot} has no ${role} sample`)
    if (p.repeats > 0)
      row.fail(`${slot} has ${p.repeats} repeated or out-of-range samples or exits`)
    const missing = []
    for (let k = 0; k < plan.slots; k++) {
      if (Number.isNaN(p.periodic[k])) missing.push(k)
      else if (p.periodic[k] < w.runStartAt + k * plan.periodMs - plan.jitterMs)
        row.fail(`${slot} slot ${k} was admitted early at ${p.periodic[k]}`)
    }
    if (missing.length > 0)
      row.fail(
        `${slot} misses periodic slot ${missing[0]} (${missing.length} of ${plan.slots} missing)`
      )
  }
  if (supervisor === null) row.gap('supervisor.json is missing or unreadable')
  else {
    if (supervisor.aborted) row.fail(`the supervisor aborted the run: ${supervisor.aborted}`)
    if (supervisor.incomplete)
      row.fail(`the supervisor reports an incomplete run: ${supervisor.incomplete}`)
    if (supervisor.problems?.length > 0)
      row.fail(
        `the driver reports ${supervisor.problems.length} problems: ${String(supervisor.problems[0]).slice(0, 80)}`
      )
    if (supervisor.forced?.length > 0)
      row.fail(
        `the supervisor forced ${supervisor.forced.join(', ')} to end after the teardown allowance`
      )
    if (supervisor.failures?.length > 0)
      row.fail(
        `the collector recorded ${supervisor.failures.length} failures: ${String(supervisor.failures[0]).slice(0, 80)}`
      )
    if (supervisor.steps?.error > 0 || supervisor.steps?.aborted > 0)
      row.fail(
        `${supervisor.steps.error} steps failed and ${supervisor.steps.aborted} were aborted`
      )
    for (const f of ['runId', 'mode', 'treeSha'])
      if (supervisor[f] !== header[f]) row.fail(`supervisor.json ${f} differs from the header`)
    if (mode === 'acceptance' && supervisor.diagnostic) row.fail('the run is marked diagnostic')
    row.numbers.lateSteps = supervisor.steps?.late ?? null
  }
  const { incidents } = journal
  for (const kind of ['problem:', 'missing-source'])
    if (incidents.has(kind))
      row[kind === 'problem:' ? 'fail' : 'gap'](
        `${incidents.get(kind).count} ${kind} diagnostics (first: ${incidents.get(kind).first})`
      )
}

/**
 * Judges K1 to K7 from the parsed artifact and the ground-truth files below `dir/apache`.
 * @param {object} input
 * @param {string} input.dir the artifact directory
 * @param {object} input.matrix @param {object} input.config @param {string} input.mode
 * @param {object} input.header parsed header @param {object|null} input.supervisor parsed receipt, or null
 * @param {Journal} input.journal the reduced stream
 * @param {string[]} input.problems validation problems of the matrix, config, header and stream
 * @returns {Promise<object>} the report: rows, NOT JUDGED items and the overall verdict
 */
async function judge({ dir, matrix, config, mode, header, supervisor, journal, problems = [] }) {
  const plan = planOf(matrix, mode)
  const rows = Object.fromEntries(Object.keys(ROW_NAMES).map((id) => [id, new Row(id)]))
  const manifest = new Map(header.files.map((f) => [f.file, f]))
  const ctx = { dir, manifest, journal, config, matrix, plan, header, supervisor, mode, problems }
  judgeCompleteness(rows.K7, ctx)
  const untrusted = problems.length > 0 ? 'the stream is not valid (see K7)' : journal.problems[0]
  const describes = new Map()
  for (const { group } of journal.recipients.flat().filter((r) => !r.share))
    describes.set(group, readDescribe(ctx, group))
  const ends = judgeCommits(rows.K4, { ...ctx, describes })
  if (untrusted) for (const id of ['K1', 'K2', 'K3', 'K5', 'K6']) rows[id].gap(untrusted)
  else {
    ctx.dumped = new Uint8Array(journal.capacity)
    const dumps = [await readDump(ctx, 'main'), await readDump(ctx, 'transactions')]
    if (judgeLog(rows.K1, { journal, dumps, ends, dumped: ctx.dumped }))
      judgeDelivery(rows.K2, journal, ctx.dumped)
    else rows.K2.gap(`delivery cannot be judged: ${rows.K1.gaps[0]}`)
    judgeDuplicates(rows.K3, journal)
    const partial = journal.incidents.get('partial handler-enter')
    if (partial)
      for (const id of ['K2', 'K3'])
        rows[id].gap(
          `${partial.count} handler calls were journaled only in part (first: ${partial.first.slice(0, 120)})`
        )
    judgeHeap(rows.K5, ctx)
    judgeResources(rows.K6, ctx)
  }
  const done = Object.values(rows).map((r) => r.done())
  const accepted = mode === 'acceptance' && done.every((r) => r.verdict === 'PASS')
  return {
    runId: header.runId,
    mode,
    treeSha: header.treeSha,
    rows: done,
    notJudged: NOT_JUDGED.map((item) => ({ ...item, verdict: 'NOT JUDGED' })),
    overall: accepted
      ? 'ACCEPTED'
      : done.some((r) => r.verdict === 'FAIL')
        ? 'REJECTED'
        : 'NOT ACCEPTED',
  }
}

/**
 * Reads and validates an artifact directory, streams its journal once and judges it. An artifact
 * whose inputs cannot be read fails K7 and leaves every other row INCONCLUSIVE.
 * @param {string} dir the artifact directory
 * @returns {Promise<object>} the report
 */
async function checkArtifact(dir) {
  const readJson = (file) => {
    try {
      return JSON.parse(fs.readFileSync(path.join(dir, file), 'utf8'))
    } catch {
      return null
    }
  }
  const sha = (data) => crypto.createHash('sha256').update(data).digest('hex')
  const reviewed = fs.readFileSync(REVIEWED_MATRIX)
  const matrix = JSON.parse(reviewed)
  const [header, config] = [readJson('header.json'), readJson('config.json')]
  const problems = [...schema.validateMatrix(matrix)]
  const archived = fs.existsSync(path.join(dir, ARCHIVED_MATRIX))
    ? fs.readFileSync(path.join(dir, ARCHIVED_MATRIX))
    : null
  if (!archived?.equals(reviewed))
    problems.push('the archived matrix is missing or differs from the reviewed matrix')
  if (header && config)
    problems.push(
      ...schema.validateConfig(config, matrix),
      ...schema.validateHeader(header, matrix)
    )
  else problems.push('header.json or config.json is missing or not JSON')
  if (fs.existsSync(path.join(dir, 'manifest.partial.json')))
    problems.push('the artifact is partial: the run left no supervisor receipt')
  if (!header || !config || problems.length > 0) {
    const rows = Object.keys(ROW_NAMES).map((id) => {
      const row = new Row(id)
      if (id === 'K7') problems.forEach((p) => row.fail(`invalid evidence: ${p}`))
      else row.gap('the artifact inputs are not valid (see K7)')
      return row.done()
    })
    const notJudged = NOT_JUDGED.map((item) => ({ ...item, verdict: 'NOT JUDGED' }))
    return {
      runId: null,
      mode: header?.mode ?? 'acceptance',
      treeSha: null,
      rows,
      notJudged,
      overall: 'REJECTED',
    }
  }
  const manifest = new Map(header.files.map((f) => [f.file, f]))
  for (const file of ['config.json', 'supervisor.json']) {
    if (!fs.existsSync(path.join(dir, file))) continue
    const bytes = fs.readFileSync(path.join(dir, file))
    checkBound(manifest, file, { bytes: bytes.length, sha256: sha(bytes) }, problems)
  }
  const journal = new Journal({ matrix, config, mode: header.mode })
  const validator = new schema.StreamValidator({ matrix, config, header })
  const read = await eachLine(path.join(dir, header.stream.file), (line, n) => {
    validator.push(line)
    try {
      journal.add(JSON.parse(line))
    } catch (e) {
      if (problems.length < 20)
        problems.push(`line ${n}: the record could not be read: ${e.message}`)
    }
  }).catch(() => null)
  if (read === null) problems.push('the stream file is missing')
  else if (read.unterminated) problems.push('the stream file ends without a newline')
  problems.push(...validator.finish())
  return judge({
    dir,
    matrix,
    config,
    mode: header.mode,
    header,
    supervisor: readJson('supervisor.json'),
    journal,
    problems,
  })
}

/** @param {object} report @returns {string} the printable report */
function formatReport(report) {
  const smoke = report.mode === 'smoke' ? ' (smoke runs are diagnostic and are never ACCEPTED)' : ''
  const lines = [
    `CORE VERDICT: ${report.overall}${smoke}`,
    `run ${report.runId ?? '-'}  mode ${report.mode}  tree ${(report.treeSha ?? '-').slice(0, 12)}`,
    'This is a core checker verdict on K1 to K7 only; the items under NOT JUDGED are not judged here.',
    '',
  ]
  for (const r of report.rows) {
    lines.push(`${r.id}  ${r.name.padEnd(24)} ${r.verdict}`)
    for (const [key, value] of Object.entries(r.numbers))
      lines.push(`      ${key}: ${JSON.stringify(value)}`)
    for (const reason of r.reasons) lines.push(`      - ${reason}`)
  }
  lines.push('', 'NOT JUDGED (no code path here prints PASS for these):')
  for (const item of report.notJudged)
    lines.push(`  NOT JUDGED  ${item.name}  (lane: ${item.lane})`)
  return lines.join('\n')
}

/**
 * Command line: `node finalSoakCore.js <artifact dir>`; prints the report.
 * @param {string[]} argv @param {(line: string) => void} [out]
 * @returns {Promise<number>} 0 for ACCEPTED, 1 for any other verdict, 2 for a usage error
 */
async function main(argv, out = console.log) {
  if (argv.length !== 1) {
    out('usage: node scripts/evidence/finalSoakCore.js <artifact dir>')
    return 2
  }
  const report = await checkArtifact(argv[0])
  out(formatReport(report))
  return report.overall === 'ACCEPTED' ? 0 : 1
}

if (require.main === module) {
  main(process.argv.slice(2)).then((code) => {
    process.exitCode = code
  })
}

module.exports = { Journal, NOT_JUDGED, checkArtifact, formatReport, heapScreen, judge, main }
