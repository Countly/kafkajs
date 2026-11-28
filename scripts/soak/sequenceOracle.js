'use strict'

/**
 * The per-partition sequence oracle of the load profile.
 *
 * Every record a load producer writes carries the key `<producer>.<incarnation>.<sequence>`: the
 * producer's identity, which life of that producer wrote it (a recreated producer is a new
 * incarnation), and a sequence that increases by one per record, per topic partition, per
 * incarnation. Producers keep a `SendLedger` (what they issued, and which sequences they did not get
 * acknowledged), consumers keep a `DeliveryLedger` (the offset ranges they were delivered), and the
 * `SequenceChecker` reads each partition once, in offset order, from an independent dump.
 *
 * Memory is O(partitions × producer incarnations) plus one interval per discontinuity: a failed
 * or aborted send on the producer side, a re-delivery or a transaction marker on the consumer side.
 * It never holds one entry per record.
 */

/**
 * What each producer kind guarantees per partition, from the producer documentation
 * (`docs/Producing.md`, `docs/Transactions.md`): for an acknowledged record, whether it may appear
 * more than once (`repeats`) and whether it may appear out of order when the producer has more
 * than one request in flight (`reorderInFlight`). A record whose send was rejected has an unknown
 * outcome: it may be absent, and it may appear at most once unless `repeats` allows more.
 *
 *   idempotent     exactly once, in order: the broker drops a retried batch by its sequence
 *   transactional  exactly once, in order, and only from committed transactions (read_committed)
 *   plain          at least once: a retry after a lost answer writes the batch again; order holds
 *                  only with one request in flight
 */
const GUARANTEES = Object.freeze({
  idempotent: Object.freeze({ repeats: false, reorderInFlight: false }),
  transactional: Object.freeze({ repeats: false, reorderInFlight: false }),
  plain: Object.freeze({ repeats: true, reorderInFlight: true }),
})

/** How many examples each finding keeps; the count is always exact. */
const SAMPLES = 20

const KEY = /^(p\d{1,4})\.(\d{1,6})\.(\d{1,15})$/

/**
 * @param {string} producer @param {number} incarnation @param {number} sequence
 * @returns {string} the record key the oracle reads back
 */
const encodeKey = (producer, incarnation, sequence) => `${producer}.${incarnation}.${sequence}`

/**
 * @param {string} key
 * @returns {{ producer: string, incarnation: number, sequence: number }|null} null when the key is
 *   not one a load producer writes
 */
const decodeKey = (key) => {
  const m = KEY.exec(key)
  return m && { producer: m[1], incarnation: Number(m[2]), sequence: Number(m[3]) }
}

/**
 * A set of integers held as sorted, disjoint, non-adjacent inclusive ranges.
 */
class IntervalSet {
  /** @param {Array<[number, number]>} [ranges] inclusive ranges in any order */
  constructor(ranges = []) {
    /** @type {Array<[number, number]>} */
    this.ranges = []
    for (const [lo, hi] of ranges) this.add(lo, hi)
  }

  /**
   * Adds `lo..hi`, merging with overlapping or adjacent ranges. Appending just past the last range
   * is O(1), which is the common case for an ascending stream.
   * @param {number} lo @param {number} [hi=lo]
   * @returns {void}
   */
  add(lo, hi = lo) {
    const r = this.ranges
    const last = r[r.length - 1]
    if (!last || lo > last[1] + 1) {
      r.push([lo, hi])
      return
    }
    if (lo >= last[0] && hi > last[1]) {
      last[1] = hi
      return
    }
    const i = this.#firstEndingAtOrAfter(lo - 1)
    let start = lo
    let end = hi
    let j = i
    while (j < r.length && r[j][0] <= hi + 1) {
      start = Math.min(start, r[j][0])
      end = Math.max(end, r[j][1])
      j++
    }
    r.splice(i, j - i, [start, end])
  }

  /**
   * Removes one value, splitting its range.
   * @param {number} v
   * @returns {boolean} whether it was in the set
   */
  delete(v) {
    const i = this.#firstEndingAtOrAfter(v)
    const range = this.ranges[i]
    if (!range || range[0] > v) return false
    const [lo, hi] = range
    const parts = []
    if (lo < v) parts.push([lo, v - 1])
    if (v < hi) parts.push([v + 1, hi])
    this.ranges.splice(i, 1, ...parts)
    return true
  }

  /** @param {number} v @returns {boolean} */
  has(v) {
    const range = this.ranges[this.#firstEndingAtOrAfter(v)]
    return Boolean(range) && range[0] <= v
  }

  /** @returns {number} how many integers the set holds */
  get count() {
    return this.ranges.reduce((n, [lo, hi]) => n + hi - lo + 1, 0)
  }

  /**
   * @param {IntervalSet} other
   * @returns {IntervalSet} the values of this set that are not in `other`
   */
  minus(other) {
    const out = new IntervalSet()
    let j = 0
    for (const [lo, hi] of this.ranges) {
      let from = lo
      while (j < other.ranges.length && other.ranges[j][1] < from) j++
      let k = j
      while (from <= hi && k < other.ranges.length && other.ranges[k][0] <= hi) {
        const [olo, ohi] = other.ranges[k]
        if (olo > from) out.add(from, olo - 1)
        from = Math.max(from, ohi + 1)
        k++
      }
      if (from <= hi) out.add(from, hi)
    }
    return out
  }

  /** @returns {Array<[number, number]>} a copy of the ranges, for a report */
  toJSON() {
    return this.ranges.map(([lo, hi]) => [lo, hi])
  }

  /** @param {number} v @returns {number} the index of the first range whose end is ≥ v */
  #firstEndingAtOrAfter(v) {
    let lo = 0
    let hi = this.ranges.length
    while (lo < hi) {
      const mid = (lo + hi) >> 1
      if (this.ranges[mid][1] < v) lo = mid + 1
      else hi = mid
    }
    return lo
  }
}

/** @param {string} topic @param {number} partition @returns {string} */
const cellOf = (topic, partition) => `${topic}/${partition}`

/**
 * One producer incarnation's record of what it issued: per topic partition the next sequence, the
 * sequences not acknowledged (failed or never settled) and those of aborted transactions, and the
 * offset of the last acknowledged record (a send's `RecordMetadata.baseOffset` is that of its first
 * record in the partition, so its last is `baseOffset + count - 1`), which the read must reach.
 */
class SendLedger {
  /**
   * @param {{ producer: string, incarnation: number, kind: keyof GUARANTEES, maxInFlight: number }} identity
   * @throws {Error} for an unknown kind
   */
  constructor({ producer, incarnation, kind, maxInFlight }) {
    if (!GUARANTEES[kind]) throw new Error(`Unknown producer kind ${kind}`)
    Object.assign(this, { producer, incarnation, kind, maxInFlight })
    /** @type {Map<string, { next: number, pending: Map<number, number>, notAcked: IntervalSet, aborted: IntervalSet, maxOffset: number }>} */
    this.cells = new Map()
  }

  /**
   * Reserves `count` consecutive sequences of one partition for one send.
   * @param {string} topic @param {number} partition @param {number} count
   * @returns {{ key: (i: number) => string, first: number }} the first sequence and the key of the
   *   i-th record
   */
  issue(topic, partition, count) {
    const cell = this.#cell(topic, partition)
    const first = cell.next
    cell.next += count
    cell.pending.set(first, count)
    return { first, key: (i) => encodeKey(this.producer, this.incarnation, first + i) }
  }

  /**
   * Settles a send `issue` reserved.
   * @param {string} topic @param {number} partition @param {number} first
   * @param {'acked'|'failed'|'aborted'} status `failed` covers a rejected send and a transaction
   *   whose commit failed: its records may or may not be in the log
   * @param {number} [baseOffset] the acknowledged `RecordMetadata.baseOffset`, when known
   * @returns {void}
   * @throws {Error} for a send that was not issued or already settled
   */
  settle(topic, partition, first, status, baseOffset) {
    const cell = this.#cell(topic, partition)
    const count = cell.pending.get(first)
    if (count === undefined) throw new Error(`No pending send ${topic}/${partition}@${first}`)
    cell.pending.delete(first)
    if (status === 'failed') cell.notAcked.add(first, first + count - 1)
    else if (status === 'aborted') cell.aborted.add(first, first + count - 1)
    else if (Number.isFinite(baseOffset))
      cell.maxOffset = Math.max(cell.maxOffset, baseOffset + count - 1)
  }

  /** @returns {number} sends issued and not yet settled */
  get unsettled() {
    let n = 0
    for (const cell of this.cells.values()) n += cell.pending.size
    return n
  }

  /**
   * The report the checker reads; a send still unsettled counts as not acknowledged.
   * @returns {object}
   */
  toJSON() {
    const cells = {}
    for (const [name, cell] of this.cells) {
      const notAcked = new IntervalSet(cell.notAcked.ranges)
      for (const [first, count] of cell.pending) notAcked.add(first, first + count - 1)
      cells[name] = {
        issued: cell.next,
        notAcked: notAcked.toJSON(),
        aborted: cell.aborted.toJSON(),
        maxOffset: cell.maxOffset,
      }
    }
    const { producer, incarnation, kind, maxInFlight } = this
    return { producer, incarnation, kind, maxInFlight, cells }
  }

  /** @param {string} topic @param {number} partition @returns {object} */
  #cell(topic, partition) {
    const name = cellOf(topic, partition)
    let cell = this.cells.get(name)
    if (!cell) {
      cell = {
        next: 0,
        pending: new Map(),
        notAcked: new IntervalSet(),
        aborted: new IntervalSet(),
        maxOffset: -1,
      }
      this.cells.set(name, cell)
    }
    return cell
  }
}

/**
 * One consumer member's record of the offsets it was delivered, per topic partition.
 */
class DeliveryLedger {
  /** @param {{ group: string, member: string }} identity */
  constructor({ group, member }) {
    Object.assign(this, { group, member })
    /** @type {Map<string, IntervalSet>} */
    this.cells = new Map()
  }

  /** @param {string} topic @param {number} partition @param {number} offset @returns {void} */
  deliver(topic, partition, offset) {
    const name = cellOf(topic, partition)
    let set = this.cells.get(name)
    if (!set) this.cells.set(name, (set = new IntervalSet()))
    set.add(offset)
  }

  /** @returns {{ group: string, member: string, delivered: Record<string, Array<[number, number]>> }} */
  toJSON() {
    const delivered = {}
    for (const [name, set] of this.cells) delivered[name] = set.toJSON()
    return { group: this.group, member: this.member, delivered }
  }
}

/**
 * Reads a dump once, in offset order per partition, and judges it against the producers' and
 * consumers' reports.
 *
 * Producer rules, per incarnation and partition (see `GUARANTEES`):
 * - loss: an acknowledged sequence that never appears
 * - duplicate: a sequence that appears again, unless the kind allows repeats
 * - reorder: a sequence that appears after a later one, unless the kind allows reordering with the
 *   incarnation's requests in flight
 * - abortedVisible: a sequence of an aborted transaction in the read_committed dump
 * - foreign: a key no producer issued, or one past what its producer issued
 *
 * Consumer rule, per group and partition: every record of the dump below the group's committed
 * offset was delivered to some member of the group (skippedCommit), and the committed offset is
 * not past the partition's log end (commitBeyondLog). Committed offsets and log ends come from
 * Apache's `kafka-consumer-groups.sh --describe`, not from the client.
 *
 * Read findings, which make a run inconclusive rather than failed: a line that is not a dump line
 * (unparseable), offsets out of order (offsetOrder), and a dump that stops before an acknowledged
 * offset (incompleteRead).
 */
class SequenceChecker {
  /**
   * @param {object} reports
   * @param {object[]} reports.producers `SendLedger#toJSON` of every incarnation
   * @param {object[]} [reports.consumers] `DeliveryLedger#toJSON` of every member
   * @param {Record<string, Record<string, number>>} [reports.committed] per group, per cell
   *   (`topic/partition`), the committed offset Apache's tool reported
   * @param {Record<string, number>} [reports.logEnds] per cell, the log end Apache's tool reported
   */
  constructor({ producers, consumers = [], committed = {}, logEnds = {} }) {
    this.logEnds = logEnds
    /** @type {Map<string, object>} producer incarnation → cell → state */
    this.cells = new Map()
    for (const report of producers) {
      const guarantee = GUARANTEES[report.kind]
      if (!guarantee) throw new Error(`Unknown producer kind ${report.kind}`)
      const allowReorder = guarantee.reorderInFlight && report.maxInFlight > 1
      for (const [name, cell] of Object.entries(report.cells)) {
        this.cells.set(`${report.producer}.${report.incarnation}|${name}`, {
          name,
          producer: report.producer,
          incarnation: report.incarnation,
          repeats: guarantee.repeats,
          reorder: allowReorder,
          issued: cell.issued,
          notAcked: new IntervalSet(cell.notAcked),
          aborted: new IntervalSet(cell.aborted),
          maxOffset: cell.maxOffset,
          expected: 0,
          gaps: new IntervalSet(),
        })
      }
    }
    /** @type {Map<string, object>} group|cell → state */
    this.groups = new Map()
    /** @type {Map<string, object[]>} cell → the group states that judge it */
    this.groupsOf = new Map()
    for (const { group, delivered } of consumers) {
      for (const [name, ranges] of Object.entries(delivered)) {
        const state = this.#group(group, name, committed)
        for (const [lo, hi] of ranges) state.delivered.add(lo, hi)
      }
    }
    for (const [group, cells] of Object.entries(committed))
      for (const name of Object.keys(cells)) this.#group(group, name, committed)
    /** @type {Map<string, { last: number, records: number }>} cell → what the dump held */
    this.partitions = new Map()
    this.findings = {}
    for (const kind of [
      'loss',
      'duplicate',
      'reorder',
      'abortedVisible',
      'foreign',
      'malformed',
      'unparseable',
      'offsetOrder',
      'skippedCommit',
      'commitBeyondLog',
      'incompleteRead',
    ])
      this.findings[kind] = { count: 0, samples: [] }
    this.counts = { records: 0, repeatsAllowed: 0, reordersAllowed: 0, notYetCommitted: 0 }
  }

  /**
   * Takes the next record of one partition. Records of one partition must come in offset order;
   * partitions may interleave.
   * @param {string} topic @param {number} partition @param {number} offset @param {string} key
   * @returns {void}
   */
  observe(topic, partition, offset, key) {
    this.counts.records++
    const name = cellOf(topic, partition)
    let part = this.partitions.get(name)
    if (!part) this.partitions.set(name, (part = { last: -1, records: 0 }))
    if (offset <= part.last) {
      this.#find('offsetOrder', `${name}: offset ${offset} after ${part.last}`)
      return
    }
    part.last = offset
    part.records++
    for (const state of this.groupsOf.get(name) ?? []) {
      if (offset >= state.committed) {
        this.counts.notYetCommitted++
        continue
      }
      const r = state.delivered.ranges
      while (state.cursor < r.length && r[state.cursor][1] < offset) state.cursor++
      if (!(state.cursor < r.length && r[state.cursor][0] <= offset))
        this.#find(
          'skippedCommit',
          `${state.group} ${name}: offset ${offset} committed, never delivered`
        )
    }
    const id = decodeKey(key)
    if (!id) {
      this.#find('malformed', `${name}@${offset}: key ${JSON.stringify(key).slice(0, 60)}`)
      return
    }
    const cell = this.cells.get(`${id.producer}.${id.incarnation}|${name}`)
    const s = id.sequence
    if (!cell || s >= cell.issued) {
      this.#find('foreign', `${name}@${offset}: ${key}`)
      return
    }
    if (cell.aborted.has(s)) this.#find('abortedVisible', `${name}@${offset}: ${key}`)
    if (s === cell.expected) {
      cell.expected++
    } else if (s > cell.expected) {
      cell.gaps.add(cell.expected, s - 1)
      cell.expected = s + 1
    } else if (cell.gaps.delete(s)) {
      if (cell.reorder) this.counts.reordersAllowed++
      else this.#find('reorder', `${name}@${offset}: ${key} after ${cell.expected - 1}`)
    } else if (cell.repeats) {
      this.counts.repeatsAllowed++
    } else {
      this.#find('duplicate', `${name}@${offset}: ${key}`)
    }
  }

  /**
   * Takes a line of the dump that is not a record line.
   * @param {string} topic @param {string} line
   * @returns {void}
   */
  unparseable(topic, line) {
    this.#find('unparseable', `${topic}: ${JSON.stringify(line).slice(0, 80)}`)
  }

  /**
   * Ends the read and judges what never appeared.
   * @returns {{ ok: boolean, complete: boolean, findings: object, counts: object }} `ok` when no
   *   finding was made; `complete` is false when the dump stopped before an acknowledged offset, in
   *   which case a loss may be the read's and not the product's
   */
  finish() {
    for (const cell of this.cells.values()) {
      const missing = new IntervalSet(cell.gaps.ranges)
      if (cell.expected < cell.issued) missing.add(cell.expected, cell.issued - 1)
      const lost = missing.minus(cell.notAcked).minus(cell.aborted)
      if (lost.count > 0)
        this.#find(
          'loss',
          `${cell.producer}.${cell.incarnation} ${cell.name}: ${JSON.stringify(lost.toJSON().slice(0, 4))}`,
          lost.count
        )
      const last = this.partitions.get(cell.name)?.last ?? -1
      if (cell.maxOffset > last)
        this.#find(
          'incompleteRead',
          `${cell.name}: read to ${last}, acknowledged ${cell.maxOffset}`
        )
    }
    for (const state of this.groups.values()) {
      const end = this.logEnds[state.name]
      if (end === undefined || state.committed > end)
        this.#find(
          'commitBeyondLog',
          `${state.group} ${state.name}: committed ${state.committed}, log end ${end ?? 'unknown'}`
        )
    }
    const complete = this.findings.incompleteRead.count === 0
    const ok = Object.values(this.findings).every((f) => f.count === 0)
    return { ok, complete, findings: this.findings, counts: { ...this.counts } }
  }

  /**
   * The sizes of the checker's own structures, which must not grow with the number of records.
   * @returns {{ cells: number, gapRanges: number, groupCells: number, deliveredRanges: number, partitions: number, samples: number }}
   */
  size() {
    let gapRanges = 0
    for (const cell of this.cells.values()) gapRanges += cell.gaps.ranges.length
    let deliveredRanges = 0
    for (const state of this.groups.values()) deliveredRanges += state.delivered.ranges.length
    let samples = 0
    for (const f of Object.values(this.findings)) samples += f.samples.length
    return {
      cells: this.cells.size,
      gapRanges,
      groupCells: this.groups.size,
      deliveredRanges,
      partitions: this.partitions.size,
      samples,
    }
  }

  /**
   * @param {string} group @param {string} name @param {Record<string, Record<string, number>>} committed
   * @returns {{ group: string, name: string, committed: number, delivered: IntervalSet, cursor: number }}
   */
  #group(group, name, committed) {
    const id = `${group}|${name}`
    let state = this.groups.get(id)
    if (!state) {
      state = {
        group,
        name,
        committed: committed[group]?.[name] ?? 0,
        delivered: new IntervalSet(),
        cursor: 0,
      }
      this.groups.set(id, state)
      if (!this.groupsOf.has(name)) this.groupsOf.set(name, [])
      this.groupsOf.get(name).push(state)
    }
    return state
  }

  /**
   * @param {string} kind @param {string} sample
   * @param {number} [weight=1] how many cases the sample stands for
   * @returns {void}
   */
  #find(kind, sample, weight = 1) {
    const f = this.findings[kind]
    f.count += weight
    if (f.samples.length < SAMPLES) f.samples.push(sample)
  }
}

const DUMP_LINE = /^Partition:(\d{1,9})\tOffset:(\d{1,15})\t(.*)$/

/**
 * Parses one line of `kafka-console-consumer.sh` with `print.partition`, `print.offset` and
 * `print.key` on and `print.value` off (Kafka 4.4's `DefaultMessageFormatter` ends such a line
 * after the key).
 * @param {string} line
 * @returns {{ partition: number, offset: number, key: string }|null}
 */
const parseDumpLine = (line) => {
  const m = DUMP_LINE.exec(line)
  return m && { partition: Number(m[1]), offset: Number(m[2]), key: m[3] }
}

module.exports = {
  GUARANTEES,
  DeliveryLedger,
  IntervalSet,
  SendLedger,
  SequenceChecker,
  decodeKey,
  encodeKey,
  parseDumpLine,
}
