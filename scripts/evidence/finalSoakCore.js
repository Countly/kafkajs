'use strict'

/**
 * The core checker of a final-soak artifact: seven rows a person can review line by line.
 *
 *   K1 no lost record      K2 no lost delivery     K3 same-owner duplicates   K4 commits
 *   K5 heap                K6 resources            K7 exit and completeness
 *
 * Each row is PASS, FAIL or INCONCLUSIVE with the numbers behind it; the overall verdict is
 * ACCEPTED only when all seven PASS, which a smoke run cannot reach (K5 needs two hourly
 * boundaries). Every other contract row is printed as NOT JUDGED with the CI lane that covers it.
 *
 * Inputs (the runbook's artifact directory): `config.json`, `header.json`, `supervisor.json`,
 * `outer-exit.json`, the archived matrix and `stream.ndjson` (read once, line by line, never held),
 * and the ground truth that Apache's own tools write in the broker container before finalize, under
 * `apache/`: `<main|transactions>.dump` (`kafka-console-consumer.sh`, read_committed, partition 0,
 * lines `Partition:<p>\tOffset:<o>\t<key>\t<value>`) and `groups/<group>.describe`
 * (`kafka-consumer-groups.sh --describe`), each with `.stderr` and `.end` (`exit=<n> lines=<n>`). A
 * missing, truncated, unparseable or unbound ground-truth file makes its rows INCONCLUSIVE.
 *
 * Memory: about 87 bytes of typed arrays per offered-id slot (`matrix.traffic.maxOffers`, about
 * 139 MB at 1.6 million; pages are touched only as ids arrive): the full 32-byte payload digest,
 * offsets, and one 4-byte ownership-interval serial per judged group. Then one id string and Map
 * entry per offered id; one entry per open ownership interval, per transaction, per id an open
 * interval shares with an overlapping one, and per record held for its offer, each capped at the
 * same capacity; open membership waves and printed outages capped by the matrix schedule; a fixed
 * sample grid per process. Any cap that is reached is a reported problem that leaves K1 to K6
 * INCONCLUSIVE. Side files are read whole only below a size cap.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const schema = require('../soak/finalSoakSchema')

const CHECKOUT = path.resolve(__dirname, '../..')
const REVIEWED_MATRIX = path.join(CHECKOUT, 'scripts/soak/finalSoakMatrix.json')
const ARCHIVED_MATRIX = 'archive/scripts/soak/finalSoakMatrix.json'
const ROLES = ['main', 'transactions']
const EXAMPLES = 3
const LIST_CAP = 20
const MAX_LINE = 1 << 20
const MAX_SMALL_FILE = 1 << 20
const MAX_JSON = 16 << 20
/** @param {string|Buffer} data @returns {string} its sha256 in hex */
const sha = (data) => crypto.createHash('sha256').update(data).digest('hex')
/** The `entered` value of an id a group entered outside every ownership interval; serials start above it. */
const OUTSIDE = 1
const ACK = { none: 0, acknowledged: 1, unknown: 2, rejected: 3, contradicted: 4 }
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

const NOT_JUDGED_ROWS = NOT_JUDGED.map((item) => ({ ...item, verdict: 'NOT JUDGED' }))

/** @param {string[]} list @param {string} text appends unless the list holds `LIST_CAP` items @returns {void} */
const capped = (list, text) => {
  if (list.length < LIST_CAP) list.push(text)
}

/** @param {object} f a `{ count, examples }` finding @returns {string} its first examples */
const first = (f) => ` (first ${f?.examples.join('; ')})`

/** One row's findings: a failure makes it FAIL, else a gap (missing proof) makes it INCONCLUSIVE. */
class Row {
  /** @param {string} id `K1`..`K7` */
  constructor(id) {
    Object.assign(this, { id, name: ROW_NAMES[id], fails: [], gaps: [], numbers: {} })
  }

  /** @param {string} reason a violation @returns {void} */
  fail(reason) {
    this.fails.push(reason)
  }

  /** @param {string} reason missing or insufficient evidence @returns {void} */
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
  const { schedule, sampling } = matrix
  const smoke = mode === 'smoke'
  const requestedMs = matrix.acceptance.modes[mode].requestedMs.value
  const quietAt = smoke ? schedule.smoke.quietAtMs : schedule.quietAtMs
  return {
    requestedMs,
    periodMs: sampling.periodMs.value,
    jitterMs: sampling.jitterMs.value,
    slots: smoke ? schedule.smoke.periodicSlots : sampling.periodicSlots.value,
    hours: smoke ? 1 : requestedMs / schedule.hourMs,
    hourMs: smoke ? requestedMs : schedule.hourMs,
    readings: sampling.quietReadings.value,
    quietAt,
    quietDeadline: quietAt[0] + schedule.quietDeadlineMs - schedule.quietAtMs[0],
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
 * two standing topics get an index; acknowledgement, entry and sample facts live in typed arrays
 * or small per-process tables, so the journal never holds the stream.
 *
 * Ownership (K3): for each group, client instance run and partition an interval opens when the
 * instance applies an assignment that acquires the partition (an `acquire` boundary with a new
 * acquisition) and closes when it revokes or loses it (`revoke-end`, `lost`). A handler entry
 * belongs to the open interval with the batch's acquisition; outside one it is an unowned entry.
 * Each interval has a serial; per group and offered id the journal keeps the serial of the interval
 * that last entered the id (`OUTSIDE` for an entry outside every interval), so a same-owner repeat is
 * the same id entered twice by one interval, whatever the offsets in between. When another interval
 * re-enters an id an open interval entered, the open one keeps that id in `shadowed` until it closes.
 */
class Journal {
  /**
   * @param {{ matrix: object, config: object, mode: string, capacity?: number }} options
   *   `capacity` caps offered ids, open ownership intervals and transactions; the matrix's
   *   `traffic.maxOffers` by default
   */
  constructor({ matrix, config, mode, capacity = matrix.traffic.maxOffers.value }) {
    this.plan = planOf(matrix, mode)
    this.capacity = capacity
    this.problems = []
    this.findings = {}
    this.topics = new Map(ROLES.map((role, n) => [config.topics?.[role], n]))
    if (this.topics.has(undefined)) this.problem('config.topics does not name the topics')
    this.clients = new Map(config.clients.map((c) => [c.slot, c]))
    this.groupIndex = new Map()
    this.recipients = ROLES.map((role) =>
      recipientsOf(matrix, config, role).map(({ group, share }) => {
        if (!this.groupIndex.has(group))
          this.groupIndex.set(group, { index: this.groupIndex.size, share })
        return { group, share, index: this.groupIndex.get(group).index }
      })
    )
    this.entered = [...this.groupIndex.keys()].map(() => new Uint32Array(capacity))
    this.ids = new Map()
    this.names = []
    this.topic = new Uint8Array(capacity)
    this.ack = new Uint8Array(capacity)
    this.offset = new Float64Array(capacity).fill(NaN)
    this.digests = Buffer.alloc(capacity * 32)
    this.enteredAt = new Float64Array(capacity).fill(NaN)
    this.txOf = []
    this.txs = new Map()
    this.owners = new Map()
    this.serials = OUTSIDE
    this.shadowed = new Map()
    this.shadowedCount = 0
    this.held = new Map()
    this.heldCount = 0
    this.idsFull = false
    Object.assign(this, { repeatsAfterChange: 0, shareRepeats: 0, redundantAcquires: 0 })
    this.trafficEnds = new Map()
    const keys = () => ({ paused: 0, held: 0 })
    this.info = {
      outages: [],
      outagesNotShown: 0,
      waves: { transfer: 0, return: 0, unobserved: 0, keys: { transfer: keys(), return: keys() } },
      incompleteWaves: [],
    }
    this.openWaves = new Map()
    const churn = matrix.schedule.executions.find((e) => e.shape === 'membership-churn')
    this.waveCap = churn.waves * 2 * Object.keys(matrix.clientKinds).length
    this.wavesFull = false
    this.outageCap = 2 * matrix.schedule.totals.outages
    this.incidents = new Map()
    this.procs = new Map(config.processes.map(({ slot }) => [slot, this.newProcess()]))
  }

  /** @param {string} text a problem that leaves K1 to K6 untrusted @returns {void} */
  problem(text) {
    capped(this.problems, text)
  }

  /** @param {string} kind @param {string} text counts one finding of the kind @returns {void} */
  note(kind, text) {
    const f = (this.findings[kind] ??= { count: 0, examples: [] })
    if (f.count++ < EXAMPLES) f.examples.push(text)
  }

  /** @returns {object} the empty facts of one process */
  newProcess() {
    return {
      started: false,
      exit: null,
      baseline: null,
      final: null,
      others: { first: Infinity, last: -Infinity },
      periodic: new Float64Array(this.plan.slots).fill(NaN),
      quiet: new Map(),
      repeats: 0,
    }
  }

  /** @param {object} r one parsed, schema-valid record @returns {void} */
  add(r) {
    if (r.kind === 'identity') this.identity(r)
    else if (r.kind === 'boundary') this.boundary(r)
    else if (r.kind === 'sample') this.sample(r)
    else if (r.kind === 'process') this.process(r)
    else if (r.kind === 'shape') this.shape(r)
    else if (r.kind === 'broker-state' && r.event !== 'isr') this.outage(r)
    else if (r.kind === 'wave') this.wave(r)
    else if (r.kind === 'wave-key') this.waveKey(r)
    else if (r.kind === 'diagnostic') {
      const kind =
        /^(missing-source|partial handler-enter|problem:|expected skip:)/.exec(r.message)?.[1] ??
        (/^probe send \S+ settled after/.test(r.message) ? 'late' : null)
      if (kind)
        this.incidents.set(kind, {
          count: (this.incidents.get(kind)?.count ?? 0) + 1,
          first: this.incidents.get(kind)?.first ?? r.message,
        })
    }
  }

  /**
   * Keeps a broker's down or ready instant for the report, up to two per scheduled outage of a day
   * (`schedule.totals.outages`); beyond that it only counts them.
   * @param {object} r a `broker-state` record other than `isr` @returns {void}
   */
  outage(r) {
    const { node, event, at, downMs = null, processAt = null } = r
    if (this.info.outages.length < this.outageCap)
      this.info.outages.push({ node, event, at, downMs, processAt })
    else this.info.outagesNotShown++
  }

  /**
   * Keeps, per process of the config, the latest end of a `traffic` shape executed as `traffic`:
   * the supervisor writes one after the final drain. Other shapes and sources are no candidates.
   * @param {object} r a `shape` record @returns {void}
   */
  shape(r) {
    if (r.execution !== 'traffic' || r.phase !== 'end' || r.shape !== 'traffic') return
    if (this.procs.has(r.source))
      this.trafficEnds.set(r.source, Math.max(this.trafficEnds.get(r.source) ?? -Infinity, r.at))
  }

  /**
   * A membership wave's transfer or return. It counts its paused keys; until that many `wave-key`
   * records cite it, it stays open, and `finish` lists it as incomplete. At most `waveCap` waves stay
   * open: every client kind running every wave of one membership execution at once, transfer and
   * return each. A wave beyond that is not kept, and the journal reports one problem.
   * @param {object} r a `wave` record @returns {void}
   */
  wave(r) {
    this.info.waves[r.event]++
    if (!r.observed) this.info.waves.unobserved++
    if (!(r.paused > 0)) return
    if (this.openWaves.size < this.waveCap)
      this.openWaves.set(r.recordId, { event: r.event, paused: r.paused, seen: 0 })
    else if (!this.wavesFull) {
      this.wavesFull = true
      this.problem(`more than ${this.waveCap} membership waves wait for their wave-key records`)
    }
  }

  /**
   * One paused key of an open wave: counted as paused for the wave's event, and as held when the
   * wave's member held it. A key citing no open wave is left to the stream validator.
   * @param {object} r a `wave-key` record @returns {void}
   */
  waveKey(r) {
    const wave = this.openWaves.get(r.cause)
    if (!wave) return
    const keys = this.info.waves.keys[wave.event]
    keys.paused++
    if (r.held) keys.held++
    if (++wave.seen === wave.paused) this.openWaves.delete(r.cause)
  }

  /** @param {object} r an identity record @returns {void} */
  identity(r) {
    if (r.stage === 'offered') this.offered(r)
    else if (r.stage === 'transaction') this.decided(r)
    else if (r.stage === 'ack' || r.stage === 'handler-enter') this.byId(r)
  }

  /**
   * An `ack` or `handler-enter` of a standing topic, reduced to the fields the rows read. Processes
   * write the journal concurrently, so a record about an id can precede the id's `offered` record
   * in the stream: it is held until the offer arrives and judged then; `finish` names those whose
   * id was never offered. The ownership of an entry is judged on arrival, because only the order
   * within its own process (the stream keeps it) relates it to that process's boundaries.
   * @param {object} r @returns {void}
   */
  byId(r) {
    if (!this.topics.has(r.key.topic)) return
    if (r.stage === 'handler-enter' && !this.groupIndex.has(r.group)) return
    const fact = {
      stage: r.stage,
      id: r.id,
      offset: r.offset,
      outcome: r.outcome,
      group: r.group,
      key: r.key,
      digest: r.payloadSha256,
      run: r.authority?.run ?? 'no authority',
      ...(r.stage === 'handler-enter' ? this.ownership(r) : {}),
    }
    const i = this.ids.get(r.id)
    if (i !== undefined) return this.resolve(i, fact)
    if (this.idsFull) return
    if (this.heldCount >= this.capacity)
      return this.problem(`more than ${this.capacity} records wait for an id not yet offered`)
    this.heldCount++
    if (this.held.has(r.id)) this.held.get(r.id).push(fact)
    else this.held.set(r.id, [fact])
  }

  /** @param {number} i @param {object} fact from `byId` @returns {void} */
  resolve(i, fact) {
    if (fact.stage === 'ack') this.acknowledge(i, fact)
    else this.entry(i, fact)
  }

  /**
   * Ends the stream: every record still held refers to an id never offered on a standing topic. A
   * send result for one is a journal problem; a handler entry for one is a delivery of a record the
   * journal never offered. A wave still open is listed as incomplete information.
   * @returns {void}
   */
  finish() {
    for (const [id, w] of this.openWaves)
      capped(this.info.incompleteWaves, `${id} has ${w.seen} of ${w.paused} wave-key records`)
    this.openWaves.clear()
    for (const facts of this.held.values())
      for (const f of facts)
        if (f.stage === 'ack') this.problem(`${f.id} is acknowledged but was never offered`)
        else this.note('neverOffered', `${f.id} in ${f.group}`)
    this.held.clear()
    this.heldCount = 0
  }

  /**
   * @param {string} id
   * @returns {{ id: string, outcome: ?string, members: number[] }|null} the transaction, or null
   *   beyond the capacity
   */
  transaction(id) {
    if (!this.txs.has(id) && this.txs.size >= this.capacity) {
      this.problem(`more than ${this.capacity} transactions`)
      return null
    }
    if (!this.txs.has(id)) this.txs.set(id, { id, outcome: null, members: [] })
    return this.txs.get(id)
  }

  /**
   * A transaction's confirmed outcome. A second record for one transaction contradicts the first:
   * the outcome becomes `contradicted`, which K1 never treats as committed or aborted.
   * @param {object} r a `transaction` record @returns {void}
   */
  decided(r) {
    const t = this.transaction(r.transaction)
    if (t && t.outcome !== null) {
      this.note('contradiction', `transaction ${r.transaction} recorded twice`)
      t.outcome = 'contradicted'
    } else if (t) t.outcome = r.outcome
  }

  /** @param {object} r an `offered` record @returns {void} */
  offered(r) {
    const topic = this.topics.get(r.key.topic)
    if (topic === undefined) return
    if (r.key.partition !== 0) this.problem(`${r.id} is on partition ${r.key.partition}`)
    if (this.ids.has(r.id)) return this.problem(`${r.id} is offered twice`)
    const i = this.names.length
    if (i >= this.capacity) {
      this.idsFull = true
      return this.problem(`more than ${this.capacity} offered ids`)
    }
    this.names.push(r.id)
    this.ids.set(r.id, i)
    this.topic[i] = topic
    this.digests.write(r.payloadSha256, i * 32, 32, 'hex')
    if (r.transaction !== null) {
      this.txOf[i] = this.transaction(r.transaction)
      this.txOf[i]?.members.push(i)
    }
    const waiting = this.held.get(r.id)
    if (!waiting) return
    this.held.delete(r.id)
    this.heldCount -= waiting.length
    waiting.forEach((fact) => this.resolve(i, fact))
  }

  /**
   * An id's send result. A second result for one id contradicts the first: the id becomes
   * `contradicted`, never acknowledged. Offset `-1` (a retried send the broker deduplicated
   * without its position) leaves the offset unknown.
   * @param {number} i the id's index @param {object} r an `ack` fact from `byId` @returns {void}
   */
  acknowledge(i, r) {
    if (this.ack[i] !== ACK.none) {
      this.note('contradiction', `${r.id} has a second send result`)
      this.ack[i] = ACK.contradicted
      return
    }
    this.ack[i] = ACK[r.outcome]
    if (r.offset === null || r.offset.startsWith('-')) return
    if (Number.isSafeInteger(Number(r.offset))) this.offset[i] = Number(r.offset)
    else this.problem(`${r.id} has an offset beyond 2^53`)
  }

  /**
   * Whether a record's client, run and authority belong to the process that emitted it: the
   * client's config slot runs in that process and group, and the authority names the same client
   * and one of its runs.
   * @param {object} r a `boundary` or `handler-enter` record @param {object} authority
   * @returns {boolean}
   */
  attributed(r, authority) {
    const slot = this.clients.get(r.client.split('.')[0])
    return (
      slot?.process === r.source &&
      slot.group === r.group &&
      authority.client === r.client &&
      authority.run.startsWith(`${r.client}.`)
    )
  }

  /** @returns {string} the ownership interval key: group, client run and partition */
  static ownerKey(group, { run }, key) {
    return `${group}|${run}|${key.topic}|${key.partition}`
  }

  /**
   * Opens or closes an ownership interval. An `acquire` with the acquisition already open is only
   * counted; one with a new acquisition replaces the interval (a re-acquisition without a revoke).
   * @param {object} r a `boundary` record @returns {void}
   */
  boundary(r) {
    if (!this.topics.has(r.key.topic)) return
    if (!this.attributed(r, r.authority) || r.run !== r.authority.run)
      return this.note('misattributed', `boundary ${r.recordId ?? r.cause} of ${r.client}`)
    const k = Journal.ownerKey(r.group, r.authority, r.key)
    const owner = this.owners.get(k)
    if (r.event === 'revoke-end' || r.event === 'lost') this.close(k)
    if (r.event !== 'acquire') return
    if (owner?.acquiredAt === r.authority.acquiredAt) this.redundantAcquires++
    else if (!owner && this.owners.size >= this.capacity)
      this.problem(`more than ${this.capacity} open ownership intervals`)
    else {
      if (owner) this.close(k)
      this.owners.set(k, { acquiredAt: r.authority.acquiredAt, serial: ++this.serials })
    }
  }

  /** @param {string} k an ownership key; closes its interval and drops what it shadowed @returns {void} */
  close(k) {
    const serial = this.owners.get(k)?.serial
    this.shadowedCount -= this.shadowed.get(serial)?.size ?? 0
    this.shadowed.delete(serial)
    this.owners.delete(k)
  }

  /**
   * The ownership of a handler entry, judged when it arrives: in an ordinary group the entry must
   * lie in an open ownership interval of its client run, whose serial it takes.
   * @param {object} r a `handler-enter` record
   * @returns {{ owned: 'share'|'misattributed'|'unowned'|'owned', serial: number }}
   */
  ownership(r) {
    const outside = (owned) => ({ owned, serial: OUTSIDE })
    if (this.groupIndex.get(r.group).share) return outside('share')
    if (r.authority === null || !this.attributed(r, r.authority)) return outside('misattributed')
    const owner = this.owners.get(Journal.ownerKey(r.group, r.authority, r.key))
    if (owner?.acquiredAt !== r.authority.acquiredAt) return outside('unowned')
    return { owned: 'owned', serial: owner.serial }
  }

  /**
   * Records that an open interval entered id `i` before another interval of its group re-entered
   * it, so a later repeat by the open interval is still found; bounded by the capacity.
   * @param {number} serial @param {number} i @returns {void}
   */
  shadow(serial, i) {
    if (![...this.owners.values()].some((o) => o.serial === serial)) return
    if (this.shadowedCount >= this.capacity)
      return this.problem(`more than ${this.capacity} ids re-entered by overlapping intervals`)
    const set = this.shadowed.get(serial) ?? new Set()
    this.shadowed.set(serial, set)
    if (!set.has(i)) this.shadowedCount++
    set.add(i)
  }

  /**
   * A handler entry, matched to its offered record by topic, partition and full payload digest, and to
   * one offset across every group; then its ownership finding is counted. The same id entered
   * twice by one interval is a same-owner repeat; a repeat in another interval, by another run or
   * in the share group is only counted.
   * @param {number} i the id's index @param {object} e a `handler-enter` fact from `byId`
   * @returns {void}
   */
  entry(i, e) {
    const group = this.groupIndex.get(e.group)
    const where = `${e.id} in ${e.group}`
    if (this.topics.get(e.key.topic) !== this.topic[i] || e.key.partition !== 0)
      return this.note('wrongRecord', `${where} on ${e.key.topic}|${e.key.partition}`)
    if (e.digest !== this.digests.toString('hex', i * 32, i * 32 + 32))
      return this.note('wrongRecord', `${where} with another payload`)
    const offset = Number(e.offset)
    if (Number.isNaN(this.enteredAt[i])) this.enteredAt[i] = offset
    else if (this.enteredAt[i] !== offset)
      return this.note('wrongRecord', `${where} at ${offset}, elsewhere at ${this.enteredAt[i]}`)
    const slot = this.entered[group.index]
    const before = slot[i]
    if (e.owned !== 'owned') {
      if (before === 0) slot[i] = OUTSIDE
      if (e.owned === 'share') this.shareRepeats += before === 0 ? 0 : 1
      else this.note(e.owned, `${where} by ${e.run}`)
      return
    }
    if (before === e.serial || this.shadowed.get(e.serial)?.has(i))
      this.note('sameOwner', `${where} by ${e.run}`)
    else if (before !== 0) this.repeatsAfterChange++
    if (before > OUTSIDE && before !== e.serial) this.shadow(before, i)
    slot[i] = e.serial
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
      const counts = new Map(r.activeResources.map((x) => [x.type, x.count]))
      p[r.role] = { counts, admittedAt: r.admittedAt, completedAt: r.completedAt }
      return
    }
    p.others.first = Math.min(p.others.first, r.admittedAt)
    p.others.last = Math.max(p.others.last, r.completedAt)
    if (r.role === 'periodic') {
      // a slot outside the grid reads undefined, not NaN, so it counts as taken
      if (!Number.isNaN(p.periodic[r.slot])) p.repeats++
      else p.periodic[r.slot] = r.admittedAt
    } else if (r.role === 'quiet') {
      const h = /^h(\d\d)$/.exec(r.boundary)?.[1]
      if (!(Number(h) < this.plan.hours && r.reading < this.plan.readings)) {
        p.repeats++
        return
      }
      const b = p.quiet.get(r.boundary) ?? new Map()
      p.quiet.set(r.boundary, b)
      if (b.has(r.reading)) p.repeats++
      b.set(r.reading, {
        heapUsed: r.heapUsed,
        admittedAt: r.admittedAt,
        completedAt: r.completedAt,
      })
    }
  }
}

/**
 * Reads a file line by line without holding it, hashing the bytes as they pass.
 * @param {string} file @param {(line: string, n: number) => void} onLine
 * @returns {Promise<{ bytes: number, sha256: string, lines: number, unterminated: boolean }>}
 * @throws {Error} when the file cannot be read or holds a line longer than `MAX_LINE`
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
    for (const line of [tail, ...parts])
      if (line.length > MAX_LINE) throw new Error(`a line is longer than ${MAX_LINE} characters`)
    for (const line of parts) onLine(line, ++lines)
  }
  return { bytes, sha256: hash.digest('hex'), lines, unterminated: tail !== '' }
}

/** @param {Map<string, object>} manifest @param {string} file @param {{ bytes: number, sha256: string }} got @param {string[]} problems @returns {void} */
function checkBound(manifest, file, got, problems) {
  const entry = manifest.get(file)
  if (!entry) problems.push(`${file}: the header manifest does not bind it`)
  else if (entry.bytes !== got.bytes || entry.sha256 !== got.sha256)
    problems.push(`${file}: differs from the header manifest`)
}

/**
 * Reads a file whole, but only when it is at most `cap` bytes.
 * @param {string} file @param {number} cap @returns {Buffer}
 * @throws {Error} `larger than <cap> bytes`, or the file system's error
 */
function readCapped(file, cap) {
  if (fs.statSync(file).size > cap) throw new Error(`larger than ${cap} bytes`)
  return fs.readFileSync(file)
}

/**
 * Reads one small ground-truth file whole (at most `MAX_SMALL_FILE` bytes) and checks it against
 * the header manifest.
 * @param {object} ctx `{ dir, manifest }` @param {string} name file below `apache/`
 * @param {string[]} problems receives every problem @param {string} [missing] the text for a missing file
 * @returns {string|null} the text, or null when it is missing or too large
 */
function readBound({ dir, manifest }, name, problems, missing = 'file is missing') {
  const file = path.join(dir, 'apache', name)
  let text
  try {
    text = readCapped(file, MAX_SMALL_FILE).toString('utf8')
  } catch (e) {
    problems.push(`apache/${name}: ${/^larger/.test(e.message) ? e.message : missing}`)
    return null
  }
  const got = { bytes: Buffer.byteLength(text), sha256: sha(text) }
  checkBound(manifest, `apache/${name}`, got, problems)
  return text
}

/**
 * Reads a step's end marker (`exit=<status> lines=<n>`), which must report a zero exit status.
 * @param {object} ctx @param {string} name the step's main file @param {string[]} problems
 * @returns {number|null} the line count the step reports, or null
 */
function readEnd(ctx, name, problems) {
  const cut = 'no end marker: the step was cut off or never ran'
  const text = readBound(ctx, `${name}.end`, problems, cut)
  const m = text === null ? null : /^exit=(-?\d+) lines=(\d+)\n$/.exec(text)
  if (text !== null && !m) problems.push(`apache/${name}.end: unparseable end marker`)
  if (m && m[1] !== '0') problems.push(`apache/${name}: the tool exit status was ${m[1]}`)
  return m ? Number(m[2]) : null
}

/**
 * Streams a topic dump and compares every record with what the journal says was offered and, when
 * a group entered it, with the offset the entries name.
 * @param {object} ctx `{ dir, manifest, journal, config, matrix, dumped }`
 * @param {string} role `main` or `transactions`
 * @returns {Promise<object>} problems (the file cannot be trusted), counts and findings
 */
async function readDump(ctx, role) {
  const { dir, journal, config, matrix, dumped } = ctx
  const name = `${role}.dump`
  const out = { problems: [], count: 0, maxOffset: -1, lastTx: null, findings: {} }
  /** Counts one finding of a kind on this dump, as `Journal#note` does. */
  const note = Journal.prototype.note.bind(out)
  const end = readEnd(ctx, name, out.problems)
  const stderr = readBound(ctx, `${name}.stderr`, out.problems)
  const topic = ROLES.indexOf(role)
  const width = matrix.traffic.valueBytes.value
  const read = await eachLine(path.join(dir, 'apache', name), (line, n) => {
    const m = /^Partition:0\tOffset:(\d{1,15})\t([^\t]*)\t([^\t]*)$/.exec(line)
    if (!m) return capped(out.problems, `apache/${name}: unparseable line ${n}`)
    const [offset, id, value] = [Number(m[1]), m[2], m[3]]
    if (offset <= out.maxOffset)
      capped(out.problems, `apache/${name}: offsets are not increasing at line ${n}`)
    out.count++
    out.maxOffset = Math.max(out.maxOffset, offset)
    const i = journal.ids.get(id)
    out.lastTx = (i !== undefined && journal.txOf[i]) || null
    if (i === undefined || journal.topic[i] !== topic)
      return note('foreign', `${id} at offset ${offset}`)
    if (dumped[i]) return note('duplicate', id)
    dumped[i] = 1
    const t = journal.txOf[i]
    if (t?.outcome === 'aborted') note('abortedPresent', id)
    if (
      journal.ack[i] === ACK.acknowledged &&
      journal.offset[i] !== offset &&
      !Number.isNaN(journal.offset[i])
    )
      note('wrongOffset', `${id} acknowledged at ${journal.offset[i]}, found at ${offset}`)
    if (value !== `${config.runId}|${id}|${t?.id ?? ''}|`.padEnd(width, '.')) note('differs', id)
    if (!Number.isNaN(journal.enteredAt[i]) && journal.enteredAt[i] !== offset)
      journal.note(
        'wrongRecord',
        `${id} entered at ${journal.enteredAt[i]}, in the log at ${offset}`
      )
  }).catch((e) => ({ error: e.message }))
  if (read.error) {
    out.problems.push(`apache/${name}: ${read.error}`)
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
 * Parses one group's describe output as Kafka prints it: a nine-column header, then one row per
 * partition (LAG = LOG-END-OFFSET − CURRENT-OFFSET, or `-` when either is `-`), and for a member
 * without an assignment one row whose topic, partition, offsets and lag are all `-` (skipped).
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
  if (text === null) return out
  const lines = text.split('\n')
  if (end !== null && end !== lines.length - (text.endsWith('\n') ? 1 : 0))
    out.problems.push(`apache/${name}: has other than the ${end} lines its end marker counted`)
  const body = lines.filter((l) => l.trim() !== '')
  const header =
    /^GROUP\s+TOPIC\s+PARTITION\s+CURRENT-OFFSET\s+LOG-END-OFFSET\s+LAG\s+CONSUMER-ID\s+HOST\s+CLIENT-ID$/
  if (!header.test(body[0]?.trim() ?? '')) {
    out.problems.push(`apache/${name}: no describe header row`)
    return out
  }
  /** @param {string} v a column @returns {number|null} its number, null for `-` */
  const num = (v) => (v === '-' ? null : Number(v))
  for (const line of body.slice(1)) {
    const c = line.trim().split(/\s+/)
    /** @param {string} why records one malformed row @returns {void} */
    const bad = (why) => capped(out.problems, `apache/${name}: ${why} "${line.slice(0, 60)}"`)
    if (c.length < 9 || c[0] !== group) bad('unparseable line')
    else if (c.slice(1, 6).every((v) => v === '-')) continue
    else if (!/^\d{1,9}$/.test(c[2]) || !c.slice(3, 5).every((v) => /^(\d{1,15}|-)$/.test(v)))
      bad('unparseable line')
    else {
      const row = { topic: c[1], partition: Number(c[2]), current: num(c[3]), logEnd: num(c[4]) }
      const lag =
        row.current === null || row.logEnd === null ? '-' : String(row.logEnd - row.current)
      if (c[5] !== lag) bad(`LAG is not ${lag}`)
      else if (out.rows.some((x) => x.topic === row.topic && x.partition === row.partition))
        bad('a second row for one partition')
      else out.rows.push(row)
    }
  }
  return out
}

/**
 * The fewest offsets of the transactions partition that a read-committed reader cannot see, as the
 * journal explains them: one control marker per decided transaction with an acknowledged member
 * and every acknowledged member of an aborted transaction. Kafka may write a retried marker twice,
 * so there is no upper bound.
 * @param {Journal} journal @returns {number}
 */
function invisibleAtLeast(journal) {
  let least = 0
  for (const t of journal.txs.values()) {
    const acked = t.members.filter((i) => journal.ack[i] === ACK.acknowledged).length
    if (acked > 0 && t.outcome === 'committed') least += 1
    else if (acked > 0 && t.outcome === 'aborted') least += 1 + acked
  }
  return least
}

/**
 * The most offsets after the transactions dump's last record that the journal explains as invisible
 * to a read-committed reader: the acknowledged members of aborted transactions placed there, one
 * control marker for each of their transactions, and the marker that closes the transaction of the
 * last dumped record. A marker follows the records it closes, and the coordinator refuses a
 * transactional id's next transaction until its markers are written. Anything else there (a retried
 * marker, an aborted member whose offset is unknown, a visible record the dump was cut off before)
 * is unexplained, so the dump stays unproved rather than possibly hiding a visible record.
 * @param {Journal} journal @param {object} dump `readDump`'s result @param {number} end the log end
 * @returns {number}
 */
function invisibleAfter(journal, dump, end) {
  const closing = new Set()
  if (/^(committed|aborted)$/.test(dump.lastTx?.outcome)) closing.add(dump.lastTx)
  let members = 0
  for (const t of journal.txs.values()) {
    if (t.outcome !== 'aborted') continue
    for (const i of t.members) {
      const at = journal.offset[i]
      if (journal.ack[i] !== ACK.acknowledged || !(at > dump.maxOffset && at < end)) continue
      members++
      closing.add(t)
    }
  }
  return members + closing.size
}

/**
 * Whether a dump holds every visible record of its partition below the log end Apache's describe
 * reports, and why not. Every dumped offset must lie below that end. The main partition holds only
 * visible records, so its dump must hold exactly the end's count. The console consumer reads in
 * offset order and can only be cut off, so a transactions dump is complete up to its last record;
 * after it, every offset up to the end must be one the journal explains as invisible
 * (`invisibleAfter`), and the dump must reach every acknowledged visible record.
 * @param {Journal} journal @param {object} dump `readDump`'s result @param {number} end
 * @param {number} n 0 for main, 1 for transactions @param {number} lastVisible the highest
 *   acknowledged offset of a visible transactions record
 * @returns {string|false} the reason it is not proved complete, or false
 */
function incomplete(journal, dump, end, n, lastVisible) {
  if (dump.maxOffset >= end) return `offset ${dump.maxOffset} lies at or beyond the log end`
  if (n === 0) return dump.count !== end && `${dump.count} records dumped`
  if (dump.maxOffset < lastVisible) return `a visible record was acknowledged at ${lastVisible}`
  const least = invisibleAtLeast(journal)
  if (end - dump.count < least)
    return `${dump.count} records dumped but at least ${least} offsets are invisible`
  const [after, most] = [end - 1 - dump.maxOffset, invisibleAfter(journal, dump, end)]
  return (
    after > most &&
    `${after} offsets follow the last dumped record but the journal explains at most ${most} of them as invisible`
  )
}

/**
 * K1: every acknowledged id, and every non-rejected member of a committed transaction, is in the
 * read-committed dump once at its acknowledged offset (anywhere when the offset is unknown),
 * aborted members never, and each dump is proved complete up to Apache's log end (`incomplete`).
 * Unknown results, contradictions and unproved completeness are INCONCLUSIVE, never PASS.
 * @param {Row} k1 @param {object} ctx `{ journal, dumps, ends, dumped }`
 * @returns {boolean} whether the dumps are usable and proved complete
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
  const contradiction = journal.findings.contradiction
  if (contradiction)
    k1.gap(
      `${contradiction.count} ids or transactions have contradictory terminal records, so the journal cannot say which is true${first(contradiction)}`
    )
  const absent = { hard: [], soft: [] }
  const uncertain = []
  let lastVisible = -1
  for (let i = 0; i < journal.names.length; i++) {
    const [t, ack, at] = [journal.txOf[i], journal.ack[i], journal.offset[i]]
    if (t?.outcome === 'committed' && ack !== ACK.rejected && at > lastVisible) lastVisible = at
    if (dumped[i] || t?.outcome === 'aborted' || ack === ACK.rejected) continue
    if (t ? t.outcome === 'committed' : ack === ACK.acknowledged) {
      const [d, end] = [dumps[journal.topic[i]], ends[journal.topic[i]]]
      absent[at < d.maxOffset || (end !== null && at >= end) ? 'hard' : 'soft'].push(
        journal.names[i]
      )
    } else if (!t) uncertain.push(journal.names[i])
  }
  const complete = dumps.every((d, n) => {
    if (ends[n] === null) {
      k1.gap(`no describe row gives the ${ROLES[n]} log end`)
      return false
    }
    const why = incomplete(journal, d, ends[n], n, lastVisible)
    if (why)
      k1.gap(
        `the ${ROLES[n]} dump stops at offset ${d.maxOffset} and the log ends at ${ends[n]}: ${why}, so it is not proved complete`
      )
    return !why
  })
  /** @param {string[]} ids @returns {string} the count phrase of absent ids */
  const list = (ids) => `${ids.length} acknowledged or committed ids absent from the dump`
  /** @param {string[]} ids @returns {string} the first few ids */
  const some = (ids) => ` (first ${ids.slice(0, EXAMPLES).join(', ')})`
  if (absent.hard.length > 0) k1.fail(`${list(absent.hard)} inside the log${some(absent.hard)}`)
  const text = `${list(absent.soft)}${some(absent.soft)}`
  if (absent.soft.length > 0 && complete) k1.fail(text)
  else if (absent.soft.length > 0) k1.gap(text)
  if (uncertain.length > 0)
    k1.gap(`${uncertain.length} unknown-result sends absent from the log${some(uncertain)}`)
  const open = [...journal.txs.values()].filter(
    (t) => t.members.length > 0 && !/^(committed|aborted)$/.test(t.outcome)
  )
  if (open.length > 0)
    k1.gap(
      `${open.length} transactions have an unknown, missing or contradicted outcome (first ${open[0].id})`
    )
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
 * K2: every handler entry names an offered id and matches its record exactly (topic, partition, full
 * payload digest, one offset that is the log's). When the dump is proved complete, every visible
 * record entered a handler in every recipient group of its topic, and no judged group entered a
 * record a read-committed reader cannot see or a record of a topic the group does not read.
 * @param {Row} k2 @param {Journal} journal @param {Uint8Array} dumped @param {boolean} complete
 * @returns {void}
 */
function judgeDelivery(k2, journal, dumped, complete) {
  const wrong = journal.findings.wrongRecord
  if (wrong) k2.fail(`${wrong.count} handler entries do not match their record${first(wrong)}`)
  const phantom = journal.findings.neverOffered
  if (phantom) k2.fail(`${phantom.count} handler entries name an id never offered${first(phantom)}`)
  if (!complete) return
  const groups = [...journal.groupIndex.keys()]
  const reads = journal.recipients.map((rs) => new Set(rs.map((r) => r.index)))
  const [missing, foreign] = [new Map(), new Map()]
  const invisible = { count: 0, examples: [] }
  /** @param {Map} map @param {number} g @param {number} i counts record i for group g */
  const count = (map, g, i) => {
    const m = map.get(groups[g]) ?? { count: 0, first: journal.names[i] }
    map.set(groups[g], { ...m, count: m.count + 1 })
  }
  let visible = 0
  for (let i = 0; i < journal.names.length; i++) {
    const recipient = reads[journal.topic[i]]
    if (dumped[i]) visible++
    for (let g = 0; g < groups.length; g++) {
      const entered = journal.entered[g][i]
      if (!dumped[i] && entered && invisible.count++ < EXAMPLES)
        invisible.examples.push(`${journal.names[i]} in ${groups[g]}`)
      else if (dumped[i] && !recipient.has(g) && entered) count(foreign, g, i)
      else if (dumped[i] && recipient.has(g) && !entered) count(missing, g, i)
    }
  }
  for (const [group, m] of missing)
    k2.fail(`${group} never entered a handler for ${m.count} visible records (first ${m.first})`)
  for (const [group, m] of foreign)
    k2.fail(`${group} entered ${m.count} records of a topic it does not read (first ${m.first})`)
  if (invisible.count > 0)
    k2.fail(
      `${invisible.count} records not visible to a read-committed reader entered a handler${first(invisible)}`
    )
  Object.assign(k2.numbers, { visibleRecords: visible, groupsJudged: groups.length })
}

/**
 * K3: a repeat within one ownership interval of one client run, an entry outside every ownership
 * interval of its run, and an entry or boundary attributed to another client, run or process fail;
 * every other repeat is counted and printed, not judged.
 * @param {Row} row @param {Journal} journal @returns {void}
 */
function judgeDuplicates(row, journal) {
  const { sameOwner, unowned, misattributed } = journal.findings
  if (sameOwner)
    row.fail(
      `${sameOwner.count} same-owner repeats within one ownership interval${first(sameOwner)}`
    )
  if (unowned)
    row.fail(`${unowned.count} entries outside an ownership interval of their run${first(unowned)}`)
  if (misattributed)
    row.fail(
      `${misattributed.count} entries or boundaries attributed to another client, run or process${first(misattributed)}`
    )
  const { repeatsAfterChange, shareRepeats, redundantAcquires } = journal
  Object.assign(row.numbers, {
    sameOwnerRepeats: sameOwner?.count ?? 0,
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
 * three boundaries and the slope is the least-squares fit of Y over X (NaN without time spread).
 * @param {{ heapUsed: number, completedAt: number }[][]} boundaries readings per boundary, in order
 * @returns {{ endpoint: number, slope: number }}
 */
function heapScreen(boundaries) {
  const Y = boundaries.map((b) => upperMedian(b.map((r) => r.heapUsed)))
  const X = boundaries.map((b) => upperMedian(b.map((r) => r.completedAt)) / 60000)
  const n = Y.length
  const edge = Math.max(1, Math.min(3, Math.floor(n / 2)))
  const endpoint = upperMedian(Y.slice(n - edge)) - upperMedian(Y.slice(0, edge))
  const mean = (v) => v.reduce((s, x) => s + x, 0) / v.length
  const [mx, my] = [mean(X), mean(Y)]
  const den = X.reduce((s, x) => s + (x - mx) ** 2, 0)
  const slope =
    Math.max(...X) > Math.min(...X)
      ? X.reduce((s, x, i) => s + (x - mx) * (Y[i] - my), 0) / den
      : NaN
  return { endpoint, slope }
}

/**
 * K5: the heap screen of each unseeded standing process on its quiet triplets, one boundary per
 * hour. A boundary counts only when all its readings were admitted inside its hour's quiet window
 * (from the reading's time to the quiet deadline) and completed within that hour. Fewer than two
 * boundaries cannot be screened and are INCONCLUSIVE.
 * @param {Row} row @param {object} ctx `{ journal, matrix, config, plan, header }` @returns {void}
 */
function judgeHeap(row, { journal, matrix, config, plan, header }) {
  const limits = {
    endpoint: matrix.bounds['heap-endpoint'].value,
    slope: matrix.bounds['heap-slope'].value,
  }
  for (const { slot } of config.processes.filter((p) => p.role === 'subject')) {
    const series = []
    for (let h = 0; h < plan.hours; h++) {
      const label = `h${String(h).padStart(2, '0')}`
      const readings = journal.procs.get(slot).quiet.get(label)
      if (!readings || readings.size < plan.readings) continue
      const H = header.window.runStartAt + h * plan.hourMs
      const inside = [...readings].every(
        ([n, r]) =>
          r.admittedAt >= H + plan.quietAt[n] &&
          r.admittedAt <= H + plan.quietDeadline &&
          r.completedAt <= H + plan.hourMs
      )
      if (inside) series.push([...readings.values()])
      else row.gap(`${slot} ${label} readings lie outside the hour's quiet window`)
    }
    if (series.length < Math.max(plan.hours, 2))
      row.gap(`${slot} has ${series.length} of ${plan.hours} quiet boundaries (at least 2 needed)`)
    if (series.length < 2) continue
    const r = heapScreen(series)
    row.numbers[slot] = { endpoint: r.endpoint, slopePerMinute: Math.round(r.slope) }
    if (!(r.endpoint <= limits.endpoint))
      row.fail(`${slot} heap endpoint ${r.endpoint} bytes exceeds ${limits.endpoint}`)
    if (!(r.slope <= limits.slope))
      row.fail(`${slot} heap slope ${Math.round(r.slope)} B/min exceeds ${limits.slope}`)
  }
}

/**
 * K6: every process's final resource counts are at or below its baseline; a type the baseline never
 * had counts against zero, so an unknown type fails. The baseline must precede every other sample
 * of its process and the final follow every other sample and the end of traffic. The end of traffic
 * is the supervisor's (`header.exit.supervisorProcess`) latest `traffic` shape end, written after
 * the final drain, and must lie between the run's end and the supervisor's observed end.
 * @param {Row} row @param {{ journal: Journal, config: object, header: object }} ctx @returns {void}
 */
function judgeResources(row, { journal, config, header }) {
  let above = 0
  const end = journal.trafficEnds.get(header.exit.supervisorProcess) ?? null
  const { runEndAt, observedEndAt } = header.window
  if (end === null)
    row.gap('the journal records no end of traffic (the final drain) from the supervisor')
  else if (end < runEndAt || end > observedEndAt)
    row.gap(
      `the end of traffic at ${end} lies outside the run end ${runEndAt} to the observed end ${observedEndAt}`
    )
  for (const { slot } of config.processes) {
    const { baseline, final, others } = journal.procs.get(slot)
    if (!baseline || !final) {
      row.gap(`${slot} has no ${baseline ? 'final' : 'baseline'} sample`)
      continue
    }
    if (baseline.completedAt > others.first)
      row.gap(`${slot} baseline was completed after its first other sample`)
    if (final.admittedAt < Math.max(others.last, end ?? Infinity))
      row.gap(`${slot} final sample was not taken last and after the end of traffic`)
    for (const [type, count] of final.counts) {
      if (count <= (baseline.counts.get(type) ?? 0)) continue
      above++
      row.fail(
        `${slot} final ${type} ${count} exceeds its baseline ${baseline.counts.get(type) ?? 0}`
      )
    }
  }
  Object.assign(row.numbers, { trafficEnd: end, typesAboveBaseline: above })
}

/**
 * K7: parent-observed exits (code 0, no signal), a run as long as its mode requires, a complete
 * sample grid admitted inside each slot's window, a clean supervisor receipt and a valid,
 * digest-matching stream.
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
      const due = w.runStartAt + k * plan.periodMs
      if (Number.isNaN(p.periodic[k])) missing.push(k)
      else if (p.periodic[k] < due - plan.jitterMs)
        row.fail(`${slot} slot ${k} was admitted early at ${p.periodic[k]}`)
      else if (p.periodic[k] > due + plan.jitterMs)
        row.fail(`${slot} slot ${k} was admitted late at ${p.periodic[k]}`)
    }
    if (missing.length > 0)
      row.fail(`${slot} misses periodic slot ${missing[0]} (${missing.length} missing)`)
  }
  if (supervisor === null) row.gap('supervisor.json is missing or unreadable')
  else {
    const s = supervisor
    if (s.aborted) row.fail(`the supervisor aborted the run: ${s.aborted}`)
    if (s.incomplete) row.fail(`the supervisor reports an incomplete run: ${s.incomplete}`)
    if (s.problems?.length > 0)
      row.fail(`the driver reports ${s.problems.length} problems: ${s.problems[0]}`)
    if (s.forced?.length > 0) row.fail(`the supervisor forced ${s.forced.join(', ')} to end`)
    if (s.failures?.length > 0)
      row.fail(`the collector recorded ${s.failures.length} failures: ${s.failures[0]}`)
    if (s.steps?.error > 0 || s.steps?.aborted > 0)
      row.fail(`${s.steps.error} steps failed, ${s.steps.aborted} aborted`)
    for (const f of ['runId', 'mode', 'treeSha'])
      if (s[f] !== header[f]) row.fail(`supervisor.json ${f} differs from the header`)
    if (mode === 'acceptance' && s.diagnostic) row.fail('the run is marked diagnostic')
    row.numbers.lateSteps = s.steps?.late ?? null
  }
  const { incidents } = journal
  for (const kind of ['problem:', 'missing-source'])
    if (incidents.has(kind))
      row[kind === 'problem:' ? 'fail' : 'gap'](
        `${incidents.get(kind).count} ${kind} diagnostics (first: ${incidents.get(kind).first})`
      )
}

/**
 * What the journal shows but no row judges: broker outages (down and ready instants, configured
 * down time, container stop or start instant; two per scheduled outage, with a count of any more), membership wave transfers and returns with the
 * paused and held keys their `wave-key` records name, waves missing some of their `wave-key`
 * records (the stream validator fails those too), probe sends that settled after their step, and
 * drain commits skipped as expected.
 * @param {Journal} journal @returns {object}
 */
function informationOf(journal) {
  const count = (kind) => journal.incidents.get(kind)?.count ?? 0
  return {
    outages: journal.info.outages,
    outagesNotShown: journal.info.outagesNotShown,
    waves: journal.info.waves,
    incompleteWaves: journal.info.incompleteWaves,
    lateProbeSends: count('late'),
    expectedSkips: count('expected skip:'),
  }
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
  journal.finish()
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
    const complete = judgeLog(rows.K1, { journal, dumps, ends, dumped: ctx.dumped })
    judgeDelivery(rows.K2, journal, ctx.dumped, complete)
    if (!complete) rows.K2.gap(`delivery cannot be judged: ${rows.K1.gaps[0]}`)
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
  /** @param {string} verdict @returns {boolean} whether any row has it */
  const has = (verdict) => done.some((r) => r.verdict === verdict)
  return {
    runId: header.runId,
    mode,
    treeSha: header.treeSha,
    rows: done,
    notJudged: NOT_JUDGED_ROWS,
    information: informationOf(journal),
    overall: has('FAIL') ? 'REJECTED' : has('INCONCLUSIVE') ? 'NOT ACCEPTED' : 'ACCEPTED',
  }
}

/**
 * Reads one JSON side file of the artifact whole, at most `MAX_JSON` bytes.
 * @param {string} dir @param {string} file @param {string[]} problems receives a size or read
 *   problem; a missing file is left to the caller
 * @returns {{ bytes: Buffer, json: object|null }|null} its bytes and parsed value (null when not
 *   JSON), or null when it is missing or cannot be read
 */
function readSide(dir, file, problems) {
  let bytes
  try {
    bytes = readCapped(path.join(dir, file), MAX_JSON)
  } catch (e) {
    if (e.code !== 'ENOENT') problems.push(`${file}: ${e.message}`)
    return null
  }
  try {
    return { bytes, json: JSON.parse(bytes) }
  } catch {
    return { bytes, json: null }
  }
}

/**
 * Reads and validates an artifact directory, streams its journal once and judges it. An artifact
 * whose inputs cannot be read fails K7 and leaves every other row INCONCLUSIVE.
 * @param {string} dir the artifact directory
 * @returns {Promise<object>} the report
 */
async function checkArtifact(dir) {
  const reviewed = fs.readFileSync(REVIEWED_MATRIX)
  const matrix = JSON.parse(reviewed)
  const problems = [...schema.validateMatrix(matrix)]
  const side = Object.fromEntries(
    ['header.json', 'config.json', 'supervisor.json', 'outer-exit.json'].map((file) => [
      file,
      readSide(dir, file, problems),
    ])
  )
  const [header, config] = [side['header.json']?.json, side['config.json']?.json]
  let archived = null
  try {
    archived = readCapped(path.join(dir, ARCHIVED_MATRIX), MAX_JSON)
  } catch {}
  if (!archived?.equals(reviewed))
    problems.push(
      `the archived matrix is missing or differs from the matrix of the checkout this checker runs from (${CHECKOUT})`
    )
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
    const mode = header?.mode ?? 'acceptance'
    return {
      runId: null,
      mode,
      treeSha: null,
      rows,
      notJudged: NOT_JUDGED_ROWS,
      overall: 'REJECTED',
    }
  }
  const manifest = new Map(header.files.map((f) => [f.file, f]))
  for (const file of ['config.json', 'supervisor.json', 'outer-exit.json']) {
    const bytes = side[file]?.bytes
    if (bytes) checkBound(manifest, file, { bytes: bytes.length, sha256: sha(bytes) }, problems)
  }
  const outer = side['outer-exit.json']?.json
  if (outer?.status !== header.exit.code || outer?.observedAt !== header.exit.observedAt)
    problems.push('outer-exit.json is missing or disagrees with the header exit')
  const journal = new Journal({ matrix, config, mode: header.mode })
  const validator = new schema.StreamValidator({ matrix, config, header })
  const read = await eachLine(path.join(dir, header.stream.file), (line, n) => {
    validator.push(line)
    try {
      journal.add(JSON.parse(line))
    } catch (e) {
      capped(problems, `line ${n}: the record could not be read: ${e.message}`)
    }
  }).catch((e) => ({ error: e.message }))
  if (read.error) problems.push(`the stream file cannot be read: ${read.error}`)
  else if (read.unterminated) problems.push('the stream file ends without a newline')
  problems.push(...validator.finish())
  const supervisor = side['supervisor.json']?.json ?? null
  return judge({ dir, matrix, config, mode: header.mode, header, supervisor, journal, problems })
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
  lines.push('', 'NOT JUDGED (outside this checker; each line names the CI job that covers it):')
  for (const item of report.notJudged)
    lines.push(`  NOT JUDGED  ${item.name}  (lane: ${item.lane})`)
  const info = report.information
  if (info) {
    lines.push('', 'NOT JUDGED information (printed for the reader, never a verdict):')
    for (const o of info.outages)
      lines.push(
        `  broker ${o.node} ${o.event} at ${o.at} (configured down ${o.downMs ?? '-'} ms, container at ${o.processAt ?? '-'})`
      )
    if (info.outagesNotShown > 0)
      lines.push(`  ${info.outagesNotShown} more broker down or ready records not shown`)
    const w = info.waves
    const keys = (event) =>
      `${w.keys[event].paused} paused keys, ${w.keys[event].held} held by the member`
    lines.push(
      `  wave transfers ${w.transfer} (${keys('transfer')}), returns ${w.return} (${keys('return')}), not observed ${w.unobserved}`
    )
    for (const wave of info.incompleteWaves) lines.push(`  incomplete wave: ${wave}`)
    lines.push(`  probe sends settled after their step: ${info.lateProbeSends}`)
    lines.push(`  drain commits skipped as expected: ${info.expectedSkips}`)
  }
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
