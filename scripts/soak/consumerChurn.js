'use strict'

/**
 * The consumer-churn profile: a verifier that works only on a journal of public observations, the
 * public probe that counts what the clients leave behind, and the run that joins them. Nothing
 * here imports the client's sources or reads its private state, so a client defect cannot hide in
 * its own bookkeeping.
 */
const fs = require('node:fs')
const { ENDPOINT_BYTES, SLOPE_BYTES_PER_MINUTE, median, slope } = require('./bounds')

/** Traversal steps between two event-loop yields while a journal is checked. */
const YIELD_EVERY = 4096

/** The kinds of replay a journal may declare. */
const REPLAY_REASONS = Object.freeze(['seek', 'reset', 'crash'])

/**
 * Audits a closed, issue-ordered journal of acknowledged log records, ownership boundaries, handler
 * deliveries and broker-confirmed commits, independently of client internals. Three sets are kept
 * apart: the acknowledged log (`produce`, confirmed by `read`), the broker commits (`commit`
 * carries the position a broker read returned) and the handler multiset (`handler`). A commit may
 * not pass the contiguous prefix of acknowledged offsets the group has handled, so one late record
 * cannot vouch for the lower ones; offsets that were never acknowledged (control or sparse ranges)
 * need no delivery.
 *
 * The oracle is streaming: `feed` takes one segment at a time and a verified incarnation is evicted
 * at its `delete`, so live state is bounded by the incarnations still in flight, not by the run.
 * Records are objects with `kind`, `group`, `incarnation`, `topic`, `partition`, `offset`
 * (decimal string) and `sequence`; `owner` names the acquisition, and a `commit` also has `next`
 * and `brokerNext`. A `position` record moves a group's start, a `replay` declares an interval
 * `[from, to)` a group may legitimately handle again. A `produce` record may name its own intended
 * `groups`, a subset of the constructor's; without it every constructor group is intended.
 */
class ConsumerChurnOracle {
  /**
   * @param {object[]} records issue-ordered observations of one bounded segment; a streaming caller
   *   passes none and uses `feed`
   * @param {string[]} groups intended recipient groups, including silent groups
   * @returns {ConsumerChurnOracle} independent verifier
   */
  constructor(records, groups) {
    this.records = records
    this.groups = [...groups]
    this.counts = { acknowledged: 0, delivered: 0, commits: 0 }
    this.work = 0
    this.acknowledged = new Map()
    this.logs = new Map()
    this.read = new Set()
    this.handled = new Set()
    this.active = new Map()
    this.cursors = new Map()
    this.starts = new Map()
    this.confirmed = new Map()
    this.replays = new Map()
  }

  /**
   * @param {object} record incarnation, topic, partition, offset and application sequence
   * @returns {string} collision-safe identity without Number conversion of offsets
   */
  key(record) {
    return JSON.stringify([
      record.incarnation,
      record.topic,
      record.partition,
      record.offset,
      record.sequence,
    ])
  }

  /**
   * Verifies the constructor's segment as a whole journal.
   * @returns {Promise<{acknowledged: number, delivered: number, commits: number}>} counts
   * @throws {Error} on loss, invalid ownership, replay, deletion or commit mismatch
   */
  async verify() {
    await this.feed(this.records)
    return this.finish()
  }

  /**
   * Checks one more segment against everything carried so far. Yields after at most 4096 traversal
   * steps, including the passes a deletion makes.
   * @param {object[]} records the next observations in issue order
   * @returns {Promise<void>}
   * @throws {Error} on the first violated invariant
   */
  async feed(records) {
    for (const record of records) {
      if (record.kind === 'produce') await this.#produce(record)
      else await this.#observe(record)
      await this.#checkpoint()
    }
  }

  /**
   * Ends the journal: every acknowledged record still carried must have been read and delivered to
   * each intended group.
   * @returns {Promise<{acknowledged: number, delivered: number, commits: number}>} counts
   * @throws {Error} when an acknowledged record was lost
   */
  async finish() {
    for (const [key, produced] of this.acknowledged) {
      if (!this.read.has(key)) this.#fail('acknowledged log loss')
      for (const group of this.#intended(produced)) {
        if (!this.handled.has(JSON.stringify([group, key]))) this.#fail('acknowledged handler loss')
        await this.#checkpoint()
      }
      await this.#checkpoint()
    }
    return { ...this.counts }
  }

  /** @returns {{ acknowledged: number, scopes: number }} what the oracle still carries live */
  retained() {
    return { acknowledged: this.acknowledged.size, scopes: this.active.size }
  }

  async #checkpoint() {
    if (++this.work % YIELD_EVERY === 0) await new Promise(setImmediate)
  }

  #fail(reason) {
    throw new Error(`Consumer journal: ${reason}`)
  }

  #intended(produced) {
    return produced.groups ?? this.groups
  }

  #scope(record) {
    return JSON.stringify([record.group, record.incarnation, record.topic, record.partition])
  }

  #log(record) {
    return JSON.stringify([record.incarnation, record.topic, record.partition])
  }

  #produce(record) {
    const key = this.key(record)
    if (this.acknowledged.has(key)) this.#fail('duplicate acknowledgement')
    const offset = BigInt(record.offset)
    const log = this.#log(record)
    const entry = this.logs.get(log) ?? { offsets: [], keys: [] }
    if (entry.offsets.length > 0 && offset <= entry.offsets[entry.offsets.length - 1])
      this.#fail('acknowledgement out of offset order')
    entry.offsets.push(offset)
    entry.keys.push(key)
    this.logs.set(log, entry)
    this.acknowledged.set(key, record)
    this.counts.acknowledged += 1
  }

  async #observe(record) {
    const id = this.#scope(record)
    const key = this.key(record)
    switch (record.kind) {
      case 'acquire':
        if (this.active.has(id)) this.#fail('overlapping ownership')
        this.active.set(id, record.owner)
        break
      case 'revoke':
      case 'loss':
        if (this.active.get(id) !== record.owner) this.#fail('wrong ownership boundary')
        this.active.delete(id)
        break
      case 'position':
        this.#position(record, id)
        break
      case 'read':
        if (!this.acknowledged.has(key)) this.#fail('unexpected log record')
        this.read.add(key)
        break
      case 'replay':
        this.#replay(record, id)
        break
      case 'handler':
        this.#handle(record, id, key)
        break
      case 'commit':
        this.#commit(record, id)
        break
      case 'delete':
        await this.#delete(record)
        break
      default:
        this.#fail('unknown observation')
    }
  }

  /** A broker read at acquisition, or an intentional move, fixes where the group's prefix starts. */
  #position(record, id) {
    const start = BigInt(record.next)
    if (start < 0n) this.#fail('negative position')
    this.starts.set(id, start)
    this.cursors.delete(id)
  }

  #replay(record, id) {
    if (!REPLAY_REASONS.includes(record.reason)) this.#fail('unknown replay reason')
    if (BigInt(record.from) > BigInt(record.to)) this.#fail('reversed replay interval')
    if (record.reason === 'crash' && BigInt(record.from) !== (this.confirmed.get(id) || 0n))
      this.#fail('replay boundary differs from confirmed commit')
    this.replays.set(id, record)
  }

  #handle(record, id, key) {
    if (!this.groups.includes(record.group)) this.#fail('unexpected group')
    const produced = this.acknowledged.get(key)
    if (!produced) this.#fail('unexpected handler record')
    if (!this.#intended(produced).includes(record.group)) this.#fail('unexpected group')
    if (this.active.get(id) !== record.owner) this.#fail('handler after ownership boundary')
    const deliveryKey = JSON.stringify([record.group, key])
    const offset = BigInt(record.offset)
    if (this.handled.has(deliveryKey)) {
      const replay = this.replays.get(id)
      if (!replay || offset < BigInt(replay.from) || offset >= BigInt(replay.to))
        this.#fail('undeclared handler replay')
    }
    this.handled.add(deliveryKey)
    this.counts.delivered += 1
    this.#advance(record, id, record.group)
  }

  /** Moves the group's contiguous prefix over every acknowledged offset it has now handled. */
  #advance(record, id, group) {
    const { offsets, keys } = this.logs.get(this.#log(record))
    let at = this.#cursor(id, offsets)
    while (at < offsets.length && this.handled.has(JSON.stringify([group, keys[at]]))) at += 1
    this.cursors.set(id, at)
    const replay = this.replays.get(id)
    const passed = at < offsets.length ? offsets[at] : offsets[at - 1] + 1n
    if (replay && passed >= BigInt(replay.to)) this.replays.delete(id)
  }

  #cursor(id, offsets) {
    if (this.cursors.has(id)) return this.cursors.get(id)
    const start = this.starts.get(id) ?? 0n
    let at = 0
    while (at < offsets.length && offsets[at] < start) at += 1
    return at
  }

  #commit(record, id) {
    if (this.active.get(id) !== record.owner) this.#fail('commit after ownership boundary')
    if (typeof record.brokerNext !== 'string' || record.next !== record.brokerNext)
      this.#fail('unconfirmed broker commit')
    const next = BigInt(record.next)
    if (next < 0n || next > this.#eligible(record, id))
      this.#fail('commit beyond processed position')
    this.confirmed.set(id, BigInt(record.brokerNext))
    this.counts.commits += 1
  }

  /** The furthest position a commit may name: the first acknowledged offset still undelivered. */
  #eligible(record, id) {
    const { offsets } = this.logs.get(this.#log(record)) ?? { offsets: [] }
    const at = this.#cursor(id, offsets)
    if (at < offsets.length) return offsets[at]
    const start = this.starts.get(id) ?? 0n
    const last = offsets.length > 0 ? offsets[offsets.length - 1] + 1n : 0n
    return last > start ? last : start
  }

  async #delete(record) {
    for (const [recordKey, produced] of this.acknowledged) {
      if (produced.incarnation === record.incarnation && produced.topic === record.topic) {
        if (!this.read.has(recordKey)) this.#fail('delete before log verification')
        for (const group of this.#intended(produced)) {
          if (!this.handled.has(JSON.stringify([group, recordKey])))
            this.#fail('delete before handler drain')
          await this.#checkpoint()
        }
      }
      await this.#checkpoint()
    }
    this.#evict(record)
  }

  /** Drops what only the deleted incarnation's verified records held. */
  #evict({ incarnation, topic }) {
    for (const [key, produced] of this.acknowledged) {
      if (produced.incarnation !== incarnation || produced.topic !== topic) continue
      this.acknowledged.delete(key)
      this.read.delete(key)
      for (const group of this.#intended(produced))
        this.handled.delete(JSON.stringify([group, key]))
    }
    for (const log of this.logs.keys()) {
      const [owner, name] = JSON.parse(log)
      if (owner === incarnation && name === topic) this.logs.delete(log)
    }
    for (const map of [this.cursors, this.starts, this.confirmed, this.replays, this.active]) {
      for (const id of map.keys()) {
        const [, owner, name] = JSON.parse(id)
        if (owner === incarnation && name === topic) map.delete(id)
      }
    }
  }
}

/**
 * The fixed timing and workload of the profile, declared before any result: one actual warm-up,
 * four measured phases, and the inclusive child budget they sit in.
 */
const CONSUMER_CHURN = Object.freeze({
  warmupMs: 180000,
  setupAndWarmupMs: 240000,
  phaseMs: 300000,
  transitionMs: 60000,
  finalMs: 180000,
  childMs: 1800000,
  progressMs: 30000,
  rate: 200,
  payloadBytes: 200,
  maxInFlight: 64,
  minGaps: 20001,
  quietSamples: 3,
  socketGraceMs: 5000,
  interningObjects: 2097152,
  interningGrowthBytes: 3 * 1024 * 1024,
  segment: YIELD_EVERY,
  groups: Object.freeze(['member-churn-cooperative', 'member-churn-consumer']),
  phases: Object.freeze([
    Object.freeze({ name: 'member-churn', workload: 'memberChurn' }),
    Object.freeze({ name: 'regex-topics', workload: 'regexTopics' }),
    Object.freeze({ name: 'pause-recreate', workload: 'pauseRecreate' }),
    Object.freeze({ name: 'mostly-idle', workload: 'mostlyIdle' }),
  ]),
})

/** The suffix each phase adds to a group id; the warm-up shares the first phase's groups. */
const GROUP_SUFFIX = Object.freeze({
  'warm-up': '',
  'member-churn': '',
  'regex-topics': '-rx',
  'pause-recreate': '-pause',
  'mostly-idle': '-idle',
})

/**
 * The one mapping from the profile's base groups to the group ids the clients join. The workloads
 * join them, and the run's oracle, gap sampler and drain checks are built from the same object.
 */
class ChurnGroups {
  /**
   * @param {string} tag the run's unique stem
   * @param {readonly string[]} [bases=CONSUMER_CHURN.groups]
   */
  constructor(tag, bases = CONSUMER_CHURN.groups) {
    this.tag = tag
    this.bases = [...bases]
  }

  /**
   * @param {string} base a base group
   * @param {string} phase the warm-up or a phase of the plan
   * @returns {string} the group id the clients of that phase join
   * @throws {Error} for a phase with no groups
   */
  id(base, phase) {
    const suffix = GROUP_SUFFIX[phase]
    if (suffix === undefined) throw new Error(`No consumer churn groups for phase ${phase}`)
    return `${base}-${this.tag}${suffix}`
  }

  /** @param {string} phase @returns {string[]} the group ids of every base group in that phase */
  forPhase(phase) {
    return this.bases.map((base) => this.id(base, phase))
  }
}

/**
 * @param {number|null} phaseSeconds a smoke run's phase length, or none for the acceptance run
 * @returns {{ acceptance: boolean, warmupMs: number, phases: Array<{ name: string, workload: string, ms: number }> }}
 *   the four phases at their full length for acceptance; a shortened run is never acceptance
 */
const consumerChurnPlan = (phaseSeconds = null) => {
  const smoke = phaseSeconds !== null
  return {
    acceptance: !smoke,
    warmupMs: smoke ? 0 : CONSUMER_CHURN.warmupMs,
    phases: CONSUMER_CHURN.phases.map((phase) => ({
      ...phase,
      ms: smoke ? phaseSeconds * 1000 : CONSUMER_CHURN.phaseMs,
    })),
  }
}

/**
 * The child's one absolute deadline. Every stage asks it before it starts, so no stage is granted
 * a budget of its own and a plan that cannot fit is refused rather than shortened.
 */
class ChurnDeadline {
  /**
   * @param {{ now: () => number }} clock
   * @param {number} [budgetMs=CONSUMER_CHURN.childMs] the whole child's inclusive budget
   * @returns {ChurnDeadline} a deadline counted from now
   */
  constructor(clock, budgetMs = CONSUMER_CHURN.childMs) {
    this.clock = clock
    this.startedAt = clock.now()
    this.deadlineAt = this.startedAt + budgetMs
  }

  /** @returns {number} milliseconds left, never negative */
  remaining() {
    return Math.max(0, this.deadlineAt - this.clock.now())
  }

  /**
   * @param {number} needMs what the next stage needs, with the cleanup it leaves behind
   * @param {string} what the stage, for the message
   * @returns {void}
   * @throws {Error} when the stage cannot fit in what is left
   */
  require(needMs, what) {
    if (this.remaining() < needMs) {
      throw new Error(`Consumer churn budget: ${what} needs ${needMs} ms, ${this.remaining()} left`)
    }
  }
}

/**
 * Offers work on a deadline schedule: the n-th offer is due at `n / rate` after the start, so a
 * late tick is made up and its lateness recorded as slip. At most `maxInFlight` offers are
 * unsettled; a tick that finds the limit reached offers none and counts backpressure.
 */
class OfferScheduler {
  /**
   * @param {object} options
   * @param {number} options.rate offers per second
   * @param {number} [options.maxInFlight=CONSUMER_CHURN.maxInFlight] unsettled offers allowed
   * @param {{ now: () => number }} options.clock a monotonic clock
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} options.sleep
   */
  constructor({ rate, maxInFlight = CONSUMER_CHURN.maxInFlight, clock, sleep }) {
    Object.assign(this, { rate, maxInFlight, clock, sleep })
    this.inFlight = 0
    this.stats = {
      offered: 0,
      settled: 0,
      failed: 0,
      backpressure: 0,
      slip: { maxMs: 0, totalMs: 0, ticks: 0 },
    }
  }

  /**
   * Offers until `until`, then waits for what is in flight.
   * @param {number} until a time of the clock
   * @param {(index: number) => Promise<void>|null} offer starts one offer; null refuses it
   * @param {AbortSignal} [signal] ends the loop early
   * @returns {Promise<void>}
   */
  async run(until, offer, signal) {
    const start = this.clock.now()
    let slot = 0
    while (this.clock.now() < until && !signal?.aborted) {
      const now = this.clock.now()
      const due = Math.floor(((now - start) * this.rate) / 1000) + 1
      if (slot < due) {
        const slip = Math.max(0, now - (start + (slot * 1000) / this.rate))
        this.stats.slip.maxMs = Math.max(this.stats.slip.maxMs, slip)
        this.stats.slip.totalMs += slip
        this.stats.slip.ticks += 1
      }
      for (; slot < due; slot++) {
        if (this.inFlight >= this.maxInFlight) this.stats.backpressure += 1
        else this.#start(offer(this.stats.offered))
      }
      await this.sleep(Math.max(1, start + (slot * 1000) / this.rate - this.clock.now()), signal)
    }
    while (this.inFlight > 0) await this.sleep(1)
  }

  #start(pending) {
    if (pending === null) return
    this.stats.offered += 1
    this.inFlight += 1
    pending
      .then(
        () => {
          this.stats.settled += 1
        },
        () => {
          this.stats.failed += 1
        }
      )
      .finally(() => {
        this.inFlight -= 1
      })
  }
}

/**
 * Entry gaps of the handler, consecutive per group and phase, in preallocated rings. A negative
 * gap is a clock anomaly and fails the run; too few gaps are inconclusive, never a pass.
 */
class GapSampler {
  /**
   * @param {object} [options]
   * @param {number} [options.capacity=131072] gaps kept per group and phase
   * @param {number} [options.minimum=CONSUMER_CHURN.minGaps] gaps each group and phase must have
   */
  constructor({ capacity = 131072, minimum = CONSUMER_CHURN.minGaps } = {}) {
    this.capacity = capacity
    this.minimum = minimum
    this.series = new Map()
  }

  /**
   * Allocates the series of one group and phase, before the baseline reading.
   * @param {string} group
   * @param {string} phase
   * @returns {void}
   */
  prepare(group, phase) {
    this.series.set(`${group}|${phase}`, {
      gaps: new Float64Array(this.capacity),
      count: 0,
      last: null,
      anomalies: 0,
    })
  }

  /**
   * Notes one handler entry.
   * @param {string} group
   * @param {string} phase
   * @param {number} atMs a monotonic time
   * @returns {void}
   * @throws {Error} for a series that was not prepared
   */
  enter(group, phase, atMs) {
    const series = this.series.get(`${group}|${phase}`)
    if (!series) throw new Error(`No gap series for ${group} in ${phase}`)
    if (series.last !== null) {
      const gap = atMs - series.last
      if (gap < 0) series.anomalies += 1
      else series.gaps[series.count++ % this.capacity] = gap
    }
    series.last = atMs
  }

  /**
   * @param {number} phaseMs how long each measured phase lasted, for the rate
   * @returns {{ summary: object[], violations: string[] }} count, rate and quantiles of every
   *   series, and each that is short, anomalous or too slow
   */
  judge(phaseMs) {
    const summary = []
    const violations = []
    for (const [key, series] of this.series) {
      const kept = Math.min(series.count, this.capacity)
      const sorted = Float64Array.from(series.gaps.subarray(0, kept)).sort()
      const at = (fraction) =>
        kept === 0 ? null : sorted[Math.min(kept - 1, Math.floor(kept * fraction))]
      const rate = (series.count * 1000) / phaseMs
      summary.push({ key, count: series.count, rate, p50: at(0.5), p90: at(0.9), p99: at(0.99) })
      if (series.anomalies > 0) violations.push(`${key}: ${series.anomalies} clock anomalies`)
      if (series.count < this.minimum)
        violations.push(`${key}: ${series.count} gaps, fewer than ${this.minimum}`)
      if (rate < CONSUMER_CHURN.rate)
        violations.push(`${key}: ${rate.toFixed(1)} entries/s, below ${CONSUMER_CHURN.rate}`)
    }
    return { summary, violations }
  }
}

/**
 * The run's observations on disk, in issue order. Segments of at most `segment` records are
 * written as they fill, and replayed one segment at a time, so the whole run is never held.
 */
class ChurnJournal {
  /**
   * @param {object} options
   * @param {string} options.file the JSON-lines file, created or truncated
   * @param {number} [options.segment=CONSUMER_CHURN.segment] records per segment
   * @param {typeof fs} [options.files=fs]
   */
  constructor({ file, segment = CONSUMER_CHURN.segment, files = fs }) {
    this.file = file
    this.segment = segment
    this.files = files
    this.fd = files.openSync(file, 'w', 0o600)
    this.staged = []
    this.written = 0
    this.cursor = 0
  }

  /** @param {object} observation one record @returns {void} */
  append(observation) {
    this.staged.push(JSON.stringify(observation))
    if (this.staged.length >= this.segment) this.flush()
  }

  /** @returns {void} writes what is staged */
  flush() {
    if (this.staged.length === 0) return
    this.files.writeSync(this.fd, `${this.staged.join('\n')}\n`)
    this.written += this.staged.length
    this.staged = []
  }

  /**
   * Feeds `consume` the records written since the last replay, one segment at a time and in
   * order, reading the file in blocks so the journal is never held whole.
   * @param {(records: object[]) => Promise<void>} consume
   * @returns {Promise<number>} how many records this replay delivered
   */
  async replay(consume) {
    this.flush()
    const block = Buffer.alloc(1 << 20)
    let tail = ''
    let batch = []
    let count = 0
    const fd = this.files.openSync(this.file, 'r')
    try {
      for (;;) {
        const read = this.files.readSync(fd, block, 0, block.length, this.cursor)
        if (read === 0) break
        this.cursor += read
        const lines = (tail + block.toString('utf8', 0, read)).split('\n')
        tail = lines.pop()
        for (const line of lines) {
          batch.push(JSON.parse(line))
          count += 1
          if (batch.length >= this.segment) {
            await consume(batch)
            batch = []
          }
        }
      }
      if (batch.length > 0) await consume(batch)
    } finally {
      this.files.closeSync(fd)
    }
    return count
  }

  /** @returns {void} writes what is staged and closes once */
  close() {
    if (this.fd === null) return
    try {
      this.flush()
    } finally {
      const { fd } = this
      this.fd = null
      this.files.closeSync(fd)
    }
  }
}

/**
 * Splits a byte stream into length-prefixed frames and reports each one's correlation id.
 */
class FrameTally {
  /** @param {number} idOffset where the correlation id sits in a frame body */
  constructor(idOffset) {
    this.idOffset = idOffset
    this.pending = Buffer.alloc(0)
  }

  /**
   * @param {Buffer} chunk bytes in stream order, in any fragmentation
   * @returns {number[]} the correlation ids of the frames this chunk completed
   */
  push(chunk) {
    this.pending =
      this.pending.length === 0 ? Buffer.from(chunk) : Buffer.concat([this.pending, chunk])
    const ids = []
    while (this.pending.length >= 4) {
      const size = this.pending.readInt32BE(0)
      if (this.pending.length < 4 + size) break
      ids.push(this.pending.readInt32BE(4 + this.idOffset))
      this.pending = this.pending.subarray(4 + size)
    }
    return ids
  }
}

/**
 * What the profile can see of a client from outside: the sockets its `socketFactory` made, the
 * requests written on them and not yet answered (counted from the frames, with fragmentation and
 * coalescing handled), the listeners left on those sockets and the public instrumentation
 * registrations. A reading that was never observed is an error, not a zero.
 */
class PublicConsumerProbe {
  /**
   * @param {object} [options]
   * @param {() => Record<string, number>} [options.activeResources] counts by type, from the event loop
   * @param {() => Promise<number>} [options.heap] the heap after two collections
   */
  constructor({ activeResources, heap } = {}) {
    this.activeResources =
      activeResources ??
      // eslint-disable-next-line n/no-unsupported-features/node-builtins -- stable from Node 22.16 and 24.0, above this package's floor of 22.0, and this is a development helper
      (() => countByType(process.getActiveResourcesInfo()))
    this.heap = heap
    this.open = new Set()
    this.justClosed = []
    this.created = 0
    this.destroyed = 0
    this.abandoned = 0
    this.registrations = new Set()
    this.queueSize = null
  }

  /**
   * @param {Function} inner the socket factory the client would have used
   * @returns {Function} a factory that counts, and watches the frames of, every socket it makes
   */
  wrap(inner) {
    return (options) => {
      const socket = inner(options)
      const state = {
        socket,
        outstanding: new Set(),
        out: new FrameTally(4),
        in: new FrameTally(0),
      }
      this.created += 1
      this.open.add(state)
      const write = socket.write.bind(socket)
      socket.write = (data, ...rest) => {
        for (const id of state.out.push(Buffer.from(data))) state.outstanding.add(id)
        return write(data, ...rest)
      }
      const onData = (chunk) => {
        for (const id of state.in.push(chunk)) state.outstanding.delete(id)
      }
      socket.on('data', onData)
      socket.once('close', () => {
        socket.off('data', onData)
        this.#closed(state)
      })
      return socket
    }
  }

  #closed(state) {
    if (!this.open.delete(state)) return
    this.destroyed += 1
    this.abandoned += state.outstanding.size
    state.outstanding.clear()
    this.justClosed.push(state.socket)
    if (this.justClosed.length > 64) this.justClosed.shift()
  }

  /**
   * Registers the probe's public listeners on a client.
   * @param {{ on: Function, events: Record<string, string> }} client a consumer, producer or admin
   * @returns {() => void} removes them
   */
  track(client) {
    const removers = ['REQUEST_QUEUE_SIZE'].flatMap((name) =>
      client.events[name]
        ? [
            client.on(client.events[name], ({ payload }) => {
              this.queueSize = payload.queueSize
            }),
          ]
        : []
    )
    const registration = () => removers.forEach((remove) => remove())
    this.registrations.add(registration)
    return () => {
      registration()
      this.registrations.delete(registration)
    }
  }

  /** @returns {number} how many sockets are open now; a client's close completes a moment after its disconnect */
  openSockets() {
    return this.open.size
  }

  /**
   * @returns {Promise<object>} sockets created and destroyed, requests still outstanding and those
   *   a closed socket abandoned, listeners left on live and just-closed sockets, registrations,
   *   active resources by type and, when a heap reader was given, the heap
   * @throws {Error} when no socket was ever observed, so no zero here is unmeasured
   */
  async snapshot() {
    if (this.created === 0) throw new Error('The public probe observed no socket')
    const outstanding = [...this.open].reduce((total, { outstanding: ids }) => total + ids.size, 0)
    const listeners = [...this.open.values()].reduce(
      (total, { socket }) => total + listenerTotal(socket),
      0
    )
    return {
      sockets: { created: this.created, destroyed: this.destroyed, open: this.open.size },
      requests: { outstanding, abandoned: this.abandoned, queueSize: this.queueSize },
      listeners: {
        open: listeners,
        closed: this.justClosed.reduce((total, socket) => total + listenerTotal(socket), 0),
        registrations: this.registrations.size,
      },
      activeResources: this.activeResources(),
      ...(this.heap ? { heapUsed: await this.heap() } : {}),
    }
  }
}

/** @param {object} socket @returns {number} the listeners on it, over every event name */
const listenerTotal = (socket) =>
  socket.eventNames().reduce((total, name) => total + socket.listenerCount(name), 0)

/** @param {string[]} types @returns {Record<string, number>} how many of each */
const countByType = (types) => {
  const counts = {}
  for (const type of types) counts[type] = (counts[type] || 0) + 1
  return counts
}

/**
 * Compares a quiet reading with the baseline over every observed resource type, not a chosen few.
 * @param {string} when the quiet point, for messages
 * @param {object} snapshot a `PublicConsumerProbe` reading taken after every client closed
 * @param {object} baseline the matching reading from before the load
 * @returns {string[]} what differs: leftover listeners or registrations, outstanding or abandoned
 *   requests, unclosed sockets, and any resource type whose count is not the baseline's
 */
const quietViolations = (when, snapshot, baseline) => {
  const violations = []
  if (snapshot.sockets.created !== snapshot.sockets.destroyed + snapshot.sockets.open)
    violations.push(`${when}: socket accounting does not add up`)
  if (snapshot.sockets.open > 0)
    violations.push(`${when}: ${snapshot.sockets.open} sockets still open`)
  if (snapshot.requests.outstanding > 0)
    violations.push(`${when}: ${snapshot.requests.outstanding} requests still outstanding`)
  if (snapshot.requests.abandoned !== baseline.requests.abandoned)
    violations.push(
      `${when}: ${snapshot.requests.abandoned - baseline.requests.abandoned} requests abandoned by a closed socket`
    )
  for (const key of ['open', 'closed', 'registrations']) {
    if (snapshot.listeners[key] > baseline.listeners[key])
      violations.push(
        `${when}: ${snapshot.listeners[key] - baseline.listeners[key]} ${key} listeners left`
      )
  }
  for (const type of new Set([
    ...Object.keys(snapshot.activeResources),
    ...Object.keys(baseline.activeResources),
  ])) {
    const now = snapshot.activeResources[type] ?? 0
    const then = baseline.activeResources[type] ?? 0
    if (now !== then) violations.push(`${when}: ${type} ${now}, baseline ${then}`)
  }
  return violations
}

/**
 * Holds the matched quiet heap readings to the soak's two bounds. Each boundary contributes at
 * least `minimum` readings; the first and last three medians may differ by at most 2 MiB and the
 * least-squares slope over real time may not pass 256 KiB per minute.
 * @param {Array<{ boundary: string, atMs: number, heapUsed: number }>} readings in time order
 * @param {{ boundaries: string[], minimum?: number }} options
 * @returns {{ violations: string[], endpointBytes: number|null, slopeBytesPerMinute: number|null }}
 */
const judgeQuietHeap = (readings, { boundaries, minimum = CONSUMER_CHURN.quietSamples }) => {
  const violations = []
  for (const boundary of boundaries) {
    const count = readings.filter((reading) => reading.boundary === boundary).length
    if (count < minimum)
      violations.push(`${boundary}: ${count} quiet heap readings, fewer than ${minimum}`)
  }
  if (violations.length > 0 || readings.length < 2 * 3)
    return {
      violations: violations.length > 0 ? violations : ['too few matched heap readings'],
      endpointBytes: null,
      slopeBytesPerMinute: null,
    }
  const endpointBytes =
    median(readings.slice(-3).map(({ heapUsed }) => heapUsed)) -
    median(readings.slice(0, 3).map(({ heapUsed }) => heapUsed))
  const slopeBytesPerMinute = slope(readings.map(({ atMs, heapUsed }) => [atMs / 60000, heapUsed]))
  if (endpointBytes > ENDPOINT_BYTES)
    violations.push(`heap endpoint grew ${endpointBytes} bytes, more than ${ENDPOINT_BYTES}`)
  if (slopeBytesPerMinute > SLOPE_BYTES_PER_MINUTE)
    violations.push(
      `heap slope ${Math.round(slopeBytesPerMinute)} bytes per minute, more than ${SLOPE_BYTES_PER_MINUTE}`
    )
  return { violations, endpointBytes, slopeBytesPerMinute }
}

/**
 * The interning sensitivity control: retains distinct small objects with varying values, takes the
 * double-collected heap and requires its growth to pass `interningGrowthBytes`; a constant or
 * reused object would not, so a disabled or interning observer fails it. The objects are released
 * and recollected before the measured series.
 * @param {object} options
 * @param {() => Promise<number>} options.heap the double-collected heap
 * @param {number} [options.count=CONSUMER_CHURN.interningObjects]
 * @returns {Promise<{ growthBytes: number, ok: boolean }>}
 */
const interningControl = async ({ heap, count = CONSUMER_CHURN.interningObjects }) => {
  const before = await heap()
  const retained = Array.from({ length: count }, (_, index) => ({
    value: index * 2 + 1,
    tag: `n${index}`,
  }))
  const growthBytes = (await heap()) - before
  const ok = growthBytes > CONSUMER_CHURN.interningGrowthBytes
  retained.length = 0
  await heap()
  return { growthBytes, ok }
}

/**
 * Journals that violate exactly one invariant each, through the same oracle as the run: a delete
 * before the log is read or drained, a lost acknowledged record, a stale incarnation at an equal
 * offset, a commit without a broker read, a replay below the confirmed boundary, a handler after a
 * revoke and a delayed commit by an old owner.
 * @returns {Array<{ name: string, diagnostic: string, records: object[] }>}
 */
const consumerControls = () => {
  const base = {
    incarnation: 'incarnation-a',
    topic: 't',
    partition: 0,
    offset: '0',
    sequence: 's0',
    group: 'g',
    owner: 'owner-a',
  }
  const at = (kind, fields = {}) => ({ ...base, kind, ...fields })
  const valid = () => [
    at('produce'),
    at('acquire'),
    at('read'),
    at('handler'),
    at('commit', { next: '1', brokerNext: '1' }),
    at('revoke'),
    at('delete'),
  ]
  const without = (kinds) => valid().filter((record) => !kinds.includes(record.kind))
  const mutate = (index, fields) =>
    valid().map((record, i) => (i === index ? { ...record, ...fields } : record))
  const insert = (index, ...records) => {
    const journal = valid()
    journal.splice(index, 0, ...records)
    return journal
  }
  return [
    {
      name: 'delete before read',
      diagnostic: 'delete before log verification',
      records: insert(2, at('delete')),
    },
    {
      name: 'delete before drain',
      diagnostic: 'delete before handler drain',
      records: without(['handler', 'commit']),
    },
    {
      name: 'acknowledged log id removed',
      diagnostic: 'acknowledged log loss',
      records: without(['read', 'commit', 'delete']),
    },
    {
      name: 'healthy handler suppressed',
      diagnostic: 'acknowledged handler loss',
      records: without(['handler', 'commit', 'delete']),
    },
    {
      name: 'stale incarnation at an equal offset',
      diagnostic: 'commit after ownership boundary',
      records: mutate(4, { incarnation: 'incarnation-old' }),
    },
    {
      name: 'broker commit without a read',
      diagnostic: 'unconfirmed broker commit',
      records: valid().map((r) => (r.kind === 'commit' ? { ...r, brokerNext: undefined } : r)),
    },
    {
      name: 'replay below the confirmed boundary',
      diagnostic: 'replay boundary differs from confirmed commit',
      records: insert(5, at('replay', { reason: 'crash', from: '0', to: '1' })),
    },
    {
      name: 'handler after revoke',
      diagnostic: 'handler after ownership boundary',
      records: insert(6, at('handler')),
    },
    {
      name: 'delayed old hook commit',
      diagnostic: 'commit after ownership boundary',
      records: insert(6, at('commit', { next: '1', brokerNext: '1' })),
    },
  ]
}

/**
 * Runs every control through a fresh oracle.
 * @param {string[]} groups the intended groups
 * @returns {Promise<Array<{ name: string, ok: boolean, diagnostic: string, received: string|null }>>} a
 *   control passes only when the oracle rejects it with its own invariant, never for another reason
 */
const runControls = async (groups = ['g']) => {
  const results = []
  for (const { name, diagnostic, records } of consumerControls()) {
    let received = null
    try {
      await new ConsumerChurnOracle(records, groups).verify()
    } catch (error) {
      received = error.message
    }
    results.push({
      name,
      diagnostic,
      received,
      ok: received !== null && received.includes(diagnostic),
    })
  }
  return results
}

/**
 * The standard socket factory, rebuilt here so the profile imports nothing from the client's
 * sources: the same host, port, TLS options, server name and keep-alive the client would use.
 * @returns {Function} `({ host, port, ssl, onConnect }) => socket`
 */
const publicSocketFactory = () => {
  const net = require('node:net')
  const tls = require('node:tls')
  return ({ host, port, ssl, onConnect }) => {
    const socket = ssl
      ? tls.connect(
          Object.assign({ host, port }, !net.isIP(host) ? { servername: host } : {}, ssl),
          onConnect
        )
      : net.connect({ host, port }, onConnect)
    socket.setKeepAlive(true, 60000)
    return socket
  }
}

module.exports = {
  CONSUMER_CHURN,
  ChurnDeadline,
  ChurnGroups,
  ChurnJournal,
  ConsumerChurnOracle,
  FrameTally,
  GapSampler,
  OfferScheduler,
  PublicConsumerProbe,
  YIELD_EVERY,
  consumerChurnPlan,
  consumerControls,
  countByType,
  interningControl,
  judgeQuietHeap,
  publicSocketFactory,
  quietViolations,
  runControls,
}
