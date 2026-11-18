'use strict'
/**
 * Final soak driver: runs the reviewed matrix's twelve real client kinds, the isolated interning
 * seed and the observer processes for one exactly timed smoke or 24-hour acceptance run, and writes
 * the schema-3 artifact the independent verifier judges. It judges nothing itself.
 *
 * Usage (the runbook `finalSoakNode.sh <sha> <mode>` is the normal entry point):
 *
 *   KAFKA_BROKERS=localhost:19092,localhost:19095,localhost:19098 \
 *     node --expose-gc --no-flush-bytecode scripts/soak/finalSoak.js --mode smoke|acceptance \
 *       --expect-sha <40-hex> --matrix scripts/soak/finalSoakMatrix.json --out <artifact dir>
 *   node scripts/soak/finalSoak.js --finalize --out <artifact dir> --status <wait status>
 *
 * The first command is the collector/supervisor process. It spawns the fifteen other persistent
 * processes (`--child <slot>`), is the only stream writer, and exits naturally after every child
 * has. The outer observer (the runbook) witnesses that exit and runs `--finalize`, which appends
 * the parent-observed supervisor exit, hashes every file and writes `header.json`.
 * `--duration` is accepted only in smoke mode, where the matrix fixes its length.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn, execFile } = require('node:child_process')
const { AsyncLocalStorage } = require('node:async_hooks')
const { validateRecord, validateConfig, SHAPES } = require('./finalSoakSchema')

const ROOT = path.resolve(__dirname, '../..')
const MATRIX = 'scripts/soak/finalSoakMatrix.json'
const SUPERVISOR = 'p15'
const OUTER = 'outer'
const MIB = 1024 * 1024
const ENQUEUE_TIMEOUT_MS = 5000
const EXPLICIT_RESOURCES = ['TCPSocketWrap', 'Timeout']

/** @param {string|Buffer} data @returns {string} lowercase hex SHA-256 */
const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex')
/** @param {string} prefix @param {number} n @returns {string} a two-digit slot id */
const slotId = (prefix, n) => `${prefix}${String(n).padStart(2, '0')}`
/** @param {object} setting a matrix `{ value, unit, source }` setting @returns {number} */
const valueOf = (setting) => setting.value

/**
 * Parses the driver command line.
 * @param {string[]} argv arguments after the script name
 * @param {Record<string, string|undefined>} env the environment, read for `KAFKA_BROKERS`
 * @returns {{ mode: string, requestedMs: number, brokers: string[], expectSha: string,
 *   matrix: string, out: string, child: string|null, finalize: boolean, status: number|null }}
 * @throws {Error} for an unknown mode, `--duration` outside smoke mode, a smoke length other than
 *   the matrix's, a malformed SHA, or missing brokers or output directory
 */
function parseArgs(argv, env) {
  const flags = {}
  for (let i = 0; i < argv.length; i++) {
    const name = argv[i]
    if (!name.startsWith('--')) throw new Error(`unexpected argument ${name}`)
    if (name === '--finalize' || name === '--cleanup') flags[name.slice(2)] = true
    else flags[name.slice(2)] = argv[++i]
  }
  if (!flags.out) throw new Error('--out <artifact dir> is required')
  const out = path.resolve(flags.out)
  if (flags.finalize) {
    return { finalize: true, out, status: Number(flags.status), child: null }
  }
  if (flags.cleanup) return { cleanup: true, out, child: null, finalize: false }
  if (flags.child) return { child: flags.child, out, finalize: false }
  if (!['smoke', 'acceptance'].includes(flags.mode))
    throw new Error('--mode must be smoke or acceptance')
  const matrix = JSON.parse(fs.readFileSync(path.resolve(ROOT, flags.matrix ?? MATRIX), 'utf8'))
  const fixed = valueOf(matrix.acceptance.modes[flags.mode].requestedMs)
  if (flags.duration !== undefined) {
    if (flags.mode !== 'smoke') throw new Error('--duration is accepted only in smoke mode')
    if (Number(flags.duration) !== fixed)
      throw new Error(`the smoke length is fixed at ${fixed} ms`)
  }
  if (!/^[0-9a-f]{40}$/.test(flags['expect-sha'] ?? ''))
    throw new Error('--expect-sha must be a 40-hex commit')
  const brokers = (env.KAFKA_BROKERS ?? '').split(',').filter(Boolean)
  if (brokers.length === 0) throw new Error('KAFKA_BROKERS must list the brokers')
  return {
    mode: flags.mode,
    requestedMs: fixed,
    brokers,
    expectSha: flags['expect-sha'],
    matrix: flags.matrix ?? MATRIX,
    out,
    child: null,
    finalize: false,
    status: null,
  }
}

/**
 * Builds the run config: sixteen persistent processes (twelve subjects in matrix kind order, the
 * seed, reader, collector/supervisor and telemetry decoder), one standing subject per kind, one
 * churn extra per churn kind in its own kind's process, the seed's single control client and the
 * observer clients. Names follow the short profile ids so every ordinary record fits its line cap.
 * @param {object} matrix the reviewed matrix
 * @param {{ mode: string, runId: string, treeSha: string, brokers: string[] }} options
 * @returns {object} a config that `validateConfig` accepts
 */
function buildConfig(matrix, { mode, runId, treeSha, brokers }) {
  const kinds = Object.keys(matrix.clientKinds)
  const processes = kinds.map((kind, i) => ({
    slot: slotId('p', i + 1),
    role: 'subject',
    arm: 'unseeded',
    controlId: null,
    kind,
  }))
  const control = 'interning-seeded'
  processes.push(
    {
      slot: 'p13',
      role: 'seed',
      arm: control,
      controlId: control,
      kind: matrix.controls[control].clientKind,
    },
    { slot: 'p14', role: 'reader', arm: 'observer', controlId: null, kind: null },
    { slot: SUPERVISOR, role: 'collector', arm: 'observer', controlId: null, kind: null },
    { slot: 'p16', role: 'decoder', arm: 'observer', controlId: null, kind: null }
  )
  const clients = []
  const add = (process, clientKind, purpose, owner) => {
    const slot = slotId('c', clients.length + 1)
    const kind = matrix.clientKinds[clientKind]
    const proc = processes.find((p) => p.slot === process)
    const real = purpose !== 'observer'
    clients.push({
      slot,
      process,
      clientKind,
      purpose,
      arm: proc.arm,
      controlId: proc.controlId,
      group: real && kind.grouped ? `g-${runId}-${owner ?? slot}` : null,
      groupInstanceId: real && kind.static ? `s-${runId}-${slot}` : null,
      standing: purpose !== 'churn' && purpose !== 'observer' ? true : purpose === 'observer',
      telemetryEnabled: real && purpose !== 'control' && kind.telemetry,
    })
    return slot
  }
  const subjectSlot = new Map(
    kinds.map((kind, i) => [kind, add(processes[i].slot, kind, 'subject')])
  )
  kinds.forEach((kind, i) => {
    if (matrix.clientKinds[kind].churn) add(processes[i].slot, kind, 'churn', subjectSlot.get(kind))
  })
  add('p13', matrix.controls[control].clientKind, 'control')
  add('p14', 'admin', 'observer')
  add(SUPERVISOR, 'admin', 'observer')
  const topic = (role) => `fs-${runId}-${role}`
  return {
    schema: matrix.schema,
    mode,
    runId,
    treeSha,
    brokers,
    processes: processes.map(({ kind, ...p }) => p),
    clients,
    topics: {
      main: topic('main'),
      transactions: topic('txn'),
      regex: topic('regex'),
      recreate: [topic('recreate-0'), topic('recreate-1')],
    },
    profile: matrix.acceptance.profile,
    timeouts: {
      metadataMaxAge: valueOf(matrix.traffic.metadataMaxAgeMs),
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      rebalanceTimeout: 60000,
      requestTimeout: 30000,
    },
    companions: matrix.acceptance.companions,
  }
}

/**
 * Expands the matrix schedule into timed steps, in start order. Work steps (`lane: 'work'`) run one
 * at a time; timer steps (periodic and quiet samples, idle windows, seed and teardown marks) start
 * at their own time regardless of work. Each execution names the client kinds that must act in it.
 * @param {object} matrix the reviewed matrix
 * @param {'smoke'|'acceptance'} mode
 * @returns {{ mode: string, requestedMs: number, steps: object[] }}
 */
function planSchedule(matrix, mode) {
  const s = matrix.schedule
  const kinds = Object.keys(matrix.clientKinds)
  const where = (test) => kinds.filter((k) => test(matrix.clientKinds[k]))
  const recipients = (role) => matrix.topology.topics.find((t) => t.role === role).recipients
  const execution = (shape) => s.executions.find((e) => e.shape === shape)
  const membership = execution('membership-churn')
  const churn = where((k) => k.churn)
  const ordinary = where((k) => k.role === 'consumer' && k.churn)
  const statics = where((k) => k.static)
  const recreation = [...new Set([...recipients('recreate'), 'producer-idempotent', 'admin'])]
  const requestedMs = valueOf(matrix.acceptance.modes[mode].requestedMs)
  const period = valueOf(matrix.sampling.periodMs)
  const smoke = mode === 'smoke'
  const hours = smoke ? 1 : requestedMs / s.hourMs
  const steps = []
  const add = (step) => steps.push({ order: steps.length, ...step })
  const work = (id, shape, at, maxMs, participants, extra = {}) =>
    add({
      id,
      type: 'execution',
      lane: 'work',
      shape,
      at,
      deadline: at + maxMs,
      participants,
      ...extra,
    })

  add({
    id: 'traffic',
    type: 'execution',
    lane: 'work',
    shape: 'traffic',
    at: 0,
    deadline: requestedMs,
    participants: kinds,
  })
  for (let k = 0; k * period <= requestedMs; k++)
    add({
      id: `k${k}`,
      type: 'periodic',
      lane: 'timer',
      slot: k,
      at: k * period,
      deadline: k * period + valueOf(matrix.sampling.jitterMs),
    })
  let outage = 0
  for (let h = 0; h < hours; h++) {
    const H = h * s.hourMs
    const hh = String(h).padStart(2, '0')
    const at = (minutes) => minutes.map((m) => H + m * 60000)
    const times = smoke
      ? {
          membership: [s.smoke.membershipAtMs],
          regex: [s.smoke.regexAtMs],
          recreation: [s.smoke.recreationAtMs],
          outage: [s.smoke.outageAtMs],
        }
      : {
          membership: at(membership.minutes),
          regex: at(execution('regex-churn').minutes),
          recreation: at(execution('topic-recreation').minutes),
          outage: h % 2 === 0 ? at(execution('broker-outage').minutes) : [],
        }
    times.membership.forEach((t, i) => {
      const id = `h${hh}m${i}`
      work(id, 'membership-churn', t, membership.maxMs, churn, {
        waves: membership.waves,
        restart: i === 0 ? statics : [],
      })
      for (let w = 0; w < membership.waves; w++) {
        const waveAt = t + w * membership.waveMaxMs
        add({
          id: `${id}w${w}`,
          type: 'execution',
          lane: 'within',
          parent: id,
          shape: 'pause-resume',
          at: waveAt,
          deadline: waveAt + membership.waveMaxMs,
          participants: ordinary,
        })
      }
    })
    times.regex.forEach((t, i) =>
      work(`h${hh}r${i}`, 'regex-churn', t, execution('regex-churn').maxMs, recipients('regex'))
    )
    times.recreation.forEach((t, i) =>
      work(`h${hh}t${i}`, 'topic-recreation', t, execution('topic-recreation').maxMs, recreation, {
        topic: (h * times.recreation.length + i) % 2,
      })
    )
    times.outage.forEach((t) => {
      const recovered = smoke
        ? s.smoke.recoveredByMs
        : H + execution('broker-outage').recoveredByMinute * 60000
      work(`h${hh}o`, 'broker-outage', t, recovered - t, kinds, {
        node: (outage % 3) + 1,
        stop: outage % 2 === 0 ? 'clean' : 'hard',
        downMs: outage % 4 < 2 ? 2000 : 12000,
      })
      outage++
    })
    const idle = smoke ? s.smoke.idleMs : { from: H + s.idleMs.from, to: H + s.idleMs.to }
    const drainMs = s.auxiliaryDrainMs.to - s.auxiliaryDrainMs.from
    work(`h${hh}d`, 'drain', idle.from - drainMs, drainMs, churn)
    add({
      id: `h${hh}i`,
      type: 'idle',
      lane: 'timer',
      shape: 'idle',
      at: idle.from,
      until: idle.to,
      participants: kinds,
    })
    const quiet = smoke ? s.smoke.quietAtMs : s.quietAtMs.map((q) => H + q)
    const quietDeadline = quiet[0] + (s.quietDeadlineMs - s.quietAtMs[0])
    quiet.forEach((q, reading) =>
      add({
        id: `h${hh}q${reading}`,
        type: 'quiet',
        lane: 'timer',
        boundary: `h${hh}`,
        reading,
        at: q,
        deadline: quietDeadline,
      })
    )
    if (h === 0 && !smoke)
      add({ id: 'seed-start', type: 'seed-start', lane: 'work', at: quietDeadline })
  }
  if (smoke) add({ id: 'seed-start', type: 'seed-start', lane: 'work', at: 0 })
  add({ id: 'seed-clear', type: 'seed-clear', lane: 'work', at: requestedMs })
  add({ id: 'disconnect', type: 'disconnect', lane: 'work', at: requestedMs })
  const rank = { 'seed-start': -1 }
  steps.sort(
    (a, b) => a.at - b.at || (rank[a.type] ?? 0) - (rank[b.type] ?? 0) || a.order - b.order
  )
  return { mode, requestedMs, steps }
}

/**
 * Counts what a plan schedules, per shape and per kind, for comparison with the matrix totals.
 * @param {{ steps: object[] }} plan
 * @returns {object} idle windows, executions by shape, pauses per ordinary kind, restarts per static
 *   kind, periodic slots, quiet readings and the shapes and kinds that act
 */
function scheduleTotals({ steps }) {
  const executions = steps.filter((s) => s.type === 'execution' || s.type === 'idle')
  const count = (shape) => executions.filter((s) => s.shape === shape).length
  const perKind = (list) => {
    const out = {}
    for (const kind of list) out[kind] = (out[kind] ?? 0) + 1
    return out
  }
  return {
    idleWindows: count('idle'),
    regexExecutions: count('regex-churn'),
    recreations: count('topic-recreation'),
    membershipExecutions: count('membership-churn'),
    outages: count('broker-outage'),
    pauseSequences: perKind(
      executions.filter((s) => s.shape === 'pause-resume').flatMap((s) => s.participants)
    ),
    staticRestarts: perKind(executions.flatMap((s) => s.restart ?? [])),
    periodicSlots: steps.filter((s) => s.type === 'periodic').length,
    quietReadings: steps.filter((s) => s.type === 'quiet').length,
    shapes: SHAPES.filter((shape) => count(shape) > 0).sort(),
    kinds: [...new Set(executions.flatMap((s) => s.participants))].sort(),
  }
}

/**
 * Runs a plan against a clock. Work steps run one at a time in plan order; a work step that cannot
 * start before its deadline is not performed, and one that finishes after its deadline is late.
 * Either way it is reported `late` and never moved: later steps, and idle windows above all, keep
 * their times. Timer steps start at their time whatever work is running. Steps with `lane: 'within'`
 * are performed by their parent work step and are reported by it, not started here.
 */
class Scheduler {
  /**
   * @param {object} options
   * @param {{ now: () => number, sleepUntil: (at: number) => Promise<void> }} options.clock
   * @param {(step: object) => Promise<void>} options.perform does the step's work
   * @param {(outcome: object) => void} [options.onOutcome] receives each settled step
   */
  constructor({ clock, perform, onOutcome = () => {} }) {
    this.clock = clock
    this.perform = perform
    this.onOutcome = onOutcome
    this.summary = { ok: 0, late: 0, error: 0 }
  }

  /**
   * @param {object[]} steps the plan's steps, in start order
   * @returns {Promise<{ ok: number, late: number, error: number }>} counts by outcome
   */
  async run(steps) {
    let work = Promise.resolve()
    const all = []
    for (const step of steps) {
      if (step.lane === 'within') continue
      if (step.lane === 'work') {
        work = work.then(() => this.runStep(step))
        all.push(work)
      } else {
        all.push(this.runStep(step))
      }
    }
    await Promise.all(all)
    return this.summary
  }

  /** @param {object} step @returns {Promise<void>} */
  async runStep(step) {
    await this.clock.sleepUntil(step.at)
    const startedAt = this.clock.now()
    const deadline = step.deadline ?? Infinity
    let outcome = 'ok'
    let error = null
    if (startedAt >= deadline) outcome = 'late'
    else {
      try {
        await this.perform(step)
      } catch (e) {
        outcome = 'error'
        error = e.message
      }
      if (outcome === 'ok' && this.clock.now() > deadline) outcome = 'late'
    }
    this.summary[outcome]++
    this.onOutcome({
      id: step.id,
      type: step.type,
      outcome,
      at: step.at,
      startedAt,
      endedAt: this.clock.now(),
      error,
    })
  }
}

/**
 * The sole routine stream writer. Records arrive with their envelope from each source; the
 * collector checks per-source sequence continuity, stamps the append time `t` (never decreasing),
 * enforces the line and stream caps and streams lines to the sink through a byte-bounded queue.
 * When the queue is full, appends wait in arrival order, so no source is reordered; a wait over five
 * seconds stops collection. Nothing is truncated or silently dropped: a line over its cap is replaced
 * by a diagnostic naming it, and every failure is listed in `failures`. State is fixed in size: one
 * sequence counter per source and the bounded queue.
 */
class Collector {
  /**
   * @param {object} options
   * @param {{ write: (b: Buffer) => Promise<unknown>, sync: () => Promise<void> }} options.sink
   * @param {{ now: () => number, sleepUntil: (at: number) => Promise<void> }} options.clock
   * @param {object} options.matrix the reviewed matrix, for the caps
   * @param {number} [options.maxQueueBytes] write queue bound, 32 MiB by default
   * @param {number} [options.maxStreamBytes] stream cap, the matrix's by default
   * @param {string} [options.source] the collector's own source slot, for its own records
   */
  constructor({
    sink,
    clock,
    matrix,
    maxQueueBytes = 32 * MIB,
    maxStreamBytes,
    source = SUPERVISOR,
  }) {
    this.sink = sink
    this.clock = clock
    this.maxQueueBytes = maxQueueBytes
    this.maxStreamBytes = maxStreamBytes ?? valueOf(matrix.host.maxStreamBytes)
    this.maxLine = valueOf(matrix.host.maxEventLineBytes)
    this.maxInventoryLine = valueOf(matrix.host.maxInventoryLineBytes)
    this.source = source
    this.queue = []
    this.queuedBytes = 0
    this.streamBytes = 0
    this.records = 0
    this.lastT = 0
    this.lastLine = null
    this.nextSeq = new Map()
    this.ownSeq = 0
    this.failures = []
    this.stopped = false
    this.writing = null
    this.waiters = []
  }

  /**
   * Appends one source record.
   * @param {object} record a record with its envelope except `t`
   * @returns {Promise<boolean>} whether the record was written
   */
  append(record) {
    if (this.stopped) return Promise.resolve(false)
    const expected = this.nextSeq.get(record.source) ?? 0
    if (record.seq !== expected)
      this.fail(`${record.source} sequence ${record.seq} follows ${expected - 1}`)
    this.nextSeq.set(record.source, record.seq + 1)
    return this.enqueue(record)
  }

  /**
   * Appends a record of the collector's own source, giving it the next own sequence number.
   * @param {object} record fields without source and seq; `recordId` defaults to a fresh one
   * @returns {Promise<boolean>}
   */
  own(record) {
    const seq = this.ownSeq++
    return this.enqueue({
      recordId: `${this.source}.c${seq}`,
      source: this.source,
      seq,
      at: this.clock.now(),
      execution: null,
      ...record,
    })
  }

  /**
   * Appends a diagnostic from the collector itself.
   * @param {string} message @param {object} [extra] open diagnostic fields
   * @returns {Promise<boolean>}
   */
  note(message, extra = {}) {
    return this.own({ kind: 'diagnostic', message, ...extra })
  }

  /** @param {string} problem */
  fail(problem) {
    if (this.failures.length < 100) this.failures.push(problem)
  }

  /** @param {object} record @returns {string} the record's line with the current append time */
  line(record) {
    this.lastT = Math.max(this.lastT, this.clock.now())
    return JSON.stringify({ ...record, t: this.lastT }) + '\n'
  }

  /** @param {object} record @returns {Promise<boolean>} */
  async enqueue(record) {
    let line = this.line(record)
    const cap = record.kind === 'inventory' ? this.maxInventoryLine : this.maxLine
    if (Buffer.byteLength(line) - 1 > cap) {
      this.fail(`${record.recordId} ${record.kind} line exceeds ${cap} bytes`)
      if (record.source !== this.source)
        await this.note(`oversize ${record.kind} record ${record.recordId} not written`)
      return false
    }
    if (
      this.waiters.length > 0 ||
      this.queuedBytes + Buffer.byteLength(line) > this.maxQueueBytes
    ) {
      if (!(await this.turn(Buffer.byteLength(line)))) return false
      line = this.line(record)
    }
    const bytes = Buffer.byteLength(line)
    if (this.stopped) return false
    if (this.streamBytes + bytes > this.maxStreamBytes) {
      this.fail(`stream exceeds ${this.maxStreamBytes} bytes`)
      this.stop()
      return false
    }
    this.streamBytes += bytes
    this.records++
    this.lastLine = line.slice(0, -1)
    this.queue.push(line)
    this.queuedBytes += bytes
    this.writing ??= this.drain()
    this.release()
    return true
  }

  /** @param {number} bytes @returns {Promise<boolean>} waits in arrival order for queue space */
  turn(bytes) {
    return new Promise((resolve) => {
      const waiter = { bytes, resolve }
      this.waiters.push(waiter)
      this.clock.sleepUntil(this.clock.now() + ENQUEUE_TIMEOUT_MS).then(() => {
        if (!this.waiters.includes(waiter)) return
        this.fail(`write queue did not drain within ${ENQUEUE_TIMEOUT_MS} ms`)
        this.stop()
      })
    })
  }

  /** Lets the first waiter in, when its line fits. */
  release() {
    const [first] = this.waiters
    if (first && this.queuedBytes + first.bytes <= this.maxQueueBytes) {
      this.waiters.shift()
      first.resolve(true)
    }
  }

  /** Stops collection; waiting appends are refused. */
  stop() {
    this.stopped = true
    for (const waiter of this.waiters.splice(0)) waiter.resolve(false)
  }

  /** @returns {Promise<void>} writes queued lines until the queue is empty */
  async drain() {
    while (this.queue.length > 0) {
      const chunk = Buffer.from(this.queue.splice(0, this.queue.length).join(''))
      await this.sink.write(chunk)
      this.queuedBytes -= chunk.length
      this.release()
    }
    this.writing = null
  }

  /** @returns {Promise<void>} flushes and syncs the stream; later appends are refused */
  async close() {
    while (this.writing || this.waiters.length > 0)
      await (this.writing ?? new Promise(setImmediate))
    this.stopped = true
    await this.sink.sync()
  }
}

/**
 * A child process's end of the stream: assigns each record its source, gap-free `seq` and event
 * time `at` when it is handed over, and sends it to the collector. At most `maxBytes` may await the
 * collector's acknowledgement; beyond that records wait in call order, and a wait over five seconds
 * marks the source incomplete and stops it. Evidence is never silently dropped or reordered, and
 * memory never grows with the run.
 */
class SourceChannel {
  /**
   * @param {object} options
   * @param {string} options.source the process slot
   * @param {{ now: () => number, sleepUntil: (at: number) => Promise<void> }} options.clock
   * @param {(message: object, done: () => void) => void} options.send sends; `done` on acknowledgement
   * @param {number} [options.maxBytes] the pending bound, 8 MiB by default
   */
  constructor({ source, clock, send, maxBytes = 8 * MIB }) {
    this.source = source
    this.clock = clock
    this.send = send
    this.maxBytes = maxBytes
    this.seq = 0
    this.pendingBytes = 0
    this.incomplete = null
    this.waiters = []
  }

  /**
   * Hands a record over. With room and nobody waiting it is sent at once, synchronously, so a cut's
   * chunks take their place before any later record.
   * @param {object} record a record without source and seq; `at` defaults to now
   * @returns {Promise<boolean>} whether the record was sent
   */
  emit(record) {
    if (this.incomplete) return Promise.resolve(false)
    const waiter = {
      record,
      at: record.at ?? this.clock.now(),
      bytes: Buffer.byteLength(JSON.stringify(record)) + 96,
    }
    if (this.waiters.length === 0 && this.fits(waiter)) {
      this.dispatch(waiter)
      return Promise.resolve(true)
    }
    return new Promise((resolve) => {
      waiter.resolve = resolve
      this.waiters.push(waiter)
      this.clock.sleepUntil(this.clock.now() + ENQUEUE_TIMEOUT_MS).then(() => {
        if (!this.waiters.includes(waiter)) return
        this.incomplete = `${this.source} could not enqueue ${record.recordId} within ${ENQUEUE_TIMEOUT_MS} ms`
        for (const w of this.waiters.splice(0)) w.resolve(false)
      })
    })
  }

  /** @param {{ bytes: number }} waiter @returns {boolean} */
  fits(waiter) {
    return this.pendingBytes + waiter.bytes <= this.maxBytes
  }

  /** @param {{ record: object, at: number, bytes: number }} waiter sends it with the next seq */
  dispatch({ record, at, bytes }) {
    const message = {
      ...record,
      source: this.source,
      seq: this.seq++,
      at,
      execution: record.execution ?? null,
    }
    this.pendingBytes += bytes
    this.send({ type: 'record', record: message }, () => {
      this.pendingBytes -= bytes
      while (this.waiters.length > 0 && this.fits(this.waiters[0])) {
        const next = this.waiters.shift()
        this.dispatch(next)
        next.resolve(true)
      }
    })
  }
}

/**
 * Takes retained samples: a synchronous inventory cut first, then two new full collections at least
 * the GC gap apart with event-loop turns between, then the memory reading. Every reading carries its
 * GC witness; IDs and the cumulative forced count are never reused.
 */
class Sampler {
  /**
   * @param {object} options
   * @param {{ now: () => number, sleepUntil: (at: number) => Promise<void> }} options.clock
   * @param {() => void} [options.gc] a full collection, `globalThis.gc` by default
   * @param {() => Promise<void>} [options.turn] one event-loop turn for finalizers
   * @param {() => { rss: number, heapUsed: number, external: number }} [options.memory]
   * @param {() => string[]} [options.resources] active resource type names
   * @param {number} [options.gcGapMs] the gap between the two collections
   * @param {string} [options.process] the process slot, for GC ids
   */
  constructor({
    clock,
    gc = globalThis.gc,
    turn = () => new Promise((resolve) => setImmediate(resolve)),
    memory = () => process.memoryUsage(),
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- present on every supported Node; the only list of live resource types
    resources = () => process.getActiveResourcesInfo(),
    gcGapMs = 100,
    process: slot = 'p00',
  }) {
    if (typeof gc !== 'function') throw new Error('sampling needs --expose-gc')
    Object.assign(this, { clock, gc, turn, memory, resources, gcGapMs, slot })
    this.forced = 0
  }

  /**
   * @param {object} options
   * @param {string} options.role baseline, periodic, quiet, closed or final
   * @param {number|null} options.slot periodic slot
   * @param {string|null} options.boundary quiet boundary
   * @param {number|null} options.reading quiet reading
   * @param {() => string} options.cut takes the inventory cut synchronously, returns its id
   * @returns {Promise<object>} the sample fields without envelope or `process`/`clients`
   */
  async read({ role, slot = null, boundary = null, reading = null, cut }) {
    const admittedAt = this.clock.now()
    const inventory = cut()
    const collect = async () => {
      const begin = this.clock.now()
      this.gc()
      const end = this.clock.now()
      this.forced++
      await this.turn()
      return [begin, end]
    }
    const [firstBeginAt, firstEndAt] = await collect()
    await this.clock.sleepUntil(firstEndAt + this.gcGapMs)
    await this.turn()
    const [secondBeginAt, secondEndAt] = await collect()
    const { rss, heapUsed, external } = this.memory()
    const counts = new Map(EXPLICIT_RESOURCES.map((type) => [type, 0]))
    for (const type of this.resources()) counts.set(type, (counts.get(type) ?? 0) + 1)
    const extra = [...counts.keys()].filter((t) => !EXPLICIT_RESOURCES.includes(t)).sort()
    return {
      role,
      slot,
      boundary,
      reading,
      admittedAt,
      completedAt: this.clock.now(),
      rss,
      heapUsed,
      external,
      activeResources: [...EXPLICIT_RESOURCES, ...extra].map((type) => ({
        type,
        count: counts.get(type),
      })),
      gc: {
        id: `${this.slot}.g${this.forced / 2}`,
        firstBeginAt,
        firstEndAt,
        secondBeginAt,
        secondEndAt,
        forcedCollections: 2,
        cumulativeForced: this.forced,
      },
      inventory,
    }
  }
}

/**
 * Development-only passive weak observer. Before any client exists it wraps named methods of the
 * product's internal classes; each wrapper remembers its receiver once, through a `WeakRef`, notes
 * pause-state origins, and calls the original with the same receiver and arguments, returning its
 * exact result (the same promise for an async method) and letting its error through unchanged. It
 * adds no product getter, option, event or channel and changes no semantics. Dead references are
 * compacted on read, so nothing grows with history.
 */
class PassiveObserver {
  /**
   * @param {object} [options]
   * @param {Record<string, Function>} [options.owners] classes by name; the real ones by default
   * @param {Record<string, string[]>} [options.watched] methods to wrap per class
   * @param {(topic: string, receiver: object) => ({ topicId: string, life: string, client: string,
   *   run: string }|null)} [options.lifetime] the accepted lifetime a new pause state starts in
   * @param {AsyncLocalStorage} [options.context] attributes discovered receivers to a client
   * @param {(name: string, method: string, receiver: object) => void} [options.onCall] told after
   *   each watched call returns; its errors are counted in `errors`, never thrown into the product
   */
  constructor({
    owners = PassiveObserver.originals(),
    watched = PassiveObserver.WATCHED,
    lifetime = () => null,
    context = null,
    onCall = null,
  } = {}) {
    this.owners = owners
    this.watched = watched
    this.lifetime = lifetime
    this.context = context
    this.onCall = onCall
    this.errors = 0
    this.refs = {}
    this.seen = new WeakSet()
    this.ids = new WeakMap()
    this.owner = new WeakMap()
    this.origins = new WeakMap()
    this.operations = new WeakMap()
    this.nextId = 1
    this.restores = []
  }

  /** The internal methods the final driver watches, by class. */
  static WATCHED = Object.freeze({
    Connection: ['connect', 'disconnect'],
    RequestQueue: ['push', 'destroy'],
    SubscriptionState: ['pause', 'resume', 'retainTopics', 'retireTopic', 'clearPauses', 'assign'],
    GroupOwnership: ['token', 'retain', 'lose', 'renew'],
    OffsetOperations: ['begin', 'end'],
    ResetObligations: ['owe', 'discharge', 'retain', 'clear'],
    ConsumerGroup: ['connect', 'sync', 'join', 'retainSubscription'],
    ShareRun: ['start', 'syncNodes'],
    ShareAckLedger: ['prepare', 'invalidate', 'settle'],
    ShareFetchLoop: ['adopt', 'dispose'],
    ClientTelemetry: ['start', 'stop'],
  })

  /** @returns {Record<string, Function>} the real classes, loaded only when the driver needs them */
  static originals() {
    return {
      Connection: require('../../src/network/connection'),
      RequestQueue: require('../../src/network/requestQueue'),
      SubscriptionState: require('../../src/consumer/subscriptionState'),
      GroupOwnership: require('../../src/consumer/groupOwnership'),
      OffsetOperations: require('../../src/consumer/offsetManager/offsetOperations'),
      ResetObligations: require('../../src/consumer/offsetManager/resetObligations'),
      ConsumerGroup: require('../../src/consumer/consumerGroup'),
      ShareRun: require('../../src/consumer/share/shareRun').ShareRun,
      ShareAckLedger: require('../../src/consumer/share/shareAckLedger').ShareAckLedger,
      ShareFetchLoop: require('../../src/consumer/share/shareFetchLoop').ShareFetchLoop,
      ClientTelemetry: require('../../src/telemetry/client'),
    }
  }

  /**
   * Wraps every watched method that exists. A missing class or method is a missing source, reported
   * by `missing()`, never an empty reading.
   * @returns {void}
   */
  install() {
    this.absent = []
    for (const [name, methods] of Object.entries(this.watched)) {
      const owner = this.owners[name]
      for (const method of methods) {
        const prototype = typeof owner === 'function' ? owner.prototype : owner
        if (typeof prototype?.[method] !== 'function') {
          this.absent.push(`${name}#${method}`)
          continue
        }
        this.wrap(name, prototype, method)
      }
    }
  }

  /** @returns {string[]} watched sources the product does not have */
  missing() {
    return this.absent ?? []
  }

  /** @param {string} name @param {object} prototype @param {string} method */
  wrap(name, prototype, method) {
    const original = prototype[method]
    const observer = this
    const wrapper = function (...args) {
      observer.remember(name, this)
      const result = original.apply(this, args)
      try {
        if (name === 'SubscriptionState') observer.stamp(this, method)
        observer.onCall?.(name, method, this)
      } catch {
        observer.errors++
      }
      return result
    }
    prototype[method] = wrapper
    this.restores.push(() => {
      if (prototype[method] === wrapper) prototype[method] = original
    })
  }

  /** @returns {void} puts every original method back */
  restore() {
    for (const undo of this.restores.splice(0).reverse()) undo()
  }

  /** @param {string} name @param {object} receiver */
  remember(name, receiver) {
    if (this.seen.has(receiver)) return
    this.seen.add(receiver)
    this.ids.set(receiver, `${name[0].toLowerCase()}${this.nextId++}`)
    const client = this.context?.getStore()
    if (client) this.owner.set(receiver, client)
    ;(this.refs[name] ??= []).push(new WeakRef(receiver))
  }

  /** @param {object} receiver @returns {string} its weak id */
  idOf(receiver) {
    if (!this.ids.has(receiver)) this.ids.set(receiver, `o${this.nextId++}`)
    return this.ids.get(receiver)
  }

  /** @param {object} receiver @returns {string|null} the client that created it */
  clientOf(receiver) {
    return this.owner.get(receiver) ?? null
  }

  /**
   * @param {string} name a watched class
   * @returns {object[]} its live discovered receivers; dead references are dropped
   */
  live(name) {
    const refs = this.refs[name] ?? []
    const out = []
    this.refs[name] = refs.filter((ref) => {
      const value = ref.deref()
      if (value !== undefined) out.push(value)
      return value !== undefined
    })
    return out
  }

  /** Gives each pause-state object that has just appeared its fixed origin. */
  stamp(receiver, method) {
    const op = `${this.idOf(receiver)}.${method}${(this.operations.get(receiver)?.n ?? 0) + 1}`
    this.operations.set(receiver, { n: (this.operations.get(receiver)?.n ?? 0) + 1, op })
    const states = receiver.subscriptionStatesByTopic ?? {}
    for (const topic of Object.keys(states)) {
      const state = states[topic]
      const origin = this.origins.get(state)
      if (origin) {
        if (method === 'pause' || method === 'resume') origin.lastOperation = op
        continue
      }
      const life = this.lifetime(topic, receiver)
      this.origins.set(state, {
        stateId: this.idOf(state),
        topicId: life?.topicId ?? null,
        life: life?.life ?? null,
        client: life?.client ?? this.clientOf(receiver),
        run: life?.run ?? null,
        lastOperation: op,
      })
    }
  }

  /**
   * Every pause-state entry of every live subscription state, including empty, resumed-only and
   * pause-all ones, with the origin fixed when the state object first appeared.
   * @returns {object[]} pause-state section entries
   */
  pauseStates() {
    const out = []
    for (const receiver of this.live('SubscriptionState')) {
      const states = receiver.subscriptionStatesByTopic ?? {}
      for (const topic of Object.keys(states)) {
        const state = states[topic]
        const o = this.origins.get(state) ?? {}
        out.push({
          client: o.client ?? this.clientOf(receiver),
          run: o.run,
          stateId: o.stateId ?? this.idOf(state),
          topic,
          originTopicId: o.topicId,
          subscriptionLife: o.life,
          pauseAll: state.pauseAll === true,
          paused: [...state.paused].sort((a, b) => a - b),
          resumed: [...state.resumed].sort((a, b) => a - b),
          lastOperation: o.lastOperation,
        })
      }
    }
    return out
  }

  /**
   * The process-wide sections of one cut, read synchronously from the original discovered owners.
   * @param {(fn: Function) => string} listenerId weak id of a listener function
   * @param {{ read: () => Record<string, Record<string, number[]>> }} roles live async resources
   * @param {number} now the cut's time
   * @returns {Record<string, object[]>} queues, requests, sockets, listeners, resources and scopes
   */
  processSections(roles, now) {
    const queues = []
    const requests = []
    const sockets = []
    const listeners = []
    const scopes = new Map()
    const scopeOf = (client) => {
      const scope = client ?? 'harness'
      if (!scopes.has(scope))
        scopes.set(scope, {
          scope,
          owner: scope,
          type: client ? 'run' : 'harness',
          live: true,
          retiredAt: null,
        })
      return scope
    }
    const emitter = (target, client) => {
      const names = target.eventNames()
      const id = this.idOf(target)
      if (names.length === 0)
        listeners.push({
          emitter: id,
          client,
          scope: scopeOf(client),
          event: null,
          registrations: [],
        })
      for (const event of names) {
        listeners.push({
          emitter: id,
          client,
          scope: scopeOf(client),
          event: String(event),
          registrations: target.rawListeners(event).map((fn) => ({
            listener: this.idOf(fn),
            scope: scopeOf(this.clientOf(fn) ?? client),
          })),
        })
      }
    }
    for (const connection of this.live('Connection')) {
      const client = this.clientOf(connection)
      const queue = connection.requestQueue
      if (queue) this.remember('RequestQueue', queue)
      const socket = connection.socket
      if (socket) {
        sockets.push({
          socket: this.idOf(socket),
          client: client ?? 'harness',
          scope: scopeOf(client),
          node: `${connection.host}:${connection.port}`,
          connection: this.idOf(connection),
          destroyed: socket.destroyed === true,
        })
        emitter(socket, client)
      }
    }
    for (const queue of this.live('RequestQueue')) {
      const client = this.clientOf(queue)
      const id = this.idOf(queue)
      queues.push({
        queue: id,
        client: client ?? 'harness',
        connection: id,
        scope: scopeOf(client),
        pending: queue.pending.length,
        inflight: queue.inflight.size,
      })
      const request = (r, phase) =>
        requests.push({
          request: this.idOf(r),
          queue: id,
          apiKey: r.entry?.apiKey ?? 0,
          phase,
          scope: scopeOf(client),
          beganAt: Math.max(0, now),
          deadlineAt: null,
        })
      for (const r of queue.pending) request(r, 'pending')
      for (const r of queue.inflight.values()) request(r, 'inflight')
      emitter(queue, client)
    }
    const resources = []
    for (const [role, types] of Object.entries(roles.read())) {
      const client = role === 'harness' || role === 'unknown' ? null : role
      for (const [type, ids] of Object.entries(types))
        for (const id of ids)
          resources.push({ resource: `a${id}`, type, scope: scopeOf(client), client })
    }
    return { queues, requests, sockets, listeners, resources, scopes: [...scopes.values()] }
  }

  /**
   * The ordinary-consumer sections for one client, from its discovered owners.
   * @param {string} client actual client id
   * @param {(topic: string) => string|null} topicIdOf the client's observed topic id
   * @returns {Record<string, object[]>} ordinary sections
   */
  ordinarySections(client, topicIdOf) {
    const mine = (name) => this.live(name).filter((o) => this.clientOf(o) === client)
    const groups = mine('ConsumerGroup')
    const keyOf = (topic, partition) => ({ topic, topicId: topicIdOf(topic), partition })
    const membership = []
    const assignment = []
    const tables = []
    const subscription = []
    for (const group of groups) {
      const run = String(group.runIdentity ?? 'r0')
      for (const [token, state] of group.ownership?.states ?? []) {
        membership.push({
          client,
          run,
          token,
          current: token === group.ownership.current,
          holders: state.holders,
          refused: state.cause != null,
        })
      }
      for (const [key, acquired] of group.acquired ?? []) {
        const [topic, partition] = String(key).split('|')
        if (!topicIdOf(topic)) continue
        assignment.push({
          client,
          key: keyOf(topic, Number(partition)),
          authority: this.authority(client, group, acquired),
        })
      }
      const accepted = [...(group.topicsSubscribed ?? [])]
        .filter(topicIdOf)
        .map((topic) => ({ topic, topicId: topicIdOf(topic) }))
      subscription.push({
        client,
        run,
        revision: group.subscription?.revision ?? 0,
        accepted,
        desired: accepted,
        observed: accepted,
      })
      tables.push({
        client,
        table: 'acquired',
        keys: [...(group.acquired?.keys() ?? [])].map((k) => tableKey(k, topicIdOf)),
      })
      tables.push({
        client,
        table: 'halted',
        keys: [...(group.haltedPartitions?.keys() ?? [])].map((k) => tableKey(k, topicIdOf)),
      })
    }
    const cells = []
    for (const ledger of mine('OffsetOperations')) {
      for (const [topic, partitions] of ledger.topics ?? []) {
        for (const [partition, cell] of partitions) {
          const tickets = [...(cell.tickets ?? [])].map((t) => this.idOf(t))
          cells.push({
            ledger: this.idOf(ledger),
            topic,
            partition: Number(partition),
            open: tickets.length,
            tickets,
          })
        }
      }
    }
    const debts = []
    for (const owed of mine('ResetObligations')) {
      for (const [topic, entry] of owed.entries ?? []) {
        if (entry?.topicId)
          debts.push({
            client,
            topic,
            topicId: entry.topicId,
            discharged: [...(entry.discharged ?? [])].sort((a, b) => a - b),
          })
      }
    }
    return {
      subscription,
      'pause-state': this.pauseStates().filter(
        (e) => e.client === client && e.originTopicId && e.subscriptionLife && e.run
      ),
      assignment,
      'membership-state': membership,
      'operation-cell': cells,
      'reset-debt': debts,
      'other-table': tables,
    }
  }

  /** @returns {object} the captured product authority of an acquired key */
  authority(client, group, acquired) {
    return {
      client,
      run: String(group.runIdentity ?? 'r0'),
      member: String(group.memberId ?? '-'),
      epoch: Number(group.generationId ?? -1),
      token: Number(group.ownership?.current ?? 0),
      assignmentEpoch: Number(group.assignmentEpoch ?? 0),
      acquiredAt: Number(acquired?.acquiredAt ?? 0),
      recreatedAt: Number(acquired?.recreatedAt ?? 0),
    }
  }

  /**
   * The share-state section for one client: live sessions, held records and ranges and cohorts.
   * @param {string} client actual client id
   * @returns {object[]} one share-state entry, or none when the client has no live run
   */
  shareSections(client) {
    const runs = this.live('ShareRun').filter((r) => this.clientOf(r) === client)
    const sessions = runs.flatMap((r) =>
      [...(r.nodes?.entries() ?? [])].map(([node, s]) => ({
        node: Number(node),
        session: Number(s?.session?.epoch ?? s?.epoch ?? 0),
      }))
    )
    const cohorts = this.live('ShareFetchLoop')
      .filter((l) => this.clientOf(l) === client)
      .flatMap((l) => [...(l.cohorts ?? [])].map((c) => this.idOf(c)))
    return [{ client, sessions, records: [], ranges: [], cohorts }]
  }
}

/** @param {string} key `topic|partition` @param {Function} topicIdOf @returns {object} a table key */
const tableKey = (key, topicIdOf) => {
  const [topic, partition] = String(key).split('|')
  return {
    topic,
    topicId: topicIdOf(topic),
    partition: partition === undefined ? null : Number(partition),
  }
}

/** The host's monotonic clock, relative to an origin captured once per run. */
class MonotonicClock {
  /** @param {number} [originMs] the run origin in monotonic ms; now by default */
  constructor(originMs = MonotonicClock.raw()) {
    this.originMs = originMs
  }

  /** @returns {number} monotonic milliseconds since boot */
  static raw() {
    return Number(process.hrtime.bigint()) / 1e6
  }

  /** @returns {number} milliseconds since the origin */
  now() {
    return MonotonicClock.raw() - this.originMs
  }

  /** @param {number} at origin-relative ms @returns {Promise<void>} */
  sleepUntil(at) {
    const wait = at - this.now()
    return wait <= 0 ? Promise.resolve() : new Promise((resolve) => setTimeout(resolve, wait))
  }
}

/** @returns {string} this boot's id, read from the kernel */
const bootId = () => fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim()

/**
 * Builds an offered record of exactly the matrix value width. The id is `(runId, producer slot,
 * sequence)`; the payload hash covers the key, value and headers with length prefixes and a distinct
 * null encoding, as the reader and handlers recompute it from received bytes.
 * @param {{ runId: string, slot: string, seq: number, valueBytes: number }} options
 * @returns {{ id: string, key: Buffer, value: Buffer, headers: object, payloadSha256: string }}
 */
const offeredRecord = ({ runId, slot, seq, valueBytes }) => {
  const id = `${slot}.${seq}`
  const value = Buffer.alloc(valueBytes, 0x2e)
  value.write(`${runId}|${id}|`, 'ascii')
  const key = Buffer.from(id)
  return { id, key, value, headers: {}, payloadSha256: payloadSha256(key, value, {}) }
}

/**
 * @param {Buffer|null} key @param {Buffer|null} value @param {Record<string, Buffer>} headers
 * @returns {string} the payload hash over length-prefixed bytes; null is encoded as length -1
 */
const payloadSha256 = (key, value, headers) => {
  const hash = crypto.createHash('sha256')
  const part = (bytes) => {
    const length = Buffer.alloc(4)
    length.writeInt32BE(bytes == null ? -1 : bytes.length)
    hash.update(length)
    if (bytes != null) hash.update(bytes)
  }
  part(key)
  part(value)
  const names = Object.keys(headers ?? {})
  part(Buffer.from(String(names.length)))
  for (const name of names) {
    part(Buffer.from(name))
    part(headers[name] == null ? null : Buffer.from(headers[name]))
  }
  return hash.digest('hex')
}

/** @param {Buffer|null} value @returns {string|null} the offered id a value carries */
const idOf = (value) => (value ? (value.toString('ascii', 0, 64).split('|')[1] ?? null) : null)

/**
 * One actual client in a child process: its construct record, its public calls with begin and end
 * records, and the async context that attributes everything it creates to it.
 */
class ClientRig {
  /**
   * @param {object} options
   * @param {ChildRuntime} options.runtime the hosting process
   * @param {object} options.slot the config client slot
   * @param {number} options.incarnation 1 for the first actual object of the slot
   */
  constructor({ runtime, slot, incarnation }) {
    this.runtime = runtime
    this.slot = slot
    this.id = `${slot.slot}.${incarnation}`
    this.calls = 0
    this.object = null
  }

  /**
   * Constructs the actual client inside its own async context.
   * @param {(kafka: object) => object} factory makes the client from its own Kafka instance
   * @returns {Promise<object>} the actual client
   */
  async construct(factory) {
    const { Kafka, logLevel } = require('../../index')
    const { runtime, slot } = this
    const kafka = runtime.context.run(
      this.id,
      () =>
        new Kafka({
          clientId: this.id,
          brokers: runtime.config.brokers,
          logLevel: logLevel.NOTHING,
        })
    )
    this.object = runtime.context.run(this.id, () => factory(kafka))
    await runtime.emit({
      kind: 'client',
      recordId: runtime.nextId(),
      event: 'construct',
      client: this.id,
      slot: slot.slot,
      clientKind: slot.clientKind,
      process: runtime.slot,
      arm: slot.arm,
      controlId: slot.controlId,
      purpose: slot.purpose,
      group: slot.group,
      groupInstanceId: slot.groupInstanceId,
      standing: slot.standing,
      telemetryEnabled: slot.telemetryEnabled,
      configSha256: runtime.configSha256,
    })
    return this.object
  }

  /**
   * Makes one public call with its begin and end records.
   * @param {'connect'|'run'|'stop'|'disconnect'} event
   * @param {() => Promise<unknown>} work the public call
   * @param {string|null} [execution]
   * @returns {Promise<unknown>} the call's result
   * @throws {Error} the call's error, after its end record
   */
  async call(event, work, execution = null) {
    const call = `${this.id}.k${++this.calls}`
    const base = { kind: 'client', event, client: this.id, call, execution }
    await this.runtime.emit({
      ...base,
      recordId: this.runtime.nextId(),
      phase: 'begin',
      result: null,
      errorType: null,
    })
    try {
      const result = await this.runtime.context.run(this.id, work)
      await this.runtime.emit({
        ...base,
        recordId: this.runtime.nextId(),
        phase: 'end',
        result: 'ok',
        errorType: null,
      })
      return result
    } catch (e) {
      await this.runtime.emit({
        ...base,
        recordId: this.runtime.nextId(),
        phase: 'end',
        result: 'error',
        errorType: e.name ?? 'Error',
      })
      throw e
    }
  }
}

/**
 * One persistent child process: its stream source, sampler, passive observer and resource roles, and
 * the workload of its role. It exits naturally after the supervisor's `disconnect` command: every
 * client disconnects, the final sample is taken, the IPC channel closes and nothing is left to run.
 */
class ChildRuntime {
  /**
   * @param {object} options
   * @param {string} options.slot this process slot
   * @param {object} options.config the run config
   * @param {object} options.matrix the reviewed matrix
   * @param {number} options.originMs the run's clock origin
   * @param {string} options.mode smoke or acceptance
   * @param {string} options.out the artifact directory, for raw files
   * @param {(message: object, done: () => void) => void} options.send IPC send with acknowledgement
   */
  constructor({ slot, config, matrix, originMs, mode, out, send }) {
    const { ResourceRoles } = require('../../testHelpers/resourceProbe')
    this.slot = slot
    this.config = config
    this.matrix = matrix
    this.mode = mode
    this.out = out
    this.configSha256 = sha256(JSON.stringify(config))
    this.clock = new MonotonicClock(originMs)
    this.context = new AsyncLocalStorage()
    this.channel = new SourceChannel({ source: slot, clock: this.clock, send })
    this.sampler = new Sampler({ clock: this.clock, process: slot })
    this.roles = new ResourceRoles({
      types: ['Timeout', 'TCPWRAP', 'PIPEWRAP', 'MESSAGEPORT'],
    }).start()
    this.process = config.processes.find((p) => p.slot === slot)
    this.slots = config.clients.filter((c) => c.process === slot)
    this.topicIds = new Map()
    this.lives = new Map()
    this.revisions = new Map()
    this.ids = 0
    this.cuts = 0
    this.observer = new PassiveObserver({
      context: this.context,
      lifetime: (topic, receiver) => {
        const client = this.observer.clientOf(receiver)
        const topicId = this.topicIds.get(topic) ?? null
        return topicId && client
          ? { topicId, life: this.lives.get(`${client}/${topic}`) ?? null, client, run: 'r1' }
          : null
      },
      onCall: (name, method, receiver) => {
        if (name === 'ConsumerGroup' && method === 'retainSubscription') this.published(receiver)
      },
    })
    this.observer.install()
    this.worker = createWorker(this)
  }

  /**
   * Emits an accepted subscription publication and keeps each accepted `(client, topic, id)`
   * lifetime: it starts at its first publication and ends when the name leaves the subscription.
   * @param {object} group the publishing consumer group
   * @returns {void}
   */
  published(group) {
    const client = this.observer.clientOf(group)
    if (!client) return
    const names = [...(group.topicsSubscribed ?? [])]
      .map((topic) => ({ topic, topicId: this.topicIds.get(topic) }))
      .filter((n) => n.topicId)
    const recordId = this.nextId()
    for (const { topic, topicId } of names) {
      const key = `${client}/${topic}`
      if (!this.lives.get(key)?.endsWith(`/${topicId}`))
        this.lives.set(key, `${recordId}/${topicId}`)
    }
    for (const key of [...this.lives.keys()]) {
      if (key.startsWith(`${client}/`) && !names.some((n) => key === `${client}/${n.topic}`))
        this.lives.delete(key)
    }
    const revision = (this.revisions.get(client) ?? 0) + 1
    this.revisions.set(client, revision)
    const kind = this.config.clients.find((c) => client.startsWith(`${c.slot}.`))?.clientKind
    const regex = this.matrix.topology.topics
      .find((t) => t.role === 'regex')
      .recipients.includes(kind)
    const mode = kind === 'consumer-dynamic' ? 'broker-regex' : regex ? 'client-pattern' : 'literal'
    const pattern = mode === 'literal' ? null : `^fs-${this.config.runId}-`
    this.emit({
      kind: 'subscription',
      recordId,
      client,
      revision,
      run: 'r1',
      names,
      mode,
      pattern,
      cause: recordId,
    })
  }

  /** @returns {string} a fresh record id of this source */
  nextId() {
    return `${this.slot}.${this.ids++}`
  }

  /** @param {object} record @returns {Promise<boolean>} */
  emit(record) {
    return this.channel.emit(record)
  }

  /** @returns {Promise<void>} the start handshake */
  async start() {
    const args = process.execArgv.filter((a) => a.startsWith('--'))
    await this.emit({
      kind: 'process',
      recordId: this.nextId(),
      event: 'start',
      process: this.slot,
      pid: process.pid,
      parent: this.slot === SUPERVISOR ? OUTER : SUPERVISOR,
      arm: this.process.arm,
      controlId: this.process.controlId,
      execArgv: args,
      nodeOptions: (process.env.NODE_OPTIONS ?? '').split(/\s+/).filter(Boolean),
      node: process.version,
      v8: process.versions.v8,
      bootId: bootId(),
    })
    for (const missing of this.observer.missing())
      await this.emit({
        kind: 'diagnostic',
        recordId: this.nextId(),
        message: `missing-source ${missing}`,
      })
    await this.sample({ role: 'baseline' })
  }

  /**
   * Takes a sample with its inventory cut and emits both.
   * @param {object} options sample role, slot, boundary and reading
   * @returns {Promise<void>}
   */
  async sample(options) {
    const chunks = []
    const sent = []
    const cut = () => {
      const inventory = `${this.slot}.i${this.cuts++}`
      const at = this.clock.now()
      const sections = this.observer.processSections(this.roles, this.clock.now())
      for (const [section, entries] of Object.entries(sections))
        chunks.push([null, section, entries])
      for (const rig of this.worker.rigs()) {
        const kind = this.matrix.clientKinds[rig.slot.clientKind]
        if (kind.inventory === 'ordinary') {
          for (const [section, entries] of Object.entries(
            this.observer.ordinarySections(rig.id, (t) => this.topicIds.get(t) ?? null)
          ))
            chunks.push([rig.id, section, entries])
        } else if (kind.inventory === 'share')
          chunks.push([rig.id, 'share-state', this.observer.shareSections(rig.id)])
      }
      for (const [client, section, entries] of chunks) {
        const parts = chunk(entries, valueOf(this.matrix.host.maxInventoryLineBytes) - 1024)
        for (const [i, part] of parts.entries()) {
          sent.push(
            this.emit({
              kind: 'inventory',
              recordId: this.nextId(),
              at,
              inventory,
              process: this.slot,
              client,
              section,
              chunk: i,
              chunks: parts.length,
              entries: part,
            })
          )
        }
      }
      chunks.length = 0
      return inventory
    }
    const reading = await this.sampler.read({ ...options, cut })
    await Promise.all(sent)
    await this.emit({
      kind: 'sample',
      recordId: this.nextId(),
      process: this.slot,
      clients: this.worker.rigs().map((r) => r.id),
      ...reading,
    })
  }

  /**
   * Handles one supervisor command and replies with its outcome.
   * @param {{ id: number, step: object }} command
   * @returns {Promise<void>}
   */
  async handle({ id, step }) {
    let error = null
    try {
      if (step.type === 'periodic') {
        await this.sample({ role: 'periodic', slot: step.slot })
        await this.worker.onTick?.(step)
      } else if (step.type === 'quiet')
        await this.sample({ role: 'quiet', boundary: step.boundary, reading: step.reading })
      else if (step.type === 'closed') await this.sample({ role: 'closed', boundary: step.id })
      else if (step.type === 'topic-ids')
        for (const [topic, topicId] of step.topicIds) this.topicIds.set(topic, topicId)
      else if (step.type === 'disconnect') await this.stop()
      else await this.worker.perform(step)
    } catch (e) {
      error = e.message
      await this.emit({
        kind: 'diagnostic',
        recordId: this.nextId(),
        message: `step ${step.id} failed: ${e.message}`,
      })
    }
    process.send({ type: 'done', id, error, incomplete: this.channel.incomplete })
  }

  /** @returns {Promise<void>} disconnects every client, takes the final sample and lets the process end */
  async stop() {
    await this.worker.disconnect()
    await this.sample({ role: 'final' })
    this.roles.stop()
    this.observer.restore()
  }
}

/** @param {object[]} entries @param {number} budget @returns {object[][]} entries split by serialized size */
const chunk = (entries, budget) => {
  const parts = [[]]
  let size = 0
  for (const entry of entries) {
    const bytes = Buffer.byteLength(JSON.stringify(entry)) + 1
    if (size + bytes > budget && parts.at(-1).length > 0) {
      parts.push([])
      size = 0
    }
    parts.at(-1).push(entry)
    size += bytes
  }
  return parts
}

/**
 * The workload of one process role. Subjects and the seed act through public client calls only;
 * observer processes read the cluster without joining any judged group.
 */
class Worker {
  /** @param {ChildRuntime} runtime */
  constructor(runtime) {
    this.runtime = runtime
    this.standing = []
    this.extras = []
    this.incarnations = new Map()
    this.active = false
  }

  /** @returns {ClientRig[]} the live actual clients of this process */
  rigs() {
    return [...this.standing, ...this.extras]
  }

  /** @param {object} slot @returns {ClientRig} a new actual client of the slot */
  rig(slot) {
    const n = (this.incarnations.get(slot.slot) ?? 0) + 1
    this.incarnations.set(slot.slot, n)
    return new ClientRig({ runtime: this.runtime, slot, incarnation: n })
  }

  /** @param {object} step @returns {Promise<void>} */
  async perform(step) {
    if (step.type === 'start') return this.begin()
    if (step.type === 'idle') this.active = false
    if (step.type === 'active') this.active = true
    const handler = this[`on${step.shape?.replace(/(^|-)(\w)/g, (_, __, c) => c.toUpperCase())}`]
    if (handler) await handler.call(this, step)
  }

  /** @returns {Promise<void>} starts the standing clients */
  async begin() {}

  /** @returns {Promise<void>} disconnects every client */
  async disconnect() {
    for (const rig of this.rigs().reverse()) {
      try {
        await rig.call('disconnect', () => rig.object.disconnect())
      } catch {
        // the end record already carries the error
      }
    }
    this.standing = []
    this.extras = []
  }
}

/** A producing subject: idempotent, transactional or telemetry-enabled, driving the main stream. */
class ProducerWorker extends Worker {
  /** @returns {Promise<void>} */
  async begin() {
    const { runtime } = this
    const slot = runtime.slots.find((c) => c.purpose === 'subject')
    const rig = this.rig(slot)
    const transactional = slot.clientKind === 'producer-transactional'
    const producer = await rig.construct((kafka) =>
      kafka.producer({
        idempotent: true,
        maxInFlightRequests: 1,
        allowAutoTopicCreation: false,
        ...(transactional ? { transactionalId: `t-${runtime.config.runId}-${slot.slot}` } : {}),
        ...(slot.telemetryEnabled ? { enableMetricsPush: true } : {}),
      })
    )
    await rig.call('connect', () => producer.connect())
    this.standing.push(rig)
    this.seq = 0
    this.transactions = 0
    const traffic = runtime.matrix.traffic
    const rate =
      slot.clientKind === 'telemetry-producer'
        ? valueOf(traffic.telemetryProducerRate)
        : valueOf(traffic.mainRate)
    this.loop = transactional ? this.transactLoop(rig) : this.sendLoop(rig, rate)
  }

  /** @returns {Promise<void>} offers main records at the rate while active */
  async sendLoop(rig, rate) {
    const { clock, config } = this.runtime
    const every = 1000 / rate
    let next = clock.now()
    while (this.standing.includes(rig)) {
      next += every
      if (this.active) await this.offer(rig, config.topics.main, 0, null)
      await clock.sleepUntil(next)
    }
  }

  /** @returns {Promise<void>} one transaction per period while active, every tenth aborted */
  async transactLoop(rig) {
    const { clock, config, matrix } = this.runtime
    const traffic = matrix.traffic
    let next = clock.now()
    while (this.standing.includes(rig)) {
      next += valueOf(traffic.transactionEveryMs)
      if (this.active) {
        const n = ++this.transactions
        const transaction = `${rig.id}.x${n}`
        const decision = n % valueOf(traffic.abortEvery) === 0 ? 'abort' : 'commit'
        const txn = await rig.object.transaction()
        for (let i = 0; i < valueOf(traffic.transactionRecords); i++)
          await this.offer(rig, config.topics.transactions, 0, transaction, txn)
        const call = `${transaction}.${decision}`
        let outcome = decision === 'commit' ? 'committed' : 'aborted'
        let errorType = null
        try {
          await (decision === 'commit' ? txn.commit() : txn.abort())
        } catch (e) {
          outcome = 'unknown'
          errorType = e.name ?? 'Error'
        }
        await this.runtime.emit({
          kind: 'identity',
          recordId: this.runtime.nextId(),
          stage: 'transaction',
          client: rig.id,
          transaction,
          decision,
          outcome,
          call,
          errorType,
        })
      }
      await clock.sleepUntil(next)
    }
  }

  /**
   * Offers one logical id once: its offered record, the send, then its settled outcome.
   * @returns {Promise<void>}
   */
  async offer(rig, topic, partition, transaction, sender = rig.object, execution = null) {
    const { runtime } = this
    const record = offeredRecord({
      runId: runtime.config.runId,
      slot: rig.slot.slot,
      seq: this.seq++,
      valueBytes: valueOf(runtime.matrix.traffic.valueBytes),
    })
    const key = { topic, topicId: runtime.topicIds.get(topic) ?? 'unknown', partition }
    await runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      execution,
      stage: 'offered',
      client: rig.id,
      id: record.id,
      key,
      payloadSha256: record.payloadSha256,
      bytes: record.value.length,
      transaction,
    })
    let ack = { outcome: 'acknowledged', offset: null, errorType: null }
    try {
      const [meta] = await sender.send({
        topic,
        acks: -1,
        messages: [{ key: record.key, value: record.value, partition }],
      })
      ack.offset = meta?.baseOffset ?? null
      if (ack.offset === null) ack.outcome = 'unknown'
    } catch (e) {
      ack = { outcome: 'unknown', offset: null, errorType: e.name ?? 'Error' }
    }
    await runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      execution,
      stage: 'ack',
      client: rig.id,
      id: record.id,
      key,
      ...ack,
    })
  }

  /** Offers the probe ids an auxiliary execution needs. @param {object} step */
  async onRegexChurn(step) {
    if (step.probe)
      for (let i = 0; i < 10; i++)
        await this.offer(
          this.standing[0],
          step.probe.topic,
          i % step.probe.partitions,
          null,
          undefined,
          step.id
        )
  }

  /** @param {object} step */
  async onTopicRecreation(step) {
    return this.onRegexChurn(step)
  }
}

/**
 * An ordinary consumer subject (or the seed): explicit commits every hundred processed records and
 * at drains, handler entries with the authority captured from the original batch context, pause
 * waves with an extra member, static restarts, and the seed's retained offset map.
 */
class ConsumerWorker extends Worker {
  /** @returns {Promise<void>} */
  async begin() {
    const slot = this.runtime.slots.find((c) => c.purpose === 'subject' || c.purpose === 'control')
    this.seeded = slot.purpose === 'control'
    this.retained = null
    this.standing.push(await this.join(slot))
  }

  /** @param {object} slot @returns {Promise<ClientRig>} a connected, running member of the slot */
  async join(slot, execution = null) {
    const { runtime } = this
    const kind = runtime.matrix.clientKinds[slot.clientKind]
    const rig = this.rig(slot)
    const t = runtime.config.timeouts
    const consumer = await rig.construct((kafka) =>
      kafka.consumer({
        groupId: slot.group,
        ...(slot.groupInstanceId ? { groupInstanceId: slot.groupInstanceId } : {}),
        ...(kind.protocol === 'consumer' ? { groupProtocol: 'consumer' } : {}),
        ...(kind.protocol === 'cooperative'
          ? { partitionAssigners: [require('../../index').PartitionAssigners.cooperativeSticky] }
          : {}),
        ...(kind.protocol === 'eager'
          ? { partitionAssigners: [require('../../index').PartitionAssigners.roundRobin] }
          : {}),
        metadataMaxAge: t.metadataMaxAge,
        sessionTimeout: t.sessionTimeout,
        heartbeatInterval: t.heartbeatInterval,
        allowAutoTopicCreation: false,
        readUncommitted: false,
        ...(slot.telemetryEnabled ? { enableMetricsPush: true } : {}),
      })
    )
    rig.processed = new Map()
    rig.uncommitted = new Map()
    consumer.on(consumer.events.GROUP_JOIN, (e) => this.round(rig, e.payload).catch(() => {}))
    await rig.call('connect', () => consumer.connect(), execution)
    const topics = this.topicsFor(slot)
    await consumer.subscribe({ topics, fromBeginning: true })
    await rig.call(
      'run',
      () =>
        consumer.run({ autoCommit: false, eachMessage: (payload) => this.handle(rig, payload) }),
      execution
    )
    return rig
  }

  /** @param {object} slot @returns {(string|RegExp)[]} the client's subscription */
  topicsFor(slot) {
    const { topics, runId } = this.runtime.config
    if (this.seeded) return [topics.main]
    if (slot.clientKind === 'consumer-dynamic')
      return [new RegExp(`^fs-${runId}-(main|txn|regex-.*|recreate-[01])$`)]
    const regex = this.runtime.matrix.topology.topics
      .find((t) => t.role === 'regex')
      .recipients.includes(slot.clientKind)
    return [
      topics.main,
      topics.transactions,
      ...topics.recreate,
      ...(regex ? [new RegExp(`^fs-${runId}-regex-`)] : []),
    ]
  }

  /**
   * One handler call: the entry record before any await, with authority from the batch context,
   * then the settled outcome, then a commit when the key reaches its threshold.
   * @returns {Promise<void>}
   */
  async handle(rig, { topic, partition, message }) {
    const { runtime } = this
    const identity = require('../../src/consumer/handlerContext').current()
    const group = runtime.observer
      .live('ConsumerGroup')
      .find((g) => runtime.observer.clientOf(g) === rig.id)
    const acquired = group?.acquired?.get(`${topic}|${partition}`)
    const authority = group
      ? {
          ...runtime.observer.authority(rig.id, group, acquired),
          assignmentEpoch: Number(identity?.assignmentEpoch ?? group.assignmentEpoch ?? 0),
        }
      : null
    const handler = `${rig.id}.h${runtime.ids}`
    const key = { topic, topicId: runtime.topicIds.get(topic) ?? 'unknown', partition }
    const id = idOf(message.value) ?? `foreign.${message.offset}`
    runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      stage: 'handler-enter',
      client: rig.id,
      group: rig.slot.group,
      handler,
      id,
      key,
      offset: message.offset,
      payloadSha256: payloadSha256(message.key, message.value, message.headers),
      authority,
      acquisition: null,
      deliveryCount: null,
    })
    if (this.retained) this.retained.set(`${partition}:${message.offset}`, String(message.offset))
    await runtime.emit({
      kind: 'identity',
      recordId: runtime.nextId(),
      stage: 'handler-end',
      handler,
      outcome: 'processed',
      errorType: null,
    })
    const k = `${topic}|${partition}`
    rig.processed.set(k, (BigInt(message.offset) + 1n).toString())
    rig.uncommitted.set(k, (rig.uncommitted.get(k) ?? 0) + 1)
    if (rig.uncommitted.get(k) >= valueOf(runtime.matrix.traffic.commitEvery))
      await this.commit(rig, topic, partition, authority)
  }

  /** Commits a key's processed next offset, then asks the supervisor for the broker readback. */
  async commit(rig, topic, partition, authority) {
    const { runtime } = this
    const k = `${topic}|${partition}`
    const next = rig.processed.get(k)
    if (!next || !authority) return
    rig.uncommitted.set(k, 0)
    const commit = `${rig.id}.m${runtime.ids}`
    const key = { topic, topicId: runtime.topicIds.get(topic) ?? 'unknown', partition }
    const base = {
      kind: 'identity',
      stage: 'commit',
      client: rig.id,
      group: rig.slot.group,
      commit,
      key,
      next,
      authority,
    }
    await runtime.emit({
      ...base,
      recordId: runtime.nextId(),
      phase: 'begin',
      outcome: null,
      errorType: null,
    })
    let outcome = 'ok'
    let errorType = null
    try {
      await rig.object.commitOffsets([{ topic, partition, offset: next }])
    } catch (e) {
      outcome = e.name === 'KafkaJSStaleAssignmentError' ? 'refused' : 'unknown'
      errorType = e.name ?? 'Error'
    }
    await runtime.emit({ ...base, recordId: runtime.nextId(), phase: 'end', outcome, errorType })
    await runtime.request({ type: 'readback', client: rig.id, group: rig.slot.group, commit, key })
  }

  /** Records a successful round and its keyed acquisitions. */
  async round(rig, payload) {
    const { runtime } = this
    const kind = runtime.matrix.clientKinds[rig.slot.clientKind]
    const protocol = kind.protocol === 'share' ? 'consumer' : kind.protocol
    const assigned = Object.entries(payload.memberAssignment ?? {}).flatMap(([topic, partitions]) =>
      partitions.map((partition) => ({
        topic,
        topicId: runtime.topicIds.get(topic) ?? 'unknown',
        partition,
      }))
    )
    const previous = rig.assigned ?? []
    const has = (list, k) => list.some((x) => x.topic === k.topic && x.partition === k.partition)
    rig.assigned = assigned
    const roundId = `${rig.id}.r${runtime.ids}`
    await runtime.emit({
      kind: 'round',
      recordId: runtime.nextId(),
      client: rig.id,
      group: rig.slot.group,
      run: 'r1',
      round: roundId,
      protocol,
      epoch: Number(payload.generationId ?? -1),
      assigned,
      revoked: previous.filter((k) => !has(assigned, k)),
      causes: [],
    })
  }

  /**
   * One membership execution: per wave pause the current keys, join the extra, wait for a transfer,
   * cut the closed cohort after the extra leaves, resume. The first execution of an hour restarts the
   * static extra with the same static id and a new client id.
   * @param {object} step the membership execution with its waves
   */
  async onMembershipChurn(step) {
    const [standing] = this.standing
    const extraSlot = this.runtime.slots.find((c) => c.purpose === 'churn')
    if (!standing || !extraSlot) return
    for (let w = 0; w < step.waves; w++) {
      const execution = `${step.id}w${w}`
      const keys = (standing.assigned ?? []).filter(
        (k) => k.topic === this.runtime.config.topics.main
      )
      await this.operation(standing, 'pause', keys, execution, () =>
        standing.object.pause(byTopic(keys))
      )
      const extra = await this.join(extraSlot, execution)
      this.extras.push(extra)
      await this.runtime.clock.sleepUntil(this.runtime.clock.now() + 20000)
      if (w === 0 && step.restart?.includes(extraSlot.clientKind)) {
        await this.operation(extra, 'restart', [], execution, () =>
          extra.object.disconnect({ leaveGroup: false })
        )
        this.extras = this.extras.filter((r) => r !== extra)
        this.extras.push(await this.join(extraSlot, execution))
      }
      for (const rig of this.extras.splice(0))
        await rig.call('disconnect', () => rig.object.disconnect(), execution)
      await this.runtime.sample({ role: 'closed', boundary: execution })
      await this.operation(standing, 'resume', keys, execution, () =>
        standing.object.resume(byTopic(keys))
      )
    }
  }

  /** Commits every processed key at a drain. */
  async onDrain() {
    for (const rig of this.rigs())
      for (const k of rig.processed?.keys() ?? []) {
        const [topic, partition] = k.split('|')
        if (rig.uncommitted.get(k) > 0)
          await this.commit(
            rig,
            topic,
            Number(partition),
            this.lastAuthority(rig, topic, Number(partition))
          )
      }
  }

  /** @returns {object|null} the client's current authority over a key */
  lastAuthority(rig, topic, partition) {
    const group = this.runtime.observer
      .live('ConsumerGroup')
      .find((g) => this.runtime.observer.clientOf(g) === rig.id)
    return group
      ? this.runtime.observer.authority(rig.id, group, group.acquired?.get(`${topic}|${partition}`))
      : null
  }

  /** Starts or clears the seed's retained offset map. */
  async perform(step) {
    if (this.seeded && (step.type === 'seed-start' || step.type === 'seed-clear')) {
      const [rig] = this.standing
      const event = step.type === 'seed-start' ? 'start' : 'clear'
      const mapEntries = this.retained?.size ?? 0
      this.retained = event === 'start' ? new Map() : null
      return this.runtime.emit({
        kind: 'seed',
        recordId: this.runtime.nextId(),
        event,
        process: this.runtime.slot,
        client: rig.id,
        controlId: 'interning-seeded',
        mapEntries,
        cause: step.id,
      })
    }
    return super.perform(step)
  }

  /** Records a public pause, resume or restart call with its keys and settlement. */
  async operation(rig, op, keys, execution, work) {
    const { runtime } = this
    const operation = `${rig.id}.o${runtime.ids}`
    const base = {
      kind: 'operation',
      operation,
      op,
      client: rig.id,
      keys,
      call: operation,
      execution,
      causes: [],
    }
    await runtime.emit({
      ...base,
      recordId: runtime.nextId(),
      phase: 'begin',
      result: null,
      errorType: null,
    })
    try {
      await work()
      await runtime.emit({
        ...base,
        recordId: runtime.nextId(),
        phase: 'end',
        result: 'ok',
        errorType: null,
      })
    } catch (e) {
      await runtime.emit({
        ...base,
        recordId: runtime.nextId(),
        phase: 'end',
        result: 'error',
        errorType: e.name ?? 'Error',
      })
    }
  }
}

/** @param {object[]} keys @returns {{ topic: string, partitions: number[] }[]} */
const byTopic = (keys) => {
  const map = new Map()
  for (const k of keys) map.set(k.topic, [...(map.get(k.topic) ?? []), k.partition])
  return [...map].map(([topic, partitions]) => ({ topic, partitions }))
}

/** The share subject: a real share consumer that accepts every record it is handed. */
class ShareWorker extends Worker {
  /** @returns {Promise<void>} */
  async begin() {
    this.standing.push(await this.join(this.runtime.slots.find((c) => c.purpose === 'subject')))
  }

  /** @returns {Promise<ClientRig>} */
  async join(slot, execution = null) {
    const { runtime } = this
    const rig = this.rig(slot)
    const consumer = await rig.construct((kafka) =>
      kafka.shareConsumer({
        groupId: slot.group,
        metadataMaxAge: runtime.config.timeouts.metadataMaxAge,
      })
    )
    await rig.call('connect', () => consumer.connect(), execution)
    await consumer.subscribe({
      topics: [runtime.config.topics.main, ...runtime.config.topics.recreate],
    })
    await rig.call(
      'run',
      () =>
        consumer.run({
          eachMessage: async ({ topic, partition, message, acknowledge }) => {
            const handler = `${rig.id}.h${runtime.ids}`
            const key = { topic, topicId: runtime.topicIds.get(topic) ?? 'unknown', partition }
            runtime.emit({
              kind: 'identity',
              recordId: runtime.nextId(),
              stage: 'handler-enter',
              client: rig.id,
              group: slot.group,
              handler,
              id: idOf(message.value) ?? `foreign.${message.offset}`,
              key,
              offset: message.offset,
              payloadSha256: payloadSha256(message.key, message.value, message.headers),
              authority: null,
              acquisition: `${rig.id}.a${topic}.${partition}`,
              deliveryCount: message.deliveryCount,
            })
            acknowledge('accept')
            await runtime.emit({
              kind: 'identity',
              recordId: runtime.nextId(),
              stage: 'handler-end',
              handler,
              outcome: 'processed',
              errorType: null,
            })
          },
        }),
      execution
    )
    return rig
  }

  /** One extra share member joins and leaves per wave. */
  async onMembershipChurn(step) {
    const slot = this.runtime.slots.find((c) => c.purpose === 'churn')
    if (!slot) return
    for (let w = 0; w < step.waves; w++) {
      const extra = await this.join(slot, `${step.id}w${w}`)
      this.extras.push(extra)
      await this.runtime.clock.sleepUntil(this.runtime.clock.now() + 20000)
      for (const rig of this.extras.splice(0))
        await rig.call('disconnect', () => rig.object.disconnect(), `${step.id}w${w}`)
      await this.runtime.sample({ role: 'closed', boundary: `${step.id}w${w}` })
    }
  }
}

/** The admin subject: topic metadata, offset readback and recovery witnesses through public calls. */
class AdminWorker extends Worker {
  /** @returns {Promise<void>} */
  async begin() {
    const rig = this.rig(this.runtime.slots.find((c) => c.purpose === 'subject'))
    const admin = await rig.construct((kafka) => kafka.admin())
    await rig.call('connect', () => admin.connect())
    this.standing.push(rig)
  }

  /** Reads metadata of the run topics at each auxiliary execution and after recovery. */
  async metadata(step) {
    const [rig] = this.standing
    const { topics } = await rig.object.fetchTopicMetadata({
      topics: [this.runtime.config.topics.main],
    })
    for (const t of topics) {
      await this.runtime.emit({
        kind: 'topic',
        recordId: this.runtime.nextId(),
        execution: step.id,
        client: rig.id,
        call: `${rig.id}.q${this.runtime.ids}`,
        op: 'metadata',
        topic: t.name,
        topicId: t.topicId ?? null,
        partitions: t.partitions.map((p) => p.partitionId),
        errorCode: 0,
      })
    }
  }

  /** @param {object} step */
  async onBrokerOutage(step) {
    if (step.recovered) await this.metadata(step)
  }

  /** @param {object} step */
  async onTopicRecreation(step) {
    await this.metadata(step)
  }
}

/** The independent raw reader: completed read_committed scans of each sealed key, from raw fetches. */
class ReaderWorker extends Worker {
  /** @param {object} step a scan request with its topic */
  async perform(step) {
    if (step.type !== 'scan') return
    const { readLog } = require('../../testHelpers/rawLog')
    const { ISOLATION_LEVEL } = require('../../src/protocol/isolationLevel')
    const { runtime } = this
    const log = await runtime.context.run('reader', () =>
      readLog({
        brokers: runtime.config.brokers,
        topic: step.topic,
        isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      })
    )
    for (const [partition, read] of log) {
      const scan = `${runtime.slot}.s${runtime.ids}`
      const raw = JSON.stringify(
        read.records.map((r) => [
          r.offset,
          r.key?.toString('base64') ?? null,
          r.value?.toString('base64') ?? null,
        ])
      )
      const rawFile = `raw/${scan}.json`
      fs.writeFileSync(path.join(runtime.out, rawFile), raw)
      const key = {
        topic: step.topic,
        topicId: runtime.topicIds.get(step.topic) ?? 'unknown',
        partition: Number(partition),
      }
      for (const r of read.committedRecords ?? read.records) {
        await runtime.emit({
          kind: 'identity',
          recordId: runtime.nextId(),
          stage: 'log',
          reader: runtime.slot,
          scan,
          id: idOf(r.value) ?? `foreign.${r.offset}`,
          key,
          offset: r.offset,
          payloadSha256: payloadSha256(r.key, r.value, r.headers ?? {}),
          transaction: null,
        })
      }
      await runtime.emit({
        kind: 'scan',
        recordId: runtime.nextId(),
        scan,
        key,
        from: '0',
        to: String(read.endOffset),
        lso: String(read.endOffset),
        highWatermark: String(read.endOffset),
        rawFile,
        rawSha256: sha256(raw),
        records: read.records.length,
      })
    }
  }
}

/**
 * The telemetry decoder: tails each broker reporter's decoded sink, archives the raw lines once in
 * segments and emits one telemetry record per push of a run client.
 */
class DecoderWorker extends Worker {
  /** @param {object} step */
  async onTick() {
    const dir = process.env.TELEMETRY_EVIDENCE_DIR
    if (!dir) return
    this.offsets ??= new Map()
    for (const file of fs.readdirSync(dir).filter((f) => /^exports-\d+\.jsonl$/.test(f))) {
      const from = this.offsets.get(file) ?? 0
      const bytes = fs.readFileSync(path.join(dir, file)).subarray(from)
      const end = bytes.lastIndexOf(0x0a) + 1
      if (end === 0) continue
      this.offsets.set(file, from + end)
      const raw = bytes.subarray(0, end)
      const rawFile = `telemetry/${file}.${from}`
      fs.mkdirSync(path.join(this.runtime.out, 'telemetry'), { recursive: true })
      fs.writeFileSync(path.join(this.runtime.out, rawFile), raw)
      for (const line of raw.toString('utf8').split('\n').filter(Boolean)) {
        const push = JSON.parse(line)
        const client = this.runtime.config.clients.find((c) =>
          push.clientId?.startsWith(`${c.slot}.`)
        )
        if (!client) continue
        const decoded = JSON.stringify({
          clientId: push.clientId,
          metricsTotal: push.metricsTotal,
          terminating: push.terminating,
        })
        const decodedFile = `${rawFile}.${push.seq}.decoded.json`
        fs.writeFileSync(path.join(this.runtime.out, decodedFile), decoded)
        await this.runtime.emit({
          kind: 'telemetry',
          recordId: this.runtime.nextId(),
          client: push.clientId,
          instanceId: String(push.clientInstanceId),
          subscriptionId: String(push.subscriptionId ?? 'unrecorded'),
          broker: Number(file.match(/\d+/)[0]),
          metrics: Number(push.metricsTotal ?? 0),
          terminating: push.terminating === true,
          rawFile,
          rawSha256: sha256(raw),
          decodedFile,
          decodedSha256: sha256(decoded),
        })
      }
    }
  }
}

/** @param {ChildRuntime} runtime @returns {Worker} the worker of the process's role and kind */
function createWorker(runtime) {
  const { role } = runtime.process
  if (role === 'reader') return new ReaderWorker(runtime)
  if (role === 'decoder') return new DecoderWorker(runtime)
  if (role === 'seed') return new ConsumerWorker(runtime)
  if (role === 'collector') return new Worker(runtime)
  const kind = runtime.slots.find((c) => c.purpose === 'subject')?.clientKind
  const def = runtime.matrix.clientKinds[kind]
  if (def.role === 'producer') return new ProducerWorker(runtime)
  if (def.role === 'share') return new ShareWorker(runtime)
  if (def.role === 'admin') return new AdminWorker(runtime)
  return new ConsumerWorker(runtime)
}

/**
 * The collector/supervisor process: creates the run topics, spawns the fifteen children, holds the
 * readiness barrier, runs the plan, owns topic and broker churn, relays readbacks, samples itself
 * like every persistent process, records every child exit it observes and leaves after them. It
 * never kills a child, unrefs a handle or calls `process.exit`.
 */
class Supervisor {
  /** @param {ReturnType<typeof parseArgs>} options */
  constructor(options) {
    this.options = options
    this.out = options.out
    this.matrix = JSON.parse(fs.readFileSync(path.resolve(ROOT, options.matrix), 'utf8'))
    this.runId = crypto.randomBytes(6).toString('hex')
    this.config = buildConfig(this.matrix, {
      mode: options.mode,
      runId: this.runId,
      treeSha: options.expectSha,
      brokers: options.brokers,
    })
    const problems = validateConfig(this.config, this.matrix)
    if (problems.length > 0) throw new Error(`invalid config: ${problems.join('; ')}`)
    this.clock = new MonotonicClock(originOf(this.out))
    this.children = new Map()
    this.waiting = new Map()
    this.commands = 0
    this.exits = 0
    this.topicIds = []
  }

  /** @param {object} record @returns {Promise<boolean>} appends a supervisor record */
  emit(record) {
    return this.runtime.emit({ recordId: this.runtime.nextId(), ...record })
  }

  /** @returns {Promise<void>} runs the whole lifecycle */
  async run() {
    fs.mkdirSync(path.join(this.out, 'raw'), { recursive: true })
    for (const file of this.matrix.acceptance.archive) {
      fs.mkdirSync(path.dirname(path.join(this.out, 'archive', file)), { recursive: true })
      fs.copyFileSync(path.join(ROOT, file), path.join(this.out, 'archive', file))
    }
    fs.writeFileSync(
      path.join(this.out, 'config.json'),
      JSON.stringify(this.config, null, 2) + '\n'
    )
    const handle = await fs.promises.open(path.join(this.out, 'stream.ndjson'), 'wx')
    const sink = { write: (b) => handle.write(b), sync: () => handle.sync() }
    this.collector = new Collector({ sink, clock: this.clock, matrix: this.matrix })
    this.runtime = new ChildRuntime({
      slot: SUPERVISOR,
      config: this.config,
      matrix: this.matrix,
      originMs: this.clock.originMs,
      mode: this.options.mode,
      out: this.out,
      send: (m, done) => this.collector.append(m.record).then(done),
    })
    this.collector.own = (record) =>
      this.runtime.emit({ recordId: this.runtime.nextId(), ...record })
    await this.runtime.start()
    const rig = this.runtime.worker.rig(this.config.clients.find((c) => c.process === SUPERVISOR))
    this.admin = await rig.construct((kafka) => kafka.admin())
    await rig.call('connect', () => this.admin.connect())
    this.runtime.worker.standing.push(rig)
    this.rig = rig
    await this.createTopics()
    for (const p of this.config.processes) if (p.slot !== SUPERVISOR) this.spawn(p.slot)
    await Promise.all(
      [...this.children.keys()].map((slot) => this.command(slot, { type: 'start', id: 'start' }))
    )
    this.runStartAt = this.clock.now()
    await this.collector.note('readiness barrier', { runStartAt: this.runStartAt })
    const plan = planSchedule(this.matrix, this.options.mode)
    const shift = (t) => (t === undefined ? t : t + this.runStartAt)
    const steps = plan.steps.map((st) => ({
      ...st,
      at: shift(st.at),
      deadline: shift(st.deadline),
      until: shift(st.until),
    }))
    const scheduler = new Scheduler({
      clock: this.clock,
      perform: (step) => this.perform(step),
      onOutcome: (o) => this.collector.note(`step ${o.id} ${o.outcome}`, { step: o.id }),
    })
    const summary = await scheduler.run(steps)
    this.runEndAt = this.runStartAt + plan.requestedMs
    await this.collector.note('schedule finished', summary)
    await this.exited
    await this.runtime.stop()
    while (this.runtime.channel.pendingBytes > 0)
      await new Promise((resolve) => setImmediate(resolve))
    await this.collector.close()
    await handle.close()
    const run = {
      runId: this.runId,
      originMs: this.clock.originMs,
      bootId: bootId(),
      runStartAt: this.runStartAt,
      runEndAt: this.runEndAt,
      requestedMs: plan.requestedMs,
      observedEndAt: this.observedEndAt,
      failures: this.collector.failures,
      incomplete: this.incomplete ?? null,
      records: this.collector.records,
      mode: this.options.mode,
      treeSha: this.options.expectSha,
    }
    fs.writeFileSync(path.join(this.out, 'supervisor.json'), JSON.stringify(run) + '\n')
  }

  /** @param {string} topic @param {number} partitions @returns {Promise<string>} creates one run topic */
  async createTopic(topic, partitions, execution = null) {
    const configEntries = [
      { name: 'min.insync.replicas', value: '2' },
      { name: 'retention.ms', value: '-1' },
      { name: 'retention.bytes', value: '-1' },
    ]
    let errorCode = 0
    try {
      await this.admin.createTopics({
        topics: [{ topic, numPartitions: partitions, replicationFactor: 3, configEntries }],
        waitForLeaders: true,
      })
    } catch {
      errorCode = -1
    }
    const [meta] = (await this.admin.fetchTopicMetadata({ topics: [topic] })).topics
    await this.topicRecord(
      'create',
      topic,
      meta?.topicId ?? null,
      meta?.partitions ?? [],
      errorCode,
      execution
    )
    this.publish(topic, meta.topicId)
    return meta.topicId
  }

  /** @param {string} topic @returns {Promise<void>} deletes a run topic and confirms its absence */
  async deleteTopic(topic, execution = null) {
    const topicId = this.topicIds.find(([t]) => t === topic)?.[1] ?? null
    let errorCode = 0
    try {
      await this.admin.deleteTopics({ topics: [topic] })
    } catch {
      errorCode = -1
    }
    await this.topicRecord('delete', topic, topicId, [], errorCode, execution)
  }

  /** Emits one admin topic result record. */
  topicRecord(op, topic, topicId, partitions, errorCode, execution) {
    return this.emit({
      kind: 'topic',
      execution,
      client: this.rig.id,
      call: `${this.rig.id}.t${this.runtime.ids}`,
      op,
      topic,
      topicId,
      partitions: partitions.map((p) => p.partitionId),
      errorCode,
    })
  }

  /** Tells every process a topic's current id. */
  publish(topic, topicId) {
    this.topicIds = [...this.topicIds.filter(([t]) => t !== topic), [topic, topicId]]
    this.runtime.topicIds.set(topic, topicId)
    for (const slot of this.children.keys())
      this.command(slot, { type: 'topic-ids', id: 'topic-ids', topicIds: [[topic, topicId]] })
  }

  /** Creates the standing run topics with RF3 and min ISR 2. */
  async createTopics() {
    const t = this.config.topics
    await this.createTopic(t.main, 1)
    await this.createTopic(t.transactions, 1)
    for (const topic of t.recreate) await this.createTopic(topic, 2)
  }

  /** @param {string} slot spawns one child with the sampling flags and an IPC channel */
  spawn(slot) {
    const args = [
      '--expose-gc',
      '--no-flush-bytecode',
      __filename,
      '--child',
      slot,
      '--out',
      this.out,
    ]
    const child = spawn(process.execPath, args, {
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
      env: process.env,
    })
    this.children.set(slot, child)
    this.exited ??= new Promise((resolve) => (this.allExited = resolve))
    child.on('message', (m) => this.receive(slot, m))
    child.send({
      type: 'init',
      config: this.config,
      matrix: this.matrix,
      originMs: this.clock.originMs,
      mode: this.options.mode,
      topicIds: this.topicIds,
    })
    child.on('exit', (code, signal) => {
      this.observedEndAt = this.clock.now()
      const exit = {
        kind: 'process',
        event: 'exit',
        process: slot,
        code,
        signal,
        spawnRecord: `${slot}.0`,
      }
      this.emit(exit).finally(() => {
        if (++this.exits === this.children.size) this.allExited()
      })
    })
  }

  /** Routes a child message: records to the collector, command outcomes, readback requests. */
  receive(slot, message) {
    const child = this.children.get(slot)
    if (message.type === 'record') {
      const { seq } = message.record
      this.collector
        .append(message.record)
        .then(() => child.connected && child.send({ type: 'ack', seq }))
    } else if (message.type === 'done') {
      if (message.incomplete) this.incomplete ??= message.incomplete
      const resolve = this.waiting.get(message.id)
      this.waiting.delete(message.id)
      resolve?.(message)
    } else if (message.type === 'readback') this.readback(slot, message)
  }

  /** Reads a group's committed offset back for a child's commit, records it and answers. */
  async readback(slot, { request, group, commit, client, key }) {
    let next = null
    let errorCode = 0
    try {
      const [topic] = await this.admin.fetchOffsets({ groupId: group, topics: [key.topic] })
      const partition = topic?.partitions.find((p) => p.partition === key.partition)
      next = partition && partition.offset !== '-1' ? partition.offset : null
    } catch {
      errorCode = -1
    }
    await this.emit({
      kind: 'identity',
      stage: 'broker-commit',
      client,
      group,
      commit,
      key,
      next,
      errorCode,
    })
    this.children.get(slot)?.send({ type: 'reply', request })
  }

  /** @returns {Promise<object>} sends a step to a child and waits for its outcome */
  command(slot, step) {
    const id = ++this.commands
    return new Promise((resolve) => {
      this.waiting.set(id, resolve)
      this.children.get(slot).send({ type: 'command', id, step })
    })
  }

  /** @param {string[]} slots @param {object} step @returns {Promise<void>} throws if any child failed */
  async send(slots, step) {
    const replies = await Promise.all(slots.map((slot) => this.command(slot, step)))
    const failed = replies.filter((r) => r.error)
    if (failed.length > 0) throw new Error(failed.map((r) => r.error).join('; '))
  }

  /** @param {string} kind @returns {string} the process of the kind's standing subject */
  processOf(kind) {
    return this.config.clients.find((c) => c.clientKind === kind && c.purpose === 'subject').process
  }

  /** Performs one plan step across the processes it involves. */
  async perform(step) {
    const all = [...this.children.keys()]
    const judged = this.config.processes
      .filter((p) => p.role === 'subject' || p.role === 'seed')
      .map((p) => p.slot)
    if (step.type === 'periodic') {
      await Promise.all([
        this.send(all, step),
        this.runtime.sample({ role: 'periodic', slot: step.slot }),
      ])
      return
    }
    if (step.type === 'quiet') return this.send(judged, step)
    if (step.type === 'seed-start' || step.type === 'seed-clear') return this.send(['p13'], step)
    if (step.type === 'disconnect') {
      await this.shape(
        {
          ...step,
          id: 'traffic',
          shape: 'traffic',
          participants: Object.keys(this.matrix.clientKinds),
        },
        'end'
      )
      return this.send(all, step)
    }
    if (step.shape === 'traffic') {
      await this.shape(step, 'start')
      return this.send(all, { type: 'active', id: step.id })
    }
    if (step.type === 'idle') {
      await this.shape(step, 'start')
      await this.send(all, step)
      await this.clock.sleepUntil(step.until)
      await this.send(all, { ...step, type: 'active' })
      return this.shape(step, 'end')
    }
    await this.shape(step, 'start')
    const participants = [...new Set(step.participants.map((k) => this.processOf(k)))]
    if (step.shape === 'broker-outage') {
      await this.outage(step)
      await this.send(participants, { ...step, recovered: true })
    } else if (step.shape === 'regex-churn' || step.shape === 'topic-recreation') {
      await this.churnTopic(step, participants)
    } else {
      await this.send(participants, step)
      if (step.shape === 'drain') {
        for (const topic of [
          this.config.topics.main,
          this.config.topics.transactions,
          ...this.config.topics.recreate,
        ])
          await this.send(['p14'], { type: 'scan', id: `${step.id}.${topic}`, topic })
      }
    }
    await this.shape(step, 'end')
  }

  /**
   * A regex execution creates a fresh topic, lets every applicable client accept it, offers probe
   * ids, drains and scans it, deletes it and lets the clients retire it. A recreation drains and
   * scans the old incarnation, deletes it, creates the same name again and probes the new one.
   */
  async churnTopic(step, participants) {
    const settle = () =>
      this.clock.sleepUntil(this.clock.now() + 3 * this.config.timeouts.metadataMaxAge)
    const producer = this.processOf('producer-idempotent')
    const probe = async (topic) => {
      await this.send([producer], { ...step, probe: { topic, partitions: 2 } })
      await settle()
      await this.send(['p14'], { type: 'scan', id: `${step.id}.${topic}`, topic })
    }
    if (step.shape === 'regex-churn') {
      const topic = `${this.config.topics.regex}-${step.id}`
      await this.createTopic(topic, 2, step.id)
      await settle()
      await this.send(participants, step)
      await probe(topic)
      await this.deleteTopic(topic, step.id)
      await settle()
      return
    }
    const topic = this.config.topics.recreate[step.topic]
    await probe(topic)
    await this.send(participants, step)
    await this.deleteTopic(topic, step.id)
    await settle()
    await this.createTopic(topic, 2, step.id)
    await settle()
    await probe(topic)
  }

  /** Stops and restarts one broker as the step says, recording the supervisor receipts. */
  async outage(step) {
    const service = `kafka${step.node}`
    const compose = (args) =>
      new Promise((resolve, reject) =>
        execFile('docker', ['compose', '-p', 'kafkajs', ...args], { env: process.env }, (e) =>
          e ? reject(e) : resolve()
        )
      )
    const op = async (name, args) => {
      const operation = `${SUPERVISOR}.b${this.runtime.ids}`
      const base = {
        kind: 'operation',
        operation,
        op: name,
        client: null,
        keys: [],
        call: operation,
        execution: step.id,
        causes: [],
      }
      await this.emit({ ...base, phase: 'begin', result: null, errorType: null })
      try {
        await compose(args)
        await this.emit({ ...base, phase: 'end', result: 'ok', errorType: null })
      } catch (e) {
        await this.emit({ ...base, phase: 'end', result: 'error', errorType: e.name ?? 'Error' })
      }
    }
    await op(
      'broker-stop',
      step.stop === 'clean' ? ['stop', service] : ['kill', '-s', 'SIGKILL', service]
    )
    await this.clock.sleepUntil(this.clock.now() + step.downMs)
    await op('broker-start', ['start', service])
    for (;;) {
      const { brokers } = await this.admin.describeCluster().catch(() => ({ brokers: [] }))
      if (brokers.length === 3) break
      await this.clock.sleepUntil(this.clock.now() + 1000)
    }
  }

  /** @param {object} step @param {'start'|'end'} phase emits the shape boundary record */
  shape(step, phase) {
    return this.emit({
      kind: 'shape',
      execution: step.id,
      phase,
      shape: step.shape,
      participants: step.participants ?? [],
      operationCount: 0,
    })
  }
}

/**
 * The run's clock origin: the one the preflight recorded on this boot, so preflight facts are on the
 * run clock, or now when there is none.
 * @param {string} out the artifact directory
 * @returns {number} the origin in monotonic ms
 */
const originOf = (out) => {
  try {
    const clock = JSON.parse(fs.readFileSync(path.join(out, 'clock.json'), 'utf8'))
    if (clock.bootId === bootId()) return clock.originMs
  } catch {
    // no preflight origin: the run starts its own
  }
  return MonotonicClock.raw()
}

/**
 * Removes the run's topics and groups after the run, from `config.json`. Called by the runbook;
 * failures are reported and leave the artifact untouched.
 * @param {{ out: string }} options
 * @returns {Promise<void>}
 */
async function cleanup({ out }) {
  const config = JSON.parse(fs.readFileSync(path.join(out, 'config.json'), 'utf8'))
  const { Kafka, logLevel } = require('../../index')
  const admin = new Kafka({
    clientId: 'final-soak-cleanup',
    brokers: config.brokers,
    logLevel: logLevel.NOTHING,
  }).admin()
  await admin.connect()
  try {
    const topics = (await admin.listTopics()).filter((t) => t.startsWith(`fs-${config.runId}-`))
    if (topics.length > 0) await admin.deleteTopics({ topics })
    const groups = [...new Set(config.clients.map((c) => c.group).filter(Boolean))]
    if (groups.length > 0) await admin.deleteGroups(groups).catch(() => {})
  } finally {
    await admin.disconnect()
  }
}

/**
 * The outer handover after the supervisor has exited: appends the parent-observed supervisor exit as
 * the `outer` source, hashes and fsyncs every file and writes `header.json`.
 * @param {{ out: string, status: number }} options the artifact and the shell's wait status
 * @returns {object} the header written
 */
function finalize({ out, status }) {
  const run = JSON.parse(fs.readFileSync(path.join(out, 'supervisor.json'), 'utf8'))
  const clock = new MonotonicClock(run.originMs)
  const observedAt = clock.now()
  const signal =
    status > 128
      ? (Object.keys(os.constants.signals).find((s) => os.constants.signals[s] === status - 128) ??
        null)
      : null
  const code = signal ? null : status
  const streamFile = path.join(out, 'stream.ndjson')
  const exit = {
    kind: 'process',
    recordId: `${OUTER}.0`,
    source: OUTER,
    seq: 0,
    at: observedAt,
    t: observedAt,
    execution: null,
    event: 'exit',
    process: SUPERVISOR,
    code,
    signal,
    spawnRecord: `${SUPERVISOR}.0`,
  }
  const problems = validateRecord(exit)
  if (problems.length > 0) throw new Error(problems.join('; '))
  fs.appendFileSync(streamFile, JSON.stringify(exit) + '\n')
  const fd = fs.openSync(streamFile, 'r+')
  fs.fsyncSync(fd)
  fs.closeSync(fd)
  const files = []
  const walk = (dir) => {
    for (const entry of fs.readdirSync(path.join(out, dir), { withFileTypes: true })) {
      const rel = dir ? `${dir}/${entry.name}` : entry.name
      if (entry.isDirectory()) walk(rel)
      else if (entry.isFile() && rel !== 'header.json') {
        const bytes = fs.readFileSync(path.join(out, rel))
        files.push({ file: rel, bytes: bytes.length, sha256: sha256(bytes) })
      }
    }
  }
  walk('')
  const stream = files.find((f) => f.file === 'stream.ndjson')
  const preflight = JSON.parse(fs.readFileSync(path.join(out, 'preflight.json'), 'utf8'))
  const header = {
    schema: 3,
    mode: run.mode,
    runId: run.runId,
    treeSha: run.treeSha,
    node: process.version,
    v8: process.versions.v8,
    platform: process.platform,
    files,
    clock: { kind: 'linux-monotonic', originMs: run.originMs, bootId: run.bootId },
    window: {
      runStartAt: run.runStartAt,
      runEndAt: run.runEndAt,
      observedEndAt: run.observedEndAt,
      requestedMs: run.requestedMs,
    },
    host: preflight.host,
    broker: preflight.broker,
    exit: { supervisorProcess: SUPERVISOR, code, signal, observedAt },
    stream: {
      file: 'stream.ndjson',
      bytes: stream.bytes,
      records: fs.readFileSync(streamFile, 'utf8').split('\n').length - 1,
      sha256: stream.sha256,
    },
    companions: [],
  }
  fs.writeFileSync(path.join(out, 'header.json'), JSON.stringify(header, null, 2) + '\n')
  return header
}

/**
 * Runs a child process: `init` builds the runtime and takes the start handshake and baseline before
 * any command is served; commands then run concurrently, so sampling never waits behind a workload.
 * The process ends by itself once the `disconnect` command has run and the channel is closed.
 * @param {{ child: string, out: string }} options
 * @returns {void}
 */
function child(options) {
  let ready = null
  const replies = new Map()
  const acks = new Map()
  process.on('message', async (message) => {
    if (message.type === 'init') {
      ready = (async () => {
        const runtime = new ChildRuntime({
          slot: options.child,
          config: message.config,
          matrix: message.matrix,
          originMs: message.originMs,
          mode: message.mode,
          out: options.out,
          send: (m, done) => {
            acks.set(m.record.seq, done)
            process.send(m)
          },
        })
        for (const [topic, topicId] of message.topicIds) runtime.topicIds.set(topic, topicId)
        let requests = 0
        runtime.request = (body) =>
          new Promise((resolve) => {
            const request = ++requests
            replies.set(request, resolve)
            process.send({ ...body, request })
          })
        await runtime.start()
        return runtime
      })()
    } else if (message.type === 'ack') {
      acks.get(message.seq)?.()
      acks.delete(message.seq)
    } else if (message.type === 'reply') {
      replies.get(message.request)?.()
      replies.delete(message.request)
    } else if (message.type === 'command') {
      const runtime = await ready
      await runtime.handle(message)
      if (message.step.type === 'disconnect') {
        while (runtime.channel.pendingBytes > 0)
          await new Promise((resolve) => setTimeout(resolve, 10))
        process.disconnect()
      }
    }
  })
}

/** @param {string[]} argv @returns {Promise<void>} the command-line entry */
async function main(argv) {
  const options = parseArgs(argv, process.env)
  if (options.finalize) {
    finalize(options)
    return
  }
  if (options.cleanup) return cleanup(options)
  if (options.child) return child(options)
  await new Supervisor(options).run()
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((e) => {
    console.error(e.stack)
    process.exitCode = 1
  })
}

module.exports = {
  parseArgs,
  buildConfig,
  planSchedule,
  scheduleTotals,
  Scheduler,
  Collector,
  SourceChannel,
  Sampler,
  PassiveObserver,
  MonotonicClock,
  offeredRecord,
  payloadSha256,
  finalize,
}
