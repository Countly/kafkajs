const { createCoordinator } = require('../src/consumer/__tests__/fixtures/lifecycleCoordinator')

const EVENTS = ['stop', 'disconnect', 'force', 'crash', 'rejoin', 'poll-expiry']
const BOUNDARIES = [
  'before-handler',
  'in-handler',
  'in-heartbeat',
  'commit:coordinator-lookup',
  'commit:pool-connect',
  'commit:auth-encode',
  'commit:pending-queue',
  'commit:retry-backoff',
  'commit:answer-held',
  'in-join',
  'in-sync',
  'in-leave',
  'poll-expiry:leave',
  'pre-release-commit',
  'hook:revoked',
  'hook:assigned',
  'hook:lost',
  'session:initial',
  'session:inflight',
]
const PROTOCOLS = ['classic-eager', 'classic-cooperative', 'consumer']
const MEMBERSHIPS = ['dynamic', 'static']
const MANUAL_EVENTS = ['stop', 'disconnect', 'force', 'crash', 'reassign', 'unsubscribe', 'rebuild']
const MANUAL_COMMON_BOUNDARIES = [
  'before-handler',
  'in-handler',
  'manual:apply',
  'resolution:list-offsets',
  'position:answer-held',
  'idle-wait',
]
const MANUAL_COMMIT_BOUNDARIES = [
  'commit:coordinator-lookup',
  'commit:pool-connect',
  'commit:auth-encode',
  'commit:pending-queue',
  'commit:retry-backoff',
  'commit:answer-held',
]
const MANUAL_MEMBERSHIPS = ['grouped', 'no-group']

/**
 * The manual-assignment cells, appended after the membership cells without joining their product:
 * a manual run has no join, sync, heartbeat, poll-expiry, group-loss or consumer-target boundary,
 * and only a grouped manual run commits, so those boundaries are excluded rather than passed
 * vacuously.
 * @returns {Array<{event: string, boundary: string, protocol: 'manual', membership: 'grouped'|'no-group'}>}
 */
const manualCells = () => {
  const cells = []
  for (const event of MANUAL_EVENTS) {
    for (const boundary of MANUAL_COMMON_BOUNDARIES)
      for (const membership of MANUAL_MEMBERSHIPS)
        cells.push({ event, boundary, protocol: 'manual', membership })
    for (const boundary of MANUAL_COMMIT_BOUNDARIES)
      cells.push({ event, boundary, protocol: 'manual', membership: 'grouped' })
  }
  return cells
}

/**
 * Generates every core lifecycle cell, with only the protocol's explicit exclusions. The committed
 * catalogue starts with exactly this list; telemetry cells are only ever appended after it.
 * @returns {Array<{event: string, boundary: string, protocol: string, membership: string}>}
 */
const coreLifecycleCells = () => {
  const cells = []
  for (const event of EVENTS)
    for (const boundary of BOUNDARIES)
      for (const protocol of PROTOCOLS)
        for (const membership of MEMBERSHIPS) {
          if (
            ['in-join', 'in-sync'].includes(boundary) &&
            (protocol === 'consumer' || ['stop', 'disconnect', 'force'].includes(event))
          )
            continue
          if (boundary === 'in-leave' && protocol !== 'consumer' && membership === 'static')
            continue
          if (boundary === 'poll-expiry:leave' && event !== 'poll-expiry') continue
          if (boundary === 'pre-release-commit' && event !== 'rejoin') continue
          cells.push({ event, boundary, protocol, membership })
        }
  return [...cells, ...manualCells()]
}

module.exports = {
  coreLifecycleCells,
  createCoordinator,
  EVENTS,
  BOUNDARIES,
  PROTOCOLS,
  MEMBERSHIPS,
}

const telemetryAssert = require('node:assert/strict')
const { Schedule: TelemetrySchedule } = require('./schedule')
const realHrtime = process.hrtime.bind(process)
const CLOCK_PATCH = Symbol.for('kafkajs:test:telemetry-clock')

/** @returns {bigint} real monotonic nanoseconds, independent of a round's patched bigint clock */
const realMonotonicNs = () => {
  const [seconds, nanoseconds] = realHrtime()
  return BigInt(seconds) * 1000000000n + BigInt(nanoseconds)
}

/** Supplies deterministic telemetry time without coupling timer delivery to clock movement. */
class TelemetryClock {
  /** @returns {TelemetryClock} an empty local timer owner */
  constructor() {
    this.time = 0
    this.sequence = 0n
    this.pending = new Map()
    this.now = () => this.time
    this.random = () => 0.5
    this.setTimeout = (callback, delay) => {
      const id = ++this.sequence
      this.pending.set(id, { callback, due: this.time + delay })
      return id
    }
    this.clearTimeout = (id) => this.pending.delete(id)
  }

  /** @param {number} time monotonic milliseconds @returns {void} changes time without delivering timers
   * @throws {RangeError} if time goes backwards or is not finite */
  jump(time) {
    if (!Number.isFinite(time) || time < this.time) throw new RangeError('Invalid clock movement')
    this.time = time
  }

  /** @param {bigint} id captured timer @returns {Function} callback even after clear
   * @throws {Error} when the timer was not captured while pending */
  callback(id) {
    const entry = this.pending.get(id)
    if (!entry) throw new Error('Unknown timer')
    return entry.callback
  }

  /** @param {bigint} id currently due timer @returns {void} delivers once
   * @throws {Error} when the timer is absent or not due */
  deliver(id) {
    const entry = this.pending.get(id)
    if (!entry || entry.due > this.time) throw new Error('Timer not due')
    this.pending.delete(id)
    entry.callback()
  }
}

/**
 * Captures independently observed frames and delegates every hold to the existing Schedule. A
 * bounded trace validates and counts each frame but keeps neither it nor the schedule's call
 * history.
 */
class TelemetryTrace {
  /**
   * @param {{bounded?: boolean}} [options] `bounded` keeps counts instead of frames
   * @returns {TelemetryTrace} an empty finite observation round
   */
  constructor({ bounded = false } = {}) {
    this.schedule = new TelemetrySchedule()
    this.frames = []
    this.bounded = bounded
    this.count = 0
  }

  /** @param {object} selector exact existing Schedule selector @returns {object} existing Gate */
  hold(selector) {
    return this.schedule.hold(selector)
  }

  /** @param {object} call captured call data @param {Function} original lazy original invocation
   * @returns {Promise<*>} original result after the selected existing gate */
  async enter(call, original) {
    const held = this.schedule.run(call, () => undefined)
    if (this.bounded) this.schedule.calls.length = 0
    await held
    return original()
  }

  /** @param {{connection:bigint,attempt:bigint,lifecycle:bigint,transport:bigint,subscription:bigint,request:bigint,correlation:number,apiKey:number,hex?:string,bytes?:Buffer}} frame actual complete socket frame;
   *   a bounded trace takes `bytes` and never builds the hex text
   * @returns {void} retains immutable bytes and issued identities, or only counts in bounded mode
   * @throws {TypeError} when an observation is incomplete */
  write(frame) {
    for (const key of [
      'connection',
      'attempt',
      'lifecycle',
      'transport',
      'subscription',
      'request',
    ])
      if (typeof frame[key] !== 'bigint') throw new TypeError('Missing issued identity')
    if (!Number.isInteger(frame.correlation) || ![71, 72].includes(frame.apiKey))
      throw new TypeError('Missing wire identity')
    if (!frame.bytes && (typeof frame.hex !== 'string' || !/^(?:[0-9a-f]{2})+$/.test(frame.hex)))
      throw new TypeError('Missing frame bytes')
    const bytes = frame.bytes ?? Buffer.from(frame.hex, 'hex')
    if (
      bytes.length < 12 ||
      bytes.readInt32BE(0) !== bytes.length - 4 ||
      bytes.readInt16BE(4) !== frame.apiKey ||
      bytes.readInt32BE(8) !== frame.correlation
    )
      throw new TypeError('Frame identity differs from bytes')
    this.count += 1
    if (!this.bounded) this.frames.push(Object.freeze({ ...frame }))
  }

  /** @param {Array<object>} expected independent literal identities and bytes
   * @returns {void} compares every ordered frame, never count alone
   * @throws {import('node:assert').AssertionError} missing, extra, reordered or substituted frame */
  assertFrames(expected) {
    telemetryAssert.deepStrictEqual(this.frames, expected)
  }
}

/**
 * The per-object patch state of a bounded round. An owner (a connection, a socket, a transport
 * handle) is held here only while it is live; its restoration lives with it and not in the round's
 * fixed restorer list, and a weak set deduplicates by identity. A completed owner is restored and
 * forgotten as soon as its `done` predicate holds, so no completed object stays reachable. Going
 * past the live bound fails instead of growing.
 */
class LiveOwners {
  /**
   * @param {number} limit the most live owners at once
   * @returns {LiveOwners} an empty table
   */
  constructor(limit) {
    this.limit = limit
    this.entries = new Map()
    this.seen = new WeakSet()
    this.issued = 0
  }

  /** @returns {number} owners that are live now */
  get size() {
    return this.entries.size
  }

  /**
   * @param {object} owner a candidate owner @returns {boolean} whether it is live in the table
   */
  has(owner) {
    return this.seen.has(owner)
  }

  /**
   * Adds one restoration for an owner, admitting the owner on its first.
   * @param {object} owner the patched object
   * @param {() => void} restore undoes one patch
   * @param {() => boolean} [done] whether the owner has completed; fixed by the first call
   * @returns {void}
   * @throws {Error} when a new owner would exceed the live bound
   */
  add(owner, restore, done = () => false) {
    const entry = this.entries.get(owner)
    if (entry) {
      entry.restores.push(restore)
      return
    }
    if (this.entries.size >= this.limit)
      throw new Error(`Bounded capture overflow: more than ${this.limit} live owners`)
    this.seen.add(owner)
    this.entries.set(owner, { restores: [restore], done })
    this.issued += 1
  }

  /** @returns {number} live owners that have already completed, which must be none after a release */
  completed() {
    return [...this.entries.values()].filter((entry) => entry.done()).length
  }

  /**
   * Restores and forgets one owner.
   * @param {object} owner the owner @returns {boolean} whether it was live
   */
  release(owner) {
    const entry = this.entries.get(owner)
    if (!entry) return false
    this.entries.delete(owner)
    this.seen.delete(owner)
    for (const restore of entry.restores.reverse()) restore()
    return true
  }

  /** @returns {number} how many completed owners were restored and forgotten */
  releaseCompleted() {
    let released = 0
    for (const [owner, entry] of [...this.entries])
      if (entry.done() && this.release(owner)) released += 1
    return released
  }

  /** @returns {Array<() => void>} every live restoration, newest first; the table is then empty */
  drain() {
    const calls = [...this.entries.values()].flatMap((entry) => entry.restores.reverse())
    this.entries.clear()
    return calls
  }
}

/** Captures the original outcome without invoking it again or converting a synchronous throw.
 * @param {Function} call exactly one admitted original call
 * @returns {Promise<{kind:string,value?:*,reason?:*}>} tagged original outcome */
const telemetryOutcome = async (call) => {
  let value
  try {
    value = call()
  } catch (reason) {
    return { kind: 'throw', reason }
  }
  try {
    return { kind: 'return', value: await value }
  } catch (reason) {
    return { kind: 'reject', reason }
  }
}

/** @param {object} actual captured original outcome @param {object} expected identity contract
 * @param {number} invocations actual admitted original calls @returns {void}
 * @throws {import('node:assert').AssertionError} changed outcome or invocation count */
const assertTelemetryOutcome = (actual, expected, invocations) => {
  telemetryAssert.equal(invocations, 1)
  telemetryAssert.equal(actual.kind, expected.kind)
  telemetryAssert.equal(
    actual.kind === 'return' ? actual.value : actual.reason,
    expected.kind === 'return' ? expected.value : expected.reason
  )
}

const TELEMETRY_UUID = '01020304-0506-0708-090a-0b0c0d0e0f10'

/** The live bounds of a bounded round: owners, dedicated sockets and recent abandoned samples. */
const BOUNDED_LIMITS = Object.freeze({ owners: 16, sockets: 8, abandoned: 64 })
const TELEMETRY_KINDS = ['producer', 'consumer', 'admin', 'shareConsumer']
const DEFAULT_SUBSCRIPTION = Object.freeze({
  clientInstanceId: TELEMETRY_UUID,
  subscriptionId: 7,
  acceptedCompressionTypes: Object.freeze([0]),
  pushIntervalMs: 100,
  telemetryMaxBytes: 4096,
  deltaTemporality: true,
  requestedMetrics: Object.freeze(['com.example.']),
})

/** The six stale-work actions in table order; the suffix is the literal test title text. */
const RELEASE_ACTIONS = Object.freeze([
  'current authority completes',
  'transport replacement refuses old work',
  'public close revokes ordinary work',
  'zero budget sends no final',
  'actual expiry wins over delayed timer',
  'public reopen cannot adopt old work',
])

/** Actions that remain meaningful once a public close has already captured its final. */
const FINAL_ACTIONS = Object.freeze([
  RELEASE_ACTIONS[0],
  RELEASE_ACTIONS[1],
  RELEASE_ACTIONS[4],
  RELEASE_ACTIONS[5],
])

/**
 * Each boundary's reachable modes. Pending dispatch uses a real in-flight ApiVersions blocker.
 * Retirement holds its result after the captured pool has actually been destroyed.
 * @type {ReadonlyArray<[string, ReadonlyArray<'get'|'normal'|'final'>]>}
 */
const RELEASE_LEDGER = Object.freeze([
  ...['S01', 'S02', 'S03', 'S04', 'S05', 'S06', 'S07', 'S08', 'S09'].map((id) => [id, ['get']]),
  ...['S10', 'S11', 'S12'].map((id) => [id, ['normal', 'final']]),
  ...['S13', 'S14', 'S15', 'S16', 'S17', 'S18'].map((id) => [id, ['get', 'normal', 'final']]),
])

/** S19 renewal sub-boundaries: handshake, supplier, encode, decode, parse, provider completion. */
const RENEWAL_BOUNDARIES = Object.freeze(['S19a', 'S19b', 'S19c', 'S19d', 'S19e', 'S19f'])

const SASL_BOUNDARIES = new Set(['S03', 'S04', 'S05', 'S06', 'S07', ...RENEWAL_BOUNDARIES])

/**
 * @param {string} boundary ledger id
 * @param {'get'|'normal'|'final'} mode operation mode
 * @returns {ReadonlyArray<string>} the actions that apply to the boundary in this mode
 */
const actionsFor = (boundary, mode) => {
  if (mode === 'final') return FINAL_ACTIONS
  return RELEASE_ACTIONS
}

/**
 * Frames written to the dedicated socket before the hold arrives, by position in the write path.
 * @param {string} boundary ledger id
 * @param {'get'|'normal'|'final'} mode operation mode
 * @returns {Array<'G'|'P'|'F'>} Get, ordinary Push and final Push in write order
 */
const prewritten = (boundary, mode, position = 'before') => {
  if (boundary === 'S18' && position === 'result')
    return mode === 'get' ? ['G'] : ['G', 'P', mode === 'final' ? 'F' : 'P']
  const base = mode === 'get' ? [] : ['G', 'P']
  if (boundary === 'S14' && position === 'after')
    return [...base, { get: 'G', normal: 'P', final: 'F' }[mode]]
  if (boundary === 'S09' || (boundary === 'S17' && mode === 'get')) return ['G']
  if (boundary === 'S17' && mode === 'normal') return [...base, 'P']
  if (boundary === 'S15' && mode === 'final') return [...base, 'P']
  if (boundary === 'S18') return mode === 'get' ? ['G'] : [...base, mode === 'final' ? 'F' : 'P']
  if (boundary === 'S17') return [...base, 'F']
  return base
}

/**
 * Builds the predeclared frame manifest of a row from independent literals. It never filters
 * actual frames: every entry is derived from the boundary position and the action alone. Only
 * frames written by the held operation itself carry its lifecycle.
 * @param {{boundary:string,mode:string,action:string}} cell the row
 * @returns {(old:object, replacement:object) => Array<object>} manifest builder over captured ids
 */
const manifestOf =
  ({ boundary, mode, action, options }) =>
  (old) => {
    const fresh = mode === 'get'
    const frame = (kind, socket, held, initial = false) => ({
      api: kind === 'G' ? 71 : 72,
      terminating: kind === 'G' ? null : kind === 'F',
      uuid: kind === 'G' && (initial || fresh) ? null : TELEMETRY_UUID,
      subscriptionId: kind === 'G' ? null : DEFAULT_SUBSCRIPTION.subscriptionId,
      socket,
      compressionType: kind === 'G' ? null : boundary === 'S12' ? 1 : 0,
      ...(held
        ? {
            lifecycle: old.lifecycle,
            transport: old.transport,
            subscription: kind === 'G' ? 0n : old.subscription,
            request: kind === 'F' ? old.closeRevision : old.running,
            closeRevision: kind === 'F' ? old.closeRevision : 0n,
          }
        : {}),
    })
    const before = prewritten(boundary, mode, options.position)
    const expected = before.map((kind, index) =>
      frame(
        kind,
        1n,
        index === before.length - 1 &&
          (['S09', 'S17'].includes(boundary) ||
            (boundary === 'S14' && options.position === 'after')),
        index === 0
      )
    )
    const successor = boundary === 'S01' && fresh && options.position === 'before' ? 1n : 2n
    const reconnect = () => [frame('G', successor), frame('P', successor)]
    if (action === RELEASE_ACTIONS[0]) {
      if (
        !['S09', 'S17', 'S18'].includes(boundary) &&
        !(boundary === 'S14' && options.position === 'after')
      )
        expected.push(frame({ get: 'G', normal: 'P', final: 'F' }[mode], 1n, true))
    } else if (action === RELEASE_ACTIONS[1] && mode !== 'final') expected.push(...reconnect())
    else if (
      action === RELEASE_ACTIONS[2] &&
      mode === 'normal' &&
      (boundary !== 'S18' || options.position === 'result') &&
      !RENEWAL_BOUNDARIES.includes(boundary)
    )
      expected.push(frame('F', 1n))
    else if (action === RELEASE_ACTIONS[5]) expected.push(...reconnect())
    return expected
  }

/**
 * @param {string} boundary ledger id
 * @returns {object} the start options that reach it: SASL boundaries authenticate, S12 needs enough
 *   legal points to exceed the compression floor and codecs [1, 0]
 */
const startFor = (boundary) => ({
  sasl: SASL_BOUNDARIES.has(boundary),
  ...(boundary === 'S12'
    ? {
        points: 'large',
        subscription: { ...DEFAULT_SUBSCRIPTION, acceptedCompressionTypes: [1, 0] },
      }
    : {}),
})

/**
 * Expands the explicit stale-work catalogue: kinds, enforcement, boundaries S01-S18 with their
 * modes and actions, then the S19 renewal sub-boundaries in normal mode.
 * @returns {Array<{name:string,kind:string,enforceRequestTimeout:boolean,boundary:string,action:string,
 *   mode:string,start:object,options:object,releaseValue:undefined,expectation:Function}>} every
 *   row, unique by name, in dimension order
 */
const telemetryReleaseCells = () => {
  const cells = []
  const ledger = [...RELEASE_LEDGER, ...RENEWAL_BOUNDARIES.map((id) => [id, ['normal']])]
  for (const kind of TELEMETRY_KINDS)
    for (const enforceRequestTimeout of [false, true])
      for (const [boundary, modes] of ledger)
        for (const mode of modes)
          for (const action of actionsFor(boundary, mode))
            for (const position of boundary === 'S01' || boundary === 'S14'
              ? ['before', 'after']
              : boundary === 'S08' || boundary === 'S09'
                ? ['before', 'decode', 'parse']
                : boundary === 'S17'
                  ? ['decode', 'parse']
                  : boundary === 'S18'
                    ? ['retire', 'result']
                    : ['before']) {
              const cell = {
                name: `${kind}/${enforceRequestTimeout ? 'enforced' : 'natural'}/${boundary}/${mode}/${position}/${action}`,
                kind,
                enforceRequestTimeout,
                boundary,
                action,
                mode,
                start: { ...startFor(boundary), drive: mode },
                options: { position },
                releaseValue: undefined,
                lateArrival: false,
              }
              cells.push({ ...cell, expectation: manifestOf(cell) })
            }
  return cells
}

/** The eight telemetry lifecycle pairs appended for every reachable consumer protocol pair. */
const TELEMETRY_PAIRS = Object.freeze([
  ['disconnect', 'telemetry:final-encode'],
  ['disconnect', 'telemetry:final-dispatch'],
  ['disconnect', 'telemetry:deadline'],
  ['crash', 'telemetry:get-answer'],
  ['crash', 'telemetry:waiter-deadline'],
  ['stop', 'telemetry:active-push'],
  ['rejoin', 'telemetry:resource-snapshot'],
  ['disconnect', 'telemetry:old-final-result'],
])

/**
 * Appends the telemetry rows after the core catalogue without recursion: reachable protocol and
 * membership pairs come from the core generator, and a manual run has no rejoin.
 * @returns {Array<{event:string,boundary:string,protocol:string,membership:string,telemetry:true,
 *   enforceRequestTimeout:boolean}>} the added rows, enforcement false then true per pair
 */
const telemetryLifecycleCells = () => {
  const combos = []
  const seen = new Set()
  for (const { protocol, membership } of coreLifecycleCells()) {
    const key = `${protocol}/${membership}`
    if (seen.has(key)) continue
    seen.add(key)
    combos.push({ protocol, membership })
  }
  const cells = []
  for (const { protocol, membership } of combos)
    for (const [event, boundary] of TELEMETRY_PAIRS) {
      if (protocol === 'manual' && event === 'rejoin') continue
      for (const enforceRequestTimeout of [false, true])
        cells.push({
          event,
          boundary,
          protocol,
          membership,
          telemetry: true,
          enforceRequestTimeout,
        })
    }
  return cells
}

/**
 * Generates every reviewable lifecycle cell: the core catalogue, byte for byte, then the telemetry
 * rows.
 * @returns {Array<object>} core cells followed by telemetry cells
 */
const lifecycleCells = () => [...coreLifecycleCells(), ...telemetryLifecycleCells()]

Object.assign(module.exports, {
  lifecycleCells,
  telemetryLifecycleCells,
  telemetryReleaseCells,
  TelemetryClock,
  TelemetryTrace,
  telemetryOutcome,
  assertTelemetryOutcome,
  RELEASE_ACTIONS,
  TELEMETRY_KINDS,
  TELEMETRY_UUID,
  DEFAULT_SUBSCRIPTION,
})

const ISSUED_REQUEST = Symbol('issued telemetry request')
const BULK_POINTS = 30
const turn = () => new Promise((resolve) => setImmediate(resolve))
const symbolOf = (target, name) =>
  Object.getOwnPropertySymbols(target).find((symbol) => symbol.description === name)

/** Reads the small protobuf subset of OTLP metrics independently of the production writer. */
const protobuf = (bytes) => {
  let at = 0
  const fields = []
  const varint = () => {
    let value = 0
    let factor = 1
    for (;;) {
      const byte = bytes[at++]
      value += (byte & 127) * factor
      if (byte < 128) return value
      factor *= 128
    }
  }
  while (at < bytes.length) {
    const tag = varint()
    const type = tag % 8
    const number = Math.floor(tag / 8)
    if (type === 0) fields.push({ number, value: varint() })
    else {
      const size = type === 1 ? 8 : varint()
      fields.push({ number, value: bytes.subarray(at, at + size) })
      at += size
    }
  }
  return fields
}
const protoValues = (bytes, number) =>
  protobuf(bytes)
    .filter((field) => field.number === number)
    .map((field) => field.value)

/** A hold over a blackholed dial: the socket exists but never connects until released. */
class DialGate {
  /** @param {PublicTelemetryRound} round owner that supplies the wire and node once started */
  constructor(round) {
    this.round = round
    this.settle = null
  }

  /** @returns {Promise<void>} resolves once a dial to the node is held @throws {Error} never held */
  async arrived() {
    const { wire, node } = this.round
    await this.round.until(() => wire.heldDialsTo(node) > 0)
    this.settle = true
  }

  /** @returns {void} lets the held dial connect */
  release() {
    const { wire, node } = this.round
    this.settle = null
    wire.up(node)
    wire.release(node)
  }
}

/**
 * Drives one actual public client of a kind through the real factory, strict scripted wire and the
 * real telemetry machine, with a deterministic telemetry clock and one hold at a time. It observes
 * the product; it never replaces the machine, payload, collector, queue or the method under test.
 * Patches are installed per round and restored by `dispose`; rounds must run one at a time.
 */
class PublicTelemetryRound {
  /**
   * @param {{kind:'producer'|'consumer'|'admin'|'shareConsumer',enforceRequestTimeout?:boolean,
   *   enabled?:boolean,protocol?:string,membership?:string,bounded?:boolean}} options the actual
   *   client and variant. `bounded` opts in to bounded development capture for a long schedule:
   *   per-object patches live in a live-owner table, dedicated sockets are tracked live beside
   *   monotonic counters, completed work is kept only as scalars and counters, and the cluster
   *   and fake sockets are bounded too. A bounded round refuses the assertions that need the whole
   *   history (`assertTrace`, `writtenFor`, `finalSnapshotIdentity`, `replacementMetricOrigin`,
   *   `forcedRetirements`, `newSetupCallsSince`); every default is unchanged.
   * @returns {PublicTelemetryRound} configuration only; no client exists until `start`
   */
  constructor({
    kind,
    enforceRequestTimeout = false,
    enabled = true,
    protocol = 'classic-eager',
    membership = 'dynamic',
    bounded = false,
  }) {
    Object.assign(this, { kind, enforceRequestTimeout, enabled, protocol, membership, bounded })
    this.owners = new LiveOwners(BOUNDED_LIMITS.owners)
    this.liveSockets = new Set()
    this.socketIds = new WeakMap()
    this.socketsIssued = 0
    this.writeCount = 0
    this.writeBytes = 0
    this.snapshotCount = 0
    this.snapshotOrdinals = new WeakMap()
    this.revisions = new Map()
    this.frameFields = []
    this.writtenCount = 0
    this.abandonedCount = 0
    this.abandonOrdinals = []
    this.handlesIssued = 0
    this.opensAttempted = 0
    this.retiredCount = 0
    this.retiredHandles = new WeakSet()
    this.pushTotals = { requested: 0, answered: 0, finalRequested: 0, finalAnswered: 0 }
    this.clock = new TelemetryClock()
    this.trace = new TelemetryTrace({ bounded })
    this.wireSchedule = new TelemetrySchedule()
    this.restores = []
    this.clockRestores = []
    this.pending = []
    this.work = new Set()
    this.waitTimeout = 10000
    this.gates = []
    this.unhandled = []
    this.bindings = []
    this.node = 1
    this.armed = false
    this.held = null
    this.foregroundSockets = 0
    this.closeEntry = 0
    this.closePromise = null
    this.written = []
    this.abandoned = []
    this.snapshots = []
    this.codecJobs = []
    this.expectedFrames = []
    this.builtHandles = []
    this.guards = { ordinary: null, closing: null }
    this.setupCalls = []
    this.socketWrites = []
    this.retirements = []
    this.finalHook = null
    this.reregisteredAt = null
    this.dedicatedConnection = null
    this.dedicatedSockets = []
    this.brokerAnswerTimers = new Set()
    this.deliveries = []
    this.eventRemovers = []
    this.retiredCorePools = []
    this.core = { handlers: [], commits: [], leaves: [], losses: [], joins: [] }
    this.listener = (reason) => this.unhandled.push(reason)
    process.on('unhandledRejection', this.listener)
  }

  /**
   * Installs one deterministic clock with restoration owned by this round. Inactive older patches
   * are skipped when the current owner restores, so an out-of-order cleanup cannot revive a clock
   * whose round has already ended. Property descriptors are restored without Jest spy state.
   * @param {object} target clock owner @param {string} key clock method
   * @param {Function} replacement deterministic reading
   * @returns {void}
   * @throws {TypeError} if the clock property cannot be replaced
   */
  patchClock(target, key, replacement) {
    const patch = {
      active: true,
      descriptor: Object.getOwnPropertyDescriptor(target, key),
      previous: target[key]?.[CLOCK_PATCH],
    }
    Object.defineProperty(replacement, CLOCK_PATCH, { value: patch })
    Object.defineProperty(target, key, {
      value: replacement,
      writable: true,
      configurable: true,
      enumerable: patch.descriptor?.enumerable ?? false,
    })
    this.clockRestores.push(() => {
      patch.active = false
      let current = target[key]?.[CLOCK_PATCH]
      if (!current || current.active) return
      let descriptor
      while (current && !current.active) {
        descriptor = current.descriptor
        current = current.previous
      }
      if (descriptor) Object.defineProperty(target, key, descriptor)
      else delete target[key]
    })
  }

  /** @returns {void} installs the round's deterministic readings with independent cleanup ownership */
  installClocks() {
    this.patchClock(Date, 'now', () => 1000 + this.clock.now())
    this.patchClock(process.hrtime, 'bigint', () => BigInt(this.clock.now()) * 1000000n)
    const imported = require('node:perf_hooks').performance
    this.patchClock(imported, 'now', () => this.clock.now())
    if (global.performance !== imported)
      this.patchClock(global.performance, 'now', () => this.clock.now())
  }

  /**
   * Replaces one property for the life of the round.
   * @param {object} target owner @param {string|symbol} key property
   * @param {(original: *) => *} make builds the replacement from the original @returns {void}
   */
  patch(target, key, make) {
    const original = target[key]
    target[key] = make(original)
    this.restores.push(() => {
      target[key] = original
    })
  }

  /**
   * Replaces one property of an object whose life is shorter than the round's. A default round
   * keeps the restoration with its other patches; a bounded round keeps it in the live-owner
   * table until `done` holds and `releaseCompleted` restores it.
   * @param {object} owner the patched object @param {string|symbol} key property
   * @param {(original: *) => *} make builds the replacement from the original
   * @param {() => boolean} [done] whether the owner has completed (bounded rounds only)
   * @returns {void} @throws {Error} when a bounded round already has its live owners; the owner
   *   is then left unpatched
   */
  patchFor(owner, key, make, done) {
    if (!this.bounded) return this.patch(owner, key, make)
    const original = owner[key]
    const replacement = make(original)
    this.owners.add(
      owner,
      () => {
        owner[key] = original
      },
      done
    )
    owner[key] = replacement
  }

  /**
   * Like `watchWork` for a per-object operation: joins its settlement without replacing its result.
   * @param {object} owner the object @param {string} key operation
   * @param {() => boolean} [done] whether the owner has completed (bounded rounds only)
   * @returns {void}
   */
  watchWorkFor(owner, key, done) {
    const round = this
    this.patchFor(
      owner,
      key,
      (original) =>
        function (...args) {
          return round.trackWork(original.apply(this, args))
        },
      done
    )
  }

  /**
   * Remembers one dedicated raw socket: an issued number and, while it is open, its live entry.
   * A default round keeps every socket in `dedicatedSockets`.
   * @param {object} raw the raw socket @returns {void}
   * @throws {Error} when a bounded round already has its live sockets
   */
  trackSocket(raw) {
    if (!raw) return
    if (!this.bounded) {
      if (!this.dedicatedSockets.includes(raw)) this.dedicatedSockets.push(raw)
      return
    }
    if (this.socketIds.has(raw)) return
    if (this.liveSockets.size >= BOUNDED_LIMITS.sockets)
      throw new Error(`Bounded capture overflow: more than ${BOUNDED_LIMITS.sockets} live sockets`)
    this.socketIds.set(raw, ++this.socketsIssued)
    this.liveSockets.add(raw)
  }

  /**
   * @param {object} raw a dedicated raw socket
   * @returns {number} its issued number counting from 1, or 0 when it was never tracked
   */
  socketNumber(raw) {
    return this.bounded ? (this.socketIds.get(raw) ?? 0) : this.dedicatedSockets.indexOf(raw) + 1
  }

  /** @returns {object[]} the dedicated raw sockets that are still open */
  openDedicated() {
    const sockets = this.bounded ? [...this.liveSockets] : this.dedicatedSockets
    return sockets.filter((socket) => !socket.destroyed)
  }

  /**
   * Counts one write of a watched dedicated socket.
   * @param {Buffer|string} bytes what was written @returns {void}
   */
  noteWrite(bytes) {
    this.writeCount += 1
    this.writeBytes += bytes.length
  }

  /**
   * Watches the writes of one dedicated socket without replacing what it does. A default round
   * records each write; a bounded one only counts it, and keeps the wrapper in the live-owner
   * table until the socket is destroyed.
   * @param {object} socket the product's socket object @returns {void}
   */
  watchWrites(socket) {
    const round = this
    if (this.bounded) {
      if (this.owners.has(socket)) return
      this.patchFor(
        socket,
        'write',
        (write) =>
          function (bytes, ...args) {
            round.noteWrite(bytes)
            return write.call(this, bytes, ...args)
          },
        () => Boolean((socket.inner ?? socket).destroyed)
      )
      return
    }
    if (this.observedSockets?.has(socket)) return
    this.observedSockets ??= new Set()
    this.observedSockets.add(socket)
    this.patch(
      socket,
      'write',
      (write) =>
        function (bytes, ...args) {
          round.noteWrite(bytes)
          round.socketWrites.push({
            socket: this,
            bytes: Buffer.from(bytes),
            time: round.clock.now(),
          })
          return write.call(this, bytes, ...args)
        }
    )
  }

  /**
   * Keeps the work promise of one asynchronous action until it settles, then forgets it. A default
   * round keeps every one, as before.
   * @param {Promise<*>} settled the observed promise @returns {void}
   */
  keepPending(settled) {
    this.pending.push(settled)
    if (!this.bounded) return
    const forget = () => {
      const at = this.pending.indexOf(settled)
      if (at !== -1) this.pending.splice(at, 1)
    }
    settled.then(forget, forget)
  }

  /**
   * Restores and forgets every completed per-object patch and drops closed sockets. Call it at a
   * verified settlement boundary, before the next checkpoint. A default round has none.
   * @returns {number} the owners restored
   */
  releaseCompleted() {
    if (!this.bounded) return 0
    for (const socket of [...this.liveSockets])
      if (socket.destroyed) this.liveSockets.delete(socket)
    return this.owners.releaseCompleted()
  }

  /**
   * @param {string} what the history-reading method
   * @returns {void} @throws {Error} in a bounded round, which keeps no whole history
   */
  assertFull(what) {
    if (this.bounded) throw new Error(`${what}() needs the full capture, not a bounded round`)
  }

  /**
   * The populations of every capture root of this round, as plain numbers, so a bounded schedule
   * can assert that none of them grows with the number of cycles.
   * @returns {Readonly<Record<string, number>>} fixed restorers, live owners, restoration entries
   *   already completed, live sockets and wire state, retained histories and bytes
   */
  boundedState() {
    const retainedBytes =
      this.socketWrites.reduce((total, write) => total + write.bytes.length, 0) +
      this.trace.frames.reduce((total, frame) => total + frame.hex.length / 2, 0) +
      this.expectedFrames.reduce((total, frame) => total + frame.hex.length / 2, 0)
    return Object.freeze({
      restorers: this.restores.length + this.clockRestores.length,
      owners: this.owners.size,
      ownersIssued: this.owners.issued,
      ownersCompleted: this.owners.completed(),
      liveSockets: this.liveSockets.size,
      socketsIssued: this.socketsIssued,
      physicalSocketsIssued: this.wire.fake.state.issued,
      wireSockets: this.wire.sockets().length,
      wireLive: this.wire.requests().length,
      handles: this.builtHandles.length,
      pending: this.pending.length,
      work: this.work.size,
      history:
        this.snapshots.length +
        this.written.length +
        this.abandoned.length +
        this.retirements.length +
        this.socketWrites.length +
        this.trace.frames.length +
        this.frameFields.length +
        this.expectedFrames.length +
        this.wireSchedule.calls.length +
        this.trace.schedule.calls.length,
      abandonedRing: this.abandonOrdinals.length,
      retainedBytes,
      writes: this.writeCount,
      writesAfterClose: this.wire.fake.state.writesAfterClose,
    })
  }

  /** @returns {number} ordinary Pushes written so far, counted whatever the history holds */
  pushesRequested() {
    return this.pushTotals.requested
  }

  /** @returns {number} ordinary Pushes that were answered, counted whatever the history holds */
  pushesAnswered() {
    return this.pushTotals.answered
  }

  /** @returns {number} Get and Push requests written so far, counted whatever the history holds */
  telemetryRequestCount() {
    return this.wire.tally('GetTelemetrySubscriptions') + this.wire.tally('PushTelemetry')
  }

  /**
   * Counts the ordinary and final Pushes from the wire's own notifications, in every mode.
   * @returns {void}
   */
  watchWire() {
    this.wire.watch((event, record) => {
      if (record.api !== 'PushTelemetry' || event === 'cancelled') return
      const names =
        record.fields.terminating === true
          ? { request: 'finalRequested', answered: 'finalAnswered' }
          : { request: 'requested', answered: 'answered' }
      this.pushTotals[names[event]] += 1
    })
  }

  /**
   * Waits for an observed condition until a real monotonic deadline, independent of the fake clock.
   * @param {() => boolean} predicate independent observation @param {number} [timeout=10000] milliseconds
   * @returns {Promise<void>} @throws {Error} when the boundary is never reached
   */
  async until(predicate, timeout = 10000) {
    const deadline = realMonotonicNs() + BigInt(timeout) * 1000000n
    while (!predicate()) {
      this.wire?.assertClean()
      if (realMonotonicNs() >= deadline) throw new Error('Telemetry boundary did not settle')
      await turn()
    }
  }

  /**
   * Bounds an observation without retaining its underlying arrival promise as owned work.
   * Rejections are observed immediately, including after timeout; the original outcome is preserved.
   * @param {Function} thunk invoked only now @param {number} [timeout=this.waitTimeout] real milliseconds
   * @returns {Promise<*>} the action's own outcome
   * @throws {Error} the original failure or a setup timeout, never a product refusal oracle
   */
  async wait(thunk, timeout = this.waitTimeout) {
    const diagnostic = new Error('Telemetry wait exceeded its bound')
    const promise = Promise.resolve().then(thunk)
    let timer
    const limit = new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(diagnostic), timeout)
    })
    try {
      return await Promise.race([promise, limit])
    } finally {
      clearTimeout(timer)
    }
  }

  /**
   * Observes actual work through its full settlement, including cancelled native/provider shells.
   * @param {Promise<*>} promise original operation @returns {Promise<*>} the same promise
   */
  trackWork(promise) {
    const settled = Promise.resolve(promise).then(
      () => this.work.delete(settled),
      () => this.work.delete(settled)
    )
    this.work.add(settled)
    return promise
  }

  /**
   * Wraps a real operation without replacing its result, rejection or receiver.
   * @param {object} target owner @param {string} key operation
   * @param {(receiver:object,args:Array<*>) => boolean} [selected] whether this is owned work
   * @returns {void} restores the wrapper with the round's other patches
   */
  watchWork(target, key, selected = () => true) {
    const round = this
    this.patch(
      target,
      key,
      (original) =>
        function (...args) {
          const result = original.apply(this, args)
          return selected(this, args) ? round.trackWork(result) : result
        }
    )
  }

  /**
   * Joins owned work, including work admitted by an earlier operation's continuation.
   * Arrival observers and public ID waiters are deliberately excluded.
   * @param {number} [timeout=10000] real milliseconds @returns {Promise<void>}
   * @throws {Error} when a shell never settles
   */
  async settleWork(timeout = 10000) {
    await this.wait(async () => {
      while (this.work.size) await Promise.all([...this.work])
    }, timeout)
  }

  /** @returns {Array<object>} wire records of the telemetry APIs, oldest first */
  telemetryRequests() {
    this.assertFull('telemetryRequests')
    return this.wire
      .requests()
      .filter((r) => r.api === 'GetTelemetrySubscriptions' || r.api === 'PushTelemetry')
  }

  /** @returns {number} dedicated sockets opened so far, from the issued counter in a bounded round */
  dedicatedCount() {
    return this.bounded ? this.socketsIssued : this.dedicatedSockets.length
  }

  /**
   * Passes the call through the trace gate once armed; a call with no rule is not delayed.
   * @param {string} boundary ledger id @returns {Promise<void>} resolves when released
   */
  async enter(boundary) {
    await this.trace.enter({ boundary }, () => undefined)
  }

  /**
   * Installs exactly one hold. Its wrapper selects the dedicated telemetry connection by its
   * private authentication owner, never by endpoint, and runs only while the round is armed.
   * @param {string} boundary ledger id S01-S19f
   * @param {{mode?:'get'|'normal'|'final',retirementResultOnly?:boolean,position?:string}} [options] operation;
   *   retirementResultOnly selects a naturally triggered retirement without expiring a held answer
   * @returns {object} a Schedule gate, or a dial gate for S02
   * @throws {Error} duplicate or unknown boundary
   */
  hold(boundary, { mode = 'normal', retirementResultOnly = false, position = 'before' } = {}) {
    if (this.held) throw new Error('Duplicate telemetry hold')
    const Connection = require('../src/network/connection')
    const Broker = require('../src/broker')
    const RequestQueue = require('../src/network/requestQueue')
    const TelemetryPayload = require('../src/telemetry/payload')
    const gzip = require('../src/protocol/message/compression/gzip')
    const round = this
    const dedicated = (connection) => connection.authenticationOwner != null
    const dedicatedCall = (call) => {
      const socket = round.dedicatedConnection?.socket
      return (
        round.wire.socketKeys.get(socket?.inner ?? socket) === `${call.node}/${call.generation}`
      )
    }
    const closing = () => round.machine?.closing != null
    const kindOf = (apiKey, final) => (apiKey === 71 ? 'get' : final ? 'final' : 'normal')
    const encodeHold = (apiKeys, position, byMode) =>
      this.patch(
        Connection.prototype,
        'send',
        (original) =>
          function (options) {
            const { apiKey } = options.request
            if (
              !round.armed ||
              !dedicated(this) ||
              !apiKeys.includes(apiKey) ||
              (byMode && kindOf(apiKey, options.establishedOnly) !== mode)
            )
              return original.call(this, options)
            const encode = options.request.encode
            const request = {
              ...options.request,
              encode: async (...args) => {
                if (position === 'before') await round.enter(boundary)
                const encoded = await encode.apply(options.request, args)
                if (position === 'after') await round.enter(boundary)
                return encoded
              },
            }
            return original.call(this, { ...options, request })
          }
      )
    const stageHold = (name, position) =>
      this.patch(
        Connection.prototype,
        symbolOf(Connection.prototype, name),
        (original) =>
          async function (...args) {
            const { request, establishedOnly } = args[0]
            const selected =
              round.armed &&
              dedicated(this) &&
              [71, 72].includes(request.apiKey) &&
              kindOf(request.apiKey, establishedOnly ?? closing()) === mode
            if (selected && position === 'before') await round.enter(boundary)
            const result = await original.apply(this, args)
            if (result?.answered) result.answered.catch(() => {})
            if (selected && position === 'after') await round.enter(boundary)
            return result
          }
      )
    const brokerHold = () =>
      this.patch(
        Broker.prototype,
        'sendTelemetry',
        (original) =>
          async function (apiKey, definition, payload, options) {
            const selected =
              round.armed &&
              kindOf(apiKey, payload.terminating) === mode &&
              dedicated(this.connectionPool.pool[0])
            const result = await original.call(this, apiKey, definition, payload, options)
            if (selected) await round.enter(boundary)
            return result
          }
      )
    const payloadHold = (position) =>
      this.patch(
        TelemetryPayload.prototype,
        'buildPayload',
        (original) =>
          async function (snapshot, options) {
            const selected =
              round.armed && round.machine?.payload === this && closing() === (mode === 'final')
            if (selected && position === 'before') await round.enter(boundary)
            const result = await original.call(this, snapshot, options)
            if (selected && position === 'after') await round.enter(boundary)
            return result
          }
      )
    const dispatchHold = (apiKeys) => {
      this.patch(
        RequestQueue.prototype,
        'sendSocketRequest',
        (original) =>
          function (request) {
            const connection = round.dedicatedConnection
            const selected =
              round.armed &&
              connection?.requestQueue === this &&
              apiKeys.includes(request.entry.apiKey) &&
              (![71, 72].includes(request.entry.apiKey) ||
                kindOf(request.entry.apiKey, closing()) === mode)
            if (!selected) return original.call(this, request)
            const continuation = round.enter(boundary).then(() => original.call(this, request))
            round.trackWork(continuation.catch((error) => request.entry.reject(error)))
          }
      )
    }
    const responseHold = (apiKey, stage) => {
      this.patch(
        Connection.prototype,
        'send',
        (original) =>
          function (options) {
            if (!round.armed || !dedicated(this) || options.request.apiKey !== apiKey)
              return original.call(this, options)
            const response = options.response
            return original.call(this, {
              ...options,
              response: {
                ...response,
                [stage]: async (...args) => {
                  const result = await response[stage](...args)
                  await round.enter(boundary)
                  return result
                },
              },
            })
          }
      )
    }
    const traced = () => {
      const gate = this.trace.hold({ boundary })
      this.gates.push(gate)
      return gate
    }
    const wired = (selector) => {
      const gate = this.wireSchedule.hold(selector)
      this.gates.push(gate)
      return gate
    }
    const holds = {
      S01: traced,
      S02: () => new DialGate(this),
      S03: traced,
      S04: () => {
        encodeHold([17], 'before')
        return traced()
      },
      S05: () => {
        dispatchHold([17])
        return traced()
      },
      S06: () => {
        encodeHold([36], 'before')
        return traced()
      },
      S07: () => {
        dispatchHold([36])
        return traced()
      },
      S08: () => {
        if (position !== 'before') {
          responseHold(18, position)
          return traced()
        }
        return wired({
          api: 'ApiVersions',
          match: (call) => round.armed && dedicatedCall(call),
        })
      },
      S09: () => {
        if (position !== 'before') {
          responseHold(71, position)
          return traced()
        }
        return wired({ api: 'GetTelemetrySubscriptions', match: () => round.armed })
      },
      S10: () => {
        payloadHold('before')
        return traced()
      },
      S11: () => {
        payloadHold('after')
        return traced()
      },
      S12: () => {
        const codec =
          this.codecType === 4 ? require('../src/protocol/message/compression/zstd') : gzip
        this.patch(codec, 'compress', (original) => async (...args) => {
          const selected = round.armed && closing() === (mode === 'final')
          // Only bounded capture keeps the production input; default callers retain their byte copy.
          const job = {
            ...(round.bounded ? { input: args[0].buffer } : { raw: Buffer.from(args[0].buffer) }),
            promise: original(...args),
            settled: false,
          }
          // Observe rejection immediately, while retaining the actual native result.
          const outcome = job.promise.then(
            (value) => ({ value }),
            (error) => ({ error })
          )
          if (selected) {
            round.codecJobs.push(job)
            await round.enter(boundary)
          }
          const result = await outcome
          job.settled = true
          if (round.bounded) job.input = null
          if ('error' in result) throw result.error
          return result.value
        })
        return traced()
      },
      S13: () => {
        encodeHold([71, 72], 'before', true)
        return traced()
      },
      S14: () => {
        stageHold('private:Connection:enqueue', position)
        return traced()
      },
      S15: () => {
        if (mode === 'final')
          return wired({
            api: 'PushTelemetry',
            match: (call) => round.armed && !call.fields.terminating,
          })
        const blocker = wired({
          api: 'ApiVersions',
          match: (call) => round.blockingDispatch === true && dedicatedCall(call),
        })
        this.patch(
          Connection.prototype,
          'send',
          (original) =>
            async function (options) {
              const api = options.request.apiKey
              if (
                round.armed &&
                dedicated(this) &&
                [71, 72].includes(api) &&
                kindOf(api, options.establishedOnly) === mode &&
                !round.blockingDispatch
              ) {
                round.blockingDispatch = true
                const definition = require('../src/protocol/requests/apiVersions').protocol({
                  version: round.wire.advertised.get('ApiVersions')[0],
                })()
                const pending = original.call(this, { ...definition, guard: options.guard })
                round.pending.push(pending.catch(() => {}))
                await round.wait(() => blocker.arrived())
              }
              return original.call(this, options)
            }
        )
        return {
          get settle() {
            return blocker.settle
          },
          arrived: async () => {
            await round.wait(() => blocker.arrived())
            await round.until(() =>
              round.dedicatedConnection.requestQueue.pending.some(
                (request) => request.entry.apiKey === (mode === 'get' ? 71 : 72)
              )
            )
          },
          release: (value) => blocker.release(value),
        }
      },
      S16: () => {
        dispatchHold([71, 72])
        return traced()
      },
      S17: () => {
        responseHold(mode === 'get' ? 71 : 72, position === 'decode' ? 'decode' : 'parse')
        return traced()
      },
      S18: () => {
        if (position === 'result') {
          brokerHold()
          return traced()
        }
        if (retirementResultOnly) return traced()
        this.retirementAnswer = wired({
          api: mode === 'get' ? 'GetTelemetrySubscriptions' : 'PushTelemetry',
          match: (call) =>
            round.armed && (mode === 'get' || call.fields.terminating === (mode === 'final')),
        })
        return traced()
      },
      S19a: () => {
        encodeHold([17], 'before')
        return traced()
      },
      S19b: traced,
      S19c: () => {
        encodeHold([36], 'before')
        return traced()
      },
      S19d: () => {
        responseHold(36, 'decode')
        return traced()
      },
      S19e: () => {
        responseHold(36, 'parse')
        return traced()
      },
      S19f: traced,
    }
    if (!holds[boundary]) throw new Error(`Unknown telemetry boundary ${boundary}`)
    const gate = holds[boundary]()
    this.watchWork(TelemetryPayload.prototype, 'buildPayload')
    this.watchWork(Connection.prototype, 'send', dedicated)
    this.watchWork(Broker.prototype, 'sendTelemetry')
    this.held = { boundary, mode, gate, position }
    if (!this.gates.includes(gate)) this.gates.push(gate)
    return gate
  }

  /**
   * Wraps every telemetry transport the real factory creates so setup, forced retirement and the
   * S01/S18 holds are observed at the real `open` and `retire` calls. Every `open` call, admitted
   * or refused, is counted in `opensAttempted` in every capture mode.
   * @returns {void}
   */
  watchTransport() {
    const Cluster = require('../src/cluster')
    const round = this
    this.patch(
      Cluster.prototype,
      'createTelemetryTransport',
      (original) =>
        function (clock) {
          const transport = original.call(this, clock)
          const open = transport.open
          const candidates = transport.candidates
          transport.candidates = () => {
            round.setupCalls.push('candidates')
            return candidates()
          }
          transport.open = async (candidate, options) => {
            round.opensAttempted += 1
            round.setupCalls.push('open')
            const selected = round.armed && round.held?.boundary === 'S01'
            if (selected && round.held.position === 'before') await round.enter('S01')
            let allocation = null
            const result = await open(candidate, {
              ...options,
              onAllocated: (handle) => {
                round.builtHandles.push(handle)
                round.handlesIssued += 1
                const retire = handle.retire
                let heldRetirement = false
                handle.retire = async (...args) => {
                  if (round.bounded) round.retiredCount += 1
                  else round.retirements.push(handle)
                  const selected =
                    round.armed &&
                    round.held?.boundary === 'S18' &&
                    round.held.position !== 'result' &&
                    !heldRetirement
                  if (selected) heldRetirement = true
                  const result = await retire(...args)
                  if (round.bounded) {
                    round.retiredHandles.add(handle)
                    const at = round.builtHandles.indexOf(handle)
                    if (at !== -1) round.builtHandles.splice(at, 1)
                  }
                  if (selected) await round.enter('S18')
                  return result
                }
                round.watchWorkFor(handle, 'retire', () => round.retiredHandles.has(handle))
                // Retain the real build while its caller's publication callback is held.
                if (selected && round.held.position === 'after') allocation = handle
                else options.onAllocated(handle)
              },
            })
            if (selected && round.held.position === 'after') {
              await round.enter('S01')
              options.onAllocated(allocation)
            }
            return result
          }
          round.watchWork(transport, 'open')
          return transport
        }
    )
  }

  /** Observes socket allocation before a dial can be held, including repeated attempts. A bounded
   * round deduplicates by weak identity and keeps the connection's restoration in the live-owner
   * table only until its socket has closed.
   * @param {object} connection actual private Connection @returns {void} never changes socket identity
   * @throws {Error} when a bounded round already has its live owners; no accessor is then installed
   */
  watchSocket(connection) {
    if (this.bounded) {
      if (this.owners.has(connection)) return
    } else {
      this.socketOwners ??= new Set()
      if (this.socketOwners.has(connection)) return
      this.socketOwners.add(connection)
    }
    this.supplierOwners ??= new WeakSet()
    const supplier = connection.authenticationOwner
    if (supplier && !this.supplierOwners.has(supplier)) {
      this.supplierOwners.add(supplier)
      this.watchWork(supplier, 'run')
    }
    const descriptor = Object.getOwnPropertyDescriptor(connection, 'socket')
    let current = connection.socket
    let hadSocket = Boolean(current)
    const round = this
    const restore = () => {
      if (!descriptor && current === undefined) {
        delete connection.socket
        return
      }
      Object.defineProperty(connection, 'socket', {
        ...(descriptor ?? { writable: true, enumerable: true, configurable: true }),
        value: current,
      })
    }
    if (this.bounded)
      this.owners.add(connection, restore, () => {
        const raw = current?.inner ?? current
        return hadSocket && (!raw || raw.destroyed === true)
      })
    else this.restores.push(restore)
    Object.defineProperty(connection, 'socket', {
      configurable: true,
      enumerable: true,
      get() {
        return current
      },
      set(socket) {
        current = socket
        const raw = socket?.inner ?? socket
        if (raw) hadSocket = true
        round.trackSocket(raw)
      },
    })
  }

  /**
   * Installs the passive observers: the dedicated connection, request guards, the final prewrite
   * hook, and every actual socket write with the machine's issued identities.
   * @returns {void}
   */
  observe() {
    const Connection = require('../src/network/connection')
    const Broker = require('../src/broker')
    const round = this
    this.watchWork(this.machine, 'run')
    this.watchWork(this.machine, 'close')
    this.watchWork(require('../src/telemetry/payload').prototype, 'buildPayload')
    this.watchWork(
      this.wireSchedule,
      'run',
      (_receiver, [call]) =>
        ['GetTelemetrySubscriptions', 'PushTelemetry'].includes(call.api) ||
        (['ApiVersions', 'SaslHandshake', 'SaslAuthenticate'].includes(call.api) &&
          call.generation > round.foregroundSockets)
    )
    this.patch(
      Connection.prototype,
      symbolOf(Connection.prototype, 'private:Connection:prepare'),
      (original) =>
        async function (...args) {
          const encoded = await original.apply(this, args)
          const issued = encoded.request[ISSUED_REQUEST]
          if (issued && !round.bounded) {
            const bytes = encoded.requestPayload.buffer
            const clientLength = bytes.readInt16BE(12)
            const bodyAt = 15 + Math.max(0, clientLength)
            round.expectedFrames.push({
              ...issued,
              correlation: encoded.correlationId,
              connection: this.socket?.inner ?? this.socket,
              hex: Buffer.concat([bytes.subarray(0, bodyAt), round.requestBody(issued)]).toString(
                'hex'
              ),
            })
          }
          return encoded
        }
    )
    this.patch(
      Connection.prototype,
      'connect',
      (original) =>
        function (...args) {
          if (this.authenticationOwner) round.watchSocket(this)
          const result = original.apply(this, args)
          if (this.authenticationOwner) {
            round.dedicatedConnection = this
            round.trackSocket(this.socket?.inner ?? this.socket)
          }
          return result
        }
    )
    this.patch(
      Connection.prototype,
      'send',
      (original) =>
        function (options) {
          if (this.authenticationOwner) {
            round.trackSocket(this.socket?.inner ?? this.socket)
            round.dedicatedConnection = this
            if (this.socket) round.watchWrites(this.socket)
          }
          if (this.authenticationOwner && [71, 72].includes(options.request.apiKey)) {
            const issued = round.admittedRequest
            telemetryAssert.ok(issued, 'telemetry request must have an issuing owner')
            options = { ...options, request: { ...options.request, [ISSUED_REQUEST]: issued } }
          }
          return original.call(this, options)
        }
    )
    this.patch(
      Broker.prototype,
      'sendTelemetry',
      (original) =>
        function (apiKey, definition, payload, options) {
          const machine = round.machine
          const final = payload.terminating === true
          const owner = final ? machine.closing : machine.running
          round.admittedRequest = {
            apiKey,
            lifecycle: final ? machine.lifecycle : owner.lifecycle,
            transport: final ? machine.transportRevision : owner.transport,
            subscription: owner.subscription?.revision ?? 0n,
            request: owner.revision,
            closeRevision: final ? owner.revision : 0n,
            ...(round.bounded
              ? {}
              : {
                  payload: {
                    ...payload,
                    ...(payload.metrics ? { metrics: Buffer.from(payload.metrics) } : {}),
                  },
                  points: (final ? round.finalSnapshotIdentity() : owner.snapshot)?.points.map(
                    (point) => ({
                      name: point.name,
                      value: point.value,
                    })
                  ),
                }),
          }
          if (apiKey === 72 && this.connectionPool.pool[0].authenticationOwner != null) {
            if (!payload.terminating) round.guards.ordinary = options.guard
            else {
              round.guards.closing = options.guard
              if (round.finalHook) {
                const hook = round.finalHook
                const onWrite = options.onWrite
                round.finalHook = null
                options = {
                  ...options,
                  onWrite: () => {
                    hook()
                    return onWrite()
                  },
                }
              }
            }
          }
          return original.call(this, apiKey, definition, payload, options)
        }
    )
    this.frameFields = []
    this.patch(this.wire, 'handle', (original) => (socket, node, generation, frame) => {
      const result = original.call(this.wire, socket, node, generation, frame)
      const apiKey = frame.readInt16BE(4)
      if (this.machine && (apiKey === 71 || apiKey === 72)) {
        const machine = this.machine
        this.trace.write({
          connection: BigInt(this.socketNumber(socket)),
          attempt: BigInt(generation),
          lifecycle: machine.lifecycle,
          transport: machine.transportRevision,
          subscription: apiKey === 71 ? 0n : (machine.subscription?.revision ?? 0n),
          request:
            apiKey === 72 && this.wire.log.at(-1).fields.terminating
              ? (machine.closing?.revision ?? 0n)
              : (machine.running?.revision ?? 0n),
          closeRevision:
            apiKey === 72 && this.wire.log.at(-1).fields.terminating
              ? (machine.closing?.revision ?? 0n)
              : 0n,
          correlation: frame.readInt32BE(8),
          apiKey,
          ...(this.bounded ? { bytes: frame } : { hex: frame.toString('hex') }),
        })
        if (!this.bounded) this.frameFields.push(this.wire.log.at(-1).fields)
      }
      return result
    })
    const metrics = this.metrics
    this.patch(metrics, 'snapshot', (original) => (subscription, resources) => {
      const snapshot = original.call(metrics, subscription, resources)
      if (this.bounded) {
        this.snapshotOrdinals.set(snapshot, ++this.snapshotCount)
        const named = new Set()
        for (const point of snapshot?.points ?? [])
          if (!named.has(point.name)) {
            named.add(point.name)
            this.revisions.set(point.name, point.identity.revision)
          }
      } else this.snapshots.push({ snapshot, closing: this.machine.closing != null })
      return snapshot
    })
    this.patch(metrics, 'written', (original) => (snapshot, included) => {
      if (this.bounded) this.writtenCount += 1
      else this.written.push({ snapshot, included })
      return original.call(metrics, snapshot, included)
    })
    this.patch(metrics, 'abandon', (original) => (snapshot) => {
      if (this.bounded) {
        this.abandonedCount += 1
        this.abandonOrdinals.push(this.snapshotOrdinals.get(snapshot) ?? 0)
        if (this.abandonOrdinals.length > BOUNDED_LIMITS.abandoned) this.abandonOrdinals.shift()
      } else this.abandoned.push(snapshot)
      return original.call(metrics, snapshot)
    })
  }

  /** Adds the inherited frame-aware chaos wrapper around every subsequently connected socket.
   * @returns {void} stores wrappers separately from the strict wire's physical sockets
   */
  useChaos() {
    const { ChaosSocket } = require('./chaosSocket')
    this.chaosSockets = []
    this.patch(this.wire, 'connect', (original) => (host, port, onConnect) => {
      const inner = original.call(this.wire, host, port, onConnect)
      const socket = new ChaosSocket(inner, { host, port }, () => 0.5)
      this.chaosSockets.push(socket)
      return socket
    })
  }

  /**
   * Creates the actual public client of the round's kind over a strict scripted wire, registers the
   * numeric points, connects and runs it to foreground readiness, then optionally drives telemetry
   * to a named boundary. It returns once that boundary is admitted, never awaiting a held call.
   * @param {{subscription?:object,sasl?:boolean,points?:'small'|'large',
   *   drive?:'connected'|'get'|'normal'|'final',kafka?:object,client?:object,
   *   beforeConnect?:(round:PublicTelemetryRound)=>void}} [options] start mode; `beforeConnect` runs
   *   once the collector exists and before the first foreground dial
   * @returns {Promise<PublicTelemetryRound>} this round
   * @throws {Error} when the actual client cannot reach its readiness boundary
   */
  async start({
    subscription = DEFAULT_SUBSCRIPTION,
    sasl = false,
    points = 'small',
    drive = 'connected',
    kafka = {},
    client = {},
    beforeConnect = null,
  } = {}) {
    const { Kafka } = require('../index')
    const { TelemetryFactory } = require('../src/telemetry/factory')
    const scripted = require('./scriptedCluster')
    const round = this
    this.subscription = { ...DEFAULT_SUBSCRIPTION, ...subscription }
    this.installClocks()
    this.patch(
      TelemetryFactory.prototype,
      'bind',
      (original) =>
        function (cluster) {
          round.cluster = cluster
          round.bindings.push(this)
          if (this.config.enableMetricsPush) this.clock = round.clock
          return original.call(this, cluster)
        }
    )
    this.watchTransport()
    const crypto = require('node:crypto')
    this.patch(
      crypto,
      'randomBytes',
      (original) =>
        (size, ...args) =>
          size === 16 && args.length === 0
            ? Buffer.from('0102030405060708090a0b0c0d0e0f10', 'hex')
            : original(size, ...args)
    )
    this.provider = async () => {
      const hold = this.armed ? this.held?.boundary : null
      if (hold === 'S03' || hold === 'S19b') await this.enter(hold)
      const token = { value: 'telemetry-token' }
      if (hold === 'S19f') await this.enter(hold)
      return token
    }
    this.watchWork(this, 'provider')
    const advertise = {
      GetTelemetrySubscriptions: [0, 0],
      PushTelemetry: [0, 0],
      ...(sasl ? { SaslHandshake: [1, 1], SaslAuthenticate: [2, 2] } : {}),
    }
    const kafkaOptions = {
      enableMetricsPush: this.enabled,
      enforceRequestTimeout: this.enforceRequestTimeout,
      ...(sasl
        ? {
            sasl: {
              mechanism: 'oauthbearer',
              oauthBearerProvider: (...args) => this.provider(...args),
            },
          }
        : {}),
      ...kafka,
    }
    if (this.kind === 'shareConsumer') {
      const { ShareRunner } = require('../src/consumer/share/shareRunner')
      const { ShareRun } = require('../src/consumer/share/shareRun')
      // Empty-fetch pacing must advance independently of deterministic telemetry time.
      const foregroundNow = () => Number(realMonotonicNs()) / 1000000
      this.patch(
        ShareRun.prototype,
        'createLoop',
        (original) =>
          function (node, ...args) {
            const loop = original.call(this, node, ...args)
            const done = () => node.session.closed
            round.patchFor(loop, 'now', () => foregroundNow, done)
            round.patchFor(node.operation, 'now', () => foregroundNow, done)
            return loop
          }
      )
      this.patch(
        ShareRunner.prototype,
        'api',
        (original) =>
          function (...args) {
            round.runner = this
            return original.apply(this, args)
          }
      )
      const { shareClient } = require('./shareHarness')
      const harness = shareClient({
        advertise,
        kafka: kafkaOptions,
        share: { groupId: 'telemetry-g', maxWaitTimeInMs: 5, ...client },
        profile: 'share',
      })
      this.harness = harness
      this.wire = harness.cluster
      this.wire.schedule = this.wireSchedule
      this.kafka = harness.client
      this.client = harness.consumer
    } else {
      const base =
        this.kind === 'consumer' && this.protocol === 'consumer'
          ? 'consumer'
          : this.kind === 'producer' && client.transactionalId
            ? 'tv2'
            : 'default'
      this.wire = scripted.createScriptedCluster({
        nodes: [{ id: 1, host: 'a', port: 9092 }],
        profile: base,
        schedule: this.wireSchedule,
        advertise,
      })
      this.kafka = new Kafka(this.wire.kafkaOptions(kafkaOptions))
    }
    this.node = this.wire.nodes[0].id
    if (this.bounded) this.wire.bound()
    // Vector throttle examples cannot elapse while the round's wall clock is frozen.
    this.patch(this.wire, 'defaultAnswer', (original) => (...args) => {
      const unthrottled = (answer) =>
        Buffer.isBuffer(answer) ? answer : { throttleTime: 0, ...answer }
      const answer = original.apply(this.wire, args)
      return answer?.then ? answer.then(unthrottled) : unthrottled(answer)
    })
    this.watchWire()
    this.wire.answer('GetTelemetrySubscriptions', (fields) => ({
      ...this.subscription,
      clientInstanceId: fields.clientInstanceId ?? this.subscription.clientInstanceId,
    }))
    this.wire.answer('PushTelemetry', () => ({}))
    if (sasl) this.wire.answer('SaslHandshake', () => ({ enabledMechanisms: ['OAUTHBEARER'] }))
    if (this.kind !== 'shareConsumer') await this.wait(() => this.createClient(client))
    if (this.kind === 'producer' || this.kind === 'admin') scripted.producerScript(this.wire)
    if (this.kind === 'producer' && client.transactionalId)
      this.wire.answer('InitProducerId', ({ producerId, producerEpoch }) => ({
        producerId: producerId === '-1' ? '1000' : producerId,
        producerEpoch: producerEpoch + 1,
      }))
    this.binding = this.bindings.at(-1) ?? null
    this.machine = this.binding?.machine ?? null
    this.metrics = this.binding?.metrics ?? null
    if (this.machine) {
      this.registerPoints(points)
      this.observe()
    }
    beforeConnect?.(this)
    await this.wait(() => this.trackWork(this.connectForeground()))
    if (this.coreProof) await this.wait(() => this.prepareCoreProof())
    this.foregroundSockets = this.wire.sockets().length
    if (!this.machine || drive === 'connected') return this
    if (drive === 'get') {
      this.arm()
      this.tick()
      if (this.held?.boundary === 'S18' && this.held.position !== 'result')
        await this.expireForRetirement()
      return this
    }
    await this.ready()
    if (this.held?.boundary.startsWith('S19')) this.armRenewal()
    this.arm()
    if (drive === 'normal') this.tick()
    else {
      if (this.held?.boundary === 'S15') await this.occupyFinalQueue()
      this.closePromise = this.beginClose(this.closeOptions(1000))
      if (this.held?.boundary === 'S15')
        await this.until(() =>
          this.dedicatedConnection.requestQueue.pending.some(
            (request) => request.entry.apiKey === 72
          )
        )
    }
    if (this.held?.boundary === 'S18' && this.held.position !== 'result')
      await this.expireForRetirement()
    return this
  }

  /** Starts one real ordinary Push whose response keeps the sole queue slot occupied at close.
   * @returns {Promise<void>} the response hold and its exact request have arrived
   */
  async occupyFinalQueue() {
    const count = this.pushesRequested()
    this.tick()
    await this.until(() => this.pushesRequested() > count)
    await this.wait(() => this.held.gate.arrived())
  }

  /** Expires a written operation so background retirement can remain held after public close.
   * The raw answer is independently retained; repeated retire calls still join actual pool cleanup.
   * @returns {Promise<void>} the retirement result hold has arrived
   */
  async expireForRetirement() {
    await this.wait(() => this.retirementAnswer.arrived())
    const machine = this.machine
    const id = machine.closing ? this.closeTimer : machine.watchdog
    const entry = this.clock.pending.get(id)
    this.clock.jump(entry.due)
    this.clock.deliver(id)
    await this.wait(() => this.held.gate.arrived())
    if (!machine.closing) await this.until(() => machine.timer !== null && !machine.running)
  }

  /**
   * Builds the actual non-share client of the round's kind.
   * @param {object} options factory options layered over the kind's defaults @returns {Promise<void>}
   */
  async createClient(options) {
    const kafka = this.kafka
    if (this.kind === 'producer') this.client = kafka.producer({ idempotent: false, ...options })
    else if (this.kind === 'admin') this.client = kafka.admin(options)
    else {
      const { prepareRecordBatches } = require('../src/consumer/__tests__/fixtures/fetchHarness')
      const { cooperativeSticky } = require('../src/consumer/assigners')
      const scripted = require('./scriptedCluster')
      await prepareRecordBatches()
      const script =
        this.protocol === 'consumer' ? scripted.consumerProtocolScript : scripted.groupScript
      script(this.wire, { batches: 1, brokerAnswerTimers: this.brokerAnswerTimers })
      this.client = kafka.consumer({
        ...(this.membership === 'no-group' ? {} : { groupId: 'telemetry-g' }),
        maxWaitTimeInMs: 20,
        groupInstanceId: this.membership === 'static' ? 'static-instance' : null,
        ...(this.protocol === 'consumer'
          ? { groupProtocol: 'consumer' }
          : {
              heartbeatInterval: 100,
              partitionAssigners:
                this.protocol === 'classic-cooperative' ? [cooperativeSticky] : undefined,
            }),
        ...options,
      })
    }
  }

  /**
   * Registers the public numeric points: a Sum at 11n and a Gauge at 0, plus enough legal gauges
   * to exceed the compression floor when requested.
   * @param {'small'|'large'} points @returns {void}
   */
  registerPoints(points) {
    this.client.registerMetricForSubscription({
      name: 'com.example.total',
      type: 'sum',
      value: 11n,
    })
    this.client.registerMetricForSubscription({
      name: 'com.example.depth',
      type: 'gauge',
      value: 0,
    })
    if (points === 'large')
      for (let index = 0; index < BULK_POINTS; index++)
        this.client.registerMetricForSubscription({
          name: `com.example.bulk.${'x'.repeat(48)}.${index}`,
          type: 'gauge',
          value: index,
        })
  }

  /**
   * Connects the public client and, for a consumer or share consumer, runs it until its actual
   * membership exists.
   * @returns {Promise<void>}
   */
  async connectForeground() {
    if (this.kind === 'producer' || this.kind === 'admin') {
      await this.client.connect()
      await this.cluster.refreshMetadata()
      return
    }
    if (this.kind === 'shareConsumer') {
      await this.client.subscribe({ topics: ['topic-a'] })
      await this.client.run({
        eachMessage: async (payload) => {
          this.core.handlers.push(payload.message.offset)
        },
        ...this.runOptions,
      })
      await this.until(() => this.runner?.current?.membership.memberEpoch > 0)
      return this.until(() => this.wire.tally('ShareFetch', 'answered') > 0)
    }
    const { TOPIC } = require('../src/consumer/__tests__/fixtures/fetchHarness')
    const Runner = require('../src/consumer/runner')
    const round = this
    this.patch(
      Runner.prototype,
      'start',
      (original) =>
        function (...args) {
          round.runner = this
          return original.apply(this, args)
        }
    )
    this.patch(
      Runner.prototype,
      'processEachMessage',
      (original) =>
        function (...args) {
          round.foregroundReady = true
          return original.apply(this, args)
        }
    )
    await this.client.connect()
    if (this.protocol === 'manual')
      await this.client.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
    else await this.client.subscribe({ topic: TOPIC, fromBeginning: true })
    this.core.crashes = 0
    this.eventRemovers.push(this.client.on(this.client.events.CRASH, () => this.core.crashes++))
    this.eventRemovers.push(
      this.client.on(this.client.events.PARTITIONS_LOST, () =>
        this.core.losses.push(this.wire.requests().length)
      )
    )
    this.eventRemovers.push(
      this.client.on(this.client.events.GROUP_JOIN, () =>
        this.core.joins.push(this.wire.requests().length)
      )
    )
    await this.client.run({
      autoCommit: false,
      eachMessage: async (payload) => {
        this.core.handlers.push(payload.message.offset)
      },
      ...this.runOptions,
    })
    await this.until(() => this.foregroundReady)
  }

  /** @returns {void} activates the hold and, for S02, blackholes the next dial */
  arm() {
    this.armed = true
    if (this.held?.gate instanceof DialGate) this.wire.blackhole(this.node)
  }

  /**
   * Makes the dedicated connection owe one reauthentication, so the actual ordinary request that
   * runs next renews through the real SASL path.
   * @returns {void}
   */
  armRenewal() {
    const Connection = require('../src/network/connection')
    const connection = this.dedicatedConnection
    const stamp = connection.authenticatedAt
    this.patch(
      Connection.prototype,
      symbolOf(Connection.prototype, 'private:Connection:shouldReauthenticate'),
      (original) =>
        function () {
          return (this === connection && this.authenticatedAt === stamp) || original.call(this)
        }
    )
  }

  /** @returns {Array<object>} ordinary Push records, oldest first */
  pushes() {
    this.assertFull('pushes')
    return this.wire.requests().filter((r) => r.api === 'PushTelemetry' && !r.fields.terminating)
  }

  /**
   * Delivers the machine's current ordinary timer, moving time to its due point first.
   * @returns {void} @throws {Error} when no ordinary timer is pending
   */
  tick() {
    const id = this.machine.timer
    const entry = this.clock.pending.get(id)
    if (!entry) throw new Error('No ordinary telemetry timer')
    this.clock.jump(Math.max(this.clock.time, entry.due))
    this.clock.deliver(id)
  }

  /**
   * Completes original readiness: one accepted Get, a stable UUID and one answered ordinary Push.
   * @returns {Promise<void>}
   */
  async ready() {
    const machine = this.machine
    telemetryAssert.ok(machine, 'Enabled round must construct its telemetry machine')
    if (!machine.subscription) {
      this.tick()
      await this.until(() => {
        telemetryAssert.notEqual(machine.state, 'STOPPED', 'Initial telemetry Get must be accepted')
        return machine.subscription !== null
      })
    }
    const answered = () => this.pushesAnswered()
    const before = answered()
    this.tick()
    await this.until(() => answered() > before && !machine.running)
  }

  /**
   * Reads actual machine, wire and collector state without sampling or mutating anything.
   * @returns {Readonly<object>} issued identities, counts and captured objects at this instant
   */
  capture() {
    const machine = this.machine
    return Object.freeze({
      lifecycle: machine.lifecycle,
      uuid: machine.uuid,
      resourceRevision: this.binding.resourceRevision,
      payloadOwner: machine.payload,
      subscriptionOwner: machine.subscription,
      runningOwner: machine.running,
      waiterOwners: [...machine.waiters],
      transport: machine.transportRevision,
      subscription: machine.subscription?.revision ?? 0n,
      running: machine.running?.revision ?? 0n,
      timerRevision: machine.timerRevision,
      closeRevision: machine.closing?.revision ?? 0n,
      closeDeadline: machine.closing?.deadline,
      closeEntry: this.closeEntry,
      reopen: machine.reopen ? { ...machine.reopen } : null,
      sequence: machine.sequence,
      handle: machine.closing?.handle ?? machine.handle,
      telemetryHandleIdentity: machine.handle,
      dedicated: this.dedicatedCount(),
      frames: this.trace.count,
      requests: this.bounded ? this.wire.requestCount() : this.wire.requests().length,
      setupCalls: this.setupCalls.length,
      written: this.bounded ? this.writtenCount : this.written.length,
      abandoned: this.bounded ? this.abandonedCount : this.abandoned.length,
      snapshots: this.bounded ? this.snapshotCount : this.snapshots.length,
      waiters: machine.waiters.size,
      timers: this.clock.pending.size,
      timer: machine.timer,
      watchdog: machine.watchdog,
      metricRevision: this.pointRevision('com.example.total'),
    })
  }

  /**
   * @param {string} name metric name @returns {bigint} identity revision of the latest sampled point
   *   of that name, or 0n before any sample
   */
  pointRevision(name) {
    if (this.bounded) return this.revisions.get(name) ?? 0n
    for (const { snapshot } of [...this.snapshots].reverse()) {
      const point = snapshot?.points.find((candidate) => candidate.name === name)
      if (point) return point.identity.revision
    }
    return 0n
  }

  /** @returns {number} destroys the newest dedicated socket with an OS-coded transport error */
  faultDedicated() {
    const socket = this.dedicatedConnection?.socket
    if (!socket) return 0
    socket.destroy(Object.assign(new Error('telemetry transport lost'), { code: 'ECONNRESET' }))
    return 1
  }

  /**
   * Runs one ordinary Get then one ordinary Push on the current transport to completion.
   * @returns {Promise<void>}
   */
  async establish() {
    const machine = this.machine
    const answered = (api) => this.wire.tally(api, 'answered')
    const gets = answered('GetTelemetrySubscriptions')
    this.tick()
    await this.until(() => answered('GetTelemetrySubscriptions') > gets && !machine.running)
    const pushes = answered('PushTelemetry')
    this.tick()
    await this.until(() => answered('PushTelemetry') > pushes && !machine.running)
  }

  /**
   * Executes the public or captured action of the stale-work table. Public close and reconnect are
   * the actual client methods; a transport replacement faults only the dedicated socket and lets
   * the machine's own watchdog and backoff retire and reopen it.
   * @param {string} name one of RELEASE_ACTIONS @param {{lateArrival?:boolean}} [options] cell options
   * @returns {Promise<void>} @throws {Error} for an unknown action
   */
  async transition(name, { lateArrival = false } = {}) {
    const machine = this.machine
    if (name === RELEASE_ACTIONS[0]) return
    if (name === RELEASE_ACTIONS[1]) {
      this.faultDedicated()
      if (machine.closing) return undefined
      const watchdog = machine.watchdog
      if (this.held?.boundary !== 'S18' || this.held.position === 'result')
        this.clock.jump(this.clock.time + 30000)
      if (watchdog && this.clock.pending.has(watchdog)) this.clock.deliver(watchdog)
      await this.until(() => machine.state === 'BACKOFF' && machine.timer !== null)
      if (this.held?.boundary === 'S12' || SASL_BOUNDARIES.has(this.held?.boundary)) {
        this.deferredReplacement = true
        return
      }
      if (this.held?.gate instanceof DialGate) this.wire.up(this.node)
      return this.establish()
    }
    if (name === RELEASE_ACTIONS[2] || name === RELEASE_ACTIONS[3]) {
      if (!this.closePromise)
        this.closePromise = this.beginClose(this.closeOptions(name === RELEASE_ACTIONS[2] ? 20 : 0))
      this.deferredClose = true
      return lateArrival ? this.until(() => this.held.gate.settle != null) : undefined
    }
    if (name === RELEASE_ACTIONS[4]) {
      this.clock.jump(machine.closing ? machine.closing.deadline + 1 : this.clock.time + 30001)
      return undefined
    }
    if (name === RELEASE_ACTIONS[5]) {
      if (machine.closing) {
        this.clock.jump(machine.closing.deadline + 1)
        this.deliverCloseWatchdog()
      } else this.closePromise = this.beginClose(this.closeOptions(0))
      await this.settleClose()
      if (this.held?.gate instanceof DialGate) this.wire.up(this.node)
      await this.client.connect()
      if (this.held?.boundary === 'S12' || SASL_BOUNDARIES.has(this.held?.boundary)) {
        this.deferredReplacement = true
        return
      }
      return this.establish()
    }
    throw new Error(`Unknown telemetry action ${name}`)
  }

  /** Replaces membership through the original runner, preserving broker-provided member strings.
   * @returns {Promise<void>} a fresh accepted member @throws {Error} for a manual run
   */
  async rejoin() {
    if (this.kind === 'shareConsumer') {
      const previous = this.runner.current
      await this.client.stop()
      await this.client.run({
        eachMessage: async (payload) => this.core.handlers.push(payload.message.offset),
      })
      telemetryAssert.notEqual(this.runner.current, previous)
      return
    }
    if (this.protocol === 'manual') throw new Error('Manual runs have no rejoin')
    await this.until(() => this.core.handlers.length === 1)
    const group = this.runner.consumerGroup
    const token = group.ownership.token()
    await this.runner.handleRebalanceError({
      kind: 'revoked',
      reason: 'assignment',
      error: new Error('assignment'),
    })
    telemetryAssert.notEqual(group.ownership.token(), token)
  }

  /** Admits a real public commit after the exact fixture record reached its handler.
   * @returns {Promise<void>} durable offset and handler evidence captured before telemetry revocation
   * @throws {Error} public commit or handler readiness failure
   */
  async prepareCoreProof() {
    await this.until(() => this.core.handlers.length === 1)
    telemetryAssert.deepStrictEqual(this.core.handlers, ['0'])
    this.core.ownership = this.runner.consumerGroup.ownership
    this.core.token = this.core.ownership.token()
    if (this.protocol !== 'manual') await this.until(() => this.core.joins.length > 0)
    this.core.joinsBefore = this.core.joins.length
    if (this.membership === 'no-group') {
      const result = await telemetryOutcome(() =>
        this.client.commitOffsets([{ topic: 'test-topic', partition: 0, offset: '1' }])
      )
      telemetryAssert.equal(result.kind, 'reject')
      return
    }
    this.wire.answer('OffsetCommit', ({ topics }) => {
      this.core.durable = topics.flatMap((topic) =>
        topic.partitions.map(({ partition, offset }) => ({ partition, offset }))
      )
      return {
        topics: topics.map((topic) => ({
          ...topic,
          partitions: topic.partitions.map(({ partition }) => ({ partition, errorCode: 0 })),
        })),
      }
    })
    await this.client.commitOffsets([{ topic: 'test-topic', partition: 0, offset: '1' }])
    this.core.commits.push([{ partition: 0, offset: '1' }])
  }

  /** Asserts original commit/leave and handler identity invariants independently of telemetry.
   * @param {number} from first core request index @param {string} event actual transition
   * @returns {void} @throws {import('node:assert').AssertionError} changed core behavior
   */
  assertCore(from, event) {
    const rows = this.wire.requests().slice(from)
    telemetryAssert.deepStrictEqual(
      rows
        .filter((r) => r.api === 'OffsetCommit')
        .map((row) =>
          row.fields.topics.flatMap((topic) =>
            topic.partitions.map(({ partition, offset }) => ({ partition, offset }))
          )
        ),
      this.core.commits
    )
    const leaves = rows.filter(
      (r) =>
        r.api === 'LeaveGroup' || (r.api === 'ConsumerGroupHeartbeat' && r.fields.memberEpoch < 0)
    )
    if (
      ['stop', 'disconnect'].includes(event) &&
      this.kind === 'consumer' &&
      this.protocol !== 'manual' &&
      (this.membership !== 'static' || this.protocol === 'consumer')
    )
      telemetryAssert.equal(leaves.length, 1)
    for (const fromLoss of this.core.losses)
      telemetryAssert.deepStrictEqual(
        this.wire
          .requests()
          .slice(
            fromLoss,
            this.core.joins.find((index) => index > fromLoss)
          )
          .filter((r) => r.api === 'OffsetCommit'),
        []
      )
    telemetryAssert.equal(new Set(this.core.handlers).size, this.core.handlers.length)
    if (this.coreProof) {
      telemetryAssert.deepStrictEqual(this.core.handlers, ['0'])
      if (this.membership !== 'no-group')
        telemetryAssert.deepStrictEqual(this.core.durable, [{ partition: 0, offset: '1' }])
      if (['crash', 'rejoin'].includes(event) && this.protocol !== 'manual') {
        telemetryAssert.ok(this.core.joins.length > this.core.joinsBefore, 'owed rejoin completed')
        telemetryAssert.equal(this.runner.running, true)
        const ownership = this.runner.consumerGroup.ownership
        telemetryAssert.ok(
          ownership !== this.core.ownership || ownership.token() !== this.core.token,
          'replacement membership must have a new issuing owner or token'
        )
        telemetryAssert.equal(ownership.refusal(ownership.token()), null)
      }
    }
  }

  /** Suspends through the actual crash owner, or closes a non-running public client.
   * @returns {Promise<void>} completes original suspension/close
   */
  async suspendForeground() {
    if (this.kind === 'consumer' || this.kind === 'shareConsumer') return this.crash()
    return this.client.disconnect(this.closeOptions(0))
  }

  /** Starts an actual replacement run, including its new membership and telemetry resume.
   * @returns {Promise<void>} original run readiness
   */
  async restartForeground() {
    if (this.kind === 'consumer') {
      await this.client.connect()
      await this.client.run({
        autoCommit: false,
        eachMessage: async ({ message }) => {
          this.core.handlers.push(message.offset)
        },
      })
      return
    }
    if (this.kind === 'shareConsumer') {
      await this.client.run({ eachMessage: async () => {} })
      return
    }
    await this.client.connect()
  }

  /** @returns {Promise<void>} signals the original crash path of the running consumer or share consumer */
  crash() {
    return crashRound(this)
  }

  /** @returns {Promise<void>} waits until the actual public close promise has settled */
  async settleClose() {
    await this.until(() => this.closeDone)
    return this.closePromise
  }

  /**
   * Synchronously invokes the actual public disconnect and observes its promise at once.
   * @param {{timeout?:number}|undefined} options inherited close options @returns {Promise<void>} the
   *   original close promise
   */
  beginClose(options) {
    const before = new Set(this.clock.pending.keys())
    this.closeEntry = this.clock.now()
    this.closeDone = false
    const promise = this.client.disconnect(options)
    this.closePromise = promise
    for (const id of this.clock.pending.keys()) if (!before.has(id)) this.closeTimer = id
    this.keepPending(
      promise.then(
        () => {
          this.closeDone = true
        },
        () => {
          this.closeDone = true
        }
      )
    )
    return promise
  }

  /**
   * @param {number} timeout milliseconds @returns {{timeout:number}|undefined} the inherited public
   *   option supported by each delivered public close owner
   */
  closeOptions(timeout) {
    return { timeout }
  }

  /** @returns {Error|null} the exact guard of the latest ordinary telemetry request, called now */
  savedOrdinaryGuard() {
    return this.guards.ordinary ? this.guards.ordinary() : null
  }

  /** @returns {Error|null} the exact guard of the captured final request, called now */
  savedClosingGuard() {
    return this.guards.closing ? this.guards.closing() : null
  }

  /** @returns {Promise<void>} delivers the recorded close timer once, when still pending and due */
  async deliverCloseWatchdog() {
    const entry = this.clock.pending.get(this.closeTimer)
    if (entry && entry.due <= this.clock.time) this.clock.deliver(this.closeTimer)
  }

  /** @returns {Array<object>} actual PushTelemetry records whose Terminating field is true */
  finalFrames() {
    return this.wire.requests().filter((r) => r.api === 'PushTelemetry' && r.fields.terminating)
  }

  /**
   * @param {object} record an earlier capture @returns {Array<string|number>} the real candidate,
   *   open, dial, Get, handshake and authenticate events since it
   */
  newSetupCallsSince(record) {
    this.assertFull('newSetupCallsSince')
    const wire = this.wire
      .requests()
      .slice(record.requests)
      .filter((r) => [17, 36, 71].includes(r.apiKey))
      .map((r) => r.api)
    const dials = Array.from(
      { length: Math.max(0, this.dedicatedCount() - record.dedicated) },
      () => 'dial'
    )
    return [...this.setupCalls.slice(record.setupCalls), ...wire, ...dials]
  }

  /** @returns {Array<object>} actual dedicated handles retired so far, forced and captured */
  forcedRetirements() {
    this.assertFull('forcedRetirements')
    return [...this.retirements]
  }

  /**
   * Runs a synchronous call immediately before the next final's original guard, commit and write.
   * @param {Function} call synchronous reentrant action @returns {void}
   */
  atFinalPrewrite(call) {
    this.finalHook = () => {
      this.reregisteredAt = this.clock.time
      this.snapshotsAtHook = this.bounded ? this.snapshotCount : this.snapshots.length
      call()
    }
  }

  /** @returns {object|undefined} the snapshot the final sampled, never a new sample */
  finalSnapshotIdentity() {
    this.assertFull('finalSnapshotIdentity')
    return this.snapshots.filter((entry) => entry.closing).at(-1)?.snapshot
  }

  /** @param {object} snapshot a captured snapshot @returns {number} commits recorded for it */
  writtenFor(snapshot) {
    this.assertFull('writtenFor')
    return this.written.filter((entry) => entry.snapshot === snapshot).length
  }

  /**
   * Independently decodes the newest ordinary Push with a protobuf reader of its own.
   * @param {{name:string}} identity metric identity
   * @returns {{value:bigint|number,origin:bigint|undefined,metricRevision:bigint}} the point
   * @throws {Error} when the point is absent
   */
  decodedLatestPoint({ name }) {
    const bytes = this.decodedMetrics(this.pushes().at(-1))
    const list = protoValues(protoValues(protoValues(bytes, 1)[0], 2)[0], 2)
    for (const metric of list) {
      if (protoValues(metric, 1)[0]?.toString() !== name) continue
      const body = protoValues(metric, 7)[0] ?? protoValues(metric, 5)[0]
      const point = protoValues(body, 1)[0]
      const integer = protoValues(point, 6)[0]
      return {
        value: integer ? integer.readBigInt64LE() : protoValues(point, 4)[0].readDoubleLE(),
        origin: protoValues(point, 2)[0]?.readBigUInt64LE(),
        metricRevision: this.pointRevision(name),
      }
    }
    throw new Error('Point absent')
  }

  /** Decompresses a real wire body with the platform decoder, independently of payload fitting.
   * @param {object} record decoded wire request @returns {Buffer} raw MetricsData bytes
   * @throws {Error} invalid compression data or a codec unavailable on this platform
   */
  decodedMetrics(record) {
    const bytes = Buffer.from(record.fields.metrics, 'hex')
    const zlib = require('node:zlib')
    if (record.fields.compressionType === 1) return zlib.gunzipSync(bytes)
    if (record.fields.compressionType === 4) {
      // eslint-disable-next-line n/no-unsupported-features/node-builtins -- codec 4 is emitted only when the platform supports it
      return zlib.zstdDecompressSync(bytes)
    }
    telemetryAssert.equal(record.fields.compressionType, 0)
    return bytes
  }

  /** Builds expected Kafka telemetry body bytes independently of the production request encoder.
   * @param {object} issued admitted body @returns {Buffer} complete body including terminal tags
   */
  requestBody({ apiKey, payload }) {
    const uuid = payload.clientInstanceId
      ? Buffer.from(payload.clientInstanceId.replace(/-/g, ''), 'hex')
      : Buffer.alloc(16)
    if (apiKey === 71) return Buffer.concat([uuid, Buffer.from([0])])
    const fixed = Buffer.alloc(6)
    fixed.writeInt32BE(payload.subscriptionId)
    fixed[4] = payload.terminating ? 1 : 0
    fixed.writeInt8(payload.compressionType, 5)
    const length = []
    let remaining = payload.metrics.length + 1
    do {
      const byte = remaining & 127
      remaining = Math.floor(remaining / 128)
      length.push(byte | (remaining ? 128 : 0))
    } while (remaining)
    return Buffer.concat([uuid, fixed, Buffer.from(length), payload.metrics, Buffer.from([0])])
  }

  /** Independently decodes every point in a complete, uncompressed MetricsData body.
   * @param {Buffer} bytes OTLP bytes @returns {Array<{name:string,value:number|bigint}>} wire order
   * @throws {Error} malformed OTLP body
   */
  decodePoints(bytes) {
    const result = []
    for (const resource of protoValues(bytes, 1))
      for (const scope of protoValues(resource, 2))
        for (const metric of protoValues(scope, 2)) {
          const name = protoValues(metric, 1)[0].toString()
          const body = protoValues(metric, 7)[0] ?? protoValues(metric, 5)[0]
          for (const point of protoValues(body, 1)) {
            const integer = protoValues(point, 6)[0]
            result.push({
              name,
              value: integer ? integer.readBigInt64LE() : protoValues(point, 4)[0].readDoubleLE(),
            })
          }
        }
    return result
  }

  /** @returns {bigint|undefined} start time the collector issued to the re-registered descriptor */
  replacementMetricOrigin() {
    this.assertFull('replacementMetricOrigin')
    return this.snapshots
      .slice(this.snapshotsAtHook ?? 0)
      .flatMap((entry) => entry.snapshot?.points ?? [])
      .find((point) => point.name === 'com.example.total')?.startTimeUnixNano
  }

  /**
   * Releases a gate and joins real operation/shell settlement before checking its final trace.
   * A deliberately unanswered response instead supplies its own write/settlement observation, so
   * delayed watchdog delivery remains under the caller's control.
   * @param {object} gate hold gate @param {*} [value] the captured original answer
   * @param {Function} [completion] scenario-specific bounded observation; defaults to owned settlement
   * @returns {Promise<void>} @throws {Error} when the selected work never completes
   */
  async drainReleased(gate, value, completion = () => this.settleWork()) {
    if (gate.settle) gate.release(value)
    if (this.retirementAnswer?.settle) this.retirementAnswer.release()
    await this.wait(completion)
    if (this.deferredClose) {
      this.deferredClose = false
      await this.settleClose()
    }
  }

  /**
   * Compares the complete ordered frame list with an independent manifest: api, flags, UUID,
   * subscription and dedicated socket. Every frame must match its pre-encode issued authority,
   * independently built full body, decoded point set and actual socket bytes.
   * @param {Array<object>} expected predeclared frames @returns {void}
   * @throws {import('node:assert').AssertionError} any missing, extra, reordered or substituted frame
   */
  assertTrace(expected) {
    this.assertFull('assertTrace')
    const frames = this.trace.frames
    const records = this.telemetryRequests()
    telemetryAssert.equal(records.length, frames.length)
    const project = (frame, fields) => ({
      api: frame.apiKey,
      terminating: frame.apiKey === 72 ? fields.terminating : null,
      uuid: fields.clientInstanceId ?? null,
      subscriptionId: frame.apiKey === 72 ? fields.subscriptionId : null,
      socket: frame.connection,
    })
    telemetryAssert.deepStrictEqual(
      frames.map((frame, index) => project(frame, this.frameFields[index])),
      expected.map(({ api, terminating, uuid, subscriptionId, socket }) => ({
        api,
        terminating,
        uuid,
        subscriptionId,
        socket,
      }))
    )
    frames.forEach((frame, index) => {
      telemetryAssert.equal(records[index].correlationId, frame.correlation)
      telemetryAssert.equal(BigInt(records[index].generation), frame.attempt)
      const issued = this.expectedFrames.find(
        (entry) =>
          entry.correlation === frame.correlation &&
          this.dedicatedSockets.indexOf(entry.connection) + 1 === Number(frame.connection)
      )
      telemetryAssert.ok(issued, 'frame must match an admitted request')
      for (const key of [
        'apiKey',
        'lifecycle',
        'transport',
        'subscription',
        'request',
        'closeRevision',
      ])
        telemetryAssert.equal(frame[key] ?? 0n, issued[key], key)
      telemetryAssert.equal(frame.hex, issued.hex, 'issued complete body and bytes')
      const fields = this.frameFields[index]
      telemetryAssert.equal(
        fields.clientInstanceId ?? null,
        issued.payload.clientInstanceId ?? null
      )
      if (frame.apiKey === 72) {
        for (const key of ['subscriptionId', 'terminating', 'compressionType'])
          telemetryAssert.equal(fields[key], issued.payload[key], key)
        telemetryAssert.equal(fields.metrics, issued.payload.metrics.toString('hex'))
        const raw = this.decodedMetrics(records[index])
        const order = (points) => [...points].sort((a, b) => a.name.localeCompare(b.name))
        telemetryAssert.deepStrictEqual(order(this.decodePoints(raw)), order(issued.points))
      }
      const write = this.socketWrites.find(
        (entry) =>
          (entry.socket.inner ?? entry.socket) === issued.connection &&
          entry.bytes.readInt32BE(8) === frame.correlation
      )
      telemetryAssert.equal(frame.hex, write?.bytes.toString('hex'), 'complete frame bytes')
      for (const key of ['lifecycle', 'transport', 'subscription', 'request', 'closeRevision'])
        if (expected[index][key] !== undefined)
          telemetryAssert.equal(frame[key], expected[index][key], key)
      if (expected[index].compressionType !== undefined)
        telemetryAssert.equal(
          frame.apiKey === 71 ? null : fields.compressionType,
          expected[index].compressionType
        )
    })
  }

  /**
   * Checks the replacement authority survived a late old completion: same issued tuple and
   * transport, no extra commit, and only samples taken before the replacement may be abandoned.
   * @param {object} before the replacement capture @returns {void}
   * @throws {import('node:assert').AssertionError} any change to the replacement
   */
  assertReplacementUnchanged(before) {
    const now = this.capture()
    for (const key of [
      'lifecycle',
      'uuid',
      'resourceRevision',
      'payloadOwner',
      'subscriptionOwner',
      'runningOwner',
      'transport',
      'subscription',
      'timerRevision',
      'running',
      'closeRevision',
      'timer',
      'watchdog',
      'waiters',
      'timers',
      'metricRevision',
    ])
      telemetryAssert.equal(now[key], before[key], key)
    telemetryAssert.deepStrictEqual(now.waiterOwners, before.waiterOwners)
    telemetryAssert.equal(now.handle, before.handle)
    telemetryAssert.deepStrictEqual(now.reopen, before.reopen)
    telemetryAssert.equal(now.written, before.written)
    if (this.bounded) {
      const since = this.abandonedCount - before.abandoned
      telemetryAssert.ok(
        since <= this.abandonOrdinals.length,
        'abandoned samples exceed the bounded record'
      )
      for (const ordinal of this.abandonOrdinals.slice(this.abandonOrdinals.length - since))
        telemetryAssert.ok(
          ordinal >= 1 && ordinal <= before.snapshots,
          'only an older sample may be abandoned late'
        )
      return
    }
    const owned = new Set(this.snapshots.slice(0, before.snapshots).map((entry) => entry.snapshot))
    for (const snapshot of this.abandoned.slice(before.abandoned))
      telemetryAssert.ok(owned.has(snapshot), 'only an older sample may be abandoned late')
  }

  /**
   * Runs one actual foreground original below the public method, with a unique outcome, and
   * records every invocation's receiver and arguments.
   * @param {{target:object,key:string|symbol,outcome?:'return'|'throw'|'reject',identity?:*,
   *   run:Function,tap?:boolean,select?:(receiver:object,args:Array)=>boolean}} options the original to control and the public call that reaches it
   * @returns {Promise<{actual:object,expected:object,log:Array<{receiver:*,args:Array}>}>} exact
   *   original boundary outcome and inherited public normalization, plus receiver/argument order
   * @throws {import('node:assert').AssertionError} an original was not invoked exactly once
   */
  async exerciseForeground({
    target,
    key,
    outcome = 'return',
    identity,
    run,
    tap = false,
    select = null,
  }) {
    const log = []
    let originalOutcome
    this.patch(
      target,
      key,
      (original) =>
        function (...args) {
          if (select && !select(this, args)) return original.apply(this, args)
          log.push({ receiver: this, args })
          if (tap) {
            const value = original.apply(this, args)
            if (value && typeof value.then === 'function')
              return value.then(
                (result) => {
                  originalOutcome = { kind: 'return', value: result }
                  return result
                },
                (reason) => {
                  originalOutcome = { kind: 'reject', reason }
                  throw reason
                }
              )
            originalOutcome = { kind: 'return', value }
            return value
          }
          originalOutcome = {
            kind: outcome === 'return' ? 'return' : outcome === 'throw' ? 'throw' : 'reject',
            [outcome === 'return' ? 'value' : 'reason']: identity,
          }
          if (outcome === 'throw') throw identity
          return outcome === 'reject' ? Promise.reject(identity) : identity
        }
    )
    const actual = await this.wait(() => telemetryOutcome(run))
    telemetryAssert.equal(log.length, 1)
    return { actual, expected: originalOutcome, log }
  }

  /**
   * Cancels unconsumed holds before disconnect, releases arrived work, and bounds all draining.
   * Every patch and listener is restored even if startup, disconnect or a native shell never settles.
   * @param {number} [timeout=10000] total real cleanup budget in milliseconds
   * @returns {Promise<void>} @throws {Error} the first cleanup failure or deadline expiry
   */
  async dispose(timeout = 10000) {
    let failure = null
    const restore = (call) => {
      try {
        call()
      } catch (error) {
        failure ??= error
      }
    }
    const deadline = realMonotonicNs() + BigInt(timeout) * 1000000n
    const remaining = () => Math.max(0, Number(deadline - realMonotonicNs()) / 1000000)
    const drain = (thunk) => this.wait(thunk, remaining())
    try {
      this.armed = false
      this.trace.schedule.rules.length = 0
      this.wireSchedule.rules.length = 0
      for (const gate of this.gates) {
        // External schedules can still call their unconsumed gate during disconnect.
        if (typeof gate.hold === 'function') gate.hold = () => Promise.resolve()
        if ((gate instanceof DialGate && this.wire) || gate.settle) restore(() => gate.release())
      }
      if (this.client) await drain(() => this.client.disconnect(this.closeOptions(0)))
      await drain(() => Promise.all(this.pending))
      await this.settleWork(remaining())
      for (const pool of this.retiredCorePools) await drain(() => pool.destroy({ drainTimeout: 0 }))
      if (this.client) {
        for (const [id, entry] of [...this.clock.pending])
          if (entry.due <= this.clock.time) this.clock.deliver(id)
        for (const timer of this.brokerAnswerTimers) clearTimeout(timer)
        this.wire.assertClean()
        telemetryAssert.ok(this.wire.sockets().every((socket) => socket.destroyed))
        if (this.machine) {
          telemetryAssert.equal(this.machine.timer, null)
          telemetryAssert.equal(this.machine.waiters.size, 0)
          telemetryAssert.equal(this.clock.pending.size, 0)
        }
        telemetryAssert.deepStrictEqual(this.unhandled, [])
      }
    } catch (error) {
      failure = error
    } finally {
      for (const timer of this.brokerAnswerTimers) clearTimeout(timer)
      if (failure) {
        for (const socket of this.wire?.sockets() ?? []) restore(() => socket.destroy())
      }
      for (const remove of this.eventRemovers.splice(0)) restore(remove)
      restore(() => process.off('unhandledRejection', this.listener))
      for (const undo of this.owners.drain()) restore(undo)
      while (this.restores.length) restore(this.restores.pop())
      while (this.clockRestores.length) restore(this.clockRestores.pop())
    }
    if (failure) throw failure
  }
}

/**
 * Triggers the original crash path of a running consumer or share consumer.
 * @param {PublicTelemetryRound} round started round with a captured runner
 * @returns {Promise<void>} settles once the crash was signalled to the owner
 */
const crashRound = async (round) => {
  if (round.kind === 'shareConsumer')
    return round.runner.current.crash(
      Object.assign(new Error('telemetry crash'), { retriable: false })
    )
  const crashes = round.core.crashes
  round.runner.fetch = async () => {
    throw Object.assign(new Error('telemetry crash'), { retriable: false })
  }
  return round.until(() => !round.machine.connected && round.core.crashes > crashes, 4000)
}

/**
 * Runs one appended telemetry lifecycle row through the actual consumer: each of the eight pairs
 * is mapped to the schedule that proves it, and every row disposes its round.
 * @param {{event:string,boundary:string,protocol:string,membership:string,
 *   enforceRequestTimeout:boolean}} cell the row
 * @returns {Promise<void>}
 * @throws {import('node:assert').AssertionError} any violated telemetry or lifecycle assertion
 */
const runTelemetryLifecycleCell = async (cell) => {
  const round = new PublicTelemetryRound({
    kind: 'consumer',
    enforceRequestTimeout: cell.enforceRequestTimeout,
    protocol: cell.protocol,
    membership: cell.membership,
  })
  round.coreProof = true
  const by = (name) => cell.boundary === `telemetry:${name}`
  try {
    if (by('final-encode') || by('final-dispatch') || by('deadline') || by('old-final-result')) {
      const id = by('final-encode') || by('deadline') ? 'S13' : by('final-dispatch') ? 'S16' : 'S17'
      const gate = round.hold(id, { mode: 'final' })
      await round.wait(() => round.start({ drive: 'final' }))
      await round.wait(() => gate.arrived())
      const before = round.capture()
      telemetryAssert.notEqual(round.savedOrdinaryGuard(), null)
      telemetryAssert.equal(round.savedClosingGuard(), null)
      await round.until(() => !round.cluster.isConnected())
      if (by('deadline')) {
        round.clock.jump(before.closeDeadline)
        telemetryAssert.notEqual(round.savedClosingGuard(), null)
        await round.drainReleased(gate)
        await round.deliverCloseWatchdog()
        await round.settleClose()
        telemetryAssert.deepStrictEqual(round.finalFrames(), [])
        telemetryAssert.equal(round.forcedRetirements().length > 0, true)
      } else if (by('old-final-result')) {
        round.clock.jump(before.closeDeadline + 1)
        await round.deliverCloseWatchdog()
        await round.settleClose()
        await round.wait(() => round.client.connect())
        await round.establish()
        const replacement = round.capture()
        await round.drainReleased(gate)
        round.assertReplacementUnchanged(replacement)
        telemetryAssert.equal(round.finalFrames().length, 1)
      } else {
        await round.drainReleased(gate)
        await round.settleClose()
        telemetryAssert.equal(round.finalFrames().length, 1)
      }
      await round.closePromise
    } else if (by('get-answer') || by('waiter-deadline')) {
      const gate = round.hold('S09', { mode: 'get' })
      await round.wait(() => round.start({ drive: 'get' }))
      await round.wait(() => gate.arrived())
      const settled = []
      if (by('waiter-deadline')) {
        for (const timeout of [10, 30])
          round.pending.push(
            round.client.clientInstanceId({ timeout }).then(
              (value) => settled.push(value),
              (error) => settled.push(error.reason)
            )
          )
        round.clock.jump(11)
      }
      await crashRound(round)
      await round.drainReleased(gate)
      telemetryAssert.equal(round.machine.uuid, null)
      telemetryAssert.equal(round.machine.waiters.size, 0)
      telemetryAssert.equal(round.trace.frames.length, 1)
      if (by('waiter-deadline')) {
        await round.until(() => settled.length === 2)
        telemetryAssert.deepStrictEqual(settled, ['timeout', 'not-connected'])
      }
      await round.restartForeground()
      if (round.protocol !== 'manual')
        await round.until(() => round.core.joins.length > round.core.joinsBefore)
    } else if (by('active-push')) {
      await round.start({ drive: 'connected' })
      await round.ready()
      await round.wait(() => round.client.stop())
      telemetryAssert.equal(round.machine.connected, true)
      const pushes = round.pushes().length
      round.tick()
      await round.until(() => round.pushes().length > pushes)
      telemetryAssert.deepStrictEqual(round.finalFrames(), [])
    } else if (by('resource-snapshot')) {
      const gate = round.hold('S10', { mode: 'normal' })
      await round.wait(() => round.start({ drive: 'normal' }))
      await round.wait(() => gate.arrived())
      const captured = round.binding.captureResources()
      const frames = round.trace.frames.length
      await round.rejoin()
      telemetryAssert.equal(captured.isCurrent(), false)
      await round.drainReleased(gate)
      telemetryAssert.equal(round.trace.frames.length, frames)
    } else throw new Error(`Unmapped telemetry lifecycle boundary ${cell.boundary}`)
    round.assertCore(0, cell.event)
  } finally {
    await round.dispose()
  }
}

Object.assign(module.exports, {
  PublicTelemetryRound,
  runTelemetryLifecycleCell,
  DialGate,
  LiveOwners,
  BOUNDED_LIMITS,
})
