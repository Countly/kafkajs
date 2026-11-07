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

/** Captures independently observed frames and delegates every hold to the existing Schedule. */
class TelemetryTrace {
  /** @returns {TelemetryTrace} an empty finite observation round */
  constructor() {
    this.schedule = new TelemetrySchedule()
    this.frames = []
  }

  /** @param {object} selector exact existing Schedule selector @returns {object} existing Gate */
  hold(selector) {
    return this.schedule.hold(selector)
  }

  /** @param {object} call captured call data @param {Function} original lazy original invocation
   * @returns {Promise<*>} original result after the selected existing gate */
  async enter(call, original) {
    await this.schedule.run(call, () => undefined)
    return original()
  }

  /** @param {{connection:bigint,attempt:bigint,lifecycle:bigint,transport:bigint,subscription:bigint,request:bigint,correlation:number,apiKey:number,hex:string}} frame actual complete socket frame
   * @returns {void} retains immutable bytes and issued identities
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
    if (typeof frame.hex !== 'string' || !/^(?:[0-9a-f]{2})+$/.test(frame.hex))
      throw new TypeError('Missing frame bytes')
    const bytes = Buffer.from(frame.hex, 'hex')
    if (
      bytes.length < 12 ||
      bytes.readInt32BE(0) !== bytes.length - 4 ||
      bytes.readInt16BE(4) !== frame.apiKey ||
      bytes.readInt32BE(8) !== frame.correlation
    )
      throw new TypeError('Frame identity differs from bytes')
    this.frames.push(Object.freeze({ ...frame }))
  }

  /** @param {Array<object>} expected independent literal identities and bytes
   * @returns {void} compares every ordered frame, never count alone
   * @throws {import('node:assert').AssertionError} missing, extra, reordered or substituted frame */
  assertFrames(expected) {
    telemetryAssert.deepStrictEqual(this.frames, expected)
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
 * The boundary ledger: each stale-work boundary with the operation modes that can reach it. S15 is
 * only reachable for a final queued behind an in-flight ordinary request, so it has no other mode.
 * S18's get and normal rows retire only through a close or reopen, so their hold arrives during
 * the action (`lateArrival`) and they carry only those actions.
 * @type {ReadonlyArray<[string, ReadonlyArray<'get'|'normal'|'final'>]>}
 */
const RELEASE_LEDGER = Object.freeze([
  ...['S01', 'S02', 'S03', 'S04', 'S05', 'S06', 'S07', 'S08', 'S09'].map((id) => [id, ['get']]),
  ...['S10', 'S11', 'S12'].map((id) => [id, ['normal', 'final']]),
  ...['S13', 'S14', 'S16', 'S17', 'S18'].map((id) => [id, ['get', 'normal', 'final']]),
  ['S15', ['final']],
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
  if (boundary === 'S18') return [RELEASE_ACTIONS[2], RELEASE_ACTIONS[3], RELEASE_ACTIONS[5]]
  return RELEASE_ACTIONS
}

/**
 * Frames written to the dedicated socket before the hold arrives, by position in the write path.
 * @param {string} boundary ledger id
 * @param {'get'|'normal'|'final'} mode operation mode
 * @returns {Array<'G'|'P'|'F'>} Get, ordinary Push and final Push in write order
 */
const prewritten = (boundary, mode) => {
  const base = mode === 'get' ? [] : ['G', 'P']
  if (boundary === 'S09' || (boundary === 'S17' && mode === 'get')) return ['G']
  if (boundary === 'S17' && mode === 'normal') return [...base, 'P']
  if (boundary === 'S17' || boundary === 'S18') return mode === 'final' ? [...base, 'F'] : base
  if (boundary === 'S15') return [...base, 'P']
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
  ({ boundary, mode, action }) =>
  (old) => {
    const fresh = mode === 'get'
    const frame = (kind, socket, held) => ({
      api: kind === 'G' ? 71 : 72,
      terminating: kind === 'G' ? null : kind === 'F',
      uuid: kind === 'G' && fresh ? null : TELEMETRY_UUID,
      subscriptionId: kind === 'G' ? null : DEFAULT_SUBSCRIPTION.subscriptionId,
      socket,
      ...(held && kind !== 'F' ? { lifecycle: old.lifecycle } : {}),
    })
    const before = prewritten(boundary, mode)
    const expected = before.map((kind, index) =>
      frame(kind, 1n, index === before.length - 1 && ['S09', 'S17'].includes(boundary))
    )
    const successor = boundary === 'S01' && fresh ? 1n : 2n
    const reconnect = () => [frame('G', successor), frame('P', successor)]
    if (action === RELEASE_ACTIONS[0]) {
      if (!['S09', 'S17', 'S18'].includes(boundary))
        expected.push(frame({ get: 'G', normal: 'P', final: 'F' }[mode], 1n, true))
    } else if (action === RELEASE_ACTIONS[1] && mode !== 'final') expected.push(...reconnect())
    else if (action === RELEASE_ACTIONS[2] && mode === 'normal' && boundary !== 'S18')
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
          for (const action of actionsFor(boundary, mode)) {
            const cell = {
              name: `${kind}/${enforceRequestTimeout ? 'enforced' : 'natural'}/${boundary}/${mode}/${action}`,
              kind,
              enforceRequestTimeout,
              boundary,
              action,
              mode,
              start: { ...startFor(boundary), drive: mode },
              options: {},
              releaseValue: undefined,
              lateArrival: boundary === 'S18' && mode !== 'final',
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
    for (let step = 0; step < 4000 && wire.heldDialsTo(node) === 0; step++) await turn()
    if (wire.heldDialsTo(node) === 0) throw new Error('Dial hold was not reached')
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
   *   enabled?:boolean,protocol?:string,membership?:string}} options the actual client and variant
   * @returns {PublicTelemetryRound} configuration only; no client exists until `start`
   */
  constructor({
    kind,
    enforceRequestTimeout = false,
    enabled = true,
    protocol = 'classic-eager',
    membership = 'dynamic',
  }) {
    Object.assign(this, { kind, enforceRequestTimeout, enabled, protocol, membership })
    this.clock = new TelemetryClock()
    this.trace = new TelemetryTrace()
    this.wireSchedule = new TelemetrySchedule()
    this.restores = []
    this.pending = []
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
    this.guards = { ordinary: null, closing: null }
    this.setupCalls = []
    this.retirements = []
    this.finalHook = null
    this.reregisteredAt = null
    this.dedicatedConnection = null
    this.brokerAnswerTimers = new Set()
    this.deliveries = []
    this.listener = (reason) => this.unhandled.push(reason)
    process.on('unhandledRejection', this.listener)
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
   * Waits for a real condition with a bounded number of event-loop turns.
   * @param {() => boolean} predicate independent observation @param {number} [limit=4000] turns
   * @returns {Promise<void>} @throws {Error} when the boundary is never reached
   */
  async until(predicate, limit = 4000) {
    for (let step = 0; step < limit && !predicate(); step++) await turn()
    if (!predicate()) throw new Error('Telemetry boundary did not settle')
  }

  /**
   * Starts an action, observes its promise at once and returns it; a boundary that never arrives
   * is a setup failure, not a product red.
   * @param {Function} thunk invoked only now @returns {Promise<*>} the action's own outcome
   */
  async wait(thunk) {
    const promise = Promise.resolve().then(thunk)
    this.pending.push(promise.catch(() => {}))
    let timer
    const limit = new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('Telemetry wait exceeded its bound')), 10000)
    })
    try {
      return await Promise.race([promise, limit])
    } finally {
      clearTimeout(timer)
    }
  }

  /** @returns {Array<object>} wire records of the telemetry APIs, oldest first */
  telemetryRequests() {
    return this.wire
      .requests()
      .filter((r) => r.api === 'GetTelemetrySubscriptions' || r.api === 'PushTelemetry')
  }

  /** @returns {number} dedicated sockets opened so far */
  dedicatedCount() {
    return Math.max(0, this.wire.sockets().length - this.foregroundSockets)
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
   * @param {{mode?:'get'|'normal'|'final'}} [options] operation the hold applies to
   * @returns {object} a Schedule gate, or a dial gate for S02
   * @throws {Error} duplicate or unknown boundary
   */
  hold(boundary, { mode = 'normal' } = {}) {
    if (this.held) throw new Error('Duplicate telemetry hold')
    const Connection = require('../src/network/connection')
    const Broker = require('../src/broker')
    const TelemetryPayload = require('../src/telemetry/payload')
    const gzip = require('../src/protocol/message/compression/gzip')
    const round = this
    const dedicated = (connection) => connection.authenticationOwner != null
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
    const traced = () => this.trace.hold({ boundary })
    const wired = (selector) => this.wireSchedule.hold(selector)
    const holds = {
      S01: traced,
      S02: () => new DialGate(this),
      S03: traced,
      S04: () => {
        encodeHold([17], 'before')
        return traced()
      },
      S05: () => {
        encodeHold([17], 'after')
        return traced()
      },
      S06: () => {
        encodeHold([36], 'before')
        return traced()
      },
      S07: () => {
        encodeHold([36], 'after')
        return traced()
      },
      S08: () =>
        wired({
          api: 'ApiVersions',
          match: (call) => round.armed && call.generation > round.foregroundSockets,
        }),
      S09: () => wired({ api: 'GetTelemetrySubscriptions', match: () => round.armed }),
      S10: () => {
        payloadHold('before')
        return traced()
      },
      S11: () => {
        payloadHold('after')
        return traced()
      },
      S12: () => {
        this.patch(gzip, 'compress', (original) => async (...args) => {
          if (round.armed && closing() === (mode === 'final')) await round.enter(boundary)
          return original(...args)
        })
        return traced()
      },
      S13: () => {
        encodeHold([71, 72], 'before', true)
        return traced()
      },
      S14: () => {
        stageHold('private:Connection:enqueue', 'before')
        return traced()
      },
      S15: () =>
        wired({ api: 'PushTelemetry', match: (call) => round.armed && !call.fields.terminating }),
      S16: () => {
        stageHold('private:Connection:prepare', 'after')
        return traced()
      },
      S17: () => {
        brokerHold()
        return traced()
      },
      S18: traced,
      S19a: () => {
        encodeHold([17], 'before')
        return traced()
      },
      S19b: traced,
      S19c: () => {
        encodeHold([36], 'before')
        return traced()
      },
      S19d: () =>
        wired({
          api: 'SaslAuthenticate',
          match: (call) => round.armed && call.generation > round.foregroundSockets,
        }),
      S19e: () => {
        this.patch(
          Connection.prototype,
          'send',
          (original) =>
            async function (options) {
              const result = await original.call(this, options)
              if (round.armed && dedicated(this) && options.request.apiKey === 36)
                await round.enter(boundary)
              return result
            }
        )
        return traced()
      },
      S19f: traced,
    }
    if (!holds[boundary]) throw new Error(`Unknown telemetry boundary ${boundary}`)
    const gate = holds[boundary]()
    this.held = { boundary, mode, gate }
    this.gates.push(gate)
    return gate
  }

  /**
   * Wraps every telemetry transport the real factory creates so setup, forced retirement and the
   * S01/S18 holds are observed at the real `open` and `retire` calls.
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
            round.setupCalls.push('open')
            if (round.armed && round.held?.boundary === 'S01') await round.enter('S01')
            return open(candidate, {
              ...options,
              onAllocated: (handle) => {
                const retire = handle.retire
                handle.retire = async (...args) => {
                  round.retirements.push(handle)
                  if (round.armed && round.held?.boundary === 'S18') await round.enter('S18')
                  return retire(...args)
                }
                options.onAllocated(handle)
              },
            })
          }
          return transport
        }
    )
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
    this.patch(
      Connection.prototype,
      'send',
      (original) =>
        function (options) {
          if (this.authenticationOwner) round.dedicatedConnection = this
          return original.call(this, options)
        }
    )
    this.patch(
      Broker.prototype,
      'sendTelemetry',
      (original) =>
        function (apiKey, definition, payload, options) {
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
          connection: BigInt(this.wire.sockets().indexOf(socket) - this.foregroundSockets + 1),
          attempt: BigInt(generation),
          lifecycle: machine.lifecycle,
          transport: machine.transportRevision,
          subscription: machine.subscription?.revision ?? 0n,
          request: machine.running?.revision ?? machine.closing?.revision ?? 0n,
          correlation: frame.readInt32BE(8),
          apiKey,
          hex: frame.toString('hex'),
        })
        this.frameFields.push(this.wire.log.at(-1).fields)
      }
      return result
    })
    const metrics = this.metrics
    this.patch(metrics, 'snapshot', (original) => (subscription, resources) => {
      const snapshot = original.call(metrics, subscription, resources)
      this.snapshots.push({ snapshot, closing: this.machine.closing != null })
      return snapshot
    })
    this.patch(metrics, 'written', (original) => (snapshot, included) => {
      this.written.push({ snapshot, included })
      return original.call(metrics, snapshot, included)
    })
    this.patch(metrics, 'abandon', (original) => (snapshot) => {
      this.abandoned.push(snapshot)
      return original.call(metrics, snapshot)
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
    this.patch(Date, 'now', () => () => 1000 + round.clock.now())
    this.patch(process.hrtime, 'bigint', () => () => BigInt(round.clock.now()) * 1000000n)
    this.patch(
      TelemetryFactory.prototype,
      'bind',
      (original) =>
        function (cluster) {
          if (this.config.enableMetricsPush) {
            this.clock = round.clock
            round.bindings.push(this)
          }
          return original.call(this, cluster)
        }
    )
    this.watchTransport()
    this.provider = async () => {
      const hold = this.armed ? this.held?.boundary : null
      if (hold === 'S03' || hold === 'S19b') await this.enter(hold)
      const token = { value: 'telemetry-token' }
      if (hold === 'S19f') await this.enter(hold)
      return token
    }
    const advertise = {
      GetTelemetrySubscriptions: [0, 0],
      PushTelemetry: [0, 0],
      ...(sasl ? { SaslHandshake: [1, 1], SaslAuthenticate: [2, 2] } : {}),
    }
    const kafkaOptions = {
      enableMetricsPush: this.enabled,
      enforceRequestTimeout: this.enforceRequestTimeout,
      ...(sasl ? { sasl: { mechanism: 'oauthbearer', oauthBearerProvider: this.provider } } : {}),
      ...kafka,
    }
    if (this.kind === 'shareConsumer') {
      const { shareClient } = require('./shareHarness')
      const harness = shareClient({
        advertise,
        kafka: kafkaOptions,
        share: client,
        profile: 'share',
      })
      this.harness = harness
      this.wire = harness.cluster
      this.wire.schedule = this.wireSchedule
      this.kafka = harness.client
      this.client = harness.consumer
    } else {
      const base = this.kind === 'consumer' && this.protocol === 'consumer' ? 'consumer' : 'default'
      this.wire = scripted.createScriptedCluster({
        nodes: [{ id: 1, host: 'a', port: 9092 }],
        profile: base,
        schedule: this.wireSchedule,
        advertise,
      })
      this.kafka = new Kafka(this.wire.kafkaOptions(kafkaOptions))
    }
    this.node = this.wire.nodes[0].id
    this.wire.answer('GetTelemetrySubscriptions', (fields) => ({
      ...this.subscription,
      clientInstanceId: fields.clientInstanceId ?? this.subscription.clientInstanceId,
    }))
    this.wire.answer('PushTelemetry', () => ({}))
    if (sasl) this.wire.answer('SaslHandshake', () => ({ enabledMechanisms: ['OAUTHBEARER'] }))
    if (this.kind !== 'shareConsumer') await this.createClient(client)
    this.binding = this.bindings.at(-1) ?? null
    this.machine = this.binding?.machine ?? null
    this.metrics = this.binding?.metrics ?? null
    if (this.machine) {
      this.registerPoints(points)
      this.observe()
    }
    beforeConnect?.(this)
    await this.connectForeground()
    this.foregroundSockets = this.wire.sockets().length
    if (!this.machine || drive === 'connected') return this
    if (drive === 'get') {
      this.arm()
      this.tick()
      return this
    }
    await this.ready()
    if (this.held?.boundary.startsWith('S19')) this.armRenewal()
    this.arm()
    if (drive === 'normal') this.tick()
    else {
      if (this.held?.boundary === 'S15') {
        const sent = this.pushes().length
        this.tick()
        await this.until(() => this.pushes().length > sent)
      }
      this.closePromise = this.beginClose(this.closeOptions(1000))
    }
    return this
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
    if (this.kind === 'producer' || this.kind === 'admin') return this.client.connect()
    if (this.kind === 'shareConsumer') {
      const { ShareRunner } = require('../src/consumer/share/shareRunner')
      const round = this
      this.patch(
        ShareRunner.prototype,
        'api',
        (original) =>
          function (...args) {
            round.runner = this
            return original.apply(this, args)
          }
      )
      await this.client.subscribe({ topics: ['topic-a'] })
      await this.client.run({ eachMessage: async () => {} })
      return this.until(() => this.runner?.current?.membership.memberEpoch > 0)
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
    await this.client.connect()
    if (this.protocol === 'manual')
      await this.client.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
    else await this.client.subscribe({ topic: TOPIC, fromBeginning: true })
    await this.client.run({ autoCommit: false, eachMessage: async () => {} })
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
    if (!machine.subscription) {
      this.tick()
      await this.until(() => machine.subscription !== null)
    }
    const answered = () => this.pushes().filter((r) => r.answeredOrder !== undefined).length
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
      transport: machine.transportRevision,
      subscription: machine.subscription?.revision ?? 0n,
      running: machine.running?.revision ?? 0n,
      timerRevision: machine.timerRevision,
      closeRevision: machine.closing?.revision ?? 0n,
      closeDeadline: machine.closing?.deadline,
      closeEntry: this.closeEntry,
      reopen: machine.reopen ? { ...machine.reopen } : null,
      sequence: machine.sequence,
      handle: machine.handle,
      telemetryHandleIdentity: machine.handle,
      dedicated: this.dedicatedCount(),
      frames: this.trace.frames.length,
      requests: this.wire.requests().length,
      setupCalls: this.setupCalls.length,
      written: this.written.length,
      abandoned: this.abandoned.length,
      snapshots: this.snapshots.length,
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
    for (const { snapshot } of [...this.snapshots].reverse()) {
      const point = snapshot?.points.find((candidate) => candidate.name === name)
      if (point) return point.identity.revision
    }
    return 0n
  }

  /** @returns {number} destroys the newest dedicated socket with an OS-coded transport error */
  faultDedicated() {
    const sockets = this.wire.sockets()
    if (sockets.length <= this.foregroundSockets) return 0
    sockets
      .at(-1)
      .destroy(Object.assign(new Error('telemetry transport lost'), { code: 'ECONNRESET' }))
    return 1
  }

  /**
   * Runs one ordinary Get then one ordinary Push on the current transport to completion.
   * @returns {Promise<void>}
   */
  async establish() {
    const machine = this.machine
    const answered = (api) =>
      this.wire.requests().filter((r) => r.api === api && r.answeredOrder !== undefined).length
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
      this.clock.jump(this.clock.time + 30000)
      if (watchdog && this.clock.pending.has(watchdog)) this.clock.deliver(watchdog)
      await this.until(() => machine.state === 'BACKOFF' && machine.timer !== null)
      return this.establish()
    }
    if (name === RELEASE_ACTIONS[2] || name === RELEASE_ACTIONS[3]) {
      if (!this.closePromise)
        this.closePromise = this.beginClose(this.closeOptions(name === RELEASE_ACTIONS[2] ? 20 : 0))
      return lateArrival ? this.until(() => this.held.gate.settle != null) : this.settleClose()
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
      await this.client.connect()
      return this.establish()
    }
    throw new Error(`Unknown telemetry action ${name}`)
  }

  /** @returns {Promise<void>} signals the original crash path of the running consumer or share consumer */
  crash() {
    return crashRound(this)
  }

  /** @returns {Promise<void>} waits until the actual public close promise has settled */
  settleClose() {
    return this.until(() => this.closeDone)
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
    for (const id of this.clock.pending.keys()) if (!before.has(id)) this.closeTimer = id
    this.pending.push(
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
   *   option on timeout-capable kinds, undefined for admin which has no overload
   */
  closeOptions(timeout) {
    return this.kind === 'admin' ? undefined : { timeout }
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
    return [...this.retirements]
  }

  /**
   * Runs a synchronous call immediately before the next final's original guard, commit and write.
   * @param {Function} call synchronous reentrant action @returns {void}
   */
  atFinalPrewrite(call) {
    this.finalHook = () => {
      this.reregisteredAt = this.clock.time
      this.snapshotsAtHook = this.snapshots.length
      call()
    }
  }

  /** @returns {object|undefined} the snapshot the final sampled, never a new sample */
  finalSnapshotIdentity() {
    return this.snapshots.filter((entry) => entry.closing).at(-1)?.snapshot
  }

  /** @param {object} snapshot a captured snapshot @returns {number} commits recorded for it */
  writtenFor(snapshot) {
    return this.written.filter((entry) => entry.snapshot === snapshot).length
  }

  /**
   * Independently decodes the newest ordinary Push with a protobuf reader of its own.
   * @param {{name:string}} identity metric identity
   * @returns {{value:bigint|number,origin:bigint|undefined,metricRevision:bigint}} the point
   * @throws {Error} when the point is absent
   */
  decodedLatestPoint({ name }) {
    const bytes = Buffer.from(this.pushes().at(-1).fields.metrics, 'hex')
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

  /** @returns {bigint|undefined} start time the collector issued to the re-registered descriptor */
  replacementMetricOrigin() {
    return this.snapshots
      .slice(this.snapshotsAtHook ?? 0)
      .flatMap((entry) => entry.snapshot?.points ?? [])
      .find((point) => point.name === 'com.example.total')?.startTimeUnixNano
  }

  /**
   * Releases an arrived gate once and lets its captured call settle or be refused.
   * @param {object} gate hold gate @param {*} [value] the captured original answer
   * @returns {Promise<void>}
   */
  async drainReleased(gate, value) {
    if (gate.settle) gate.release(value)
    for (let step = 0; step < 200; step++) await turn()
  }

  /**
   * Compares the complete ordered frame list with an independent manifest: api, flags, UUID,
   * subscription and dedicated socket, plus the held operation's lifecycle where predeclared; each
   * frame must pair with a real wire record by connection generation and correlation.
   * @param {Array<object>} expected predeclared frames @returns {void}
   * @throws {import('node:assert').AssertionError} any missing, extra, reordered or substituted frame
   */
  assertTrace(expected) {
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
      if (expected[index].lifecycle !== undefined)
        telemetryAssert.equal(frame.lifecycle, expected[index].lifecycle)
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
    for (const key of ['lifecycle', 'transport', 'subscription', 'timerRevision'])
      telemetryAssert.equal(now[key], before[key], key)
    telemetryAssert.equal(now.handle, before.handle)
    telemetryAssert.deepStrictEqual(now.reopen, before.reopen)
    telemetryAssert.equal(now.written, before.written)
    const owned = new Set(this.snapshots.slice(0, before.snapshots).map((entry) => entry.snapshot))
    for (const snapshot of this.abandoned.slice(before.abandoned))
      telemetryAssert.ok(owned.has(snapshot), 'only an older sample may be abandoned late')
  }

  /**
   * Runs one actual foreground original below the public method, with a unique outcome, and
   * records every invocation's receiver and arguments.
   * @param {{target:object,key:string|symbol,outcome:'return'|'throw'|'reject',identity:*,
   *   run:Function}} options the original to control and the public call that reaches it
   * @returns {Promise<{actual:object,log:Array<{receiver:*,args:Array}>}>} tagged outcome and calls
   */
  async exerciseForeground({ target, key, outcome, identity, run }) {
    const log = []
    this.patch(
      target,
      key,
      () =>
        function (...args) {
          log.push({ receiver: this, args })
          if (outcome === 'throw') throw identity
          return outcome === 'reject' ? Promise.reject(identity) : identity
        }
    )
    return { actual: await telemetryOutcome(run), log }
  }

  /**
   * Releases only arrived gates, finishes every owned promise, disconnects through the public
   * client, proves the round owns no work, then restores every patch.
   * @returns {Promise<void>} @throws {Error} when the round leaves timers, sockets or failures
   */
  async dispose() {
    let failure = null
    try {
      for (const gate of this.gates) if (gate.settle) gate.release()
      if (this.client) {
        await this.wait(() => this.client.disconnect(this.closeOptions(0)))
        await Promise.all(this.pending)
        for (let step = 0; step < 4 && this.machine && this.clock.pending.size; step++) {
          for (const [id, entry] of [...this.clock.pending])
            if (entry.due <= this.clock.time) this.clock.deliver(id)
          await turn()
        }
        for (const timer of this.brokerAnswerTimers) clearTimeout(timer)
        await turn()
        await turn()
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
      process.off('unhandledRejection', this.listener)
      while (this.restores.length) this.restores.pop()()
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
      Object.assign(new Error('telemetry crash'), { retriable: true, retryTime: 5 })
    )
  round.runner.fetch = async () => {
    throw Object.assign(new Error('telemetry crash'), { retriable: false })
  }
  return round.until(() => !round.machine.connected, 4000)
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
      round.binding.updateResources(captured.values)
      telemetryAssert.equal(captured.isCurrent(), false)
      await round.drainReleased(gate)
      telemetryAssert.equal(round.trace.frames.length, frames)
    } else throw new Error(`Unmapped telemetry lifecycle boundary ${cell.boundary}`)
  } finally {
    await round.dispose()
  }
}

Object.assign(module.exports, { PublicTelemetryRound, runTelemetryLifecycleCell, DialGate })
