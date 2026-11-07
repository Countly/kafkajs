'use strict'

/**
 * The telemetry churn profile: one parent that supervises four one-client worker processes, one per
 * public client kind. Each worker holds the descriptors and instance ID of its client through
 * subscription changes, telemetry reconnects and public close/reopen cycles, and is judged on its
 * own memory and exact resource identities. The parent owns the broker resource, the independent
 * record oracle and the natural-exit observations of the children.
 *
 * Development-only and public-API-only: nothing here reads a private member of the client.
 * `soak.js` injects its `Soak`, `instrument` and verdict so this module never requires it back.
 */
const crypto = require('node:crypto')
// eslint-disable-next-line n/no-unsupported-features/node-builtins -- development driver; only the resource lifetime hooks are used
const { createHook } = require('node:async_hooks')
const { fork } = require('node:child_process')
const { createChaosSocketFactory, FrameFilter } = require('../../testHelpers/chaosSocket')

const KINDS = Object.freeze(['producer', 'consumer', 'admin', 'shareConsumer'])
const STEPS = Object.freeze({ subscriptionMs: 10000, retireMs: 30000, reopenMs: 60000 })
const ORDER = Object.freeze(['reopen', 'retire', 'subscription'])
const MINIMUMS = Object.freeze({ subscription: 30, retire: 10, reopen: 5 })
const LATE_MS = 5000
const DESCRIPTORS = 128
const CORPUS = Object.freeze({ records: 10000, batch: 100, batchMs: 3000, bytes: 200 })
const SETUP_MS = 48000
const NATURAL_EXIT_MS = 10000
const MAX_STDERR = 65536
const WORKER_ENV = 'TELEMETRY_WORKER_KIND'
const WORKER_TOKEN = 'TELEMETRY_WORKER_TOKEN'
const SUBSCRIPTIONS = Object.freeze(['com.example.', '', 'org.apache.kafka.'])
const TRACKED = new Set([
  'Timeout',
  'TCPWRAP',
  'TCPSERVERWRAP',
  'PIPEWRAP',
  'TTYWRAP',
  'UDPWRAP',
  'MESSAGEPORT',
])

/**
 * @param {number} ms delay
 * @returns {Promise<void>} resolves after the delay; the timer is not unreferenced
 */
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * @param {string} kind a client kind @returns {string} its topic suffix and group label
 */
const labelOf = (kind) => (kind === 'shareConsumer' ? 'share' : kind)

/**
 * The fixed identity of one corpus record. Records are unique across kinds and positions.
 * @param {string} stem run prefix @param {string} kind client kind @param {number} index position
 * @returns {string} the record key
 */
const recordId = (stem, kind, index) => `${stem}:${labelOf(kind)}:${String(index).padStart(5, '0')}`

/**
 * The fixed 200 byte value of a record, carrying its identity.
 * @param {string} id record identity @returns {Buffer} exactly `CORPUS.bytes` bytes
 */
const valueOf = (id) => Buffer.from(id.padEnd(CORPUS.bytes, '.'))

/** The deterministic action table of one run: what happens at which millisecond after the epoch. */
class TelemetrySchedule {
  /**
   * @param {{warmupMs:number, measureMs:number}} window warm-up and measurement lengths
   */
  constructor({ warmupMs, measureMs }) {
    this.warmupMs = warmupMs
    this.endMs = warmupMs + measureMs
    const at = new Map()
    for (const [action, step] of [
      ['reopen', STEPS.reopenMs],
      ['retire', STEPS.retireMs],
      ['subscription', STEPS.subscriptionMs],
    ])
      for (let ms = step; ms < this.endMs; ms += step) at.set(ms, [...(at.get(ms) ?? []), action])
    this.ticks = [...at]
      .sort((a, b) => a[0] - b[0])
      .map(([ms, actions]) => ({ at: ms, actions: ORDER.filter((a) => actions.includes(a)) }))
  }

  /**
   * @param {{at:number}} tick one table row @returns {boolean} whether it falls in the measurement
   */
  measured(tick) {
    return tick.at >= this.warmupMs
  }

  /**
   * @returns {{subscription:number, retire:number, reopen:number}} the measured action counts the
   *   table holds, which a completed run must reach
   */
  required() {
    const counts = { subscription: 0, retire: 0, reopen: 0 }
    for (const tick of this.ticks)
      if (this.measured(tick)) for (const action of tick.actions) counts[action] += 1
    return counts
  }
}

/**
 * Tracks the live timers and handles of this process from before the target is built, with stable
 * issued tokens and a finite live map. Resources created before `start` (the IPC channel) are not
 * seen; resources the fixture itself owns are the caller's to keep out of the comparison.
 */
class ProcessResources {
  /** Builds a disabled tracker. */
  constructor() {
    this.live = new Map()
    this.hook = createHook({
      init: (id, type) => {
        if (TRACKED.has(type)) this.live.set(id, { type, token: { id } })
      },
      destroy: (id) => {
        this.live.delete(id)
      },
    })
  }

  /** @returns {void} begins tracking */
  start() {
    this.hook.enable()
  }

  /** @returns {void} stops tracking and forgets every resource */
  stop() {
    this.hook.disable()
    this.live.clear()
  }

  /**
   * @returns {Record<string, object[]>} the token of each live resource, by type. Tokens are the
   *   same object for the same resource, so a probe can tell a replacement from the original.
   */
  identities() {
    const roles = {}
    for (const { type, token } of this.live.values()) (roles[`resource:${type}`] ??= []).push(token)
    return roles
  }
}

/**
 * The classifying socket factory of one worker: every socket is a chaos socket, and the first
 * request frame of API 71 or 72 marks that very socket as the telemetry transport. It counts Get
 * and Push frames and can retire the one open telemetry socket, leaving core sockets untouched.
 */
class TelemetryTap {
  /** @param {Function} inner the underlying socket factory */
  constructor(inner) {
    const chaos = createChaosSocketFactory(inner)
    this.records = []
    this.factory = (options) => this.track(chaos(options))
  }

  /**
   * @param {object} socket the chaos socket just issued @returns {object} the same socket, classified
   */
  track(socket) {
    const record = { socket, telemetry: false, closed: false, gets: 0, pushes: 0 }
    const classifier = new FrameFilter({
      headerBytes: 12,
      decide: (header) => {
        const api = header.length >= 12 ? header.readInt16BE(4) : -1
        if (api === 71 || api === 72) {
          record.telemetry = true
          if (api === 71) record.gets += 1
          else record.pushes += 1
        }
        return false
      },
    })
    const write = socket.write
    socket.write = function (data, encoding) {
      classifier.push(Buffer.from(data, encoding))
      return write.call(this, data, encoding)
    }
    socket.on('close', () => {
      record.closed = true
      this.records = this.records.filter((entry) => !entry.closed || entry.telemetry)
    })
    this.records.push(record)
    return socket
  }

  /** @returns {{gets:number, pushes:number, sockets:number}} totals over every telemetry socket seen */
  counts() {
    const seen = this.records.filter((r) => r.telemetry)
    return {
      gets: seen.reduce((sum, r) => sum + r.gets, 0),
      pushes: seen.reduce((sum, r) => sum + r.pushes, 0),
      sockets: seen.length,
    }
  }

  /** @returns {number} destroyed telemetry sockets, at most one: the captured transport only */
  retire() {
    const open = this.records.filter((r) => r.telemetry && !r.closed)
    for (const { socket } of open.slice(0, 1))
      socket.destroy(new Error('telemetry transport retired'))
    return Math.min(1, open.length)
  }

  /** @returns {number} telemetry sockets still open */
  open() {
    return this.records.filter((r) => r.telemetry && !r.closed).length
  }
}

/**
 * The retained metric descriptors of one client: the explicit registrations whose bounded
 * retention is the baseline of the memory screen. One slot can be replaced by an identity of the
 * same charge to exercise registration churn; the journal changes only after a registration or
 * removal succeeded.
 */
class Registry {
  /**
   * @param {object} client a public client with the additive registration methods
   * @param {string} kind its kind, part of every name
   * @param {number} [count] canonical descriptors
   */
  constructor(client, kind, count = DESCRIPTORS) {
    this.client = client
    this.kind = kind
    this.count = count
    this.variant = 'a'
    this.registered = []
    this.bytes = 0
  }

  /**
   * @param {number} index descriptor number @param {string} [variant] identity variant of slot 0
   * @returns {object} the legal numeric descriptor
   */
  descriptor(index, variant = 'a') {
    return {
      name: `com.example.soak.${this.kind}.${String(index).padStart(3, '0')}`,
      type: 'sum',
      unit: '1',
      value: BigInt(index + 1),
      attributes: { slot: String(index), variant: index === 0 ? variant : 'a' },
    }
  }

  /**
   * @param {object} descriptor a descriptor @returns {number} its metadata charge in bytes
   */
  charge(descriptor) {
    return (
      Buffer.byteLength(descriptor.name) +
      Buffer.byteLength(descriptor.unit) +
      Object.entries(descriptor.attributes).reduce(
        (sum, [key, value]) => sum + Buffer.byteLength(key) + Buffer.byteLength(value),
        0
      )
    )
  }

  /** @returns {void} registers the canonical population @throws {Error} the first refusal */
  fill() {
    for (let i = 0; i < this.count; i++) this.add(this.descriptor(i))
  }

  /**
   * @param {object} descriptor to register @returns {void} journals it only on success
   */
  add(descriptor) {
    this.client.registerMetricForSubscription(descriptor)
    this.registered.push(descriptor)
    this.bytes += this.charge(descriptor)
  }

  /**
   * @param {object} descriptor to remove @returns {void} journals the removal only on success
   */
  remove(descriptor) {
    this.client.unregisterMetricFromSubscription({
      name: descriptor.name,
      attributes: descriptor.attributes,
    })
    this.registered = this.registered.filter((entry) => entry !== descriptor)
    this.bytes -= this.charge(descriptor)
  }

  /** @returns {void} swaps slot 0 for an identity of the same charge */
  replace() {
    const next = this.variant === 'a' ? 'b' : 'a'
    this.remove(this.registered.find((d) => d.name === this.descriptor(0).name))
    this.add(this.descriptor(0, next))
    this.variant = next
  }

  /** @returns {void} puts the canonical identity back, so every checkpoint compares one registry */
  restore() {
    if (this.variant !== 'a') this.replace()
  }

  /**
   * @param {string} uuid the retained client instance ID
   * @returns {string} a fingerprint of the canonical descriptors, their charge and the UUID
   */
  digest(uuid) {
    const canon = this.registered
      .map((d) => [d.name, d.type, d.unit, d.attributes])
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
    return crypto
      .createHash('sha256')
      .update(JSON.stringify({ kind: this.kind, uuid, bytes: this.bytes, canon }))
      .digest('hex')
  }
}

/**
 * Compares the independent identity sets of the ordinary work: what was offered, what the raw log
 * holds, what the handler saw and what was committed. Every set must equal its expectation exactly.
 */
class OrdinaryOracle {
  /**
   * @param {{expected:string[], durable:string[], handled:string[], committed?:Object<string,string>,
   *   processed?:Object<string,string>}} input independent readings
   * @returns {{ok:boolean, violations:string[], hashes:object}} the verdict with set hashes
   */
  static check({ expected, durable, handled, committed, processed }) {
    const violations = []
    const sorted = (ids) => [...ids].sort()
    const same = (a, b) => JSON.stringify(sorted(a)) === JSON.stringify(sorted(b))
    const unique = (ids) => new Set(ids).size === ids.length
    if (!unique(expected)) violations.push('expected identities repeat')
    if (!same(durable, expected) || !unique(durable)) violations.push('durable identities differ')
    if (!same(handled, expected) || !unique(handled)) violations.push('handler identities differ')
    if (committed !== undefined || processed !== undefined) {
      const keys = new Set([...Object.keys(committed ?? {}), ...Object.keys(processed ?? {})])
      for (const key of keys)
        if (committed?.[key] !== processed?.[key])
          violations.push(`committed end differs on ${key}`)
    }
    const hash = (ids) =>
      crypto
        .createHash('sha256')
        .update(JSON.stringify(sorted(ids)))
        .digest('hex')
    return {
      ok: violations.length === 0,
      violations,
      hashes: { expected: hash(expected), durable: hash(durable), handled: hash(handled) },
    }
  }
}

/**
 * One worker: a single public client of one kind, its retained registry and baseline, the public
 * workload and the fixed churn schedule, run under the inherited `Soak` sampler with a verdict of
 * its own. It never closes a client it did not create and never exits the process.
 */
class TelemetryWorker {
  /**
   * @param {object} options
   * @param {string} options.kind the one target client kind
   * @param {object} options.client the target public client
   * @param {TelemetryTap} options.tap its classifying socket tap
   * @param {Registry} options.registry its retained descriptors
   * @param {{now: () => number}} options.clock monotonic milliseconds
   * @param {(ms:number) => Promise<void>} [options.sleep] real delay
   * @param {object} options.ordinary kind-specific workload: `start`, `stop`, `drain`, `ids`
   * @param {{warmupMs:number, measureMs:number}} options.limits the fixed window
   * @param {object} [options.schedule] a `TelemetrySchedule`
   */
  constructor({ kind, client, tap, registry, clock, sleep = wait, ordinary, limits, schedule }) {
    Object.assign(this, { kind, client, tap, registry, clock, sleep, ordinary, limits })
    this.schedule = schedule ?? new TelemetrySchedule(limits)
    this.uuid = null
    this.actions = {
      warm: { subscription: 0, retire: 0, reopen: 0 },
      measured: { subscription: 0, retire: 0, reopen: 0 },
    }
    this.missed = []
    this.reopens = []
    this.rotations = []
    this.confirmations = []
    this.timing = { boundaryMs: null, measurementMs: null, cleanupMs: null }
    /** The supervising Soak, set by the owner before `work` runs. */
    this.sampler = null
  }

  /**
   * Registers the descriptors, obtains the UUID, disconnects and leaves the client disconnected
   * with exactly that state retained.
   * @returns {Promise<string>} the retained instance ID
   * @throws {Error} a failed registration, connect or UUID wait
   */
  async prepare() {
    this.registry.fill()
    await this.client.connect()
    this.uuid = await this.client.clientInstanceId({ timeout: 15000 })
    await this.client.disconnect()
    return this.uuid
  }

  /**
   * @param {number} epoch the monotonic reading at which the schedule began
   * @param {number} at the target millisecond after the epoch
   * @returns {Promise<void>} resolves once the monotonic clock reads `at`, whatever the timers did
   */
  async until(epoch, at) {
    for (;;) {
      const left = at - (this.clock.now() - epoch)
      if (left <= 0) return
      await this.sleep(Math.min(left, 1000))
    }
  }

  /**
   * One public close then, as the next public action, an immediate connect on the same object.
   * @returns {Promise<void>} after the reopen is proven by the cached UUID and a new Push frame
   * @throws {Error} a changed UUID or a missing Push on the replacement socket
   */
  async reopen() {
    await this.ordinary.drain()
    const before = this.tap.counts()
    const order = []
    await this.client.disconnect()
    order.push('disconnect-resolved')
    const connecting = this.client.connect()
    order.push('connect-called')
    await connecting
    await this.ordinary.resume()
    const cached = await this.client.clientInstanceId({ timeout: 0 })
    await this.pushAfter(before.pushes, LATE_MS)
    this.reopens.push({
      order,
      uuid: cached,
      push: this.tap.counts().pushes > before.pushes,
      open: this.tap.open(),
    })
    if (cached !== this.uuid) throw new Error('The instance ID changed across a public reopen')
    if (!this.reopens.at(-1).push) throw new Error('No normal Push followed the public reopen')
  }

  /**
   * Retires the one captured telemetry socket and waits for the replacement's first Push.
   * @returns {Promise<void>} @throws {Error} when no telemetry socket was open or no Push followed
   */
  async retire() {
    const before = this.tap.counts()
    if (this.tap.retire() !== 1) throw new Error('No open telemetry socket to retire')
    await this.pushAfter(before.pushes, 4 * LATE_MS)
    if (this.tap.counts().pushes <= before.pushes)
      throw new Error('No Push followed the telemetry reconnect')
  }

  /**
   * @param {number} pushes the Push frame total to exceed @param {number} ms bound
   * @returns {Promise<void>} resolves at the first newer Push frame or at the bound
   */
  async pushAfter(pushes, ms) {
    const deadline = this.clock.now() + ms
    while (this.tap.counts().pushes <= pushes && this.clock.now() < deadline) await this.sleep(100)
  }

  /**
   * Records that the broker resource just changed and waits for this client's next Get frame.
   * @param {number} index the rotation number @returns {void} confirmation is joined at cleanup
   */
  rotated(index) {
    const gets = this.tap.counts().gets
    const entry = { index, confirmed: false, measured: null }
    this.rotations.push(entry)
    this.confirmations.push(
      (async () => {
        const deadline = this.clock.now() + LATE_MS
        while (this.tap.counts().gets <= gets && this.clock.now() < deadline) await this.sleep(100)
        entry.confirmed = this.tap.counts().gets > gets
      })()
    )
  }

  /**
   * Runs the warm-up and measurement under the supervising Soak, then the cleanup. The sampler is
   * stopped and joined at the measurement boundary, before any cleanup await.
   * @param {number} ms the fixed warm-up plus measurement
   * @returns {Promise<void>} after cleanup; a failure keeps the primary and every cleanup error
   * @throws {Error|AggregateError} the first workload failure with the cleanup failures
   */
  async work(ms) {
    const { warmupMs, measureMs } = this.limits
    if (ms !== warmupMs + measureMs) throw new Error('Telemetry churn runs its fixed 360000 ms')
    const errors = []
    const epoch = this.clock.now()
    let boundary = null
    try {
      const connecting = this.client.connect()
      await connecting
      await this.ordinary.start()
      for (const tick of this.schedule.ticks) {
        await this.until(epoch, tick.at)
        if (boundary === null && this.schedule.measured(tick)) boundary = this.clock.now() - epoch
        const late = this.clock.now() - epoch - tick.at
        const bucket = this.schedule.measured(tick) ? this.actions.measured : this.actions.warm
        if (late > LATE_MS) {
          this.missed.push({ at: tick.at, actions: tick.actions, late })
          continue
        }
        for (const action of tick.actions) {
          if (action === 'reopen') await this.reopen()
          else if (action === 'retire') await this.retire()
          else this.registry.replace()
          bucket[action] += 1
        }
      }
      await this.until(epoch, this.schedule.endMs)
      this.timing.boundaryMs = boundary
      this.timing.measurementMs = this.clock.now() - epoch - warmupMs
      await this.sampler.endSampling()
    } catch (error) {
      errors.push(error)
    }
    const cleanupAt = this.clock.now()
    await this.sampler.endSampling().catch((error) => errors.push(error))
    for (const step of [
      async () => {
        for (const outcome of await Promise.allSettled(this.confirmations))
          if (outcome.status === 'rejected') throw outcome.reason
      },
      () => this.ordinary.stop(),
      () => this.registry.restore(),
      () => this.client.disconnect(),
    ]) {
      try {
        await step()
      } catch (error) {
        errors.push(error)
      }
    }
    this.timing.cleanupMs = this.clock.now() - cleanupAt
    if (errors.length === 1) throw errors[0]
    if (errors.length) throw new AggregateError(errors, 'Telemetry worker failed')
  }

  /**
   * @returns {{ok:boolean, violations:string[]}} whether every required measured action completed
   *   and none was skipped by a late tick
   */
  requirements() {
    const violations = []
    const need = this.schedule.required()
    for (const action of Object.keys(need))
      if (this.actions.measured[action] < Math.min(need[action], MINIMUMS[action]))
        violations.push(`measured ${action} actions ${this.actions.measured[action]}`)
    if (this.missed.some((m) => m.at >= this.schedule.warmupMs))
      violations.push('a measured tick was missed')
    return { ok: violations.length === 0, violations }
  }
}

/**
 * Supervises the four worker processes: one forked driver per kind, a fixed handshake, bounded
 * output, and a natural-exit observation that starts at the explicit-disconnect receipt.
 */
class TelemetryProcesses {
  /**
   * @param {object} options
   * @param {string} options.script the driver to fork
   * @param {Function} [options.spawn] the fork function; a spec supplies a fake
   * @param {Function} [options.now] a monotonic millisecond clock
   * @param {string[]} [options.args] the driver arguments
   */
  constructor({ script, spawn = fork, now = () => performance.now(), args = [] }) {
    Object.assign(this, { script, spawn, now, args })
    this.workers = new Map()
  }

  /**
   * Forks one worker and registers it.
   * @param {string} kind exactly one of the four kinds
   * @returns {object} the supervised record
   * @throws {Error} for an unknown or already started kind
   */
  start(kind) {
    if (!KINDS.includes(kind)) throw new Error(`Unknown telemetry worker kind ${kind}`)
    if (this.workers.has(kind)) throw new Error(`Telemetry worker ${kind} already started`)
    const token = crypto.randomBytes(8).toString('hex')
    const child = this.spawn(this.script, [...this.args, '--profile', 'telemetry'], {
      execPath: process.execPath,
      execArgv: ['--expose-gc'],
      env: { ...process.env, [WORKER_ENV]: kind, [WORKER_TOKEN]: token },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    })
    const worker = {
      kind,
      token,
      child,
      messages: [],
      stderr: '',
      stdout: '',
      receiptAt: null,
      exit: null,
      waiters: [],
    }
    child.stdout?.on('data', (chunk) => {
      if (worker.stdout.length < MAX_STDERR) worker.stdout += chunk
    })
    child.stderr?.on('data', (chunk) => {
      if (worker.stderr.length < MAX_STDERR) worker.stderr += chunk
    })
    child.on('message', (message) => this.receive(kind, message))
    worker.closed = new Promise((resolve) => {
      child.once('close', (code, signal) => {
        worker.exit = { event: 'close', code, signal, closedAt: this.now() }
        resolve(worker.exit)
      })
    })
    child.once('error', (error) => {
      worker.error = error
    })
    this.workers.set(kind, worker)
    return worker
  }

  /**
   * Accepts one IPC message from a worker, validating its handshake and wake-ups.
   * @param {string} kind the sending worker @param {object} message the message
   * @returns {void}
   */
  receive(kind, message) {
    const worker = this.workers.get(kind)
    if (!worker || typeof message !== 'object' || message === null) return
    if (message.type === 'hello' && (message.kind !== kind || message.token !== worker.token)) {
      worker.error = new Error(`Substituted telemetry worker handshake for ${kind}`)
      return
    }
    if (message.type === 'receipt') worker.receiptAt = this.now()
    worker.messages.push(message)
    for (const wake of worker.waiters.splice(0)) wake()
  }

  /**
   * @param {string} kind the worker @param {string} type the message type to wait for
   * @param {number} ms bound @returns {Promise<object>} the first such message
   * @throws {Error} on timeout, a handshake failure or an early exit
   */
  async message(kind, type, ms) {
    const worker = this.workers.get(kind)
    const deadline = this.now() + ms
    for (;;) {
      if (worker.error) throw worker.error
      const found = worker.messages.find((m) => m.type === type)
      if (found) return found
      const failed = worker.messages.find((m) => m.type === 'failure')
      if (failed) throw new Error(`Telemetry worker ${kind} failed: ${failed.message}`)
      if (worker.exit) throw new Error(`Telemetry worker ${kind} exited before ${type}`)
      const left = deadline - this.now()
      if (left <= 0) throw new Error(`Telemetry worker ${kind} sent no ${type} within ${ms} ms`)
      await new Promise((resolve) => {
        worker.waiters.push(resolve)
        setTimeout(resolve, Math.min(left, 250)).unref?.()
      })
    }
  }

  /**
   * Waits for the natural close of one worker and reports the facts the acceptance needs.
   * @param {string} kind the worker @param {number} [ms] the bound after the receipt
   * @returns {Promise<object>} the natural-exit row; a timeout is reported, never turned into success
   */
  async join(kind, ms = NATURAL_EXIT_MS) {
    const worker = this.workers.get(kind)
    const receipt = worker.receiptAt
    const limit = receipt === null ? 0 : Math.max(0, ms - (this.now() - receipt))
    let timer
    const timeout = new Promise((resolve) => {
      timer = setTimeout(() => resolve(null), limit)
    })
    const closed = await Promise.race([worker.closed, timeout])
    clearTimeout(timer)
    const exit = closed ?? { event: 'timeout', code: null, signal: null }
    const unhandled = worker.messages.filter((m) => m.type === 'unhandled').length
    return {
      kind,
      event: exit.event,
      code: exit.code,
      signal: exit.signal,
      stderrBytes: Buffer.byteLength(worker.stderr),
      unhandled,
      watchdogExpired: closed === null,
      afterDisconnectMs: receipt === null || !exit.closedAt ? null : exit.closedAt - receipt,
    }
  }

  /**
   * Kills a worker that must not outlive a failed run. Its natural-exit row stays failed.
   * @param {string} kind the worker @returns {void}
   */
  taint(kind) {
    const worker = this.workers.get(kind)
    if (worker && !worker.exit) worker.child.kill('SIGKILL')
  }
}

/**
 * The ordinary public work of one worker kind, independent of telemetry. Every kind reports the
 * identities it offered and handled so the parent can compare them with the raw log.
 */
class OrdinaryWork {
  /**
   * @param {object} options
   * @param {string} options.kind the client kind
   * @param {object} options.client the target public client
   * @param {string} options.stem run prefix @param {string} options.topic the corpus topic
   * @param {{now:()=>number}} options.clock monotonic time
   * @param {(ms:number)=>Promise<void>} [options.sleep] real delay
   */
  constructor({ kind, client, stem, topic, clock, sleep = wait }) {
    Object.assign(this, { kind, client, stem, topic, clock, sleep })
    this.running = false
    this.loop = null
    this.offered = []
    this.handled = []
    this.processed = new Map()
    this.committed = new Map()
    this.inflight = Promise.resolve()
  }

  /** @returns {Promise<void>} begins this kind's work; the paced loop is joined by `stop` */
  async start() {
    this.running = true
    if (this.kind === 'consumer' || this.kind === 'shareConsumer') await this.attach()
    this.loop = this.pace()
    this.loop.catch(() => {})
  }

  /** @returns {Promise<void>} subscribes and runs the consuming kinds, again after each reopen */
  async attach() {
    const handle = async ({ partition, message }) => {
      this.handled.push(String(message.key))
      if (this.kind === 'consumer')
        this.processed.set(partition, String(BigInt(message.offset) + 1n))
    }
    if (this.kind === 'shareConsumer') {
      await this.client.subscribe({ topics: [this.topic] })
      await this.client.run({ eachMessage: handle })
    } else {
      await this.client.subscribe({ topic: this.topic, fromBeginning: true })
      await this.client.run({ autoCommit: false, eachMessage: handle })
    }
  }

  /** @returns {Promise<void>} the paced producer or admin loop until `stop` */
  async pace() {
    for (let batch = 0; this.running; batch++) {
      const startedAt = this.clock.now()
      if (this.kind === 'producer' && batch < CORPUS.records / CORPUS.batch) {
        const ids = Array.from({ length: CORPUS.batch }, (_, i) =>
          recordId(this.stem, 'producer', batch * CORPUS.batch + i)
        )
        this.inflight = this.client.send({
          topic: this.topic,
          acks: -1,
          messages: ids.map((id) => ({ key: id, value: valueOf(id) })),
        })
        await this.inflight
        this.offered.push(...ids)
      } else if (this.kind === 'admin') {
        this.inflight = this.client.describeCluster({ timeout: 3000 })
        await this.inflight
      } else if (this.kind === 'consumer') await this.commit()
      const left = CORPUS.batchMs - (this.clock.now() - startedAt)
      if (left > 0) await this.sleep(left)
    }
  }

  /** @returns {Promise<void>} commits the processed next offsets of the classic consumer */
  async commit() {
    const offsets = [...this.processed]
      .filter(([partition, offset]) => this.committed.get(partition) !== offset)
      .map(([partition, offset]) => ({ topic: this.topic, partition, offset }))
    if (!offsets.length) return
    await this.client.commitOffsets(offsets)
    for (const { partition, offset } of offsets) this.committed.set(partition, offset)
  }

  /** @returns {Promise<void>} finishes in-flight work and confirms every commit before a close */
  async drain() {
    await this.inflight.catch(() => {})
    if (this.kind === 'consumer') await this.commit()
  }

  /** @returns {Promise<void>} consuming kinds run again on the reconnected object */
  async resume() {
    if (this.running && (this.kind === 'consumer' || this.kind === 'shareConsumer'))
      await this.attach()
  }

  /** @returns {Promise<void>} stops the loop and joins it, keeping its failure */
  async stop() {
    this.running = false
    if (this.loop) await this.loop
    await this.drain()
  }

  /**
   * @returns {{offered:string[], handled:string[], processed:Object<string,string>,
   *   committed:Object<string,string>}} the identity sets this worker saw
   */
  ids() {
    return {
      offered: this.offered,
      handled: this.handled,
      processed: Object.fromEntries(this.processed),
      committed: Object.fromEntries(this.committed),
    }
  }
}

/**
 * The parent's own telemetry-disabled clients: the topics, the broker resource, the paced
 * producer of the consumer and share corpora, and the independent raw-log and offset readers.
 */
class ParentFixture {
  /**
   * @param {object} options
   * @param {string[]} options.brokers seed addresses @param {string} options.stem run prefix
   * @param {object} options.kafka a `Kafka` whose clients opt out of telemetry
   * @param {Function} options.readLog the raw log reader @param {Function} options.withResource
   *   the owner of one initially absent broker resource
   */
  constructor({ brokers, stem, kafka, readLog, withResource }) {
    Object.assign(this, { brokers, stem, kafka, readLog, withResource })
    this.admin = kafka.admin({ enableMetricsPush: false })
    this.producer = kafka.producer({ enableMetricsPush: false, idempotent: true })
    this.rotationEntries = { 'interval.ms': '1000', match: `client_id=^${stem}-` }
  }

  /** @param {string} kind the client kind @returns {string} its corpus topic */
  topic(kind) {
    return `${this.stem}-${labelOf(kind)}`
  }

  /** @param {string} kind @returns {string} its consumer group */
  group(kind) {
    return `${this.stem}-${labelOf(kind)}-group`
  }

  /** @returns {Promise<void>} connects and creates the three corpus topics, 3 partitions RF3 */
  async open() {
    await this.admin.connect()
    await this.producer.connect()
    await this.admin.createTopics({
      waitForLeaders: true,
      topics: ['producer', 'consumer', 'shareConsumer'].map((kind) => ({
        topic: this.topic(kind),
        numPartitions: 3,
        replicationFactor: 3,
        configEntries: [{ name: 'min.insync.replicas', value: '2' }],
      })),
    })
  }

  /**
   * @param {function():Promise<void>} work runs while the resource exists
   * @returns {Promise<void>} after the resource was created, read back, and deleted again
   */
  inResource(work) {
    return this.withResource(
      this.admin,
      { name: this.stem, entries: { metrics: SUBSCRIPTIONS[2], ...this.rotationEntries } },
      work
    )
  }

  /**
   * Sets the next subscription value and reads it back.
   * @param {number} index rotation number @returns {Promise<string>} the value now in force
   * @throws {Error} when the readback differs
   */
  async rotate(index) {
    const metrics = SUBSCRIPTIONS[index % SUBSCRIPTIONS.length]
    const resource = { type: 16, name: this.stem }
    await this.admin.incrementalAlterConfigs({
      timeout: 3000,
      resources: [
        { ...resource, configEntries: [{ name: 'metrics', operation: 0, value: metrics }] },
      ],
    })
    const read = await this.admin.describeConfigs({
      timeout: 3000,
      resources: [{ ...resource, configNames: ['metrics'] }],
    })
    const value = read.resources[0].configEntries.find(
      (e) => e.configName === 'metrics'
    )?.configValue
    if ((value ?? '') !== metrics) throw new Error('The subscription readback differs')
    return metrics
  }

  /**
   * Produces records `from`..`from + count` of one parent-owned corpus.
   * @param {string} kind consumer or shareConsumer @param {number} from first position
   * @param {number} count how many @returns {Promise<string[]>} the produced identities
   */
  async produce(kind, from, count) {
    const ids = Array.from({ length: count }, (_, i) => recordId(this.stem, kind, from + i))
    await this.producer.send({
      topic: this.topic(kind),
      acks: -1,
      messages: ids.map((id) => ({ key: id, value: valueOf(id) })),
    })
    return ids
  }

  /** @param {string} kind @returns {Promise<string[]>} every key in the topic's raw log */
  async durable(kind) {
    const log = await this.readLog({ brokers: this.brokers, topic: this.topic(kind) })
    return [...log.values()].flatMap(({ records }) => records.map((r) => String(r.key)))
  }

  /** @returns {Promise<Object<string,string>>} the group's committed next offsets, unresolved */
  async committed() {
    const answer = await this.admin.fetchOffsets({
      groupId: this.group('consumer'),
      topics: [this.topic('consumer')],
      resolveOffsets: false,
    })
    return Object.fromEntries(
      answer.flatMap(({ partitions }) => partitions.map((p) => [p.partition, String(p.offset)]))
    )
  }

  /** @returns {Promise<void>} disconnects every client the fixture opened */
  async close() {
    const outcomes = await Promise.allSettled([this.producer.disconnect(), this.admin.disconnect()])
    const errors = outcomes.filter((o) => o.status === 'rejected').map((o) => o.reason)
    if (errors.length) throw new AggregateError(errors, 'Fixture cleanup failed')
  }
}

/**
 * The parent of a run: configures the broker resource, starts four workers concurrently, drives the
 * subscription rotation and the parent-owned corpora, and writes one oracle, four natural-exit
 * rows, four resource verdicts and one final verdict.
 */
class TelemetryChurn {
  /**
   * @param {object} options
   * @param {object} options.fixture a `ParentFixture`
   * @param {TelemetryProcesses} options.processes the supervised workers
   * @param {(row:object)=>void} options.record evidence sink
   * @param {{now:()=>number}} options.clock monotonic time
   * @param {(ms:number)=>Promise<void>} [options.sleep] real delay
   * @param {object} options.limits the `TELEMETRY` limits
   * @param {Function} options.verdict `TelemetryResourceVerdict.evaluate`
   * @param {string[]} options.brokers seed addresses @param {string} options.stem run prefix
   */
  constructor({ fixture, processes, record, clock, sleep = wait, limits, verdict, brokers, stem }) {
    Object.assign(this, {
      fixture,
      processes,
      record,
      clock,
      sleep,
      limits,
      verdict,
      brokers,
      stem,
    })
    this.closed = null
    this.taint = null
  }

  /**
   * Runs the whole fixed case and judges it.
   * @returns {Promise<{ok:boolean, violations:string[], clientKinds:string[], workers:object[],
   *   oracle:object, setupMs:number, cleanupMs:number, totalMs:number}>}
   * @throws {Error} a failure of the case itself; cleanup is `close`'s
   */
  async run() {
    const t0 = this.clock.now()
    const { warmupMs, measureMs, cleanupMs: cleanupBound, caseMs } = this.limits
    const violations = []
    await this.fixture.open()
    let results
    let setupMs
    let epoch
    await this.fixture.inResource(async () => {
      const setupLeft = () => Math.max(1, SETUP_MS - (this.clock.now() - t0))
      for (const kind of KINDS) this.processes.start(kind)
      await Promise.all(KINDS.map((kind) => this.processes.message(kind, 'hello', setupLeft())))
      for (const kind of KINDS)
        this.processes.workers.get(kind).child.send({
          type: 'config',
          stem: this.stem,
          brokers: this.brokers,
          topic: this.fixture.topic(kind),
          group: this.fixture.group(kind),
        })
      await Promise.all(KINDS.map((kind) => this.processes.message(kind, 'ready', setupLeft())))
      setupMs = this.clock.now() - t0
      epoch = this.clock.now()
      for (const kind of KINDS) this.processes.workers.get(kind).child.send({ type: 'go' })
      const ordinary = this.drive(epoch, warmupMs + measureMs)
      results = await Promise.all(
        KINDS.map((kind) =>
          this.processes.message(kind, 'result', warmupMs + measureMs + cleanupBound)
        )
      )
      await ordinary
    })
    const cleanupAt = this.clock.now()
    const workers = []
    for (const result of results) {
      const exit = await this.processes.join(result.kind)
      const input = { ...result.input, clientCount: 1 }
      const evaluated = this.verdict(input)
      workers.push({ kind: result.kind, input, result: evaluated, exit, uuid: result.uuid })
      this.record({
        kind: 'resource-verdict',
        clientKind: result.kind,
        clientId: result.clientId,
        uuid: result.uuid,
        input,
        result: evaluated,
      })
      this.record({ kind: 'natural-exit', clientKind: result.kind, ...exit })
      if (!evaluated.ok) violations.push(`${result.kind}: ${evaluated.violations.join(', ')}`)
      if (!result.requirements.ok)
        violations.push(`${result.kind}: ${result.requirements.violations}`)
      if (result.rotations.filter((r) => r.confirmed).length < MINIMUMS.subscription)
        violations.push(`${result.kind}: too few confirmed subscription transitions`)
      if (
        exit.event !== 'close' ||
        exit.code !== 0 ||
        exit.signal !== null ||
        exit.stderrBytes !== 0 ||
        exit.unhandled !== 0 ||
        exit.watchdogExpired ||
        !(Number.isFinite(exit.afterDisconnectMs) && exit.afterDisconnectMs >= 0) ||
        exit.afterDisconnectMs > this.limits.exitMs
      )
        violations.push(`${result.kind}: the child did not exit naturally`)
    }
    const oracle = await this.oracle(results)
    this.record({ kind: 'oracle', ...oracle })
    if (!oracle.ok) violations.push(...oracle.violations)
    const totalMs = this.clock.now() - t0
    const cleanupMs = this.clock.now() - cleanupAt
    if (totalMs > caseMs) violations.push('the case exceeded its total budget')
    const verdict = { ok: violations.length === 0, violations, setupMs, cleanupMs, totalMs }
    this.record({ kind: 'verdict', ...verdict, warmupMs, measurementMs: measureMs })
    return { ...verdict, clientKinds: [...KINDS], workers, oracle }
  }

  /**
   * Runs the parent-side schedule: subscription rotations every 10 s and the paced corpora.
   * @param {number} epoch monotonic start @param {number} endMs the schedule length
   * @returns {Promise<void>} after the last rotation and the last corpus batch
   */
  async drive(epoch, endMs) {
    let from = 0
    const batches = CORPUS.records / CORPUS.batch
    for (let index = 0; index * STEPS.subscriptionMs + STEPS.subscriptionMs < endMs; index++) {
      const at = (index + 1) * STEPS.subscriptionMs
      while (this.clock.now() - epoch < at) await this.sleep(250)
      await this.fixture.rotate(index)
      for (const kind of KINDS)
        this.processes.workers.get(kind).child.send({ type: 'rotated', index })
      const due = Math.min(batches, Math.floor(at / CORPUS.batchMs))
      while (from < due) {
        await this.fixture.produce('consumer', from * CORPUS.batch, CORPUS.batch)
        await this.fixture.produce('shareConsumer', from * CORPUS.batch, CORPUS.batch)
        from += 1
      }
    }
    while (from < batches) {
      await this.fixture.produce('consumer', from * CORPUS.batch, CORPUS.batch)
      await this.fixture.produce('shareConsumer', from * CORPUS.batch, CORPUS.batch)
      from += 1
    }
  }

  /**
   * Compares the independent raw logs and committed offsets with each worker's own sets.
   * @param {object[]} results the four worker results @returns {Promise<object>} the oracle row
   */
  async oracle(results) {
    const byKind = Object.fromEntries(results.map((r) => [r.kind, r.ordinary]))
    const expected = (kind) =>
      Array.from({ length: CORPUS.records }, (_, i) => recordId(this.stem, kind, i))
    const checks = {}
    for (const kind of ['producer', 'consumer', 'shareConsumer']) {
      const durable = await this.fixture.durable(kind)
      checks[kind] = OrdinaryOracle.check({
        expected: expected(kind),
        durable,
        handled: kind === 'producer' ? byKind.producer.offered : byKind[kind].handled,
        ...(kind === 'consumer'
          ? { committed: await this.fixture.committed(), processed: byKind.consumer.processed }
          : {}),
      })
    }
    const violations = Object.entries(checks).flatMap(([kind, check]) =>
      check.violations.map((v) => `${kind}: ${v}`)
    )
    return {
      ok: violations.length === 0,
      violations,
      independentSetHashes: Object.fromEntries(
        Object.entries(checks).map(([kind, check]) => [kind, check.hashes])
      ),
      recordCounts: Object.fromEntries(
        Object.entries(byKind).map(([kind, o]) => [
          kind,
          { offered: o.offered.length, handled: o.handled.length },
        ])
      ),
    }
  }

  /**
   * Cleans up once: kills no clean worker, joins every child it started, closes its own clients.
   * @returns {Promise<void>} @throws {Error|AggregateError} every cleanup failure, first one first
   */
  close() {
    this.closed ??= (async () => {
      const errors = []
      for (const kind of KINDS) {
        const worker = this.processes.workers.get(kind)
        if (worker && !worker.exit) {
          const exit = await this.processes.join(kind).catch((error) => error)
          if (exit instanceof Error || exit.event !== 'close') this.processes.taint(kind)
          if (exit instanceof Error) errors.push(exit)
        }
      }
      try {
        await this.fixture.close()
      } catch (error) {
        errors.push(error)
      }
      if (errors.length === 1) throw errors[0]
      if (errors.length) throw new AggregateError(errors, 'Telemetry cleanup failed')
    })()
    return this.closed
  }
}

/**
 * The child side of one worker: handshake, one public target client, setup, the supervised soak,
 * and the explicit-disconnect receipt after which the process is left to exit by itself.
 */
class TelemetryWorkerProcess {
  /**
   * @param {object} options
   * @param {string} options.kind the one client kind
   * @param {object} options.soak injected `{ Soak, WORKLOADS, PROFILES, instrument, TELEMETRY,
   *   TelemetryResourceVerdict, logLevel, Kafka }`
   * @param {object} [options.proc] the process, for its IPC channel; a spec supplies a fake
   * @param {{now:()=>number}} [options.clock] monotonic time
   * @param {Function} [options.sleep] real delay
   */
  constructor({
    kind,
    soak,
    proc = process,
    clock = { now: () => performance.now() },
    sleep = wait,
  }) {
    Object.assign(this, { kind, soak, proc, clock, sleep })
    this.inbox = []
    this.waiting = []
    this.worker = null
  }

  /**
   * @param {string} type message type @returns {Promise<object>} the next such message from the parent
   * @throws {Error} when the process has no IPC channel
   */
  expect(type) {
    return new Promise((resolve) => {
      const take = () => {
        const index = this.inbox.findIndex((m) => m.type === type)
        if (index < 0) return false
        resolve(this.inbox.splice(index, 1)[0])
        return true
      }
      if (!take()) this.waiting.push(take)
    })
  }

  /** @returns {void} routes parent messages: rotations go to the worker, the rest wait to be taken */
  listen() {
    this.proc.on('message', (message) => {
      if (message?.type === 'rotated' && this.worker) this.worker.rotated(message.index)
      else this.inbox.push(message)
      this.waiting = this.waiting.filter((take) => !take())
    })
    this.proc.on('unhandledRejection', () => this.proc.send({ type: 'unhandled' }))
  }

  /**
   * Builds the one public client of this kind with telemetry on.
   * @param {object} kafka the instrumented `Kafka` @param {object} config the parent's configuration
   * @returns {object} the public client
   */
  create(kafka, config) {
    const common = { enableMetricsPush: true }
    if (this.kind === 'producer') return kafka.producer({ ...common, idempotent: true })
    if (this.kind === 'admin') return kafka.admin(common)
    if (this.kind === 'shareConsumer')
      return kafka.shareConsumer({ ...common, groupId: config.group })
    return kafka.consumer({
      ...common,
      groupId: config.group,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTimeInMs: 500,
      readUncommitted: false,
    })
  }

  /**
   * Runs the worker to its receipt.
   * @returns {Promise<{ok:boolean, violations:string[]}>} the resource verdict of this one client
   * @throws {Error} a failed setup or run, after the receipt was still sent
   */
  async run() {
    const { soak, kind, proc } = this
    if (typeof proc.send !== 'function')
      throw new Error('A telemetry worker needs its parent channel')
    this.listen()
    proc.send({ type: 'hello', kind, token: process.env[WORKER_TOKEN] })
    const config = await this.expect('config')
    const resources = new ProcessResources()
    resources.start()
    let verdict
    let failure
    try {
      const tap = new TelemetryTap((options) => soak.socketFactory(options))
      const { kafka, probe } = soak.instrument(
        {
          clientId: `${config.stem}-${kind}`,
          brokers: config.brokers,
          logLevel: soak.logLevel.NOTHING,
          socketFactory: tap.factory,
        },
        { publicOnly: true, memory: true, identities: () => resources.identities() }
      )
      const client = this.create(kafka, config)
      const registry = new Registry(client, kind)
      const worker = new TelemetryWorker({
        kind,
        client,
        tap,
        registry,
        clock: this.clock,
        sleep: this.sleep,
        limits: soak.TELEMETRY,
        ordinary: new OrdinaryWork({
          kind,
          client,
          stem: config.stem,
          topic: config.topic,
          clock: this.clock,
          sleep: this.sleep,
        }),
      })
      this.worker = worker
      const uuid = await worker.prepare()
      const measured = {
        snapshot: async () => {
          registry.restore()
          return { ...(await probe.snapshot()), registryDigest: registry.digest(uuid) }
        },
      }
      const { warmupMs, measureMs } = soak.TELEMETRY
      /** A reading belongs to the measurement only inside the fixed window, never after its end. */
      const inWindow = ({ atMs }) => atMs >= warmupMs && atMs <= warmupMs + measureMs
      let readings
      const judge = (run) => {
        readings = run
        return soak.TelemetryResourceVerdict.evaluate({
          clientCount: 1,
          baseline: run.baseline,
          samples: run.samples.filter(inWindow),
          teardown: run.teardown,
          measurementMs: worker.timing.measurementMs,
          cleanupMs: worker.timing.cleanupMs,
        })
      }
      const sampler = new soak.Soak({
        kafka,
        probe: measured,
        phases: soak.PROFILES.telemetry,
        context: { telemetryChurn: worker },
        sampleMs: soak.TELEMETRY.sampleMs,
        warmupMs: soak.TELEMETRY.warmupMs,
        judge,
        record: () => {},
        workloads: soak.WORKLOADS,
        clock: this.clock,
      })
      worker.sampler = sampler
      proc.send({ type: 'ready', kind, uuid })
      await this.expect('go')
      verdict = await sampler.run()
      proc.send({
        type: 'result',
        kind,
        clientId: `${config.stem}-${kind}`,
        uuid,
        verdict,
        input: {
          baseline: readings.baseline,
          samples: readings.samples.filter(inWindow),
          warmSamples: readings.samples.filter(({ atMs }) => atMs < warmupMs).length,
          lateSamples: readings.samples.filter(({ atMs }) => atMs > warmupMs + measureMs).length,
          teardown: readings.teardown,
          measurementMs: worker.timing.measurementMs,
          cleanupMs: worker.timing.cleanupMs,
        },
        requirements: worker.requirements(),
        rotations: worker.rotations,
        actions: worker.actions,
        missed: worker.missed,
        reopens: worker.reopens,
        ordinary: worker.ordinary.ids(),
      })
    } catch (error) {
      failure = error
      proc.send({ type: 'failure', kind, message: String(error?.message ?? error).slice(0, 2048) })
    }
    resources.stop()
    proc.send({ type: 'receipt', kind })
    proc.disconnect?.()
    if (failure) throw failure
    return verdict
  }
}

module.exports = {
  KINDS,
  SCHEDULE_STEPS: STEPS,
  MINIMUMS,
  CORPUS,
  SETUP_MS,
  NATURAL_EXIT_MS,
  WORKER_ENV,
  WORKER_TOKEN,
  SUBSCRIPTIONS,
  OrdinaryOracle,
  OrdinaryWork,
  ParentFixture,
  ProcessResources,
  Registry,
  TelemetryChurn,
  TelemetryProcesses,
  TelemetrySchedule,
  TelemetryTap,
  TelemetryWorker,
  TelemetryWorkerProcess,
  labelOf,
  recordId,
  valueOf,
  wait,
}
