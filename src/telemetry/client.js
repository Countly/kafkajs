const { performance } = require('node:perf_hooks')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('../errors')
const TelemetryPayload = require('./payload')
const MAX_TIMER = 2147483647
const UUID =
  /^(?!00000000-0000-0000-0000-000000000000$)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const CLOCK = { now: () => performance.now(), random: Math.random, setTimeout, clearTimeout }

/**
 * Validates the public getter options once, reading the timeout property a single time.
 * @param {{timeout?:number}|undefined} options caller options; omitted or empty selects 60000
 * @returns {number} integer milliseconds from 0 to 2147483647
 * @throws {KafkaJSInvalidConfigurationError} fixed text for any invalid shape, including a throwing accessor
 */
const waitTimeout = (options) => {
  try {
    if (options === undefined) return 60000
    if (options === null || typeof options !== 'object' || Array.isArray(options)) throw Error()
    const value = options.timeout
    const timeout = value === undefined ? 60000 : value
    if (!Number.isInteger(timeout) || timeout < 0 || timeout > MAX_TIMER) throw Error()
    return timeout
  } catch {
    throw new KafkaJSInvalidConfigurationError('Invalid telemetry timeout')
  }
}

/** Owns one bounded subscription loop. Foreground operations never await this class. */
module.exports = class ClientTelemetry {
  /**
   * @param {{kind:string,metrics:object,transport:object,clock?:object,logger?:object,config:object}} options
   * config is a trusted copied factory snapshot; makeError/resources are private factory functions.
   * @throws {KafkaJSInvalidConfigurationError} invalid private bounds
   */
  constructor({ kind, metrics, transport, clock = CLOCK, logger = null, config = {} }) {
    Object.assign(this, { kind, metrics, transport, clock, logger, config })
    this.maxBytes = config.telemetryMaxBytes ?? 1048576
    if (!Number.isInteger(this.maxBytes) || this.maxBytes < 1 || this.maxBytes > 1048576)
      throw new KafkaJSInvalidConfigurationError('Invalid telemetry byte limit')
    this.state = config.enableMetricsPush === false ? 'DISABLED' : 'DORMANT'
    this.connected = false
    this.unsupported = false
    this.stopped = false
    this.sequence = 0n
    this.lifecycle = 0n
    this.transportRevision = 0n
    this.subscription = null
    this.uuid = null
    this.handle = null
    this.candidate = null
    this.payload = new TelemetryPayload(0n)
    this.running = null
    this.timer = null
    this.timerRevision = 0n
    this.watchdog = null
    this.waiterTimer = null
    this.waiters = new Set()
    this.refused = new Set()
    this.failures = 0
    this.throttleUntil = 0
    this.penalty = null
    this.recoveryUsed = false
    this.rejectedCodecs = new Set()
    this.hadFinal = false
    this.reopen = null
    this.closing = null
    this.omittedPoints = 0n
    this.warnedSubscription = null
    this.transport.onCandidatesChanged?.(() => {
      if (this.connected && this.state === 'DORMANT') this.resume()
    })
  }

  /**
   * @param {{timeout?:number}|undefined} options public getter options
   * @returns {number} validated timeout in milliseconds
   * @throws {KafkaJSInvalidConfigurationError} invalid options; used by a client that owns no machine
   */
  static waitTimeout(options) {
    return waitTimeout(options)
  }

  /** @returns {bigint} non-reused local issuance, including across reopen */
  issue() {
    return ++this.sequence
  }

  /** @param {string} reason bounded own reason @returns {Error} fixed refusal; the owner may inject its own constructor */
  error(reason) {
    return this.config.makeError
      ? this.config.makeError(reason)
      : Object.assign(new KafkaJSNonRetriableError('Telemetry ' + reason), { reason })
  }

  /** @returns {void} arms after foreground usability; active calls are idempotent */
  resume() {
    if (this.config.enableMetricsPush === false || this.stopped) return
    if (this.connected && this.state !== 'DORMANT') return
    if (!this.connected) {
      this.lifecycle = this.issue()
      this.connected = true
      this.closing = null
      this.state = 'DORMANT'
      this.reopen = this.hadFinal
        ? { revision: this.issue(), lifecycle: this.lifecycle, used: false }
        : null
    }
    this.unsupported = false
    this.schedule('get', 0)
  }

  /** @returns {void} crash suspension retains UUID/descriptors but sends no final */
  suspend() {
    this.invalidate('not-connected')
    this.connected = false
    if (!['DISABLED', 'STOPPED'].includes(this.state)) this.state = 'DORMANT'
    this.reopen = null
    this.subscription = null
    this.metrics.resetTransient()
  }

  /** @param {string} reason waiter refusal @param {boolean} [keepSocket] close captures its own socket @returns {void} synchronous revocation */
  invalidate(reason, keepSocket = false) {
    this.lifecycle = this.issue()
    this.clearTimer()
    const op = this.running
    this.running = null
    if (op) {
      this.abandon(op)
      op.cancel()
    }
    this.clock.clearTimeout(this.watchdog)
    this.watchdog = null
    const old = this.handle
    this.handle = null
    this.transportRevision = this.issue()
    this.payload.retire()
    if (old && !keepSocket) this.observeRetire(old)
    this.settleWaiters(reason)
  }

  /** @param {object} handle captured dedicated socket @returns {void} rejection observed immediately */
  observeRetire(handle) {
    Promise.resolve()
      .then(() => handle.retire({ drainTimeout: 0 }))
      .catch(() => {})
  }

  /** @returns {void} permanently revokes a delivered old callback */
  clearTimer() {
    this.clock.clearTimeout(this.timer)
    this.timer = null
    this.timerRevision = this.issue()
  }

  /**
   * Schedules from completion with stepped timers and no catch-up burst.
   * @param {'get'|'push'} action next operation @param {number} delay relative milliseconds
   * @returns {void} only while ordinary lifecycle is usable
   */
  schedule(action, delay) {
    this.clearTimer()
    if (!this.connected || ['DISABLED', 'STOPPED', 'CLOSING', 'CLOSED'].includes(this.state)) return
    const revision = this.timerRevision
    const lifecycle = this.lifecycle
    const due = Math.max(this.clock.now() + delay, this.throttleUntil)
    const tick = () => {
      if (this.timerRevision !== revision || this.lifecycle !== lifecycle || !this.connected) return
      const remaining = due - this.clock.now()
      if (remaining > 0) {
        this.timer = this.clock.setTimeout(tick, Math.min(MAX_TIMER, remaining))
        return
      }
      this.timer = null
      this.run(action).catch(() => this.stop())
    }
    this.timer = this.clock.setTimeout(
      tick,
      Math.min(MAX_TIMER, Math.max(0, due - this.clock.now()))
    )
  }

  /**
   * Captures request authority before candidate lookup or any await. One request owns the watchdog.
   * @param {'get'|'push'} action next operation @returns {Promise<void>} isolated completion
   */
  async run(action) {
    if (this.running || !this.connected) return
    const op = {
      revision: this.issue(),
      lifecycle: this.lifecycle,
      transport: this.transportRevision,
      subscription: action === 'push' ? this.subscription : null,
      handle: this.handle,
      written: false,
      snapshot: null,
      deadline: this.clock.now() + 30000,
    }
    const cancelled = new Promise((resolve) => {
      op.cancel = () => resolve(null)
    })
    this.running = op
    const current = () => this.current(op)
    const work = async () => {
      if (!op.handle) {
        const candidates = this.transport.candidates()
        this.refused = new Set(
          [...this.refused].filter((revision) =>
            candidates.some((c) => c.endpointRevision === revision)
          )
        )
        const candidate = candidates.find(
          (c) =>
            !this.refused.has(c.endpointRevision) &&
            [71, 72].every(
              (key) => c.versions[key]?.minVersion <= 0 && c.versions[key]?.maxVersion >= 0
            )
        )
        if (!candidate) {
          this.state = 'DORMANT'
          this.unsupported = true
          this.metrics.pauseObservation()
          this.settleWaiters('unsupported')
          return null
        }
        op.candidate = this.candidate = candidate
        op.transport = this.transportRevision = this.issue()
        this.rejectedCodecs.clear()
        this.payload.resetTransport(op.transport)
        op.handle = await this.transport.open(candidate, {
          guard: () => (current() ? null : this.error('closed')),
          deadline: op.deadline,
          onAllocated: (handle) => {
            op.handle = handle
            if (current()) this.handle = handle
            else this.observeRetire(handle)
          },
        })
        if (!current()) return null
        this.handle = op.handle
      }
      if (!current()) return null
      const options = { guard: () => (current() ? null : this.error('closed')) }
      if (action === 'get') {
        this.state = 'GETTING'
        return op.handle.get({ clientInstanceId: this.uuid }, options)
      }
      if (!op.subscription) throw this.error('stopped')
      this.state = 'PUSHING'
      const resource = this.config.resources
        ? this.config.resources()
        : { values: [], isCurrent: () => true }
      op.phase = 'payload'
      op.resource = resource
      op.snapshot = this.metrics.snapshot(op.subscription, resource.values)
      if (!op.snapshot) return null
      const result = await this.payload.buildPayload(op.snapshot, {
        telemetryMaxBytes: this.maxBytes,
        brokerMaxBytes: op.subscription.telemetryMaxBytes,
        penaltyMaxBytes: this.penalty ?? undefined,
        acceptedCompressionTypes: op.subscription.acceptedCompressionTypes.filter(
          (codec) => !this.rejectedCodecs.has(codec)
        ),
        isCurrent: () => current() && resource.isCurrent() && this.metrics.isCurrent(op.snapshot),
      })
      if (!result || !current() || !resource.isCurrent() || !this.metrics.isCurrent(op.snapshot))
        return null
      op.result = result
      op.phase = 'push'
      options.guard = () =>
        current() && resource.isCurrent() && (op.written || this.metrics.isCurrent(op.snapshot))
          ? null
          : this.error('closed')
      options.onWrite = () => {
        const refusal = options.guard()
        if (refusal) throw refusal
        if (op.written) throw this.error('stopped')
        op.written = true
        this.metrics.written(op.snapshot, result.includedIdentities)
        this.omittedPoints += BigInt(result.omittedIdentities.length)
        if (
          result.omittedIdentities.length &&
          this.warnedSubscription !== op.subscription.revision
        ) {
          this.warnedSubscription = op.subscription.revision
          // Application logging must run after the synchronous commit/write section.
          Promise.resolve().then(() => this.warn('Telemetry points omitted'))
        }
      }
      return op.handle.push(
        {
          clientInstanceId: this.uuid,
          subscriptionId: op.subscription.subscriptionId,
          terminating: false,
          compressionType: result.compressionType,
          metrics: result.metrics,
        },
        options
      )
    }
    this.watchdog = this.clock.setTimeout(() => {
      if (this.running !== op) return
      op.expired = true
      if (op.handle) this.observeRetire(op.handle)
      op.cancel()
    }, 30000)
    try {
      const response = await Promise.race([Promise.resolve().then(work), cancelled])
      if (!this.owns(op)) return
      if (op.expired || this.clock.now() >= op.deadline)
        throw Object.assign(this.error('timeout'), { retriable: true })
      if (response === null) {
        if (this.state !== 'DORMANT') this.schedule(action, this.interval())
        return
      }
      this.throttle(response)
      if (!current()) return
      if (action === 'get') this.install(response)
      else {
        this.failures = 0
        this.penalty = null
        this.recoveryUsed = false
        this.reopen = null
        this.hadFinal = false
        this.state = 'READY'
        this.schedule('push', this.interval())
      }
    } catch (error) {
      if (this.owns(op)) {
        const expired = op.expired || this.clock.now() >= op.deadline
        if (!expired && !current()) return
        if (
          !expired &&
          op.resource &&
          (!op.resource.isCurrent() ||
            (op.snapshot && !op.written && !this.metrics.isCurrent(op.snapshot)))
        ) {
          this.schedule('push', this.interval())
          return
        }
        const failure = expired ? Object.assign(this.error('timeout'), { retriable: true }) : error
        this.handleError(failure, action, op)
      }
    } finally {
      this.abandon(op)
      if (this.running === op) {
        this.running = null
        this.clock.clearTimeout(this.watchdog)
        this.watchdog = null
      }
    }
  }

  /** @param {object} op captured request @returns {boolean} owner identity excluding deadline */
  owns(op) {
    return (
      this.running === op &&
      this.lifecycle === op.lifecycle &&
      this.connected &&
      !['STOPPED', 'CLOSING', 'CLOSED'].includes(this.state)
    )
  }

  /** @param {object} op captured request @returns {boolean} complete ordinary authority */
  current(op) {
    return (
      this.owns(op) &&
      this.transportRevision === op.transport &&
      this.clock.now() < op.deadline &&
      (!op.subscription || this.subscription === op.subscription)
    )
  }

  /** @param {object} op sample owner @returns {void} unsent work merges back once; written work never rewinds */
  abandon(op) {
    if (op.snapshot && !op.written && !op.abandoned) {
      op.abandoned = true
      this.metrics.abandon(op.snapshot)
    }
  }

  /**
   * Validates into temporary owned metadata before publishing any field or UUID.
   * @param {object} value complete successful Get @returns {void} one absolute replacement
   * @throws {Error} malformed field or changed accepted UUID
   */
  install(value) {
    const id = value.clientInstanceId
    const int32 = (n) => Number.isInteger(n) && n >= -2147483648 && n <= 2147483647
    if (
      (!this.uuid && (typeof id !== 'string' || !UUID.test(id))) ||
      (id !== null && this.uuid && id !== this.uuid) ||
      !int32(value.subscriptionId) ||
      !int32(value.pushIntervalMs) ||
      !int32(value.telemetryMaxBytes) ||
      value.telemetryMaxBytes <= 0 ||
      typeof value.deltaTemporality !== 'boolean' ||
      !Array.isArray(value.acceptedCompressionTypes) ||
      value.acceptedCompressionTypes.length > 128 ||
      value.acceptedCompressionTypes.some((n) => !Number.isInteger(n) || n < -128 || n > 127) ||
      !Array.isArray(value.requestedMetrics) ||
      value.requestedMetrics.length > 256 ||
      value.requestedMetrics.some((n) => typeof n !== 'string' || Buffer.byteLength(n) > 256) ||
      value.requestedMetrics.reduce((sum, n) => sum + Buffer.byteLength(n), 0) > 16384
    )
      throw this.error('stopped')
    const subscription = Object.freeze({
      revision: this.issue(),
      subscriptionId: value.subscriptionId,
      acceptedCompressionTypes: Object.freeze([...value.acceptedCompressionTypes]),
      pushIntervalMs: value.pushIntervalMs <= 0 ? 300000 : Math.max(100, value.pushIntervalMs),
      telemetryMaxBytes: Math.min(this.maxBytes, value.telemetryMaxBytes),
      deltaTemporality: value.deltaTemporality,
      requestedMetrics: Object.freeze([...value.requestedMetrics]),
    })
    this.metrics.select(subscription)
    this.uuid = this.uuid || id
    this.subscription = subscription
    this.settleWaiters()
    const empty = subscription.requestedMetrics.length === 0
    this.state = empty ? 'POLL' : 'READY'
    if (empty) this.failures = 0
    this.schedule(
      empty ? 'get' : 'push',
      empty ? this.interval() : Math.round(this.interval() * (0.5 + this.clock.random()))
    )
  }

  /** @returns {number} accepted interval or fixed pre-subscription fallback */
  interval() {
    return this.subscription?.pushIntervalMs ?? 300000
  }

  /** @param {object} value trusted response/error @returns {void} private monotonic quota deadline @throws {Error} invalid time */
  throttle(value) {
    const duration = value.clientSideThrottleTime ?? value.throttleTime ?? 0
    if (!Number.isInteger(duration) || duration < -2147483648 || duration > 2147483647)
      throw this.error('stopped')
    this.throttleUntil = Math.max(this.throttleUntil, this.clock.now() + Math.max(0, duration))
  }

  /**
   * Reads bounded own data fields without invoking arbitrary error getters or string coercion.
   * Numeric protocol codes outrank transport retry hints; causes are traversed at most eight times.
   * @param {*} error original thrown value
   * @returns {object} fixed numeric/retry classification, never the original error or cause
   */
  classify(error) {
    const result = { code: undefined, retriable: false, clientSideThrottleTime: 0 }
    const seen = new Set()
    try {
      for (
        let depth = 0;
        depth < 8 && error && typeof error === 'object' && !seen.has(error);
        depth++
      ) {
        seen.add(error)
        const field = (name) => Object.getOwnPropertyDescriptor(error, name)?.value
        const code = field('code')
        if (result.code === undefined && typeof code === 'number') result.code = code
        result.retriable = result.retriable || field('retriable') === true
        if (depth === 0) result.clientSideThrottleTime = field('clientSideThrottleTime') ?? 0
        error = field('cause')
      }
      return result
    } catch {
      return { code: undefined, retriable: false, clientSideThrottleTime: 0 }
    }
  }

  /**
   * Applies the numeric table without a foreground retrier. A successful Get cannot refill42.
   * @param {Error} error trusted stack error @param {'get'|'push'} action request kind
   * @param {object} op original request identity @returns {void} finite local recovery
   */
  handleError(error, action, op) {
    if (op.expired || this.clock.now() >= op.deadline) {
      this.retire(op)
      this.state = 'BACKOFF'
      this.schedule('get', this.backoff())
      return
    }
    const failure = this.classify(error)
    this.throttle(failure)
    const code = failure.code
    if (code === 35) {
      if (this.candidate) this.refused.add(this.candidate.endpointRevision)
      this.retire(op)
      this.state = 'BACKOFF'
      this.schedule('get', this.backoff())
      return
    }
    if ((code === 117 || code === 76) && action === 'push') {
      if (code === 76 && op.result) this.rejectedCodecs.add(op.result.compressionType)
      const immediate = !this.recoveryUsed
      this.recoveryUsed = true
      this.state = 'BACKOFF'
      this.schedule('get', immediate ? 0 : this.backoff())
      return
    }
    if (code === 117 || code === 76) {
      this.state = 'BACKOFF'
      this.schedule('get', this.backoff())
      return
    }
    if (code === 118) {
      this.penalty = Math.max(
        1,
        Math.floor(
          Math.min(
            this.maxBytes,
            this.subscription?.telemetryMaxBytes ?? this.maxBytes,
            this.penalty ?? this.maxBytes
          ) / 2
        )
      )
      this.state = 'BACKOFF'
      this.schedule('get', this.interval())
      return
    }
    if (code === 89) {
      this.state = 'BACKOFF'
      this.schedule('get', this.interval())
      return
    }
    if (
      code === 42 &&
      action === 'push' &&
      this.reopen &&
      this.reopen.lifecycle === op.lifecycle &&
      !this.reopen.used
    ) {
      this.reopen.used = true
      this.retire(op)
      this.state = 'BACKOFF'
      this.schedule('get', this.backoff())
      return
    }
    if (op.phase === 'payload') {
      this.state = 'BACKOFF'
      this.schedule('get', this.backoff())
      return
    }
    if (code === undefined && failure.retriable === true) {
      this.retire(op)
      this.state = 'BACKOFF'
      this.schedule('get', this.backoff())
      return
    }
    this.stop()
  }

  /** @returns {number} jittered exponential delay with a final cap */
  backoff() {
    const delay = Math.min(30000, 1000 * 2 ** Math.min(this.failures++, 5))
    return Math.min(30000, Math.round(delay * (0.8 + 0.4 * this.clock.random())))
  }

  /** @param {object} op original request @returns {void} captured transport only */
  retire(op) {
    if (op.handle) this.observeRetire(op.handle)
    if (this.handle === op.handle) this.handle = null
    this.transportRevision = this.issue()
    this.payload.retire()
  }

  /** @param {string} message fixed own literal @returns {void} hostile logger contained */
  warn(message) {
    try {
      this.logger?.warn(message)
    } catch {}
  }

  /** @returns {void} terminal object-local transition; no foreground policy update */
  stop() {
    if (this.state === 'STOPPED') return
    this.invalidate('stopped')
    this.state = 'STOPPED'
    this.stopped = true
    this.warn('Telemetry stopped')
  }

  /**
   * Resolves a cached accepted ID, otherwise waits under an independent original deadline measured
   * from entry, so a slow options accessor consumes the same budget.
   * Precedence: validation, disabled, cached ID, closed, not connected, unsupported, stopped, timeout,
   * then admission, which first rejects waiters already past their own deadline.
   * @param {{timeout?:number}} [options] timeout defaults to 60000; zero reads the cache only
   * @returns {Promise<string>} bounded to 64 live waiters; no implicit connection
   * @throws {Error} asynchronous validation or typed telemetry refusal, including `limit`
   */
  async clientInstanceId(options) {
    const entered = this.clock.now()
    const timeout = waitTimeout(options)
    if (this.config.enableMetricsPush === false) throw this.error('disabled')
    if (this.uuid) return this.uuid
    if (['CLOSING', 'CLOSED'].includes(this.state)) throw this.error('closed')
    if (!this.connected) throw this.error('not-connected')
    if (this.unsupported) throw this.error('unsupported')
    if (this.state === 'STOPPED') throw this.error('stopped')
    const deadline = entered + timeout
    if (!timeout || this.clock.now() >= deadline) throw this.error('timeout')
    this.reclaimExpiredWaiters()
    if (this.waiters.size === 64) throw this.error('limit')
    return new Promise((resolve, reject) => {
      this.waiters.add({ resolve, reject, deadline, lifecycle: this.lifecycle })
      this.armWaiters()
    })
  }

  /**
   * Frees the slots of waiters whose own deadline has passed without waiting for timer delivery.
   * Synchronous and local: no Get, transport or timer is touched; the admitting caller arms once.
   * @returns {void} each expired waiter is rejected with a timeout; live waiters are untouched
   */
  reclaimExpiredWaiters() {
    const now = this.clock.now()
    for (const waiter of this.waiters) {
      if (now < waiter.deadline) continue
      this.waiters.delete(waiter)
      waiter.reject(this.error('timeout'))
    }
  }

  /** @returns {void} one earliest waiter deadline timer */
  armWaiters() {
    this.clock.clearTimeout(this.waiterTimer)
    this.waiterTimer = null
    if (!this.waiters.size) return
    const due = Math.min(...[...this.waiters].map((w) => w.deadline))
    this.waiterTimer = this.clock.setTimeout(
      () => this.settleWaiters(),
      Math.min(MAX_TIMER, Math.max(0, due - this.clock.now()))
    )
  }

  /** @param {string} [reason] terminal/suspension reason @returns {void} expired callers settle timeout before removal */
  settleWaiters(reason) {
    for (const waiter of this.waiters) {
      const expired = this.clock.now() >= waiter.deadline
      if (!expired && !reason && !this.uuid) continue
      this.waiters.delete(waiter)
      if (expired || reason) waiter.reject(this.error(expired ? 'timeout' : reason))
      else waiter.resolve(this.uuid)
    }
    this.armWaiters()
  }

  /** @param {object} metric numeric descriptor @returns {void} explicit storage only @throws {Error} validation/capacity refusal */
  registerMetricForSubscription(metric) {
    if (this.config.enableMetricsPush === false) throw this.error('disabled')
    try {
      this.metrics.register(metric)
    } catch (error) {
      if (error instanceof KafkaJSNonRetriableError && error.reason === 'limit')
        throw this.error('limit')
      throw error
    }
  }

  /** @param {object} identity canonical descriptor identity @returns {void} absent is a no-op @throws {Error} explicit validation refusal */
  unregisterMetricFromSubscription(identity) {
    this.metrics.unregister(identity)
  }

  /**
   * Captures a distinct closing authority synchronously; no retry, setup or reconnect is allowed.
   * The settled episode keeps its result but releases its retired handle and subscription.
   * @param {{deadline?:number,remaining?:Function,zero?:boolean}} budget original core remaining predicate
   * @returns {Promise<void>} one captured final within min1000ms/original remaining budget
   */
  close({ remaining = () => Infinity, zero = false } = {}) {
    if (this.closing) return this.closing.promise
    const old = this.handle
    const subscription = this.subscription
    const eligible = !!(
      old?.isConnected() &&
      this.uuid &&
      subscription?.requestedMetrics.length &&
      !this.reopen &&
      !['DISABLED', 'DORMANT', 'STOPPED'].includes(this.state)
    )
    const deadline = this.clock.now() + Math.max(0, Math.min(1000, zero ? 0 : remaining()))
    const episode = {
      revision: this.issue(),
      handle: old,
      subscription,
      uuid: this.uuid,
      deadline,
      attempted: false,
    }
    this.invalidate('closed', eligible)
    this.connected = false
    this.state = 'CLOSING'
    this.closing = episode
    const current = () =>
      this.closing === episode &&
      this.state === 'CLOSING' &&
      this.clock.now() < deadline &&
      remaining() > 0
    episode.promise = (async () => {
      let snapshot = null
      let written = false
      let timer
      const expired = new Promise((resolve) => {
        timer = this.clock.setTimeout(
          () => {
            if (this.closing === episode) {
              this.payload.retire()
              if (old) this.observeRetire(old)
            }
            resolve()
          },
          Math.max(0, deadline - this.clock.now())
        )
      })
      try {
        if (!eligible || !current()) return
        const resources = this.config.resources
          ? this.config.resources()
          : { values: [], isCurrent: () => true }
        snapshot = this.metrics.snapshot(subscription, resources.values)
        if (!snapshot) return
        this.payload.resetTransport(this.transportRevision)
        const work = async () => {
          const payload = await this.payload.buildPayload(snapshot, {
            telemetryMaxBytes: this.maxBytes,
            brokerMaxBytes: subscription.telemetryMaxBytes,
            penaltyMaxBytes: this.penalty ?? undefined,
            acceptedCompressionTypes: subscription.acceptedCompressionTypes.filter(
              (c) => !this.rejectedCodecs.has(c)
            ),
            isCurrent: () => current() && resources.isCurrent() && this.metrics.isCurrent(snapshot),
          })
          if (!payload || !current()) return
          const guard = () =>
            current() && resources.isCurrent() && (written || this.metrics.isCurrent(snapshot))
              ? null
              : this.error('closed')
          await old.push(
            {
              clientInstanceId: episode.uuid,
              subscriptionId: subscription.subscriptionId,
              terminating: true,
              compressionType: payload.compressionType,
              metrics: payload.metrics,
            },
            {
              guard,
              onWrite: () => {
                const refusal = guard()
                if (refusal) throw refusal
                if (episode.attempted) throw this.error('closed')
                episode.attempted = true
                this.hadFinal = true
                written = true
                this.metrics.written(snapshot, payload.includedIdentities)
              },
            }
          )
        }
        await Promise.race([Promise.resolve().then(work), expired])
      } catch {
      } finally {
        this.clock.clearTimeout(timer)
        if (snapshot && !written) this.metrics.abandon(snapshot)
        if (old) await old.retire({ drainTimeout: 0 }).catch(() => {})
        episode.handle = null
        episode.subscription = null
        if (this.closing === episode) {
          this.payload.retire()
          try {
            this.metrics.resetTransient()
          } catch {}
          this.subscription = null
          this.state = 'CLOSED'
        }
      }
    })()
    return episode.promise
  }
}
