const {
  KafkaJSConnectionError,
  KafkaJSRequestTimeoutError,
  KafkaJSInvalidConfigurationError,
} = require('../errors')

/** Setup and retry timing without retaining connection objects. */
class ConnectionSetupPolicy {
  /**
   * Resolves omitted values before network work. Only legacy timeout options coerce strings.
   * @param {object} explicit public option names or internal timeout/backoff aliases
   * @param {() => number} random uniform source in [0, 1]
   * @returns {ConnectionSetupPolicy} immutable timing policy
   * @throws {KafkaJSInvalidConfigurationError} for invalid ranges or types
   */
  static resolve(explicit = {}, random = Math.random) {
    const names = [
      'connectionTimeout',
      'connectionTimeoutMax',
      'reconnectBackoff',
      'reconnectBackoffMax',
      'authenticationTimeout',
    ]
    const aliases = ['timeout', 'timeoutMax', 'backoff', 'backoffMax', 'authenticationTimeout']
    const values = names.map((name, i) => {
      let value = explicit[name] === undefined ? explicit[aliases[i]] : explicit[name]
      if (value === undefined) return value
      if ((i === 0 || i === 4) && typeof value === 'string' && value.trim() !== '')
        value = Number(value)
      if (
        typeof value !== 'number' ||
        !Number.isInteger(value) ||
        value < (i === 2 || i === 3 ? 0 : 1) ||
        value > 2147483647
      ) {
        throw new KafkaJSInvalidConfigurationError(
          `${name} must be an integer in its supported range`
        )
      }
      return value
    })
    const [timeout, timeoutMax, backoff, backoffMax, authenticationTimeout = 10000] = values
    const pair = (base, max, defaultBase, defaultMax, name) => {
      const low = base ?? Math.min(defaultBase, max ?? defaultMax)
      const high = max ?? (base === undefined ? defaultMax : base)
      if (high < low)
        throw new KafkaJSInvalidConfigurationError(`${name} must be at least its base`)
      return [low, high]
    }
    const [timeoutBase, timeoutCeiling] = pair(timeout, timeoutMax, 10000, 30000, names[1])
    const [waitBase, waitCeiling] = pair(backoff, backoffMax, 50, 1000, names[3])
    return Object.freeze(
      new ConnectionSetupPolicy({
        timeoutBase,
        timeoutCeiling,
        waitBase,
        waitCeiling,
        authenticationTimeout,
        random,
      })
    )
  }

  /** @param {object} values normalized bases, ceilings, authentication cap and random source */
  constructor({ timeoutCeiling, waitCeiling, ...values }) {
    Object.assign(this, values)
    this.timeoutMax = timeoutCeiling
    this.waitMax = waitCeiling
  }

  /** @returns {number} maximum setup budget */
  get timeoutCeiling() {
    return this.timeoutMax
  }

  /** @returns {number} maximum retry delay */
  get waitCeiling() {
    return this.waitMax
  }

  /**
   * Samples a bounded Java-style interval, truncating to integer milliseconds.
   * @param {number} base interval before growth
   * @param {number} max absolute cap
   * @param {number} exponent consecutive failure exponent
   * @returns {number} delay; flat pairs have no jitter
   */
  interval(base, max, exponent) {
    if (base === 0 || base === max) return base
    const term = Math.min(max, base * 2 ** Math.min(exponent, 31))
    return Math.min(max, Math.floor(term * (0.8 + 0.4 * this.random())))
  }

  /** @param {number} failures consecutive physical failures @returns {number} setup budget */
  timeoutFor(failures) {
    return this.interval(this.timeoutBase, this.timeoutCeiling, failures)
  }

  /** @param {number} failures consecutive physical failures @returns {number} retry wait */
  waitFor(failures) {
    return failures === 0 ? 0 : this.interval(this.waitBase, this.waitCeiling, failures - 1)
  }
}

/** Revisioned failure state shared by every pool for one client. */
class NodeSetupStates {
  /** @param {ConnectionSetupPolicy} policy timing rules */
  constructor(policy) {
    this.policy = policy
    this.entries = new Map()
    this.live = new Map()
    this.endpoints = new Set()
    this.sequence = 0n
  }

  /**
   * Replaces the bootstrap/metadata endpoint union and prunes removed settled addresses.
   * @param {Iterable<string>} addresses eligible endpoints
   * @returns {void}
   */
  retain(addresses) {
    this.endpoints = new Set(addresses)
    for (const address of this.entries.keys()) this.prune(address)
  }

  /** @param {string} address endpoint @returns {void} */
  prune(address) {
    if (!this.endpoints.has(address) && !this.live.has(address)) this.entries.delete(address)
  }

  /** @param {string} address endpoint @param {number} now monotonic ms @returns {object} snapshot */
  state(address, now) {
    const entry = this.entries.get(address)
    return {
      failures: entry?.failures ?? 0,
      retryAt: entry?.retryAt ?? now,
      waitMs: Math.max(0, (entry?.retryAt ?? now) - now),
    }
  }

  /**
   * Captures a revision before any await. Tokens never repeat after deletion.
   * @param {string} address endpoint
   * @returns {object} one settlement token
   */
  begin(address) {
    let entry = this.entries.get(address)
    if (!entry) {
      entry = { revision: ++this.sequence, failures: 0, retryAt: 0 }
      this.entries.set(address, entry)
    }
    const token = { id: ++this.sequence, entry, active: true }
    if (!this.live.has(address)) this.live.set(address, new Set())
    this.live.get(address).add(token)
    return token
  }

  /**
   * Records one physical failure unless sibling readiness superseded its revision.
   * @param {string} address endpoint
   * @param {number} now monotonic failure time
   * @param {object} token original begin token
   * @returns {boolean} whether this failure applied
   */
  failed(address, now, token) {
    if (!token?.active || this.entries.get(address) !== token.entry) return false
    const entry = token.entry
    entry.failures += 1
    entry.retryAt = now + this.policy.waitFor(entry.failures)
    this.finish(address, token)
    return true
  }

  /**
   * Deletes failure state and invalidates older reports without retaining tombstones.
   * @param {string} address endpoint
   * @param {object} [token] original token when called by an attempt
   * @returns {boolean} whether readiness still belongs to this revision
   */
  ready(address, token) {
    if (token && (!token.active || this.entries.get(address) !== token.entry)) {
      this.finish(address, token)
      return false
    }
    this.entries.delete(address)
    if (token) this.finish(address, token)
    return true
  }

  /** @param {string} address endpoint @param {object} token original token @returns {void} */
  finish(address, token) {
    token.active = false
    const live = this.live.get(address)
    live?.delete(token)
    if (live?.size === 0) this.live.delete(address)
    this.prune(address)
  }

  /** Invalidates all reports on final user disconnect. @returns {void} */
  clear() {
    this.entries.clear()
    this.endpoints.clear()
  }
}

/** One abortable wait followed by one setup budget, or one standalone SASL cap. */
class SetupAttempt {
  /**
   * @param {object} options
   * @param {NodeSetupStates|null} [options.states] null for authentication-only work
   * @param {string} options.address endpoint
   * @param {ConnectionSetupPolicy} options.policy timing rules
   * @param {number} [options.timeoutMs] authentication-only cap
   * @param {() => number} [options.now] monotonic clock
   */
  constructor({
    states = null,
    address,
    policy,
    timeoutMs,
    now = () => Number(process.hrtime.bigint()) / 1e6,
  }) {
    this.brokerManaged = false
    this.states = states
    this.address = address
    this.policy = policy
    this.now = now
    this.token = states?.begin(address)
    const snapshot = states?.state(address, now())
    this.retryAt = snapshot?.retryAt ?? now()
    this.timeout = timeoutMs ?? policy.timeoutFor(snapshot?.failures ?? 0)
    this.phase = 'TCP/TLS'
    this.status = 'active'
    this.timer = null
    this.deadline = Infinity
    this.limit = Infinity
    this.error = null
    this.ended = new Promise((resolve, reject) => {
      this.reject = reject
    })
    this.ended.catch(() => {})
  }

  /** @returns {number} remaining backoff */
  waitMs() {
    return Math.max(0, this.retryAt - this.now())
  }

  /** Waits abortably before the setup clock starts. @returns {Promise<void>} */
  async wait() {
    this.assertCurrent()
    const delay = this.waitMs()
    if (delay > 0) {
      try {
        await Promise.race([
          new Promise((resolve) => {
            this.timer = setTimeout(resolve, delay)
          }),
          this.ended,
        ])
      } finally {
        clearTimeout(this.timer)
        this.timer = null
      }
    }
    this.assertCurrent()
  }

  /**
   * Starts one budget. Expiry records failure before synchronous original-generation teardown.
   * @param {(error: Error) => void} onExpire teardown callback
   * @returns {void}
   */
  arm(onExpire) {
    this.assertCurrent()
    if (this.deadline !== Infinity) return
    this.onExpire = onExpire
    this.deadline = this.now() + this.timeout
    this.schedule(this.deadline)
  }

  /** @param {number} deadline monotonic phase/setup deadline @returns {void} */
  schedule(deadline) {
    clearTimeout(this.timer)
    this.limit = deadline
    this.timer = setTimeout(
      () => {
        if (this.now() < this.limit) this.schedule(this.limit)
        else this.expire()
      },
      Math.min(2147483647, Math.max(0, Math.ceil(deadline - this.now())))
    )
  }

  /**
   * Settles locally without waiting for an abandoned supplier. Its continuations use this guard.
   * @param {Promise<*>} promise already-started work
   * @param {{ phase: string, cap?: number }} options whole-phase cap
   * @returns {Promise<*>} result before expiry
   * @throws {KafkaJSConnectionError} after expiry/cancellation
   */
  async guard(promise, { phase, cap } = {}) {
    promise.catch(() => {})
    this.assertCurrent()
    this.phase = phase ?? this.phase
    if (cap !== undefined) this.schedule(Math.min(this.deadline, this.now() + cap))
    try {
      const result = await Promise.race([promise, this.ended])
      this.assertCurrent()
      return result
    } finally {
      if (cap !== undefined && this.status === 'active') this.schedule(this.deadline)
    }
  }

  /**
   * Settles expiry before teardown; boundary checks share this path. TCP/TLS keeps the
   * legacy connection-error text; phase and budget remain available as error properties.
   * @returns {void}
   */
  expire() {
    if (this.status !== 'active') return
    const error = new KafkaJSConnectionError(
      this.phase === 'TCP/TLS'
        ? 'Connection timeout'
        : `Connection setup ${this.phase} timed out for ${this.address} (node timeout ${this.timeout}ms)`,
      { broker: this.address }
    )
    error.phase = this.phase
    error.timeout = this.timeout
    this.fail(error)
    this.onExpire?.(error)
  }

  /** @returns {void} @throws {Error} when its captured private authority or original setup ended */
  assertCurrent() {
    const refusal = this.externalGuard && this.externalGuard()
    if (refusal) throw refusal
    if (this.now() >= this.limit) this.expire()
    if (this.status !== 'active')
      throw (
        this.error ||
        new KafkaJSConnectionError('Connection setup ended', {
          broker: this.address,
        })
      )
  }

  /** @returns {Error|null} private pre-write refusal */
  refusal() {
    try {
      this.assertCurrent()
      return null
    } catch (error) {
      return error
    }
  }

  /** Clears the timer and resets only the original revision. @returns {void} */
  ready() {
    this.assertCurrent()
    this.states?.ready(this.address, this.token)
    this.status = 'ready'
    clearTimeout(this.timer)
    this.timer = null
  }

  /**
   * Counts transport/expiry/request-timeout failures before ready, once.
   * @param {Error} error original setup error
   * @returns {void}
   */
  fail(error) {
    if (this.status !== 'active') return
    const physical =
      error instanceof KafkaJSConnectionError || error instanceof KafkaJSRequestTimeoutError
    if (physical && error.code !== 58 && this.states)
      this.states.failed(this.address, this.now(), this.token)
    if (this.states && this.token.active) this.states.finish(this.address, this.token)
    this.end('failed', error)
  }

  /** @param {Error} [error] deliberate cancellation reason @returns {void} */
  cancel(
    error = new KafkaJSConnectionError('Connection setup cancelled', {
      broker: this.address,
    })
  ) {
    if (this.status !== 'active') return
    if (this.states) this.states.finish(this.address, this.token)
    this.end('cancelled', error)
  }

  /** @param {string} status final status @param {Error} error rejection @returns {void} */
  end(status, error) {
    this.status = status
    this.error = error
    clearTimeout(this.timer)
    this.timer = null
    this.reject(error)
  }
}

module.exports = { ConnectionSetupPolicy, NodeSetupStates, SetupAttempt }
