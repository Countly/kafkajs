const dns = require('dns')
const { inspect } = require('util')
const createRetry = require('../retry')
const { INT_32_MAX_VALUE } = require('../constants')
const { KafkaJSBootstrapResolutionError, KafkaJSInvalidConfigurationError } = require('../errors')

const IDLE = 'idle'
const PENDING = 'pending'
const OPEN = 'open'
const FAILED = 'failed'

/**
 * One attempt to open the gate: its deadline, its per-host retry state and the promise every
 * caller of `wait()` shares. A callback or timer that outlives its round compares against
 * `BootstrapResolution#round` and is ignored.
 */
class Round {
  /** @param {number} deadlineAt monotonic millisecond at which the round fails */
  constructor(deadlineAt) {
    this.deadlineAt = deadlineAt
    this.deadline = null
    /** @type {Map<string, { delay: number|null, timer: ReturnType<typeof setTimeout>|null }>} */
    this.hosts = new Map()
    this.lastError = null
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
    // A round whose callers have all gone is not an unhandled rejection.
    this.promise.catch(() => {})
  }
}

/**
 * A DNS readiness gate for the initial connection of one client. With a positive `timeout`,
 * `wait()` resolves once at least one bootstrap host resolves to an address, and rejects with one
 * cached `KafkaJSBootstrapResolutionError` when none does within the timeout. It only checks
 * readiness: the connection still dials the original hostname, so TLS server names and custom
 * socket factories are unaffected. With `timeout` 0 it does nothing.
 *
 * The deadline is absolute: a host list or its failure, a lookup or a lookup result that arrives
 * once it has passed fails the round with the cached error, even before the deadline timer fires.
 *
 * States: idle, pending, open (permanent) and failed (permanent, until the client is recreated).
 * `cancel()` ends a pending round and returns to idle, so the next `wait()` starts a fresh one.
 */
class BootstrapResolution {
  /**
   * @param {object} options
   * @param {number} [options.timeout=0] readiness deadline in milliseconds, 0 disabling the gate
   * @param {() => Promise<string[]>} options.resolveHosts the original hostnames of the bootstrap
   *   list, read once per round
   * @param {(host: string, options: { all: true }, callback: Function) => void} [options.lookup]
   *   `dns.lookup` by default
   * @param {import('../../types').RetryOptions} [options.retry] paces the retries of a failing host
   * @param {() => number} [options.now] monotonic milliseconds
   * @throws {KafkaJSInvalidConfigurationError} for an invalid `timeout`
   */
  constructor({
    timeout = 0,
    resolveHosts,
    lookup = (host, options, callback) => dns.lookup(host, options, callback),
    retry,
    now = () => performance.now(),
  }) {
    this.timeout = BootstrapResolution.validateTimeout(timeout)
    this.resolveHosts = resolveHosts
    this.lookup = lookup
    this.backoff = createRetry.backoff(retry)
    this.now = now
    this.state = IDLE
    /** @type {Round|null} */
    this.round = null
    /** @type {KafkaJSBootstrapResolutionError|null} */
    this.failure = null
  }

  /**
   * @param {unknown} value the configured `bootstrapResolveTimeout`
   * @returns {number} the value, an integer from 0 to the largest timer delay
   * @throws {KafkaJSInvalidConfigurationError} for anything else
   */
  static validateTimeout(value) {
    if (Number.isInteger(value) && value >= 0 && value <= INT_32_MAX_VALUE) return value
    throw new KafkaJSInvalidConfigurationError(
      `bootstrapResolveTimeout must be an integer from 0 to ${INT_32_MAX_VALUE}, got ${inspect(value)}`
    )
  }

  /**
   * Resolves at once for a disabled or open gate, shares the pending round of concurrent callers
   * and starts a round when idle. The deadline error is cached: every later call rejects with that
   * same object and does no lookup.
   * @returns {Promise<void>}
   * @throws {KafkaJSBootstrapResolutionError} when the deadline passed without any host resolving
   * @throws {Error} the error given to `cancel()`, or the failure of the bootstrap list source
   */
  wait() {
    if (this.timeout === 0 || this.state === OPEN) return Promise.resolve()
    if (this.state === FAILED) return Promise.reject(this.failure)
    if (this.state === IDLE) this.start()
    return this.round.promise
  }

  /**
   * Ends a pending round: its callers reject with `error`, its timers are cleared and any lookup
   * still in flight is ignored when it returns. An open or failed gate keeps its state.
   * @param {Error} error what the waiting callers receive
   * @returns {void}
   */
  cancel(error) {
    if (this.state === PENDING) this.end(IDLE, (round) => round.reject(error))
  }

  /** @private @returns {void} */
  start() {
    const round = new Round(this.now() + this.timeout)
    this.round = round
    this.state = PENDING
    round.deadline = setTimeout(() => this.expire(round), this.timeout)
    new Promise((resolve) => resolve(this.resolveHosts())).then(
      (hosts) => this.dispatch(round, hosts),
      (error) => this.listFailed(round, error)
    )
  }

  /**
   * A bootstrap list source that fails before the deadline ends the round with its own error and
   * returns to idle; one that fails once the deadline has passed fails the round with the cached
   * error, the source's error as its cause.
   * @private
   * @param {Round} round
   * @param {Error} error
   * @returns {void}
   */
  listFailed(round, error) {
    if (!this.isCurrent(round)) return
    if (!this.expired(round)) return this.end(IDLE, () => round.reject(error))
    round.lastError = error
    this.expire(round)
  }

  /**
   * @private
   * @param {Round} round
   * @returns {boolean} whether `round` is still the one in progress
   */
  isCurrent(round) {
    return this.round === round
  }

  /**
   * @private
   * @param {Round} round
   * @returns {boolean} whether the round's absolute deadline has passed
   */
  expired(round) {
    return this.now() >= round.deadlineAt
  }

  /**
   * Starts one lookup per distinct host, all at once. A list that arrives after the deadline
   * fails the round instead.
   * @private
   * @param {Round} round
   * @param {string[]} hosts
   * @returns {void}
   */
  dispatch(round, hosts) {
    if (!this.isCurrent(round)) return
    if (this.expired(round)) return this.expire(round)
    for (const host of new Set(hosts)) {
      if (!this.isCurrent(round)) return
      round.hosts.set(host, { delay: null, timer: null })
      this.attempt(round, host)
    }
  }

  /**
   * One lookup of `host`, or the round's failure once the deadline has passed. A synchronous
   * throw counts as a failed lookup.
   * @private
   * @param {Round} round
   * @param {string} host
   * @returns {void}
   */
  attempt(round, host) {
    if (this.expired(round)) return this.expire(round)
    const settle = (error, addresses) => this.settle(round, host, error, addresses)
    try {
      this.lookup(host, { all: true }, settle)
    } catch (error) {
      settle(error)
    }
  }

  /**
   * Opens the gate on an address, otherwise schedules the host's next attempt. Any result that
   * arrives after the deadline fails the round, a failed lookup still supplying the last error.
   * @private
   * @param {Round} round
   * @param {string} host
   * @param {Error|null} [error]
   * @param {Array<{ address: string }>} [addresses]
   * @returns {void}
   */
  settle(round, host, error, addresses) {
    if (!this.isCurrent(round)) return
    const resolved = !error && addresses?.length > 0
    if (!resolved) round.lastError = error ?? new Error(`${host} resolved to no address`)
    if (this.expired(round)) return this.expire(round)
    if (resolved) return this.end(OPEN, (current) => current.resolve())
    this.retryLater(round, host)
  }

  /**
   * Waits the retry schedule's next delay, clamped to the time left and to at least 1 ms, so a
   * failing lookup can never become a busy loop. The round fails instead once the deadline is due.
   * @private
   * @param {Round} round
   * @param {string} host
   * @returns {void}
   */
  retryLater(round, host) {
    const remaining = round.deadlineAt - this.now()
    if (remaining <= 0) return this.expire(round)
    const entry = round.hosts.get(host)
    entry.delay = entry.delay === null ? this.backoff.initial() : this.backoff.next(entry.delay)
    entry.timer = setTimeout(
      () => {
        entry.timer = null
        if (this.isCurrent(round)) this.attempt(round, host)
      },
      Math.max(1, Math.min(entry.delay, remaining))
    )
  }

  /**
   * Fails the round with the cached terminal error.
   * @private
   * @param {Round} round
   * @returns {void}
   */
  expire(round) {
    if (!this.isCurrent(round)) return
    const hosts = [...round.hosts.keys()]
    const where = hosts.length > 0 ? hosts.join(', ') : 'the bootstrap list'
    this.failure = new KafkaJSBootstrapResolutionError(
      `Bootstrap DNS resolution failed: none of ${where} resolved within ${this.timeout} ms` +
        (round.lastError ? ` (last error: ${round.lastError.message})` : ''),
      { cause: round.lastError ?? undefined }
    )
    this.end(FAILED, (current) => current.reject(this.failure))
  }

  /**
   * Leaves the pending state: clears every timer of the round, makes its late callbacks inert and
   * settles its callers.
   * @private
   * @param {string} state the state entered
   * @param {(round: Round) => void} settleCallers
   * @returns {void}
   */
  end(state, settleCallers) {
    const round = this.round
    clearTimeout(round.deadline)
    for (const { timer } of round.hosts.values()) clearTimeout(timer)
    round.hosts.clear()
    this.round = null
    this.state = state
    settleCallers(round)
  }
}

module.exports = BootstrapResolution
