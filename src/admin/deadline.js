const { AsyncLocalStorage } = require('node:async_hooks')
const createRetry = require('../retry')
const plainWaitFor = require('../utils/waitFor')
const {
  KafkaJSTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
} = require('../errors')

const context = new AsyncLocalStorage()
const DEFAULT_API_TIMEOUT = 60000
const MAX_API_TIMEOUT = 2147483647

/**
 * Validates a caller's admin budget before any work starts. Undefined means the supplied default.
 * @param {number|undefined} value configured or per-call milliseconds
 * @param {number} [fallback=60000] validated default
 * @returns {number} integer milliseconds
 * @throws {KafkaJSInvalidConfigurationError} for a value outside 1..2147483647
 */
const validateApiTimeout = (value, fallback = DEFAULT_API_TIMEOUT) => {
  if (value === undefined) return fallback
  if (!Number.isInteger(value) || value < 1 || value > MAX_API_TIMEOUT) {
    throw new KafkaJSInvalidConfigurationError('apiTimeout must be an integer from 1 to 2147483647')
  }
  return value
}

/** One immutable admin invocation, its monotonic deadline, and its local wait/delay ownership. */
class AdminDeadline {
  /**
   * Captures the budget once. Null is reserved for discovery ownership, which has no timer.
   * @param {string} method original public method name
   * @param {number|null} timeoutMs validated budget or internal discovery ownership
   * @returns {AdminDeadline} active invocation
   */
  constructor(method, timeoutMs) {
    this.method = method
    this.timeoutMs = timeoutMs
    this.token = Symbol(method)
    this.deadlineAt = timeoutMs === null ? Infinity : performance.now() + timeoutMs
    this.reason = null
    this.cleanups = new Set()
    this.timer = timeoutMs === null ? null : setTimeout(() => this.stop(this.timeout()), timeoutMs)
    this.guard = () => this.refusal()
  }

  /** @returns {KafkaJSTimeout} one stable error naming the original method and unknown outcome */
  timeout() {
    this.timeoutError ??= new KafkaJSTimeout(
      `${this.method} did not complete within ${this.timeoutMs} ms; outcome is unknown; a mutation may have been applied`
    )
    return this.timeoutError
  }

  /** @returns {Error|null} actual-clock refusal, including when timer delivery is withheld */
  refusal() {
    if (!this.reason && performance.now() >= this.deadlineAt) this.stop(this.timeout())
    return this.reason
  }

  /** @returns {boolean} whether the invocation can no longer start or publish work */
  get expired() {
    return this.refusal() !== null
  }

  /** @returns {void} @throws {Error} the first terminal refusal */
  check() {
    const error = this.refusal()
    if (error) throw error
  }

  /**
   * Registers synchronous local cleanup; it must never disconnect shared transport.
   * @param {(error: Error) => void} cleanup timer, unsent-request or local-builder disposal
   * @returns {() => void} idempotent unregister function
   */
  own(cleanup) {
    const error = this.refusal()
    if (error) cleanup(error)
    else this.cleanups.add(cleanup)
    return () => this.cleanups.delete(cleanup)
  }

  /**
   * Invalidates the captured token and clears all local resources exactly once.
   * @param {Error} reason the stable terminal reason
   * @returns {void}
   */
  stop(reason) {
    if (this.reason) return
    this.reason = reason
    clearTimeout(this.timer)
    this.timer = null
    const cleanups = [...this.cleanups]
    this.cleanups.clear()
    for (const cleanup of cleanups) {
      try {
        cleanup(reason)
      } catch {}
    }
  }

  /**
   * Waits lazily and observes both late outcomes. A timed out caller leaves shared work running.
   * @template T
   * @param {() => T|Promise<T>} work unstarted operation
   * @returns {Promise<T>} result only while the original token and clock remain valid
   * @throws {Error} original failure or invocation refusal
   */
  wait(work) {
    return new Promise((resolve, reject) => {
      let unown = () => {}
      let settled = false
      const finish = (error, value) => {
        if (settled) return
        settled = true
        unown()
        const refusal = this.refusal()
        if (refusal || error) reject(refusal || error)
        else resolve(value)
      }
      try {
        this.check()
        unown = this.own((error) => finish(error))
        if (settled) {
          unown()
          return
        }
        this.check()
        Promise.resolve(work()).then(
          (value) => finish(null, value),
          (error) => finish(error)
        )
      } catch (error) {
        finish(error)
      }
    })
  }

  /**
   * Owns a retry or polling delay; retirement removes the timer before rejecting.
   * @param {number} milliseconds the existing backoff, unchanged
   * @returns {Promise<void>}
   * @throws {Error} original invocation refusal
   */
  delay(milliseconds) {
    return this.wait(
      () =>
        new Promise((resolve, reject) => {
          let unown = () => {}
          const timer = setTimeout(() => {
            unown()
            resolve()
          }, milliseconds)
          unown = this.own((error) => {
            clearTimeout(timer)
            reject(error)
          })
        })
    )
  }

  /**
   * Uses the existing counted retrier and the scoped ordinary branch, without changing options.
   * @param {object} retryOptions existing retry settings
   * @returns {Function} call-scoped retrier
   * @throws {Error} existing configuration error, exhaustion, bail or invocation refusal
   */
  retrier(retryOptions) {
    const retry = createRetry(retryOptions)
    return (fn) => retry(fn, this)
  }
}

/** Shared metadata work owns its retries; individual callers only own their wait. */
class MetadataDiscovery extends AdminDeadline {
  /**
   * @param {() => Promise<any>} work one metadata refresh
   * @param {() => void} remove deletes only this episode from its owning map
   * @returns {MetadataDiscovery} not started until its first caller is enrolled
   */
  constructor(work, remove) {
    super('metadata discovery', null)
    this.work = work
    this.remove = remove
    this.callers = new Set()
    this.running = null
    this.settled = false
  }

  /** @returns {Error|null} refusal when no live caller remains; no physical setup is cancelled */
  refusal() {
    if (this.reason) return this.reason
    if (this.running && !this.settled && ![...this.callers].some((caller) => !caller?.guard())) {
      this.stop(new KafkaJSNonRetriableError('Metadata discovery has no waiting callers'))
      this.remove()
    }
    return this.reason
  }

  /**
   * Enrolls before starting work, then detaches only this caller on its terminal outcome.
   * @param {AdminDeadline|null} caller null represents an ordinary unbounded waiter
   * @returns {Promise<any>} the refresh result, bounded only for this caller
   * @throws {Error} caller refusal or original refresh failure
   */
  async join(caller) {
    const ticket = { guard: caller ? caller.guard : () => null }
    this.callers.add(ticket)
    let unown = () => {}
    const leave = () => {
      this.callers.delete(ticket)
      this.refusal()
    }
    if (caller) unown = caller.own(leave)
    if (!this.running) {
      this.running = Promise.resolve()
        .then(() =>
          context.run(this, () => {
            this.check()
            return this.work()
          })
        )
        .finally(() => {
          this.settled = true
          this.remove()
          this.stop(new KafkaJSNonRetriableError('Metadata discovery completed'))
          this.work = null
        })
      this.running.catch(() => {})
    }
    try {
      return await (caller ? caller.wait(() => this.running) : this.running)
    } finally {
      unown()
      leave()
    }
  }
}

/** @returns {AdminDeadline|undefined} current private admin/discovery invocation */
const currentDeadline = () => context.getStore()

/**
 * Captures one outer deadline; nested public helpers inherit its original token and budget.
 * @template T
 * @param {string} method public method name
 * @param {number} timeoutMs validated budget
 * @param {() => Promise<T>|T} work unstarted method body
 * @returns {Promise<T>}
 * @throws {Error} original failure or KafkaJSTimeout
 */
const runWithDeadline = async (method, timeoutMs, work) => {
  const inherited = currentDeadline()
  if (inherited) {
    const result = await inherited.wait(work)
    inherited.check()
    return result
  }
  const deadline = new AdminDeadline(method, timeoutMs)
  try {
    const result = await context.run(deadline, () => deadline.wait(work))
    deadline.check()
    return result
  } finally {
    deadline.stop(new KafkaJSNonRetriableError('Admin invocation completed'))
  }
}

/**
 * Creates a metadata-only coalescer. Key identity preserves existing producer/consumer scoping.
 * @param {Function} work original refresh implementation
 * @param {Function} [keyOf] existing scope key selector
 * @returns {Function} enrollment wrapper; each argument list is captured once per episode
 */
const shareMetadata = (work, keyOf = () => undefined) => {
  const entries = new Map()
  return function (...args) {
    const key = keyOf(...args)
    let episode = entries.get(key)
    if (!episode || episode.refusal()) {
      episode = new MetadataDiscovery(
        () => work.apply(this, args),
        () => {
          if (entries.get(key) === episode) entries.delete(key)
        }
      )
      entries.set(key, episode)
    }
    return episode.join(currentDeadline() || null)
  }
}

/**
 * Chooses the current scope's counted retrier, retaining the plain path outside admin work.
 * @param {object} retryOptions original retry settings
 * @returns {Function} existing or scoped retrier
 */
const retrierOf = (retryOptions) =>
  currentDeadline()?.retrier(retryOptions) || createRetry(retryOptions)

/**
 * Checks immediately in the awaiting continuation, before it can publish the returned value.
 * @template T
 * @param {T} value awaited value
 * @returns {T} unchanged value
 * @throws {Error} actual-clock/token refusal
 */
const checked = (value) => {
  currentDeadline()?.check()
  return value
}

/**
 * Checks the original invocation before starting any lazy operation, observing late outcomes.
 * @template T
 * @param {() => T|Promise<T>} work operation not started yet
 * @returns {Promise<T>|T} unchanged plain path or invocation-owned wait
 * @throws {Error} original failure or invocation refusal
 */
const waitOf = (work) => currentDeadline()?.wait(work) ?? work()

/**
 * Starts shared physical setup outside caller ownership. The caller still waits under its scope.
 * @template T
 * @param {() => T|Promise<T>} work setup whose separate budget owns cancellation
 * @returns {Promise<T>|T} shared work's result
 */
const sharedSetup = (work) => context.exit(work)

/** the consumer's captured consumer operation adapted only at the GroupDescriptions boundary. */
class ConsumerDescriptionScope {
  /**
   * @param {object} operation the consumer's original ConsumerOperation, never renewed here
   * @returns {ConsumerDescriptionScope} private adapter without its own timer
   */
  constructor(operation) {
    this.operation = operation
    this.guard = () => operation.guard()
  }

  /** @returns {void} @throws {Error} original consumer timeout/run refusal */
  check() {
    this.operation.check()
  }

  /** @param {Function} work lazy work @returns {Promise<any>} captured consumer wait */
  wait(work) {
    return this.operation.wait(work)
  }

  /**
   * Registers local cleanup with the consumer's original error-bearing disposer.
   * @param {(error: Error) => void} cleanup receives the original stable consumer refusal
   * @returns {Function} unregister without changing the consumer operation contract
   */
  own(cleanup) {
    return this.operation.register(cleanup)
  }

  /** @param {number} ms existing backoff @returns {Promise<void>} cancellable delay */
  delay(ms) {
    return this.operation.sleep(ms)
  }

  /** @param {object} options existing retry settings @returns {Function} captured counted executor */
  retrier(options) {
    return (action) => this.operation.retry(action, options)
  }
}

/**
 * Preserves the consumer's delivery-timeout/run semantics through the shared group-describe implementation.
 * @param {object|undefined} operation captured consumer operation
 * @param {Function} work original group-describe body
 * @returns {Promise<any>|any} original result; no new admin budget on the consumer path
 */
const withConsumerOperation = (operation, work) =>
  operation ? context.run(new ConsumerDescriptionScope(operation), work) : work()

/**
 * Keeps the legacy polling interval and local wait cap, with all timers owned by the call.
 * @param {Function} fn existing predicate; first invocation follows the first delay
 * @param {object} [options] legacy delay/maxWait/ignoreTimeout/timeoutMessage
 * @returns {Promise<any>} first truthy result
 * @throws {Error} predicate failure, local cap, or original invocation refusal
 */
const waitForOf = (fn, options = {}) => {
  const deadline = currentDeadline()
  if (!deadline) return plainWaitFor(fn, options)
  const { delay = 50, maxWait = 10000, timeoutMessage = 'Timeout', ignoreTimeout = false } = options
  return deadline.wait(
    () =>
      new Promise((resolve, reject) => {
        let timer
        let cap
        let done = false
        let totalWait = 0
        const until = ignoreTimeout ? Infinity : performance.now() + maxWait
        let unown = () => {}
        const finish = (error, value) => {
          if (done) return
          done = true
          clearTimeout(timer)
          clearTimeout(cap)
          unown()
          if (error) reject(error)
          else resolve(value)
        }
        const tick = async () => {
          try {
            deadline.check()
            if (done) return
            if (performance.now() >= until) return finish(new KafkaJSTimeout(timeoutMessage))
            totalWait += delay
            const value = await deadline.wait(() => fn(totalWait))
            deadline.check()
            if (done) return
            if (performance.now() >= until) return finish(new KafkaJSTimeout(timeoutMessage))
            if (value) finish(null, value)
            else timer = setTimeout(tick, delay)
          } catch (error) {
            finish(error)
          }
        }
        unown = deadline.own((error) => finish(error))
        if (done) {
          unown()
          return
        }
        try {
          deadline.check()
          timer = setTimeout(tick, delay)
          if (!ignoreTimeout)
            cap = setTimeout(() => finish(new KafkaJSTimeout(timeoutMessage)), maxWait)
        } catch (error) {
          finish(error)
        }
      })
  )
}

/**
 * Composes the established frame/lifecycle guard before the admin check, preserving byte lengths.
 * @param {Function|null|undefined} guard established guard
 * @param {AdminDeadline|undefined} [deadline] captured caller, defaults to the current store
 * @returns {Function|null|undefined} original guard unchanged on the ordinary path
 */
const withAdminGuard = (guard, deadline = currentDeadline()) =>
  deadline ? (frameLength) => guard?.(frameLength) || deadline.guard() : guard

/**
 * Wraps every public operation exactly once; control/event methods remain direct functions.
 * @param {object} methods completed admin factory surface, including future family spreads
 * @param {object} optionsAt method to index of its public options argument
 * @param {number} defaultTimeout validated AdminConfig budget
 * @returns {object} wrapped admin surface
 * @throws {KafkaJSInvalidConfigurationError} per-call invalid apiTimeout, before method work
 */
const wrapAdmin = (methods, optionsAt, defaultTimeout) => {
  for (const [method, index] of Object.entries(optionsAt)) {
    const work = methods[method]
    if (typeof work !== 'function') throw new Error(`Missing admin operation ${method}`)
    methods[method] = async (...args) => {
      const timeoutMs = validateApiTimeout(args[index]?.apiTimeout, defaultTimeout)
      return runWithDeadline(method, timeoutMs, () => work(...args))
    }
  }
  return methods
}

module.exports = {
  AdminDeadline,
  runWithDeadline,
  retrierOf,
  currentDeadline,
  checked,
  waitOf,
  shareMetadata,
  sharedSetup,
  withAdminGuard,
  validateApiTimeout,
  wrapAdmin,
  waitForOf,
  withConsumerOperation,
}
