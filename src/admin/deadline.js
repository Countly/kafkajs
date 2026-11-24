const { AsyncLocalStorage } = require('node:async_hooks')
const createRetry = require('../retry')
const plainWaitFor = require('../utils/waitFor')
const { responseSourceOf } = require('../broker/failureSource')
const { itemsOf, keyOf } = require('./unknownOutcome')
const {
  KafkaJSTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
  KafkaJSProtocolError,
  KafkaJSUnknownOutcomeError,
} = require('../errors')

const context = new AsyncLocalStorage()
const DEFAULT_API_TIMEOUT = 60000
const MAX_API_TIMEOUT = 2147483647

/** Errors from decoded broker answers and errors that terminate a whole admin call. */
const answered = new WeakSet()
const wholeCall = new WeakSet()

/** @param {unknown} error @returns {boolean} whether this is a final broker answer */
const isAnsweredError = (error) => answered.has(error)
/** @param {unknown} error @returns {boolean} whether the entire call must reject */
const endsCall = (error) => error instanceof KafkaJSUnknownOutcomeError || wholeCall.has(error)

const mutations = new AsyncLocalStorage()

/** @param {string} method @param {object[]} requests @param {Error} cause
 * @returns {KafkaJSUnknownOutcomeError} the failure for the written, unanswered requests
 */
const unknownOutcome = (method, requests, cause) =>
  new KafkaJSUnknownOutcomeError(`${method}: no usable answer to a written request`, {
    requests,
    cause,
  })

/** The writes of one public invocation, independent of its deadline. */
class MutationHistory {
  /** @param {string} method invocation name @param {AdminDeadline} deadline its deadline */
  constructor(method, deadline) {
    this.method = method
    this.deadline = deadline
    this.apis = new Map()
    this.closed = false
  }

  /** Releases the written records and prevents later continuations from adding to them. */
  close() {
    this.closed = true
    this.apis.clear()
  }

  /** @param {object} protocolRequest built wire request @param {number} nodeId target broker
   * @returns {{request:object,failed:Function}} transport hooks and failure classifier
   */
  latch(protocolRequest, nodeId) {
    const { apiKey, apiName: api, apiVersion } = protocolRequest.request
    const items = itemsOf(protocolRequest, nodeId)
    const description = { api, apiVersion, nodeId, items }
    const keys = items?.map(keyOf)
    const record = { description, decoded: false, cause: undefined, failure: undefined }
    let written = false
    return {
      request: {
        ...protocolRequest,
        onWrite: () => {
          this.deadline.check()
          if (this.closed)
            throw new KafkaJSNonRetriableError(`${this.method}: admin call completed`)
          let history = this.apis.get(apiKey)
          if (!history) {
            history = { first: null, unnamed: null, byKey: new Map() }
            this.apis.set(apiKey, history)
          }
          const blocked =
            history.unnamed ||
            (keys ? keys.map((key) => history.byKey.get(key)).find(Boolean) : history.first)
          if (blocked) {
            if (!blocked.decoded) {
              blocked.failure ??= unknownOutcome(this.method, [blocked.description], blocked.cause)
              throw blocked.failure
            }
            const error = new KafkaJSNonRetriableError(
              `${this.method}: a ${api} request was not sent: an earlier request of this call carried the same items`,
              { cause: blocked.cause }
            )
            wholeCall.add(error)
            throw error
          }
          protocolRequest.onWrite?.()
          history.first ??= record
          if (keys) {
            for (const key of keys) history.byKey.set(key, record)
          } else {
            history.unnamed = record
          }
          written = true
        },
        onDecoded: (response) => {
          record.decoded = true
          protocolRequest.onDecoded?.(response)
        },
      },
      failed: (error) => {
        const refusal = this.deadline.guard()
        if (refusal) return refusal
        if (!written) return error
        record.cause = error
        if (
          record.decoded ||
          (responseSourceOf(error) === apiKey && error instanceof KafkaJSProtocolError)
        ) {
          record.decoded = true
          answered.add(error)
          return error
        }
        record.failure ??= unknownOutcome(this.method, [description], error)
        return record.failure
      },
    }
  }
}

/** @returns {MutationHistory|undefined} the invocation allowed to enroll a mutation */
const currentMutation = () => {
  const history = mutations.getStore()
  return history?.deadline === currentDeadline() ? history : undefined
}

/** @param {Promise[]} work sends in this attempt
 * @param {(value:any, index:number) => void} [accept] retains answered work across retries
 * @returns {Promise<any[]>} resolved values in input order
 * @throws {Error} the highest priority failure, after all sends settle
 */
const settleMutations = async (work, accept) => {
  const outcomes = await Promise.allSettled(work)
  checked(null)
  const failures = outcomes.flatMap((outcome) =>
    outcome.status === 'rejected' ? [outcome.reason] : []
  )
  const unknowns = [
    ...new Set(failures.filter((error) => error instanceof KafkaJSUnknownOutcomeError)),
  ]
  if (unknowns.length === 1) throw unknowns[0]
  if (unknowns.length > 1) {
    const requests = [...new Set(unknowns.flatMap((error) => error.requests))]
    throw unknownOutcome(currentMutation()?.method ?? 'Admin mutation', requests, unknowns[0].cause)
  }
  const whole = failures.find(endsCall)
  if (whole) throw whole
  const answer = failures.find(isAnsweredError)
  if (answer) throw answer
  if (accept) {
    outcomes.forEach((outcome, index) => {
      if (outcome.status === 'fulfilled') accept(outcome.value, index)
    })
  }
  if (failures.length > 0) throw failures[0]
  return outcomes.map(({ value }) => value)
}

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
   * @param {import('../producer/generation')|null} [lifecycle] captured admin lifecycle
   * @param {number} [startedAt] monotonic public invocation entry time
   * @returns {AdminDeadline} active invocation
   */
  constructor(method, timeoutMs, lifecycle = null, startedAt = performance.now()) {
    this.lifecycle = lifecycle
    this.method = method
    this.timeoutMs = timeoutMs
    this.token = Symbol(method)
    this.deadlineAt = timeoutMs === null ? Infinity : startedAt + timeoutMs
    this.reason = null
    this.cleanups = new Set()
    this.timer =
      timeoutMs === null
        ? null
        : setTimeout(
            () => this.stop(this.timeout()),
            Math.max(0, this.deadlineAt - performance.now())
          )
    this.guard = () => this.refusal()
  }

  /** @returns {KafkaJSTimeout} one stable error naming the original method and unknown outcome */
  timeout() {
    this.timeoutError ??= new KafkaJSTimeout(
      `${this.method} did not complete within ${this.timeoutMs} ms; outcome is unknown; a mutation, if any, may have been applied`
    )
    return this.timeoutError
  }

  /** @returns {Error|null} actual-clock refusal, including when timer delivery is withheld */
  refusal() {
    const closed = this.lifecycle?.guard()
    if (!this.reason && closed) this.stop(closed)
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
      const finish = (error, value, failed = true) => {
        if (settled) return
        settled = true
        unown()
        const refusal = this.refusal()
        if (refusal || failed) reject(refusal || error)
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
        Promise.resolve(context.run(this, work)).then(
          (value) => finish(null, value, false),
          (error) => finish(error)
        )
      } catch (error) {
        finish(error)
      }
    })
  }

  /**
   * Runs local disposal under the original scope even when invoked by an external event.
   * Cleanup may run after retirement, but its transport still sees the original refusal.
   * @template T
   * @param {() => T} work local cleanup, never a new user operation
   * @returns {T} cleanup result
   * @throws {Error} cleanup failure
   */
  run(work) {
    return context.run(this, work)
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
    return (fn) =>
      retry(async (bail, ...attempt) => {
        try {
          return await fn(bail, ...attempt)
        } catch (error) {
          this.check()
          if (endsCall(error) || isAnsweredError(error)) return bail(error)
          throw error
        }
      }, this)
  }
}

/** Shared metadata work owns its retries; individual callers only own their wait. */
class MetadataDiscovery extends AdminDeadline {
  /**
   * @param {() => Promise<any>} work one metadata refresh
   * @param {() => void} remove deletes only this episode from its owning map
   * @param {any} [epoch] the epoch it started in, see `shareMetadata`
   * @returns {MetadataDiscovery} not started until its first caller is enrolled
   */
  constructor(work, remove, epoch) {
    super('metadata discovery', null)
    this.work = work
    this.remove = remove
    this.epoch = epoch
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
 * Runs a call with its own write history and releases it on settlement.
 * @param {string} method invocation name
 * @param {AdminDeadline} deadline inherited or newly created budget
 * @param {Function} work admitted invocation
 * @returns {Promise<any>} the result under the invocation's deadline
 */
const runMutation = async (method, deadline, work) => {
  const history = new MutationHistory(method, deadline)
  try {
    return await mutations.run(history, () => deadline.wait(work))
  } finally {
    history.close()
  }
}

/**
 * Captures one outer deadline; nested public helpers inherit its original token and budget.
 * @template T
 * @param {string} method public method name
 * @param {number} timeoutMs validated budget
 * @param {() => Promise<T>|T} work unstarted method body
 * @param {import('../producer/generation')|null} [lifecycle] original lifecycle, never renewed
 * @param {number} [startedAt] monotonic public invocation entry, before lazy admission
 * @param {boolean} [publicCall=false] whether this entry owns a new mutation history
 * @returns {Promise<T>}
 * @throws {Error} original failure or KafkaJSTimeout
 */
const runWithDeadline = async (
  method,
  timeoutMs,
  work,
  lifecycle = null,
  startedAt = performance.now(),
  publicCall = false
) => {
  const inherited = currentDeadline()
  if (inherited && (!lifecycle || inherited.lifecycle === lifecycle)) {
    const result = await (publicCall ? runMutation(method, inherited, work) : inherited.wait(work))
    inherited.check()
    return result
  }
  const deadline = new AdminDeadline(method, timeoutMs, lifecycle, startedAt)
  const unregister = lifecycle?.register((error) => deadline.stop(error))
  try {
    const result = await context.run(deadline, () => runMutation(method, deadline, work))
    deadline.check()
    return result
  } finally {
    unregister?.()
    deadline.stop(new KafkaJSNonRetriableError('Admin invocation completed'))
  }
}

/**
 * Creates a metadata-only coalescer. Key identity preserves existing producer/consumer scoping.
 * @param {Function} work original refresh implementation
 * @param {Function} [keyOf] existing scope key selector
 * @param {() => any} [epochOf] a call never joins an episode started in another epoch, and an
 *   episode whose epoch moved before its deferred start skips the work and resolves undefined
 * @returns {Function} enrollment wrapper; each argument list is captured once per episode
 */
const shareMetadata = (work, keyOf = () => undefined, epochOf = () => undefined) => {
  const entries = new Map()
  return function (...args) {
    const key = keyOf(...args)
    const epoch = epochOf()
    let episode = entries.get(key)
    if (!episode || episode.epoch !== epoch || episode.refusal()) {
      episode = new MetadataDiscovery(
        () => (epochOf() === epoch ? work.apply(this, args) : undefined),
        () => {
          if (entries.get(key) === episode) entries.delete(key)
        },
        epoch
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
 * Runs shared physical setup or observer notifications outside caller ownership.
 * The caller's continuation retains its original scope.
 * @template T
 * @param {() => T|Promise<T>} work shared setup or observers with an independent lifetime
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
        const finish = (error, value, failed = true) => {
          if (done) return
          done = true
          clearTimeout(timer)
          clearTimeout(cap)
          unown()
          if (failed) reject(error)
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
            if (value) finish(null, value, false)
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
 * @param {import('./lifecycle')} [lifecycle] admission and shared close owner
 * @param {Object<string, (...args: any[]) => any[]>} [inputs] per method, reads and checks the
 *   call's arguments when the call is admitted and returns the copy the method runs with
 * @returns {object} wrapped admin surface
 * @throws {KafkaJSInvalidConfigurationError} per-call invalid apiTimeout, before method work
 */
const wrapAdmin = (methods, optionsAt, defaultTimeout, lifecycle, inputs = {}) => {
  for (const [method, index] of Object.entries(optionsAt)) {
    const work = methods[method]
    if (typeof work !== 'function') throw new Error(`Missing admin operation ${method}`)
    const inputOf = inputs[method]
    methods[method] = async (...args) => {
      const startedAt = performance.now()
      const timeoutMs = validateApiTimeout(args[index]?.apiTimeout, defaultTimeout)
      const admit = inputOf && (() => inputOf(...args))
      const run = (generation, admitted = args) =>
        runWithDeadline(method, timeoutMs, () => work(...admitted), generation, startedAt, true)
      return lifecycle ? lifecycle.run(run, admit) : run(null, admit?.())
    }
  }
  return methods
}

module.exports = {
  AdminDeadline,
  runWithDeadline,
  retrierOf,
  currentDeadline,
  currentMutation,
  endsCall,
  isAnsweredError,
  settleMutations,
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
