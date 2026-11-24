const { AsyncLocalStorage } = require('node:async_hooks')
const createRetry = require('../retry')
const plainWaitFor = require('../utils/waitFor')
const {
  KafkaJSTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
  KafkaJSUnknownOutcomeError,
} = require('../errors')
const { isMutation, itemsOf, keyOf, definiteRefusals } = require('./unknownOutcome')

const context = new AsyncLocalStorage()
const DEFAULT_API_TIMEOUT = 60000
const MAX_API_TIMEOUT = 2147483647

/** The errors with which the latch refused to write a request whose items were already answered. */
const resendRefusals = new WeakSet()

/**
 * Whether an error is the latch's refusal to write again items an earlier request of the same call
 * carried and got an answer for (`AdminDeadline#latch`); its `cause` is that answer's error, if any.
 * @param {unknown} error
 * @returns {boolean}
 */
const isResendRefusal = (error) => resendRefusals.has(error)

/** One physical send of a request that changes broker state, within one admin call. */
class WriteRecord {
  /**
   * @param {{ request: { apiKey: number, apiName: string, apiVersion: number } }} protocolRequest
   *   the built request; it is not kept
   * @param {number} nodeId the broker it goes to
   */
  constructor(protocolRequest, nodeId) {
    const { apiKey, apiName, apiVersion } = protocolRequest.request
    this.apiKey = apiKey
    this.api = apiName
    this.apiVersion = apiVersion
    this.nodeId = nodeId
    this.items = itemsOf(protocolRequest, nodeId)
    this.keys = this.items && this.items.map(keyOf)
    this.written = false
    this.decoded = false
    this.cause = null
    /** @type {Set<string>} items its answer definitely refused, not yet sent again */
    this.permits = new Set()
    /** whether its answer refused the whole request, when its items cannot be named */
    this.whole = false
  }

  /**
   * Records what its decoded answer definitely refused (`definiteRefusals`). Never throws.
   * @param {object} decoded the decoded answer to this send
   * @returns {void}
   */
  grant(decoded) {
    const refused = definiteRefusals(this.apiKey, decoded, this.items)
    if (refused !== true) for (const key of refused) this.permits.add(key)
    else if (this.keys === null) this.whole = true
    else for (const key of this.keys) this.permits.add(key)
  }

  /** @returns {{ api: string, apiVersion: number, nodeId: number, items: any[]|null }} */
  describe() {
    return { api: this.api, apiVersion: this.apiVersion, nodeId: this.nodeId, items: this.items }
  }
}

/**
 * The written sends of one API within one admin call: for each item, the latest send that carried
 * it, and the latest send whose items could not be named.
 */
class ApiHistory {
  constructor() {
    /** @type {Map<string, WriteRecord>} */
    this.latest = new Map()
    /** @type {WriteRecord|null} */
    this.unnamed = null
  }

  /**
   * The earlier sends that forbid writing `record`. A named item may go again only when its latest
   * send holds a permit for it; a send whose items cannot be named may go again only after a send
   * of the same kind whose whole request was refused, and never after a named send.
   * @param {WriteRecord} record the send about to be written
   * @returns {WriteRecord[]}
   */
  blocking(record) {
    const found = new Set()
    if (record.keys === null) {
      for (const earlier of this.latest.values()) found.add(earlier)
      if (this.unnamed && !this.unnamed.whole) found.add(this.unnamed)
    } else {
      for (const key of record.keys) {
        const earlier = this.latest.get(key)
        if (earlier && !earlier.permits.has(key)) found.add(earlier)
      }
      if (this.unnamed) found.add(this.unnamed)
    }
    found.delete(record)
    return [...found]
  }

  /**
   * Notes that `record` is being written, after its final check passed: it uses up the permits it
   * relied on and becomes the latest send of its items.
   * @param {WriteRecord} record
   * @returns {void}
   */
  written(record) {
    if (record.keys === null) {
      if (this.unnamed) this.unnamed.whole = false
      this.unnamed = record
    } else {
      for (const key of record.keys) {
        this.latest.get(key)?.permits.delete(key)
        this.latest.set(key, record)
      }
    }
  }
}

/**
 * The error that must end a retry loop instead of `error`, if any (see `AdminDeadline#retrier`).
 * @param {unknown} error what the attempt failed with
 * @param {unknown} previous what the attempt before it failed with, or `null`
 * @returns {unknown|null}
 */
const finalError = (error, previous) => {
  if (error instanceof KafkaJSUnknownOutcomeError) return error
  if (isResendRefusal(error)) return error.cause ?? previous ?? error
  return null
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
    /** @type {Map<number, ApiHistory>} the written mutating sends of this call, by API */
    this.history = new Map()
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
    this.history.clear()
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
   * An attempt that throws or bails with `KafkaJSUnknownOutcomeError` ends the retries at once with
   * that error, unwrapped. An attempt the latch refused because its items were already sent and
   * answered ends them with that answer's error, or else with the error of the attempt before it.
   * @param {object} retryOptions existing retry settings
   * @returns {Function} call-scoped retrier
   * @throws {Error} existing configuration error, exhaustion, bail or invocation refusal
   * @throws {KafkaJSUnknownOutcomeError} when a written request of the call got no answer
   */
  retrier(retryOptions) {
    const retry = createRetry(retryOptions)
    return (fn) => {
      let previous = null
      return retry((bail, ...rest) => {
        const end = (error) => bail(finalError(error, previous) ?? error)
        return fn(end, ...rest).catch((error) => {
          const final = finalError(error, previous)
          previous = error
          if (final) return bail(final)
          throw error
        })
      }, this)
    }
  }

  /**
   * The replay latch of one physical send of a request that changes broker state: within this
   * call, no layer can write again an item a written send carried, unless that send's decoded answer
   * definitely refused the item (`definiteRefusals`); each such permit serves one later write.
   * Called once per send, before anything is written. Any other request, and any send after the
   * call ended, gets `null` and is not recorded.
   * @param {{ request: { apiKey: number }, args?: object, onWrite?: Function, onDecoded?: Function }} protocolRequest
   *   the built request
   * @param {number} nodeId the broker it goes to
   * @returns {{ request: object, guard: () => Error|null, failed: (error: unknown) => unknown }|null}
   *   a copy of the request whose `onWrite` and `onDecoded` also keep the record (after the
   *   caller's own hook for a write, before it for an answer), the pre-write check of this send,
   *   and what to call with the error that ended it: it returns the error to throw, a
   *   `KafkaJSUnknownOutcomeError` naming this send when it was written and got no decoded answer
   */
  latch(protocolRequest, nodeId) {
    if (this.reason || !isMutation(protocolRequest)) return null
    const record = new WriteRecord(protocolRequest, nodeId)
    const { onWrite, onDecoded } = protocolRequest
    return {
      request: {
        ...protocolRequest,
        onWrite: () => {
          onWrite?.()
          record.written = true
          if (this.reason) return
          let history = this.history.get(record.apiKey)
          if (!history) this.history.set(record.apiKey, (history = new ApiHistory()))
          history.written(record)
        },
        onDecoded: (decoded) => {
          record.decoded = true
          record.grant(decoded)
          onDecoded?.(decoded)
        },
      },
      guard: () => this.resendRefusal(record),
      failed: (error) => {
        record.cause ??= error
        return record.written && !record.decoded ? this.unknownOutcome([record]) : error
      },
    }
  }

  /**
   * Refuses a send when an earlier written send of this call forbids it (`ApiHistory#blocking`).
   * A send is never compared with itself, so its own later checks never refuse it.
   * @param {WriteRecord} record the send about to be written
   * @returns {Error|null} `KafkaJSUnknownOutcomeError` when a blocking send got no answer, else a
   *   `KafkaJSNonRetriableError` whose `cause` is the error of the answered send; `null` to write
   */
  resendRefusal(record) {
    const blocking = this.history.get(record.apiKey)?.blocking(record) ?? []
    if (blocking.length === 0) return null
    const unanswered = blocking.filter((earlier) => !earlier.decoded)
    if (unanswered.length > 0) return this.unknownOutcome(unanswered)
    const refusal = new KafkaJSNonRetriableError(
      `${this.method}: a ${record.api} request for these items was answered; it was not sent again`,
      { cause: blocking.find((earlier) => earlier.cause)?.cause ?? undefined }
    )
    resendRefusals.add(refusal)
    return refusal
  }

  /**
   * @param {WriteRecord[]} records written sends of this call without an answer
   * @returns {KafkaJSUnknownOutcomeError} naming them and their items
   */
  unknownOutcome(records) {
    return new KafkaJSUnknownOutcomeError(
      `${this.method}: a ${records[0].api} request was sent and no answer arrived; it may have been sent and applied. It was not sent again.`,
      { requests: records.map((record) => record.describe()), cause: records[0].cause ?? undefined }
    )
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
 * Captures one outer deadline; nested public helpers inherit its original token and budget.
 * @template T
 * @param {string} method public method name
 * @param {number} timeoutMs validated budget
 * @param {() => Promise<T>|T} work unstarted method body
 * @param {import('../producer/generation')|null} [lifecycle] original lifecycle, never renewed
 * @param {number} [startedAt] monotonic public invocation entry, before lazy admission
 * @returns {Promise<T>}
 * @throws {Error} original failure or KafkaJSTimeout
 */
const runWithDeadline = async (
  method,
  timeoutMs,
  work,
  lifecycle = null,
  startedAt = performance.now()
) => {
  const inherited = currentDeadline()
  if (inherited && (!lifecycle || inherited.lifecycle === lifecycle)) {
    const result = await inherited.wait(work)
    inherited.check()
    return result
  }
  const deadline = new AdminDeadline(method, timeoutMs, lifecycle, startedAt)
  const unregister = lifecycle?.register((error) => deadline.stop(error))
  try {
    const result = await context.run(deadline, () => deadline.wait(work))
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
 * @param {(() => Error|null)|null} [latched] the replay check of one physical send
 *   (`AdminDeadline#latch`), asked last
 * @returns {Function|null|undefined} original guard unchanged on the ordinary path
 */
const withAdminGuard = (guard, deadline = currentDeadline(), latched = null) =>
  deadline
    ? (frameLength) => guard?.(frameLength) || deadline.guard() || latched?.() || null
    : guard

/**
 * Wraps every public operation exactly once; control/event methods remain direct functions.
 * @param {object} methods completed admin factory surface, including future family spreads
 * @param {object} optionsAt method to index of its public options argument
 * @param {number} defaultTimeout validated AdminConfig budget
 * @param {import('./lifecycle')} [lifecycle] admission and shared close owner
 * @returns {object} wrapped admin surface
 * @throws {KafkaJSInvalidConfigurationError} per-call invalid apiTimeout, before method work
 */
const wrapAdmin = (methods, optionsAt, defaultTimeout, lifecycle) => {
  for (const [method, index] of Object.entries(optionsAt)) {
    const work = methods[method]
    if (typeof work !== 'function') throw new Error(`Missing admin operation ${method}`)
    methods[method] = async (...args) => {
      const startedAt = performance.now()
      const timeoutMs = validateApiTimeout(args[index]?.apiTimeout, defaultTimeout)
      const run = (generation) =>
        runWithDeadline(method, timeoutMs, () => work(...args), generation, startedAt)
      return lifecycle ? lifecycle.run(run) : run(null)
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
  isResendRefusal,
  validateApiTimeout,
  wrapAdmin,
  waitForOf,
  withConsumerOperation,
}
