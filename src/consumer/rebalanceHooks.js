const { AsyncLocalStorage } = require('node:async_hooks')
const { performance } = require('node:perf_hooks')
const { isNativeError } = require('node:util').types
const describeThrown = require('../network/describeThrown')
const Generation = require('../producer/generation')
const { KafkaJSNonRetriableError, KafkaJSRebalanceListenerError } = require('../errors')
const storage = new AsyncLocalStorage()
const names = ['onPartitionsRevoked', 'onPartitionsAssigned', 'onPartitionsLost']

/** Owns one run's callback snapshot and pending invocation waits. */
module.exports = class RebalanceHooks {
  /**
   * @param {object|undefined} listener public listener
   * @param {string} kind membership kind
   * @param {boolean} [hasGroup=true] group-ID presence
   * @returns {object|null} immutable private callbacks, or null without a listener
   * @throws {KafkaJSNonRetriableError} invalid shape or unsupported mode
   */
  static validate(listener, kind, hasGroup = true) {
    if (listener === undefined) return null
    if (!hasGroup)
      throw new KafkaJSNonRetriableError('Consumer groupId must be a non-empty string.')
    if (kind === 'manual')
      throw new KafkaJSNonRetriableError('rebalanceListener is unavailable with manual assignment')
    if (listener === null || typeof listener !== 'object' || Array.isArray(listener)) {
      throw new KafkaJSNonRetriableError('rebalanceListener must be an object')
    }
    const copy = {}
    for (const name of names) {
      if (name in listener) {
        const hook = listener[name]
        if (typeof hook !== 'function')
          throw new KafkaJSNonRetriableError(`${name} must be a function`)
        copy[name] = hook
      }
    }
    return Object.freeze(copy)
  }

  /** @returns {object|undefined} original invocation record, never a batch or public payload */
  static current() {
    return storage.getStore()
  }

  /**
   * @param {object} record captured invocation
   * @returns {import('../producer/generation')} its fence, closed when the invocation settles,
   *   expires or is retired, so work started for it stops dialling and retrying
   */
  static fence(record) {
    return record.fence
  }

  /**
   * @param {object|undefined} [record] captured invocation, defaulting to the caller
   * @returns {Error|null} refusal or null for valid or external work
   */
  static refusal(record = storage.getStore()) {
    return record ? record.owner.refusal(record) : null
  }

  /**
   * Runs internal work outside the user invocation; its caller also clears handler context.
   * @param {() => unknown} work internal work
   * @returns {unknown} exact work result
   * @throws {Error} work failure unchanged
   */
  static outside(work) {
    return storage.run(undefined, work)
  }

  /**
   * @param {object} listener validated callback snapshot
   * @param {object} options run-owned dependencies
   * @param {(context: object) => boolean} options.isCurrent run/round currentness
   * @param {(error: Error, context: object) => void} options.onExpire synchronous fencing
   * @param {() => number} [options.now] shared monotonic clock
   */
  constructor(listener, { isCurrent, onExpire, now = () => performance.now() }) {
    this.listener = listener
    this.isCurrent = isCurrent
    this.onExpire = onExpire
    this.now = now
    this.pending = new Set()
  }

  /** @param {string} name hook name @returns {boolean} effective callback presence */
  has(name) {
    return (
      typeof (
        this.listener[name] ??
        (name === 'onPartitionsLost' && this.listener.onPartitionsRevoked)
      ) === 'function'
    )
  }

  /**
   * Checks lifetime and actual time. Each API must additionally check its captured ownership.
   * @param {object} record captured invocation
   * @returns {Error|null} stable refusal
   */
  refusal(record) {
    if (record.failure) return record.failure
    if (record.active && this.now() >= record.context.deadlineAt) {
      record.expire()
      return record.failure
    }
    if (!record.active || !this.isCurrent(record.context)) {
      record.failure = new KafkaJSNonRetriableError(
        'The rebalance hook invocation is no longer current'
      )
      record.cancel(record.failure)
      return record.failure
    }
    return null
  }

  /**
   * Ends matching client waits without awaiting user code; identities compare by reference.
   * @param {object} identity run and optional round
   * @param {Error} error owner retirement reason
   * @returns {void} detached continuations remain fenced
   */
  retire(identity, error) {
    for (const record of this.pending) {
      if (
        record.context.run === identity.run &&
        (identity.round === undefined || record.context.round === identity.round)
      ) {
        record.failure ||= error
        record.cancel(record.failure)
      }
    }
  }

  /**
   * Calls a hook once within the unchanged round budget. The owner handles ordinary failure.
   * @param {'onPartitionsRevoked'|'onPartitionsAssigned'|'onPartitionsLost'} name callback
   * @param {object} payload copied event-shaped payload
   * @param {object} options invocation boundary
   * @param {number} options.deadlineAt absolute monotonic round deadline
   * @param {object} options.identity run, round, membership and acquisitions
   * @returns {Promise<void>} completion or failure, without callback retry
   * @throws {KafkaJSRebalanceListenerError|Error} callback failure, timeout or retirement
   */
  async invoke(name, payload, { deadlineAt, identity }) {
    const hook =
      this.listener[name] ??
      (name === 'onPartitionsLost' ? this.listener.onPartitionsRevoked : undefined)
    if (!hook) return
    const context = Object.freeze({
      run: identity.run,
      round: identity.round,
      invocation: Object.freeze({}),
      membership: identity.membership,
      acquisitions: Object.freeze(identity.acquisitions.map((cell) => Object.freeze({ ...cell }))),
      deadlineAt,
      hook: name,
    })
    let rejectWait
    const stopped = new Promise((_, reject) => {
      rejectWait = reject
    })
    const record = { owner: this, context, active: true, failure: null, fence: new Generation() }
    record.cancel = (error) => {
      record.active = false
      clearTimeout(timer)
      record.fence.close(error)
      rejectWait(error)
    }
    record.expire = () => {
      if (record.failure) return
      const cause = new Error('Rebalance listener deadline expired')
      record.failure = new KafkaJSRebalanceListenerError(cause.message, {
        hook: name,
        reason: 'timeout',
        cause,
      })
      record.cancel(record.failure)
      this.onExpire(record.failure, context)
    }
    this.pending.add(record)
    const task = Promise.resolve().then(() =>
      storage.run(record, async () => {
        const before = this.refusal(record)
        if (before) throw before
        try {
          await hook(payload)
        } catch (thrownValue) {
          const refusal = this.refusal(record)
          if (refusal) throw refusal
          const cause = isNativeError(thrownValue)
            ? thrownValue
            : new Error(describeThrown(thrownValue))
          throw new KafkaJSRebalanceListenerError(`Rebalance listener ${name} threw`, {
            hook: name,
            reason: 'threw',
            cause,
            thrownValue,
          })
        }
        const after = this.refusal(record)
        if (after) throw after
      })
    )
    const timer = setTimeout(() => record.expire(), Math.max(0, Math.ceil(deadlineAt - this.now())))
    try {
      await Promise.race([task, stopped])
    } finally {
      record.active = false
      clearTimeout(timer)
      record.fence.close(
        record.failure ||
          new KafkaJSNonRetriableError('The rebalance hook invocation is no longer current')
      )
      this.pending.delete(record)
    }
  }
}
