/**
 * The rebalance-protocol seam. `ClassicGroupProtocol` implements it over JoinGroup/SyncGroup;
 * a future protocol implements it over its own requests. The module carries the two mode
 * constants, the mode-selection rule, and the typedefs every implementation is checked against.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 */

const {
  KafkaJSNonRetriableError,
  KafkaJSDeliveryTimeout,
  KafkaJSCommitFailedError,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')
const createRetry = require('../../retry')

/** Every member revokes its whole assignment before rejoining. */
const EAGER = 'eager'

/** A member keeps the partitions it retains and revokes only what moved (KIP-429). */
const COOPERATIVE = 'cooperative'

/**
 * @typedef {'eager' | 'cooperative'} RebalanceProtocol
 * @typedef {{ topic: string, partitions: number[] }} TopicPartitions
 *
 * @typedef {Object} RebalanceResult
 * @property {TopicPartitions[]} assignment the full target assignment after this round
 * @property {TopicPartitions[]} added      assignment minus what was previously owned
 * @property {TopicPartitions[]} revoked    previously owned minus assignment
 * @property {Map<string, number[]>|null} [unionSnapshot] a classic leader's partition sets of the
 *   topics only other members subscribe to, from the sync; absent for any other member
 *
 * @typedef {Object} GroupProtocol
 * @property {'classic' | 'consumer' | 'manual'} kind which membership implementation this is; read by the
 *   round loop, the poll-expiry path and the error contract, none of which can reach the protocol
 *   except through `ConsumerGroup`
 * @property {number} memberEpoch the current member epoch, or null for a protocol that has none
 * @property {number} joinCount how many joins the coordinator has accepted, counted before the
 *   round's sync or apply; only ever goes up, so it tells a membership that replaced another even
 *   when the generation, the epoch and the member id came back the same
 * @property {() => Promise<import('../../../types').Broker|void>} connect resolves and publishes
 *   the group coordinator; manual membership only checks local readiness
 * @property {(context: { topics: string[], topicsSubscribed: string[],
 *   beforeSync: () => void, growSubscription?: (names: string[]) => Promise<void> }) => Promise<Object>} rebalance
 *   ONE exchange with the coordinator - a JoinGroup + SyncGroup for the classic protocol, one
 *   reconciliation step for the consumer protocol - returning the result the caller applies.
 *   Plain, not self-guarded and not a loop: the single flight, the round loop and the
 *   instrumentation events belong to the caller. `growSubscription` is supplied only by a caller
 *   whose subscription the coordinator may widen.
 * @property {(deps: { rebalanceCoordinator: Object, heartbeatManager: Object,
 *   commitBeforeRelease?: Function }) => void} attach
 *   Late-bound collaborators, forwarded by `ConsumerGroup#attach`.
 * @property {(options: { interval: number }) => Promise<void>} heartbeat
 * @property {() => number} heartbeatInterval how long until the next heartbeat should go out; the
 *   heartbeat manager reads this and nothing else when it schedules
 * @property {() => boolean} needsReconciliation whether the coordinator wants something this member
 *   does not have yet; always false for a protocol whose assignment is decided at join time
 * @property {(error: Error) => void} onMembershipError performs the state move a lost-membership
 *   error implies, wherever it was learned - the heartbeat, an offset commit or a fetch
 * @property {() => void} onStopRequested fails whatever the protocol is waiting on, because the
 *   consumer is stopping
 * @property {(options?: { leaveGroup?: boolean }) => Promise<void>} leave `leaveGroup` `true`
 *   removes the member from the group at once, `false` keeps it there until its session expires,
 *   and `undefined` is the default: a dynamic member leaves, a static one stays
 * @property {() => Promise<void>} leaveForPollExpiry the poll-overrun variant of `leave`: no
 *   commit, no partition events, the response ignored
 * @property {() => RebalanceProtocol} rebalanceProtocol
 * @property {(options?: { clearMemberId?: boolean }) => void} resetGeneration
 * @property {(revision?: number) => boolean} forgetMemberId forgets the member id of the membership
 *   the revision names, the current one when absent; false when it was retired or the protocol keeps its id
 * @property {() => TopicPartitions[]} owned
 * @property {(topicPartitions: TopicPartitions[]) => void} narrowOwned shrink only
 * @property {(topicPartitions: TopicPartitions[]) => void} setOwned widen to what was applied
 * @property {(snapshot: {names: string[], revision: number}) => void} updateSubscription takes the
 *   newest desired subscription: the consumer protocol declares it on its next heartbeat, the
 *   classic protocol on its next join
 */

/** Precedence: the highest id in the intersection wins, matching the Java client. */
const PROTOCOL_IDS = { [EAGER]: 0, [COOPERATIVE]: 1 }

/**
 * Chooses the rebalance protocol for a set of assigners, once, at consumer construction.
 *
 * The rule is Java's: intersect what every configured assigner supports and take the highest id.
 * An assigner that declares nothing supports eager only, which is what makes a rolling upgrade
 * work - a group running one old and one new assigner stays eager until every member has dropped
 * the old one.
 *
 * An empty assigner list is not rejected here: it has always been accepted at construction and
 * fails at JoinGroup instead, so it keeps that behaviour and reports the conservative protocol
 * rather than the whole universe the intersection would otherwise return.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
 * @param {import('../../../types').Assigner[]} assigners
 * @returns {RebalanceProtocol}
 * @throws {KafkaJSNonRetriableError} when the assigners share no protocol
 */
const selectRebalanceProtocol = (assigners) => {
  if (assigners.length === 0) {
    return EAGER
  }

  const supported = assigners.map((assigner) => ({
    name: assigner.name,
    protocols: assigner.supportedProtocols ? assigner.supportedProtocols() : [EAGER],
  }))

  const intersection = supported.reduce(
    (shared, { protocols }) => shared.filter((protocol) => protocols.includes(protocol)),
    [EAGER, COOPERATIVE]
  )

  if (intersection.length === 0) {
    const described = supported
      .map(({ name, protocols }) => `${name} supports [${protocols.join(', ')}]`)
      .join('; ')

    throw new KafkaJSNonRetriableError(
      `The configured partition assigners share no rebalance protocol: ${described}`
    )
  }

  return intersection.reduce((best, protocol) =>
    PROTOCOL_IDS[protocol] > PROTOCOL_IDS[best] ? protocol : best
  )
}

const callDeadlines = new WeakSet()

/**
 * Marks the refusal a public API call's own deadline produced, so a layer that only sees the
 * refusal can tell an expired call (its run and membership may still be current) from a retired
 * run or a forced close. Held weakly, so it retains no error.
 * @template {Error} E
 * @param {E} error the timeout a public call's operation refuses with
 * @returns {E} the same error
 */
const markCallDeadline = (error) => {
  callDeadlines.add(error)
  return error
}

/**
 * @param {unknown} refusal what an operation's guard returned
 * @returns {boolean} whether it is a refusal marked by `markCallDeadline`
 */
const isCallDeadline = (refusal) => callDeadlines.has(refusal)

/**
 * One consumer control operation, with one monotonic budget and detachable owned waits.
 * Captured run, hook and ownership predicates remain caller-owned and are never recaptured.
 * Retiring this scope does not cancel application promises or undo transmitted requests.
 * Its `register`, `check` and `guard` match a producer `Generation`, so the same object is
 * accepted as the `fence` of `Cluster`, `BrokerPool` and `createRetry`.
 */
class ConsumerOperation {
  /**
   * @param {object} [options] immutable entry context
   * @param {number} [options.deadlineAt=Infinity] absolute performance.now() deadline
   * @param {() => Error|null} [options.guard] original identity and ownership refusal
   * @param {() => number} [options.now] monotonic clock, shared with the deadline owner
   * @param {(cause?: Error) => Error} [options.onTimeout] original typed timeout factory
   */
  constructor({
    deadlineAt = Infinity,
    guard = () => null,
    now = () => performance.now(),
    onTimeout = (cause) =>
      new KafkaJSDeliveryTimeout('Consumer operation deadline expired', { cause }),
  } = {}) {
    this.deadlineAt = deadlineAt
    this.identityGuard = guard
    this.now = now
    this.onTimeout = onTimeout
    this.error = null
    this.lastError = undefined
    this.waiters = new Set()
    this.disposers = new Set()
    this.timer = null
    this.arm()
  }

  /**
   * Checks time and original identity before an effect, including when timer delivery is late.
   * A bound property, so it can be handed to the connection layer as a pre-write guard.
   * @returns {Error|null} the first terminal refusal, stable across later checks
   */
  guard = () => {
    if (this.error) return this.error
    if (this.now() >= this.deadlineAt) this.retire(this.onTimeout(this.lastError))
    if (!this.error) {
      const error = this.identityGuard()
      if (error) this.retire(error)
    }
    return this.error
  }

  /**
   * Reports refusal to fence consumers that inspect lifecycle state before installing resources.
   * @returns {boolean} true after retirement, identity refusal or actual deadline expiry
   */
  get closed() {
    return this.guard() !== null
  }

  /**
   * Throws a terminal refusal before starting work or publishing its result.
   * @returns {void}
   * @throws {Error} expiry, original identity refusal or explicit retirement
   */
  check() {
    const error = this.guard()
    if (error) throw error
  }

  /**
   * Remaining original budget; this never renews it.
   * @returns {number} nonnegative milliseconds, or Infinity
   */
  remaining() {
    return Math.max(0, this.deadlineAt - this.now())
  }

  /**
   * Owns at most one deadline wake. Early timer delivery re-arms the same original deadline.
   * @private
   * @returns {void}
   */
  arm() {
    if (this.error || this.deadlineAt === Infinity) return
    this.timer = setTimeout(
      () => {
        this.timer = null
        if (!this.guard()) this.arm()
      },
      Math.min(2147483647, Math.max(0, Math.ceil(this.remaining())))
    )
  }

  /**
   * Registers synchronous client cleanup; the caller removes it when its resource settles.
   * A retired operation cancels immediately.
   * @param {(error: Error) => void} cancel remove one owned listener, timer or queue registration
   * @returns {() => void} unregister without cancelling
   */
  register(cancel) {
    const error = this.guard()
    if (error) {
      cancel(error)
      return () => {}
    }
    this.disposers.add(cancel)
    return () => this.disposers.delete(cancel)
  }

  /**
   * Fences continuations, clears owned resources and rejects all client-facing waits once.
   * Cleanup failures cannot prevent other cleanup or replace the first terminal reason.
   * @param {Error} [error] original retirement reason
   * @returns {void}
   */
  retire(error = new KafkaJSCommitFailedError('Consumer operation is no longer current')) {
    if (this.error) return
    this.error = error
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
    for (const cancel of this.disposers) {
      try {
        cancel(error)
      } catch {}
    }
    this.disposers.clear()
    for (const reject of this.waiters) reject(error)
    this.waiters.clear()
  }

  /**
   * Releases a settled operation and refuses any orphaned continuation that retained it.
   * @returns {void}
   */
  finish() {
    this.retire()
  }

  /**
   * Bounds a thunk without starting it after refusal; late rejection is always observed.
   * Every effect inside the thunk must still call check after its own asynchronous boundaries.
   * @template T
   * @param {() => T|Promise<T>} action client operation or application callback invocation
   * @returns {Promise<T>} settles at completion, expiry or explicit retirement
   * @throws {Error} action failure, expiry or original identity refusal
   */
  wait(action) {
    return new Promise((resolve, reject) => {
      try {
        this.check()
      } catch (error) {
        reject(error)
        return
      }
      const fail = (error) => {
        this.waiters.delete(fail)
        reject(error)
      }
      this.waiters.add(fail)
      let result
      try {
        result = action()
      } catch (error) {
        fail(error)
        return
      }
      Promise.resolve(result).then((value) => {
        this.waiters.delete(fail)
        try {
          this.check()
          resolve(value)
        } catch (error) {
          reject(error)
        }
      }, fail)
    })
  }

  /**
   * Sleeps on a client-owned timer that retirement removes as well as settling the caller.
   * A delay computed from a deadline that has just passed may be slightly negative; it is
   * scheduled as an immediate timer.
   * @param {number} delay milliseconds; a negative value sleeps for 0
   * @returns {Promise<void>}
   * @throws {Error} expiry, original identity refusal or explicit retirement
   */
  sleep(delay) {
    return this.wait(
      () =>
        new Promise((resolve) => {
          let unown = () => {}
          const timer = setTimeout(
            () => {
              unown()
              resolve()
            },
            Math.max(0, delay)
          )
          unown = this.register(() => clearTimeout(timer))
        })
    )
  }

  /**
   * This operation as the private scope the shared retrier accepts, so that retrier's backoff timer
   * is owned by the operation: retirement removes it and settles the retried call at once.
   * @returns {{guard: () => Error|null, wait: Function, delay: (delay: number) => Promise<void>}}
   */
  retryScope() {
    return {
      guard: this.guard,
      wait: (action) => this.wait(action),
      delay: (delay) => this.sleep(delay),
    }
  }

  /**
   * Retries only this consumer operation, retaining the original deadline across nested work.
   * Uses the existing backoff and error rules without scheduling an unowned shared-retrier timer.
   * @template T
   * @param {(bail: (error: Error) => never, retryCount: number, retryTime: number) => T|Promise<T>} action attempt
   * @param {object} [options] existing retry options; maxTotalTime is already captured at entry
   * @returns {Promise<T>} accepted attempt result
   * @throws {Error} original bail, terminal error, exhausted retry budget or scope refusal
   */
  async retry(action, options = {}) {
    const configs = Object.assign({}, createRetry.defaults, options)
    const backoff = createRetry.backoff(configs)
    let retryTime = backoff.initial()
    for (let retryCount = 0; ; retryCount++) {
      this.check()
      let bailed = false
      let bailError
      const bail = (error) => {
        bailed = true
        bailError = error || new Error('Aborted')
        throw bailError
      }
      try {
        const value = await this.wait(() => action(bail, retryCount, retryTime))
        if (bailed) throw bailError
        return value
      } catch (error) {
        this.check()
        this.lastError = error
        if (bailed) throw bailError
        if (
          error.retriable === false ||
          ['RangeError', 'ReferenceError', 'SyntaxError', 'TypeError'].includes(error.name)
        ) {
          throw new KafkaJSNonRetriableError(error, {
            cause: error.cause || error,
          })
        }
        if (retryCount >= configs.retries) {
          throw new KafkaJSNumberOfRetriesExceeded(error, {
            retryCount,
            retryTime,
            cause: error.cause || error,
          })
        }
        if (retryTime > this.remaining()) {
          throw new KafkaJSDeliveryTimeout(
            'Next consumer attempt would exceed its original deadline',
            { cause: error }
          )
        }
        const nextRetryTime = backoff.next(retryTime)
        await this.sleep(retryTime)
        retryTime = nextRetryTime
      }
    }
  }
}

module.exports = {
  EAGER,
  COOPERATIVE,
  selectRebalanceProtocol,
  ConsumerOperation,
  markCallDeadline,
  isCallDeadline,
}
