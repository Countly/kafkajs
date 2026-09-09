/**
 * Sends group heartbeats on a timer, so a handler that awaits no longer starves them (KIP-62).
 *
 * A `setTimeout` CHAIN, rescheduled from the previous tick's completion rather than an interval:
 * a slow request must not queue ticks behind itself. The resulting `interval + requestDuration`
 * drift is why a `heartbeatInterval` above a third of `sessionTimeout` is warned about.
 *
 * The handle is deliberately NOT unref'd: an unref'd heartbeat would let the process exit while
 * this member is still in the group.
 *
 * The manager emits nothing and depends on neither the runner nor the instrumentation emitter.
 * It reports through two injected callbacks, so the component it reports to can hold every rule
 * about what a verdict means. The tick is also where the handler's poll deadline is DETECTED -
 * only while heartbeats are enabled, only while no rebalance round is in flight, and never in
 * place of one: an overrun hands off to `onPollExpired` and the tick ends there.
 *
 * Two entry points arm the chain: `ensureStarted()`, which is idempotent and does nothing when a
 * chain is already armed, and `requestTick()`, which supersedes whatever is armed with a
 * zero-delay one. The tick's delay and the `interval` it reports are read from the membership on
 * every schedule, so a protocol whose cadence the coordinator owns is followed rather than
 * approximated.
 *
 * `stop()` and `shutdown()` are different verbs. `stop()` cancels the chain but leaves the manager
 * usable, because the poll-interval guard stops it and the recovery round rejoins through
 * `requestTick()`. `shutdown()` is terminal: the consumer is going away, and neither entry point
 * may arm anything after it, or heartbeats and a non-unref'd timer would outlive
 * `consumer.disconnect()`.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread
 */
module.exports = class HeartbeatManager {
  /**
   * @param {object} options
   * @param {{ heartbeat: (options: { interval: number }) => Promise<void> }} options.groupMembership
   * @param {import('./pollTimer')} options.pollTimer
   * @param {number} options.heartbeatInterval
   * @param {import('../../types').Logger} options.logger
   * @param {(error: Error) => void} options.onError called with whatever a heartbeat threw
   * @param {(details: { elapsedMs: number }) => Promise<void>} options.onPollExpired the sequence
   *   a poll-interval overrun runs. It owns everything an overrun implies, including stopping
   *   this manager, so the tick awaits it and then evaluates nothing further.
   * @param {() => boolean} [options.isRebalancing] whether a rebalance round is running right now;
   *   the poll deadline is not evaluated while one is
   */
  constructor({
    groupMembership,
    pollTimer,
    heartbeatInterval,
    logger,
    onError,
    onPollExpired,
    isRebalancing = () => false,
  }) {
    this.groupMembership = groupMembership
    this.pollTimer = pollTimer
    this.heartbeatInterval = heartbeatInterval
    this.logger = logger.namespace('HeartbeatManager')
    this.onError = onError
    this.onPollExpired = onPollExpired
    this.isRebalancing = isRebalancing

    this.handle = null
    this.disabled = false
    this.inFlight = false
    /**
     * Latched by `shutdown()` only. Once set, neither `ensureStarted()` nor `requestTick()` arms
     * anything again: the runner has left the group and any chain armed after that would keep
     * heartbeating - and hold the event loop open - for a member that is gone.
     */
    this.stopped = false
    /**
     * Identifies the armed chain. A tick captures it before its `await` and compares it after, so
     * a `stop()` + `ensureStarted()` landing inside an in-flight heartbeat cannot leave the
     * settling tick scheduling a second chain on top of the new one.
     */
    this.chainId = 0
    /**
     * Set by `requestTick()` while a tick is in flight, consumed by that tick's `finally`. The
     * flag exists because arming a second chain under a running one would double it.
     */
    this.tickRequested = false
  }

  /** @returns {boolean} whether the chain is armed */
  isRunning() {
    return this.handle !== null
  }

  /**
   * Arms the timer if it is not already armed. Idempotent by design: it is called after the
   * initial join AND after every successful rebalance round, so a manager stopped by the poll
   * guard is re-armed by the round that recovers from it - without a second timer when one is
   * already running.
   * @returns {void}
   */
  ensureStarted() {
    if (this.handle || this.stopped) {
      return
    }

    this.chainId++
    this.schedule()
  }

  /** Cancels the chain, leaving the manager armable again. @returns {void} */
  stop() {
    if (this.handle) {
      clearTimeout(this.handle)
      this.handle = null
    }
  }

  /**
   * Cancels the chain for good.
   *
   * The terminal counterpart of `stop()`, called when the runner is shutting down or has crashed.
   * A plain `stop()` cannot carry this meaning: the poll-interval guard stops the manager too, and
   * the round that recovers from an overrun rejoins by arming a heartbeat on the stopped manager.
   * @returns {void}
   */
  shutdown() {
    this.stopped = true
    this.stop()
  }

  /** Suspends evaluation without cancelling the chain. @returns {void} */
  disable() {
    this.disabled = true
  }

  /** Resumes evaluation. @returns {void} */
  enable() {
    this.disabled = false
  }

  /**
   * Asks for a heartbeat NOW instead of at the next scheduled tick.
   *
   * The KIP-848 membership needs three heartbeats to leave immediately - the joining one, the ack
   * and the leave - and `schedule()` alone would delay them by up to one server interval, which
   * for the ack also delays the "on generated" state transition that follows it.
   *
   * Deliberately UNguarded on `handle`: the join runs before `ensureStarted()` does, so this is
   * what arms the chain in the first place, and the poll-expiry recovery round rejoins the same
   * way on a manager the guard has stopped. The chain it arms is the same one `ensureStarted()`
   * would have armed later, so nothing is doubled. A DISABLED manager is left alone, because
   * `enable()` re-arms nothing, and a SHUT DOWN one is left alone for good.
   * @returns {void}
   */
  requestTick() {
    if (this.disabled || this.stopped) {
      return
    }

    // A tick already sending cannot be pre-empted: arming here would leave two live chains, so
    // the flag is handed to that tick's `finally`, which re-arms through `armNow()`.
    if (this.inFlight) {
      this.tickRequested = true
      return
    }

    this.armNow()
  }

  /**
   * Supersedes whatever is armed with a zero-delay chain.
   *
   * `chainId` is bumped and CAPTURED, exactly as `schedule()` does: a callback that read the live
   * field would always match `tick()`'s own guard, so a chain superseded between arming and firing
   * would not be rejected.
   * @private
   * @returns {void}
   */
  armNow() {
    if (this.handle) {
      clearTimeout(this.handle)
      this.handle = null
    }

    const chainId = ++this.chainId
    this.handle = setTimeout(() => this.tick(chainId), 0)
  }

  /**
   * The delay the next tick is armed with, and the `interval` that tick reports to the membership.
   *
   * Read PER SCHEDULE rather than once at construction: under the consumer group protocol the
   * cadence is the `HeartbeatIntervalMs` of the last response and moves whenever the coordinator
   * says so, and while a backoff is armed it is the backoff's remaining delay.
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
   *
   * A membership that does not answer keeps the configured value, which is what the classic
   * implementation's interval is.
   * @private
   * @returns {number} milliseconds
   */
  nextInterval() {
    const fromMembership = this.groupMembership.heartbeatInterval

    if (typeof fromMembership !== 'function') {
      return this.heartbeatInterval
    }

    const interval = fromMembership.call(this.groupMembership)
    return Number.isFinite(interval) && interval >= 0 ? interval : this.heartbeatInterval
  }

  /**
   * @private
   * @returns {void}
   */
  schedule() {
    const { chainId } = this
    this.handle = setTimeout(() => this.tick(chainId), this.nextInterval())
  }

  /**
   * One tick, in the Java client's order: suspended, then the poll deadline, then the heartbeat.
   * Reschedules while suspended, sends otherwise, and never queues a second send behind a slow
   * one.
   * @private
   * @param {number} [chainId] the chain this tick belongs to; a tick from a superseded chain
   *   returns without sending or rescheduling
   * @returns {Promise<void>}
   */
  async tick(chainId = this.chainId) {
    // A stopped manager must NOT reschedule: it would resurrect itself and leak the handle.
    // A disabled one must, because `enable()` does not re-arm anything.
    if (!this.handle || chainId !== this.chainId) {
      return
    }

    if (this.disabled) {
      this.schedule()
      return
    }

    // Time spent inside a rebalance round is the coordinator's, not the handler's: a JoinGroup is
    // held until every member rejoins, and the rebalance timeout is the same number as the poll
    // deadline, so a slow round would otherwise expire a member that was never slow - mid-round,
    // where the LeaveGroup lands under a generation its own SyncGroup is still using. The round
    // resets the deadline itself once it completes.
    if (this.pollTimer.expired() && !this.isRebalancing()) {
      await this.reportPollExpired(chainId)
      return
    }

    if (this.inFlight) {
      this.schedule()
      return
    }

    this.inFlight = true
    let rearm = false

    try {
      await this.groupMembership.heartbeat({ interval: this.nextInterval() })
    } catch (error) {
      // A result that landed after `stop()`, after the chain was re-armed under a new generation,
      // or inside the JoinGroup window belongs to a generation nobody is in any more: it is
      // DISCARDED, not classified. Classifying it would raise a verdict - and for codes the
      // rebalance path never drops, an abort and a discard of uncommitted offsets - for a
      // generation this member's own join is already replacing.
      // @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
      if (this.handle && chainId === this.chainId && !this.disabled) {
        // The callback is injected, so its throw-safety is not this class's to assume: an escape
        // here would be an unhandled rejection (the tick's promise is discarded by setTimeout)
        // AND would skip the reschedule below, killing the chain until the session expires.
        try {
          this.onError(error)
        } catch (reportingError) {
          this.logger.error('Heartbeat error handler failed', {
            error: reportingError.message || reportingError,
            cause: error.message || error,
          })
        }
      } else {
        this.logger.debug(
          'Discarding heartbeat result received after stop or inside the join window',
          {
            error: error.message,
          }
        )
      }
    } finally {
      this.inFlight = false
      // Guarded on `handle`, which is the one fact `stop()` sets: a tick only ever runs on a
      // manager that WAS armed, so a null handle here means a stop landed inside this heartbeat
      // and re-arming would leave a non-unref'd timer holding the event loop open after the member
      // has left.
      rearm = this.tickRequested && this.handle !== null
      this.tickRequested = false
    }

    if (rearm) {
      // Bumps `chainId`, so this tick's captured id no longer matches and the tail below cannot
      // add a second chain on top of the one just armed.
      this.armNow()
    } else if (this.handle && chainId === this.chainId) {
      this.schedule()
    }
  }

  /**
   * Hands the overrun to the injected sequence and evaluates nothing further this tick.
   *
   * The chain is deliberately NOT rescheduled on success: the sequence stops this manager and the
   * round that recovers re-arms it through `ensureStarted()`. It IS rescheduled when the sequence
   * throws, because a manager that never heartbeats again would lose the session outright.
   *
   * @private
   * @param {number} chainId
   * @returns {Promise<void>}
   */
  async reportPollExpired(chainId) {
    try {
      await this.onPollExpired({ elapsedMs: this.pollTimer.elapsed() })

      // This class's own invariant, not the sequence's: `handle` must never point at a timeout
      // that has already fired, or `isRunning()` reports an armed chain that does not exist and
      // `ensureStarted()` refuses to re-arm it. The sequence normally stops this manager itself,
      // in which case this is a no-op - but it also has a path that returns without doing so. The
      // chain check keeps it from cancelling a chain the recovery re-armed inside the await.
      if (chainId === this.chainId) {
        this.stop()
      }
    } catch (error) {
      this.logger.error('Poll-interval overrun handler failed', {
        error: error.message || error,
      })

      if (this.handle && chainId === this.chainId) {
        this.schedule()
      }
    }
  }
}
