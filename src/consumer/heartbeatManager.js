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
 * about what a verdict means.
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
   *   a poll-interval overrun runs. Held, not yet called: the deadline only means something once
   *   a batch hand-off records one, so the tick evaluates it beside those reset points, not here.
   */
  constructor({ groupMembership, pollTimer, heartbeatInterval, logger, onError, onPollExpired }) {
    this.groupMembership = groupMembership
    this.pollTimer = pollTimer
    this.heartbeatInterval = heartbeatInterval
    this.logger = logger.namespace('HeartbeatManager')
    this.onError = onError
    this.onPollExpired = onPollExpired

    this.handle = null
    this.disabled = false
    this.inFlight = false
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
    if (this.handle) {
      return
    }

    this.schedule()
  }

  /** Cancels the chain. @returns {void} */
  stop() {
    if (this.handle) {
      clearTimeout(this.handle)
      this.handle = null
    }
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
   * @private
   * @returns {void}
   */
  schedule() {
    this.handle = setTimeout(() => this.tick(), this.heartbeatInterval)
  }

  /**
   * One tick: reschedule while suspended, send otherwise, and never queue a second send behind a
   * slow one.
   * @private
   * @returns {Promise<void>}
   */
  async tick() {
    // A stopped manager must NOT reschedule: it would resurrect itself and leak the handle.
    // A disabled one must, because `enable()` does not re-arm anything.
    if (!this.handle) {
      return
    }

    if (this.disabled) {
      this.schedule()
      return
    }

    if (this.inFlight) {
      this.schedule()
      return
    }

    this.inFlight = true

    try {
      await this.groupMembership.heartbeat({ interval: this.heartbeatInterval })
    } catch (error) {
      // An in-flight response landing after `stop()` belongs to a generation nobody is in any
      // more; classifying it would raise a verdict for a member that has already left.
      if (this.handle) {
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
        this.logger.debug('Discarding heartbeat result received after stop', {
          error: error.message,
        })
      }
    } finally {
      this.inFlight = false
    }

    if (this.handle) {
      this.schedule()
    }
  }
}
