/**
 * The deadline a handler has to return within, on a MONOTONIC clock.
 *
 * `Date.now()` is wall-clock: an NTP step or a VM resume between a poll and the next check would
 * declare an expiry with no handler overrun, and an expiry is destructive - the member leaves the
 * group and discards uncommitted offsets. `performance.now()` cannot move backwards or jump.
 *
 * The timer only REPORTS: it starts nothing, cancels nothing and emits nothing. Whoever detects
 * an expiry owns what happens next.
 */
module.exports = class PollTimer {
  /**
   * @param {{ pollDeadline: number }} options milliseconds a handler may take between batches
   */
  constructor({ pollDeadline }) {
    this.pollDeadline = pollDeadline
    this.lastPollAt = performance.now()
    this.stopped = false
  }

  /** Records a batch hand-off, or a fetch round that had nothing to hand off. @returns {void} */
  poll() {
    this.lastPollAt = performance.now()
  }

  /** Restarts the deadline, e.g. after a rebalance. @returns {void} */
  reset() {
    this.lastPollAt = performance.now()
    this.stopped = false
  }

  /** Stops reporting expiry until the next `reset()`. @returns {void} */
  stop() {
    this.stopped = true
  }

  /** @returns {number} milliseconds since the last poll */
  elapsed() {
    return performance.now() - this.lastPollAt
  }

  /** @returns {number} the configured deadline, for the event payload */
  deadline() {
    return this.pollDeadline
  }

  /** @returns {boolean} whether the deadline has passed and reporting is enabled */
  expired() {
    return !this.stopped && this.elapsed() > this.pollDeadline
  }
}
