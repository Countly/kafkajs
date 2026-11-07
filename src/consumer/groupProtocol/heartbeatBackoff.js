const retryDefaults = require('../../retry/defaults')

/**
 * Java's `retry.backoff.max.ms` default: a heartbeat retry never waits longer than this, so a
 * member reaches a moved coordinator well within the session the coordinator restarts on load.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java
 */
const HEARTBEAT_BACKOFF_MAX_MS = 1000

/**
 * The exponential backoff a heartbeat error arms, with the same shape the client's retrier uses:
 * `initialRetryTime * multiplier ** attempts`, jittered by `factor` and capped at `maxRetryTime`,
 * which is itself held to `HEARTBEAT_BACKOFF_MAX_MS`. Its remaining delay is what the membership
 * reports as its heartbeat interval while it is armed, because the heartbeat manager reads only
 * that.
 */
class HeartbeatBackoff {
  /** @param {{ initialRetryTime?: number, maxRetryTime?: number, multiplier?: number, factor?: number }} [retry] */
  constructor(retry = {}) {
    const { initialRetryTime, maxRetryTime, multiplier, factor } = {
      ...retryDefaults,
      ...retry,
    }
    this.initialRetryTime = initialRetryTime
    this.maxRetryTime = Math.min(maxRetryTime, HEARTBEAT_BACKOFF_MAX_MS)
    this.multiplier = multiplier
    this.factor = factor
    this.reset()
  }

  /** Clears the backoff entirely, so the next attempt is immediate. @returns {void} */
  reset() {
    this.attempts = 0
    this.until = 0
    this.due = false
  }

  /**
   * Clears the backoff and marks one request due right away: an answer that says "ask the new
   * coordinator" is not a reason to wait, and the request must not be held by the interval gate
   * either.
   * @returns {void}
   */
  retryNow() {
    this.reset()
    this.due = true
  }

  /** @param {number} now @returns {void} */
  onFailedAttempt(now) {
    const base = Math.min(
      this.maxRetryTime,
      this.initialRetryTime * this.multiplier ** this.attempts
    )
    this.attempts += 1
    const jittered = Math.round(base * (1 + this.factor * (Math.random() * 2 - 1)))
    this.until = now + Math.min(this.maxRetryTime, jittered)
    this.due = true
  }

  /**
   * Whether the one retry this backoff owes has come due. It is a single token, not a mode: an
   * error that neither arms a backoff nor clears one - `TOPIC_AUTHORIZATION_FAILED` is the code
   * designed to recur forever without touching it - must not leave the interval gate bypassed for
   * every later call.
   * @param {number} now
   * @returns {boolean}
   */
  isDue(now) {
    return this.due && !this.pending(now)
  }

  /** Consumes the retry an elapsed backoff owed, so it lets exactly one request past. @returns {void} */
  takeDue() {
    this.due = false
  }

  /** @param {number} now @returns {boolean} */
  pending(now) {
    return this.until > now
  }

  /** @param {number} now @returns {number} milliseconds left, 0 when nothing is armed */
  remaining(now) {
    return Math.max(0, this.until - now)
  }
}

module.exports = HeartbeatBackoff
