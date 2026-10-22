const { isStaleCoordinator } = require('../errors')
const { isConnectionLoss } = require('../network/connectionFailure')

/**
 * One outage seen by a consumer path: the lost connections and coordinator moves it met since it
 * last succeeded. The path retries them without spending its retry budget for as long as the
 * outage is shorter than the classic member's session timeout, the time the coordinator keeps a
 * member it does not hear from. A longer outage has cost the membership already and is handled as
 * any other failure. The outage ends only when the path succeeds; its deadline runs on a monotonic
 * clock from its first failure, so slow attempts cannot extend it. The Java consumer never surfaces
 * a lost connection from `poll()`.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
 */
module.exports = class Outage {
  /**
   * @param {number} limitMs how long an outage is ridden out: the member's session timeout
   */
  constructor(limitMs) {
    this.limitMs = limitMs
    this.since = null
  }

  /**
   * Whether the path should ride `error` out: retry it after a pause, without counting it. The
   * first such error starts the outage.
   * @param {*} error what the path's attempt threw
   * @param {number} [now=performance.now()] a monotonic time, in milliseconds
   * @returns {boolean} false for any other error, and for every error once the outage has lasted
   *   the limit
   */
  absorbs(error, now = performance.now()) {
    if (!isConnectionLoss(error) && !(error != null && isStaleCoordinator(error))) return false
    if (this.since === null) this.since = now
    return now - this.since < this.limitMs
  }

  /**
   * Ends the outage: the path succeeded.
   * @returns {void}
   */
  end() {
    this.since = null
  }
}
