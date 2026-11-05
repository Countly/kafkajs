const { isStaleCoordinator, COORDINATOR_LOAD_IN_PROGRESS_CODE } = require('../errors')
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
class Outage {
  /**
   * @param {number} limitMs how long an outage is ridden out: the member's session timeout
   */
  constructor(limitMs) {
    this.limitMs = limitMs
    this.since = null
  }

  /**
   * Whether the path should ride `error` out: retry it after a pause, without counting it. It rides
   * a lost connection and a coordinator that moved, is unavailable or is still loading the group
   * (16, 15, 14). The first such error starts the outage.
   * @param {*} error what the path's attempt threw
   * @param {number} [now=performance.now()] a monotonic time, in milliseconds
   * @returns {boolean} false for any other error, and for every error once the outage has lasted
   *   the limit
   */
  absorbs(error, now = performance.now()) {
    const coordinator =
      error != null &&
      (isStaleCoordinator(error) || error.code === COORDINATOR_LOAD_IN_PROGRESS_CODE)
    if (!isConnectionLoss(error) && !coordinator) return false
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

/**
 * Fetch outages indexed by broker. Each clock uses Outage's classification and budget, so a
 * productive sibling cannot renew a failed broker's deadline. Manager failure retains clocks
 * for the runner's counted retries; removal and explicit stop retire them.
 */
class FetchOutage {
  /**
   * Creates independent broker clocks with the same budget.
   * @param {number} limitMs the session window, or Infinity for consumer protocol
   */
  constructor(limitMs) {
    this.limitMs = limitMs
    this.nodes = new Map()
  }

  /**
   * Decides a failed Fetch using only its broker's clock.
   * @param {*} error the Fetch error
   * @param {number} [now=performance.now()] monotonic milliseconds
   * @param {number} nodeId the broker that failed
   * @returns {boolean} whether the runner can ride out this failure
   */
  absorbs(error, now = performance.now(), nodeId) {
    let outage = this.nodes.get(nodeId)
    if (!outage) {
      outage = new Outage(this.limitMs)
      if (!outage.absorbs(error, now)) return false
      this.nodes.set(nodeId, outage)
      return true
    }
    return outage.absorbs(error, now)
  }

  /**
   * Clears only the successful or retired broker's clock; never creates state.
   * @param {number} nodeId the broker that succeeded or retired
   * @returns {void}
   */
  end(nodeId) {
    this.nodes.delete(nodeId)
  }
}

module.exports = Outage
module.exports.FetchOutage = FetchOutage
