const { KafkaJSNonRetriableError } = require('../../errors')

/**
 * Records one captured close's authority and absolute budgets; it owns no transport or timer.
 *
 * With `t0` the first valid disconnect call, `T` its timeout, `R = min(1000, floor(T / 4))` and
 * `D = t0 + T`, handlers are awaited until `H = D - R`. After that boundary every final and the
 * leave share one cap, `F = min(D, boundary + requestTimeout)`. Clock equality at `D` or `F`
 * forbids traffic. Time is read from the injected clock at every check, so a late timer cannot
 * authorize a write.
 */
class ShareCloseEpisode {
  /**
   * @param {object} options Captured identity and monotonic clock dependencies.
   * @param {object} options.run Original run object, never replaced after an await.
   * @param {number} options.requestTimeout Finite final-and-leave cap in milliseconds.
   * @param {() => number} [options.now] Monotonic milliseconds, normally `performance.now`.
   * @returns {ShareCloseEpisode} A natural-wait episode with no timer or wire authority yet.
   */
  constructor({ run, requestTimeout, now = () => performance.now() }) {
    Object.defineProperty(this, 'identity', { value: Symbol('share-close') })
    this.run = run
    this.requestTimeout = requestTimeout
    this.now = now
    this.disconnectSet = false
    this.deadlineAt = Infinity
    this.handlerBoundary = Infinity
    this.finalDeadline = Infinity
    this.frozen = false
    this.revoked = false
    this.leaveClaimed = false
    this.nodes = new Map()
  }

  /**
   * Supplies the first valid disconnect's deadline; zero always revokes unsent work at once.
   * Later nonzero calls cannot renew, shorten or replace the first supplied deadline.
   * @param {number|undefined} timeout Already-validated timeout; absent means a natural wait.
   * @param {number} [startedAt] Original public call entry time, captured before parsing options.
   * @returns {void}
   */
  disconnect(timeout, startedAt = this.now()) {
    if (!this.disconnectSet) {
      this.disconnectSet = true
      if (timeout !== undefined) {
        this.deadlineAt = startedAt + timeout
        this.handlerBoundary = this.deadlineAt - Math.min(1000, Math.floor(timeout / 4))
      }
    }
    if (timeout === 0) this.revoked = true
  }

  /**
   * Freezes one boundary and shares one final cap across every node and the leave.
   * @param {Array<{nodeId: number, current: () => boolean}>} nodes Captured idle node bindings;
   *   busy and unopened nodes are omitted. A later call cannot adopt replacement sessions.
   * @returns {void}
   */
  freeze(nodes) {
    if (this.frozen) return
    this.frozen = true
    this.finalDeadline = Math.min(this.deadlineAt, this.now() + this.requestTimeout)
    for (const node of nodes) this.nodes.set(node.nodeId, { binding: node, claimed: false })
  }

  /**
   * Checks actual time against the whole-call deadline and the shared final cap.
   * @returns {Error|null} A refusal once the episode is revoked or a deadline was reached.
   */
  guard() {
    if (this.now() >= Math.min(this.deadlineAt, this.finalDeadline)) this.revoked = true
    return this.revoked ? new KafkaJSNonRetriableError('Captured close is no longer current') : null
  }

  /**
   * Checks actual time and one captured binding at every pre-write continuation.
   * @param {{current: () => boolean}} binding Captured node binding or coordinator binding.
   * @returns {Error|null} Null only while the captured authority remains valid.
   */
  refusal(binding) {
    const refused = this.guard()
    if (refused) return refused
    return this.frozen && binding.current()
      ? null
      : new KafkaJSNonRetriableError('Captured close is no longer current')
  }

  /**
   * Claims a node's final once. A claimed final never becomes eligible again.
   * @param {number} nodeId Captured established node.
   * @returns {object|null} The original immutable binding, or null for refused or duplicate work.
   */
  claimFinal(nodeId) {
    const entry = this.nodes.get(nodeId)
    if (!entry || entry.claimed || this.refusal(entry.binding)) return null
    entry.claimed = true
    return entry.binding
  }

  /**
   * Claims the one leave, which goes after the finals; callers keep the same predicate to the wire.
   * @param {{current: () => boolean}} coordinator Captured established coordinator binding.
   * @returns {boolean} Whether this episode authorized its one leave attempt.
   */
  claimLeave(coordinator) {
    if (this.leaveClaimed || this.refusal(coordinator)) return false
    this.leaveClaimed = true
    return true
  }

  /**
   * Retires the record after its owner detached the captured transport and resources.
   * @returns {void}
   */
  finish() {
    this.revoked = true
    this.nodes.clear()
    this.run = null
  }
}

module.exports = { ShareCloseEpisode }
