const { KafkaJSNonRetriableError } = require('../../errors')
const { ConsumerOperation } = require('../groupProtocol/interface')

/**
 * Records one captured close's authority, absolute budgets and operation-owned deadline wake.
 * It owns no transport.
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
    this.operation = null
    this.deadlineOperation = null
    this.releaseDeadline = null
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
    if (timeout === 0) this.revoke()
    else this.armDeadline()
  }

  /**
   * Gives the episode the one bounded operation that owns its finals and leave, so a revocation
   * can wake their waits at once instead of at the original deadline. An episode that is already
   * revoked retires it immediately; an earlier disconnect deadline also owns a wake.
   * @param {import('../groupProtocol/interface').ConsumerOperation} operation The traffic's operation.
   * @returns {void}
   */
  attach(operation) {
    this.operation = operation
    this.armDeadline()
  }

  /**
   * Wakes attached traffic at a newly supplied earlier deadline. The traffic operation retains
   * its original cap; this owned wake can only shorten its wait, and checks actual time even
   * when timer delivery is delayed. Later disconnects cannot replace the first deadline.
   * @private
   * @returns {void}
   */
  armDeadline() {
    const operation = this.operation
    if (!operation) return
    const refused = this.guard()
    if (refused) {
      operation.retire(refused)
      return
    }
    const deadlineAt = Math.min(this.deadlineAt, this.finalDeadline)
    if (deadlineAt >= operation.deadlineAt || this.deadlineOperation) return
    const wake = new ConsumerOperation({
      deadlineAt,
      now: this.now,
      onTimeout: () => {
        this.revoke()
        return this.guard()
      },
    })
    this.deadlineOperation = wake
    this.releaseDeadline = operation.register(() => wake.finish())
  }

  /**
   * Revokes every unsent frame and retires the traffic operation in the same tick. A final that was
   * already written and is unanswered settles lost through its own wait, which then closes the
   * exact socket it was written on; nothing is resent.
   * @returns {void}
   */
  revoke() {
    this.revoked = true
    this.operation?.retire(this.guard())
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
    this.releaseDeadline?.()
    this.deadlineOperation?.finish()
    this.deadlineOperation = null
    this.releaseDeadline = null
    this.operation = null
    this.run = null
  }
}

module.exports = { ShareCloseEpisode }
