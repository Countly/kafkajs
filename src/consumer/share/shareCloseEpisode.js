const { KafkaJSNonRetriableError } = require('../../errors')
const { ConsumerOperation } = require('../groupProtocol/interface')

/**
 * Records one captured close's authority, absolute budgets and operation-owned deadline wake.
 * It owns no transport.
 *
 * With `t0` the first valid disconnect call, `T` its timeout, `R = min(1000, floor(T / 4))` and
 * `D = t0 + T`, handlers are awaited until `H = D - R`. From the end of that wait, `B`, every final
 * and the leave share one cap, `F = min(D, B + requestTimeout)`. Clock equality at `D` or `F`
 * forbids traffic. Time is read from the injected clock at every check, so a late timer cannot
 * authorize a write.
 *
 * The episode also keeps the node records it closes (`records`), the data binding of each
 * connection frozen at the freeze (`bindings`), the captured final on a connection (`finals`)
 * and the connections it closed, so every connection is closed at most once; `finish` releases
 * all of them.
 */
class ShareCloseEpisode {
  /**
   * @param {object} options Captured identity and monotonic clock dependencies.
   * @param {object} options.run Original run object, never replaced after an await.
   * @param {number} options.requestTimeout Finite final-and-leave cap in milliseconds.
   * @param {() => number} [options.now] Monotonic milliseconds, normally `performance.now`.
   * @param {object[]} [options.records] The node records the close works on, snapshotted at entry.
   * @param {object} [options.logger] Logger whose `debug` reports a failed connection close.
   * @returns {ShareCloseEpisode} A natural-wait episode with no timer or wire authority yet.
   */
  constructor({ run, requestTimeout, now = () => performance.now(), records = [], logger }) {
    Object.defineProperty(this, 'identity', { value: Symbol('share-close') })
    this.run = run
    this.requestTimeout = requestTimeout
    this.now = now
    this.logger = logger
    this.disconnectSet = false
    this.deadlineAt = Infinity
    this.handlerBoundary = Infinity
    this.capStartedAt = null
    this.finalDeadline = Infinity
    this.frozen = false
    this.revoked = false
    this.leaveClaimed = false
    this.nodes = new Map()
    this.records = records
    this.expired = []
    this.bindings = new Map()
    this.finals = new Map()
    this.closedConnections = new Set()
    this.operation = null
    this.drainOperation = null
    this.deadlineOperation = null
    this.releaseDeadline = null
  }

  /**
   * Starts the shared cap once, at the end of the handler wait: `F = min(D, at + requestTimeout)`.
   * Later calls, including the one inside {@link ShareCloseEpisode#freeze}, keep the first cap.
   * @param {number} at The episode time the handler wait ended.
   * @returns {void}
   */
  startCap(at) {
    if (this.capStartedAt !== null) return
    this.capStartedAt = at
    this.finalDeadline = Math.min(this.deadlineAt, at + this.requestTimeout)
  }

  /**
   * @returns {number} The latest freeze time: `B` when the handler wait ran to its boundary `H`,
   *   else `min(B + floor(requestTimeout / 2), H, D)`. Read again after a later disconnect.
   */
  drainBoundary() {
    const begin = this.capStartedAt
    if (begin >= this.handlerBoundary) return begin
    return Math.min(
      begin + Math.floor(this.requestTimeout / 2),
      this.handlerBoundary,
      this.deadlineAt
    )
  }

  /**
   * Closes one data or coordinator connection at most once per episode, so a connection already
   * disconnecting, which still reports itself connected, is never disconnected or written again.
   * @param {{disconnect: (options: {drainTimeout: number}) => Promise<*>}} connection The socket.
   * @returns {void} The close runs in the background; a failure is logged at debug level.
   */
  closeConnection(connection) {
    if (this.closedConnections.has(connection)) return
    this.closedConnections.add(connection)
    connection.disconnect({ drainTimeout: 0 }).catch((error) => {
      this.logger?.debug('Share captured connection close failed', { error: error.message })
    })
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
   * revoked retires it immediately; an earlier disconnect deadline also owns a wake. A wake left
   * from an earlier operation is finished first, so the new operation always gets its own.
   * @param {import('../groupProtocol/interface').ConsumerOperation} operation The traffic's operation.
   * @returns {void}
   */
  attach(operation) {
    this.releaseDeadline?.()
    this.deadlineOperation?.finish()
    this.releaseDeadline = null
    this.deadlineOperation = null
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
   * Freezes one boundary and shares one final cap across every node and the leave. The cap starts
   * here unless the handler wait already started it.
   * @param {Array<{nodeId: number, current: () => boolean}>} nodes Captured idle node bindings;
   *   busy and unopened nodes are omitted. A later call cannot adopt replacement sessions.
   * @returns {void}
   */
  freeze(nodes) {
    if (this.frozen) return
    this.frozen = true
    this.startCap(this.now())
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
   * Retires the record after its owner detached the captured transport and resources, and drops
   * every node record, binding and connection it kept.
   * @returns {void}
   */
  finish() {
    this.revoked = true
    this.nodes.clear()
    this.records = null
    this.expired = []
    this.bindings.clear()
    this.finals.clear()
    this.closedConnections.clear()
    this.drainOperation?.retire()
    this.drainOperation = null
    this.releaseDeadline?.()
    this.deadlineOperation?.finish()
    this.deadlineOperation = null
    this.releaseDeadline = null
    this.operation = null
    this.run = null
  }
}

module.exports = { ShareCloseEpisode }
