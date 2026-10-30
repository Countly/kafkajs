const ResetObligations = require('./offsetManager/resetObligations')
const { ConsumerOperation } = require('./groupProtocol/interface')

/**
 * What one explicit connection of a consumer owns: the fence of its connect, the deadline scopes of
 * its public calls, its topic-recreation registration with the resets it owes, and its crash-restart
 * timer. A consumer starts a new lifetime at the first connect, run or start after an explicit
 * disconnect began, so a close that settles late releases only what it owned and never what its
 * replacement registered. A stop and a crash restart keep the same lifetime and its debt.
 */
class ControlLifetime {
  /**
   * @param {ResetObligations} [obligations] the debt table of a predecessor that already released
   *   it, which is empty and unlinked; omitted, a table of its own, so a replacement that begins
   *   while its predecessor's close is still running never shares the predecessor's
   */
  constructor(obligations = new ResetObligations()) {
    /** Retired when this lifetime ends or its close is forced; no deadline, so it owns no timer. */
    this.fence = new ConsumerOperation()
    /** @type {Set<ConsumerOperation>} the public calls in flight, retired by a forced close */
    this.calls = new Set()
    this.obligations = obligations
    /** @type {(() => void)|null} removes the topic-recreation listener */
    this.removeListener = null
    /** @type {ReturnType<typeof setTimeout>|null} the pending crash restart */
    this.restartTimer = null
    /** True once an explicit disconnect or disposal began; the next acquisition starts a new one. */
    this.ending = false
    /** True once `release` ran, so its debt table is empty and free to hand to a successor. */
    this.released = false
  }

  /**
   * Retires the connect fence and every public call in flight, cancelling their owned timers.
   * @param {Error} error the first terminal reason
   * @returns {void}
   */
  retire(error) {
    this.fence.retire(error)
    for (const call of [...this.calls]) call.retire(error)
  }

  /**
   * Cancels the pending crash restart, if any.
   * @returns {void}
   */
  cancelRestart() {
    clearTimeout(this.restartTimer)
    this.restartTimer = null
  }

  /**
   * Releases everything this lifetime registered: the listener, the debt and the restart timer.
   * Idempotent; called once, synchronously, when its explicit disconnect settles.
   * @returns {void}
   */
  release() {
    this.removeListener?.()
    this.removeListener = null
    this.obligations.clear()
    this.cancelRestart()
    this.released = true
  }
}

module.exports = ControlLifetime
