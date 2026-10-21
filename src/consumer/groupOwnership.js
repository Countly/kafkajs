const { KafkaJSCommitFailedError } = require('../errors')

/**
 * Fences commits by the membership they began under. Only current membership and memberships
 * retained by outstanding operations are stored; renewing an idle member retains no history.
 */
class GroupOwnership {
  constructor() {
    this.current = 0
    this.permanent = null
    this.states = new Map([[0, { cause: null, holders: 0 }]])
  }

  /** @returns {number} the current membership identity */
  token() {
    return this.current
  }

  /**
   * Retains one operation's identity until its response and local completion settle.
   * @private
   * @param {number} token the identity captured before any await
   * @returns {() => void} an idempotent release; it never makes a refused operation valid
   */
  retain(token) {
    const state = this.states.get(token)
    if (!state) return () => {}
    state.holders += 1
    let released = false
    return () => {
      if (released) return
      released = true
      state.holders -= 1
      if (token !== this.current && state.holders === 0) this.states.delete(token)
    }
  }

  /**
   * Invalidates exactly the current identity. An obsolete answer changes nothing.
   * @param {Error} cause the loss, including a synthetic poll-expiry error
   * @param {number} token the failing operation's identity
   * @returns {void}
   */
  lose(cause, token) {
    if (token !== this.current) return
    const state = this.states.get(token)
    if (!state.cause) state.cause = cause
  }

  /**
   * Permanently ends this run when abandonment belongs to its current identity.
   * @private
   * @param {Error} cause the fatal loss
   * @param {number} token the failing operation's identity
   * @returns {void}
   */
  abandon(cause, token) {
    if (token !== this.current) return
    this.lose(cause, token)
    this.permanent = cause
  }

  /**
   * Starts a replacement membership unless this run was abandoned.
   * @returns {number} the identity the replacement uses
   */
  renew() {
    if (this.permanent) return this.current
    const previous = this.states.get(this.current)
    if (previous.holders === 0) this.states.delete(this.current)
    this.current += 1
    this.states.set(this.current, { cause: null, holders: 0 })
    return this.current
  }

  /**
   * Creates a refusal without inventing a broker code. Callers retain tokens across awaits.
   * @param {number} token the operation's immutable identity
   * @returns {KafkaJSCommitFailedError|null} null while the operation still owns membership
   */
  refusal(token) {
    const cause = this.states.get(token)?.cause || this.permanent
    if (cause) {
      return new KafkaJSCommitFailedError('The consumer no longer owns this membership', {
        cause,
        reason:
          cause.type === 'POLL_INTERVAL_EXCEEDED' ? 'poll-interval-exceeded' : 'membership-lost',
      })
    }
    if (token !== this.current) {
      return new KafkaJSCommitFailedError('The commit outlived its assignment', {
        reason: 'stale-assignment',
      })
    }
    return null
  }
}

module.exports = GroupOwnership
