const { KafkaJSCommitFailedError } = require('../errors')

const ASSIGNMENT_REFUSED = 'assignment-refused'

/** @returns {{cause: Error|null, withheld: Error|null, holders: number}} a fresh identity's state */
const freshState = () => ({ cause: null, withheld: null, holders: 0 })

/**
 * Fences commits by the membership they began under. Only current membership and memberships
 * retained by outstanding operations are stored; renewing an idle member retains no history.
 *
 * An identity is current, withheld (its assignment was refused but the membership is kept) or
 * lost. A loss always wins over a withhold.
 */
class GroupOwnership {
  /**
   * @param {Object} [options]
   * @param {(cause: Error) => boolean} [options.isStale] whether a loss answers about a generation
   *   the member no longer holds
   */
  constructor({ isStale = () => false } = {}) {
    this.isStale = isStale
    this.current = 0
    this.permanent = null
    this.states = new Map([[0, freshState()]])
  }

  /**
   * @param {unknown} error a refusal
   * @returns {boolean} whether it refuses work because its assignment was withheld
   */
  static isAssignmentRefusal(error) {
    return error?.reason === ASSIGNMENT_REFUSED
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
   * Invalidates exactly the current identity. An obsolete answer changes nothing, and neither does
   * an answer about a generation the member no longer holds (Java's `generationUnchanged()`).
   * @param {Error} cause the loss, including a synthetic poll-expiry error
   * @param {number} token the failing operation's identity
   * @returns {void}
   */
  lose(cause, token) {
    if (token !== this.current || this.isStale(cause)) return
    const state = this.states.get(token)
    if (!state.cause) state.cause = cause
  }

  /**
   * Withholds the current identity's assignment while keeping its membership: the token does not
   * change, so answers stamped with it stay current, and a later `lose` still wins.
   * @param {Error} cause the refusal
   * @returns {void}
   */
  withhold(cause) {
    const state = this.states.get(this.current)
    state.withheld ??= cause
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
    this.states.set(this.current, freshState())
    return this.current
  }

  /**
   * @param {number} token an operation's identity
   * @returns {Error|null} the loss of that identity, or of the whole run once abandoned
   */
  lost(token) {
    return this.states.get(token)?.cause || this.permanent
  }

  /**
   * @param {number} token an operation's identity
   * @returns {boolean} whether its assignment is withheld and it is not lost
   */
  withheld(token) {
    const state = this.states.get(token)
    return Boolean(state?.withheld) && !state.cause && !this.permanent
  }

  /**
   * Creates a refusal without inventing a broker code. Callers retain tokens across awaits. A loss
   * comes first, then a withheld assignment, then a replaced identity.
   * @param {number} token the operation's immutable identity
   * @returns {KafkaJSCommitFailedError|null} null while the operation still owns membership
   */
  refusal(token) {
    const state = this.states.get(token)
    const cause = state?.cause || this.permanent
    if (cause) {
      return new KafkaJSCommitFailedError('The consumer no longer owns this membership', {
        cause,
        reason:
          cause.type === 'POLL_INTERVAL_EXCEEDED' ? 'poll-interval-exceeded' : 'membership-lost',
      })
    }
    if (state?.withheld) {
      return new KafkaJSCommitFailedError(
        'The consumer refused the assignment this work began under',
        { cause: state.withheld, reason: ASSIGNMENT_REFUSED }
      )
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
