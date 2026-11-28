const { KafkaJSCommitFailedError } = require('../errors')
const Generation = require('../producer/generation')

const ASSIGNMENT_REFUSED = 'assignment-refused'

/** @returns {{cause: Error|null, withheld: Error|null, holders: number}} a fresh identity's state */
const freshState = () => ({ cause: null, withheld: null, holders: 0 })

/**
 * The one owner of a member's ownership authority. Work started under it may change or publish
 * state only while that authority is still current, checked when it publishes:
 *
 * - membership identities (tokens) fence commits by the membership they began under. Only the
 *   current one and those retained by outstanding operations are stored; renewing an idle member
 *   retains no history. An identity is current, withheld (its assignment was refused but the
 *   membership is kept) or lost; a loss always wins over a withhold;
 * - the fetch authority (`authority`) fences every Fetch and the refreshes, hints and discoveries
 *   it starts. At most one is open; it closes with its run or at a withhold, and only the next
 *   usable install (`renew`) opens another, so an ordinary install resets no Fetch session.
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
    /** @type {Generation|null} the terminal fence of the run the fetch authority closes with */
    this.runFence = null
    this.releaseAuthority = () => {}
    this.openAuthority()
  }

  /**
   * Opens the fetch authority of a new run; it closes with `fence`. The previous run's authority
   * closes with its own run.
   * @param {Generation} fence the run's terminal fence
   * @returns {void}
   */
  startRun(fence) {
    this.runFence = fence
    this.openAuthority()
  }

  /**
   * Opens a fresh fetch authority with one registration on the run fence.
   * @private
   * @returns {void}
   */
  openAuthority() {
    const authority = new Generation()
    /** @type {Generation} the fence every Fetch captures at entry */
    this.authority = authority
    this.releaseAuthority = this.runFence?.register((error) => authority.close(error)) ?? (() => {})
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
   * change, so answers stamped with it stay current, and a later `lose` still wins. The fetch
   * authority closes with `cause` in the same step.
   * @param {Error} cause the refusal
   * @returns {void}
   */
  withhold(cause) {
    const state = this.states.get(this.current)
    state.withheld ??= cause
    this.releaseAuthority()
    this.authority.close(cause)
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
   * A usable install: opens a new fetch authority when the current one is closed, and starts a
   * replacement membership unless this run was abandoned.
   * @returns {number} the identity the replacement uses
   */
  renew() {
    if (this.authority.closed) this.openAuthority()
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
   * Whether work begun under `token` may still report what it did. False once the run was
   * abandoned or once that identity's assignment was withheld, also when a loss followed the
   * withhold: the history lasts as long as an operation retains the token. A loss alone keeps it
   * true.
   * @param {number} token an operation's identity, retained by that operation
   * @returns {boolean}
   */
  publishes(token) {
    return !this.permanent && !this.states.get(token)?.withheld
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
