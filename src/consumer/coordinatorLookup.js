const Generation = require('../producer/generation')

/**
 * @typedef {object} PendingLookup
 * @property {Generation} fence ends the lookup's retries and dials; closed when the membership
 *   retires it or the last caller gives up
 * @property {Generation} unfenced stands in for the callers that bring no fence of their own;
 *   replaced whenever it is closed
 * @property {number} waiters callers still waiting that can keep it alive, unfenced ones included
 * @property {Promise<import('../../types').Broker>} promise
 */

/**
 * The one pending group-coordinator lookup of a membership. The heartbeat, the joins and every
 * commit that need the coordinator while it is being looked up wait for the same request instead
 * of starting their own.
 *
 * The lookup belongs to the membership, not to the caller that started it: it runs under its own
 * fence, and each caller waits under its own fence and deadline. A caller that gives up or closes
 * leaves the others waiting; only when the last caller has gone is the lookup itself cancelled, so
 * a closed caller never causes a dial.
 */
class CoordinatorLookup {
  /** @type {PendingLookup|null} */
  #pending = null
  #cluster
  #groupId

  /**
   * @param {object} options
   * @param {import('../../types').Cluster} options.cluster
   * @param {string} options.groupId
   */
  constructor({ cluster, groupId }) {
    this.#cluster = cluster
    this.#groupId = groupId
  }

  /**
   * Waits for the membership's pending lookup, starting it when none is pending.
   * @param {import('../producer/generation')|import('./groupProtocol/interface').ConsumerOperation} [fence]
   *   the caller's own fence and deadline: it ends this caller's wait and, when no other caller is
   *   left, the lookup. Absent, the caller waits for the answer.
   * @returns {Promise<import('../../types').Broker>} the coordinator
   * @throws {Error} the lookup's failure, or the caller's own refusal
   */
  find(fence) {
    const refusal = fence?.guard()
    if (refusal) return Promise.reject(refusal)

    const lookup = this.#start()
    lookup.waiters += 1
    const own = fence ?? lookup.unfenced

    return new Promise((resolve, reject) => {
      let unregister = () => {}
      unregister = own.register((error) => {
        lookup.waiters -= 1
        if (lookup.waiters === 0) this.#abandon(lookup, error)
        reject(error)
      })
      lookup.promise.then(
        (coordinator) => {
          unregister()
          const closed = own.guard()
          if (closed) reject(closed)
          else resolve(coordinator)
        },
        (error) => {
          unregister()
          reject(error)
        }
      )
    })
  }

  /**
   * Ends the pending lookup's ownership by its callers' membership without cancelling it: its
   * waiters still get the answer, and the next caller starts a lookup of its own.
   * @returns {void}
   */
  detach() {
    this.#pending = null
  }

  /**
   * Cancels the pending lookup for everyone: its retries and dials end and its waiters fail with
   * `error`. The next caller starts a new lookup. For a forced close; a graceful stop uses
   * `releaseUnfenced`.
   * @param {Error} error why the membership can no longer wait for the coordinator
   * @returns {void}
   */
  retire(error) {
    const lookup = this.#pending
    this.#pending = null
    lookup?.fence.close(error)
  }

  /**
   * Fails the callers that brought no fence of their own (the heartbeat), leaving the fenced ones
   * to finish under their own fence and deadline. The lookup ends with the last caller, so one
   * answered after that installs nothing. A later unfenced caller can still join.
   * @param {Error} error why those callers stop waiting
   * @returns {void}
   */
  releaseUnfenced(error) {
    const lookup = this.#pending
    if (!lookup) return
    const released = lookup.unfenced
    lookup.unfenced = new Generation()
    released.close(error)
  }

  /**
   * @private
   * @param {PendingLookup} lookup the lookup whose last caller left
   * @param {Error} error why it left
   * @returns {void}
   */
  #abandon(lookup, error) {
    if (this.#pending === lookup) this.#pending = null
    lookup.fence.close(error)
  }

  /**
   * @private
   * @returns {PendingLookup} the pending lookup, started now when there is none
   */
  #start() {
    if (this.#pending) return this.#pending
    const fence = new Generation()
    /** @type {PendingLookup} */
    const lookup = { fence, unfenced: new Generation(), waiters: 0, promise: null }
    lookup.promise = Promise.resolve(
      this.#cluster.findGroupCoordinator({ groupId: this.#groupId, fence })
    ).finally(() => {
      if (this.#pending === lookup) this.#pending = null
    })
    lookup.promise.catch(() => {})
    this.#pending = lookup
    return lookup
  }
}

module.exports = CoordinatorLookup
