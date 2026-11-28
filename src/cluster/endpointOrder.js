/**
 * Orders what a broker pool learns about where its nodes are, so the broker table only ever moves a
 * node to the newest thing learned about it.
 *
 * Every observation of a node's endpoint takes a stamp from one clock: a published Metadata
 * response, a leader hint as it is applied, a FindCoordinator answer as its request is sent. A
 * node's endpoint is written only by its newest observation, and a write that awaited a pool build
 * is installed only if no newer observation of that node arrived meanwhile. A Metadata response
 * names every node of the cluster, so it supersedes every earlier observation at once, including
 * those of nodes it no longer lists; a cleared broker table does the same.
 */
class EndpointOrder {
  constructor() {
    this.clock = 0
    /** The newest observation of every node not observed alone since. */
    this.floor = 0
    /** @type {Map<string, number>} nodes observed alone since `floor`, by node id */
    this.stamps = new Map()
  }

  /**
   * @returns {number} a stamp later than every stamp issued before
   */
  next() {
    return ++this.clock
  }

  /**
   * Records an observation of every node, which supersedes every earlier one.
   * @returns {number} its stamp
   */
  supersedeAll() {
    this.stamps.clear()
    this.floor = this.next()
    return this.floor
  }

  /**
   * Makes `stamp` the newest observation of the node, unless a newer one was recorded.
   * @param {number|string} nodeId
   * @param {number} stamp
   * @returns {boolean} whether `stamp` is now the node's newest observation
   */
  claim(nodeId, stamp) {
    const key = String(nodeId)
    const newest = this.stamps.get(key) ?? this.floor
    if (stamp < newest) return false
    if (stamp > newest) this.stamps.set(key, stamp)
    return true
  }

  /**
   * @param {number|string} nodeId
   * @param {number} stamp
   * @returns {boolean} whether `stamp` is still the node's newest observation
   */
  owns(nodeId, stamp) {
    return (this.stamps.get(String(nodeId)) ?? this.floor) === stamp
  }
}

module.exports = EndpointOrder
