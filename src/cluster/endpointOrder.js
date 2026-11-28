/**
 * Orders the intended route of each node, independently of the pools currently installed. Every
 * write of a node's address into the broker table is made by the holder of the node's newest claim
 * (`BrokerPool`'s one writer); an older observation never replaces a newer one, whichever finishes
 * first.
 *
 * Source             | Claim
 * ------------------ | ------------------------------------------------------------------------
 * Metadata response  | At publication, with the response's stamp, for a changed or newly listed
 *                    | row, and for a repeated row whose node a leader hint, or a coordinator
 *                    | answer no lookup still names, moved elsewhere: the newer response wins, as
 *                    | the Java client replaces hinted nodes. Any other repeated row keeps its
 *                    | stamp, so unchanged responses never overtake a newer answer. Its rack is
 *                    | written whenever the installed and the intended address are the row's.
 * Leader hint        | One stamp for the whole set, taken before its first pool build; every row
 *                    | claims at once. The set is committed with the partition's route in one
 *                    | synchronous step, or refused whole.
 * Coordinator answer | The stamp taken when its request was written. A route it moved is owned by
 *                    | the lookups whose key it answers; repeated responses leave such a route.
 * Seed adoption      | Made by the row that claims the node. The seed is held under at most one
 *                    | node id: another id at its address gets its own pool.
 *
 * Lifecycle event    | Effect
 * ------------------ | ------------------------------------------------------------------------
 * Disconnect         | Raises the floor above every stamp issued and forgets every route.
 * Rebootstrap        | The same, once the new seed is installed.
 * Node removal       | A failure removal, or a response that no longer lists the node, claims a
 *                    | removal; a response keeps a removal already claimed. Once applied, by the
 *                    | next response for a failure removal, the node's route is released and the
 *                    | removal's stamp raises the floor for nodes without a route, so an older
 *                    | answer for that node still loses. A coordinator answer still installing,
 *                    | or a route one moved that a lookup key still names, stays while responses
 *                    | omit the node; a hinted node does not.
 *
 * A writer that stops without installing withdraws its claim: the node falls back to the newest
 * claim not withdrawn, whose writer, or the next response, completes it. Claims of responses are
 * never withdrawn; a later response takes them over. An installed claim drops what it superseded.
 */
class EndpointOrder {
  /** Creates an empty routing generation. */
  constructor() {
    this.clock = 0
    this.floor = 0
    /** The newest released removal: a node without a route is not claimed at or below it. */
    this.absentFloor = 0
    /** @type {Map<string, Claim>} each node's newest claim not withdrawn */
    this.routes = new Map()
  }

  /**
   * @returns {number} a stamp later than every stamp issued before
   */
  next() {
    return ++this.clock
  }

  /**
   * Forgets every route and fences every earlier stamp.
   * @returns {number} the new floor
   */
  supersedeAll() {
    this.routes.clear()
    this.floor = this.next()
    return this.floor
  }

  /**
   * @param {number} stamp
   * @returns {boolean} whether the observation belongs to the current lifecycle
   */
  isCurrent(stamp) {
    return stamp > this.floor
  }

  /**
   * Records the node's intended address, or its removal (`null`), unless a newer observation owns
   * the node. A claim at the node's current stamp and address is that same claim. A Metadata claim
   * records Metadata as the source; any other claim that keeps the current address keeps its
   * source, so an answer that agrees with Metadata changes no precedence.
   * @param {number|string} nodeId
   * @param {number} stamp
   * @param {string|null} address `host:port`, or null for a removal
   * @param {'metadata'|'hint'|'coordinator'|'removal'} source
   * @returns {Claim|null} the node's newest claim, or null when a newer one owns the node
   */
  claim(nodeId, stamp, address, source) {
    if (!this.isCurrent(stamp)) return null
    const key = String(nodeId)
    const route = this.routes.get(key)
    if (!route && stamp <= this.absentFloor) return null
    if (route && stamp <= route.stamp) {
      return stamp === route.stamp && address === route.address ? route : null
    }
    const claim = {
      key,
      stamp,
      address,
      source: source !== 'metadata' && route?.address === address ? route.source : source,
      settled: false,
      withdrawn: false,
      previous: route,
    }
    this.routes.set(key, claim)
    return claim
  }

  /**
   * @param {Claim|null|undefined} claim
   * @returns {boolean} whether the claim may still be installed: it is its node's newest, in the
   *   current lifecycle, and nothing installed it yet
   */
  owns(claim) {
    return (
      Boolean(claim) &&
      !claim.settled &&
      this.isCurrent(claim.stamp) &&
      this.routes.get(claim.key) === claim
    )
  }

  /**
   * @param {Claim} claim
   * @returns {boolean} whether the claim, installed or not, is still its node's newest
   */
  isNewest(claim) {
    return this.isCurrent(claim.stamp) && this.routes.get(claim.key) === claim
  }

  /**
   * @param {number|string} nodeId
   * @returns {Claim|undefined} the node's newest claim in the current lifecycle
   */
  route(nodeId) {
    return this.routes.get(String(nodeId))
  }

  /**
   * @param {number|string} nodeId
   * @param {string} address `host:port`
   * @returns {boolean} whether the node's newest claim names this address
   */
  intends(nodeId, address) {
    return this.route(nodeId)?.address === address
  }

  /**
   * Marks an installed claim; what it superseded can no longer come back.
   * @param {Claim} claim
   * @returns {void}
   */
  settle(claim) {
    claim.settled = true
    claim.previous = undefined
  }

  /**
   * Gives up a claim that installed nothing. When it is still the node's newest, the node falls
   * back to the newest claim before it that was not withdrawn, or to no route.
   * @param {Claim|null|undefined} claim
   * @returns {boolean} whether the node's route changed
   */
  withdraw(claim) {
    if (!claim || claim.settled || claim.withdrawn) return false
    claim.withdrawn = true
    if (this.routes.get(claim.key) !== claim) return false
    let previous = claim.previous
    while (previous?.withdrawn) previous = previous.previous
    if (previous) this.routes.set(claim.key, previous)
    else this.routes.delete(claim.key)
    return true
  }

  /**
   * Forgets the node of an applied removal; its stamp keeps older observations of the node out.
   * @param {Claim} removal the node's newest claim, a removal
   * @returns {void}
   */
  release(removal) {
    this.settle(removal)
    if (this.routes.get(removal.key) === removal) this.routes.delete(removal.key)
    if (removal.stamp > this.absentFloor) this.absentFloor = removal.stamp
  }
}

/**
 * @typedef {object} Claim
 * @property {string} key the node id
 * @property {number} stamp when the observation was made
 * @property {string|null} address `host:port`, or null for a removal
 * @property {'metadata'|'hint'|'coordinator'|'removal'} source who last changed the address
 * @property {boolean} settled installed or applied
 * @property {boolean} withdrawn given up without installing
 * @property {Claim|undefined} previous the claim it superseded, until it settles
 */

module.exports = EndpointOrder
