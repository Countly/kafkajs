/**
 * Orders intended per-node routes independently of the pools currently installed.
 *
 * Source             | Ordering
 * ------------------ | ------------------------------------------------------------------------
 * Metadata response  | At publication, a changed host/port or newly listed ID claims that node.
 *                    | A repeated address keeps its original stamp, including during a build.
 * Leader hint        | One stamp when admitted, shared by all rows. The first usable row per ID wins.
 * Coordinator answer | The physical request's pre-write stamp. Each attempt has its own stamp.
 * Seed adoption      | Inherits the claiming row's stamp. A seed pool acquires at most one node ID.
 *
 * Lifecycle event    | Ordering
 * ------------------ | ------------------------------------------------------------------------
 * Disconnect         | Raises the floor for all nodes and drops routing observations.
 * Rebootstrap        | Raises the floor when the new seed commits and drops routing observations.
 * Node removal       | A failure of the current pool claims a tombstone. Metadata omission claims
 *                    | one when a listed address disappears and still owns the route by address.
 *                    | A repeated omission leaves its stamp alone. An independently learned
 *                    | different address survives omission of the old advertised address.
 *
 * The greater stamp wins for that node, including while a build or removal is pending.
 * An older answer can reuse a matching installed pool only if it matches current intent and
 * belongs to the same lifecycle. All other refusals can use the existing bounded retry.
 */
class EndpointOrder {
  /** Creates an empty routing generation. */
  constructor() {
    this.clock = 0
    this.floor = 0
    /** @type {Map<string, {stamp: number, address: string|null}>} */
    this.routes = new Map()
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
   * Records the node's desired address or removal, unless a newer observation already owns it.
   * @param {number|string} nodeId
   * @param {number} stamp
   * @param {string|null} address
   * @returns {boolean} whether `stamp` is now the node's newest observation
   */
  claim(nodeId, stamp, address) {
    if (!this.isCurrent(stamp)) return false
    const key = String(nodeId)
    const route = this.routes.get(key)
    if (route && stamp <= route.stamp) return stamp === route.stamp && address === route.address
    this.routes.set(key, { stamp, address })
    return true
  }

  /**
   * @param {number|string} nodeId
   * @param {number} stamp
   * @returns {boolean} whether `stamp` is still the node's newest observation
   */
  owns(nodeId, stamp) {
    return this.routes.get(String(nodeId))?.stamp === stamp
  }
}

module.exports = EndpointOrder
