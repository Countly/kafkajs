const caches = new WeakMap()

/**
 * Publication identities for one group's committed cache, independent of ledger retention.
 * Only cells with a private reset position need an identity. Publishing or clearing removes
 * that identity; the next private position receives a fresh one, even for the same offset.
 */
class CommittedOffsetRevisions {
  /** @returns {CommittedOffsetRevisions} empty revision state. */
  constructor() {
    this.topics = new Map()
  }

  /**
   * Returns the shared revision source for a cache, without extending its lifetime.
   * @param {object} cache group's committed offsets
   * @returns {CommittedOffsetRevisions} shared source
   */
  static forCache(cache) {
    let revisions = caches.get(cache)
    if (!revisions) {
      revisions = new CommittedOffsetRevisions()
      caches.set(cache, revisions)
    }
    return revisions
  }

  /**
   * Captures a cell's current publication identity, including an absent shared cell.
   * @param {string} topic topic name
   * @param {number|string} partition partition id
   * @returns {symbol} identity, stable until publication or invalidation
   */
  capture(topic, partition) {
    let cells = this.topics.get(topic)
    if (!cells) this.topics.set(topic, (cells = new Map()))
    const key = String(partition)
    if (!cells.has(key)) cells.set(key, Symbol('committed offset revision'))
    return cells.get(key)
  }

  /**
   * Reads a revision without allocating on the fetch path.
   * @param {string} topic topic name
   * @param {number|string} partition partition id
   * @returns {symbol|undefined} current identity
   */
  current(topic, partition) {
    return this.topics.get(topic)?.get(String(partition))
  }

  /**
   * Advances a cell's revision on every accepted publication or clear, even at the same value.
   * Removing the old identity prevents reactivation and bounds state by tracked cells.
   * @param {string} topic topic name
   * @param {number|string} partition partition id
   * @returns {void}
   */
  advance(topic, partition) {
    const cells = this.topics.get(topic)
    cells?.delete(String(partition))
    if (cells?.size === 0) this.topics.delete(topic)
  }

  /**
   * Invalidates all cells when a topic's shared cache is cleared.
   * @param {string} topic topic name
   * @returns {void}
   */
  clearTopic(topic) {
    this.topics.delete(topic)
  }
}

module.exports = CommittedOffsetRevisions
