const caches = new WeakMap()

/**
 * Publication revisions for one group's committed cache, independent of ledger retention.
 * It keeps one number per partition ever published or cleared and per topic ever cleared, never
 * per private reset position, so it grows with the group's topology, as the cache's topic keys do.
 * Capturing the cache sequence allocates nothing; later publications or clears supersede it.
 */
class CommittedOffsetRevisions {
  /** @returns {CommittedOffsetRevisions} empty revision state. */
  constructor() {
    this.seq = 0
    this.cells = new Map()
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
   * Captures the cache sequence without retaining any private position or cell.
   * @returns {number} sequence at capture time
   */
  capture() {
    return this.seq
  }

  /**
   * Checks whether a cell has been published or cleared since capture, without allocating.
   * @param {string} topic topic name
   * @param {number|string} partition partition id
   * @param {number} captured sequence at capture time
   * @returns {boolean} whether the private position remains current
   */
  isCurrent(topic, partition, captured) {
    const cell = this.cells.get(topic)?.get(String(partition))
    const cleared = this.topics.get(topic)
    return (
      (cell === undefined || cell <= captured) && (cleared === undefined || cleared <= captured)
    )
  }

  /**
   * Advances a cell's revision on every accepted publication or clear, even at the same value.
   * @param {string} topic topic name
   * @param {number|string} partition partition id
   * @returns {void}
   */
  advance(topic, partition) {
    let cells = this.cells.get(topic)
    if (!cells) this.cells.set(topic, (cells = new Map()))
    cells.set(String(partition), ++this.seq)
  }

  /**
   * Invalidates a topic's cells and replaces their revisions with one topic stamp.
   * @param {string} topic topic name
   * @returns {void}
   */
  clearTopic(topic) {
    this.topics.set(topic, ++this.seq)
    this.cells.delete(topic)
  }
}

module.exports = CommittedOffsetRevisions
