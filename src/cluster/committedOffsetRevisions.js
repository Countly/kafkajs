const caches = new WeakMap()

/**
 * Publication history of one group's committed cache, as far as private positions need it.
 *
 * A private position (a reset policy's answer, a sentinel seek, a local seek) captures the stamp of
 * its cell. The cell keeps that one stamp until a publication, a clear or a retirement removes it;
 * a later capture issues a new one. A stamp is never issued twice and a position is current only
 * while its cell still holds the identical stamp, so removing a stamp can only make positions
 * stale, never current again. Only captured, unpublished cells are kept: the size follows the
 * private positions of live topics, never the names the group has seen.
 */
class CommittedOffsetRevisions {
  /** @returns {CommittedOffsetRevisions} empty revision state. */
  constructor() {
    /** Counts revision events; a numeric stamp is a fresh value of it. */
    this.seq = 0
    /** @type {Map<string, Map<string, number|object>>} topic to partition to stamp */
    this.cells = new Map()
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
   * Returns the revision source of a cache only if one exists; never creates one.
   * @param {object} cache group's committed offsets
   * @returns {CommittedOffsetRevisions|undefined}
   */
  static peek(cache) {
    return caches.get(cache)
  }

  /**
   * Counts the retained stamps of a cache without creating a revision source.
   * @param {object} cache group's committed offsets
   * @returns {{topics: number, cells: number}} topics with a stamp and stamped partition cells
   */
  static sizeOf(cache) {
    const revisions = caches.get(cache)
    let cells = 0
    for (const partitions of revisions?.cells.values() ?? []) cells += partitions.size
    return { topics: revisions?.cells.size ?? 0, cells }
  }

  /**
   * Returns the cell's stamp, issuing one if the cell has none. Captures of one cell with no
   * publication between them share the stamp. Past the safe-integer range a stamp is a fresh object,
   * which equals nothing else, so stamps stay unique without ever resetting the counter.
   * @param {string} topic topic name
   * @param {number|string} partition partition id
   * @returns {number|object} the stamp to keep with the private position
   */
  capture(topic, partition) {
    let partitions = this.cells.get(topic)
    if (!partitions) this.cells.set(topic, (partitions = new Map()))
    const key = String(partition)
    let stamp = partitions.get(key)
    if (stamp === undefined) {
      stamp = this.seq < Number.MAX_SAFE_INTEGER ? ++this.seq : {}
      partitions.set(key, stamp)
    }
    return stamp
  }

  /**
   * Checks whether a cell still holds the stamp a private position captured, without allocating.
   * @param {string} topic topic name
   * @param {number|string} partition partition id
   * @param {number|object} stamp what `capture` returned
   * @returns {boolean} whether the private position remains current
   */
  isCurrent(topic, partition, stamp) {
    return stamp !== undefined && this.cells.get(topic)?.get(String(partition)) === stamp
  }

  /**
   * Removes a cell's stamp on every accepted publication or clear, even at the same value.
   * @param {string} topic topic name
   * @param {number|string} partition partition id
   * @returns {void}
   */
  advance(topic, partition) {
    if (this.seq < Number.MAX_SAFE_INTEGER) this.seq++
    const partitions = this.cells.get(topic)
    if (partitions?.delete(String(partition)) && partitions.size === 0) this.cells.delete(topic)
  }

  /**
   * Removes every stamp of a topic, for a recreation, a topic reset or the topic's retirement.
   * @param {string} topic topic name
   * @returns {void}
   */
  clearTopic(topic) {
    if (this.seq < Number.MAX_SAFE_INTEGER) this.seq++
    this.cells.delete(topic)
  }
}

module.exports = CommittedOffsetRevisions
