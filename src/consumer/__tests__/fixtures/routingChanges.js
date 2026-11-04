const Cluster = require('../../../cluster')

/** The most changes one observation keeps; later ones are counted, never stored. */
const MAX_CHANGES = 200
/** The cluster's own subscription, taken before any observation replaces it. */
const SUBSCRIBE = Cluster.prototype.onRoutingChange

/**
 * Observes the routing changes of every consumer cluster that subscribes while it is attached, so
 * a live case can place each accepted routing change in time and read what the cache held right
 * after it. It uses the subscription the consumer's own idle wake uses (`Cluster#onRoutingChange`),
 * and touches no production code.
 *
 * The cache publishes on every accepted Metadata install and also on a leader hint, a by-ID
 * write-through and an invalidation, and the notice carries no cause. A row therefore means "the
 * routing inputs changed here"; a Metadata install is the row whose leaders or epochs differ from
 * the row before it, or the one that repeats them after a stale answer was refused.
 */
class RoutingChanges {
  /**
   * Creates an empty observation on the wire transcript's monotonic clock.
   * @param {Array<{ label: string, topic: string, partition: number }>} partitions the partitions
   *   whose cached leader and epoch each change records
   * @param {() => number} [now] clock, replaceable for offline replay
   */
  constructor(partitions, now = () => performance.now()) {
    this.partitions = partitions
    this.now = now
    /** @type {Array<{ at: number, partitions: object[] }>} oldest first, at most `MAX_CHANGES` */
    this.changes = []
    this.dropped = 0
    /** @type {Map<object, () => void>} the unsubscribe of each watched cluster */
    this.watched = new Map()
    this.spy = null
  }

  /**
   * Starts watching every cluster that subscribes from now on. Pair it with `detach`.
   * @returns {void}
   */
  attach() {
    const observer = this
    this.spy = jest
      .spyOn(Cluster.prototype, 'onRoutingChange')
      .mockImplementation(function onRoutingChange(handler) {
        observer.watch(this)
        return SUBSCRIBE.call(this, handler)
      })
  }

  /**
   * Subscribes once to a cluster's routing changes.
   * @param {import('../../../cluster')} cluster
   * @returns {void}
   */
  watch(cluster) {
    if (this.watched.has(cluster)) return
    this.watched.set(
      cluster,
      SUBSCRIBE.call(cluster, () => this.record(cluster))
    )
  }

  /**
   * Notes one routing change: the time, and for each partition the leader and epoch the cache
   * holds and the leader the cluster routes by, `null` where it knows none.
   * @param {import('../../../cluster')} cluster
   * @returns {void}
   */
  record(cluster) {
    if (this.changes.length >= MAX_CHANGES) {
      this.dropped += 1
      return
    }
    const { metadataCache } = cluster
    this.changes.push({
      at: this.now(),
      partitions: this.partitions.map(({ label, topic, partition }) => ({
        label,
        leader: metadataCache.leader(topic, partition),
        epoch: metadataCache.leaderEpoch(topic, partition),
        routed: this.routedLeader(cluster, topic, partition),
      })),
    })
  }

  /**
   * @param {import('../../../cluster')} cluster
   * @param {string} topic
   * @param {number} partition
   * @returns {number|null} the leader a fetch round would route the partition by, or null when
   *   the cluster has no metadata for it
   */
  routedLeader(cluster, topic, partition) {
    try {
      const row = cluster.findTopicPartitionMetadata(topic).find((p) => p.partitionId === partition)
      return row ? row.leader : null
    } catch {
      return null
    }
  }

  /**
   * @param {number} at `performance.now()` of the earliest change wanted
   * @returns {{ changes: object[], dropped: number }} the changes from `at` on, and how many later
   *   ones the cap discarded
   */
  since(at) {
    return { changes: this.changes.filter((change) => change.at >= at), dropped: this.dropped }
  }

  /**
   * Stops watching and restores the cluster's own subscription method. Idempotent.
   * @returns {void}
   */
  detach() {
    this.spy?.mockRestore()
    this.spy = null
    for (const unsubscribe of this.watched.values()) unsubscribe()
    this.watched.clear()
  }
}

module.exports = { RoutingChanges, MAX_CHANGES }
