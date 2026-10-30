const arrayDiff = require('../utils/arrayDiff')
const Generation = require('../producer/generation')

const MIN_READ_INTERVAL_MS = 1000
const READ_BUDGET_MS = 5000
const RETIRED = new Error('The leader union watch was cleared')
const LEADERSHIP_LOST = new Error('The member is no longer the group leader')

/**
 * @param {Map<string, number[]>} snapshot the partition ids the leader assigned from
 * @param {Array<{ topic: string, topicErrorCode: number, partitionMetadata: Array<{ partitionId: number }> }>} topicMetadata
 * @returns {{ topic: string, unknownPartitions: number[] }|null} the first topic that gained
 *   partitions; a topic the broker answered with an error says nothing
 */
const driftIn = (snapshot, topicMetadata) => {
  for (const { topic, topicErrorCode, partitionMetadata } of topicMetadata) {
    const known = snapshot.get(topic)
    if (!known || topicErrorCode) continue
    const unknownPartitions = arrayDiff(
      partitionMetadata.map((row) => row.partitionId),
      known
    )
    if (unknownPartitions.length) return { topic, unknownPartitions }
  }
  return null
}

/**
 * Watches the topics only the group's other members subscribe to, from the leader's side.
 *
 * The classic leader unions every member's subscription while it assigns, but its own metadata
 * refresh and stale-assignment check cover only its own subscription, so a partition added later
 * to a follower-only topic would never start a round. The watch keeps the partition sets the leader
 * assigned from and rereads those topics, at most once per `metadataMaxAge`, with a bounded
 * non-installing Metadata request. A gained partition is latched as `drift`, which the group turns
 * into its ordinary stale-assignment rejoin.
 *
 * It never adds a topic to the cluster's targets and never creates one, and an armed watch costs
 * the fetch path one property read. Reads are single-flight. Each watch lifetime, from one `arm` or
 * `clear` to the next, owns its reads: a read refuses to transmit, and publishes nothing, once its
 * lifetime ended, its run was closed or the member stopped leading, and a caller waiting on a
 * retired read is released at once.
 */
class LeaderUnionWatch {
  /**
   * @param {object} options
   * @param {import('../cluster')} options.cluster
   * @param {import('../../types').Logger} options.logger
   * @param {number} options.metadataMaxAge pacing of the reads, in milliseconds, floored at one second
   * @param {() => boolean} [options.isLeader] whether the member still leads; defaults to always
   */
  constructor({ cluster, logger, metadataMaxAge, isLeader = () => true }) {
    this.cluster = cluster
    this.logger = logger
    this.isLeader = isLeader
    this.intervalMs = Math.max(metadataMaxAge || 0, MIN_READ_INTERVAL_MS)
    /** @type {Map<string, number[]>|null} partition ids per topic at assignment; null when idle */
    this.snapshot = null
    /** @type {{ topic: string, unknownPartitions: number[] }|null} the latched drift */
    this.drift = null
    this.nextReadAt = 0
    /** One watch lifetime; closing it retires every read that began under it. */
    this.lifetime = new Generation()
    /** @type {Promise<void>|null} */
    this.reading = null
  }

  /**
   * Builds the snapshot the leader assigned from.
   * @param {string[]} unionOnly topic names no one but the other members subscribe to
   * @param {Map<string, Array<{ partitionId: number }>>} metadata partition rows per topic; a
   *   topic with no row, such as one that did not exist yet, starts with no partitions
   * @returns {Map<string, number[]>|null} null when there is nothing to watch
   */
  static snapshotOf(unionOnly, metadata) {
    if (!unionOnly.length) return null
    return new Map(
      unionOnly.map((topic) => [topic, (metadata.get(topic) ?? []).map((row) => row.partitionId)])
    )
  }

  /** @returns {boolean} whether a snapshot is being watched */
  get armed() {
    return this.snapshot !== null
  }

  /**
   * Starts watching from a new assignment, or stops when there is no snapshot. Replaces whatever
   * was watched and drops any read still in flight.
   * @param {Map<string, number[]>|null|undefined} snapshot from {@link LeaderUnionWatch.snapshotOf}
   * @returns {void}
   */
  arm(snapshot) {
    this.clear()
    if (!snapshot) return
    this.snapshot = snapshot
    this.nextReadAt = performance.now() + this.intervalMs
  }

  /**
   * Stops watching and forgets the snapshot, the latched drift and any read in flight. The old
   * lifetime closes, so its reads refuse to transmit and every caller waiting on one is released.
   * @returns {void}
   */
  clear() {
    this.lifetime.close(RETIRED)
    this.lifetime = new Generation()
    this.snapshot = null
    this.drift = null
    this.reading = null
  }

  /**
   * Rereads the watched topics when the pacing interval has passed. Concurrent callers share one
   * read. A failed read is logged and retried after the next interval; it never fails the caller
   * except by the fence closing. A caller whose read was retired by `clear` returns at once,
   * without waiting for the read to end.
   * @param {import('../producer/generation')} fence the run that owns the call
   * @returns {Promise<void>}
   * @throws {Error} the fence's close error
   */
  async poll(fence) {
    if (!this.snapshot || this.drift) return
    const { lifetime } = this
    if (!this.reading) {
      if (performance.now() < this.nextReadAt) return
      this.reading = this.read(fence, lifetime).finally(() => {
        if (this.lifetime === lifetime) this.reading = null
      })
    }
    try {
      await fence.wait(lifetime.wait(this.reading))
    } catch (error) {
      if (!lifetime.closed) throw error
      fence.check()
    }
  }

  /**
   * One bounded read. Never rejects.
   * @private
   * @param {import('../producer/generation')} fence
   * @param {Generation} lifetime the watch lifetime the read belongs to
   * @returns {Promise<void>}
   */
  async read(fence, lifetime) {
    const { snapshot } = this
    const startedAt = performance.now()
    const guard = () =>
      fence.guard() || lifetime.guard() || (this.isLeader() ? null : LEADERSHIP_LOST)
    try {
      const { topicMetadata } = await this.cluster.groupMetadata({
        topics: [...snapshot.keys()],
        allowAutoTopicCreation: false,
        deadlineAt: startedAt + READ_BUDGET_MS,
        guard,
      })
      if (!guard()) this.drift = driftIn(snapshot, topicMetadata)
    } catch (error) {
      if (!lifetime.closed && !fence.closed) {
        this.logger.debug('Leader union metadata read failed, retrying later', {
          error: error.message,
        })
      }
    } finally {
      if (!lifetime.closed) this.nextReadAt = performance.now() + this.intervalMs
    }
  }
}

module.exports = LeaderUnionWatch
