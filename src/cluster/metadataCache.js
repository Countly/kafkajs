const { CLUSTER_TOPIC_RECREATED } = require('./instrumentationEvents')

const UNKNOWN_EPOCH = -1
const NO_LEADER_ID = -1
const NO_TOPIC_ID = '00000000-0000-0000-0000-000000000000'

/**
 * Owner of the client's topic ids and per-partition leader epochs.
 *
 * A Metadata response is the only source of truth for both, and this cache is the only writer.
 * Two things make it more than a map:
 *
 * 1. **Write-through.** Whenever an accepted `leader`/`leaderEpoch` pair differs from what a
 *    decoded response carries, the pair is written back onto that response's own partition
 *    object. The cluster's routing (`findTopicPartitionMetadata`, `findLeaderForPartitions`,
 *    `fetchTopicsOffset` and the consumer's read-replica selection) all read those objects, so a
 *    leader hint becomes visible to every route the instant it is accepted, with no reader change
 *    and nothing allocated on the produce or fetch hot path.
 * 2. **Monotonic epochs, bounded by incarnation.** A leader epoch only moves forward within one
 *    topic incarnation and one cluster generation. A topic recreated under a new id, and a
 *    cluster replaced under a rebootstrap, both restart at epoch 0 - so an id change drops that
 *    topic's epochs and `clear()` drops everything, and the comparison only runs when both sides
 *    actually know their epoch.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
 * @see clients/src/main/resources/common/message/MetadataResponse.json
 */
module.exports = class MetadataCache {
  /**
   * @param {object} options
   * @param {import('../../types').Logger} options.logger
   * @param {import('../instrumentation/emitter')|null} [options.instrumentationEmitter]
   */
  constructor({ logger, instrumentationEmitter = null }) {
    this.logger = logger.namespace('MetadataCache')
    this.instrumentationEmitter = instrumentationEmitter

    /**
     * Per topic: its id, and per partition the accepted leader/epoch pair plus `decoded` - a
     * reference to that partition's object inside the *current* `brokerPool.metadata`. `decoded`
     * is what makes the write-through work: an accepted leader hint writes onto it, and every
     * cluster route reads it. It is re-seated on every refresh **for topics present in that
     * refresh's response** (both the accept and the defend branch), because `brokerPool.metadata`
     * replaces the objects for those topics each time; a topic absent from a scoped refresh (e.g.
     * `Cluster#metadata({ topics: [...] })`) keeps its previous `decoded` reference untouched, by
     * the same rule that leaves its epoch and id untouched (see `update()`).
     *
     * Entries are mutated in place and reused across refreshes - only a first sighting, a topic
     * recreation or `clear()` allocates.
     *
     * @type {Map<string, { topicId: string|null, partitions: Map<number, { leader: number, leaderEpoch: number, decoded: object }> }>}
     */
    this.topics = new Map()
    /** @type {Map<string, string>} topic id -> topic name */
    this.topicIds = new Map()
  }

  /**
   * Applies a decoded Metadata response, in three ordered steps, and writes accepted
   * leader/leaderEpoch pairs back onto the response's own partition objects.
   *
   * Topics absent from the response are left untouched: a Metadata response is scoped to the
   * topics it was asked for, so absence is never evidence of deletion or recreation.
   *
   * Allocation contract: one `Set` of live broker ids per call, and one entry per topic and per
   * partition only on its first sighting (or after a recreation or a `clear()`). A steady-state
   * refresh mutates the existing entries - this runs on every admin call, since the admin client
   * has no `metadataMaxAge`.
   *
   * @param {{ brokers: Array<{ nodeId: number }>, topicMetadata: Array<{ topic: string, topicId?: string|null, partitionMetadata: Array<{ partitionId: number, leader: number, leaderEpoch: number }> }> }} metadata decoded response, mutated in place
   * @returns {void}
   */
  update(metadata) {
    const liveBrokerIds = new Set()
    for (const { nodeId } of metadata.brokers) {
      liveBrokerIds.add(nodeId)
    }

    for (const topicMetadata of metadata.topicMetadata) {
      const name = topicMetadata.topic
      let entry = this.topics.get(name)

      // Step 1 - topic-id diff, before any epoch comparison. A recreation drops this topic's
      // partition entries entirely, so the new incarnation's epoch 0 is accepted.
      const recreated = this.diffTopicId(name, entry, topicMetadata.topicId)

      if (!entry || recreated) {
        entry = { topicId: null, partitions: new Map() }
        this.topics.set(name, entry)
      }

      entry.topicId = topicMetadata.topicId ?? null

      for (const partition of topicMetadata.partitionMetadata) {
        const cached = entry.partitions.get(partition.partitionId)

        if (!cached) {
          entry.partitions.set(partition.partitionId, {
            leader: partition.leader,
            leaderEpoch: partition.leaderEpoch,
            decoded: partition,
          })
          continue
        }

        // Step 2 - never defend a leader the cluster no longer advertises.
        // Step 3 - compare only when both sides know their epoch.
        const defends =
          liveBrokerIds.has(cached.leader) &&
          cached.leaderEpoch > UNKNOWN_EPOCH &&
          partition.leaderEpoch > UNKNOWN_EPOCH &&
          partition.leaderEpoch < cached.leaderEpoch

        if (defends) {
          this.logger.debug('Ignoring stale metadata for partition, keeping the cached leader', {
            topic: name,
            partition: partition.partitionId,
            cachedLeaderEpoch: cached.leaderEpoch,
            decodedLeaderEpoch: partition.leaderEpoch,
          })
          partition.leader = cached.leader
          partition.leaderEpoch = cached.leaderEpoch
        } else {
          cached.leader = partition.leader
          cached.leaderEpoch = partition.leaderEpoch
        }

        // Re-seated in both branches: the previous response's object is about to be discarded.
        cached.decoded = partition
      }

      if (topicMetadata.topicId && topicMetadata.topicId !== NO_TOPIC_ID) {
        this.topicIds.set(topicMetadata.topicId, name)
      }
    }
  }

  /**
   * @private
   * @param {string} name
   * @param {{ topicId: string|null }|undefined} entry
   * @param {string|null|undefined} topicId the id the fresh response carries
   * @returns {boolean} true when this is a recreation (a known name, a different non-zero id)
   */
  diffTopicId(name, entry, topicId) {
    const previousTopicId = entry && entry.topicId
    const isRecreation =
      previousTopicId != null &&
      topicId != null &&
      topicId !== NO_TOPIC_ID &&
      topicId !== previousTopicId

    if (!isRecreation) {
      return false
    }

    this.topicIds.delete(previousTopicId)
    this.logger.debug('Topic has been recreated under a new id', {
      topic: name,
      previousTopicId,
      topicId,
    })
    this.emit(CLUSTER_TOPIC_RECREATED, { topic: name, previousTopicId, topicId })
    return true
  }

  /**
   * Applies a leader hint the broker attached to a produce error (KIP-951). Synchronous, and the
   * write-through onto the decoded partition object makes it visible to every cluster route
   * immediately - `findLeaderForPartitions`, `fetchTopicsOffset` and the consumer's read-replica
   * selection all read that object.
   *
   * @param {string} topic
   * @param {number} partition
   * @param {{ leaderId: number, leaderEpoch: number }} hint
   * @returns {boolean} true when the hint advanced the epoch and was applied
   */
  applyLeaderHint(topic, partition, { leaderId, leaderEpoch }) {
    // ProduceResponse's CurrentLeader defaults to { LeaderId: -1, LeaderEpoch: -1 } when the
    // broker has no hint to offer - never a real leader, regardless of the cached epoch.
    if (leaderId === NO_LEADER_ID) {
      return false
    }

    const entry = this.topics.get(topic)
    const cached = entry && entry.partitions.get(partition)

    if (!cached) {
      return false
    }

    if (cached.leaderEpoch > UNKNOWN_EPOCH && leaderEpoch <= cached.leaderEpoch) {
      return false
    }

    cached.leader = leaderId
    cached.leaderEpoch = leaderEpoch
    cached.decoded.leader = leaderId
    cached.decoded.leaderEpoch = leaderEpoch
    return true
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {number} the current leader epoch, or -1 when unknown
   */
  leaderEpoch(topic, partition) {
    const entry = this.topics.get(topic)
    const cached = entry && entry.partitions.get(partition)
    return cached && cached.leaderEpoch != null ? cached.leaderEpoch : UNKNOWN_EPOCH
  }

  /** @param {string} name @returns {string|undefined} */
  topicId(name) {
    const entry = this.topics.get(name)
    return entry && entry.topicId != null ? entry.topicId : undefined
  }

  /** @param {string} topicId @returns {string|undefined} */
  topicName(topicId) {
    return this.topicIds.get(topicId)
  }

  /**
   * Forgets everything. Called on a rebootstrap: after one, the client has no basis for
   * believing any cached epoch, and a retained epoch would make it reject a fresh cluster's
   * epoch-0 metadata and write dead node ids into the routing structure.
   * @returns {void}
   */
  clear() {
    this.topics.clear()
    this.topicIds.clear()
  }

  /**
   * @private
   * @param {string} eventName
   * @param {object} payload
   */
  emit(eventName, payload) {
    this.instrumentationEmitter && this.instrumentationEmitter.emit(eventName, payload)
  }
}
