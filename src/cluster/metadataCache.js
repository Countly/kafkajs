const { CLUSTER_TOPIC_RECREATED } = require('./instrumentationEvents')
const { sharedSetup } = require('../admin/deadline')
const describeThrown = require('../network/describeThrown')

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
   * @param {(topic: string) => boolean} [options.isWanted] whether the client still targets a
   *   topic; a scoped refresh keeps an absent topic only while this says so (every topic by default)
   */
  constructor({ logger, instrumentationEmitter = null, isWanted = () => true }) {
    this.logger = logger.namespace('MetadataCache')
    this.instrumentationEmitter = instrumentationEmitter
    this.isWanted = isWanted

    /**
     * Per topic: its id, and per partition the accepted leader/epoch pair plus `decoded` - a
     * reference to that partition's object inside the *current* `brokerPool.metadata`. `decoded`
     * is what makes the write-through work: an accepted leader hint writes onto it, and every
     * cluster route reads it. It is re-seated on every refresh **for topics present in that
     * refresh's response** (both the accept and the defend branch), because `brokerPool.metadata`
     * replaces the objects for those topics each time; a topic **absent** from a response keeps
     * its id and epochs but releases `decoded` (set to `null`), because that object belongs to a
     * response nothing reads any more and holding it would pin the whole partition record.
     *
     * `seenAt` is the refresh generation that last carried the topic (0 when only an id lookup
     * has); it is what the release and prune pass at the end of `update()` compares against.
     *
     * Entries are mutated in place and reused across refreshes - only a first sighting, a topic
     * recreation or `clear()` allocates.
     *
     * @type {Map<string, { topicId: string|null, seenAt: number, decodedReleased: boolean, partitions: Map<number, { leader: number, leaderEpoch: number, decoded: object|null }> }>}
     */
    this.topics = new Map()
    /** @type {Map<string, string>} topic id -> topic name */
    this.topicIds = new Map()
    /**
     * Names an id lookup resolved that the client does not target yet: a scoped refresh keeps
     * them until their owner registers them (they become wanted) or retires them
     * (`releasePending`, `forget`). A full refresh that does not carry one still prunes it.
     * @type {Set<string>}
     */
    this.pendingTopics = new Set()
    /** Incremented once per `update()`; stamped onto every topic that response carried. */
    this.generation = 0
    /** @type {Set<() => void>} synchronous internal listeners, independent of instrumentation */
    this.routingListeners = new Set()
  }

  /**
   * Applies a decoded Metadata response, in three ordered steps, and writes accepted
   * leader/leaderEpoch pairs back onto the response's own partition objects.
   *
   * A topic absent from the response keeps its id and its epochs while the client still wants it -
   * a Metadata response is scoped to the topics it was asked for, so absence is never evidence of
   * deletion or recreation - but releases its `decoded` references, which belong to a response no
   * route reads any more. An absent topic nothing wants any more is dropped, as the Java client's
   * `retainTopic` drops it, so describing short-lived names cannot grow the cache. When
   * the response is a **full-cluster** one (the refresh asked for no topics, so the broker
   * answered with every topic it has), absence *is* evidence: those topics are pruned outright,
   * which is what stops one regex subscription or one `listTopics()` from seeding this cache with
   * every topic in the cluster forever.
   *
   * `TOPIC_RECREATED` is emitted last, once every entry above has been written, so a listener
   * that throws cannot leave the cache un-advanced and re-diffing the same recreation on every
   * later refresh - and `emit()` swallows such a throw anyway.
   *
   * Allocation contract: one `Set` of live broker ids per call (plus one array only when a
   * recreation actually happened), and one entry per topic and per partition only on its first
   * sighting (or after a recreation or a `clear()`). A steady-state refresh mutates the existing
   * entries - this runs on every admin call, since the admin client has no `metadataMaxAge`.
   *
   * @param {{ brokers: Array<{ nodeId: number }>, topicMetadata: Array<{ topic: string, topicId?: string|null, partitionMetadata: Array<{ partitionId: number, leader: number, leaderEpoch: number }> }> }} metadata decoded response, mutated in place
   * @param {object} [options]
   * @param {boolean} [options.full=false] true when the refresh was unscoped, i.e. the response
   *        describes every topic in the cluster
   * @param {() => boolean} [options.isCurrent] refuses stale work before any cache/event effect
   * @param {boolean} [options.publish=true] `false` leaves the routing notification to the caller,
   *        which sends it once the answer's brokers are installed
   * @param {Array<() => void>} [options.notices] receives the debug logs and topic-recreated events
   *        instead of running them, so a caller installing more in the same step runs them after it
   * @returns {void}
   */
  update(metadata, { full = false, isCurrent = () => true, publish = true, notices } = {}) {
    if (!isCurrent()) return
    const liveBrokerIds = this.liveBrokerIds(metadata)

    this.generation += 1
    let recreations = null

    for (const topicMetadata of metadata.topicMetadata) {
      const name = topicMetadata.topic
      let entry = this.topics.get(name)

      // Step 1 - topic-id diff, before any epoch comparison. A recreation drops this topic's
      // partition entries entirely, so the new incarnation's epoch 0 is accepted.
      const recreation = this.diffTopicId(name, entry, topicMetadata.topicId, notices)

      if (!entry || recreation) {
        entry = { topicId: null, seenAt: 0, decodedReleased: false, partitions: new Map() }
        this.topics.set(name, entry)
      }

      if (recreation) {
        recreations = recreations || []
        recreations.push(recreation)
      }

      // The all-zero uuid is "no id", not an id: a broker answers with it while a topic is deleted
      // or still being created. It is never recorded - `topicId()` must not hand out a sentinel -
      // and it never erases a remembered id either, so the real id that follows a deleted window
      // still compares against the previous incarnation and the recreation is detected.
      entry.topicId = this.usableTopicId(topicMetadata.topicId) ?? entry.topicId
      entry.seenAt = this.generation
      entry.decodedReleased = false

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

        // Step 2 - never defend a positive leader the cluster no longer advertises.
        // Step 3 - compare only when both sides know their epoch.
        if (this.isStale(cached, partition, liveBrokerIds)) {
          const stale = {
            topic: name,
            partition: partition.partitionId,
            cachedLeaderEpoch: cached.leaderEpoch,
            decodedLeaderEpoch: partition.leaderEpoch,
          }
          this.notify(notices, () =>
            this.logger.debug(
              'Ignoring stale metadata for partition, keeping the cached leader',
              stale
            )
          )
          partition.leader = cached.leader
          partition.leaderEpoch = cached.leaderEpoch
        } else {
          cached.leader = partition.leader
          cached.leaderEpoch = partition.leaderEpoch
        }

        // Re-seated in both branches: the previous response's object is about to be discarded.
        cached.decoded = partition
      }

      if (entry.topicId != null) {
        this.topicIds.set(entry.topicId, name)
      }
    }

    this.releaseTopicsAbsentFrom(full)

    for (const recreation of recreations || []) {
      this.notify(notices, () => this.emit(CLUSTER_TOPIC_RECREATED, recreation))
    }

    if (publish) this.publishRoutingChange()
  }

  /**
   * Runs a notification now, or queues it when the caller collects them.
   * @private
   * @param {Array<() => void>|undefined} notices the caller's queue, if any
   * @param {() => void} notice a log line or an event
   * @returns {void}
   */
  notify(notices, notice) {
    if (notices) notices.push(notice)
    else notice()
  }

  /**
   * Subscribes to routing changes: every accepted metadata install, accepted leader hint, by-ID
   * write-through and invalidation. The handler runs synchronously, after the change is visible,
   * so it must only record and wake; it receives nothing. A handler that throws is logged and
   * dropped, so one listener cannot take the metadata path down.
   * @param {() => void} handler called once per change
   * @returns {() => void} idempotent unsubscribe
   */
  onRoutingChange(handler) {
    this.routingListeners.add(handler)
    return () => this.routingListeners.delete(handler)
  }

  /**
   * Notifies the routing listeners. Conservative: it does not diff what changed.
   * Listeners can start long-lived work, so they must not inherit the publishing discovery.
   * @private
   * @returns {void}
   */
  publishRoutingChange() {
    for (const listener of this.routingListeners) {
      try {
        sharedSetup(listener)
      } catch (e) {
        this.logger.error('Routing change listener threw, ignoring it', {
          error: describeThrown(e),
        })
      }
    }
  }

  /**
   * Removes a topic and the reverse mapping for its remembered id.
   * Repeated removal is harmless; no entry is retained for a forgotten name.
   * @param {string} topic name to forget
   * @param {{publish?: boolean}} [options] `publish: false` leaves the routing notification to a
   *   caller that installs more in the same step
   * @returns {void}
   */
  forget(topic, { publish = true } = {}) {
    const entry = this.topics.get(topic)
    if (entry?.topicId != null) this.topicIds.delete(entry.topicId)
    this.topics.delete(topic)
    this.pendingTopics.delete(topic)
    if (entry && publish) this.publishRoutingChange()
  }

  /**
   * Ends every pending id resolution not in `keep`: a name the client now targets only stops
   * being pending, any other is forgotten. The owner of the id lookups calls it with the names it
   * still holds whenever its assignment changes, so no name stays pending beyond its assignment.
   * @param {Iterable<string>} [keep] names the owner still holds
   * @returns {void}
   */
  releasePending(keep = []) {
    const kept = new Set(keep)
    for (const topic of this.pendingTopics) {
      if (kept.has(topic)) continue
      if (this.isWanted(topic)) this.pendingTopics.delete(topic)
      else this.forget(topic)
    }
  }

  /**
   * Second pass of `update()`: every cached topic the response did not carry either loses its
   * `decoded` references (a scoped response says nothing about it) or is dropped entirely (a
   * full-cluster response says it no longer exists, or nothing wants it any more). A pending id
   * resolution (`pendingTopics`) is kept until its owner registers or retires it; one the client
   * now wants stops being pending here. Bounded by the number of cached topics.
   *
   * @private
   * @param {boolean} full true when the response described the whole cluster
   * @returns {void}
   */
  releaseTopicsAbsentFrom(full) {
    for (const [name, entry] of this.topics) {
      if (this.pendingTopics.has(name) && this.isWanted(name)) this.pendingTopics.delete(name)

      if (entry.seenAt === this.generation) {
        continue
      }

      if (full || (!this.pendingTopics.has(name) && !this.isWanted(name))) {
        this.topics.delete(name)
        this.pendingTopics.delete(name)
        entry.topicId != null && this.topicIds.delete(entry.topicId)
        continue
      }

      if (!entry.decodedReleased) {
        for (const cached of entry.partitions.values()) {
          cached.decoded = null
        }
        entry.decodedReleased = true
      }
    }
  }

  /**
   * Records the topics a **scoped, non-routing** Metadata response names, and nothing else.
   *
   * `update()` is for the response that becomes `brokerPool.metadata`: it bumps the generation,
   * re-seats every `decoded` write-through reference, and releases the references of every topic
   * the response did not carry. A response fetched only to resolve topic ids is routed nowhere, so
   * putting it through `update()` would release the write-through of every *other* cached topic -
   * and an accepted leader hint would then stop reaching `findLeaderForPartitions` until the next
   * full refresh.
   *
   * This entry point therefore touches only the topics the response names: their id (recreation
   * detection included) and their partitions' leader/leaderEpoch, under the same monotonic rule
   * `update()` applies. It never seats a `decoded` reference - the response's partition objects
   * are not the ones the routing path reads - and never releases, prunes or re-stamps another
   * entry. A pair it accepts *is* written through to an existing `decoded`, exactly as
   * `applyLeaderHint` does, so the cached view and the routing view cannot diverge. Every name it
   * records that the client does not target is marked pending, whether this call created its entry
   * or an earlier refresh did, so a scoped refresh cannot drop it before its owner registers it.
   *
   * @param {{ brokers: Array<{ nodeId: number }>, topicMetadata: Array<{ topic: string, topicId?: string|null, partitionMetadata: Array<{ partitionId: number, leader: number, leaderEpoch: number }> }> }} metadata decoded response; not mutated
   * @param {{isCurrent?: () => boolean}} [options] composed scope and target installation guard
   * @returns {void}
   */
  updateTopicsById(metadata, { isCurrent = () => true } = {}) {
    if (!isCurrent()) return
    const liveBrokerIds = this.liveBrokerIds(metadata)
    let recreations = null
    let changed = false

    for (const topicMetadata of metadata.topicMetadata) {
      const name = topicMetadata.topic
      let entry = this.topics.get(name)
      const recreation = this.diffTopicId(name, entry, topicMetadata.topicId)

      if (!entry || recreation) {
        entry = { topicId: null, seenAt: 0, decodedReleased: false, partitions: new Map() }
        this.topics.set(name, entry)
      }

      if (recreation) {
        recreations = recreations || []
        recreations.push(recreation)
      }

      if (!this.isWanted(name)) this.pendingTopics.add(name)

      const usableId = this.usableTopicId(topicMetadata.topicId)
      changed = changed || (usableId != null && usableId !== entry.topicId)
      entry.topicId = usableId ?? entry.topicId

      for (const partition of topicMetadata.partitionMetadata) {
        const cached = entry.partitions.get(partition.partitionId)

        if (!cached) {
          entry.partitions.set(partition.partitionId, {
            leader: partition.leader,
            leaderEpoch: partition.leaderEpoch,
            decoded: null,
          })
          changed = true
          continue
        }

        if (this.isStale(cached, partition, liveBrokerIds)) {
          continue
        }

        changed =
          changed ||
          cached.leader !== partition.leader ||
          cached.leaderEpoch !== partition.leaderEpoch
        cached.leader = partition.leader
        cached.leaderEpoch = partition.leaderEpoch

        if (cached.decoded) {
          cached.decoded.leader = partition.leader
          cached.decoded.leaderEpoch = partition.leaderEpoch
        }
      }

      if (entry.topicId != null) {
        this.topicIds.set(entry.topicId, name)
      }
    }

    for (const recreation of recreations || []) {
      this.emit(CLUSTER_TOPIC_RECREATED, recreation)
    }

    if (changed || recreations) this.publishRoutingChange()
  }

  /**
   * @private
   * @param {{ brokers: Array<{ nodeId: number }> }} metadata
   * @returns {Set<number>} the node ids the response advertises
   */
  liveBrokerIds(metadata) {
    const ids = new Set()
    for (const { nodeId } of metadata.brokers) {
      ids.add(nodeId)
    }
    return ids
  }

  /**
   * Whether a response's partition is provably behind what the cache already accepted: the cached
   * leader is still advertised or is `-1` (an election the cache has seen, which a lagging
   * response must not undo), both sides know their epoch, and the response's is older. A cached
   * positive leader the response no longer advertises is not defended. Shared by `update()` and
   * `updateTopicsById()` so the monotonic rule cannot drift between them.
   *
   * @private
   * @param {{ leader: number, leaderEpoch: number }} cached
   * @param {{ leader: number, leaderEpoch: number }} partition
   * @param {Set<number>} liveBrokerIds
   * @returns {boolean}
   */
  isStale(cached, partition, liveBrokerIds) {
    return (
      (cached.leader === NO_LEADER_ID || liveBrokerIds.has(cached.leader)) &&
      cached.leaderEpoch > UNKNOWN_EPOCH &&
      partition.leaderEpoch > UNKNOWN_EPOCH &&
      partition.leaderEpoch < cached.leaderEpoch
    )
  }

  /**
   * @private
   * @param {string|null|undefined} topicId
   * @returns {string|null} the id, or null when it is absent or the all-zero "no id" sentinel
   */
  usableTopicId(topicId) {
    return topicId != null && topicId !== NO_TOPIC_ID ? topicId : null
  }

  /**
   * Detects a recreation (a known name arriving under a different, non-zero id) and forgets the
   * previous id. Deliberately does **not** emit: `update()` emits once the cache has advanced.
   *
   * @private
   * @param {string} name
   * @param {{ topicId: string|null }|undefined} entry
   * @param {string|null|undefined} topicId the id the fresh response carries
   * @param {Array<() => void>} [notices] receives the debug log instead of running it
   * @returns {{ topic: string, previousTopicId: string, topicId: string }|null} the event payload
   *          for a recreation, null otherwise
   */
  diffTopicId(name, entry, topicId, notices) {
    const previousTopicId = entry && entry.topicId
    const freshTopicId = this.usableTopicId(topicId)
    const isRecreation =
      previousTopicId != null && freshTopicId != null && freshTopicId !== previousTopicId

    if (!isRecreation) {
      return null
    }

    this.topicIds.delete(previousTopicId)
    const recreation = { topic: name, previousTopicId, topicId: freshTopicId }
    this.notify(notices, () =>
      this.logger.debug('Topic has been recreated under a new id', { ...recreation })
    )
    return recreation
  }

  /**
   * Applies a leader hint the broker attached to a produce error (KIP-951). Synchronous, and the
   * write-through onto the decoded partition object makes it visible to every cluster route
   * immediately - `findLeaderForPartitions`, `fetchTopicsOffset` and the consumer's read-replica
   * selection all read that object. A topic whose `decoded` reference was released (it was absent
   * from the last response) still updates the cached pair; the next refresh that carries the
   * topic re-seats `decoded` and the routing structure catches up there.
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

    if (cached.decoded) {
      cached.decoded.leader = leaderId
      cached.decoded.leaderEpoch = leaderEpoch
    }

    this.publishRoutingChange()
    return true
  }

  /**
   * Forgets one partition's leader epoch, in the cache **and** in its write-through copy, so both
   * views agree. `leader` is left untouched in both: the client still knows where to route, it
   * just stops claiming to know which epoch it is routing against.
   *
   * This is the answer to `UNKNOWN_LEADER_EPOCH` (75), which means the *client* is ahead of the
   * broker. The monotonic guard in `update()` would otherwise defend the higher cached epoch, the
   * refresh that follows would be rejected, and every retry would resend the same epoch forever.
   * With the epoch unknown, the next request carries `-1` (no fencing) and the next metadata
   * response is accepted outright by the "either side unknown" branch.
   *
   * @see https://kafka.apache.org/43/design/protocol
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean} true when a cached entry existed and was forgotten
   */
  forgetLeaderEpoch(topic, partition) {
    const entry = this.topics.get(topic)
    const cached = entry && entry.partitions.get(partition)

    if (!cached) {
      return false
    }

    cached.leaderEpoch = UNKNOWN_EPOCH

    // `decoded` is null for a topic released by `releaseTopicsAbsentFrom`; guarded exactly as
    // `applyLeaderHint` guards it.
    if (cached.decoded) {
      cached.decoded.leaderEpoch = UNKNOWN_EPOCH
    }

    this.publishRoutingChange()
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

  /**
   * The cached leader of one partition, as of the last Metadata response or accepted leader hint.
   * A map lookup that fetches nothing.
   * @param {string} topic
   * @param {number} partition
   * @returns {number} the node id, or -1 when the partition is unknown or has no leader
   */
  leader(topic, partition) {
    const entry = this.topics.get(topic)
    const cached = entry && entry.partitions.get(partition)
    return cached && cached.leader != null ? cached.leader : NO_LEADER_ID
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
    this.pendingTopics.clear()
    this.publishRoutingChange()
  }

  /**
   * Emits an instrumentation event, best-effort. Listeners are invoked synchronously by the
   * emitter, and the clients' public `on()` wrapper only guards a *rejected promise*, not a
   * synchronous throw - so one application listener that throws would otherwise propagate out of
   * a metadata refresh and take the client's metadata path down with it. A throw is logged and
   * dropped instead. Listener work is independent of the publishing discovery's lifetime.
   *
   * @private
   * @param {string} eventName
   * @param {object} payload
   * @returns {void}
   */
  emit(eventName, payload) {
    if (!this.instrumentationEmitter) {
      return
    }

    try {
      sharedSetup(() => this.instrumentationEmitter.emit(eventName, payload))
    } catch (e) {
      this.logger.error('Instrumentation event listener threw, ignoring it', {
        eventName,
        topic: payload && payload.topic,
        error: describeThrown(e),
      })
    }
  }
}
