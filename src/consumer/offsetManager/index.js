const Long = require('../../utils/long')
const isInvalidOffset = require('./isInvalidOffset')
const initializeConsumerOffsets = require('./initializeConsumerOffsets')
const { isStaleCoordinator } = require('../../errors')
const {
  events: { COMMIT_OFFSETS },
} = require('../instrumentationEvents')

const { keys, assign } = Object
const indexTopics = (topics) => topics.reduce((obj, topic) => assign(obj, { [topic]: {} }), {})

const PRIVATE = {
  COMMITTED_OFFSETS: Symbol('private:OffsetManager:committedOffsets'),
}

// Shared, never mutated: the default for resolveOffsets()'s seekedPartitions parameter, so the
// overwhelmingly common no-seek call allocates nothing.
const EMPTY_SEEKED_PARTITIONS = new Set()
module.exports = class OffsetManager {
  /**
   * Key format `resolveOffsets()`'s `seekedPartitions` set uses to identify a topic/partition,
   * shared with `ConsumerGroup#seekOffsets()` so both sides agree on it without either owning
   * the other's internals.
   * @param {string} topic
   * @param {number} partition
   * @returns {string}
   */
  static seekedPartitionKey(topic, partition) {
    return `${topic}\u0000${partition}`
  }

  /**
   * @param {Object} options
   * @param {import("../../../types").Cluster} options.cluster
   * @param {import("../../../types").Broker} options.coordinator
   * @param {import("../../../types").IMemberAssignment} options.memberAssignment
   * @param {boolean} options.autoCommit
   * @param {number | null} options.autoCommitInterval
   * @param {number | null} options.autoCommitThreshold
   * @param {{[topic: string]: { fromBeginning: boolean }}} options.topicConfigurations
   * @param {import("../../instrumentation/emitter")} options.instrumentationEmitter
   * @param {string} options.groupId
   * @param {number} options.generationId
   * @param {string} options.memberId
   * @param {string} [options.groupInstanceId=null] Static membership id (KIP-345); carried on
   *                                every offset commit so the coordinator can identify this
   *                                member across restarts.
   * @param {Map<string, string>} [options.pendingTopicResets] Topics deleted and recreated while
   *                                subscribed, each keyed by topic and stamped with its new topic
   *                                id (owned by the long-lived `Consumer`; shared by reference
   *                                across every `OffsetManager` a session builds).
   */
  constructor({
    cluster,
    coordinator,
    memberAssignment,
    autoCommit,
    autoCommitInterval,
    autoCommitThreshold,
    topicConfigurations,
    instrumentationEmitter,
    groupId,
    generationId,
    memberId,
    groupInstanceId = null,
    pendingTopicResets = new Map(),
  }) {
    this.cluster = cluster
    this.coordinator = coordinator

    // memberAssignment format:
    // {
    //   'topic1': [0, 1, 2, 3],
    //   'topic2': [0, 1, 2, 3, 4, 5],
    // }
    this.memberAssignment = memberAssignment

    this.topicConfigurations = topicConfigurations
    this.instrumentationEmitter = instrumentationEmitter
    this.groupId = groupId
    this.generationId = generationId
    this.memberId = memberId
    this.groupInstanceId = groupInstanceId
    this.pendingTopicResets = pendingTopicResets

    this.autoCommit = autoCommit
    this.autoCommitInterval = autoCommitInterval
    this.autoCommitThreshold = autoCommitThreshold
    this.lastCommit = Date.now()

    this.topics = keys(memberAssignment)
    this.clearAllOffsets()
  }

  /**
   * The offset this partition would be fetched from next, seeding the resolved offset from the
   * committed one the first time.
   *
   * The seeding is skipped when there is no committed offset: writing one in would leave an own,
   * enumerable key holding `undefined`, which `countResolvedOffsets()` and `uncommittedOffsets()`
   * both walk. Such a partition is not fetched this round anyway.
   *
   * @param {string} topic
   * @param {number} partition
   * @returns {Long} the committed offset, or 0 when there is none or it is invalid
   */
  nextOffset(topic, partition) {
    const committedOffset = this.committedOffsets()[topic][partition]

    if (!this.resolvedOffsets[topic][partition] && committedOffset != null) {
      this.resolvedOffsets[topic][partition] = committedOffset
    }

    let offset = this.resolvedOffsets[topic][partition]
    if (isInvalidOffset(offset)) {
      offset = '0'
    }

    return Long.fromValue(offset)
  }

  /**
   * @returns {Promise<import("../../../types").Broker>}
   */
  async getCoordinator() {
    if (!this.coordinator.isConnected()) {
      // findBroker() would resolve the same (stale) node after a failover; ask the cluster
      // which broker currently owns the group instead. Upstream PR #1779.
      this.coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId })
    }

    return this.coordinator
  }

  /**
   * Rewinds the resolved offset back to the committed one, dropping the tracked epoch with it: the
   * position moves without a record behind it, and keeping the epoch of a record further ahead
   * would overstate where this partition actually is.
   * @param {import("../../../types").TopicPartition} topicPartition
   * @returns {void}
   */
  resetOffset({ topic, partition }) {
    this.resolvedOffsets[topic][partition] = this.committedOffsets()[topic][partition]
    this.resolvedEpochs[topic][partition] = -1
  }

  /**
   * Forgets every offset this member holds for one topic, re-seeding rather than deleting: the
   * fetch loop and `nextOffset` both index `committedOffsets[topic][partition]` without a guard,
   * so a missing topic key is a TypeError inside the fetch, not a recoverable state.
   * @param {string} topic
   * @returns {void}
   */
  resetOffsetsForTopic(topic) {
    this.committedOffsets()[topic] = {}
    this.resolvedOffsets[topic] = {}
    this.resolvedEpochs[topic] = {}
  }

  /**
   * @param {import("../../../types").TopicPartitionOffset & { leaderEpoch?: number }} topicPartitionOffset
   *   `leaderEpoch` is the epoch of the record at `offset`, or -1 when no record backs it - a
   *   position advanced past a log-compacted batch, an offset outside the batch a handler
   *   resolved, or a seek. It defaults to -1 so an omission is safe rather than stale: keeping a
   *   previous epoch would understate the position and could seek the partition backwards.
   */
  resolveOffset({ topic, partition, offset, leaderEpoch = -1 }) {
    this.resolvedOffsets[topic][partition] = Long.fromValue(offset).add(1).toString()
    this.resolvedEpochs[topic][partition] = leaderEpoch
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {number} the leader epoch of the last record consumed on this partition, -1 when unknown
   */
  lastConsumedEpoch(topic, partition) {
    const epoch = this.resolvedEpochs[topic] && this.resolvedEpochs[topic][partition]
    return typeof epoch === 'number' ? epoch : -1
  }

  /**
   * Forgets the tracked epoch **without touching the offset** - the only writer that does.
   *
   * It exists for the one transition that needs it: position validation gave up on a partition
   * (its bounded retries were exhausted), so the partition must be fetched again unvalidated, from
   * the same offset. `resolveOffset` would advance the offset, and every other clearing path
   * (`seek`, `setDefaultOffset`, `clearOffsets`, `clearAllOffsets`, `resetOffsetsForTopic`) drops
   * the committed offset too.
   *
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  forgetConsumedEpoch(topic, partition) {
    if (this.resolvedEpochs[topic]) {
      this.resolvedEpochs[topic][partition] = -1
    }
  }

  /**
   * @returns {Long}
   */
  countResolvedOffsets() {
    const committedOffsets = this.committedOffsets()

    const subtractOffsets = (resolvedOffset, committedOffset) => {
      // A key with no value is "nothing resolved", not an offset: `Long.fromValue` would throw.
      if (resolvedOffset == null) {
        return Long.fromValue(0)
      }

      const resolvedOffsetLong = Long.fromValue(resolvedOffset)
      return isInvalidOffset(committedOffset)
        ? resolvedOffsetLong
        : resolvedOffsetLong.subtract(Long.fromValue(committedOffset))
    }

    const subtractPartitionOffsets = (resolvedTopicOffsets, committedTopicOffsets) =>
      keys(resolvedTopicOffsets).map((partition) =>
        subtractOffsets(resolvedTopicOffsets[partition], committedTopicOffsets[partition])
      )

    const subtractTopicOffsets = (topic) =>
      subtractPartitionOffsets(this.resolvedOffsets[topic], committedOffsets[topic])

    const offsetsDiff = this.topics.flatMap(subtractTopicOffsets)
    return offsetsDiff.reduce((sum, offset) => sum.add(offset), Long.fromValue(0))
  }

  /**
   * @param {import("../../../types").TopicPartition} topicPartition
   */
  async setDefaultOffset({ topic, partition }) {
    const { groupId, generationId, memberId, groupInstanceId } = this
    const defaultOffset = this.cluster.defaultOffset(this.topicConfigurations[topic])
    const coordinator = await this.getCoordinator()

    await coordinator.offsetCommit({
      groupId,
      memberId,
      groupGenerationId: generationId,
      groupInstanceId,
      topics: [
        {
          topic,
          partitions: [{ partition, offset: defaultOffset }],
        },
      ],
    })

    this.clearOffsets({ topic, partition })
  }

  /**
   * Commit the given offset to the topic/partition. If the consumer isn't assigned to the given
   * topic/partition this method will be a NO-OP.
   *
   * @param {import("../../../types").TopicPartitionOffset} topicPartitionOffset
   * @returns {Promise<boolean>} whether the seek was applied (`false` only when the partition is
   *                              not currently assigned to this member). The caller uses this to
   *                              tell `resolveOffsets()` which partitions were just seeked, so a
   *                              seek applied after a topic recreation can win over that
   *                              recreation's one-shot reset for the same partition.
   */
  async seek({ topic, partition, offset }) {
    if (!this.memberAssignment[topic] || !this.memberAssignment[topic].includes(partition)) {
      return false
    }

    if (!this.autoCommit) {
      this.resolveOffset({
        topic,
        partition,
        offset: Long.fromValue(offset).subtract(1).toString(),
        // A seek names a position, not a record: nothing has been consumed at it yet.
        leaderEpoch: -1,
      })
      return true
    }

    const { groupId, generationId, memberId, groupInstanceId } = this
    const coordinator = await this.getCoordinator()

    await coordinator.offsetCommit({
      groupId,
      memberId,
      groupGenerationId: generationId,
      groupInstanceId,
      topics: [
        {
          topic,
          partitions: [{ partition, offset }],
        },
      ],
    })

    this.clearOffsets({ topic, partition })
    return true
  }

  async commitOffsetsIfNecessary() {
    const now = Date.now()

    const timeoutReached =
      this.autoCommitInterval != null && now >= this.lastCommit + this.autoCommitInterval

    const thresholdReached =
      this.autoCommitThreshold != null &&
      this.countResolvedOffsets().gte(Long.fromValue(this.autoCommitThreshold))

    if (timeoutReached || thresholdReached) {
      return this.commitOffsets()
    }
  }

  /**
   * Return all locally resolved offsets which are not marked as committed, by topic-partition.
   * @returns {import('../../../types').OffsetsByTopicPartition}
   */
  uncommittedOffsets() {
    const offsets = (topic) => keys(this.resolvedOffsets[topic])
    const emptyPartitions = ({ partitions }) => partitions.length > 0
    const toPartitions = (topic) => (partition) => ({
      partition,
      offset: this.resolvedOffsets[topic][partition],
      leaderEpoch: this.lastConsumedEpoch(topic, partition),
    })
    const changedOffsets =
      (topic) =>
      ({ partition, offset }) => {
        return (
          offset != null &&
          offset !== this.committedOffsets()[topic][partition] &&
          Long.fromValue(offset).greaterThanOrEqual(0)
        )
      }

    // Select and format updated partitions
    const topicsWithPartitionsToCommit = this.topics
      .map((topic) => ({
        topic,
        partitions: offsets(topic).map(toPartitions(topic)).filter(changedOffsets(topic)),
      }))
      .filter(emptyPartitions)

    return { topics: topicsWithPartitionsToCommit }
  }

  /**
   * Commits `offsets.topics`, defaulting to everything resolved since the last commit, and emits
   * `COMMIT_OFFSETS` with the payload that was sent.
   *
   * When the offsets come from `uncommittedOffsets()` each partition entry carries `leaderEpoch`
   * (-1 when no record backs the offset), so that key is visible on the emitted payload too.
   * Offsets a caller supplies are forwarded untouched and carry whatever they were given.
   *
   * @param {import('../../../types').Offsets} [offsets]
   * @returns {Promise<void>}
   */
  async commitOffsets(offsets = {}) {
    const { groupId, generationId, memberId, groupInstanceId } = this
    const { topics = this.uncommittedOffsets().topics } = offsets

    if (topics.length === 0) {
      this.lastCommit = Date.now()
      return
    }

    const payload = {
      groupId,
      memberId,
      groupGenerationId: generationId,
      groupInstanceId,
      topics,
    }

    try {
      const coordinator = await this.getCoordinator()
      await coordinator.offsetCommit(payload)
      this.instrumentationEmitter.emit(COMMIT_OFFSETS, payload)

      // Update local reference of committed offsets
      topics.forEach(({ topic, partitions }) => {
        const updatedOffsets = partitions.reduce(
          (obj, { partition, offset }) => assign(obj, { [partition]: offset }),
          {}
        )

        this[PRIVATE.COMMITTED_OFFSETS][topic] = assign(
          {},
          this.committedOffsets()[topic],
          updatedOffsets
        )
      })

      this.lastCommit = Date.now()
    } catch (e) {
      // The coordinator has changed due to a restart or broker reassignment; refresh the
      // metadata *and* re-discover the coordinator so the next attempt targets the right broker.
      // Best-effort: if the retrier behind findGroupCoordinator exhausts, it rejects with a
      // non-retriable KafkaJSNumberOfRetriesExceeded, which must not replace the retriable `e`
      // the caller is waiting for -- the next retry of `e` will attempt discovery again anyway.
      if (isStaleCoordinator(e)) {
        try {
          await this.cluster.refreshMetadata()
          this.coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId })
        } catch (_) {
          // best-effort recovery, see above -- fall through to rethrow the original `e`
        }
      }

      throw e
    }
  }

  /**
   * @param {Set<string>} [seekedPartitions] Keys (`OffsetManager.seekedPartitionKey(topic,
   *                                partition)`) of partitions a `seek()` was just applied to,
   *                                in this same `ConsumerGroup#seekOffsets()` call. A seeked
   *                                partition wins over a pending topic-recreation reset for that
   *                                partition only - it is newer, explicit user intent, and its
   *                                offset targets the current topic incarnation. A seek issued
   *                                *before* the recreation never reaches here: the recreation's
   *                                `ConsumerGroup#invalidateTopic` already dropped it from the
   *                                pending-seek queue, since its target belonged to the old
   *                                incarnation.
   * @returns {Promise<void>}
   */
  async resolveOffsets(seekedPartitions = EMPTY_SEEKED_PARTITIONS) {
    const { groupId } = this
    const invalidOffset = (topic) => (partition) => {
      return isInvalidOffset(this.committedOffsets()[topic][partition])
    }

    const pendingPartitions = this.topics
      .map((topic) => ({
        topic,
        partitions: this.memberAssignment[topic]
          .filter(invalidOffset(topic))
          .map((partition) => ({ partition })),
      }))
      .filter((t) => t.partitions.length > 0)

    if (pendingPartitions.length === 0) {
      return
    }

    const coordinator = await this.getCoordinator()
    const { responses: consumerOffsets } = await coordinator.offsetFetch({
      groupId,
      topics: pendingPartitions,
    })

    this.drainPendingTopicResets(consumerOffsets, seekedPartitions)

    // Seed the tracked epoch from what the coordinator has committed, before
    // initializeConsumerOffsets rebuilds these entries as { partition, offset } and drops every
    // other field - for every partition, not only the ones it resolved. An invalid offset here is
    // the "this partition is being reset" signal drainPendingTopicResets just wrote, so its stale
    // epoch is deliberately not seeded.
    for (const { topic, partitions } of consumerOffsets) {
      const epochs = this.resolvedEpochs[topic]
      if (!epochs) {
        continue
      }

      for (const entry of partitions) {
        if (isInvalidOffset(entry.offset) || typeof entry.committedLeaderEpoch !== 'number') {
          continue
        }
        epochs[entry.partition] = entry.committedLeaderEpoch
      }
    }

    const unresolvedPartitions = consumerOffsets.map(({ topic, partitions }) =>
      assign(
        {
          topic,
          partitions: partitions
            .filter(({ offset }) => isInvalidOffset(offset))
            .map(({ partition }) => assign({ partition })),
        },
        this.topicConfigurations[topic]
      )
    )

    const indexPartitions = (obj, { partition, offset }) => {
      return assign(obj, { [partition]: offset })
    }

    const hasUnresolvedPartitions = () => unresolvedPartitions.some((t) => t.partitions.length > 0)

    let offsets = consumerOffsets
    if (hasUnresolvedPartitions()) {
      const topicOffsets = await this.cluster.fetchTopicsOffset(unresolvedPartitions)
      offsets = initializeConsumerOffsets(consumerOffsets, topicOffsets)
    }

    offsets.forEach(({ topic, partitions }) => {
      this.committedOffsets()[topic] = partitions.reduce(indexPartitions, {
        ...this.committedOffsets()[topic],
      })
    })
  }

  /**
   * Applies any one-shot reset scheduled for the topics this response covers, by replacing their
   * committed offsets with the invalid sentinel so every partition falls through to the normal
   * "unresolved" path and is resolved per the topic's own `fromBeginning`.
   *
   * A pending entry is stamped with the topic id the recreation produced. It applies when the
   * cluster's cached id is unknown (the cache was cleared, or the topic has not appeared in a
   * metadata response yet) **or** equal to the stamp, and is dropped without resetting only when
   * a *known* cached id differs - treating unknown as "changed" would let a rebootstrap silently
   * discard the reset, which is the stale-offset skip this mechanism exists to prevent. Either
   * way the entry is consumed at the first resolution that covers the topic.
   *
   * A partition in `seekedPartitions` is left untouched even when its topic is being reset: a
   * `seek()` applied after the recreation targets the *current* incarnation and is newer, more
   * specific user intent than the reset, so it wins for that partition while the rest of the
   * topic still resets normally.
   *
   * @private
   * @param {{topic: string, partitions: {partition: number, offset: string}[]}[]} consumerOffsets
   *   mutated in place: a reset topic's partition offsets become the invalid sentinel
   * @param {Set<string>} seekedPartitions `OffsetManager.seekedPartitionKey(topic, partition)`
   *   entries to exempt from the reset
   * @returns {void}
   */
  drainPendingTopicResets(consumerOffsets, seekedPartitions) {
    for (const { topic, partitions } of consumerOffsets) {
      if (!this.pendingTopicResets.has(topic)) {
        continue
      }

      const stampedTopicId = this.pendingTopicResets.get(topic)
      const currentTopicId = this.cluster.topicId(topic)
      this.pendingTopicResets.delete(topic)

      if (currentTopicId !== undefined && currentTopicId !== stampedTopicId) {
        continue
      }

      for (const partition of partitions) {
        if (seekedPartitions.has(OffsetManager.seekedPartitionKey(topic, partition.partition))) {
          continue
        }

        partition.offset = '-1'
      }
    }
  }

  /**
   * @private
   * @param {import("../../../types").TopicPartition} topicPartition
   */
  clearOffsets({ topic, partition }) {
    delete this.committedOffsets()[topic][partition]
    delete this.resolvedOffsets[topic][partition]
    delete this.resolvedEpochs[topic][partition]
  }

  /**
   * @private
   */
  clearAllOffsets() {
    const committedOffsets = this.committedOffsets()

    for (const topic in committedOffsets) {
      delete committedOffsets[topic]
    }

    for (const topic of this.topics) {
      committedOffsets[topic] = {}
    }

    this.resolvedOffsets = indexTopics(this.topics)

    /**
     * The leader epoch of the last record consumed on each partition, beside its resolved offset.
     * -1 means unknown, which disables position validation for that partition until it consumes a
     * record again. Cleared with the offset by every path that clears an offset.
     * @type {{[topic: string]: {[partition: number]: number}}}
     */
    this.resolvedEpochs = indexTopics(this.topics)
  }

  committedOffsets() {
    if (!this[PRIVATE.COMMITTED_OFFSETS]) {
      this[PRIVATE.COMMITTED_OFFSETS] = this.groupId
        ? this.cluster.committedOffsets({ groupId: this.groupId })
        : {}
    }

    return this[PRIVATE.COMMITTED_OFFSETS]
  }
}
