const Long = require('../../utils/long')
const sleep = require('../../utils/sleep')
const isInvalidOffset = require('./isInvalidOffset')
const initializeConsumerOffsets = require('./initializeConsumerOffsets')
const { KafkaJSError, isStaleCoordinator } = require('../../errors')
const { stampGeneration, stampOffsetCommit } = require('../rebalanceErrorContract')
const {
  events: { COMMIT_OFFSETS },
} = require('../instrumentationEvents')

const { keys, assign } = Object
const indexTopics = (topics) => topics.reduce((obj, topic) => assign(obj, { [topic]: {} }), {})

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const STALE_MEMBER_EPOCH = 'STALE_MEMBER_EPOCH'
const UNSTABLE_OFFSET_COMMIT = 'UNSTABLE_OFFSET_COMMIT'

/**
 * How long a retried offset fetch first waits before it is sent again, and the ceiling an
 * UNSTABLE_OFFSET_COMMIT wait doubles up to: the Java client's `retry.backoff.ms` and
 * `retry.backoff.max.ms` defaults.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java
 */
const RETRY_INTERVAL = 100
const UNSTABLE_RETRY_MAX_INTERVAL = 1000

/** Used when no logger is injected, so a manager built without one still runs and logs nothing. */
const SILENT_LOGGER = { debug() {}, info() {}, warn() {}, error() {} }

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
   * @private
   * @param {import("../../../types").IMemberAssignment} memberAssignment
   * @returns {Map<string, Set<number>>}
   */
  static indexAssignment(memberAssignment) {
    return new Map(keys(memberAssignment).map((topic) => [topic, new Set(memberAssignment[topic])]))
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
   * @param {'classic'|'consumer'} [options.membershipProtocol='classic'] which group protocol this
   *                                member runs. Under `'consumer'` the offset fetch carries the
   *                                member id and epoch, which is how the coordinator validates a
   *                                read against a live member (KIP-848).
   * @param {number} [options.staleEpochTimeout=300000] how long an offset fetch answered with
   *                                UNSTABLE_OFFSET_COMMIT, or with a stale member epoch, keeps
   *                                being re-sent.
   * @param {() => {generationId: number, memberId: string,
   *                                coordinator: import("../../../types").Broker}} [options.membership]
   *                                a LIVE read of the membership this manager should send under.
   *                                The offset-fetch retry consults it before every attempt under
   *                                the consumer protocol, because the epoch it needs is installed
   *                                by the heartbeats while the retry loop is running - a snapshot
   *                                would re-send a rejected epoch forever. Defaults to this
   *                                manager's own snapshot, which is what every caller that has no
   *                                membership to offer wants.
   * @param {() => boolean} [options.isStopping] whether the consumer has been asked to stop. The
   *                                offset-fetch retry abandons its deadline the moment this is
   *                                true, because the fetch loop it runs inside is one
   *                                `Runner#stop()` waits for.
   * @param {() => boolean} [options.isRebalancePending] whether a rebalance is waiting for the
   *                                current fetch round to end. The offset-fetch retry abandons its
   *                                deadline the moment this is true, so the member can rejoin.
   *                                Defaults to never.
   * @param {() => void} [options.markPolled] called before each UNSTABLE_OFFSET_COMMIT re-send: a
   *                                fetch round waiting on an open transaction is still polling, as
   *                                an empty round is. Defaults to a no-op.
   * @param {import('../../../types').Logger} [options.logger] where this manager reports a stale
   *                                member epoch and why an offset fetch is re-sent. Optional and
   *                                silent by default: the consumer group is the only production
   *                                caller and passes its own namespaced logger, while every test
   *                                that constructs a manager directly cares about offsets rather
   *                                than log lines.
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
    membershipProtocol = 'classic',
    staleEpochTimeout = 300000,
    membership = null,
    isStopping = null,
    isRebalancePending = null,
    markPolled = null,
    logger = SILENT_LOGGER,
  }) {
    this.cluster = cluster
    this.coordinator = coordinator

    // memberAssignment format:
    // {
    //   'topic1': [0, 1, 2, 3],
    //   'topic2': [0, 1, 2, 3, 4, 5],
    // }
    this.memberAssignment = memberAssignment

    /**
     * The assignment as a lookup, so the ownership guard on the resolve path is a Set membership
     * test rather than a linear scan of a partition array on every resolved offset.
     * @type {Map<string, Set<number>>}
     */
    this.assignedPartitions = OffsetManager.indexAssignment(memberAssignment)

    this.topicConfigurations = topicConfigurations
    this.instrumentationEmitter = instrumentationEmitter
    this.groupId = groupId
    this.generationId = generationId
    this.memberId = memberId
    this.groupInstanceId = groupInstanceId
    this.pendingTopicResets = pendingTopicResets
    this.membershipProtocol = membershipProtocol
    this.staleEpochTimeout = staleEpochTimeout
    this.membership =
      membership ||
      (() => ({
        generationId: this.generationId,
        memberId: this.memberId,
        coordinator: this.coordinator,
      }))
    this.isStopping = isStopping || (() => false)
    this.isRebalancePending = isRebalancePending || (() => false)
    this.markPolled = markPolled || (() => {})
    this.logger = logger

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
   * The offset this partition has resolved to, exactly as stored.
   *
   * A pure read, on the fetch hot path: no seeding (unlike `nextOffset`, which writes
   * `resolvedOffsets` when it seeds from the committed offset), no allocation, and no throw for a
   * topic whose entries were pruned or never created.
   * @param {string} topic
   * @param {number} partition
   * @returns {string|undefined} the decimal string, or undefined when nothing is resolved
   */
  resolvedOffsetAt(topic, partition) {
    const offsets = this.resolvedOffsets[topic]
    return offsets ? offsets[partition] : undefined
  }

  /**
   * One OffsetCommit, with every failure marked as raised on the commit path.
   *
   * Every commit this manager sends goes through here - the periodic auto-commit, a user's
   * `commitOffsets`, the pre-release commit, `seek` and the out-of-range reset - so the stamp is
   * on the error however it later reaches the rebalance contract. It has to be recorded here
   * because the contract classifies a commit failure far from where it was raised: an
   * auto-commit's error surfaces out of the fetch round and is classified on the FETCH path.
   * @private
   * @param {object} payload the `Broker#offsetCommit` payload
   * @returns {Promise<object>} the decoded response
   * @throws {Error} whatever the coordinator answered, stamped
   */
  async sendOffsetCommit(payload) {
    const coordinator = await this.getCoordinator()

    try {
      return await coordinator.offsetCommit(payload)
    } catch (e) {
      throw stampOffsetCommit(e)
    }
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
    // A handler can still be running for a partition this member lost during a cooperative
    // round. Writing its position back would resolve an offset against a generation that no
    // longer owns the partition, and the commit would then roll the new owner back.
    if (!this.owns(topic, partition)) {
      return
    }

    this.resolvedOffsets[topic][partition] = Long.fromValue(offset).add(1).toString()
    this.resolvedEpochs[topic][partition] = leaderEpoch
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean} whether this member holds this partition at the current generation
   */
  owns(topic, partition) {
    const partitions = this.assignedPartitions.get(topic)
    return partitions !== undefined && partitions.has(partition)
  }

  /**
   * Moves this manager to a new assignment without discarding what the retained partitions know.
   *
   * Only the incremental (cooperative) apply calls this; an apply that revokes everything builds a
   * fresh manager instead. Pruning is at PARTITION level, so a fully revoked topic keeps an empty
   * object: `nextOffset` indexes `[topic][partition]` unguarded, and a topic-level prune would
   * silently rewind a partition this member kept back to the start of its log.
   *
   * @param {object} options
   * @param {Array<{topic: string, partitions: number[]}>} options.revoked partitions to prune
   * @param {import("../../../types").IMemberAssignment} options.memberAssignment the full new one;
   *   it already names every added topic, so the keys they need are created from it
   * @param {number} options.generationId
   * @param {string} options.memberId
   * @param {import("../../../types").Broker} options.coordinator re-resolved every sync, so a
   *   coordinator that moved is used for the next commit
   * @returns {void}
   */
  updateAssignment({ revoked, memberAssignment, generationId, memberId, coordinator }) {
    this.memberAssignment = memberAssignment
    this.assignedPartitions = OffsetManager.indexAssignment(memberAssignment)
    this.topics = keys(memberAssignment)
    this.generationId = generationId
    this.memberId = memberId
    this.coordinator = coordinator

    // Keys first, prune second: another consumer of this group on the same client shares the
    // committed-offsets object and may have removed a topic key since the last round.
    const committedOffsets = this.committedOffsets()
    for (const topic of this.topics) {
      committedOffsets[topic] = committedOffsets[topic] || {}
      this.resolvedOffsets[topic] = this.resolvedOffsets[topic] || {}
      this.resolvedEpochs[topic] = this.resolvedEpochs[topic] || {}
    }

    for (const { topic, partitions } of revoked) {
      for (const partition of partitions) {
        this.clearOffsets({ topic, partition })
      }
    }
  }

  /**
   * Points this manager at the membership it should send under, without touching the assignment,
   * the resolved offsets or the committed cache.
   *
   * `updateAssignment` is the wrong tool for this: it prunes offsets and rebuilds the topic index.
   * A commit issued between two rebalances - the pre-release commit, above all - only needs the
   * generation, the member id and the coordinator to be current, because a request built from a
   * stale generation draws ILLEGAL_GENERATION under the classic protocol and STALE_MEMBER_EPOCH
   * under the consumer one.
   *
   * Under the consumer protocol `generationId` IS the member epoch: `ConsumerGroup.generationId`
   * aliases it, so the same field carries `GenerationIdOrMemberEpoch` on the wire.
   *
   * @param {object} membership
   * @param {number} membership.generationId
   * @param {string} membership.memberId
   * @param {import("../../../types").Broker} membership.coordinator
   * @returns {void}
   */
  useMembership({ generationId, memberId, coordinator }) {
    this.generationId = generationId
    this.memberId = memberId
    this.coordinator = coordinator
  }

  /**
   * Re-reads the membership this manager sends under from the injected accessor.
   *
   * The coordinator is kept when the membership has none: a consumer-protocol membership nulls its
   * own `coordinator` while it re-finds one after a `NOT_COORDINATOR`, and `getCoordinator()`
   * dereferences the field. The stale broker it keeps is re-resolved there anyway.
   * @private
   * @returns {void}
   */
  refreshMembership() {
    const { generationId, memberId, coordinator } = this.membership()

    this.useMembership({
      generationId,
      memberId,
      coordinator: coordinator || this.coordinator,
    })
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
    await this.sendOffsetCommit({
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

    await this.sendOffsetCommit({
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
      return this.commitOffsets({}, { auto: true })
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
   * `uncommittedOffsets().topics`, optionally narrowed to a set of topic-partitions.
   * @private
   * @param {import('../groupProtocol/interface').TopicPartitions[]|null} partitions
   * @returns {import('../../../types').OffsetsByTopicPartition["topics"]}
   */
  selectUncommitted(partitions) {
    const { topics } = this.uncommittedOffsets()

    if (!partitions) {
      return topics
    }

    const wanted = new Map(partitions.map(({ topic, partitions: ps }) => [topic, new Set(ps)]))

    return topics
      .map(({ topic, partitions: entries }) => ({
        topic,
        partitions: entries.filter(({ partition }) => {
          const selected = wanted.get(topic)
          return selected !== undefined && selected.has(Number(partition))
        }),
      }))
      .filter(({ partitions: entries }) => entries.length > 0)
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
   * @param {object} [options]
   * @param {import('../groupProtocol/interface').TopicPartitions[]|null} [options.partitions=null]
   *   restrict the default (uncommitted) selection to these topic-partitions. Applied HERE rather
   *   than by the caller, so a retry re-reads what is uncommitted now instead of replaying a list
   *   built before the last batch resolved. Ignored when `offsets.topics` is given.
   * @param {boolean} [options.auto=false] this is the interval/threshold auto-commit rather than a
   *   deliberate one. A stale member epoch is logged and swallowed for it, because the next
   *   interval carries whatever epoch the heartbeats have installed by then; every other caller
   *   sees the error.
   * @returns {Promise<void>}
   */
  async commitOffsets(offsets = {}, { partitions = null, auto = false } = {}) {
    const { groupId, generationId, memberId, groupInstanceId } = this
    const { topics = this.selectUncommitted(partitions) } = offsets

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
      await this.sendOffsetCommit(payload)
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

      if (e.type === STALE_MEMBER_EPOCH) {
        stampGeneration(e, generationId)

        if (auto) {
          // Java does not even back this off: the epoch is not retriable, and the next interval
          // commit is built from whatever epoch the heartbeats have installed by then.
          // @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java
          this.logger.error('Auto-commit rejected with a stale member epoch; skipping this one', {
            groupId,
            memberId,
            memberEpoch: generationId,
          })

          this.lastCommit = Date.now()
          return
        }

        throw stampGeneration(
          stampOffsetCommit(
            new KafkaJSError(
              {
                message: `OffsetCommit failed with stale member epoch. ${e.message}`,
                type: e.type,
                code: e.code,
                helpUrl: e.helpUrl,
              },
              { retriable: false, cause: e }
            )
          ),
          generationId
        )
      }

      // A commit issued before a rebalance and answered after it is answered about the generation
      // it was sent under, not the one this member holds by then.
      throw stampGeneration(e, generationId)
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

    const { responses: consumerOffsets } = await this.fetchCommittedOffsets(pendingPartitions)

    this.drainPendingTopicResets(consumerOffsets, seekedPartitions)

    // Seed the tracked epoch from what the coordinator has committed, before
    // initializeConsumerOffsets rebuilds these entries as { partition, offset } and drops every
    // other field - for every partition, not only the ones it resolved. An invalid offset here is
    // the "this partition is being reset" signal drainPendingTopicResets just wrote, so its stale
    // epoch is deliberately not seeded. Nor is a position that has moved off the committed offset
    // since this request was sent: one fetcher per broker runs this over the whole assignment, so
    // a reply can land after a record was consumed or a seek applied, and that position owns its
    // epoch. An unknown committed epoch (-1, a commit that carried no epoch) never replaces a
    // known one, even at the committed offset.
    for (const { topic, partitions } of consumerOffsets) {
      const epochs = this.resolvedEpochs[topic]
      if (!epochs) {
        continue
      }

      const positions = this.resolvedOffsets[topic]
      for (const entry of partitions) {
        if (isInvalidOffset(entry.offset) || typeof entry.committedLeaderEpoch !== 'number') {
          continue
        }

        const position = positions && positions[entry.partition]
        if (position != null && position !== entry.offset) {
          continue
        }

        if (entry.committedLeaderEpoch < 0 && epochs[entry.partition] >= 0) {
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
   * One OffsetFetch, carrying this member's identity when the coordinator wants it, and asking for
   * stable offsets only.
   *
   * Under the consumer group protocol the coordinator validates the read against a live member, so
   * the request carries `MemberId` and `MemberEpoch` (OffsetFetch v9). Under the classic protocol -
   * and for every admin read - nothing is sent and the request is byte-identical to what it was.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/resources/common/message/OffsetFetchRequest.json
   *
   * Every request sets `RequireStable` (OffsetFetch v7+; dropped below), under both protocols and
   * whatever the isolation level, as the Java consumer does: a partition with a transactional
   * offset commit still open answers UNSTABLE_OFFSET_COMMIT instead of an offset the transaction
   * may yet roll back (KIP-447).
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
   *
   * Two answers are re-sent until the deadline (`staleEpochTimeout`, on a monotonic clock)
   * passes. UNSTABLE_OFFSET_COMMIT, under both protocols, until the transaction resolves: 100 ms
   * apart at first, doubling to a 1 000 ms ceiling, and each re-send counts as a poll
   * (`markPolled`) so a member waiting on a transaction is not evicted for it. A stale member
   * epoch, under the consumer protocol while the member holds an epoch, 100 ms apart - Java's
   * `isStaleEpochErrorAndValidEpochAvailable`. Under the consumer protocol the membership is
   * re-read from the injected `membership` accessor before EVERY attempt, because it is the
   * heartbeat chain that installs a fresh epoch while this loop is sleeping; re-sending this
   * manager's construction-time snapshot would draw the same answer until the deadline. A stop or
   * a pending rebalance abandons the loop within one interval: the fetch round it runs inside is
   * one that `Runner#stop()` and the rejoin both wait for. Neither error is made retriable in the
   * error table: that would change every other decoder of them, including the admin client's.
   * @private
   * @param {{topic: string, partitions: {partition: number}[]}[]} topics
   * @returns {Promise<{responses: Array}>}
   * @throws {Error} the last error, once the deadline passes, the consumer is stopping, a
   *   rebalance is pending, or - for a stale epoch - the member has no epoch
   */
  async fetchCommittedOffsets(topics) {
    const deadline = performance.now() + this.staleEpochTimeout
    let unstableInterval = RETRY_INTERVAL

    for (;;) {
      if (this.membershipProtocol === 'consumer') {
        this.refreshMembership()
      }

      const coordinator = await this.getCoordinator()
      const memberFields =
        this.membershipProtocol === 'consumer'
          ? { memberId: this.memberId, memberEpoch: this.generationId }
          : undefined

      try {
        return await coordinator.offsetFetch({
          groupId: this.groupId,
          topics,
          ...memberFields,
          requireStable: true,
        })
      } catch (e) {
        const unstable = e.type === UNSTABLE_OFFSET_COMMIT
        const staleEpoch =
          e.type === STALE_MEMBER_EPOCH &&
          this.membershipProtocol === 'consumer' &&
          this.generationId > 0

        if (
          !(unstable || staleEpoch) ||
          this.isStopping() ||
          this.isRebalancePending() ||
          performance.now() >= deadline
        ) {
          throw e
        }

        this.logger.debug(
          unstable
            ? 'OffsetFetch answered while a transactional offset commit is open, re-sending'
            : 'OffsetFetch rejected with a stale member epoch, re-sending',
          {
            groupId: this.groupId,
            memberId: this.memberId,
            memberEpoch: this.generationId,
          }
        )

        await sleep(unstable ? unstableInterval : RETRY_INTERVAL)

        if (this.isStopping() || this.isRebalancePending()) {
          throw e
        }

        if (unstable) {
          unstableInterval = Math.min(unstableInterval * 2, UNSTABLE_RETRY_MAX_INTERVAL)
          this.markPolled()
        }
      }
    }
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
    // Each key is guarded: the committed-offsets object is shared with every other consumer of
    // this group on the same client, so a topic this manager once held can be absent by the time
    // it prunes it.
    const committedOffsets = this.committedOffsets()

    if (committedOffsets[topic]) {
      delete committedOffsets[topic][partition]
    }

    if (this.resolvedOffsets[topic]) {
      delete this.resolvedOffsets[topic][partition]
    }

    if (this.resolvedEpochs[topic]) {
      delete this.resolvedEpochs[topic][partition]
    }
  }

  /**
   * @private
   */
  clearAllOffsets() {
    const committedOffsets = this.committedOffsets()

    for (const topic of this.topics) {
      const partitions = committedOffsets[topic]

      if (!partitions) {
        committedOffsets[topic] = {}
        continue
      }

      for (const partition of this.memberAssignment[topic]) {
        delete partitions[partition]
      }
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
