const Long = require('../../utils/long')
const sleep = require('../../utils/sleep')
const dictionary = require('../../utils/dictionary')
const isInvalidOffset = require('./isInvalidOffset')
const initializeConsumerOffsets = require('./initializeConsumerOffsets')
const OffsetOperations = require('./offsetOperations')
const ResetObligations = require('./resetObligations')
const { KafkaJSError, KafkaJSCommitFailedError, isStaleCoordinator } = require('../../errors')
const {
  stampGeneration,
  stampOffsetCommit,
  stampOwnership,
  learnOwnershipLoss,
  REBALANCE_PATHS,
} = require('../rebalanceErrorContract')
const {
  events: { COMMIT_OFFSETS },
} = require('../instrumentationEvents')

const { keys, assign } = Object
const indexTopics = (topics) => dictionary(topics.map((topic) => [topic, {}]))

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const STALE_MEMBER_EPOCH = 'STALE_MEMBER_EPOCH'
const UNSTABLE_OFFSET_COMMIT = 'UNSTABLE_OFFSET_COMMIT'
const UNKNOWN_TOPIC_ID = 'UNKNOWN_TOPIC_ID'

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

module.exports = class OffsetManager {
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
   * @param {(error: Error) => void} [options.onCommitError=null] raises a current lost commit's rejoin obligation before rejecting its caller
   * @param {import('../groupOwnership')} [options.ownership=null] membership fence for this run; absent in isolated managers
   * @param {OffsetOperations} [options.operations] The ledger that orders this group's writes to
   *                                the committed-offset cache (owned by the `Kafka` instance and
   *                                shared by every participant in the group; a private one by
   *                                default).
   * @param {ResetObligations} [options.obligations] The resets this consumer owes recreated topics
   *                                (owned by the long-lived `Consumer`; shared by reference across
   *                                every `OffsetManager` a session builds).
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
    operations = new OffsetOperations(),
    ownership = null,
    onCommitError = null,
    obligations = new ResetObligations(),
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
    this.operations = operations
    this.ownership = ownership
    this.onCommitError = onCommitError
    this.obligations = obligations
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

    /** @type {Map<string, Map<number, {timestamp: number, ticket: object}>>} */
    this.pendingSeeks = new Map()
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
   *
   * Under the consumer protocol each topic carries its id (see `withTopicIds`). A commit answered
   * `UNKNOWN_TOPIC_ID` refreshes the metadata before the retriable error is rethrown, so the retry
   * reads the topic's current id.
   * @private
   * @param {object} payload the `Broker#offsetCommit` payload
   * @param {{guard?: Function}} [options] an immutable caller identity, checked through transmission and settlement
   * @returns {Promise<object>} the decoded response
   * @throws {Error} whatever the coordinator answered, stamped
   */
  async sendOffsetCommit(payload, { guard = null } = {}) {
    const token = this.ownership?.token()
    const release = this.ownership?.retain(token)
    const check = () => this.ownership?.refusal(token) || guard?.() || null

    try {
      let refusal = check()
      if (refusal) throw refusal
      const coordinator = await this.getCoordinator()
      refusal = check()
      if (refusal) throw refusal
      const request = { ...payload, topics: this.withTopicIds(payload.topics) }
      const result =
        this.ownership || guard
          ? await coordinator.offsetCommit(request, { guard: check })
          : await coordinator.offsetCommit(request)
      refusal = check()
      if (refusal) throw refusal
      return result
    } catch (error) {
      const stamped = stampOwnership(
        stampGeneration(stampOffsetCommit(error), payload.groupGenerationId),
        token
      )
      const lost = learnOwnershipLoss(
        this.ownership,
        stamped,
        REBALANCE_PATHS.OFFSET_COMMIT,
        this.membershipProtocol
      )
      if (lost) this.onCommitError?.(stamped)
      const refusal = check()
      if (refusal)
        throw stampOwnership(
          stampGeneration(stampOffsetCommit(refusal), payload.groupGenerationId),
          token
        )
      if (error.type === UNKNOWN_TOPIC_ID) await this.refreshAfterUnknownTopicId()
      throw stamped
    } finally {
      release?.()
    }
  }

  /**
   * The topic entries of a request, each with its topic's id under the consumer group protocol, so
   * the broker can key the request by id (OffsetCommit and OffsetFetch v10) when it knows every
   * one. The classic protocol and every admin read send names, as they always did. Returns a new
   * list and new entries: the originals are the `COMMIT_OFFSETS` payload a listener receives, which
   * stays as it was. An id the metadata cache has not seen is `undefined`, which keeps the whole
   * request by name.
   * @private
   * @param {{topic: string}[]} topics
   * @returns {object[]}
   */
  withTopicIds(topics) {
    return this.membershipProtocol === 'consumer'
      ? topics.map((entry) => ({ ...entry, topicId: this.cluster.topicId(entry.topic) }))
      : topics
  }

  /**
   * Refreshes the metadata after a request keyed by id was refused, so that the next attempt reads
   * the id the topic has now (or none, which sends by name). Best-effort: a refresh that fails must
   * not replace the retriable error the caller is waiting for.
   * @private
   * @returns {Promise<void>}
   */
  async refreshAfterUnknownTopicId() {
    try {
      await this.cluster.refreshMetadata()
    } catch (_) {
      // see above
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
    this.operations.touchTopic(topic)
    for (const partition of this.pendingSeeks.get(topic)?.keys() || []) {
      this.clearPendingSeek(topic, partition)
    }
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
        this.operations.touch(topic, partition)
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
   * Commits the topic's default offset for one partition, then forgets what this member holds for
   * it, so the next resolution reads the committed offset back. The forgetting is skipped when a
   * newer operation or a barrier (a revoke, a recreation) reached the partition while the commit
   * was in flight.
   * @param {import("../../../types").TopicPartition} topicPartition
   * @returns {Promise<void>}
   */
  async setDefaultOffset({ topic, partition }) {
    const defaultOffset = this.cluster.defaultOffset(this.topicConfigurations[topic])
    await this.commitAndClear({ topic, partition, offset: defaultOffset })
  }

  /**
   * Seeks an assigned partition. A concrete offset retains the existing auto-commit behavior.
   * A -2 or -1 sentinel is a local intent for the next ListOffsets resolution, independent of
   * subscription defaults and auto-commit; it is never sent as a committed offset. An unassigned
   * partition is a no-op. Pending intent ends on concrete publication, cancellation or stop.
   *
   * A seek is a mutation of the offset ledger: it is applied unless a seek or commit that began
   * after it was applied first, or a barrier reached the partition while its commit was in
   * flight. An applied seek settles the topic's reset obligation for the partition.
   *
   * @param {import("../../../types").TopicPartitionOffset} topicPartitionOffset
   * @returns {Promise<boolean>} whether the seek was applied: `false` when the partition is not
   *                              currently assigned to this member, when this member lost its
   *                              membership, or when a newer operation or barrier superseded it
   *                              while its commit was in flight
   * @throws {Error} whatever the offset conversion or the commit raised; the ticket ends either way
   */
  async seek({ topic, partition, offset }) {
    if (this.ownership?.refusal(this.ownership.token())) return false
    if (!this.memberAssignment[topic] || !this.memberAssignment[topic].includes(partition)) {
      return false
    }

    if (offset === '-2' || offset === '-1') {
      const ticket = this.operations.begin([{ topic, partition }], { mutation: true })
      this.clearOffsets({ topic, partition })
      this.operations.apply(ticket, topic, partition)
      let partitions = this.pendingSeeks.get(topic)
      if (!partitions) {
        partitions = new Map()
        this.pendingSeeks.set(topic, partitions)
      }
      partitions.set(partition, { timestamp: Number(offset), ticket })
      return true
    }

    if (!this.autoCommit) {
      const ticket = this.operations.begin([{ topic, partition }], { mutation: true })

      try {
        this.clearPendingSeek(topic, partition)
        this.resolveOffset({
          topic,
          partition,
          offset: Long.fromValue(offset).subtract(1).toString(),
          // A seek names a position, not a record: nothing has been consumed at it yet.
          leaderEpoch: -1,
        })
        this.operations.apply(ticket, topic, partition)
      } finally {
        this.operations.end(ticket)
      }

      return true
    }

    return this.commitAndClear({ topic, partition, offset })
  }

  /**
   * One OffsetCommit for a single partition followed by the clear of its cached offset, position
   * and epoch, under a mutation ticket: the clear is a post-await write, so it is applied only
   * while the ticket is still current. A failed commit applies nothing and supersedes nothing.
   * @private
   * @param {import("../../../types").TopicPartitionOffset} topicPartitionOffset
   * @returns {Promise<boolean>} whether the clear was applied
   * @throws {Error} whatever the commit raised
   */
  async commitAndClear({ topic, partition, offset }) {
    const { groupId, generationId, memberId, groupInstanceId } = this
    const ticket = this.operations.begin([{ topic, partition }], { mutation: true })
    const token = this.ownership?.token()
    const release = this.ownership?.retain(token)
    const guard = this.ownership ? () => this.ownership.refusal(token) : null

    try {
      await this.sendOffsetCommit(
        {
          groupId,
          memberId,
          groupGenerationId: generationId,
          groupInstanceId,
          topics: [{ topic, partitions: [{ partition, offset }] }],
        },
        { guard }
      )

      if (guard?.() || this.operations.isSuperseded(ticket, topic, partition)) {
        return false
      }

      this.clearOffsets({ topic, partition })
      this.operations.apply(ticket, topic, partition)
      return true
    } catch (error) {
      if (error instanceof KafkaJSCommitFailedError) return false
      throw error
    } finally {
      release?.()
      this.operations.end(ticket)
    }
  }

  async commitOffsetsIfNecessary(options) {
    const now = Date.now()

    const timeoutReached =
      this.autoCommitInterval != null && now >= this.lastCommit + this.autoCommitInterval

    const thresholdReached =
      this.autoCommitThreshold != null &&
      this.countResolvedOffsets().gte(Long.fromValue(this.autoCommitThreshold))

    if (timeoutReached || thresholdReached) {
      return this.commitOffsets({}, { ...options, auto: true })
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
   * @param {() => Error|null} [options.guard] immutable caller identity retained across retries
   * @param {boolean} [options.auto=false] this is the interval/threshold auto-commit rather than a
   *   deliberate one. A stale member epoch is logged and swallowed for it, because the next
   *   interval carries whatever epoch the heartbeats have installed by then; every other caller
   *   sees the error.
   * @returns {Promise<void>}
   */
  async commitOffsets(offsets = {}, options = {}) {
    if (!this.ownership && !options.guard) return this.commitWithOwnership(offsets, options)
    const token = this.ownership?.token()
    const release = this.ownership?.retain(token)
    const guard = () => this.ownership?.refusal(token) || options.guard?.() || null
    try {
      return await this.commitWithOwnership(offsets, { ...options, guard })
    } finally {
      release?.()
    }
  }

  /**
   * Completes a commit while its outer operation retains the original membership identity.
   * @private
   * @param {object} offsets the selected offsets
   * @param {{partitions?: object[], auto?: boolean, guard: Function}} options the immutable guard
   * @returns {Promise<void>} automatic refusals resolve without publishing
   * @throws {Error} explicit refusals or the original broker error
   */
  async commitWithOwnership(offsets, { partitions = null, auto = false, guard } = {}) {
    const refusal = this.ownership?.refusal(this.ownership.token()) || guard?.()
    if (refusal) {
      if (auto) {
        this.logger.debug('Skipping a commit without membership ownership')
        return
      }
      throw refusal
    }
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
      await this.sendAndRecordCommit(payload, topics, { guard })
      const refusal = guard?.()
      if (refusal) throw refusal
      this.lastCommit = Date.now()
    } catch (e) {
      if (e instanceof KafkaJSCommitFailedError) {
        if (auto) {
          this.logger.debug('Skipping a commit without membership ownership')
          return
        }
        throw e
      }
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
   * Sends one OffsetCommit and records what the coordinator accepted in the committed cache, cell
   * by cell, under a mutation ticket. A cell that a newer commit or seek, or a barrier, reached
   * while the request was in flight keeps its newer value, whichever answer arrived first. The
   * cache write never moves a resolved position. Emits `COMMIT_OFFSETS` once the request succeeds.
   * @private
   * @param {object} payload the `Broker#offsetCommit` payload
   * @param {{topic: string, partitions: {partition: number|string, offset: string}[]}[]} topics
   * @param {{guard?: Function}} [options] checked again before local publication
   * @returns {Promise<void>}
   * @throws {Error} whatever the commit raised; nothing is recorded then
   */
  async sendAndRecordCommit(payload, topics, options) {
    const ticket = this.operations.begin(this.coveredCells(topics), { mutation: true })

    try {
      await this.sendOffsetCommit(payload, options)
      this.instrumentationEmitter.emit(COMMIT_OFFSETS, payload)
      const refusal = options?.guard?.()
      if (refusal) throw refusal
      this.recordCommitted(ticket, topics)
    } finally {
      this.operations.end(ticket)
    }
  }

  /**
   * Writes the accepted offsets of a commit into the committed cache and applies the ticket to
   * each written cell, which settles the cell's reset obligation for every consumer of the group.
   * @private
   * @param {object} ticket
   * @param {{topic: string, partitions: {partition: number|string, offset: string}[]}[]} topics
   * @returns {void}
   */
  recordCommitted(ticket, topics) {
    const committedOffsets = this.committedOffsets()

    for (const { topic, partitions } of topics) {
      const accepted = {}
      const written = []

      for (const { partition, offset } of partitions) {
        if (!this.operations.isSuperseded(ticket, topic, partition)) {
          accepted[partition] = offset
          written.push(partition)
        }
      }

      if (written.length > 0) {
        committedOffsets[topic] = assign({}, committedOffsets[topic], accepted)
        written.forEach((partition) => this.operations.apply(ticket, topic, partition))
      }
    }
  }

  /**
   * @private
   * @param {{topic: string, partitions: {partition: number|string}[]}[]} topics
   * @returns {{topic: string, partition: number|string}[]} the ledger cells these entries cover
   */
  coveredCells(topics) {
    return topics.flatMap(({ topic, partitions }) =>
      partitions.map(({ partition }) => ({ topic, partition }))
    )
  }

  /**
   * Resolves the partitions of the assignment that hold no valid offset: the group's committed
   * offset, else the topic's own `fromBeginning` choice through ListOffsets. A current explicit
   * sentinel seek overrides the committed answer and supplies a per-partition timestamp.
   *
   * A resolution is a read of the offset ledger: it takes a ticket over the cells it resolves and
   * fills them in one synchronous final step after both awaits. It never supersedes anything. An
   * answer is written only for a cell that no seek, commit or barrier (a revoke, a reset, a
   * recreation) reached since the ticket began and that is still unresolved, so a newer
   * operation's value is never overwritten and a resolver never replaces a sibling's. The leader
   * epochs the coordinator committed are seeded in the same step, under the same check.
   *
   * A topic recreated while this member is subscribed owes a reset on every partition (see
   * `ResetObligations`). The reset is applied here, after the OffsetFetch answer, to the owed
   * partitions: their offset becomes the invalid sentinel, so they resolve through ListOffsets per
   * the topic's `fromBeginning`. Reading an obligation does not consume it: a partition settles
   * only when an offset is written for it or a seek or commit is applied, so a ListOffsets that
   * throws leaves the reset owed.
   *
   * A cell whose answer was dropped keeps its invalid offset and is resolved again, at once by the
   * caller (`ConsumerGroup#seekOffsets`) and at the latest by the next fetch round.
   *
   * @returns {Promise<boolean>} `false` when an answer was dropped because the cell was superseded
   *                              or the topic was recreated while a request was in flight, `true`
   *                              otherwise (a cell a sibling had already resolved is not a drop)
   */
  async resolveOffsets() {
    const invalidOffset = (topic) => (partition) => {
      return (
        this.pendingSeekTimestamp(topic, partition) !== undefined ||
        isInvalidOffset(this.committedOffsets()[topic][partition])
      )
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
      this.settleObligations()
      return true
    }

    const ticket = this.operations.begin(this.coveredCells(pendingPartitions))

    try {
      const requestedIds = new Map(
        pendingPartitions.map(({ topic }) => [topic, this.cluster.topicId(topic)])
      )
      const { responses } = await this.fetchCommittedOffsets(pendingPartitions)

      let complete = true
      const consumerOffsets = []
      for (const { topic, partitions } of responses) {
        const current = partitions.filter(
          ({ partition }) => !this.isStale(ticket, topic, partition, requestedIds)
        )
        complete = complete && current.length === partitions.length
        if (current.length > 0) {
          consumerOffsets.push({ topic, partitions: current })
        }
      }

      this.markOwedResets(consumerOffsets)
      this.markPendingSeeks(consumerOffsets)
      const epochSeeds = this.collectEpochSeeds(consumerOffsets)

      const unresolvedPartitions = consumerOffsets.map(({ topic, partitions }) =>
        assign(
          {
            topic,
            partitions: partitions
              .filter(({ offset }) => isInvalidOffset(offset))
              .map(({ partition }) => {
                const timestamp = this.pendingSeekTimestamp(topic, partition)
                return timestamp === undefined ? { partition } : { partition, timestamp }
              }),
          },
          this.topicConfigurations[topic]
        )
      )

      let offsets = consumerOffsets
      if (unresolvedPartitions.some((t) => t.partitions.length > 0)) {
        const topicOffsets = await this.cluster.fetchTopicsOffset(unresolvedPartitions)
        offsets = initializeConsumerOffsets(consumerOffsets, topicOffsets)
      }

      return this.applyResolution(ticket, offsets, epochSeeds, requestedIds) && complete
    } finally {
      this.operations.end(ticket)
    }
  }

  /**
   * The timestamp of a still-current explicit sentinel seek. A retained mutation ticket lets
   * a newer commit, seek or barrier cancel the intent even between resolution attempts.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @returns {number|undefined} -2 or -1, or undefined when no current seek remains
   * @throws Never.
   */
  pendingSeekTimestamp(topic, partition) {
    const pending = this.pendingSeeks.get(topic)?.get(partition)
    if (!pending) {
      return undefined
    }
    if (
      !this.owns(topic, partition) ||
      this.isStopping() ||
      this.operations.isSuperseded(pending.ticket, topic, partition)
    ) {
      this.clearPendingSeek(topic, partition)
      return undefined
    }
    return pending.timestamp
  }

  /**
   * Makes an explicit sentinel override a coordinator's committed offset for this partition.
   * The concrete ListOffsets result is published under the resolution's existing read ticket.
   * @private
   * @param {{topic: string, partitions: {partition: number, offset: string}[]}[]} entries mutated
   * @returns {void}
   * @throws Never.
   */
  markPendingSeeks(entries) {
    for (const { topic, partitions } of entries) {
      for (const entry of partitions) {
        if (this.pendingSeekTimestamp(topic, entry.partition) !== undefined) {
          entry.offset = '-1'
        }
      }
    }
  }

  /**
   * Releases one sentinel intent and its ledger ticket. Idempotent; does not fence a resolver.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   * @throws Never.
   */
  clearPendingSeek(topic, partition) {
    const partitions = this.pendingSeeks.get(topic)
    const pending = partitions?.get(partition)
    if (!pending) {
      return
    }
    this.operations.end(pending.ticket)
    partitions.delete(partition)
    if (partitions.size === 0) {
      this.pendingSeeks.delete(topic)
    }
  }

  /**
   * Cancels all explicit sentinel seeks and fences their in-flight answers on stop or rebuild.
   * Retains no ledger ticket, topic or partition after returning.
   * @returns {void}
   * @throws Never.
   */
  clearPendingSeeks() {
    for (const [topic, partitions] of this.pendingSeeks) {
      for (const [partition, pending] of partitions) {
        this.operations.touch(topic, partition)
        this.operations.end(pending.ticket)
      }
    }
    this.pendingSeeks.clear()
  }

  /**
   * Whether an answer for a cell no longer describes it: the topic was recreated (its id was known
   * when the request went out and is known now, and the two differ, or a barrier reached the
   * cell) or a newer operation was applied since the ticket began. An id that was merely learned
   * or lost (a cleared cache) is not a recreation.
   * @private
   * @param {object} ticket the resolution's read ticket
   * @param {string} topic
   * @param {number} partition
   * @param {Map<string, string|undefined>} requestedIds each requested topic's cached id when the
   *   request went out
   * @returns {boolean}
   */
  isStale(ticket, topic, partition, requestedIds) {
    const requestedId = requestedIds.get(topic)
    const currentId = this.cluster.topicId(topic)
    const recreated =
      requestedId !== undefined && currentId !== undefined && requestedId !== currentId

    return recreated || this.operations.isSuperseded(ticket, topic, partition)
  }

  /**
   * Turns every owed partition of the answered topics into "no committed offset", so it falls
   * through to ListOffsets. Runs after the OffsetFetch answer, so a reset a sibling settled while
   * the request was in flight is respected. A reset that the cached topic id contradicts is
   * disposed of and never applied (`ResetObligations#current`).
   * @private
   * @param {{topic: string, partitions: {partition: number, offset: string}[]}[]} entries mutated
   *   in place: an owed partition's offset becomes `'-1'`
   * @returns {void}
   */
  markOwedResets(entries) {
    for (const { topic, partitions } of entries) {
      if (this.obligations.current(topic, this.cluster.topicId(topic)) === undefined) {
        continue
      }

      for (const entry of partitions) {
        if (this.obligations.isOwed(topic, entry.partition)) {
          entry.offset = '-1'
        }
      }
    }
  }

  /**
   * The leader epochs the coordinator committed with the valid offsets an answer carries. An
   * offset marked for reset is invalid here, so its stale epoch is deliberately not collected.
   * @private
   * @param {{topic: string, partitions: {partition: number, offset: string,
   *   committedLeaderEpoch?: number}[]}[]} entries
   * @returns {{topic: string, partition: number, offset: string, epoch: number}[]}
   */
  collectEpochSeeds(entries) {
    const seeds = []

    for (const { topic, partitions } of entries) {
      for (const { partition, offset, committedLeaderEpoch } of partitions) {
        if (!isInvalidOffset(offset) && typeof committedLeaderEpoch === 'number') {
          seeds.push({ topic, partition, offset, epoch: committedLeaderEpoch })
        }
      }
    }

    return seeds
  }

  /**
   * The resolution's single final step, synchronous after both awaits: writes each answered cell
   * that is still current and unresolved, discharges the reset it settles, deletes the obligations
   * every assigned partition has discharged, and seeds the consumed leader epochs.
   * @private
   * @param {object} ticket the resolution's read ticket
   * @param {{topic: string, partitions: {partition: number, offset: string}[]}[]} offsets
   * @param {{topic: string, partition: number, offset: string, epoch: number}[]} epochSeeds
   * @param {Map<string, string|undefined>} requestedIds
   * @returns {boolean} `false` when a cell was dropped as stale
   */
  applyResolution(ticket, offsets, epochSeeds, requestedIds) {
    const committedOffsets = this.committedOffsets()
    let complete = true

    for (const { topic, partitions } of offsets) {
      const resolved = {}
      const written = []

      for (const { partition, offset } of partitions) {
        if (this.isStale(ticket, topic, partition, requestedIds)) {
          complete = false
        } else if (
          isInvalidOffset(committedOffsets[topic] && committedOffsets[topic][partition]) ||
          this.pendingSeekTimestamp(topic, partition) !== undefined
        ) {
          resolved[partition] = offset
          written.push(partition)
        }
      }

      committedOffsets[topic] = assign({}, committedOffsets[topic], resolved)

      for (const partition of written) {
        if (!isInvalidOffset(resolved[partition])) {
          this.clearPendingSeek(topic, partition)
        }
        this.obligations.discharge(topic, partition, this.cluster.topicId(topic))
      }

      const assigned = this.assignedPartitions.get(topic)
      if (assigned) {
        this.obligations.settle(topic, assigned)
      }
    }

    this.seedConsumedEpochs(ticket, epochSeeds, requestedIds)
    return complete
  }

  /**
   * Seeds the tracked epoch from what the coordinator committed, for every answered partition and
   * not only the ones this resolution wrote, because `initializeConsumerOffsets` rebuilds the
   * entries as `{ partition, offset }` and drops every other field. The epoch belongs to the
   * offset it was committed with, so it is skipped for a cell that was superseded (the newer
   * operation owns its epoch), for a position that has moved off that offset since the request
   * was sent (one fetcher per broker runs this over the whole assignment, so a reply can land
   * after a record was consumed), for an unset position when the cell holds another offset (a
   * sibling's reset or a commit got there first), and when an unknown committed epoch (-1) would
   * replace a known one.
   * @private
   * @param {object} ticket
   * @param {{topic: string, partition: number, offset: string, epoch: number}[]} seeds
   * @param {Map<string, string|undefined>} requestedIds
   * @returns {void}
   */
  seedConsumedEpochs(ticket, seeds, requestedIds) {
    const committedOffsets = this.committedOffsets()

    for (const { topic, partition, offset, epoch } of seeds) {
      const epochs = this.resolvedEpochs[topic]
      if (!epochs || this.isStale(ticket, topic, partition, requestedIds)) {
        continue
      }

      const position = this.resolvedOffsets[topic] && this.resolvedOffsets[topic][partition]
      const held = committedOffsets[topic] && committedOffsets[topic][partition]
      if (position != null ? position !== offset : held !== offset) {
        continue
      }

      if (epoch < 0 && epochs[partition] >= 0) {
        continue
      }

      epochs[partition] = epoch
    }
  }

  /**
   * Deletes the reset obligations that every partition this member holds has discharged, so a
   * reset another consumer or a confirmed transaction settled leaves no entry behind. A no-op
   * when nothing is owed, which is the steady state.
   * @private
   * @returns {void}
   */
  settleObligations() {
    if (this.obligations.size() === 0) {
      return
    }

    for (const topic of this.topics) {
      this.obligations.settle(topic, this.assignedPartitions.get(topic))
    }
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
          topics: this.withTopicIds(topics),
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
   * @private
   * @param {import("../../../types").TopicPartition} topicPartition
   */
  clearOffsets({ topic, partition }) {
    this.clearPendingSeek(topic, partition)
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
    this.clearPendingSeeks()
    const committedOffsets = this.committedOffsets()

    for (const topic of this.topics) {
      const partitions = committedOffsets[topic]

      if (!partitions) {
        committedOffsets[topic] = {}
      }

      for (const partition of this.memberAssignment[topic]) {
        if (partitions) {
          delete partitions[partition]
        }

        this.operations.touch(topic, partition)
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
