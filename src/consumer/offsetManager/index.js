const CommittedOffsetRevisions = require('../../cluster/committedOffsetRevisions')
const Long = require('../../utils/long')
const sleep = require('../../utils/sleep')
const dictionary = require('../../utils/dictionary')
const isInvalidOffset = require('./isInvalidOffset')
const OffsetOperations = require('./offsetOperations')
const { fencedDelay } = require('../fetchPreparation')
const ResetObligations = require('./resetObligations')
const { ResetResolver } = require('../offsetReset')
const { KafkaJSError, KafkaJSCommitFailedError, isStaleCoordinator } = require('../../errors')
const { isCallDeadline } = require('../groupProtocol/interface')
const {
  stampGeneration,
  stampOffsetCommit,
  stampOwnership,
  stampMembership,
  learnOwnershipLoss,
  REBALANCE_PATHS,
} = require('../rebalanceErrorContract')
const {
  events: { COMMIT_OFFSETS },
} = require('../instrumentationEvents')

const { keys, assign } = Object
const indexTopics = (topics) => dictionary(topics.map((topic) => [topic, {}]))
const NO_REFUSALS = Object.freeze([])

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

/**
 * One member's view of a consumer group's offsets for one assignment: the position each assigned
 * partition fetches from next, its tracked leader epochs, the sentinel seeks still waiting for a
 * boundary, and every write to them and to the group's committed-offset cache.
 *
 * A `ConsumerGroup` builds a manager when it rebuilds the whole assignment and moves the existing
 * one with `updateAssignment` for an incremental (cooperative) change; a stop or a full rebuild
 * retires it (`clearPendingSeeks`). It owns only consumer-local state. The committed-offset cache
 * and the operation ledger are shared by every participant of the group on one `Kafka` instance,
 * and the reset obligations belong to the long-lived `Consumer`; all three are injected.
 *
 * A position a reset policy, a sentinel seek or a local seek derives is this manager's own: it is
 * never written into the shared cache as if committed and no sibling reads it, so members of one
 * group with different policies each fetch from their own answer. Only commits (the coordinator's
 * answer, an auto-commit, a transactional publication) are shared.
 *
 * Writes are ordered through the ledger, not by arrival: `resolveOffsets` is a read that fills
 * unresolved cells from the coordinator or ListOffsets, while `seek`, `setDefaultOffset` and the
 * commits are mutations, and a stale answer never overwrites a newer operation. Every OffsetCommit
 * leaves through one sender that checks the membership before it writes. The kafkajs 2.2.4
 * contract of `seek`, `resolveOffset` and `commitOffsets` is kept apart from the documented
 * behavior changes: explicit sentinel boundaries, ordered offset writes and the refusal of
 * commits after a lost membership.
 */
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
   * @param {string} [options.groupId] absent for a consumer without a group: no coordinator,
   *                                OffsetFetch or OffsetCommit is ever used, and every position is local
   * @param {Record<string, Record<number, string>>} [options.offsets] the private committed-offset
   *                                cache of a consumer without a group; unused when a group is set
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
   * @param {'classic'|'consumer'|'manual'} [options.membershipProtocol='classic'] which group
   *                                protocol this member runs; `'manual'` sends simple-consumer
   *                                identity and never a member id or epoch. Under `'consumer'` the offset fetch carries the
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
   * @param {(topic: string, partition: number, manager: OffsetManager, preparation?: object) => object} [options.captureResetContext=null]
   *                                captures the group's identity for one cell (run, membership,
   *                                assignment, acquisition, clock and topic id) before an offset
   *                                resolution awaits anything; it travels with a `none` refusal so
   *                                the group can tell a current answer from a stale one. Absent in
   *                                isolated managers, whose refusals then carry no context.
   * @param {(fence?: object) => Promise<import("../../../types").Broker>} [options.findCoordinator]
   *                                the membership's single-flight coordinator lookup, shared with
   *                                its heartbeat, joins and other commits. Absent in isolated
   *                                managers, which ask the cluster directly.
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
    offsets,
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
    captureResetContext = null,
    findCoordinator = (fence) =>
      cluster.findGroupCoordinator(fence ? { groupId, fence } : { groupId }),
    logger = SILENT_LOGGER,
  }) {
    this.cluster = cluster
    this.findCoordinator = findCoordinator
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
    this.privateOffsets = offsets
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
    this.captureResetContext = captureResetContext
    this.logger = logger

    this.autoCommit = autoCommit
    this.autoCommitInterval = autoCommitInterval
    this.autoCommitThreshold = autoCommitThreshold
    this.lastCommit = Date.now()

    /** @type {Map<string, Map<number, {timestamp: number, ticket: object}>>} */
    this.pendingSeeks = new Map()
    /**
     * The `none` refusals of the cells this manager holds, keyed by `topic|partition`, until a
     * seek, a revoke, a recreation or the manager's retirement ends them. `taken` marks the ones
     * `takeResetRefusals` already handed out.
     * @type {Map<string, {refusal: object, taken: boolean}>}
     */
    this.resetRefusals = new Map()
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
   * @param {string} [committedOffset] baseline already read by the fetch builder
   * @returns {Long} the committed offset, or 0 when there is none or it is invalid
   */
  nextOffset(topic, partition, committedOffset = this.heldOffsetAt(topic, partition)) {
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
   * @param {{guard?: Function, fence?: import('../../producer/generation')}} [options] an immutable
   *   caller identity, checked through transmission and settlement, and the terminal fence that
   *   binds the coordinator discovery and the metadata refresh a refused topic id starts
   * @returns {Promise<object>} the decoded response
   * @throws {Error} whatever the coordinator answered, stamped; a refusal from the caller's guard
   *   replaces it, and only a public call's expired deadline (`isCallDeadline`) first lets the
   *   original answer's ownership loss be learned for its own token. A retired run or a forced
   *   close publishes nothing
   */
  async sendOffsetCommit(payload, { guard = null, fence } = {}) {
    const token = this.ownership?.token()
    const { revision } = this.membership()
    const release = this.ownership?.retain(token)
    const check = () => this.ownership?.refusal(token) || guard?.() || null

    try {
      let refusal = check()
      if (refusal) throw refusal
      const coordinator = await this.getCoordinator(fence && { guard: check, fence })
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
      const stampRefusal = (refusal) =>
        stampOwnership(
          stampGeneration(stampOffsetCommit(refusal), payload.groupGenerationId),
          token
        )
      const stamped = stampMembership(
        stampOwnership(stampGeneration(stampOffsetCommit(error), payload.groupGenerationId), token),
        revision
      )
      const callerRefusal = guard?.()
      if (!callerRefusal || isCallDeadline(callerRefusal)) {
        const lost = learnOwnershipLoss(
          this.ownership,
          stamped,
          REBALANCE_PATHS.OFFSET_COMMIT,
          this.membershipProtocol
        )
        if (lost) this.onCommitError?.(stamped)
      }
      if (callerRefusal) throw stampRefusal(callerRefusal)
      const refusal = check()
      if (refusal) throw stampRefusal(refusal)
      if (error.type === UNKNOWN_TOPIC_ID) await this.refreshAfterUnknownTopicId(fence)
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
   * @param {import('../../producer/generation')} [fence] the caller's terminal fence: its close
   *   ends the refresh and a late answer installs nothing
   * @returns {Promise<void>}
   */
  async refreshAfterUnknownTopicId(fence) {
    try {
      await this.cluster.refreshMetadata(...(fence ? [{ fence }] : []))
    } catch (_) {
      // see above
    }
  }

  /**
   * @param {{guard: () => Error|null, fence: import('../../producer/generation')}} [preparation]
   *   a caller's identity: its fence binds the discovery's connections, retries and refreshes, and
   *   its guard is checked before the answer is published, so a retired caller installs nothing
   * @returns {Promise<import("../../../types").Broker>}
   * @throws {Error} the preparation's refusal; absent, nothing changes
   */
  async getCoordinator(preparation) {
    if (!this.coordinator?.isConnected()) {
      // findBroker() would resolve the same (stale) node after a failover; ask the cluster
      // which broker currently owns the group instead. Upstream PR #1779.
      const coordinator = await this.findCoordinator(preparation?.fence)
      this.refuse(preparation)
      this.coordinator = coordinator
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
    this.resolvedOffsets[topic][partition] = this.heldOffsetAt(topic, partition)
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
    this.retireResetRefusals(topic)
    for (const partition of this.pendingSeeks.get(topic)?.keys() || []) {
      this.clearPendingSeek(topic, partition)
    }
    CommittedOffsetRevisions.forCache(this.committedOffsets()).clearTopic(topic)
    this.committedOffsets()[topic] = {}
    this.resetPositions[topic] = {}
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
      this.resetPositions[topic] = this.resetPositions[topic] || {}
      this.resolvedOffsets[topic] = this.resolvedOffsets[topic] || {}
      this.resolvedEpochs[topic] = this.resolvedEpochs[topic] || {}
    }

    for (const { topic, partitions } of revoked) {
      for (const partition of partitions) {
        this.clearOffsets({ topic, partition })
        this.retireResetRefusals(topic, partition)
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

    const subtractTopicOffsets = (topic) =>
      keys(this.resolvedOffsets[topic]).map((partition) =>
        subtractOffsets(this.resolvedOffsets[topic][partition], this.heldOffsetAt(topic, partition))
      )

    const offsetsDiff = this.topics.flatMap(subtractTopicOffsets)
    return offsetsDiff.reduce((sum, offset) => sum.add(offset), Long.fromValue(0))
  }

  /**
   * Commits the topic's default offset for one partition, then forgets what this member holds for
   * it, so the next resolution reads the committed offset back. The forgetting is skipped when a
   * newer operation or a barrier (a revoke, a recreation) reached the partition while the commit
   * was in flight. A `by_duration` topic is reset by `resetByDuration` instead: its timestamp is
   * resolved to an actual offset, which is committed only with auto-commit enabled.
   * @param {import("../../../types").TopicPartition} topicPartition
   * @param {{guard: () => Error|null, fence: import('../../producer/generation')}} [preparation]
   *   the caller's immutable preparation identity; see `seek`
   * @returns {Promise<void>}
   */
  async setDefaultOffset({ topic, partition }, preparation) {
    if (this.topicConfigurations[topic].resetPolicy?.kind === 'by_duration') {
      return this.resetByDuration({ topic, partition }, preparation)
    }
    const defaultOffset = this.cluster.defaultOffset(this.topicConfigurations[topic])
    await this.commitAndClear({ topic, partition, offset: defaultOffset }, preparation)
  }

  /**
   * Resolves a duration reset under the mutation intent captured before lookup.
   * Local resets fence older work; only a confirmed concrete commit discharges debt.
   * @private
   * @param {{topic:string,partition:number}} topicPartition current assigned cell
   * @param {{guard: () => Error|null, fence: import('../../producer/generation')}} [preparation]
   *   the caller's round identity: refused before the ticket begins, part of every currentness
   *   check, and handed to both lookups and the commit; absent, behaves as without a round
   * @returns {Promise<boolean>} false when superseded, otherwise actual position installed
   * @throws {Error} lookup or identity-valid commit failure, or the preparation's refusal
   */
  async resetByDuration({ topic, partition }, preparation) {
    this.refuse(preparation)
    const ticket = this.operations.begin([{ topic, partition }], { mutation: true })
    const requestedId = this.cluster.topicId(topic)
    const token = this.ownership?.token()
    const release = this.ownership?.retain(token)
    const current = () => {
      const known = this.cluster.topicId(topic)
      return (
        this.owns(topic, partition) &&
        !this.isStopping() &&
        !preparation?.guard() &&
        !this.ownership?.refusal(token) &&
        !this.operations.isSuperseded(ticket, topic, partition) &&
        !(requestedId !== undefined && known !== undefined && requestedId !== known)
      )
    }
    const guard = () =>
      current() ? null : new KafkaJSCommitFailedError('Offset reset was superseded')
    try {
      if (!current()) return false
      const { offsets } = await new ResetResolver(this.cluster).resolve(
        [{ topic, partitions: [{ partition, offset: '-1' }] }],
        this.topicConfigurations,
        current,
        () => null,
        undefined,
        preparation
      )
      if (!current()) return false
      const actualOffset = offsets[0]?.partitions[0]?.offset
      if (!/^[0-9]+$/.test(actualOffset || ''))
        throw new KafkaJSError('Reset returned no actual offset')
      if (this.autoCommit) {
        const payload = {
          groupId: this.groupId,
          memberId: this.memberId,
          groupGenerationId: this.generationId,
          groupInstanceId: this.groupInstanceId,
          topics: [{ topic, partitions: [{ partition, offset: actualOffset }] }],
        }
        await this.sendOffsetCommit(payload, {
          guard,
          ...(preparation && { fence: preparation.fence }),
        })
        if (!current()) return false
        this.instrumentationEmitter.emit(COMMIT_OFFSETS, payload)
      }
      if (!current()) return false
      this.clearPendingSeek(topic, partition)
      if (this.autoCommit) {
        CommittedOffsetRevisions.forCache(this.committedOffsets()).advance(topic, partition)
        this.committedOffsets()[topic][partition] = actualOffset
        delete this.resetPositions[topic]?.[partition]
      } else this.holdResetPosition(topic, partition, actualOffset)
      this.resolveOffset({
        topic,
        partition,
        offset: Long.fromValue(actualOffset).subtract(1).toString(),
        leaderEpoch: -1,
      })
      const confirmed =
        this.autoCommit && requestedId !== undefined && requestedId === this.cluster.topicId(topic)
      if (confirmed) this.operations.apply(ticket, topic, partition)
      else this.operations.fence(ticket, topic, partition)
      return true
    } finally {
      release?.()
      this.operations.end(ticket)
    }
  }

  /**
   * Refuses work for a caller whose identity was retired or replaced.
   * @private
   * @param {{guard: () => Error|null}} [preparation] absent for an unguarded caller
   * @returns {void}
   * @throws {Error} what the caller's guard returned
   */
  refuse(preparation) {
    const refusal = preparation?.guard()
    if (refusal) throw refusal
  }

  /**
   * Seeks an assigned partition. A concrete offset retains the existing auto-commit behavior.
   * A -2 or -1 sentinel is a local intent for the next ListOffsets resolution, independent of
   * subscription defaults and auto-commit; it is never sent as a committed offset. An unassigned
   * partition is a no-op. Pending intent ends on concrete publication, cancellation or stop.
   *
   * A seek is a mutation of the offset ledger: it is applied unless a seek or commit that began
   * after it was applied first, or a barrier reached the partition while its commit was in
   * flight. A concrete seek without auto-commit changes only the local position. A
   * sentinel seek fences older answers at once; publishing its boundary remains local.
   * Recreation debt settles only after a successful concrete commit. Cancellation before
   * publication (stop or rebuild) leaves the reset owed; a failed
   * ListOffsets leaves both the reset and the pending sentinel for the next resolution.
   *
   * @param {import("../../../types").TopicPartitionOffset} topicPartitionOffset
   * @param {{guard: () => Error|null, fence: import('../../producer/generation')}} [preparation]
   *   a fetch round's immutable identity, checked before the commit is written and after it
   *   settles; absent for an unguarded caller, which behaves as before
   * @returns {Promise<boolean>} whether the seek was applied: `false` when the partition is not
   *                              currently assigned to this member, when this member lost its
   *                              membership, or when a newer operation or barrier superseded it
   *                              while its commit was in flight
   * @throws {Error} whatever the offset conversion or the commit raised, or the preparation's
   *   refusal; the ticket ends either way
   */
  async seek({ topic, partition, offset }, preparation) {
    this.refuse(preparation)
    if (this.ownership?.refusal(this.ownership.token())) return false
    if (!this.memberAssignment[topic] || !this.memberAssignment[topic].includes(partition)) {
      return false
    }

    if (offset === '-2' || offset === '-1') {
      const ticket = this.operations.begin([{ topic, partition }], { mutation: true })
      this.clearOffsets({ topic, partition })
      this.operations.fence(ticket, topic, partition)
      let partitions = this.pendingSeeks.get(topic)
      if (!partitions) {
        partitions = new Map()
        this.pendingSeeks.set(topic, partitions)
      }
      partitions.set(partition, { timestamp: Number(offset), ticket })
      this.retireResetRefusals(topic, partition)
      return true
    }

    if (!this.autoCommit || this.groupId === undefined) {
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
        this.operations.fence(ticket, topic, partition)
      } finally {
        this.operations.end(ticket)
      }

      this.retireResetRefusals(topic, partition)
      return true
    }

    const applied = await this.commitAndClear({ topic, partition, offset }, preparation)
    if (applied) this.retireResetRefusals(topic, partition)
    return applied
  }

  /**
   * One OffsetCommit for a single partition followed by the clear of its cached offset, position
   * and epoch, under a mutation ticket: the clear is a post-await write, so it is applied only
   * while the ticket is still current. A failed commit applies nothing and supersedes nothing. A
   * concrete offset settles a recreation reset only when the topic is provably the incarnation the
   * commit was sent for; a sentinel never does.
   * @private
   * @param {import("../../../types").TopicPartitionOffset} topicPartitionOffset
   * @param {{guard: () => Error|null}} [preparation] the caller's identity, conjoined with this
   *   manager's own ownership guard; its refusal is thrown, not reported as `false`
   * @returns {Promise<boolean>} whether the clear was applied
   * @throws {Error} whatever the commit raised
   */
  async commitAndClear({ topic, partition, offset }, preparation) {
    const { groupId, generationId, memberId, groupInstanceId } = this
    const token = this.ownership?.token()
    const guard =
      this.ownership || preparation
        ? () => preparation?.guard() || this.ownership?.refusal(token) || null
        : null
    this.refuse(preparation)
    const ticket = this.operations.begin([{ topic, partition }], { mutation: true })
    const requestedId = this.cluster.topicId(topic)
    const release = this.ownership?.retain(token)

    try {
      if (groupId !== undefined) {
        await this.sendOffsetCommit(
          {
            groupId,
            memberId,
            groupGenerationId: generationId,
            groupInstanceId,
            topics: [{ topic, partitions: [{ partition, offset }] }],
          },
          { guard, ...(preparation && { fence: preparation.fence }) }
        )
      }

      if (guard?.() || this.operations.isSuperseded(ticket, topic, partition)) {
        return false
      }

      this.clearOffsets({ topic, partition })
      if (
        groupId !== undefined &&
        !isInvalidOffset(offset) &&
        this.isConfirmedIncarnation(topic, requestedId)
      ) {
        this.operations.apply(ticket, topic, partition)
      } else this.operations.fence(ticket, topic, partition)
      return true
    } catch (error) {
      if (error instanceof KafkaJSCommitFailedError) return false
      throw error
    } finally {
      release?.()
      this.operations.end(ticket)
    }
  }

  /**
   * Auto-commits the resolved offsets when the interval has elapsed or the offsets resolved beyond
   * the committed ones add up to the threshold, and does nothing otherwise. Either trigger is
   * optional (a null or undefined interval or threshold never fires) and one is enough.
   *
   * The commit is automatic (`auto: true`): a lost ownership and a stale member epoch are logged
   * and skipped, not thrown. `lastCommit` restarts the interval when the commit succeeded, when
   * there was nothing to commit and when a stale member epoch was skipped, but not when an
   * ownership refusal was.
   * @param {{partitions?: import('../groupProtocol/interface').TopicPartitions[]|null,
   *   guard?: () => Error|null}} [options] forwarded to `commitOffsets`: the topic-partitions to
   *   narrow the selection to, and the caller's identity guard
   * @returns {Promise<void>} settles once the commit did; at once when neither trigger fired
   * @throws {Error} what an automatic commit still raises: a broker error that is neither an
   *   ownership refusal nor a stale member epoch
   */
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
          offset !== this.heldOffsetAt(topic, partition) &&
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
   * @param {import('../../producer/generation')} [options.fence] the caller's terminal fence: its
   *   close ends the coordinator discovery this commit starts
   * @param {boolean} [options.auto=false] this is the interval/threshold auto-commit rather than a
   *   deliberate one. A stale member epoch is logged and swallowed for it, because the next
   *   interval carries whatever epoch the heartbeats have installed by then; every other caller
   *   sees the error.
   * @returns {Promise<void>}
   */
  async commitOffsets(offsets = {}, options = {}) {
    if (this.groupId === undefined) return
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
   * @param {{partitions?: object[], auto?: boolean, guard: Function, fence?: object}} options the
   *   immutable guard and the caller's optional terminal fence
   * @returns {Promise<void>} automatic refusals resolve without publishing
   * @throws {Error} explicit refusals or the original broker error
   */
  async commitWithOwnership(offsets, { partitions = null, auto = false, guard, fence } = {}) {
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
      await this.sendAndRecordCommit(payload, topics, { guard, fence })
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
          this.coordinator = await this.findCoordinator()
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
   * @param {{guard?: Function, fence?: object}} [options] the guard is checked again before local
   *   publication; the fence binds the coordinator discovery
   * @returns {Promise<void>}
   * @throws {Error} whatever the commit raised; nothing is recorded then
   */
  async sendAndRecordCommit(payload, topics, options) {
    const ticket = this.operations.begin(this.coveredCells(topics), { mutation: true })
    const requestedIds = new Map(topics.map(({ topic }) => [topic, this.cluster.topicId(topic)]))

    try {
      await this.sendOffsetCommit(payload, options)
      this.instrumentationEmitter.emit(COMMIT_OFFSETS, payload)
      const refusal = options?.guard?.()
      if (refusal) throw refusal
      this.recordCommitted(ticket, topics, requestedIds)
    } finally {
      this.operations.end(ticket)
    }
  }

  /**
   * Writes the accepted offsets of a commit into the committed cache and applies the ticket to
   * each written cell, which settles the cell's reset obligation for every consumer of the group
   * when the topic is provably the incarnation the commit was sent for. Otherwise the cell is
   * only fenced and the debt stays owed.
   * @private
   * @param {object} ticket
   * @param {{topic: string, partitions: {partition: number|string, offset: string}[]}[]} topics
   * @param {Map<string, string|undefined>} [requestedIds] each topic's id when the commit was sent;
   *   absent, nothing proves the incarnation
   * @returns {void}
   */
  recordCommitted(ticket, topics, requestedIds = new Map()) {
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
        for (const partition of written) {
          CommittedOffsetRevisions.forCache(committedOffsets).advance(topic, partition)
          delete this.resetPositions[topic]?.[partition]
        }
        const confirmed = this.isConfirmedIncarnation(topic, requestedIds.get(topic))
        written.forEach((partition) =>
          confirmed
            ? this.operations.apply(ticket, topic, partition)
            : this.operations.fence(ticket, topic, partition)
        )
      }
    }
  }

  /**
   * Whether a write sent under `requestedId` provably belongs to the topic incarnation known now.
   * An id unknown when the write began, or unknown or different now, proves nothing.
   * @private
   * @param {string} topic
   * @param {string|undefined} requestedId the topic's id when the write was sent
   * @returns {boolean}
   */
  isConfirmedIncarnation(topic, requestedId) {
    return requestedId !== undefined && requestedId === this.cluster.topicId(topic)
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
   * fills them in one synchronous final step after both awaits. It never supersedes anything. Its
   * ticket is owned by this manager, so cancelling this manager's sentinel seeks can fence it
   * without fencing any other participant's. An answer is written only for a cell that no seek,
   * commit or barrier (a revoke, a reset, a recreation) reached since the ticket began and that
   * is still unresolved, so a newer operation's value is never overwritten and a resolver never
   * replaces a sibling's commit. A position the reset policy, a sentinel seek or the none policy's
   * local position supplies stays private to this manager. The leader epochs the coordinator committed are seeded in the same
   * step, under the same check.
   *
   * A topic recreated while this member is subscribed owes a reset on every partition (see
   * `ResetObligations`). The reset is applied here, after the OffsetFetch answer, to the owed
   * partitions: their offset becomes the invalid sentinel, so they resolve through ListOffsets per
   * the topic's `fromBeginning`. Reading an obligation does not consume it: a partition settles
   * only when a concrete new-incarnation commit is confirmed, so local publication, local seek
   * and a failed ListOffsets leave the reset owed.
   *
   * A cell whose answer was dropped keeps its invalid offset and is resolved again, at once by the
   * caller (`ConsumerGroup#seekOffsets`) and at the latest by the next fetch round.
   *
   * Unresolved cells go through `ResetResolver`, which applies each topic's reset policy. A cell
   * the none policy refuses gets no position and no ListOffsets request: its refusal, carrying the
   * identity captured before the first await, is held for `takeResetRefusals` and the cell is
   * skipped until a seek, a revoke, a recreation or this manager's retirement ends it. A refusal
   * is handled, not a dropped answer.
   *
   * @param {{guard: () => Error|null, fence: import('../../producer/generation')}} [preparation]
   *   a fetch round's immutable identity: checked after each await and before each request, reset
   *   marking and publication, so a refused round leaves no trace; absent, nothing changes
   * @returns {Promise<boolean>} `false` when an answer was dropped because the cell was superseded
   *                              or the topic was recreated while a request was in flight, `true`
   *                              otherwise (a cell a sibling had already resolved is not a drop)
   * @throws {Error} the preparation's refusal; the ticket ends either way
   */
  async resolveOffsets(preparation) {
    this.refuse(preparation)
    const invalidOffset = (topic) => (partition) => {
      return (
        !this.resetRefusals.get(topic)?.has(partition) &&
        (this.pendingSeekTimestamp(topic, partition) !== undefined ||
          isInvalidOffset(this.heldOffsetAt(topic, partition)))
      )
    }

    const pendingPartitions = this.topics
      .filter((topic) => this.obligations.current(topic) !== null)
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

    const ticket = this.operations.begin(this.coveredCells(pendingPartitions), { owner: this })

    try {
      const requestedIds = new Map(
        pendingPartitions.map(({ topic }) => [topic, this.cluster.topicId(topic)])
      )
      const contexts = this.captureResetContexts(pendingPartitions, preparation)
      const { responses } = await this.fetchCommittedOffsets(pendingPartitions, preparation)
      this.refuse(preparation)

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
      const derived = this.derivedCells(consumerOffsets)
      this.adoptLocalPositions(consumerOffsets)
      const epochSeeds = this.collectEpochSeeds(consumerOffsets)

      let offsets = consumerOffsets
      if (
        consumerOffsets.some(({ partitions }) => partitions.some((p) => isInvalidOffset(p.offset)))
      ) {
        const current = (topic, partition) =>
          this.owns(topic, partition) &&
          !preparation?.guard() &&
          !this.isStale(ticket, topic, partition, requestedIds)
        const resolved = await new ResetResolver(this.cluster).resolve(
          consumerOffsets,
          this.topicConfigurations,
          current,
          (topic, partition) => contexts?.get(`${topic}|${partition}`) ?? null,
          (topic, partition) => this.pendingSeekTimestamp(topic, partition),
          preparation
        )
        this.refuse(preparation)
        offsets = resolved.offsets.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition, offset }) => ({
            partition,
            offset: isInvalidOffset(offset) ? undefined : offset,
          })),
        }))
        this.holdResetRefusals(resolved.refusals, requestedIds)
        const total = consumerOffsets.reduce((sum, t) => sum + t.partitions.length, 0)
        const handled = offsets.reduce((sum, t) => sum + t.partitions.length, 0)
        complete = complete && handled + resolved.refusals.length === total
      }

      return this.applyResolution(ticket, offsets, epochSeeds, requestedIds, derived) && complete
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
   * Under the none policy, treats an invalid answer for a cell this member already positioned (a
   * local seek without auto-commit) as that position, so the cell is neither refused nor looked up
   * and the committed cache can hold the position the fetch starts from. A cell with no local
   * position is left for the resolver to refuse.
   * @private
   * @param {{topic: string, partitions: {partition: number, offset: string}[]}[]} entries mutated
   *   in place
   * @returns {void}
   */
  adoptLocalPositions(entries) {
    for (const { topic, partitions } of entries) {
      if (this.topicConfigurations[topic]?.resetPolicy?.kind !== 'none') continue
      for (const entry of partitions) {
        const position = this.resolvedOffsetAt(topic, entry.partition)
        if (isInvalidOffset(entry.offset) && position != null && !isInvalidOffset(position)) {
          entry.offset = position
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
   * Ends a pending sentinel whose concrete boundary was just published. Local publication
   * leaves recreation debt owed until a concrete offset is durably committed. A no-op for a cell without a pending
   * sentinel. Only the resolution's final step calls it, after it checked the cell is current, so
   * the boundary never belongs to an earlier incarnation.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   * @throws Never.
   */
  settleSentinel(topic, partition) {
    if (!this.pendingSeeks.get(topic)?.has(partition)) {
      return
    }
    this.clearPendingSeek(topic, partition)
  }

  /**
   * Captures the group's identity for every cell a resolution is about to ask about, before its
   * first await, so an answer is judged by the identity it was asked under. Nothing is allocated
   * for a manager without a capture callback.
   * @private
   * @param {{topic: string, partitions: {partition: number}[]}[]} pendingPartitions
   * @param {object} [preparation] the fetch round's identity, captured with each cell
   * @returns {Map<string, object>|null} the contexts by `topic|partition`, null without a callback
   */
  captureResetContexts(pendingPartitions, preparation) {
    if (!this.captureResetContext) return null
    const contexts = new Map()
    for (const { topic, partitions } of pendingPartitions) {
      for (const { partition } of partitions) {
        contexts.set(
          `${topic}|${partition}`,
          this.captureResetContext(topic, partition, this, preparation)
        )
      }
    }
    return contexts
  }

  /**
   * Keeps the refusals a resolution just produced, and skips their cells until a seek, a revoke,
   * a recreation or this manager's retirement ends them. A refusal is never a position: nothing
   * reaches the committed cache, the resolved offsets or the ledger. Synchronous, so it runs under
   * the identity the resolver last checked.
   * @private
   * @param {{topic: string, partition: number}[]} refusals current refusals from the resolver
   * @param {Map<string, string|undefined>} requestedIds each topic's id when the request went out
   * @returns {void}
   */
  holdResetRefusals(refusals, requestedIds) {
    for (const refusal of refusals) {
      const { topic, partition } = refusal
      const partitions = this.resetRefusals.get(topic) ?? new Map()
      this.resetRefusals.set(topic, partitions)
      partitions.set(partition, { refusal, taken: false, topicId: requestedIds.get(topic) })
    }
  }

  /**
   * Hands out the refusals no caller has taken yet. They stay held (the cells are skipped by later
   * resolutions) until retired.
   * @returns {ReadonlyArray<object>} each `{ topic, partition, offset, reason, error, context }`;
   *   a shared empty array when there is nothing new
   * @throws Never.
   */
  takeResetRefusals() {
    if (this.resetRefusals.size === 0) return NO_REFUSALS
    const fresh = []
    for (const partitions of this.resetRefusals.values()) {
      for (const entry of partitions.values()) {
        if (!entry.taken) {
          entry.taken = true
          fresh.push(entry.refusal)
        }
      }
    }
    return fresh.length > 0 ? fresh : NO_REFUSALS
  }

  /**
   * @param {string} topic
   * @param {number} partition
   * @returns {object|null} the held refusal while this member still owns the partition and the
   *   topic is the incarnation it was raised for; a stale one is dropped
   * @throws Never.
   */
  currentResetRefusal(topic, partition) {
    const entry = this.resetRefusals.get(topic)?.get(partition)
    if (!entry) return null
    const known = this.cluster.topicId(topic)
    if (
      this.owns(topic, partition) &&
      !(entry.topicId !== undefined && known !== undefined && entry.topicId !== known)
    ) {
      return entry.refusal
    }
    this.retireResetRefusals(topic, partition)
    return null
  }

  /**
   * Ends the held refusals of one partition, or of a whole topic, so the cell is resolved again.
   * Called when a seek, a revoke or a recreation reaches it, and by the group for a refusal whose
   * identity is no longer current.
   * @param {string} topic
   * @param {number} [partition] every partition of the topic when absent
   * @returns {void}
   */
  retireResetRefusals(topic, partition) {
    const partitions = this.resetRefusals.get(topic)
    if (!partitions) return
    if (partition === undefined) partitions.clear()
    else partitions.delete(partition)
    if (partitions.size === 0) this.resetRefusals.delete(topic)
  }

  /**
   * The partitions the metadata cache holds for a topic, for the reset debt deletion guard.
   * @private
   * @param {string} topic
   * @returns {number[]|undefined} undefined when the cluster has no metadata to offer (an isolated
   *   manager), an empty list when it has none for this topic
   */
  knownPartitions(topic) {
    if (typeof this.cluster.findTopicPartitionMetadata !== 'function') return undefined
    try {
      return this.cluster.findTopicPartitionMetadata(topic).map(({ partitionId }) => partitionId)
    } catch {
      return []
    }
  }

  /**
   * Cancels all explicit sentinel seeks on stop or rebuild and fences this manager's own in-flight
   * resolutions of their cells, so a held boundary is never published for a cancelled intent. The
   * fence is an owner barrier: no other participant's ticket is affected. A cancelled sentinel
   * settles no reset. Also ends the held `none` refusals, which belong to this manager's run.
   * Retains no ledger ticket, topic or partition after returning.
   * @returns {void}
   * @throws Never.
   */
  clearPendingSeeks() {
    for (const [topic, partitions] of this.pendingSeeks) {
      for (const [partition, pending] of partitions) {
        this.operations.touchOwned(this, topic, partition)
        this.operations.end(pending.ticket)
      }
    }
    this.pendingSeeks.clear()
    this.resetRefusals.clear()
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
   * that is still current and unresolved, retains recreation debt until a concrete commit,
   * deletes obligations every assigned partition has durably discharged, and seeds epochs.
   * @private
   * @param {object} ticket the resolution's read ticket
   * @param {{topic: string, partitions: {partition: number, offset: string}[]}[]} offsets
   * @param {{topic: string, partition: number, offset: string, epoch: number}[]} epochSeeds
   * @param {Map<string, string|undefined>} requestedIds
   * @param {Set<string>} [derived] the `topic|partition` cells whose answer comes from a reset
   *   policy, a sentinel seek or a local position rather than from a commit. Those stay private to
   *   this manager (`holdResetPosition`); only a commit the coordinator answered reaches the
   *   group's shared cache. Defaults to none.
   * @returns {boolean} `false` when a cell was dropped as stale
   */
  applyResolution(ticket, offsets, epochSeeds, requestedIds, derived = new Set()) {
    const committedOffsets = this.committedOffsets()
    let complete = true

    for (const { topic, partitions } of offsets) {
      const resolved = {}
      const written = []

      for (const { partition, offset } of partitions) {
        if (this.isStale(ticket, topic, partition, requestedIds)) {
          complete = false
        } else if (
          isInvalidOffset(this.heldOffsetAt(topic, partition)) ||
          this.pendingSeekTimestamp(topic, partition) !== undefined
        ) {
          if (derived.has(`${topic}|${partition}`)) this.holdResetPosition(topic, partition, offset)
          else {
            CommittedOffsetRevisions.forCache(committedOffsets).advance(topic, partition)
            resolved[partition] = offset
          }
          written.push(partition)
        }
      }

      committedOffsets[topic] = assign({}, committedOffsets[topic], resolved)

      for (const partition of written) {
        if (!isInvalidOffset(this.heldOffsetAt(topic, partition))) {
          this.settleSentinel(topic, partition)
        }
      }

      const assigned = this.assignedPartitions.get(topic)
      if (assigned) {
        this.obligations.settle(topic, assigned, this.knownPartitions(topic))
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
    for (const { topic, partition, offset, epoch } of seeds) {
      const epochs = this.resolvedEpochs[topic]
      if (!epochs || this.isStale(ticket, topic, partition, requestedIds)) {
        continue
      }

      const position = this.resolvedOffsets[topic] && this.resolvedOffsets[topic][partition]
      const held = this.heldOffsetAt(topic, partition)
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
      this.obligations.settle(
        topic,
        this.assignedPartitions.get(topic),
        this.knownPartitions(topic)
      )
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
   * @param {{guard: () => Error|null, fence: import('../../producer/generation')}} [preparation]
   *   a fetch round's identity: checked before every attempt and after every await, forwarded to
   *   the request, and the re-send delay is bound to its fence so cancelling clears the timer
   * @returns {Promise<{responses: Array}>}
   * @throws {Error} the last error, once the deadline passes, the consumer is stopping, a
   *   rebalance is pending, or - for a stale epoch - the member has no epoch; or the
   *   preparation's refusal
   */
  async fetchCommittedOffsets(topics, preparation) {
    if (this.groupId === undefined) {
      this.refuse(preparation)
      return {
        responses: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            offset: '-1',
            committedLeaderEpoch: -1,
            metadata: null,
            errorCode: 0,
          })),
        })),
      }
    }

    const deadline = performance.now() + this.staleEpochTimeout
    let unstableInterval = RETRY_INTERVAL

    for (;;) {
      this.refuse(preparation)
      if (this.membershipProtocol === 'consumer') {
        this.refreshMembership()
      }

      const coordinator = await this.getCoordinator(preparation)
      this.refuse(preparation)
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
          ...(preparation && { guard: preparation.guard }),
        })
      } catch (e) {
        this.refuse(preparation)
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

        const interval = unstable ? unstableInterval : RETRY_INTERVAL
        await (preparation ? fencedDelay(preparation.fence, interval) : sleep(interval))
        this.refuse(preparation)

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

    CommittedOffsetRevisions.forCache(committedOffsets).advance(topic, partition)
    if (committedOffsets[topic]) {
      delete committedOffsets[topic][partition]
    }

    if (this.resetPositions[topic]) {
      delete this.resetPositions[topic][partition]
    }

    if (this.resolvedOffsets[topic]) {
      delete this.resolvedOffsets[topic][partition]
    }

    if (this.resolvedEpochs[topic]) {
      delete this.resolvedEpochs[topic][partition]
    }
  }

  /**
   * Forgets everything this member holds for its assignment. It still deletes the assigned cells
   * of the shared cache: a rebuilt assignment must read the coordinator again rather than resume
   * from a cell written under the previous one, and only commits live there, so a sibling loses at
   * most a cell it re-reads from the coordinator. Clearing a cell advances its revision, so this
   * member's reset positions for it go stale and are resolved again, as they were when they lived in
   * the cell.
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
        CommittedOffsetRevisions.forCache(committedOffsets).advance(topic, partition)
        if (partitions) {
          delete partitions[partition]
        }

        this.operations.touch(topic, partition)
      }
    }

    this.resolvedOffsets = indexTopics(this.topics)

    /**
     * The positions this member's own reset policy, sentinel seek or local seek gave a cell, which
     * no other consumer of the group may read. See `holdResetPosition`.
     * @type {{[topic: string]: {[partition: number]: {offset: string|undefined, revision: number}}}}
     */
    this.resetPositions = indexTopics(this.topics)

    /**
     * The leader epoch of the last record consumed on each partition, beside its resolved offset.
     * -1 means unknown, which disables position validation for that partition until it consumes a
     * record again. Cleared with the offset by every path that clears an offset.
     * @type {{[topic: string]: {[partition: number]: number}}}
     */
    this.resolvedEpochs = indexTopics(this.topics)
  }

  /**
   * The group's committed-offset cache: a topic to partition to offset-string dictionary.
   *
   * Looked up from the cluster on first use and kept by reference, so every participant of the
   * group on one `Kafka` instance (other consumers, a producer's transactional publication) reads
   * and writes the same object, with its writes ordered through the operation ledger. A manager
   * without a `groupId` uses the private cache it was given, or an empty one.
   * @returns {{[topic: string]: {[partition: number|string]: string}}}
   */
  committedOffsets() {
    if (!this[PRIVATE.COMMITTED_OFFSETS]) {
      this[PRIVATE.COMMITTED_OFFSETS] =
        this.groupId === undefined
          ? this.privateOffsets || dictionary()
          : this.cluster.committedOffsets({ groupId: this.groupId })
    }

    return this[PRIVATE.COMMITTED_OFFSETS]
  }

  /**
   * The offset this member treats as committed for a cell: the position its own reset gave it
   * (`holdResetPosition`) while the shared cell revision is unchanged, else what the group's shared
   * cache holds. A commit any participant writes to the shared cell afterwards (an auto-commit, a
   * transactional publication) therefore takes over, as it did when the reset itself was written
   * there. Every read of the fetch position's baseline goes through here; writes of real commits
   * still go to `committedOffsets()`.
   * @param {string} topic
   * @param {number|string} partition
   * @returns {string|undefined} the offset string, or undefined (or an invalid offset) when none
   */
  heldOffsetAt(topic, partition) {
    const shared = this.committedOffsets()[topic]?.[partition]
    const own = this.resetPositions[topic]?.[partition]
    if (own !== undefined) {
      const revisions = CommittedOffsetRevisions.forCache(this.committedOffsets())
      if (revisions.isCurrent(topic, partition, own.revision)) return own.offset
      delete this.resetPositions[topic][partition]
    }
    return shared
  }

  /**
   * Keeps a position derived from a reset policy, a sentinel seek or a local seek in this manager
   * only, with the shared cell's revision at that moment so a later commit in the shared cell can be
   * told apart from it. A sibling consumer of the same group never reads it, so two members with
   * different reset policies each fetch from their own answer. Replaced by the next one, and
   * dropped with the cell by `clearOffsets`, `resetOffsetsForTopic` and this manager's own commit.
   * @private
   * @param {string} topic
   * @param {number|string} partition
   * @param {string|undefined} offset the derived position; undefined when it could not be resolved
   * @returns {void}
   */
  holdResetPosition(topic, partition, offset) {
    this.resetPositions[topic] ??= {}
    this.resetPositions[topic][partition] = {
      offset,
      revision: CommittedOffsetRevisions.forCache(this.committedOffsets()).capture(),
    }
  }

  /**
   * The cells of an OffsetFetch answer that carry no concrete committed offset (none, or marked
   * for a reset or a sentinel seek), whose position will come from this member alone.
   * @private
   * @param {{topic: string, partitions: {partition: number, offset: string}[]}[]} entries
   * @returns {Set<string>} `topic|partition` keys
   */
  derivedCells(entries) {
    const cells = new Set()
    for (const { topic, partitions } of entries) {
      for (const { partition, offset } of partitions) {
        if (isInvalidOffset(offset)) cells.add(`${topic}|${partition}`)
      }
    }
    return cells
  }
}
