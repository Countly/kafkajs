const sleep = require('../utils/sleep')
const websiteUrl = require('../utils/websiteUrl')
const arrayDiff = require('../utils/arrayDiff')
const createRetry = require('../retry')

const OffsetManager = require('./offsetManager')
const OffsetOperations = require('./offsetManager/offsetOperations')
const GroupOwnership = require('./groupOwnership')
const ResetObligations = require('./offsetManager/resetObligations')
const Batch = require('./batch')
const SeekOffsets = require('./seekOffsets')
const SubscriptionState = require('./subscriptionState')
const PositionValidator = require('./positionValidator')
const {
  events: { CONNECT, RECEIVED_UNSUBSCRIBED_TOPICS, LOG_TRUNCATION },
} = require('./instrumentationEvents')
const {
  KafkaJSError,
  KafkaJSStaleTopicMetadataAssignment,
  isRebalancing,
  isLostMember,
  isFatalMembership,
  isFencedInstance,
} = require('../errors')
const { failure, createErrorFromCode } = require('../protocol/error')
const {
  partitionFetchAction,
  topLevelFetchAction,
  fetchedPartitionDropReason,
} = require('./fetchErrorContract')
const apiKeys = require('../protocol/requests/apiKeys')
const { requests } = require('../protocol/requests')
const ClassicGroupProtocol = require('./groupProtocol/classic')
const ConsumerGroupProtocol = require('./groupProtocol/consumer')
const {
  sentGeneration,
  stampOwnership,
  obsoleteOwnership,
  REBALANCE_PATHS,
} = require('./rebalanceErrorContract')
const { EAGER } = require('./groupProtocol/interface')

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const STALE_MEMBER_EPOCH = 'STALE_MEMBER_EPOCH'

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const LEADER_NOT_AVAILABLE = 5

/** How often the pre-release commit re-checks whether the heartbeats have installed a new epoch. */
const STALE_EPOCH_POLL_INTERVAL = 100

// One warning once a partition has failed five rounds running, then one every twenty-five.
const FETCH_ERROR_WARN_THRESHOLD = 5
const FETCH_ERROR_WARN_INTERVAL = 25

const { keys } = Object

const STALE_METADATA_ERRORS = [
  'LEADER_NOT_AVAILABLE',
  // Fetch before v9 uses NOT_LEADER_FOR_PARTITION
  'NOT_LEADER_FOR_PARTITION',
  // Fetch after v9 uses {FENCED,UNKNOWN}_LEADER_EPOCH
  'FENCED_LEADER_EPOCH',
  'UNKNOWN_LEADER_EPOCH',
  'UNKNOWN_TOPIC_OR_PARTITION',
  // Fetch/Metadata topic-id routing errors: the topic id in the request no longer matches a topic
  'UNKNOWN_TOPIC_ID',
  // the broker's topic id for a partition disagrees with what the client sent
  'INCONSISTENT_TOPIC_ID',
  // a fetch session was built against a topic id the broker no longer recognises
  'FETCH_SESSION_TOPIC_ID_ERROR',
]

// The last Fetch version that keys topics by name. A request whose topics are not all known by id
// is capped here rather than being split across two versions.
// @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
const FETCH_TOPIC_NAMES_MAX_VERSION = 12

/**
 * How many times one `seekOffsets()` call resolves offsets again after a resolution dropped an
 * answer because a competing seek, commit or recreation reached the cell meanwhile.
 */
const MAX_RESOLUTION_PASSES = 3

module.exports = class ConsumerGroup {
  /**
   * @param {object} options
   * @param {import('../../types').RetryOptions} options.retry
   * @param {import('../../types').Cluster} options.cluster
   * @param {string} options.groupId
   * @param {string[]} options.topics
   * @param {Record<string, { fromBeginning?: boolean }>} options.topicConfigurations
   * @param {import('../../types').Logger} options.logger
   * @param {import('../instrumentation/emitter')} options.instrumentationEmitter
   * @param {import('../../types').Assigner[]} options.assigners
   * @param {number} options.sessionTimeout
   * @param {number} options.rebalanceTimeout
   * @param {number} options.maxBytesPerPartition
   * @param {number} options.minBytes
   * @param {number} options.maxBytes
   * @param {number} options.maxWaitTimeInMs
   * @param {boolean} options.autoCommit
   * @param {number} options.autoCommitInterval
   * @param {number} options.autoCommitThreshold
   * @param {number} options.isolationLevel
   * @param {string} options.rackId
   * @param {boolean} options.checkCrcs verify the CRC32C checksum of every record batch received
   *                                (Java's `check.crcs`); a mismatch is a non-retriable error
   * @param {import('./groupProtocol/interface').RebalanceProtocol} [options.rebalanceProtocol]
   *                                the mode chosen from the configured assigners; defaults to
   *                                eager, the protocol every assigner supports.
   * @param {string} [options.groupInstanceId=null] Static membership id (KIP-345); sent on
   *                                JoinGroup/SyncGroup/Heartbeat/LeaveGroup so the coordinator
   *                                recognizes this member across restarts.
   * @param {number} options.metadataMaxAge
   * @param {OffsetOperations} [options.operations] The ledger that orders this group's writes to
   *                                the committed-offset cache (owned by the `Kafka` instance and
   *                                shared by every participant in the group; a private one by
   *                                default). Passed to every `OffsetManager` this group builds.
   * @param {ResetObligations} [options.obligations] The resets the consumer owes recreated topics
   *                                (owned by the long-lived `Consumer`, which survives the crash
   *                                restarts that rebuild this group). Linked to the ledger here and
   *                                passed to every `OffsetManager` this group builds.
   * @param {'classic'|'consumer'} [options.groupProtocol='classic'] which group membership protocol
   *                                to run. `'consumer'` is the broker-driven KIP-848 protocol.
   * @param {string|null} [options.groupRemoteAssignor=null] the server-side assignor name to ask
   *                                for; `null` lets the coordinator pick its first configured one.
   *                                Consumer protocol only.
   * @param {string|null} [options.subscribedTopicRegex=null] an RE2 pattern sent to the coordinator
   *                                verbatim; consumer protocol only.
   * @param {{ fromBeginning: boolean }|null} [options.regexSubscription=null] the options the regex
   *                                subscription was made with, copied to a name-keyed
   *                                `topicConfigurations` entry for every name the regex resolves to.
   * @param {0|1} [options.maxPrefetchBatches=0] reserved for a fetch layer that runs one Fetch
   *                                ahead of the workers. Accepted and stored; inert in this
   *                                release, and read by nothing.
   * @param {number} [options.heartbeatInterval=3000] the configured heartbeat cadence. Forwarded to
   *                                the classic membership, which owns the interval; under the
   *                                consumer protocol the coordinator owns it and this is ignored.
   */
  constructor({
    retry,
    cluster,
    groupId,
    topics,
    topicConfigurations,
    logger,
    instrumentationEmitter,
    assigners,
    sessionTimeout,
    rebalanceTimeout,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    autoCommit,
    autoCommitInterval,
    autoCommitThreshold,
    isolationLevel,
    rackId,
    checkCrcs,
    groupInstanceId = null,
    rebalanceProtocol = EAGER,
    metadataMaxAge,
    operations = new OffsetOperations(),
    obligations = new ResetObligations(),
    groupProtocol = 'classic',
    groupRemoteAssignor = null,
    subscribedTopicRegex = null,
    regexSubscription = null,
    maxPrefetchBatches = 0,
    heartbeatInterval = 3000,
  }) {
    /** @type {import("../../types").Cluster} */
    this.cluster = cluster
    this.groupId = groupId
    this.topics = topics
    this.topicsSubscribed = topics
    this.topicConfigurations = topicConfigurations
    this.logger = logger.namespace('ConsumerGroup')
    this.instrumentationEmitter = instrumentationEmitter
    this.retrier = createRetry(Object.assign({}, retry))
    this.maxBytesPerPartition = maxBytesPerPartition
    this.minBytes = minBytes
    this.maxBytes = maxBytes
    this.maxWaitTime = maxWaitTimeInMs
    this.autoCommit = autoCommit
    this.autoCommitInterval = autoCommitInterval
    this.autoCommitThreshold = autoCommitThreshold
    this.isolationLevel = isolationLevel
    this.rackId = rackId
    this.checkCrcs = checkCrcs
    this.metadataMaxAge = metadataMaxAge
    this.operations = operations
    this.ownership = new GroupOwnership()
    this.acquired = new Map()
    this.acquireClock = 0
    this.obligations = obligations
    this.obligations.attachTo(this.operations)

    this.rebalanceTimeout = rebalanceTimeout
    this.subscribedTopicRegex = subscribedTopicRegex
    this.regexSubscription = regexSubscription
    this.maxPrefetchBatches = maxPrefetchBatches

    const membershipOptions = {
      ownership: this.ownership,
      cluster,
      groupId,
      groupInstanceId,
      rebalanceTimeout,
      rackId,
      instrumentationEmitter,
      // `this.logger`, already namespaced, rather than the raw `logger`: the protocol logs through
      // the same object the group exposes, so anything observing the group's logger keeps working.
      logger: this.logger,
    }

    /** @type {import('./groupProtocol/interface').GroupProtocol} */
    this.groupMembership =
      groupProtocol === 'consumer'
        ? new ConsumerGroupProtocol({
            ...membershipOptions,
            // The protocol calls it `serverAssignor` because that is the wire field's name; this
            // layer calls it `groupRemoteAssignor` because that is the public option's name.
            serverAssignor: groupRemoteAssignor,
            topics: this.topics,
            subscribedTopicRegex,
            metadataMaxAge,
            retry,
          })
        : new ClassicGroupProtocol({
            ...membershipOptions,
            sessionTimeout,
            heartbeatInterval,
            assigners,
            mode: rebalanceProtocol,
          })

    this.seekOffset = new SeekOffsets()

    this.partitionsPerSubscribedTopic = null
    /**
     * Preferred read replica per topic and partition
     *
     * Each of the partitions tracks the preferred read replica (`nodeId`) and a timestamp
     * until when that preference is valid.
     *
     * @type {{[topicName: string]: {[partition: number]: {nodeId: number, expireAt: number}}}}
     */
    this.preferredReadReplicasPerTopicPartition = {}
    /**
     * Consecutive fetch rounds in which a partition produced an error instead of a batch, keyed by
     * `topic|partition`. Reset on that partition's next delivered batch, and wholesale at sync -
     * nothing enumerates "partitions that left the assignment", so the sync reset is what bounds
     * it. Its only purpose is to make a partition that is quietly making no progress audible.
     * @type {Map<string, number>}
     */
    this.consecutiveFetchErrors = new Map()

    /** @type {PositionValidator} */
    this.positionValidator = new PositionValidator({ logger: this.logger })

    /**
     * Brokers already logged as unable to serve position validation, so the notice is one per
     * broker per assignment rather than one per round. Cleared at sync with the validator.
     * @type {Set<number>}
     */
    this.validationUnusableLogged = new Set()

    /**
     * Position validation gets its own bounded retrier rather than the group's: the group's budget
     * comes from the user's `retry` options and could block a whole node's fetch loop while a
     * partition that is merely unvalidated could have been fetched.
     */
    this.validationRetrier = createRetry({ retries: 2, maxRetryTime: 5000 })

    this.offsetManager = null
    this.subscriptionState = new SubscriptionState()

    /** @type {Object|null} set by `attach`, absent until a `Runner` exists */
    this.rebalanceCoordinator = null

    /**
     * Bumped every time the assignment is applied in a way that replaces the offsets, so a batch
     * fetched under the previous one can be recognised and dropped instead of re-delivered.
     * @type {number}
     */
    this.assignmentEpoch = 0

    /**
     * Set while a stale assignment has been reported and the rejoin it asked for has not settled,
     * so the report is made once per pending period rather than once per fetch round.
     */
    this.staleAssignmentPending = false
  }

  /**
   * Late-binds the collaborators the `Runner` owns. The group is constructed before the runner
   * that builds them, so they cannot arrive through this object's constructor.
   *
   * The membership is also given the pre-release commit, because a leave at epoch > 0 has to
   * commit before it gives its partitions up and the membership cannot reach an offset manager.
   * @param {{ rebalanceCoordinator: Object, heartbeatManager: Object }} deps
   * @returns {void}
   */
  attach({ rebalanceCoordinator, heartbeatManager }) {
    this.rebalanceCoordinator = rebalanceCoordinator
    this.groupMembership.attach({
      rebalanceCoordinator,
      heartbeatManager,
      commitBeforeRelease: (partitions) => this.commitBeforeRelease(partitions),
    })
  }

  /**
   * Drops this member's generation and member id without telling the coordinator. Used after a
   * poll-interval overrun, where the LeaveGroup has already been sent (or deliberately not sent,
   * for a static member) and the next round must take the lost-membership branch.
   *
   * Bumps `assignmentEpoch` too: the assignment was held under the membership being dropped, so
   * every batch fetched under it is stale and must neither be committed nor heartbeated for. The
   * next `applyRebalanceResult` bumps again, so staleness can never stick.
   * @returns {void}
   */
  resetGenerationLocally() {
    this.groupMembership.resetGeneration({ clearMemberId: true })
    this.assignmentEpoch += 1
  }

  /**
   * Stops advertising ownership of anything, without touching the generation or the live
   * assignment. Java's `onJoinPrepare` does the same (`assignFromSubscribed(emptySet)`) before a
   * round that gives everything up: the JoinGroup then carries no `OwnedPartitions`, so no peer
   * withholds a partition on this member's behalf and the round reports the whole previous
   * assignment as revoked rather than a delta against a set it no longer holds.
   * @returns {void}
   */
  forgetOwnedPartitions() {
    this.groupMembership.setOwned([])
  }

  /**
   * Re-arms stale-assignment detection.
   * @returns {void}
   */
  clearStaleAssignmentPending() {
    this.staleAssignmentPending = false
  }

  get memberId() {
    return this.groupMembership.memberId
  }

  set memberId(value) {
    this.groupMembership.memberId = value
  }

  get generationId() {
    return this.groupMembership.generationId
  }

  set generationId(value) {
    this.groupMembership.generationId = value
  }

  get leaderId() {
    return this.groupMembership.leaderId
  }

  set leaderId(value) {
    this.groupMembership.leaderId = value
  }

  get members() {
    return this.groupMembership.members
  }

  set members(value) {
    this.groupMembership.members = value
  }

  get groupProtocol() {
    return this.groupMembership.groupProtocol
  }

  set groupProtocol(value) {
    this.groupMembership.groupProtocol = value
  }

  get groupInstanceId() {
    return this.groupMembership.groupInstanceId
  }

  set groupInstanceId(value) {
    this.groupMembership.groupInstanceId = value
  }

  get coordinator() {
    return this.groupMembership.coordinator
  }

  set coordinator(value) {
    this.groupMembership.coordinator = value
  }

  get lastRequest() {
    return this.groupMembership.lastRequest
  }

  set lastRequest(value) {
    this.groupMembership.lastRequest = value
  }

  /**
   * Which membership protocol is running. Read by the coordinator's follow-up condition and stale-
   * verdict guard, by the runner's stop and heartbeat paths, and by the error contract - none of
   * which can reach the protocol object.
   * @returns {'classic'|'consumer'}
   */
  get kind() {
    return this.groupMembership.kind
  }

  /**
   * How many joins the coordinator has accepted: a count that only goes up, so two reads differ
   * exactly when a join was accepted in between, whatever generation, epoch or member id it
   * returned.
   * @returns {number}
   */
  get joinCount() {
    return this.groupMembership.joinCount
  }

  /** @returns {number|null} the current member epoch, null under the classic protocol */
  get memberEpoch() {
    return this.groupMembership.memberEpoch ?? null
  }

  /**
   * The delay the heartbeat chain should use next. Configured under classic; the coordinator's
   * `HeartbeatIntervalMs`, or a pending backoff's remainder, under the consumer protocol.
   * @returns {number} milliseconds
   */
  heartbeatInterval() {
    return this.groupMembership.heartbeatInterval()
  }

  /**
   * Whether the membership holds a target assignment that still differs from what this member
   * owns. Always false under classic. Unresolved topic ids deliberately do NOT make it true - an
   * id that can never resolve would otherwise spin the rebalance chain into its convergence guard.
   * @returns {boolean}
   */
  needsReconciliation() {
    return this.groupMembership.needsReconciliation()
  }

  /**
   * Routes an error raised on the commit or fetch path into the membership state machine, so a
   * fencing learned somewhere other than a heartbeat still leaves the member where the recovery
   * round expects it. Idempotent; a no-op under classic.
   * @param {Error} error
   * @returns {void}
   */
  onMembershipError(error) {
    return this.groupMembership.onMembershipError(error)
  }

  /**
   * Tells the membership the consumer is shutting down, so a join waiting on the coordinator is
   * rejected rather than waiting for a heartbeat chain that is about to be cancelled. Cancels
   * queued sentinel seeks and pending sentinel resolutions for either group protocol.
   * @returns {void}
   */
  onStopRequested() {
    for (const [key, offset] of this.seekOffset) {
      if (offset === '-2' || offset === '-1') {
        this.seekOffset.delete(key)
      }
    }
    this.offsetManager?.clearPendingSeeks()
    return this.groupMembership.onStopRequested()
  }

  /**
   * The poll-expiry variant of `leave()`: no commit, no callbacks, response ignored. A no-op under
   * classic, whose poll-expiry path uses `leave()` or sends nothing at all.
   * @returns {Promise<void>}
   */
  async leaveForPollExpiry() {
    return this.groupMembership.leaveForPollExpiry()
  }

  /**
   * The offset this partition has resolved to, as the decimal string the offset manager stores.
   * A pure read: no seeding, no allocation, and `undefined` before an offset manager exists.
   * @param {string} topic
   * @param {number} partition
   * @returns {string|undefined}
   */
  resolvedOffsetAt(topic, partition) {
    return this.offsetManager ? this.offsetManager.resolvedOffsetAt(topic, partition) : undefined
  }

  isLeader() {
    return this.groupMembership.isLeader()
  }

  /** @returns {import('./groupProtocol/interface').RebalanceProtocol} */
  rebalanceProtocol() {
    return this.groupMembership.rebalanceProtocol()
  }

  /**
   * Warns once when static membership is configured but the negotiated JoinGroup version cannot
   * carry it. Kept on the group because callers reach for it here.
   * @returns {void}
   */
  warnIfStaticMembershipInactive() {
    return this.groupMembership.warnIfStaticMembershipInactive()
  }

  /**
   * Leaves the group, so the coordinator rebalances immediately instead of waiting for the
   * session to expire.
   * @param {{ leaveGroup?: boolean }} [options] `true` removes the member whatever its kind,
   *   `false` keeps it in the group until its session expires, and `undefined` is the default
   * @returns {Promise<void>}
   */
  async leave(options) {
    return this.groupMembership.leave(options)
  }

  /**
   * Tells every assigner to forget the state it retained across rounds.
   * @returns {void}
   */
  clearAssignerState() {
    return this.groupMembership.clearAssignerState()
  }

  getNodeIds() {
    return this.cluster.getNodeIds()
  }

  async connect() {
    await this.cluster.connect()
    this.instrumentationEmitter.emit(CONNECT)
    await this.cluster.refreshMetadataIfNecessary()
  }

  /**
   * Applies one sync result to this member's local state and reports what changed.
   *
   * Filters out topics the member is not subscribed to, refreshes metadata once when the
   * assignment names a partition the member has never heard of, and replaces the assignment.
   * Emits exactly one event, `RECEIVED_UNSUBSCRIBED_TOPICS`, which it already owned.
   *
   * Two ways to apply the offsets:
   *
   * - **rebuild** (the default, and what an eager round does): a fresh `OffsetManager`, the whole
   *   position-validation state dropped, every fetch-error counter cleared, and the assignment
   *   epoch moved so batches fetched under the previous assignment are recognised and dropped.
   * - **incremental** (`rebuildOffsets: false`, what a cooperative round does): the manager is
   *   kept and told the delta, so a retained partition keeps its position, its committed offset
   *   and its tracked epoch; only the revoked partitions lose theirs. The epoch does not move,
   *   because batches in flight for retained partitions are still valid.
   *
   * Pause state survives either way: `subscriptionState.update()` replaces the assignment only.
   *
   * @param {{ assignment: Object, userData?: Buffer|null }} result the decoded sync result
   * @param {{ rebuildOffsets?: boolean }} [options] `rebuildOffsets: false` selects the
   *   incremental apply; it still builds the first manager, because there is nothing to keep
   * @returns {Promise<import('./groupProtocol/interface').RebalanceResult>}
   */
  async applyRebalanceResult(result, { rebuildOffsets = true } = {}) {
    const { groupId, generationId, memberId, topicsSubscribed, coordinator } = this
    const decodedAssignment = result.assignment
    this.logger.debug('Received assignment', {
      groupId,
      generationId,
      memberId,
      memberAssignment: decodedAssignment,
    })

    const assignedTopics = keys(decodedAssignment)
    const topicsNotSubscribed = arrayDiff(assignedTopics, topicsSubscribed)

    if (topicsNotSubscribed.length > 0) {
      const payload = {
        groupId,
        generationId,
        memberId,
        assignedTopics,
        topicsSubscribed,
        topicsNotSubscribed,
      }

      this.instrumentationEmitter.emit(RECEIVED_UNSUBSCRIBED_TOPICS, payload)
      this.logger.warn('Consumer group received unsubscribed topics', {
        ...payload,
        helpUrl: websiteUrl(
          'docs/faq',
          'why-am-i-receiving-messages-for-topics-im-not-subscribed-to'
        ),
      })
    }

    // Remove unsubscribed topics from the list
    const safeAssignment = arrayDiff(assignedTopics, topicsNotSubscribed)
    const currentMemberAssignment = safeAssignment.map((topic) => ({
      topic,
      partitions: decodedAssignment[topic],
    }))

    // Check if the consumer is aware of all assigned partitions
    for (const assignment of currentMemberAssignment) {
      const { topic, partitions: assignedPartitions } = assignment
      const knownPartitions = this.partitionsPerSubscribedTopic.get(topic)
      const isAwareOfAllAssignedPartitions = assignedPartitions.every((partition) =>
        knownPartitions.includes(partition)
      )

      if (!isAwareOfAllAssignedPartitions) {
        this.logger.warn('Consumer is not aware of all assigned partitions, refreshing metadata', {
          groupId,
          generationId,
          memberId,
          topic,
          knownPartitions,
          assignedPartitions,
        })

        // If the consumer is not aware of all assigned partitions, refresh metadata
        // and update the list of partitions per subscribed topic. It's enough to perform
        // this operation once since refresh metadata will update metadata for all topics
        await this.cluster.refreshMetadata()
        this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
        break
      }
    }

    this.topics = currentMemberAssignment.map(({ topic }) => topic)

    const { added, revoked } = this.groupMembership.diffAssignment(currentMemberAssignment)

    // Before anything prunes them, and before `updateAssignment` replaces the membership the
    // offset manager commits under.
    if (!rebuildOffsets && revoked.length) {
      await this.commitBeforeRelease(revoked)
    }

    // Narrowed before the apply mutates anything: a revoked partition must stop being advertised
    // and stop accepting resolved offsets the instant the apply decides it is gone. `setOwned`
    // below is the widening half.
    this.groupMembership.narrowOwned(currentMemberAssignment)

    const memberAssignment = currentMemberAssignment.reduce(
      (partitionsByTopic, { topic, partitions }) => ({
        ...partitionsByTopic,
        [topic]: partitions,
      }),
      {}
    )

    if (rebuildOffsets || !this.offsetManager) {
      this.offsetManager?.clearPendingSeeks()
      this.offsetManager = new OffsetManager({
        cluster: this.cluster,
        topicConfigurations: this.topicConfigurations,
        instrumentationEmitter: this.instrumentationEmitter,
        memberAssignment,
        autoCommit: this.autoCommit,
        autoCommitInterval: this.autoCommitInterval,
        autoCommitThreshold: this.autoCommitThreshold,
        coordinator,
        groupId,
        generationId,
        memberId,
        groupInstanceId: this.groupInstanceId,
        operations: this.operations,
        ownership: this.ownership,
        onCommitError: (error) => {
          const verdict = this.rebalanceCoordinator?.classify(error, REBALANCE_PATHS.OFFSET_COMMIT)
          if (verdict?.kind === 'lost') this.rebalanceCoordinator.requestRejoin(verdict)
        },
        obligations: this.obligations,
        membershipProtocol: this.kind,
        staleEpochTimeout: this.rebalanceTimeout,
        // Live reads, not the snapshot above: the offset-fetch retry inside the manager needs the
        // epoch the heartbeats install while it is sleeping, and it must abandon its deadline the
        // moment the runner is asked to stop or a rebalance is waiting for this fetch round to end.
        // The rebalance coordinator is attached after this group is built, so it is read at call
        // time.
        membership: () => ({
          generationId: this.generationId,
          memberId: this.memberId,
          coordinator: this.coordinator,
        }),
        isStopping: () => this.isStopping(),
        isRebalancePending: () => Boolean(this.rebalanceCoordinator?.verdict),
        markPolled: () => this.rebalanceCoordinator?.pollTimer.poll(),
        logger: this.logger,
      })
      this.assignmentEpoch += 1

      this.consecutiveFetchErrors.clear()
      this.positionValidator.clear()
    } else {
      this.offsetManager.updateAssignment({
        revoked,
        memberAssignment,
        generationId,
        memberId,
        coordinator,
      })

      for (const { topic, partitions } of revoked) {
        for (const partition of partitions) {
          this.consecutiveFetchErrors.delete(`${topic}|${partition}`)
          this.seekOffset.pop(topic, partition)
        }
      }

      this.positionValidator.forgetPartitions(revoked)
    }

    // Wholesale in both branches: its members are broker node ids, not partitions.
    this.validationUnusableLogged.clear()

    if (rebuildOffsets) this.acquired.clear()
    for (const { topic, partitions } of revoked) {
      for (const partition of partitions) this.acquired.delete(`${topic}|${partition}`)
    }
    for (const { topic, partitions } of currentMemberAssignment) {
      for (const partition of partitions) {
        const key = `${topic}|${partition}`
        if (!this.acquired.has(key)) {
          const acquiredAt = ++this.acquireClock
          this.acquired.set(key, { acquiredAt, changedAt: acquiredAt })
        }
      }
    }
    this.subscriptionState.update(currentMemberAssignment)

    // A copy: the assignment reaches third-party assigners here, and one that sorts or splices
    // its argument would otherwise corrupt the ownership set `setOwned` retains below.
    this.groupMembership.notifyAssignment({
      partitions: currentMemberAssignment.map(({ topic, partitions }) => ({
        topic,
        partitions: [...partitions],
      })),
      userData: result.userData == null ? null : result.userData,
    })

    this.groupMembership.setOwned(currentMemberAssignment)
    this.clearStaleAssignmentPending()

    return { assignment: currentMemberAssignment, added, revoked }
  }

  /**
   * Joins the group and syncs, retrying the whole sequence on a rebalance-class error.
   * `REBALANCE_IN_PROGRESS` is not a retriable protocol error on its own - the member has to go
   * back through FindCoordinator and JoinGroup - so it is re-thrown as a retriable
   * `KafkaJSError` to restart the sequence through the retrier.
   *
   * Applying the result and reporting the round belong to the caller: this method performs one
   * exchange and returns what it decoded.
   * @returns {Promise<{ assignment: Object, userData: Buffer|null }>} the decoded sync result
   */
  joinAndSync() {
    return this.retrier(async (bail) => {
      try {
        return await this.groupMembership.rebalance({
          topics: this.topics,
          topicsSubscribed: this.topicsSubscribed,
          beforeSync: () => {
            this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
          },
          growSubscription: (names) => this.growSubscription(names),
        })
      } catch (e) {
        if (isRebalancing(e)) {
          throw new KafkaJSError(e)
        }

        if (e.type === 'UNKNOWN_MEMBER_ID') {
          // KIP-848 members keep their id for life: the coordinator answers 25 for an epoch it no
          // longer recognises, not for an id it wants replaced.
          if (this.kind === 'classic') {
            this.memberId = null
          }

          throw new KafkaJSError(e)
        }

        // A fencing that lands while the join is in flight must rejoin, not crash. It is not in
        // `isRebalancing`'s list, and under the classic protocol the equivalent code (22) is - so
        // this arm exists only because the consumer protocol's fencing codes are different.
        if (isLostMember(e) && !isFatalMembership(e) && !isFencedInstance(e)) {
          throw new KafkaJSError(e)
        }

        // A join the membership gave up on because the coordinator took longer than the rebalance
        // timeout to answer. Retried rather than fatal: the next attempt starts a fresh deadline,
        // and a round that never settles is what the deadline exists to prevent.
        if (e.type === 'JOIN_DEADLINE_EXCEEDED') {
          throw new KafkaJSError(e)
        }

        bail(e)
      }
    })
  }

  /**
   * Widens the subscription with names a broker-computed assignment resolved to, before the apply
   * can filter them out as unsubscribed.
   *
   * Three things in order, each of which something downstream would otherwise trip over: the name
   * set, a name-keyed `topicConfigurations` entry (the offset manager indexes it positionally and
   * destructures the result), and a cluster target topic (without which metadata is never fetched
   * for it).
   *
   * NEVER pushes: `this.topics` and `this.topicsSubscribed` are the same array object at
   * construction, so a mutation would edit both.
   * @param {string[]} names newly resolved topic names
   * @returns {Promise<void>}
   */
  async growSubscription(names) {
    const added = names.filter((name) => !this.topicsSubscribed.includes(name))

    if (!added.length) {
      return
    }

    this.topicsSubscribed = [...this.topicsSubscribed, ...added]

    for (const name of added) {
      this.topicConfigurations[name] = this.topicConfigurations[name] || {
        ...this.regexSubscription,
      }
    }

    await this.cluster.addMultipleTargetTopics(added)
  }

  /**
   * @param {import("../../types").TopicPartition} topicPartition
   */
  resetOffset({ topic, partition }) {
    this.offsetManager.resetOffset({ topic, partition })
  }

  /**
   * @param {import("../../types").TopicPartitionOffset & { leaderEpoch?: number }} topicPartitionOffset
   */
  resolveOffset({ topic, partition, offset, leaderEpoch }) {
    this.offsetManager.resolveOffset({ topic, partition, offset, leaderEpoch })
  }

  /**
   * Update the consumer offset for the given topic/partition. This will be used
   * on the next fetch. If this API is invoked for the same topic/partition more
   * than once, the latest offset will be used on the next fetch.
   *
   * @param {import("../../types").TopicPartitionOffset} topicPartitionOffset
   */
  seek({ topic, partition, offset }) {
    this.seekOffset.set(topic, partition, offset)
  }

  /**
   * Drops every piece of state this member holds for one topic, because that topic was deleted
   * and recreated: its offsets, its preferred read replicas, and any pending seek. The
   * corresponding reset is owed (`ResetObligations`) and applied at the next offset resolution.
   *
   * A no-op before the first sync (`offsetManager` is null until then) and for a topic outside
   * the current member assignment - the second guard keeps this member from writing a key it
   * does not own into the committed-offsets map, which is shared by every consumer built from
   * the same client with the same group id.
   *
   * Precedence with `seek()`: a seek pending here (queued but not yet applied) targets the
   * *old* incarnation and is dropped along with everything else, via `seekOffset.deleteTopic()`.
   * A seek issued *after* this call returns, applied in the same `seekOffsets()` cycle, targets
   * the current incarnation and wins over the reset for that partition: an applied seek settles
   * the partition's obligation (`OffsetOperations#apply`).
   *
   * @param {string} topic
   * @returns {void}
   */
  invalidateTopic(topic) {
    if (!this.offsetManager || !Object.hasOwn(this.offsetManager.memberAssignment, topic)) {
      return
    }

    this.preferredReadReplicasPerTopicPartition[topic] = {}
    this.seekOffset.deleteTopic(topic)
    this.positionValidator.forgetTopic(topic, this.offsetManager.memberAssignment[topic])
    this.offsetManager.resetOffsetsForTopic(topic)
  }

  pause(topicPartitions) {
    this.logger.info(`Pausing fetching from ${topicPartitions.length} topics`, {
      topicPartitions,
    })
    this.subscriptionState.pause(topicPartitions)
  }

  resume(topicPartitions) {
    this.logger.info(`Resuming fetching from ${topicPartitions.length} topics`, {
      topicPartitions,
    })
    this.subscriptionState.resume(topicPartitions)
  }

  assigned() {
    return this.subscriptionState.assigned()
  }

  /**
   * Whether a topic-partition is still part of this member's assignment.
   *
   * Positional, and deliberately so: this is called once per message, and an options object would
   * be an allocation per record on the consume hot path.
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean}
   */
  isAssigned(topic, partition) {
    return this.subscriptionState.isAssigned(topic, partition)
  }

  paused() {
    return this.subscriptionState.paused()
  }

  /**
   * @param {string} topic
   * @param {string} partition
   * @returns {boolean} whether the specified topic-partition are paused or not
   */
  isPaused(topic, partition) {
    return this.subscriptionState.isPaused(topic, partition)
  }

  async commitOffsetsIfNecessary(options) {
    await this.offsetManager.commitOffsetsIfNecessary(options)
  }

  /**
   * @param {import("../../types").Offsets} [offsets]
   * @param {{ partitions?: import('./groupProtocol/interface').TopicPartitions[]|null, auto?: boolean }} [options]
   *   forwarded verbatim to the offset manager, which owns what each flag means
   * @returns {Promise<void>}
   */
  async commitOffsets(offsets, options) {
    await this.offsetManager.commitOffsets(offsets, options)
  }

  uncommittedOffsets() {
    return this.offsetManager.uncommittedOffsets()
  }

  /**
   * Drop every resolved-but-uncommitted offset. Used when the member is fenced or its
   * generation is stale: those offsets belong to a generation the coordinator has discarded.
   */
  discardUncommittedOffsets() {
    if (this.offsetManager) {
      this.offsetManager.clearAllOffsets()
    }
  }

  /**
   * Commits what handlers resolved on the partitions this member is about to stop owning, before
   * anything prunes them.
   *
   * Called with the revoked delta by the incremental apply, and with the whole owned set by a
   * group exit - hence "release" rather than "revoke". Without it a partition that moves between
   * members redelivers everything since the last periodic commit.
   *
   * Every failure is logged and SWALLOWED, including a lost membership. This runs inside
   * `applyRebalanceResult`, which a background rebalance chain drives and whose only error sink is
   * the consumer's crash handler; a throw here would turn a failed commit into a dead consumer.
   * The next heartbeat reports the membership loss on a path that has a verdict.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerMembershipManager.java
   *
   * A stale member epoch is the one retried case: the request is rebuilt from the membership and
   * the offsets each attempt, so a retry carries the epoch the heartbeats have installed by then.
   * Bounded by `rebalanceTimeout` - the limit the broker enforces on the reconciliation - and
   * abandoned the moment a stop is requested, because the chain this runs inside is one the
   * runner's `stop()` waits for.
   *
   * @param {import('./groupProtocol/interface').TopicPartitions[]} partitions the partitions this
   *   member is about to stop owning
   * @returns {Promise<void>} always resolves
   */
  async commitBeforeRelease(partitions) {
    if (!this.autoCommit || !this.offsetManager || !partitions.length) {
      return
    }

    const deadline = Date.now() + this.rebalanceTimeout

    for (;;) {
      // The manager's membership snapshot is still the previous generation at this point: it is
      // written at construction and at `updateAssignment`, and this call deliberately precedes
      // both.
      this.offsetManager.useMembership({
        generationId: this.generationId,
        memberId: this.memberId,
        coordinator: this.coordinator,
      })

      try {
        await this.offsetManager.commitOffsets({}, { partitions })
        return
      } catch (e) {
        if (!(await this.awaitFreshEpochFor(e, deadline))) {
          this.logger.warn(
            'Commit before releasing partitions failed; proceeding with the rebalance',
            {
              groupId: this.groupId,
              memberId: this.memberId,
              generationId: this.generationId,
              error: e.message || e,
            }
          )

          return
        }
      }
    }
  }

  /**
   * Whether a failed pre-release commit is worth re-sending, and if so waits until it is.
   *
   * Only a stale member epoch qualifies, and only once the coordinator has actually given this
   * member a newer one - re-sending the same epoch would draw the same answer forever.
   * @private
   * @param {Error} error
   * @param {number} deadline `Date.now()` past which the commit is abandoned
   * @returns {Promise<boolean>}
   */
  async awaitFreshEpochFor(error, deadline) {
    if (this.kind !== 'consumer' || error.type !== STALE_MEMBER_EPOCH) {
      return false
    }

    const sentUnder = sentGeneration(error)

    while (Date.now() < deadline && !this.isStopping()) {
      if (this.generationId > 0 && this.generationId !== sentUnder) {
        return true
      }

      await sleep(STALE_EPOCH_POLL_INTERVAL)
    }

    return false
  }

  /** @private @returns {boolean} whether the runner has been asked to stop */
  isStopping() {
    return Boolean(
      this.rebalanceCoordinator &&
      this.rebalanceCoordinator.runner &&
      this.rebalanceCoordinator.runner.stopRequested
    )
  }

  async heartbeat({ interval }) {
    return this.groupMembership.heartbeat({ interval })
  }

  /**
   * Runs one fetch round against a single broker: resolves what this node owes, sends one Fetch,
   * and turns the response into batches for the runner.
   *
   * Topic ids are all-or-nothing per round. Every topic in the request is looked up in the
   * metadata cache; if all have ids the request negotiates the broker's highest Fetch version and
   * is keyed by id, and if even one is missing the whole request is capped at the last version
   * keyed by name. No extra metadata refresh is issued for a missing id.
   *
   * Error codes are classified rather than thrown wholesale. A partition-level code costs only
   * its own partition - the rest of the response is still delivered, the partition yields no
   * batch, its offset is left where it was, and a rejoin is never triggered by it. A code with no
   * classification is thrown, which is the behaviour every version had. At most one metadata
   * refresh runs per round, after the response has been walked, however many partitions asked for
   * one. The top-level code is classified the same way, before the walk. A partition whose error
   * carried a usable leader hint routes itself next round and asks for no refresh at all.
   *
   * Errors are not propagated: anything thrown is handed to `recoverFromFetch`, which either
   * recovers or rethrows, and the round then yields no batches.
   *
   * @param {number} nodeId the broker to fetch from
   * @returns {Promise<Batch[]>} one batch per fetched partition; empty when nothing was owed,
   *   when the round was recovered from an error, or when every response entry was dropped
   */
  async fetch(nodeId) {
    const ownershipToken = this.ownership.token()
    try {
      await this.cluster.refreshMetadataIfNecessary()
      this.checkForStaleAssignment()

      let topicPartitions = this.subscriptionState.assigned()
      topicPartitions = this.filterPartitionsByNode(nodeId, topicPartitions)
      // Captured before anything is awaited: this is the assignment the request below is built
      // from, and it is what every batch this round produces is stamped with.
      const assignmentEpoch = this.assignmentEpoch
      const acquireClock = this.acquireClock

      await this.seekOffsets(topicPartitions)
      await this.validatePositions(topicPartitions)

      const requests = this.buildFetchRequests(topicPartitions)

      if (!requests.length) {
        await sleep(this.maxWaitTime)
        return []
      }

      const { topicIds, maxVersion } = this.resolveTopicIds(requests)

      if (this.logger.debugEnabled()) {
        this.logger.debug('Fetching', {
          nodeId,
          maxVersion,
          topics: requests.map(({ topic, partitions }) => ({
            topic,
            topicId: topicIds && topicIds.get(topic),
            partitions: partitions.map(({ partition, fetchOffset, currentLeaderEpoch }) => ({
              partition,
              fetchOffset,
              currentLeaderEpoch,
            })),
          })),
        })
      }

      const broker = await this.cluster.findBroker({ nodeId })

      const {
        errorCode: topLevelErrorCode,
        responses,
        nodeEndpoints,
      } = await broker.fetch({
        maxWaitTime: this.maxWaitTime,
        minBytes: this.minBytes,
        maxBytes: this.maxBytes,
        isolationLevel: this.isolationLevel,
        topics: requests,
        topicIds,
        maxVersion,
        rackId: this.rackId,
        checkCrcs: this.checkCrcs,
      })

      // The top-level code is only decoded from Fetch v7 up, so an older response carries none -
      // and `failure(undefined)` is true, which is why the guard is on the field's presence.
      if (topLevelErrorCode != null && failure(topLevelErrorCode)) {
        await this.handleFetchTopLevelError(topLevelErrorCode)
        return []
      }

      let needsRefresh = false
      const batches = []
      // Hoisted out of the per-partition loop: the response row below is the hot path, and its
      // payload must not be built once per partition per round only to be discarded.
      const debug = this.logger.debugEnabled()

      for (const { topicName, topicId, partitions } of responses) {
        if (topicName == null) {
          // Only reachable if a broker answered with a topic id this request never asked for.
          // Treated as stale metadata rather than dropped silently, and checked before the lookup
          // below, whose `undefined` would make the next line a TypeError.
          this.logger.warn('Fetch response carried an unknown topic id, refreshing metadata', {
            groupId: this.groupId,
            memberId: this.memberId,
            topicId,
          })
          needsRefresh = true
          continue
        }

        const topicRequestData = requests.find(({ topic }) => topic === topicName)

        let preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topicName]
        if (!preferredReadReplicas) {
          this.preferredReadReplicasPerTopicPartition[topicName] = preferredReadReplicas = {}
        }

        // Built once per topic, not once per partition: the loop below is the consume hot path.
        const context = {
          topicName,
          topicRequestData,
          preferredReadReplicas,
          nodeEndpoints,
          batches,
          debug,
        }

        for (const partitionData of partitions) {
          const partitionNeedsRefresh = await this.handleFetchedPartition(partitionData, context)
          needsRefresh = needsRefresh || partitionNeedsRefresh
        }
      }

      if (needsRefresh) {
        await this.cluster.refreshMetadata()
      }

      for (const batch of batches) {
        batch.assignmentEpoch = assignmentEpoch
        batch.acquireClock = acquireClock
        batch.ownershipKey = `${batch.topic}|${batch.partition}`
      }

      return batches
    } catch (e) {
      stampOwnership(e, ownershipToken)
      if (obsoleteOwnership(this.ownership, e)) throw e
      await this.recoverFromFetch(e)
      return []
    }
  }

  /**
   * Builds this round's request entries from the node-filtered assignment.
   *
   * Three filters decide what is asked for. A partition with no committed offset is left out
   * because recovering from `OFFSET_OUT_OF_RANGE` clears offsets per partition
   * (`OffsetManager#clearOffsets`), and a concurrent round would otherwise fetch from an offset
   * that is being rebuilt - see https://github.com/tulios/kafkajs/pull/570 for the concurrency
   * that exposed it and https://github.com/tulios/kafkajs/pull/578 for the filter. A paused
   * partition is left out, and so is one whose position another fetcher is validating: its
   * position is the thing being checked, so fetching from it would read from an offset this
   * client is not yet sure still exists.
   *
   * @private
   * @param {import("../../types").TopicPartitions[]} topicPartitions the node-filtered assignment
   * @returns {{topic: string, partitions: {partition: number, fetchOffset: string,
   *   currentLeaderEpoch: number, maxBytes: number}[]}[]} one entry per topic with something to
   *   fetch; a topic whose partitions were all filtered out is absent
   */
  buildFetchRequests(topicPartitions) {
    const committedOffsets = this.offsetManager.committedOffsets()
    const activeTopicPartitions = this.getActiveTopicPartitions()

    return topicPartitions
      .map(({ topic, partitions }) => ({
        topic,
        partitions: partitions
          .filter(
            (partition) =>
              committedOffsets[topic][partition] != null &&
              activeTopicPartitions[topic].has(partition) &&
              !this.positionValidator.isAwaiting(topic, partition)
          )
          .map((partition) => ({
            partition,
            fetchOffset: this.offsetManager.nextOffset(topic, partition).toString(),
            // The client's current view of this partition's leader epoch, so the broker can
            // reject a request built on metadata that has since moved. -1 when the cache has
            // not seen the partition, which is the field's own wire default and means
            // "do not fence".
            currentLeaderEpoch: this.cluster.leaderEpoch(topic, partition),
            maxBytes: this.maxBytesPerPartition,
          })),
      }))
      .filter(({ partitions }) => partitions.length)
  }

  /**
   * Resolves the topic ids for one round's requests, all-or-nothing.
   *
   * A fetch goes out keyed by id only when every topic in it has one, otherwise the whole request
   * falls back to the last version keyed by name. This is the rule the Java consumer applies, and
   * it avoids splitting one round into two requests at two versions. No extra metadata refresh is
   * issued for a missing id: the round already refreshed if necessary, and an id is only missing
   * inside a delete/recreate window.
   *
   * @private
   * @param {{topic: string}[]} requests this round's request entries
   * @returns {{topicIds: Map<string, string>|undefined, maxVersion: number|undefined}} the
   *   name -> id map and no ceiling when every topic has an id; `undefined` and the last
   *   name-keyed version when any is missing
   */
  resolveTopicIds(requests) {
    const topicIds = new Map()

    for (const { topic } of requests) {
      const topicId = this.cluster.topicId(topic)

      if (topicId === undefined) {
        return { topicIds: undefined, maxVersion: FETCH_TOPIC_NAMES_MAX_VERSION }
      }

      topicIds.set(topic, topicId)
    }

    return { topicIds, maxVersion: undefined }
  }

  /**
   * Recovers from the fetch response's top-level error code, or rethrows it.
   *
   * @private
   * @param {number} errorCode the response's top-level code, already known to be a failure
   * @returns {Promise<void>} resolves once the round can be abandoned with no batches
   * @throws {Error} the error the code maps to, for a code with no classification: that keeps the
   *   recovery path a request-level fetch error has always taken - refresh and rejoin for stale
   *   metadata, crash for anything else.
   */
  async handleFetchTopLevelError(errorCode) {
    if (topLevelFetchAction(errorCode) === 'throw') {
      throw createErrorFromCode(errorCode)
    }

    this.logger.warn('Fetch response carried a top-level error, refreshing metadata', {
      groupId: this.groupId,
      memberId: this.memberId,
      errorCode,
    })

    await this.cluster.refreshMetadata()
  }

  /**
   * Turns one partition of a fetch response into a batch, or applies the recovery its error code
   * asks for.
   *
   * A partition-level error costs only its own partition: no batch is produced, the offset is left
   * where it was, and a rejoin is never triggered by it. A code with no classification is thrown,
   * which is the behaviour every version had.
   *
   * The batch is appended to `context.batches` rather than returned, so that the success path of
   * the consume hot path allocates nothing but the batch itself.
   *
   * @private
   * @param {object} partitionData one decoded partition of the response
   * @param {object} context this topic's state for the round
   * @param {string} context.topicName
   * @param {object} context.topicRequestData the request entry this topic was fetched with
   * @param {object} context.preferredReadReplicas this topic's preferred-replica map, written here
   * @param {object[]} [context.nodeEndpoints] the response's KIP-951 endpoints, if any
   * @param {import('./batch')[]} context.batches the round's batches, appended to
   * @param {boolean} context.debug whether debug logging is on for this round
   * @returns {Promise<boolean>} whether this partition asks for a metadata refresh
   * @throws {Error} the error an unclassified partition code maps to
   */
  async handleFetchedPartition(partitionData, context) {
    const { topicName, topicRequestData, preferredReadReplicas, nodeEndpoints, batches, debug } =
      context
    const { partition, errorCode, preferredReadReplica } = partitionData

    if (debug) {
      this.logger.debug('Fetch response partition', {
        topic: topicName,
        partition,
        errorCode,
        highWatermark: partitionData.highWatermark,
        messageCount: partitionData.messages ? partitionData.messages.length : 0,
      })
    }

    if (failure(errorCode)) {
      const action = partitionFetchAction(errorCode)

      if (action === 'throw') {
        throw createErrorFromCode(errorCode)
      }

      this.recordFetchError(topicName, partition, errorCode)

      if (action === 'offset-out-of-range') {
        // The existing recovery, for this partition only: drop the preferred replica if one is
        // set, otherwise reset per the topic's own fromBeginning.
        await this.recoverFromOffsetOutOfRange({ topic: topicName, partition })
      } else if (action === 'skip-and-forget-epoch') {
        // The client is ahead of this broker. Defending the cached epoch would make every retry
        // resend the same rejected value; forgetting it makes the next request carry -1 and the
        // next metadata response converge. Warned every time, because this rewrites cached
        // cluster state and usually clears on the very next round.
        this.logger.warn('Broker does not know the leader epoch we sent, forgetting it', {
          groupId: this.groupId,
          memberId: this.memberId,
          topic: topicName,
          partition,
        })
        this.cluster.forgetLeaderEpoch(topicName, partition)
      } else if (action === 'stale-metadata') {
        const applied = await this.applyFetchLeaderHint(
          topicName,
          partition,
          partitionData.currentLeader,
          nodeEndpoints
        )

        // A partition whose hint was usable already knows where to go next round; one whose hint
        // was dropped, or that never carried one, needs the refresh.
        return !applied
      }

      return false
    }

    const dropReason = fetchedPartitionDropReason(
      this.seekOffset.size !== 0 && this.seekOffset.has(topicName, partition),
      this.subscriptionState.isPaused(topicName, partition),
      this.positionValidator.isAwaiting(topicName, partition)
    )

    if (dropReason != null) {
      this.logger.debug('Dropping fetched partition', {
        topic: topicName,
        partition,
        reason: dropReason,
      })
      return false
    }

    // Guarded on the map being non-empty: in a healthy steady state it always is empty, and this
    // is the per-partition success path of every fetch round.
    if (this.consecutiveFetchErrors.size !== 0) {
      this.consecutiveFetchErrors.delete(`${topicName}|${partition}`)
    }

    if (preferredReadReplica != null && preferredReadReplica !== -1) {
      const { nodeId: currentPreferredReadReplica } = preferredReadReplicas[partition] || {}
      if (currentPreferredReadReplica !== preferredReadReplica) {
        this.logger.info(`Preferred read replica is now ${preferredReadReplica}`, {
          groupId: this.groupId,
          memberId: this.memberId,
          topic: topicName,
          partition,
        })
      }
      preferredReadReplicas[partition] = {
        nodeId: preferredReadReplica,
        expireAt: Date.now() + this.metadataMaxAge,
      }
    }

    const partitionRequestData = topicRequestData.partitions.find(
      (requested) => requested.partition === partition
    )

    batches.push(new Batch(topicName, partitionRequestData.fetchOffset, partitionData))
    return false
  }

  /**
   * Recovers from an error thrown by a fetch round, or rethrows it.
   * @param {Error} e
   * @returns {Promise<void>}
   * @throws {Error} `e` itself when it is not one of the errors this method knows how to recover
   *   from
   */
  async recoverFromFetch(e) {
    if (STALE_METADATA_ERRORS.includes(e.type) || e.name === 'KafkaJSTopicMetadataNotLoaded') {
      this.logger.debug('Stale cluster metadata, refreshing...', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: e.message,
      })

      await this.cluster.refreshMetadata()
      this.requestResync(e)
      return
    }

    if (e.name === 'KafkaJSStaleTopicMetadataAssignment') {
      this.logger.warn(`${e.message}, resync group`, {
        groupId: this.groupId,
        memberId: this.memberId,
        topic: e.topic,
        unknownPartitions: e.unknownPartitions,
      })

      this.requestResync(e)
      return
    }

    if (e.name === 'KafkaJSOffsetOutOfRange') {
      await this.recoverFromOffsetOutOfRange(e)
      return
    }

    if (e.name === 'KafkaJSConnectionClosedError') {
      this.cluster.removeBroker({ host: e.host, port: e.port })
      return
    }

    if (e.name === 'KafkaJSBrokerNotFound' || e.name === 'KafkaJSConnectionClosedError') {
      this.logger.debug(`${e.message}, refreshing metadata and retrying...`)
      await this.cluster.refreshMetadata()
      return
    }

    throw e
  }

  /**
   * Asks for a rejoin because this member's view of the assignment is stale, WITHOUT awaiting the
   * round.
   *
   * This runs inside the fetcher's own awaited fetch, so awaiting a round here would block that
   * fetcher's iteration for the whole join and, through the single-flight chain, everything
   * waiting on it. Before a run is in flight there is no coordinator to ask, and refreshing
   * metadata is all that can be done.
   *
   * @private
   * @param {Error} error
   * @returns {void}
   */
  requestResync(error) {
    if (!this.rebalanceCoordinator) {
      this.logger.debug('No rebalance coordinator attached; refreshed metadata only', {
        groupId: this.groupId,
        error: error.message,
      })
      return
    }

    this.rebalanceCoordinator.requestRejoin({
      kind: 'revoked',
      reason: 'stale-assignment',
      error,
    })
  }

  /**
   * Records that one partition produced an error instead of a batch this round, and makes a
   * partition that keeps failing audible: one warning at five consecutive error rounds, then one
   * every twenty-five. Without it a partition parked on a "skip" code would make no progress and
   * no noise at the default log level.
   *
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {number} errorCode
   * @returns {void}
   */
  recordFetchError(topic, partition, errorCode) {
    const key = `${topic}|${partition}`
    const consecutiveRounds = (this.consecutiveFetchErrors.get(key) || 0) + 1
    this.consecutiveFetchErrors.set(key, consecutiveRounds)

    if (
      consecutiveRounds === FETCH_ERROR_WARN_THRESHOLD ||
      (consecutiveRounds > FETCH_ERROR_WARN_THRESHOLD &&
        (consecutiveRounds - FETCH_ERROR_WARN_THRESHOLD) % FETCH_ERROR_WARN_INTERVAL === 0)
    ) {
      this.logger.warn('Partition has produced consecutive fetch errors', {
        groupId: this.groupId,
        memberId: this.memberId,
        topic,
        partition,
        errorCode,
        consecutiveRounds,
      })
    }
  }

  /**
   * Applies a leader hint a fetch response attached to a failing partition (KIP-951), so the next
   * round routes to the new leader instead of waiting for a metadata refresh.
   *
   * A hint is only safe once the client can reach the node it names. It can, in one of two ways:
   * the response carried an endpoint **for that leader**, which is applied before the cache write
   * publishes the id, or the client already knows the node. Otherwise the write-through would
   * publish a node id that has no broker behind it, the partition would be bucketed under a
   * fetcher that does not exist, and it would stop being fetched with no error, no event and no
   * log until the next metadata refresh. In that case the hint is dropped and the caller refreshes
   * instead.
   *
   * Endpoints that name *other* nodes do not vouch for this leader - a response can move several
   * partitions and omit the listener for one of them - but they are still forwarded, because they
   * are the freshest addresses the client has for the nodes they do name.
   *
   * The membership test goes through `cluster.hasNode`, which owns the one place a node id crosses
   * the string/number boundary: the broker map is keyed by string and a decoded leader id is a
   * number.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {{ leaderId: number, leaderEpoch: number }|null} currentLeader
   * @param {{ nodeId: number, host: string, port: number, rack: string|null }[]} [nodeEndpoints]
   *   absent below Fetch v16, empty on a v16+ response that carried no hint
   * @returns {Promise<boolean>} true when the hint was applied and no refresh is needed for it
   */
  async applyFetchLeaderHint(topic, partition, currentLeader, nodeEndpoints) {
    if (currentLeader == null || currentLeader.leaderId === -1) {
      return false
    }

    const { leaderId, leaderEpoch } = currentLeader
    const endpoints =
      Array.isArray(nodeEndpoints) && nodeEndpoints.length > 0 ? nodeEndpoints : undefined
    const canReachLeader =
      (endpoints != null && endpoints.some(({ nodeId }) => nodeId === leaderId)) ||
      this.cluster.hasNode(leaderId)

    if (!canReachLeader) {
      this.logger.debug('Dropping a leader hint for an unknown node, refreshing metadata instead', {
        groupId: this.groupId,
        topic,
        partition,
        leaderId,
      })
      return false
    }

    return await this.cluster.applyLeaderHint({
      topic,
      partition,
      leaderId,
      leaderEpoch,
      nodeEndpoints: endpoints,
    })
  }

  async recoverFromOffsetOutOfRange(e) {
    // If we are fetching from a follower try with the leader before resetting offsets.
    // The stored value is { nodeId, expireAt } (see the JSDoc on
    // preferredReadReplicasPerTopicPartition), not a bare nodeId.
    const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[e.topic]
    if (preferredReadReplicas && preferredReadReplicas[e.partition] != null) {
      this.logger.info('Offset out of range while fetching from follower, retrying with leader', {
        topic: e.topic,
        partition: e.partition,
        groupId: this.groupId,
        memberId: this.memberId,
      })
      delete preferredReadReplicas[e.partition]
    } else {
      this.logger.error('Offset out of range, resetting to default offset', {
        topic: e.topic,
        partition: e.partition,
        groupId: this.groupId,
        memberId: this.memberId,
      })

      await this.offsetManager.setDefaultOffset({
        topic: e.topic,
        partition: e.partition,
      })
    }
  }

  generatePartitionsPerSubscribedTopic() {
    const map = new Map()

    for (const topic of this.topicsSubscribed) {
      const partitions = this.cluster
        .findTopicPartitionMetadata(topic)
        .map((m) => m.partitionId)
        .sort()

      map.set(topic, partitions)
    }

    return map
  }

  /**
   * Raises `KafkaJSStaleTopicMetadataAssignment` the first time this member's view of a subscribed
   * topic's partitions falls behind the cluster's.
   *
   * Latched: the check is skipped while a report is pending, so it costs one warning and one
   * rejoin request per pending period rather than one per fetch round per broker. A cooperative
   * round stops no fetcher, and the sleep that paces an idle round is downstream of this throw, so
   * an unlatched check would spin every fetcher for the whole duration of the round it asked for.
   * The latch is cleared by the apply, and by the rebalance chain's own `finally`.
   *
   * @returns {void}
   * @throws {KafkaJSStaleTopicMetadataAssignment} when a subscribed topic gained partitions
   */
  checkForStaleAssignment() {
    if (!this.partitionsPerSubscribedTopic || this.staleAssignmentPending) {
      return
    }

    const newPartitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()

    for (const [topic, partitions] of newPartitionsPerSubscribedTopic) {
      const diff = arrayDiff(partitions, this.partitionsPerSubscribedTopic.get(topic))

      if (diff.length > 0) {
        this.staleAssignmentPending = true
        throw new KafkaJSStaleTopicMetadataAssignment('Topic has been updated', {
          topic,
          unknownPartitions: diff,
        })
      }
    }
  }

  /**
   * Applies every pending `seek()` for the given topic/partitions, then resolves offsets for the
   * rest. A seek applied here that lands on a topic with a pending recreation reset (set by
   * `invalidateTopic()` earlier in this same fetch cycle) wins over that reset for its own
   * partition: an applied seek settles the partition's reset obligation.
   *
   * A resolution that dropped an answer (a seek, commit or recreation reached the cell while the
   * request was in flight) is repeated at once, after a yield to the event loop, at most
   * `MAX_RESOLUTION_PASSES` times, instead of leaving the broker's round to wait out an empty
   * fetch. No pass can spin: each needs a fresh competing operation to drop again. After the last
   * pass the round proceeds as it always did.
   * @param {import("../../types").TopicPartitions[]} topicPartitions
   * @returns {Promise<void>}
   */
  async seekOffsets(topicPartitions) {
    for (const { topic, partitions } of topicPartitions) {
      for (const partition of partitions) {
        const seekEntry = this.seekOffset.pop(topic, partition)
        if (!seekEntry) {
          continue
        }

        this.logger.debug('Seek offset', {
          groupId: this.groupId,
          memberId: this.memberId,
          seek: seekEntry,
        })

        await this.offsetManager.seek(seekEntry)
      }
    }

    for (let pass = 1; pass <= MAX_RESOLUTION_PASSES; pass++) {
      if ((await this.offsetManager.resolveOffsets()) || pass === MAX_RESOLUTION_PASSES) {
        break
      }

      await new Promise((resolve) => setImmediate(resolve))
    }

    // Guarded, not just level-checked: this runs once per fetch round, and most rounds resolve
    // nothing, so the per-topic payload must not be built to be thrown away.
    if (this.logger.debugEnabled()) {
      const committedOffsets = this.offsetManager.committedOffsets()
      for (const { topic } of topicPartitions) {
        this.logger.debug('Resolved offsets', {
          groupId: this.groupId,
          topic,
          partitions: committedOffsets[topic],
        })
      }
    }
  }

  hasSeekOffset({ topic, partition }) {
    return this.seekOffset.has(topic, partition)
  }

  /**
   * Checks, before this round's request is built, that each assigned partition's position still
   * exists on its current leader (KIP-320).
   *
   * A partition is examined when the leader epoch it was last validated against has changed - or
   * was never recorded - and both its tracked epoch and the cached leader epoch are known. Java
   * uses the same two triggers and no others. Validation is a **blocking gate**: a partition still
   * awaiting a reply is absent from the request built afterwards, which is why this method must
   * never return leaving one in that state.
   *
   * Nothing this method does may reach the caller as an exception. A transport failure, a
   * coordinator failure or anything unexpected is warned and swallowed, every partition it touched
   * is marked validated, and the round proceeds - validation is an optimisation over
   * correctness-on-the-next-round, never a reason to crash a consumer.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation
   * @param {import("../../types").TopicPartitions[]} topicPartitions the node-filtered assignment,
   *   which is a superset of what the request keeps: it also contains partitions with no committed
   *   offset, which are skipped here - there is no position to check, and reading one would create
   *   a resolved offset holding `undefined`.
   * @returns {Promise<void>}
   */
  async validatePositions(topicPartitions) {
    let awaiting
    const committedOffsets = this.offsetManager.committedOffsets()

    try {
      for (const { topic, partitions } of topicPartitions) {
        for (const partition of partitions) {
          // A queued user seek is newer, more specific intent than anything validation could
          // conclude. seekOffsets() has just popped this round's seeks, so the only entries this
          // can find are seeks that arrived while validation itself was awaiting.
          if (this.seekOffset.size !== 0 && this.seekOffset.has(topic, partition)) {
            continue
          }

          const currentLeaderEpoch = this.cluster.leaderEpoch(topic, partition)

          // Asked before anything is read or allocated: the steady state of a round is that every
          // partition is already validated against this epoch, and that round must cost nothing.
          if (!this.positionValidator.needsValidation(topic, partition, currentLeaderEpoch)) {
            continue
          }

          // The same filter the request build applies. `nextOffset()` writes the committed offset
          // into the resolved ones, so calling it for a partition whose offset was just cleared
          // would store `undefined` there and break the next commit.
          if (committedOffsets[topic][partition] == null) {
            continue
          }

          const trackedEpoch = this.offsetManager.lastConsumedEpoch(topic, partition)
          // No early return while validation is disabled: `request()` still answers 'validated'
          // rather than 'unchanged' for a partition whose leader epoch moved, and the
          // preferred-read-replica clearing below still has to happen - a replica chosen under the
          // old leader is not a safe place to keep reading from, whether or not this client is
          // still asking the leader about its position.
          const { action, generation } = this.positionValidator.request(topic, partition, {
            offset: this.offsetManager.nextOffset(topic, partition).toString(),
            trackedEpoch,
            currentLeaderEpoch,
          })

          if (action === 'unchanged') {
            continue
          }

          const replicas = this.preferredReadReplicasPerTopicPartition[topic]
          if (replicas) {
            delete replicas[partition]
          }

          if (action === 'awaiting') {
            awaiting = awaiting || []
            awaiting.push({ topic, partition, trackedEpoch, currentLeaderEpoch, generation })
          }
        }
      }

      if (awaiting) {
        await this.runValidationRequests(awaiting)
      }
    } catch (e) {
      this.logger.warn('Position validation failed, fetching unvalidated', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: e.message,
      })
    } finally {
      // The invariant: no partition may be left awaiting with no request in flight. Carries the
      // generation each entry was requested under, so a sweep that runs after a rebalance cannot
      // record another fetcher's fresh entry as validated against this round's epoch.
      for (const { topic, partition, currentLeaderEpoch, generation } of awaiting || []) {
        if (this.positionValidator.isAwaiting(topic, partition)) {
          this.positionValidator.markValidated(topic, partition, currentLeaderEpoch, generation)
        }
      }
    }
  }

  /**
   * Issues the OffsetForLeaderEpoch requests for the partitions `validatePositions()` put into
   * flight, and applies every partition's outcome.
   *
   * Grouping is by **leader**, not by the node this round fetches from: a partition may be read
   * from a preferred replica while only its leader can answer for its epochs. It is by topic as
   * well, because `Topic` is a map key on the request.
   *
   * @private
   * @param {{topic: string, partition: number, trackedEpoch: number, currentLeaderEpoch: number,
   *   generation: number}[]} awaiting
   * @returns {Promise<void>}
   */
  async runValidationRequests(awaiting) {
    const entriesByTopic = new Map()
    for (const entry of awaiting) {
      const entries = entriesByTopic.get(entry.topic) || []
      entries.push(entry)
      entriesByTopic.set(entry.topic, entries)
    }

    /** @type {Map<number, {topic: string, entries: object[]}[]>} */
    const perLeader = new Map()
    for (const [topic, entries] of entriesByTopic) {
      const partitionsPerLeader = this.cluster.findLeaderForPartitions(
        topic,
        entries.map(({ partition }) => partition)
      )

      for (const [leader, partitions] of Object.entries(partitionsPerLeader)) {
        const nodeId = Number(leader)
        const led = new Set(partitions)
        const topics = perLeader.get(nodeId) || []
        topics.push({ topic, entries: entries.filter(({ partition }) => led.has(partition)) })
        perLeader.set(nodeId, topics)
      }
    }

    for (const [nodeId, topics] of perLeader) {
      await this.validateOnLeader(nodeId, topics)
    }
  }

  /**
   * Validates one leader's share of this round, retrying only the partitions that asked for it.
   *
   * Whether the leader can serve the API at all is decided from its advertised versions, never by
   * catching `KafkaJSServerDoesNotSupportApiKey`: that error is non-retriable and would reach the
   * fetch's own recovery, crashing the consumer once per round.
   *
   * @private
   * @param {number} nodeId the leader of every partition in `topics`
   * @param {{topic: string, entries: object[]}[]} topics
   * @returns {Promise<void>}
   */
  async validateOnLeader(nodeId, topics) {
    // A non-retriable reply from an earlier leader in this same round switched validation off for
    // the assignment; asking the remaining leaders would buy nothing and cost a round trip each.
    if (this.positionValidator.isDisabled()) {
      this.markAllValidated(topics)
      return
    }

    const broker = await this.cluster.findBroker({ nodeId })
    const version = broker.negotiatedVersion(
      apiKeys.OffsetForLeaderEpoch,
      requests.OffsetForLeaderEpoch
    )

    if (version == null) {
      if (!this.validationUnusableLogged.has(nodeId)) {
        this.validationUnusableLogged.add(nodeId)
        this.logger.info('Broker cannot validate positions, fetching unvalidated', {
          groupId: this.groupId,
          memberId: this.memberId,
          nodeId,
        })
      }

      this.markAllValidated(topics)
      return
    }

    let pending = topics

    try {
      await this.validationRetrier(async () => {
        const response = await broker.offsetForLeaderEpoch({
          topics: pending.map(({ topic, entries }) => ({
            topic,
            partitions: entries.map(({ partition, currentLeaderEpoch, trackedEpoch }) => ({
              partition,
              currentLeaderEpoch,
              leaderEpoch: trackedEpoch,
            })),
          })),
        })

        pending = await this.applyValidationResponse(pending, response)

        if (pending.length > 0) {
          // `createRetry` only retries a thrown error, and the response codec deliberately does
          // not throw on a per-partition code.
          throw new KafkaJSError('Position validation retriable')
        }
      })
    } catch (e) {
      if (e.name !== 'KafkaJSNumberOfRetriesExceeded') {
        throw e
      }

      this.logger.warn('Position validation did not complete, fetching unvalidated', {
        groupId: this.groupId,
        memberId: this.memberId,
        nodeId,
        error: e.message,
      })

      for (const { topic, entries } of pending) {
        for (const { partition, currentLeaderEpoch, generation } of entries) {
          if (!this.positionValidator.isAwaiting(topic, partition)) {
            continue
          }

          // The epoch is cleared and the offset left alone: the partition is fetched from where it
          // is, unvalidated, and validates again once it consumes a record. Both only happen when
          // the mark applied - on a stale generation this round's exhaustion says nothing about
          // the partition the current assignment holds.
          if (
            this.positionValidator.markValidated(topic, partition, currentLeaderEpoch, generation)
          ) {
            this.offsetManager.forgetConsumedEpoch(topic, partition)
          }
        }
      }
    }
  }

  /**
   * Classifies every partition of one OffsetForLeaderEpoch reply and applies its outcome.
   *
   * @private
   * @param {{topic: string, entries: object[]}[]} pending the partitions this attempt asked about
   * @param {object} response the decoded reply
   * @returns {Promise<{topic: string, entries: object[]}[]>} the partitions to ask again about,
   *   empty when the round is done. A partition the reply does not mention is left to the caller's
   *   awaiting sweep rather than retried: the broker answered without it.
   */
  async applyValidationResponse(pending, response) {
    const replies = new Map()
    for (const { topic, partitions } of response.topics) {
      for (const reply of partitions) {
        replies.set(`${topic}|${reply.partition}`, reply)
      }
    }

    const retry = []
    let fatal = false

    for (const { topic, entries } of pending) {
      const retriable = []

      for (const entry of entries) {
        const reply = replies.get(`${topic}|${entry.partition}`)

        if (!reply) {
          continue
        }

        // The partition lost its committed offset while the request was in flight - a concurrent
        // OffsetOutOfRange recovery cleared it. There is no position left to compare the reply
        // with, and reading one would store `undefined` as its resolved offset. Left to the
        // caller's awaiting sweep, like a partition the reply never mentioned.
        if (this.offsetManager.committedOffsets()[topic][entry.partition] == null) {
          continue
        }

        // Read again now, not the pair captured when the request was made: comparing the captured
        // pair with itself could never detect a position that moved under the request.
        const offset = this.offsetManager.nextOffset(topic, entry.partition).toString()
        const outcome = this.positionValidator.complete(
          topic,
          entry.partition,
          { offset, trackedEpoch: this.offsetManager.lastConsumedEpoch(topic, entry.partition) },
          {
            errorCode: reply.errorCode,
            endOffset: reply.endOffset,
            leaderEpoch: reply.leaderEpoch,
            seekQueued: this.seekOffset.has(topic, entry.partition),
            generation: entry.generation,
          }
        )

        if (outcome.action === 'retry') {
          retriable.push(entry)
        } else if (outcome.action === 'fatal') {
          fatal = true
        } else {
          try {
            await this.applyValidationOutcome(topic, entry.partition, offset, outcome)
          } catch (e) {
            // `complete()` already recorded the partition as validated, so leaving it there would
            // hide a divergence for good: the next round would answer 'unchanged' and the position
            // it was told to leave would stand, with no event and no second attempt. Returning the
            // key to unvalidated is what makes the next round decide it again.
            this.positionValidator.forgetTopic(topic, [entry.partition])
            this.logger.warn('Could not apply a position validation outcome, retrying next round', {
              groupId: this.groupId,
              memberId: this.memberId,
              topic,
              partition: entry.partition,
              error: e.message,
            })
          }
        }
      }

      if (retriable.length > 0) {
        retry.push({ topic, entries: retriable })
      }
    }

    if (fatal) {
      // Once per assignment, and only from the call that flips validation off: `disable()` makes
      // `request()` answer without a reply from here on and `validateOnLeader` returns early for
      // every remaining leader of this round, but fetchers run one per node and two of them can
      // reach a non-retriable reply at the same moment.
      if (!this.positionValidator.isDisabled()) {
        this.logger.warn('Positions cannot be validated, continuing without validation', {
          groupId: this.groupId,
          memberId: this.memberId,
        })
        this.positionValidator.disable()
      }

      this.markAllValidated(pending)
      return []
    }

    return retry
  }

  /**
   * Moves every partition of these entries on without a reply, leaving their tracked epochs alone.
   *
   * Each entry is marked under the generation it was requested with, so an entry belonging to a
   * previous assignment is a no-op rather than a claim about the current one.
   *
   * @private
   * @param {{topic: string, entries: object[]}[]} topics
   * @returns {void}
   */
  markAllValidated(topics) {
    for (const { topic, entries } of topics) {
      for (const { partition, currentLeaderEpoch, generation } of entries) {
        this.positionValidator.markValidated(topic, partition, currentLeaderEpoch, generation)
      }
    }
  }

  /**
   * Applies one validation outcome that concluded the log diverged, and announces it.
   *
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {string} offset the position that was validated
   * @param {{action: string, divergingOffset: string|null, leaderEpoch: number}} outcome
   * @returns {Promise<void>}
   */
  async applyValidationOutcome(topic, partition, offset, outcome) {
    if (outcome.action === 'truncated') {
      this.logger.warn('Log truncation detected, repositioning the partition', {
        groupId: this.groupId,
        topic,
        partition,
        offset,
        divergingOffset: outcome.divergingOffset,
      })
      // Applied inline, not through the seek queue: the queue is drained at the top of a round, so
      // a queued seek would land one round late - and would overwrite a user seek queued in the
      // same window, since the queue is last-writer-wins per key.
      await this.offsetManager.seek({ topic, partition, offset: outcome.divergingOffset })
    } else if (outcome.action === 'reset') {
      this.logger.warn('Log truncation detected but the divergence point is unknown, resetting', {
        groupId: this.groupId,
        topic,
        partition,
        offset,
      })
      await this.offsetManager.setDefaultOffset({ topic, partition })
    } else {
      return
    }

    this.instrumentationEmitter.emit(LOG_TRUNCATION, {
      topic,
      partition,
      offset,
      divergingOffset: outcome.divergingOffset,
      leaderEpoch: outcome.leaderEpoch,
    })
  }

  /**
   * For each of the partitions find the best nodeId to read it from. A partition whose leader is
   * negative (`-1`) has none and is read from nowhere, its preferred read replica included, as the
   * Java consumer reads it from nowhere and asks for new metadata: this throws the retriable
   * `LEADER_NOT_AVAILABLE`, which `recoverFromFetch` answers with a metadata refresh, so the fetch
   * round it is part of sends nothing.
   *
   * @param {string} topic
   * @param {number[]} partitions
   * @returns {{[nodeId: number]: number[]}} per-node assignment of partitions
   * @throws {import('../errors').KafkaJSProtocolError} `LEADER_NOT_AVAILABLE`, for a partition
   *   whose leader is negative
   * @see Cluster~findLeaderForPartitions
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractFetch.java
   */
  // Invariant: The resulting object has each partition referenced exactly once
  findReadReplicaForPartitions(topic, partitions) {
    const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic)
    const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topic]
    return partitions.reduce((result, id) => {
      const partitionId = parseInt(id, 10)
      const metadata = partitionMetadata.find((p) => p.partitionId === partitionId)
      if (!metadata) {
        return result
      }

      if (metadata.leader == null) {
        throw new KafkaJSError('Invalid partition metadata', { topic, partitionId, metadata })
      }

      if (metadata.leader < 0) {
        throw createErrorFromCode(LEADER_NOT_AVAILABLE)
      }

      // Pick the preferred replica if there is one, and it isn't known to be offline, otherwise the leader.
      let nodeId = metadata.leader
      if (preferredReadReplicas) {
        const { nodeId: preferredReadReplica, expireAt } = preferredReadReplicas[partitionId] || {}
        if (Date.now() >= expireAt) {
          this.logger.debug('Preferred read replica information has expired, using leader', {
            topic,
            partitionId,
            groupId: this.groupId,
            memberId: this.memberId,
            preferredReadReplica,
            leader: metadata.leader,
          })
          // Drop the entry
          delete preferredReadReplicas[partitionId]
        } else if (preferredReadReplica != null) {
          // Valid entry, check whether it is not offline
          // Note that we don't delete the preference here, and rather hope that eventually that replica comes online again
          const offlineReplicas = metadata.offlineReplicas
          if (Array.isArray(offlineReplicas) && offlineReplicas.includes(nodeId)) {
            this.logger.debug('Preferred read replica is offline, using leader', {
              topic,
              partitionId,
              groupId: this.groupId,
              memberId: this.memberId,
              preferredReadReplica,
              leader: metadata.leader,
            })
          } else {
            nodeId = preferredReadReplica
          }
        }
      }
      const current = result[nodeId] || []
      return { ...result, [nodeId]: [...current, partitionId] }
    }, {})
  }

  filterPartitionsByNode(nodeId, topicPartitions) {
    return topicPartitions.map(({ topic, partitions }) => ({
      topic,
      partitions: this.findReadReplicaForPartitions(topic, partitions)[nodeId] || [],
    }))
  }

  getActiveTopicPartitions() {
    const activeSubscriptionState = this.subscriptionState.active()

    const activeTopicPartitions = {}
    activeSubscriptionState.forEach(({ topic, partitions }) => {
      activeTopicPartitions[topic] = new Set(partitions)
    })

    return activeTopicPartitions
  }
}
