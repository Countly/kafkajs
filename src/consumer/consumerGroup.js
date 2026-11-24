const { failureSourceOf } = require('../broker/failureSource')
const sleep = require('../utils/sleep')
const websiteUrl = require('../utils/websiteUrl')
const arrayDiff = require('../utils/arrayDiff')
const createRetry = require('../retry')
const Generation = require('../producer/generation')

const OffsetManager = require('./offsetManager')
const OffsetOperations = require('./offsetManager/offsetOperations')
const GroupOwnership = require('./groupOwnership')
const CoordinatorLookup = require('./coordinatorLookup')
const ResetObligations = require('./offsetManager/resetObligations')
const Batch = require('./batch')
const SeekOffsets = require('./seekOffsets')
const SubscriptionState = require('./subscriptionState')
const Subscription = require('./subscription')
const IdleWait = require('./idleWait')
const LeaderUnionWatch = require('./leaderUnionWatch')
const { FetchPreparation, InvocationPreparation, fencedDelay } = require('./fetchPreparation')

/** Stands for an acquisition that ended before a batch prepared under it was stamped. */
const RETIRED_ACQUISITION = Object.freeze({
  acquiredAt: 0,
  changedAt: 0,
  recreatedAt: 0,
  soughtAt: 0,
  retired: true,
})

/**
 * @param {import('./handlerContext').Acquisition} acquired
 * @param {number} clock
 * @returns {boolean} whether the partition was acquired again, sought, reset or recreated after
 *   `clock`
 */
const isChangedSince = (acquired, clock) => acquired.changedAt > clock || acquired.soughtAt > clock

/**
 * Indexes items by one field, keeping the first item for a repeated key, so a lookup answers
 * exactly what `Array#find` on the same field would.
 * @template T
 * @param {T[]} items
 * @param {string} field
 * @returns {Map<*, T>}
 */
const indexFirstBy = (items, field) => {
  const index = new Map()
  for (const item of items) if (!index.has(item[field])) index.set(item[field], item)
  return index
}

/** Metadata arrays up to this length are searched; longer ones are indexed. */
const SHORT_METADATA = 8

const { FetchSessions } = require('./fetchSession')
const PositionValidator = require('./positionValidator')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const Outage = require('./outage')
const RebalanceHooks = require('./rebalanceHooks')
const {
  events: { CONNECT, RECEIVED_UNSUBSCRIBED_TOPICS, LOG_TRUNCATION, PARTITION_HALTED },
} = require('./instrumentationEvents')
const {
  KafkaJSError,
  KafkaJSCommitFailedError,
  KafkaJSNonRetriableError,
  KafkaJSRebalanceListenerError,
  KafkaJSStaleTopicMetadataAssignment,
  KafkaJSNoOffsetError,
  isRebalancing,
  isLostMember,
  isFatalMembership,
  isFencedInstance,
  isStaleCoordinator,
  COORDINATOR_LOAD_IN_PROGRESS_CODE,
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
const ManualGroupProtocol = require('./groupProtocol/manual')
const {
  sentGeneration,
  isHeartbeatFatal,
  isStaleMembershipVerdict,
  stampOwnership,
  obsoleteOwnership,
  consumerStoppingError,
  joinDeadlineError,
  isFatalTopicError,
  REBALANCE_PATHS,
} = require('./rebalanceErrorContract')
const { EAGER } = require('./groupProtocol/interface')
const describeThrown = require('../network/describeThrown')
const { isConnectionLoss } = require('../network/connectionFailure')
const dictionary = require('../utils/dictionary')

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const STALE_MEMBER_EPOCH = 'STALE_MEMBER_EPOCH'

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

// The last Fetch version that keys topics by name: the ceiling of a removal-only request for a
// session that was built by name.
// @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
const FETCH_TOPIC_NAMES_MAX_VERSION = 12

// Recorded for a partition left out of a fetch round because its topic id is not known yet.
const UNKNOWN_TOPIC_ID_CODE = 100

/** The first Fetch version that carries a session ID and epoch (KIP-227). */
const FETCH_SESSION_MIN_VERSION = 7

/** The first Fetch version keyed by topic ID rather than name (KIP-516). */
const FETCH_TOPIC_IDS_MIN_VERSION = 13

/** OffsetForLeaderEpoch codes with their own handling: the client's epoch is ahead; an ACL refusal. */
const UNKNOWN_LEADER_EPOCH_CODE = 75
const TOPIC_AUTHORIZATION_FAILED_CODE = 29

/** At most one warning per held partition in this interval, after the first failure's. */
const HELD_WARNING_INTERVAL = 60000

/**
 * How many times one `seekOffsets()` call resolves offsets again after a resolution dropped an
 * answer because a competing seek, commit or recreation reached the cell meanwhile.
 */
const MAX_RESOLUTION_PASSES = 3

/**
 * How many node-filtered snapshots one `fetch(nodeId)` prepares at most. A routing change during
 * the first yields once and prepares a replacement; after the second, only a snapshot that
 * completed preparation is built, so churn can delay a round but never starve it.
 */
const MAX_PREPARATION_ATTEMPTS = 2

/**
 * @param {import('../producer/generation')} [fence] a run's terminal fence
 * @returns {[{fence: import('../producer/generation')}]|[]} the options argument that binds a
 *   cluster call to the run; none for an unfenced caller, which keeps its call shape
 */
const boundTo = (fence) => (fence ? [{ fence }] : [])

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
   *                                (Java's `check.crcs`); a mismatch halts the partition
   * @param {number} [options.maxDecompressedBatchBytes] the most a record batch may decompress
   *                                to; a batch past it halts the partition
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
   * @param {{ fromBeginning: boolean, resetPolicy: object }|null} [options.regexSubscription=null]
   *                                the options the regex subscription was made with (the legacy
   *                                flag and the normalized reset policy), copied to a name-keyed
   *                                `topicConfigurations` entry for every name the regex resolves to.
   * @param {Set<string>} [options.unregisteredTopics] literal names subscribed while their topic did
   *                                not exist (consumer protocol), owned by the long-lived `Consumer`
   *                                and shared with every group it builds. `growSubscription`
   *                                registers each as a cluster target when the coordinator assigns
   *                                it, and removes it from the set.
   * @param {Subscription|null} [options.subscription=null] the consumer's declarations when it holds
   *                                a client-side pattern: names are then discovered at metadata
   *                                refreshes and published by revision, never by `topics` alone.
   *                                Null for a literal-only consumer, whose names never change.
   * @param {0|1} [options.maxPrefetchBatches=0] reserved for a fetch layer that runs one Fetch
   *                                ahead of the workers. Accepted and stored; inert in this
   *                                release, and read by nothing.
   * @param {number} [options.heartbeatInterval=3000] the configured heartbeat cadence. Forwarded to
   *                                the classic membership, which owns the interval; under the
   *                                consumer protocol the coordinator owns it and this is ignored.
   * @param {{isCurrent: () => boolean}} [options.runIdentity] the unique identity of the run that
   *                                built this group; carried by handlers so a retired run's caller
   *                                is never mistaken for a replacement's.
   * @param {{topic: string, partitions: number[]}[]} [options.manual] a validated manual selection;
   *                                when present the group runs the local manual protocol and never
   *                                joins a group, heartbeats or polls for membership.
   * @param {Record<string, Record<number, string>>} [options.privateOffsets] the committed-offset
   *                                cache of a consumer without a group, passed to every manager.
   * @param {object|null} [options.rebalanceListener] the run's validated private callback table;
   *   absent for a run without a listener, which keeps every existing path
   * @param {boolean} [options.fetchSessions=false] whether this run uses incremental Fetch
   *   sessions (KIP-227). `false` constructs no session state and sends full requests.
   * @param {number} [options.requestTimeout=30000] the client's request timeout; it caps a session
   *   close that has no disconnect deadline
   * @param {string|null} [options.retainedMemberId=null] the classic member id a crashed run of this
   *   consumer held, sent by the first JoinGroup; ignored under any other protocol
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
    maxDecompressedBatchBytes,
    groupInstanceId = null,
    rebalanceProtocol = EAGER,
    metadataMaxAge,
    operations = new OffsetOperations(),
    obligations = new ResetObligations(),
    groupProtocol = 'classic',
    groupRemoteAssignor = null,
    subscribedTopicRegex = null,
    regexSubscription = null,
    unregisteredTopics = new Set(),
    subscription = null,
    maxPrefetchBatches = 0,
    heartbeatInterval = 3000,
    runIdentity = null,
    manual = null,
    privateOffsets = undefined,
    rebalanceListener = null,
    fetchSessions = false,
    requestTimeout = 30000,
    retainedMemberId = null,
  }) {
    /** @type {import("../../types").Cluster} */
    this.cluster = cluster
    this.groupId = groupId
    this.runIdentity = runIdentity
    this.privateOffsets = privateOffsets
    this.topics = topics
    this.topicsSubscribed = topics
    this.topicConfigurations = topicConfigurations
    this.logger = logger.namespace('ConsumerGroup')
    this.instrumentationEmitter = instrumentationEmitter
    this.retrier = createRetry(Object.assign({}, retry))
    this.backoff = createRetry.backoff(retry)
    this.sessionTimeout = sessionTimeout
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
    this.maxDecompressedBatchBytes = maxDecompressedBatchBytes
    this.metadataMaxAge = metadataMaxAge
    this.operations = operations
    this.ownership = new GroupOwnership({
      isStale: (cause) =>
        isStaleMembershipVerdict({ kind: 'lost', error: cause }, this.kind, this.generationId),
    })
    /** @type {import('./rebalanceHooks')|null} awaited callbacks of this run, null without a listener */
    this.rebalanceHooks = rebalanceListener
      ? new RebalanceHooks(rebalanceListener, {
          isCurrent: ({ round }) => !round.guard(),
          onExpire: (error, { membership, round }) => {
            this.ownership.lose(error, membership)
            round.retire(error)
          },
        })
      : null
    /** @type {WeakMap<object, Error>} the first ordinary callback failure of each round */
    this.listenerFailures = new WeakMap()
    /** @type {Map<string, import('./handlerContext').Acquisition>} one per owned partition */
    this.acquired = new Map()
    this.acquireClock = 0
    /** The latest `soughtAt` stamped, so a check finds "nothing sought since" in constant time. */
    this.lastSoughtAt = 0
    /** The latest `acquiredAt` stamped, for the same constant-time check of reacquisitions. */
    this.lastAcquiredAt = 0
    /**
     * The partitions a structurally corrupt Fetch batch halted, keyed by `topic|partition`: the
     * first offset of that batch, its reason and the error. A halted partition is left out of every
     * Fetch request until an applied seek, a revoke, a recreation of its topic or a wholesale
     * rebuild of the assignment removes its entry; nothing skips the batch on its own.
     * @type {Map<string, { offset: string, reason: string, error: Error }>}
     */
    this.haltedPartitions = new Map()
    this.obligations = obligations
    this.obligations.attachTo(this.operations)
    this.obligations.claimSubscription(this.topicsSubscribed)

    this.rebalanceTimeout = rebalanceTimeout
    this.subscribedTopicRegex = subscribedTopicRegex
    this.regexSubscription = regexSubscription
    this.unregisteredTopics = unregisteredTopics
    this.subscription = subscription
    /** @type {{revision: number}|null} the newest discovered snapshot not yet handed to `updateSubscription` */
    this.discovered = null
    this.notifiedRevision = null
    this.maxPrefetchBatches = maxPrefetchBatches

    /** The one pending coordinator lookup of this membership, shared by its protocol and commits. */
    this.coordinatorLookup = new CoordinatorLookup({ cluster, groupId })

    const membershipOptions = {
      ownership: this.ownership,
      coordinatorLookup: this.coordinatorLookup,
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
    this.groupMembership = manual
      ? new ManualGroupProtocol({
          cluster,
          groupId,
          topicPartitions: manual,
          runIdentity: runIdentity || { isCurrent: () => true },
        })
      : groupProtocol === 'consumer'
        ? new ConsumerGroupProtocol({
            ...membershipOptions,
            // The protocol calls it `serverAssignor` because that is the wire field's name; this
            // layer calls it `groupRemoteAssignor` because that is the public option's name.
            serverAssignor: groupRemoteAssignor,
            topics: this.topics,
            subscribedTopicRegex,
            subscription,
            metadataMaxAge,
            retry,
          })
        : new ClassicGroupProtocol({
            ...membershipOptions,
            sessionTimeout,
            heartbeatInterval,
            assigners,
            mode: rebalanceProtocol,
            retainedMemberId,
          })

    this.seekOffset = new SeekOffsets()

    /**
     * The run's Fetch sessions, one per broker node; null when the option is off, which keeps
     * every Fetch a full, sessionless request. A new group builds a new registry, so a restart
     * never reuses a session.
     * @type {FetchSessions|null}
     */
    this.fetchSessions = fetchSessions
      ? new FetchSessions({
          sendClose: (nodeId, fields, operation) =>
            this.sendSessionClose(nodeId, fields, operation),
          requestTimeout,
          logger: this.logger,
        })
      : null

    this.partitionsPerSubscribedTopic = null
    this.leaderUnionWatch = new LeaderUnionWatch({
      cluster,
      logger: this.logger,
      metadataMaxAge,
      isLeader: () => this.isLeader(),
    })
    /**
     * Preferred read replica per topic and partition
     *
     * Each of the partitions tracks the preferred read replica (`nodeId`) and a timestamp
     * until when that preference is valid.
     *
     * @type {{[topicName: string]: {[partition: number]: {nodeId: number, expireAt: number}}}}
     */
    this.preferredReadReplicasPerTopicPartition = dictionary()
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

    /** A full refresh is owed after a validation failure, even if a refresh is already running. */
    this.validationRefresh = null
    this.refreshValidationMetadata = sharedPromiseTo(
      async (fence) => {
        const obligation = this.validationRefresh
        await this.refreshCluster({ fence })
        fence.check()
        if (this.validationRefresh === obligation) this.validationRefresh = null
      },
      (fence) => fence
    )

    this.offsetManager = null
    /**
     * Monotonic invalidation version for the inputs of node selection (leaders, preferred
     * replicas, assignment). A prepared snapshot compares one integer to it. Never reset, so a
     * snapshot of an older run can never look current.
     * @type {number}
     */
    this.routingVersion = 0
    /**
     * Advances when the assignment, ownership or a topic incarnation is replaced, not for an
     * ordinary leader move: it tells a prepared request that can still be sent after a routing
     * change from one whose positions were replaced.
     * @type {number}
     */
    this.preparationEpoch = 0
    /**
     * The pending metadata refresh for partitions without a leader, shared by every node's fetcher
     * of the current run, and when the last one started (`performance.now()`). `error` is its
     * failure, once it failed.
     * @type {{ promise: Promise<void>, error?: Error }|null}
     */
    this.leaderlessRefresh = null
    this.leaderlessRefreshAt = -Infinity
    this.leaderlessRefreshInterval =
      retry?.initialRetryTime ?? createRetry.defaults.initialRetryTime
    this.subscriptionState = new SubscriptionState({
      onAssignmentChange: () => this.preparationChanged(),
    })
    this.idleWait = this.createRun()

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
   * @param {{ rebalanceCoordinator: Object, heartbeatManager: Object,
   *   onHeartbeatFatal?: (error: Error) => void }} deps `onHeartbeatFatal` is forwarded to a
   *   membership that hands its fatal heartbeat answers to the run
   * @returns {void}
   */
  attach({ rebalanceCoordinator, heartbeatManager, onHeartbeatFatal }) {
    this.rebalanceCoordinator = rebalanceCoordinator
    this.groupMembership.attach({
      rebalanceCoordinator,
      heartbeatManager,
      onHeartbeatFatal,
      commitBeforeRelease: (...args) => this.commitBeforeRelease(...args),
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

  /**
   * The member id a restarted run should rejoin under, so a crash that left the coordinator's
   * membership in place does not orphan it for a session timeout.
   * @returns {string|null} the classic member id, joined or pending; null under any other protocol
   */
  retainableMemberId() {
    return this.kind === 'classic' ? this.groupMembership.retainableMemberId() : null
  }

  get memberId() {
    return this.groupMembership.memberId
  }

  set memberId(value) {
    this.groupMembership.memberId = value
  }

  /**
   * Forgets the member id, for an UNKNOWN_MEMBER_ID answer or a local discard.
   * @param {number} [revision] the membership revision the answered request began under; the
   *   current membership when absent
   * @returns {boolean} whether the id was forgotten
   */
  forgetMemberId(revision) {
    return this.groupMembership.forgetMemberId(revision)
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
   * which can reach the protocol object. `'manual'` is the local assignment kind: it never joins.
   * @returns {'classic'|'consumer'|'manual'}
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
   * Whether a heartbeat call would do nothing now: none in flight to join and none due. Only the
   * classic membership answers; any other is never idle, so its callers always make the call.
   * @param {number} interval the interval the call would pass
   * @returns {boolean}
   */
  heartbeatIdle(interval) {
    return this.groupMembership.heartbeatIdle?.(interval) ?? false
  }

  /**
   * Whether the membership holds a target assignment that still differs from what this member
   * owns, counting only what a round's apply would keep. Always false under classic. Unresolved
   * topic ids and assigned topics outside a client-pattern subscription deliberately do NOT make it
   * true - either would otherwise keep the rebalance chain running for ever.
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
   * Creates one run's idle state and subscribes it to routing changes. The run owns the
   * unsubscribe: disposing it removes the listener synchronously, so a stopped, restarted or
   * disposed consumer leaves none behind.
   * A cluster without `onRoutingChange` publishes nothing: such a run is woken only by its own
   * lifecycle events.
   * @private
   * @returns {IdleWait} an open run with its routing subscription registered on its fence
   */
  createRun() {
    const run = new IdleWait()
    const unsubscribe = this.cluster.onRoutingChange?.(() => {
      if (!run.fence.closed) this.routingChanged()
    })
    if (unsubscribe) run.fence.register(unsubscribe)
    run.fence.register(this.operations.onRelease(() => run.wake()))
    return run
  }

  /**
   * Publishes a change to the inputs of node selection: advances the routing version so every
   * prepared snapshot becomes stale, then wakes current idle waiters. Neither saves a wake.
   * @returns {void}
   */
  routingChanged() {
    this.routingVersion += 1
    this.idleWait.wake()
  }

  /**
   * Publishes a replacement of the assignment, ownership or a topic incarnation: a preparation
   * that began earlier can no longer be sent, and routing is stale as well.
   * @returns {void}
   */
  preparationChanged() {
    this.preparationEpoch += 1
    this.routingChanged()
  }

  /**
   * Creates fresh run-owned idle state after the previous run was retired.
   * @returns {IdleWait} the new run, whose `fence` closes when this run is stopped or retired
   */
  startIdleWait() {
    this.rebalanceHooks?.retire({ run: this.idleWait }, consumerStoppingError())
    this.roundContext?.retire(consumerStoppingError())
    this.leaderUnionWatch?.clear()
    this.retireLeaderlessRefresh()
    this.idleWait.dispose()
    this.idleWait = this.createRun()
    this.subscriptionState.clearPauses()
    this.retainSubscription(this.topicsSubscribed)
    return this.idleWait
  }

  /**
   * Tells the membership the consumer is shutting down, so a join waiting on the coordinator is
   * rejected rather than waiting for a heartbeat chain that is about to be cancelled, and the
   * heartbeat stops waiting on the coordinator lookup. Commits already waiting on it finish under
   * their own fence; only a forced close ends the lookup for them. Cancels pending sentinel
   * resolutions for either group protocol. Queued seeks stay queued, so the final commit still
   * leaves their partitions out; they end with the run.
   * @returns {void}
   */
  onStopRequested() {
    this.coordinatorLookup.releaseUnfenced(consumerStoppingError())
    this.rebalanceHooks?.retire({ run: this.idleWait }, consumerStoppingError())
    this.roundContext?.retire(consumerStoppingError())
    this.leaderUnionWatch?.clear()
    this.retireLeaderlessRefresh()
    this.idleWait.dispose()
    this.subscriptionState.clearPauses()
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

  /**
   * The broker nodes a fetcher runs for. With Fetch sessions it also retires the session of any
   * node that left the set; an empty set is a refresh in progress and retires nothing.
   * @returns {number[]} the current node IDs
   */
  getNodeIds() {
    const nodeIds = this.cluster.getNodeIds()
    if (this.fetchSessions && nodeIds.length) this.fetchSessions.retain(nodeIds)
    return nodeIds
  }

  /**
   * Closes this run's positive-ID Fetch sessions, best effort and in parallel, within the close
   * that owns the call. Called after the fetchers stopped.
   * @param {{deadlineAt?: number, guard?: () => Error|null}} [scope] the close's original deadline
   *   and identity refusal; without a finite deadline the close is capped at two seconds
   * @returns {Promise<void>} resolves when every close settled or the budget ended; never rejects
   */
  async closeFetchSessions(scope) {
    if (this.fetchSessions) await this.fetchSessions.closeAll(scope)
  }

  /**
   * Retires every Fetch session locally for a forced close, sending nothing: a fetcher may still
   * hold a request, and no new network work races it.
   * @returns {void}
   */
  retireFetchSessions() {
    this.fetchSessions?.retireAll()
  }

  /**
   * Cancels the pending coordinator lookup for every caller, for a forced close: a commit still
   * waiting on it fails with `error` and nothing is installed or written when it is answered.
   * @param {Error} error the terminal reason
   * @returns {void}
   */
  retireCoordinatorLookup(error) {
    this.coordinatorLookup.retire(error)
  }

  /**
   * Sends one session-closing Fetch to a node under the close's operation: the lookup, the
   * connection and the pre-write check all end at that operation's deadline.
   * @private
   * @param {number} nodeId the node that holds the session
   * @param {{sessionId: number, sessionEpoch: number, topics: [], forgottenTopics: []}} fields
   *   the close request: a positive session ID and the final epoch, with no partitions
   * @param {import('./groupProtocol/interface').ConsumerOperation} operation the close scope
   * @returns {Promise<void>}
   * @throws {Error} the operation's refusal or the transport failure; the caller logs it
   */
  async sendSessionClose(nodeId, fields, operation) {
    const broker = await this.cluster.findBroker({ nodeId, fence: operation })
    operation.check()
    await broker.fetch({
      ...fields,
      guard: operation.guard,
      maxWaitTime: 0,
      minBytes: 0,
      maxBytes: this.maxBytes,
      isolationLevel: this.isolationLevel,
      topicIds: new Map(),
      rackId: this.rackId,
      checkCrcs: this.checkCrcs,
      maxDecompressedBatchBytes: this.maxDecompressedBatchBytes,
    })
  }

  /**
   * Connects the cluster and loads its first metadata. A run's fence ends the connect and the first
   * refresh when the run is retired meanwhile, so a stop that lands during a held connect leaves
   * nothing joined.
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the run's terminal fence
   * @returns {Promise<void>}
   * @throws {Error} the cluster's failure, or the fence's close error
   */
  async connect({ fence } = {}) {
    await this.cluster.connect(...boundTo(fence))
    fence?.check()
    this.instrumentationEmitter.emit(CONNECT)
    // A pattern consumer's first running discovery is the observation that starts continuity, so it
    // must not be skipped because the metadata happens to be fresh.
    await (this.subscription
      ? this.refreshCluster({ fence })
      : this.cluster.refreshMetadataIfNecessary(...boundTo(fence)))
  }

  /**
   * One metadata refresh on the cluster. A client-pattern consumer passes its subscription, the
   * run's currency check and the observer, so an accepted response is evaluated and observed once
   * inside its install and the newest desired revision is handed on afterwards. Every refresh lets
   * a partition without a leader through as leader -1, so routing skips it; a strict refresh would
   * fail until its retries end and take the fetch loop down with it.
   * @param {object} [options]
   * @param {Generation} [options.fence] the run's terminal fence, binding the refresh to the run
   * @param {boolean} [options.ifNecessary=false] refresh only when the cache is missing or expired
   * @returns {Promise<void>}
   * @throws {Error} the refresh's failure or the fence's close error
   */
  async refreshCluster({ fence, ifNecessary = false } = {}) {
    if (!this.subscription) {
      const options = { ...(fence && { fence }), allowLeaderless: true }
      await (ifNecessary
        ? this.cluster.refreshMetadataIfNecessary(options)
        : this.cluster.refreshMetadata(options))
      return
    }

    const run = this.idleWait
    const isCurrent = () => this.idleWait === run && !run.disposed
    const options = {
      ...(fence && { fence }),
      allowLeaderless: true,
      metadataScope: this.subscription,
      isCurrent,
      onMetadata: (metadata) => this.observeDiscovery(metadata),
    }

    await (ifNecessary
      ? this.cluster.refreshMetadataIfNecessary(options)
      : this.cluster.refreshMetadata(options))

    if (this.discovered) {
      const snapshot = this.discovered
      this.discovered = null
      await this.updateSubscription(snapshot, { isCurrent })
    }
  }

  /**
   * Evaluates and observes one accepted full response, synchronously inside its install. Evaluates
   * once, owes the reset of every name that returned or appeared without a known equal incarnation
   * before any of them can read a group offset, and keeps the newest snapshot for
   * `updateSubscription`. A debt that is already owed for the same incarnation is left as it is.
   * @param {object} metadata the accepted response
   * @returns {void}
   */
  observeDiscovery(metadata) {
    this.discovered = this.subscription.evaluate(metadata)
    for (const { topic, topicId } of this.subscription.observe(metadata)) {
      if (this.obligations.current(topic) === topicId) continue
      this.obligations.owe(topic, topicId)
      this.operations.touchTopic(topic)
      this.invalidateTopic(topic)
    }
  }

  /**
   * Hands the newest desired subscription to the membership. Nothing is published or pruned here:
   * the applied revision changes only when a round applies the pinned one, at the next control
   * opportunity, so old handlers and their final commits keep the configuration they started with.
   * @param {{revision: number, names: string[]}} snapshot a desired snapshot from `Subscription#evaluate`
   * @param {object} [options]
   * @param {() => boolean} [options.isCurrent] whether the run that discovered it is still current
   * @returns {Promise<void>}
   */
  async updateSubscription(snapshot, { isCurrent = () => true } = {}) {
    if (!isCurrent() || snapshot.revision === this.notifiedRevision) return
    this.notifiedRevision = snapshot.revision
    this.groupMembership.updateSubscription(snapshot)
  }

  /**
   * Whether the desired subscription differs from the applied one. A level, not an event: it stays
   * true until a round applies, so a request ignored during a round still gets its following round.
   * @returns {boolean}
   */
  subscriptionPending() {
    return this.subscription?.needsApply() ?? false
  }

  /**
   * At a fetch opportunity, asks for one ordinary revoked reconciliation while the desired
   * subscription is unapplied and no round is in flight. A round already running applies the pin it
   * took and the coordinator's following round picks up anything newer.
   * @returns {void}
   */
  checkSubscriptionChange() {
    const coordinator = this.rebalanceCoordinator
    if (!this.subscriptionPending() || !coordinator || coordinator.isRebalancing()) return
    coordinator.requestRejoin({
      kind: 'revoked',
      reason: 'subscription-changed',
      error: new KafkaJSError('The subscription changed'),
    })
  }

  /**
   * Applies one sync result to this member's local state and reports what changed.
   *
   * Filters out topics the member is not subscribed to, refreshes metadata once when the
   * assignment names a partition the member has never heard of, and replaces the assignment.
   * Emits `RECEIVED_UNSUBSCRIBED_TOPICS` and warns for filtered names under classic only.
   * Under the consumer protocol a target can lag the round subscription, so filtered names are
   * logged at debug level. Java accepts resolved target assignments without the classic assignment
   * check, but separately makes explicitly unsubscribed topics non-fetchable. Keep this filter:
   * suppressing the event must not enable fetching outside the round subscription.
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
   * Under the consumer protocol only the LOST round renews an ownership that carries a loss,
   * whatever learned it (a heartbeat, a commit, a poll expiry): any other apply is refused with that
   * loss before it changes anything, so the runner's LOST round reports and discards it first.
   *
   * @param {{ assignment: Object, userData?: Buffer|null }} result the decoded sync result
   * @param {{ rebuildOffsets?: boolean, lost?: boolean, round?: object, additionHold?: object }}
   *   [options] `rebuildOffsets: false` selects the incremental apply; it still builds the first
   *   manager, because there is nothing to keep. `lost` marks the LOST round's apply.
   *   `additionHold` is the round's token for the partitions this apply adds: they are held before
   *   they are published and the caller releases the token once its assigned callback has returned.
   * @returns {Promise<import('./groupProtocol/interface').RebalanceResult>}
   * @throws {Error} the round's refusal, the fatal error, or the loss the ownership carries
   */
  async applyRebalanceResult(
    result,
    { rebuildOffsets = true, lost = false, round, additionHold } = {}
  ) {
    const check = () => {
      const membershipError =
        this.kind === 'consumer' &&
        (this.groupMembership.state === 'FATAL'
          ? this.groupMembership.fatalError
          : this.groupMembership.state === 'FENCED' || !lost
            ? this.ownership?.refusal(this.ownership.token())?.cause
            : null)
      const error = round?.guard() || membershipError
      if (error) {
        round?.retire(error)
        throw error
      }
    }
    check()
    const { groupId, generationId, memberId, coordinator } = this
    const topicsSubscribed = round?.pin ? round.pin.names : this.topicsSubscribed
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

      if (this.kind === 'consumer') {
        this.logger.debug('Dropping assigned topics outside the round subscription', payload)
      } else {
        this.instrumentationEmitter.emit(RECEIVED_UNSUBSCRIBED_TOPICS, payload)
        this.logger.warn('Consumer group received unsubscribed topics', {
          ...payload,
          helpUrl: websiteUrl(
            'docs/faq',
            'why-am-i-receiving-messages-for-topics-im-not-subscribed-to'
          ),
        })
      }
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
      const known = knownPartitions && new Set(knownPartitions)
      const isAwareOfAllAssignedPartitions = assignedPartitions.every((partition) =>
        known.has(partition)
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
        await this.cluster.refreshMetadata({ guard: round?.guard })
        check()
        this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
        break
      }
    }

    const { added, revoked } = this.groupMembership.diffAssignment(currentMemberAssignment)

    // Before anything prunes them, and before `updateAssignment` replaces the membership the
    // offset manager commits under.
    if (!rebuildOffsets && revoked.length) {
      if (this.rebalanceHooks) await this.releaseRevoked(revoked, round)
      else await this.commitBeforeRelease(revoked, { round })
      check()
    }

    // Narrowed before the apply mutates anything: a revoked partition must stop being advertised
    // and stop accepting resolved offsets the instant the apply decides it is gone. `setOwned`
    // below is the widening half.
    check()
    this.ownership?.renew()
    this.coordinatorLookup.detach()
    if (round?.pin) this.publishSubscription(round.pin)
    this.topics = currentMemberAssignment.map(({ topic }) => topic)
    this.groupMembership.narrowOwned(currentMemberAssignment)

    const memberAssignment = dictionary(
      currentMemberAssignment.map(({ topic, partitions }) => [topic, partitions])
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
        offsets: this.privateOffsets,
        ownership: this.ownership,
        onCommitError: (error) => {
          const verdict = this.rebalanceCoordinator?.classify(error, REBALANCE_PATHS.OFFSET_COMMIT)
          if (verdict?.kind === 'lost') this.rebalanceCoordinator.requestRejoin(verdict)
        },
        obligations: this.obligations,
        membershipProtocol: this.kind,
        staleEpochTimeout: this.rebalanceTimeout,
        // Live reads, not the snapshot above: under the consumer protocol every commit and offset
        // fetch needs the epoch the heartbeats install, also between applies, and the fetch retry
        // must abandon its deadline the moment the runner is asked to stop or a rebalance is
        // waiting for this fetch round to end. The rebalance coordinator is attached after this
        // group is built, so it is read at call time.
        membership: () => ({
          generationId: this.generationId,
          memberId: this.memberId,
          coordinator: this.coordinator,
          revision: this.groupMembership.membershipRevision,
        }),
        findCoordinator: (fence, guard) => {
          if (this.kind === 'classic') return this.groupMembership.connect({ fence, guard })
          if (this.kind === 'consumer') return this.groupMembership.connect(undefined, fence, guard)
          return this.coordinatorLookup.find(fence)
        },
        isStopping: () => this.isStopping(),
        seeks: {
          clock: () => this.acquireClock,
          changedSince: (since) =>
            this.lastSoughtAt > since || this.lastAcquiredAt > since || this.hasSeekGates(),
          isStaleSince: (topic, partition, since) => this.isStaleSince(topic, partition, since),
          isGated: (topic, partition) => this.hasSeekGates() && this.isSeekGated(topic, partition),
        },
        isRebalancePending: () => Boolean(this.rebalanceCoordinator?.verdict),
        markPolled: () => this.rebalanceCoordinator?.pollTimer.poll(),
        captureResetContext: (topic, partition, manager, preparation) =>
          this.captureResetContext(topic, manager, preparation),
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
        }
      }

      this.positionValidator.forgetPartitions(revoked)
    }

    // Wholesale in both branches: its members are broker node ids, not partitions.
    this.validationUnusableLogged.clear()

    if (rebuildOffsets) {
      this.retireAcquisitions()
      this.acquired.clear()
      this.haltedPartitions.clear()
    }
    for (const { topic, partitions } of revoked) {
      for (const partition of partitions) {
        const key = `${topic}|${partition}`
        const acquired = this.acquired.get(key)
        if (acquired) acquired.retired = true
        this.acquired.delete(key)
        this.haltedPartitions.delete(key)
        this.seekOffset.pop(topic, partition)
      }
    }
    for (const { topic, partitions } of currentMemberAssignment) {
      for (const partition of partitions) {
        const key = `${topic}|${partition}`
        if (!this.acquired.has(key)) {
          const acquiredAt = (this.lastAcquiredAt = ++this.acquireClock)
          this.acquired.set(key, {
            acquiredAt,
            changedAt: acquiredAt,
            recreatedAt: 0,
            soughtAt: 0,
            retired: false,
          })
          if (this.seekOffset.size !== 0 && this.seekOffset.has(topic, partition)) {
            this.markSought(topic, partition)
          }
        }
      }
    }
    if (additionHold) {
      this.subscriptionState.hold(rebuildOffsets ? currentMemberAssignment : added, additionHold)
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
    this.leaderUnionWatch.arm(result.unionSnapshot)
    this.pruneAssignmentState()

    return { assignment: currentMemberAssignment, added, revoked }
  }

  /**
   * Applies the manual selection locally: no join, no group round, no events and no deadline. The
   * run's identity and idle state are checked before every side effect and after the awaited apply,
   * so a stop or a replacement run changes nothing here.
   * @returns {Promise<void>} resolves once the selection is acquired and visible
   * @throws {Error} `CONSUMER_STOPPING` when the run was retired before or during the apply
   */
  async applyManualAssignment() {
    const run = this.idleWait
    const guard = () =>
      this.idleWait !== run || run.disposed || this.isStopping() || !this.runIdentity?.isCurrent()
        ? consumerStoppingError()
        : null
    const refuse = () => {
      const error = guard()
      if (error) throw error
    }
    refuse()
    this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
    const result = await this.groupMembership.rebalance()
    refuse()
    await this.applyRebalanceResult(result, {
      round: { guard, retire: () => {}, fence: run.fence },
    })
    refuse()
  }

  /**
   * The payload every awaited callback receives: the event schema, copied at the stage it is built.
   * @param {Array<import('./groupProtocol/interface').TopicPartitions>} partitions the callback's set
   * @returns {{ groupId: string, memberId: string|null, generationId: number|null, partitions: Array }}
   */
  hookPayload(partitions) {
    return {
      groupId: this.groupId,
      memberId: this.memberId,
      generationId: this.generationId,
      partitions: partitions.map(({ topic, partitions }) => ({
        topic,
        partitions: [...partitions],
      })),
    }
  }

  /**
   * The immutable identity one callback invocation carries: the run and round, the membership
   * token as it is now (a lost one stays lost) and a copy of each named partition's acquisition.
   * @param {object} round the captured round
   * @param {Array<import('./groupProtocol/interface').TopicPartitions>} partitions the callback's set
   * @returns {{ run: object, round: object, membership: number, acquisitions: object[] }}
   */
  hookIdentity(round, partitions) {
    const acquisitions = []
    for (const { topic, partitions: ids } of partitions) {
      for (const partition of ids) {
        acquisitions.push({
          topic,
          partition,
          assignmentEpoch: this.assignmentEpoch,
          acquiredAt: this.acquired.get(`${topic}|${partition}`)?.acquiredAt ?? null,
          topicId: this.cluster.topicId?.(topic),
        })
      }
    }
    return { run: round.run, round, membership: this.ownership.token(), acquisitions }
  }

  /**
   * Compares the partitions a commit names with the original cells of the callback invocation that
   * issues it. A named partition the invocation holds a cell for must still have the same
   * assignment, acquisition and topic incarnation; one it holds no cell for keeps the ordinary
   * explicit-offset rules.
   * @param {object} context the invocation's frozen context
   * @param {Array<{topic: string, partitions: Array<{partition: number}>}>} topics the commit's named partitions
   * @returns {KafkaJSCommitFailedError|null} a refusal that writes nothing, or null
   */
  hookCommitRefusal({ acquisitions }, topics = []) {
    for (const { topic, partitions } of topics) {
      for (const { partition } of partitions) {
        const cell = acquisitions.find((one) => one.topic === topic && one.partition === partition)
        if (
          cell &&
          (cell.assignmentEpoch !== this.assignmentEpoch ||
            cell.acquiredAt !== (this.acquired.get(`${topic}|${partition}`)?.acquiredAt ?? null) ||
            cell.topicId !== this.cluster.topicId?.(topic))
        ) {
          return new KafkaJSCommitFailedError(
            "The callback's acquisition of the partition changed before the commit",
            { reason: 'stale-assignment' }
          )
        }
      }
    }
    return null
  }

  /**
   * Calls one awaited callback within the round's remaining budget. An ordinary throw is recorded
   * as the round's first listener failure and the round carries on; a retired or expired round
   * rethrows the round's own reason, so a stop stays a stop and a timeout stays typed.
   * @param {object} round the captured round
   * @param {'onPartitionsRevoked'|'onPartitionsAssigned'|'onPartitionsLost'} name the callback
   * @param {Array<import('./groupProtocol/interface').TopicPartitions>} partitions the callback's set
   * @param {object} identity the invocation identity from `hookIdentity`
   * @returns {Promise<void>} resolves without a listener, or once the callback has settled
   * @throws {Error} the round's refusal, or a non-ordinary callback failure
   */
  async invokeHook(round, name, partitions, identity) {
    if (!this.rebalanceHooks?.has(name)) return
    try {
      await this.rebalanceHooks.invoke(name, this.hookPayload(partitions), {
        deadlineAt: round.roundDeadlineAt,
        identity,
      })
    } catch (error) {
      const refusal = round.guard()
      if (refusal) throw refusal
      if (!(error instanceof KafkaJSRebalanceListenerError) || error.reason !== 'threw') throw error
      if (!this.listenerFailures.has(round)) this.listenerFailures.set(round, error)
    }
  }

  /**
   * @param {object} round the captured round
   * @returns {Error|undefined} the first ordinary callback failure of this round, cleared
   */
  takeListenerFailure(round) {
    const failure = this.listenerFailures.get(round)
    this.listenerFailures.delete(round)
    return failure
  }

  /**
   * A listener round's revoke half of a cooperative or consumer-protocol apply: new admission to the
   * revoked partitions stops, only their running handlers are awaited (when a revoked callback
   * exists), the eligible final commit runs, then the revoked callback. The hold is this call's own
   * token and is released here, immediately before the caller narrows ownership.
   * @private
   * @param {Array<import('./groupProtocol/interface').TopicPartitions>} revoked the revoked delta
   * @param {object} round the captured round
   * @returns {Promise<void>}
   * @throws {Error} typed quiescence expiry, the round's refusal or a non-ordinary failure
   */
  async releaseRevoked(revoked, round) {
    const state = this.subscriptionState
    const hold = Object.freeze({})
    const identity = this.hookIdentity(round, revoked)
    state.hold(revoked, hold)
    try {
      if (this.rebalanceHooks.has('onPartitionsRevoked')) {
        await this.rebalanceCoordinator.runner.waitForPartitions(revoked, {
          deadlineAt: round.roundDeadlineAt,
          identity,
        })
      }
      await this.commitBeforeRelease(revoked, { round })
      await this.invokeHook(round, 'onPartitionsRevoked', revoked, identity)
      const refusal = round.guard()
      if (refusal) throw refusal
    } finally {
      this.releaseHold(round, hold)
    }
  }

  /**
   * Releases one hold token and wakes the captured run, never a replacement, when the release made
   * work runnable.
   * @param {object} round the captured round the token belongs to
   * @param {object} hold the exact token
   * @returns {void}
   */
  releaseHold(round, hold) {
    if (
      this.subscriptionState.release(hold) &&
      this.idleWait === round.run &&
      !round.run.disposed
    ) {
      round.run.wake()
    }
  }

  /**
   * Captures the run and one monotonic round budget before any round callback.
   * @returns {object} immutable identity plus guarded remaining-time waits
   * @throws {Error} an expired wait rejects with the existing retriable join-deadline error
   */
  beginRebalanceRound() {
    this.roundContext?.retire()
    this.leaderUnionWatch?.clear()
    const run = this.idleWait
    const leaderAttempt = Symbol('leader attempt')
    const roundDeadlineAt =
      performance.now() +
      (this.groupMembership?.joinBudget?.(this.rebalanceTimeout) ?? this.rebalanceTimeout)
    const expired = joinDeadlineError({
      rebalanceTimeoutMs: this.rebalanceTimeout,
    })
    const waiters = new Set()
    const fence = new Generation()
    // Pinned before the round's first await and kept through its retries and its apply.
    const pin = this.subscription?.pin() ?? null
    let retired = null
    let issued = false
    let currentAttempt = leaderAttempt
    const round = {
      run,
      leaderAttempt,
      roundDeadlineAt,
      /** Closed with the round, so work started for it stops dialling and retrying. */
      fence,
      /** The desired subscription this round joins and applies; null without a client pattern. */
      pin,
      allowAutoTopicCreation:
        this.regexSubscription != null || this.subscribedTopicRegex != null || this.subscription
          ? false
          : this.cluster.allowAutoTopicCreation,
      /**
       * Captures a unique retry identity without renewing the round deadline.
       * @returns {object} guarded leader context; the first keeps the pre-hook identity
       */
      attempt: () => {
        const token = issued ? Symbol('leader attempt') : leaderAttempt
        issued = true
        currentAttempt = token
        const guard = () => round.guard() || (currentAttempt !== token ? expired : null)
        return Object.freeze({
          ...round,
          leaderAttempt: token,
          guard,
          wait: (work) =>
            round.wait(async () => {
              const before = guard()
              if (before) throw before
              const result = await work()
              const after = guard()
              if (after) throw after
              return result
            }),
        })
      },
      /** @returns {Error|null} captured retirement or actual-time refusal */
      guard: () =>
        retired ||
        (this.idleWait !== run || run.disposed || this.isStopping()
          ? consumerStoppingError()
          : this.roundContext !== round || performance.now() >= roundDeadlineAt
            ? expired
            : null),
      /** @param {Error} [error] terminal reason; first reason wins @returns {void} */
      retire: (error = expired) => {
        if (retired) return
        retired = error
        if (pin) this.subscription.abandon(pin)
        fence.close(error)
        for (const reject of waiters) reject(error)
        waiters.clear()
      },
      /**
       * Sleeps within the original budget and removes the owned timer on every exit.
       * @param {number} ms requested backoff or polling delay in milliseconds
       * @returns {Promise<void>} resolves after the delay while the round remains current
       * @throws {Error} stop, replacement or original round expiry
       */
      delay: async (ms) => {
        let timer
        try {
          await round.wait(
            () =>
              new Promise((resolve) => {
                timer = setTimeout(resolve, ms)
              })
          )
        } finally {
          clearTimeout(timer)
        }
      },
      /**
       * Bounds one continuation with the unchanged round deadline.
       * @param {() => Promise<*>} work lazy work, never started after refusal
       * @returns {Promise<*>} original result or failure
       * @throws {Error} original work failure, stop, replacement or join expiry
       */
      wait: async (work) => {
        const check = () => {
          const error = round.guard()
          if (error) throw error
        }
        check()
        let timer, rejectWait
        const stopped = new Promise((_, reject) => {
          rejectWait = reject
          waiters.add(reject)
          timer = setTimeout(
            () => round.retire(expired),
            Math.max(0, roundDeadlineAt - performance.now())
          )
        })
        const task = Promise.resolve()
          .then(() => {
            check()
            return work()
          })
          .then((result) => {
            check()
            return result
          })
        try {
          return await Promise.race([task, stopped])
        } finally {
          waiters.delete(rejectWait)
          clearTimeout(timer)
        }
      },
    }
    this.roundContext = Object.freeze(round)
    return round
  }

  /**
   * Joins the group and syncs, retrying the whole sequence on a rebalance-class error.
   * `REBALANCE_IN_PROGRESS` is not a retriable protocol error on its own - the member has to go
   * back through FindCoordinator and JoinGroup - so it is re-thrown as a retriable
   * `KafkaJSError` to restart the sequence through the retrier.
   *
   * For a classic member, a lost connection (refused, reset, closed, timed out) or a coordinator
   * that moved, is unavailable or is still loading the group (14, 15, 16) ends the retrier's
   * attempt; the sequence then starts again after the retrier's first delay, looking the
   * coordinator up anew, for as long as that outage is shorter than the session timeout, counted
   * on a monotonic clock from its first failure. A longer outage fails the call with its error as
   * before. A stop requested meanwhile fails it as a stop, which the round treats as no crash,
   * whether it is seen when such an attempt fails (even past the session timeout) or after the
   * pause. A consumer-protocol member keeps the retrier's counted attempts.
   *
   * Applying the result and reporting the round belong to the caller: this method performs one
   * exchange and returns what it decoded.
   * @returns {Promise<{ assignment: Object, userData: Buffer|null }>} the decoded sync result
   * @throws {Error} what the join retrier gave up with, when it is not a loss this method rides out
   *   or the outage has lasted the session timeout; `CONSUMER_STOPPING` when a stop came during one
   * @param {object} [round] original pre-callback round identity and deadline
   */
  async joinAndSync(round = this.beginRebalanceRound()) {
    if (this.kind === 'consumer') return this.joinAndSyncAttempt(round)
    const outage = new Outage(this.sessionTimeout)

    while (true) {
      try {
        return await this.joinAndSyncAttempt(round)
      } catch (e) {
        if (
          this.isStopping() &&
          (isConnectionLoss(e) ||
            (e != null && (isStaleCoordinator(e) || e.code === COORDINATOR_LOAD_IN_PROGRESS_CODE)))
        ) {
          throw consumerStoppingError()
        }
        if (round.guard() || !outage.absorbs(e)) throw e

        const delay = this.backoff.initial()
        this.logger.warn('Lost the group coordinator while joining, trying again', {
          groupId: this.groupId,
          memberId: this.memberId,
          error: describeThrown(e),
          delay,
        })
        await round.delay(delay)
        if (this.isStopping()) throw consumerStoppingError()
      }
    }
  }

  /**
   * One run of the join retrier: JoinGroup and SyncGroup, retried on a rebalance-class error.
   * @private
   * @returns {Promise<{ assignment: Object, userData: Buffer|null }>} the decoded sync result
   * @throws {Error} what the retrier gave up with
   * @param {object} round original round; retries create attempts without renewing its time
   */
  joinAndSyncAttempt(round) {
    return this.retrier(async (bail) => {
      const { membershipRevision } = this.groupMembership
      const attempt = round.attempt()
      const topicsSubscribed = attempt.pin ? [...attempt.pin.names] : this.topicsSubscribed
      try {
        return await attempt.wait(() =>
          this.groupMembership.rebalance({
            topics: this.topics,
            topicsSubscribed,
            beforeSync: () => {
              const refusal = attempt.guard()
              if (refusal) throw refusal
              this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic(
                attempt.pin?.names
              )
            },
            growSubscription: (names) => {
              const refusal = attempt.guard()
              if (refusal) throw refusal
              return this.growSubscription(names)
            },
            round: attempt,
          })
        )
      } catch (e) {
        const refusal = attempt.guard()
        if (refusal || isHeartbeatFatal(e)) return bail(e)
        if (isRebalancing(e)) {
          throw new KafkaJSError(e)
        }

        if (e.type === 'UNKNOWN_MEMBER_ID') {
          // KIP-848 members keep their id for life: the coordinator answers 25 for an epoch it no
          // longer recognises, not for an id it wants replaced.
          if (this.kind === 'classic') {
            this.forgetMemberId(membershipRevision)
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
        // timeout to answer. A retry replaces only its token within the original round budget;
        // the round's own expiry is terminal for this chain.
        if (e.type === 'JOIN_DEADLINE_EXCEEDED') {
          throw new KafkaJSError(e)
        }

        bail(e)
      }
    }, round)
  }

  /**
   * Applies a subscription lifecycle, never a rebalance assignment. The names also become the
   * consumer's subscribed names in its group's ownership record.
   * @param {string[]} topics accepted names with configuration already installed
   * @returns {void}
   */
  retainSubscription(topics) {
    this.topicsSubscribed = [...topics]
    this.obligations?.claimSubscription(this.topicsSubscribed)
    this.subscriptionState.retainTopics(topics)
    const live = new Set(topics)
    for (const topic of Object.keys(this.topicConfigurations)) {
      if (!live.has(topic)) delete this.topicConfigurations[topic]
    }
  }

  /**
   * Makes a round's pinned subscription the applied one, after the old owner's handlers, final
   * commit and revoke work are done. Replaces the configuration the offset manager reads, then
   * retires the pause intent and configuration of names that left. Synchronous, so no reader sees
   * a half-applied revision.
   * @param {{revision: number, names: string[], topicConfigurations: Map}} pin the round's pinned snapshot
   * @returns {void}
   * @throws {Error} the consumer-stopping error when the run was retired and the pin refuses
   */
  publishSubscription(pin) {
    if (!this.subscription.apply(pin)) throw consumerStoppingError()
    this.topicConfigurations = Subscription.configurationsOf(pin)
    if (this.offsetManager) this.offsetManager.topicConfigurations = this.topicConfigurations
    this.retainSubscription(pin.names)
    this.obligations.retain(new Set([...pin.names, ...this.subscription.desired.names]))
  }

  /**
   * Prunes assignment-owned routing after either apply, and the queued seeks of topics that are
   * neither assigned nor subscribed any more. A seek of a subscribed partition this member does
   * not hold stays queued until the partition is acquired (`seek`).
   * @returns {void}
   */
  pruneAssignmentState() {
    const assigned = this.subscriptionState.assignedPartitionSetsByTopic
    for (const topic of Object.keys(this.preferredReadReplicasPerTopicPartition)) {
      if (!assigned.get(topic)?.size) delete this.preferredReadReplicasPerTopicPartition[topic]
    }
    if (this.seekOffset.size !== 0) {
      const subscribed = new Set(this.topicsSubscribed)
      for (const [key] of this.seekOffset) {
        const [topic, partition] = JSON.parse(key)
        if (!assigned.get(topic)?.has(partition) && !subscribed.has(topic)) {
          this.seekOffset.delete(key)
        }
      }
    }
    if (this.kind === 'consumer' && this.subscribedTopicRegex != null) {
      this.retainSubscription([
        ...new Set([
          ...this.groupMembership.subscribedTopicNames,
          ...this.groupMembership.grownTopicNames,
        ]),
      ])
    }
  }

  /**
   * Widens the subscription with names a broker-computed assignment resolved to, before the apply
   * can filter them out as unsubscribed.
   *
   * First each name is registered as a cluster target on its own (without which metadata is never
   * fetched for it): a name a lookup by name does not know yet stays pending, and nothing else is
   * published for it. Then, for the registered names only, the name set and a name-keyed
   * `topicConfigurations` entry (the offset manager indexes it positionally and destructures the
   * result). A literal name subscribed before its topic existed already holds both and gets only
   * the target, once.
   *
   * NEVER pushes: `this.topics` and `this.topicsSubscribed` are the same array object at
   * construction, so a mutation would edit both.
   * @param {string[]} names newly resolved topic names
   * @returns {Promise<string[]>} the names now registered; the others are pending
   * @throws {Error} a fatal topic error of the registration (17 or 29, say)
   */
  async growSubscription(names) {
    // A client-pattern member's names come from its pinned revision: an assigned name that left it
    // is filtered by the apply, never turned into a named target.
    if (this.subscription) return names

    const unseen = names.filter(
      (name) => !this.topicsSubscribed.includes(name) || this.unregisteredTopics.has(name)
    )
    if (!unseen.length) return names

    const registered = new Set(await this.cluster.registerAssignedTopics(unseen))
    const added = unseen.filter(
      (name) => registered.has(name) && !this.topicsSubscribed.includes(name)
    )
    for (const name of registered) this.unregisteredTopics.delete(name)

    if (added.length) {
      this.topicsSubscribed = [...this.topicsSubscribed, ...added]
      this.obligations.claimSubscription(this.topicsSubscribed)
    }
    for (const name of added) {
      this.topicConfigurations[name] = this.topicConfigurations[name] || {
        ...this.regexSubscription,
      }
    }

    return names.filter((name) => registered.has(name) || !unseen.includes(name))
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
   * The request also stamps an owned partition as sought (`markSought`), at once rather than when
   * the seek is applied, so a batch fetched before it can neither resolve nor commit an offset
   * from this moment on, while the seek is pending and after it is applied.
   *
   * A seek of a subscribed partition this member does not hold is kept, the latest per partition,
   * and is applied before the first delivery once the partition is acquired, as in kafkajs 2.2.4;
   * it is dropped once its topic is neither assigned nor subscribed. Losing a partition drops the
   * seek queued while it was held: that seek was the previous owner's work.
   *
   * @param {import("../../types").TopicPartitionOffset} topicPartitionOffset
   * @returns {void} queues intent and wakes idle fetchers; resolution remains next-round work
   */
  seek({ topic, partition, offset }) {
    this.seekOffset.set(topic, partition, offset)
    this.markSought(topic, partition)
    this.idleWait.wake()
  }

  /**
   * Drops every piece of state this member holds for one topic, because that topic was deleted
   * and recreated: its offsets, its preferred read replicas, any pending seek and any halt of its
   * partitions, which belonged to the old incarnation. The corresponding reset is owed
   * (`ResetObligations`) and applied at the next offset resolution.
   *
   * A no-op before the first sync (`offsetManager` is null until then) and for a topic outside
   * the current member assignment - the second guard keeps this member from writing a key it
   * does not own into the committed-offsets map, which is shared by every consumer built from
   * the same client with the same group id.
   *
   * Precedence with `seek()`: a seek pending here (queued but not yet applied) targets the
   * *old* incarnation and is dropped along with everything else, via `seekOffset.deleteTopic()`.
   * A seek issued *after* this call returns, applied in the same `seekOffsets()` cycle, targets
   * the current incarnation and wins over the reset for that partition's local position; the
   * obligation itself settles only when a confirmed current-incarnation commit is applied
   * (`OffsetOperations#apply`), which a seek makes only with auto-commit on.
   *
   * @param {string} topic
   * @returns {void}
   */
  invalidateTopic(topic) {
    this.subscriptionState.retireTopic(topic)
    this.preparationChanged()
    if (!this.offsetManager || !Object.hasOwn(this.offsetManager.memberAssignment, topic)) {
      return
    }

    this.preferredReadReplicasPerTopicPartition[topic] = {}
    this.seekOffset.deleteTopic(topic)
    this.positionValidator.forgetTopic(topic, this.offsetManager.memberAssignment[topic])
    this.offsetManager.resetOffsetsForTopic(topic)
    for (const partition of this.offsetManager.memberAssignment[topic]) {
      this.markRecreated(topic, partition)
      this.haltedPartitions.delete(`${topic}|${partition}`)
    }
  }

  /**
   * Records that a seek, a reset or a topic recreation was applied to an owned partition, by
   * stamping it with the next value of the acquisition clock. A corruption marker fetched by a
   * request built before that stamp is then refused, even when the position came back to the same
   * offset. A partition this member does not own is left alone.
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  markChanged(topic, partition) {
    const acquired = this.acquired.get(`${topic}|${partition}`)
    if (acquired) acquired.changedAt = ++this.acquireClock
  }

  /**
   * Records that a seek was requested for an owned partition, by stamping `soughtAt` with the next
   * value of the acquisition clock, so a batch fetched under an earlier clock is known to be
   * superseded (`Runner#isSuperseded`) for good, whichever way the seek ends. Admission changes no
   * position: the cursor, epoch, committed cache and halt stay until the seek is applied, so a
   * seek that fails leaves the partition where it was. Only the latest stamp is kept, and it is
   * dropped with the partition's acquisition, so the record stays bounded by the assignment. A
   * partition this member does not own is left alone.
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  markSought(topic, partition) {
    const key = `${topic}|${partition}`
    const acquired = this.acquired.get(key)
    if (!acquired) return
    acquired.soughtAt = this.lastSoughtAt = ++this.acquireClock
  }

  /**
   * Whether a popped seek still speaks for its partition: the acquisition it was popped under is
   * the current one, no later seek was admitted and the topic was not recreated since. Only such
   * a seek may write its commit or settle the partition's gate.
   * @private
   * @param {string} key `topic|partition`
   * @param {import('./handlerContext').Acquisition|undefined} acquired the acquisition the seek
   *   was popped under
   * @param {number|undefined} admitted its `soughtAt` when popped
   * @returns {boolean}
   */
  isCurrentSeek(key, acquired, admitted) {
    return (
      acquired !== undefined &&
      acquired === this.acquired.get(key) &&
      acquired.soughtAt === admitted &&
      !(acquired.recreatedAt > admitted)
    )
  }

  /**
   * Whether a popped seek may still change its partition here: its run is this group's live run,
   * its offset manager is the current one, and the acquisition it was popped under still holds the
   * partition in the same incarnation. A newer seek admitted since does not end it; that seek
   * replaces its effect when it applies. A seek popped with no acquisition to compare needs only
   * the run and the manager.
   * @private
   * @param {string} key `topic|partition`
   * @param {{acquired?: object, admitted?: number, run: object, manager: object}} popped
   * @returns {boolean}
   */
  mayTakeEffect(key, { acquired, admitted, run, manager }) {
    return (
      run === this.idleWait &&
      !run.disposed &&
      manager === this.offsetManager &&
      (acquired === undefined ||
        (acquired === this.acquired.get(key) &&
          !acquired.retired &&
          !(acquired.recreatedAt > admitted)))
    )
  }

  /**
   * Settles a popped seek that reached its partition (it was applied, or the broker accepted its
   * commit) while it may still take effect (`mayTakeEffect`): fences answers built before it,
   * clears the partition's halt (a seek is how an application resumes a halted partition), retires
   * the position validation of the position it replaced and wakes the idle fetcher. Runs once per
   * seek, before its reservation is released.
   * @private
   * @param {string} key `topic|partition`
   * @param {{topic: string, partition: number}} seek the popped seek
   * @param {object} popped what `mayTakeEffect` compares
   * @returns {void}
   */
  settleSeek(key, { topic, partition }, popped) {
    if (popped.settled || !this.mayTakeEffect(key, popped)) return
    popped.settled = true
    if (popped.acquired) popped.acquired.changedAt = ++this.acquireClock
    this.haltedPartitions.delete(key)
    this.positionValidator.forgetTopic(topic, [partition])
    this.idleWait.wake()
  }

  /**
   * Logs the one WARN of a popped seek that failed, contained so the log cannot replace the
   * seek's error.
   * @private
   * @param {{topic: string, partition: number, offset: string}} seek the failed seek
   * @param {Error} error why it failed
   * @returns {void}
   */
  warnFailedSeek({ topic, partition, offset }, error) {
    try {
      this.logger.warn('Seek failed; the partition continues from its position before the seek', {
        groupId: this.groupId,
        topic,
        partition,
        offset,
        error: error?.message,
      })
    } catch {}
  }

  /**
   * Whether a partition was sought after `since`, the acquisition clock read when its offset was
   * selected: that offset is no longer the position the application asked for. Constant time
   * while nothing was sought since.
   * @param {string} topic
   * @param {number|string} partition
   * @param {number} since the acquisition clock at selection
   * @returns {boolean}
   */
  isSoughtAfter(topic, partition, since) {
    return this.lastSoughtAt > since && this.acquired.get(`${topic}|${partition}`)?.soughtAt > since
  }

  /**
   * The per-cell authority of a default commit selected at `since`: whether the partition was
   * sought, or acquired again, after it, or still has a seek that is not applied. A seek's
   * admission keeps the partition's position, so this gate is what keeps a pending seek's cell
   * out of every default commit, the final one included. Checked when the request is written and
   * again when its answer is published, only once something was sought or acquired since or a
   * seek is pending (`changedSince`).
   * @param {string} topic
   * @param {number|string} partition
   * @param {number} since the acquisition clock at selection
   * @returns {boolean}
   */
  isStaleSince(topic, partition, since) {
    const key = `${topic}|${partition}`
    const acquired = this.acquired.get(key)
    return (
      acquired !== undefined &&
      (acquired.acquiredAt > since ||
        acquired.soughtAt > since ||
        this.isSeekGated(topic, partition))
    )
  }

  /**
   * Whether any seek could gate a partition: a seek is queued or a reservation is held. Constant
   * time, so a round with no seek pays nothing for the per-partition check.
   * @returns {boolean}
   */
  hasSeekGates() {
    return this.seekOffset.size !== 0 || this.operations.reserved !== 0
  }

  /**
   * Whether a seek holds an owned partition back from Fetch, validation, resolution and default
   * commits: a seek of it is queued, or the reservation this acquisition took in this run and
   * incarnation is held. A reservation taken by another acquisition, run, incarnation or consumer
   * blocks only a new pop (`seekOffsets`), never this owner's fetch.
   * @param {string} topic
   * @param {number|string} partition
   * @returns {boolean} false for a partition this member does not own
   */
  isSeekGated(topic, partition) {
    const acquired = this.acquired.get(`${topic}|${partition}`)
    if (acquired === undefined) return false
    if (this.seekOffset.size !== 0 && this.seekOffset.has(topic, Number(partition))) return true
    const owner = this.operations.reservation(topic, partition)?.owner
    return (
      owner !== undefined &&
      owner.acquired === acquired &&
      owner.run === this.idleWait &&
      !(acquired.recreatedAt > owner.admitted)
    )
  }

  /**
   * The acquisition a batch prepared under `clock` was fetched under: the partition's current one,
   * unless it was acquired after `clock` or is no longer held, in which case the batch's own
   * acquisition has ended and a shared retired one stands for it.
   * @param {string} key `topic|partition`
   * @param {number} clock the preparation's acquisition clock
   * @returns {import('./handlerContext').Acquisition}
   */
  acquisitionAt(key, clock) {
    const acquired = this.acquired.get(key)
    return acquired !== undefined && acquired.acquiredAt <= clock ? acquired : RETIRED_ACQUISITION
  }

  /**
   * Retires every current acquisition, for a rebuild or the end of the run: a handler identity
   * that carries one is superseded for good, whatever acquisition replaces it.
   * @returns {void}
   */
  retireAcquisitions() {
    for (const acquired of this.acquired.values()) acquired.retired = true
  }

  /**
   * Records that the topic of an owned partition was recreated: a change as `markChanged` stamps,
   * plus the same clock value as `recreatedAt`, so a batch fetched under an earlier clock is
   * known to hold records of the old incarnation (`Runner#isSuperseded`).
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  markRecreated(topic, partition) {
    const key = `${topic}|${partition}`
    const acquired = this.acquired.get(key)
    if (!acquired) return
    acquired.changedAt = acquired.recreatedAt = ++this.acquireClock
  }

  pause(topicPartitions) {
    this.logger.info(`Pausing fetching from ${topicPartitions.length} topics`, {
      topicPartitions,
    })
    this.subscriptionState.pause(topicPartitions)
  }

  /**
   * Resumes the requested pause intent and wakes idle fetchers in this run.
   * @param {import('../../types').TopicPartitions[]} topicPartitions topics or partitions
   * @returns {void} does not clear a corruption halt or cancel an in-flight Fetch
   */
  resume(topicPartitions) {
    this.logger.info(`Resuming fetching from ${topicPartitions.length} topics`, {
      topicPartitions,
    })
    this.subscriptionState.resume(topicPartitions)
    this.idleWait.wake()
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
   * Commits through the offset manager.
   * @param {import("../../types").Offsets} [offsets]
   * @param {{ partitions?: import('./groupProtocol/interface').TopicPartitions[]|null, auto?: boolean, guard?: () => Error|null }} [options]
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
   * @param {object} [options] private scope; a rebalance `round` or a close `operation`, not both
   * @param {object} [options.round] owns the original monotonic budget, polls and transmission
   * @param {import('./groupProtocol/interface').ConsumerOperation} [options.operation] the close that
   *   owns a leave's final commit: it bounds and fences the lookup, the write, the stale-epoch wait
   *   and the local publication, and the manager and token captured at entry stay the only ones used.
   *   A runner that was retired refuses the commit whatever the scope.
   */
  async commitBeforeRelease(partitions, { round, operation } = {}) {
    if (!this.autoCommit || !this.offsetManager || !partitions.length) {
      return
    }

    const scope = round ?? this.closeScope(operation)
    const deadline = scope ? scope.roundDeadlineAt : Date.now() + this.rebalanceTimeout
    const capturedManager = this.offsetManager
    const retired = () => this.rebalanceCoordinator?.runner?.retirement ?? null
    const refusal = () => scope?.guard() || retired()
    const current = () => !refusal() && (!scope || this.offsetManager === capturedManager)

    for (;;) {
      if (!current()) return
      const manager = scope ? capturedManager : this.offsetManager
      // The manager's membership snapshot is still the previous generation at this point: it is
      // written at construction and at `updateAssignment`, and this call deliberately precedes
      // both.
      manager.useMembership({
        generationId: this.generationId,
        memberId: this.memberId,
        coordinator: this.coordinator,
      })

      try {
        if (!current()) return
        const commit = () => {
          if (!current()) return
          return manager.commitOffsets({}, { partitions, guard: refusal, fence: operation })
        }
        if (scope) await scope.wait(commit)
        else await commit()
        return
      } catch (e) {
        if (!current()) return
        if (!(await this.awaitFreshEpochFor(e, deadline, scope))) {
          if (!current()) return
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
   * A close operation as the scope of a pre-release commit, with the round's shape: its guard, its
   * cancellable wait and delay, and the monotonic deadline, which is the close's own and never later
   * than the rebalance timeout.
   * @private
   * @param {import('./groupProtocol/interface').ConsumerOperation} [operation]
   * @returns {object|undefined} undefined without an operation
   */
  closeScope(operation) {
    if (!operation) return undefined
    return {
      guard: operation.guard,
      wait: (action) => operation.wait(action),
      delay: (delay) => operation.sleep(delay),
      roundDeadlineAt: Math.min(operation.deadlineAt, performance.now() + this.rebalanceTimeout),
    }
  }

  /**
   * Whether a failed pre-release commit is worth re-sending, and if so waits until it is.
   *
   * Only a stale member epoch qualifies, and only once the coordinator has actually given this
   * member a newer one - re-sending the same epoch would draw the same answer forever.
   * @private
   * @param {Error} error
   * @param {number} deadline monotonic round deadline, or Date.now() without a round
   * @param {object} [round] captured round; cancels polling on expiry, stop or replacement
   * @returns {Promise<boolean>} false after retirement; standalone leave behavior is unchanged
   */
  async awaitFreshEpochFor(error, deadline, round) {
    if (this.kind !== 'consumer' || error.type !== STALE_MEMBER_EPOCH) {
      return false
    }

    const sentUnder = sentGeneration(error)

    while ((round ? performance.now() : Date.now()) < deadline && !this.isStopping()) {
      if (round?.guard()) return false
      if (this.generationId > 0 && this.generationId !== sentUnder) {
        return true
      }

      if (round) {
        try {
          await round.delay(STALE_EPOCH_POLL_INTERVAL)
        } catch (_) {
          return false
        }
      } else {
        await sleep(STALE_EPOCH_POLL_INTERVAL)
      }
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

  /**
   * One heartbeat through the membership's own gate.
   * @param {{ interval: number, guard?: () => Error|null, onAccepted?: () => void }} options
   *   `onAccepted` is the runner's clock effect; only the consumer protocol runs it
   * @returns {Promise<boolean|void>} void for classic and manual; under the consumer protocol, true
   *   only when a heartbeat was sent and answered with success for the membership the member holds
   */
  async heartbeat({ interval, guard, onAccepted }) {
    return this.groupMembership.heartbeat({ interval, guard, onAccepted })
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
   * With Fetch sessions on (KIP-227) a broker version of 7 or above sends only what changed since
   * the node's last accepted request, and the reply may omit partitions with nothing new. The
   * session validates the reply as a whole before anything is published, a session error code
   * only resets it, and every response row is read against this round's full desired map, never
   * the wire diff. A node whose last partition just left sends one removal request, then idles.
   * Without them the request is the plain full one, built exactly as before.
   *
   * Positions are prepared by `prepareFetch`, at most twice per call, and only that prepared
   * snapshot is built. A stop or restart of the run, or a preparation replaced under it, returns
   * an empty round without error; an idle round sleeps only while its snapshot is current.
   *
   * The run's fence follows the call to its end: every refresh, hint and recovery the response
   * starts is bound to it, and each continuation checks it before it publishes, so a response
   * still being processed at stop changes nothing in a replacement run and returns no batch.
   *
   * @param {number} nodeId the broker to fetch from
   * @returns {Promise<Batch[]>} one batch per fetched partition; empty when nothing was owed,
   *   when the round was recovered from an error, or when every response entry was dropped
   */
  async fetch(nodeId) {
    const run = this.idleWait
    const ownershipToken = this.ownership.token()
    let prepared = null
    let requestClock
    try {
      prepared = await this.prepareFetch(nodeId, run, ownershipToken)
      if (!prepared) return []

      // The request is built from the positions as they stand now; a corruption marker in its
      // answer is refused if a seek, a reset or a recreation reaches the partition after this.
      prepared.check()
      const { assignmentEpoch } = prepared
      requestClock = this.acquireClock
      const resolved = this.resolveTopicIds(this.buildFetchRequests(prepared.parts))
      const { requests, missing } = resolved
      if (missing) {
        for (const { topic, partitions } of missing) {
          for (const { partition } of partitions) {
            this.recordFetchError(topic, partition, UNKNOWN_TOPIC_ID_CODE)
          }
        }
      }
      const registry = this.fetchSessions
      // With sessions, a node whose last partitions just left still owes the broker one removal.
      const forgetting = requests.length === 0 ? registry?.active(nodeId) : undefined

      if (!requests.length && !forgetting) {
        return await this.idleFetchRound(prepared, run, missing !== null)
      }

      const { topicIds, maxVersion } = forgetting
        ? this.forgettingAddressing(forgetting)
        : { topicIds: resolved.topicIds, maxVersion: undefined }

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

      const broker = await run.fence.wait(this.cluster.findBroker({ nodeId, fence: run.fence }))
      prepared.check()

      let tracked = null
      if (registry) {
        const version = this.negotiatedFetchVersion(broker, maxVersion)
        if (version == null || version < FETCH_SESSION_MIN_VERSION) {
          registry.release(nodeId)
          if (!requests.length) return await this.idleFetchRound(prepared, run, missing !== null)
        } else {
          tracked = this.trackFetch(
            registry,
            nodeId,
            requests,
            topicIds,
            version >= FETCH_TOPIC_IDS_MIN_VERSION
          )
        }
      }

      let response
      try {
        response = await broker.fetch({
          guard: prepared.guard,
          maxWaitTime: this.maxWaitTime,
          minBytes: this.minBytes,
          maxBytes: this.maxBytes,
          isolationLevel: this.isolationLevel,
          topics: tracked ? tracked.wire.topics : requests,
          topicIds: tracked ? tracked.wire.topicIds : topicIds,
          maxVersion,
          rackId: this.rackId,
          checkCrcs: this.checkCrcs,
          maxDecompressedBatchBytes: this.maxDecompressedBatchBytes,
          sessionId: tracked?.wire.sessionId,
          sessionEpoch: tracked?.wire.sessionEpoch,
          forgottenTopics: tracked?.wire.forgottenTopics,
        })
      } catch (error) {
        // A retired run's failure may only reset the session whose epoch it left unknown: replica
        // hints and routing belong to the run that replaced it.
        if (tracked && run.fence.closed) {
          tracked.session.handleError(tracked.token)
          return []
        }
        // A stale session's failure belongs to a node or run that moved on: it recovers nothing.
        if (tracked && !this.failTrackedFetch(tracked, requests)) return []
        throw error
      }

      // A response that outlived its run is dropped whole: it must not apply hints, preferred
      // replicas or offsets to a replacement run, and the wire request is not cancelled. The
      // broker may have advanced its epoch for it, so a session still waiting on it starts over.
      if (run.fence.closed) {
        tracked?.session.handleError(tracked.token)
        return []
      }

      if (tracked) {
        // Before anything is published: a stale answer changes nothing, and a refused one resets
        // the session and is handled below its own contract.
        if (!tracked.session.current(tracked.token)) return []
        if (!tracked.session.handleResponse(tracked.token, response)) {
          await this.refuseTrackedResponse(response, run.fence)
          return []
        }
      }

      const { errorCode: topLevelErrorCode, responses, nodeEndpoints } = response

      // The top-level code is only decoded from Fetch v7 up, so an older response carries none -
      // and `failure(undefined)` is true, which is why the guard is on the field's presence.
      if (topLevelErrorCode != null && failure(topLevelErrorCode)) {
        await this.handleFetchTopLevelError(topLevelErrorCode, run.fence)
        return []
      }

      let needsRefresh = false
      const batches = []
      // Hoisted out of the per-partition loop: the response row below is the hot path, and its
      // payload must not be built once per partition per round only to be discarded.
      const debug = this.logger.debugEnabled()
      const requestsByTopic = indexFirstBy(requests, 'topic')

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

        const topicRequestData = requestsByTopic.get(topicName)

        let preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topicName]
        if (!preferredReadReplicas) {
          this.preferredReadReplicasPerTopicPartition[topicName] = preferredReadReplicas = {}
        }

        // Built once per topic, not once per partition: the loop below is the consume hot path.
        const context = {
          topicName,
          topicId: topicId ?? topicIds?.get(topicName),
          requestedPartitions:
            topicRequestData && indexFirstBy(topicRequestData.partitions, 'partition'),
          preferredReadReplicas,
          nodeEndpoints,
          batches,
          debug,
          assignmentEpoch,
          requestClock,
          prepared,
          fence: run.fence,
        }

        for (const partitionData of partitions) {
          const partitionNeedsRefresh = await this.handleFetchedPartition(partitionData, context)
          run.fence.check()
          needsRefresh = needsRefresh || partitionNeedsRefresh
        }
      }

      if (needsRefresh) {
        await this.refreshCluster({ fence: run.fence })
        run.fence.check()
      } else if (missing) {
        await this.refreshMissingTopicIds(run)
        run.fence.check()
      } else if (prepared.leaderless) {
        await this.refreshLeaderlessMetadata(run)
        run.fence.check()
      }

      for (const batch of batches) {
        batch.assignmentEpoch = assignmentEpoch
        batch.acquireClock = requestClock
        batch.ownershipKey = `${batch.topic}|${batch.partition}`
        batch.acquisition = this.acquisitionAt(batch.ownershipKey, requestClock)
      }

      return batches
    } catch (e) {
      // A retired run, or a preparation replaced under it, is a local outcome, not a failure.
      if (run.fence.closed || prepared?.isSuperseded(e)) return []
      stampOwnership(e, ownershipToken)
      if (obsoleteOwnership(this.ownership, e)) throw e
      try {
        await this.recoverFromFetch(
          e,
          run.fence,
          ...(prepared ? [this.fetchedResetContext(prepared, requestClock)] : [])
        )
      } catch (recoveryError) {
        if (run.fence.closed) return []
        throw recoveryError
      }
      return []
    }
  }

  /**
   * Waits out a round with nothing to ask this node: it sleeps while its snapshot stays current,
   * after asking for the metadata of partitions that have no leader or no topic id.
   * @private
   * @param {FetchPreparation} prepared the round's snapshot
   * @param {IdleWait} run the run that owns the round
   * @param {boolean} [missingIds=false] whether a topic was left out for a missing topic id
   * @returns {Promise<Batch[]>} always empty
   * @throws {Error} a terminal topic refusal learned for a missing id, see `refreshMissingTopicIds`
   */
  async idleFetchRound(prepared, run, missingIds = false) {
    if (missingIds) await this.refreshMissingTopicIds(run)
    else if (prepared.leaderless) await this.refreshLeaderlessMetadata(run)
    // Registers only while the snapshot is current: an invalidated one returns through the
    // fetcher's yield floor and a fresh snapshot is captured next round.
    await run.wait(
      prepared.idleDuration(this.maxWaitTime),
      () => !run.fence.closed && prepared.isCurrent()
    )
    return []
  }

  /**
   * How a removal-only request is addressed: in the mode its session last used, never decided by
   * the metadata cache, which no longer lists the partitions being removed.
   * @private
   * @param {import('./fetchSession').FetchSession} session the node's session
   * @returns {{topicIds: Map<string, string>|undefined, maxVersion: number|undefined}}
   */
  forgettingAddressing(session) {
    return session.usesTopicIds
      ? { topicIds: new Map(), maxVersion: undefined }
      : { topicIds: undefined, maxVersion: FETCH_TOPIC_NAMES_MAX_VERSION }
  }

  /**
   * The Fetch version this broker would select for a request with the same ceiling `Broker#fetch`
   * derives, without building a request.
   * @private
   * @param {import('../broker')} broker the connected node
   * @param {number|undefined} maxVersion the round's topic-name ceiling, if any
   * @returns {number|null} the version, or null when the broker is not usable for Fetch
   */
  negotiatedFetchVersion(broker, maxVersion) {
    return broker.negotiatedVersion(
      apiKeys.Fetch,
      requests.Fetch,
      maxVersion === undefined ? {} : { [apiKeys.Fetch]: maxVersion }
    )
  }

  /**
   * Builds this round's session request. The session records the full desired map, so the wire
   * carries only what changed; the caller keeps the full map for the response.
   * @private
   * @param {FetchSessions} registry the run's sessions
   * @param {number} nodeId the node being fetched
   * @param {object[]} requests the full desired entries
   * @param {Map<string, string>|undefined} topicIds the full topic name to ID mapping
   * @param {boolean} usesTopicIds whether the negotiated version is keyed by ID
   * @returns {{session: import('./fetchSession').FetchSession, token: object, wire: object,
   *   previous: Map<string, object>}} the request identity, wire fields and the cells held before
   * @throws {Error} a retired registry, or a missing topic ID in ID mode
   */
  trackFetch(registry, nodeId, requests, topicIds, usesTopicIds) {
    const session = registry.forNode(nodeId)
    const previous = session.partitions
    return { session, previous, ...session.build(requests, { topicIds, usesTopicIds }) }
  }

  /**
   * Handles a send that threw for a tracked request: a current session is reset to a full request
   * at the unchanged offsets, and the preferred replicas of its partitions are dropped, as the
   * Java client does after a failed send.
   * @private
   * @param {{session: object, token: object, previous: Map<string, object>}} tracked the request
   * @param {object[]} requests the round's full desired entries
   * @returns {boolean} whether the session was current; a stale failure changes nothing
   */
  failTrackedFetch({ session, token, previous }, requests) {
    if (!session.current(token)) return false
    let cleared = false
    const clear = (topic, partition) => {
      const replicas = this.preferredReadReplicasPerTopicPartition[topic]
      if (replicas && replicas[partition] != null) {
        delete replicas[partition]
        cleared = true
      }
    }
    for (const cell of previous.values()) clear(cell.topic, cell.partition)
    for (const { topic, partitions } of requests) {
      for (const { partition } of partitions) clear(topic, partition)
    }
    if (cleared) this.routingChanged()
    session.handleError(token)
    return true
  }

  /**
   * Applies the contract of a response the session refused. A top-level session code resets the
   * session and refreshes metadata only for a topic ID error; any other top-level code keeps its
   * ordinary classification. A response whose partitions did not match the session is dropped
   * whole, and an unknown topic ID still asks for a refresh.
   * @private
   * @param {object} response the decoded response
   * @param {import('../producer/generation')} fence the run that received it
   * @returns {Promise<void>}
   * @throws {Error} the error an unclassified top-level code maps to, or the fence's close error
   */
  async refuseTrackedResponse(response, fence) {
    if (response.errorCode) {
      await this.handleFetchTopLevelError(response.errorCode, fence, { session: true })
      return
    }

    const unknownTopic = response.responses.find(({ topicName }) => topicName == null)
    if (unknownTopic) {
      this.logger.warn('Fetch response carried an unknown topic id, refreshing metadata', {
        groupId: this.groupId,
        memberId: this.memberId,
        topicId: unknownTopic.topicId,
      })
      await this.refreshCluster({ fence })
      return
    }

    this.logger.debug('Fetch session response did not match its request, starting a new session', {
      groupId: this.groupId,
      memberId: this.memberId,
    })
  }

  /**
   * Prepares the node's partitions for one Fetch, in at most `MAX_PREPARATION_ATTEMPTS` attempts.
   *
   * Each attempt captures one node-filtered snapshot with the routing version and the batch
   * stamps it was taken under, then seeks and validates exactly those partitions. If routing
   * changed meanwhile and an attempt remains, it yields once and prepares a replacement. After the
   * last attempt it returns the snapshot that actually completed preparation, even if routing
   * moved again: it never substitutes a newer filter result or admits a partition it did not
   * prepare. The unchanged path costs one filter and scalar checks.
   *
   * Every wait is released when the run's fence closes, and every continuation checks the
   * snapshot's guard, so a pending preparation never outlives a stop or restart.
   *
   * @private
   * @param {number} nodeId the broker this round fetches from
   * @param {IdleWait} run the run that owns this call, captured at entry
   * @param {number} ownershipToken the membership identity captured before any await
   * @returns {Promise<FetchPreparation|null>} the prepared snapshot, or null when there is
   *   nothing safe to send: the run was retired or the last preparation was replaced
   * @throws {Error} a real failure of the live run, for `recoverFromFetch`
   */
  async prepareFetch(nodeId, run, ownershipToken) {
    run.fence.check()
    await run.fence.wait(
      this.validationRefresh
        ? this.refreshValidationMetadata(run.fence)
        : this.refreshCluster({ fence: run.fence, ifNecessary: true })
    )
    run.fence.check()
    this.checkSubscriptionChange()
    this.groupMembership.recheckHeldTopics?.()
    if (this.leaderUnionWatch.armed) await this.pollLeaderUnion(run.fence)
    this.checkForStaleAssignment()

    let prepared = null
    for (let attempt = 1; attempt <= MAX_PREPARATION_ATTEMPTS; attempt++) {
      while (this.validationRefresh) {
        await run.fence.wait(this.refreshValidationMetadata(run.fence))
      }
      // Captured before anything is awaited: this is the assignment the request is built from,
      // and it is what every batch this round produces is stamped with.
      const snapshot = this.captureFetchPreparation(nodeId, run, ownershipToken)
      prepared = null
      try {
        snapshot.check()
        await run.fence.wait(this.seekOffsets(snapshot.parts, snapshot))
        while (this.validationRefresh) {
          await run.fence.wait(this.refreshValidationMetadata(run.fence))
        }
        snapshot.check()
        await run.fence.wait(this.validatePositions(snapshot.parts, snapshot))
        snapshot.check()
        prepared = snapshot
      } catch (error) {
        if (run.fence.closed) return null
        if (!snapshot.isSuperseded(error)) throw error
      }

      if ((prepared && prepared.isCurrent()) || attempt === MAX_PREPARATION_ATTEMPTS) break
      await fencedDelay(run.fence)
      run.fence.check()
    }

    return prepared
  }

  /**
   * Takes one node-filtered snapshot of the assignment. The filter runs once, with its lazy
   * preferred-replica expiry cleanup, and the routing version is captured after it so that cleanup
   * does not invalidate the snapshot it belongs to.
   * @private
   * @param {number} nodeId the broker this round fetches from
   * @param {IdleWait} run the owning run
   * @param {number} ownershipToken the membership identity captured by `fetch`
   * @returns {FetchPreparation} the snapshot with its stamps
   */
  captureFetchPreparation(nodeId, run, ownershipToken) {
    const routing = { expiresAt: Infinity, leaderless: false }
    const parts = this.filterPartitionsByNode(nodeId, this.subscriptionState.assigned(), routing)
    return new FetchPreparation(
      this,
      run,
      parts,
      routing.expiresAt,
      ownershipToken,
      routing.leaderless
    )
  }

  /**
   * Builds this round's request entries from the node-filtered assignment.
   *
   * Three filters decide what is asked for. A partition with no committed offset is left out
   * because recovering from `OFFSET_OUT_OF_RANGE` clears offsets per partition
   * (`OffsetManager#clearOffsets`), and a concurrent round would otherwise fetch from an offset
   * that is being rebuilt - see https://github.com/tulios/kafkajs/pull/570 for the concurrency
   * that exposed it and https://github.com/tulios/kafkajs/pull/578 for the filter. A paused
   * partition is left out, and so is one whose position is being validated or has not validated
   * yet (held): fetching from it would read from an offset this client is not yet sure still
   * exists. A halted partition is left out until its halt is cleared;
   * the check is skipped while nothing is halted, which is the steady state.
   *
   * Pause and hold state is checked per requested partition, so the cost follows the node's
   * partitions, not the whole assignment.
   *
   * @private
   * @param {import("../../types").TopicPartitions[]} topicPartitions the node-filtered assignment
   * @returns {{topic: string, partitions: {partition: number, fetchOffset: string,
   *   currentLeaderEpoch: number, maxBytes: number}[]}[]} one entry per topic with something to
   *   fetch; a topic whose partitions were all filtered out is absent
   */
  buildFetchRequests(topicPartitions) {
    const manager = this.offsetManager
    const state = this.subscriptionState

    return topicPartitions
      .filter(({ topic }) => this.obligations.current(topic) !== null)
      .map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.reduce((requests, partition) => {
          const baseline = manager.heldOffsetAt(topic, partition)
          if (
            baseline != null &&
            state.isAssigned(topic, partition) &&
            !state.isPaused(topic, partition) &&
            !state.isHeld(topic, partition) &&
            (this.haltedPartitions.size === 0 ||
              !this.haltedPartitions.has(`${topic}|${partition}`)) &&
            !(this.hasSeekGates() && this.isSeekGated(topic, partition))
          ) {
            const currentLeaderEpoch = this.cluster.leaderEpoch(topic, partition)
            if (this.positionValidator.holdsFetch(topic, partition, currentLeaderEpoch)) {
              return requests
            }
            requests.push({
              partition,
              fetchOffset: manager.nextOffset(topic, partition, baseline).toString(),
              currentLeaderEpoch,
              maxBytes: this.maxBytesPerPartition,
            })
          }
          return requests
        }, []),
      }))
      .filter(({ partitions }) => partitions.length)
  }

  /**
   * Resolves the topic ids for one round's requests and leaves out every topic without one.
   *
   * A topic is never fetched by name: after a delete and recreate the name belongs to the new
   * topic, and a by-name fetch would read it at the old topic's position. A topic left out is
   * fetched again once a metadata refresh brings its id; the caller reports it and asks for that
   * refresh. Costs one cache lookup per topic; the list is copied only when a topic is left out.
   *
   * @private
   * @param {{topic: string}[]} requests this round's request entries
   * @returns {{requests: object[], topicIds: Map<string, string>, missing: object[]|null}} the
   *   entries that have an id (the same array when all do), their name -> id map, and the entries
   *   left out, or null when none was
   */
  resolveTopicIds(requests) {
    const topicIds = new Map()
    let missing = null

    for (const request of requests) {
      const topicId = this.cluster.topicId(request.topic)
      if (topicId === undefined) (missing ??= []).push(request)
      else topicIds.set(request.topic, topicId)
    }

    return {
      requests: missing ? requests.filter(({ topic }) => topicIds.has(topic)) : requests,
      topicIds,
      missing,
    }
  }

  /**
   * Recovers from the fetch response's top-level error code, or rethrows it.
   *
   * @private
   * @param {number} errorCode the response's top-level code, already known to be a failure
   * @param {import('../producer/generation')} [fence] the run that received the response: its
   *   close ends the refresh, and a late answer installs nothing
   * @param {{session?: boolean}} [options] the request was tracked by a Fetch session, whose
   *   session-ID and epoch errors then only reset it and refresh nothing
   * @returns {Promise<void>} resolves once the round can be abandoned with no batches
   * @throws {Error} the error the code maps to, for a code with no classification: that keeps the
   *   recovery path a request-level fetch error has always taken - refresh and rejoin for stale
   *   metadata, crash for anything else; or the fence's close error.
   */
  async handleFetchTopLevelError(errorCode, fence, { session = false } = {}) {
    const action = topLevelFetchAction(errorCode, { session })
    if (action === 'throw') {
      throw createErrorFromCode(errorCode)
    }

    if (action === 'session-reset') {
      this.logger.debug('The broker reset the Fetch session', {
        groupId: this.groupId,
        memberId: this.memberId,
        errorCode,
      })
      return
    }

    this.logger.warn('Fetch response carried a top-level error, refreshing metadata', {
      groupId: this.groupId,
      memberId: this.memberId,
      errorCode,
    })

    await this.refreshCluster({ fence })
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
   * A partition whose answer carries a corruption marker is halted when the marker is accepted
   * ({@link ConsumerGroup#acceptHalt}): the halt is stored first, the records before the corrupt
   * batch still go on as an ordinary batch, and then the halt is reported once.
   *
   * @private
   * @param {object} partitionData one decoded partition of the response
   * @param {object} context this topic's state for the round
   * @param {string} context.topicName
   * @param {string} [context.topicId] the id the answer or the request keyed the topic with
   * @param {Map<number, {fetchOffset: string}>} context.requestedPartitions this topic's request
   *   entries by partition
   * @param {object} context.preferredReadReplicas this topic's preferred-replica map, written here
   * @param {object[]} [context.nodeEndpoints] the response's KIP-951 endpoints, if any
   * @param {import('./batch')[]} context.batches the round's batches, appended to
   * @param {boolean} context.debug whether debug logging is on for this round
   * @param {number} context.assignmentEpoch the assignment the round began under
   * @param {number} context.requestClock the acquisition clock when the request was built
   * @param {FetchPreparation} context.prepared the round's preparation, whose identity a refusal
   *   raised from this answer carries
   * @param {import('../producer/generation')} [context.fence] the terminal fence of the run that
   *   received the response; the recovery and hint work it starts is bound to it
   * @returns {Promise<boolean>} whether this partition asks for a metadata refresh
   * @throws {Error} the error an unclassified partition code maps to, or the fence's close error
   */
  async handleFetchedPartition(partitionData, context) {
    const { topicName, requestedPartitions, preferredReadReplicas, nodeEndpoints, batches, debug } =
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

    // An answer for a position the partition has left (another node's answer came first, or a
    // reset cleared the offset) is not used at all: not delivered, not recovered from.
    const requested = requestedPartitions?.get(partition)
    if (
      requested !== undefined &&
      typeof this.offsetManager?.positionOf === 'function' &&
      this.offsetManager.positionOf(topicName, partition) !== String(requested.fetchOffset)
    ) {
      this.logger.debug('Dropping fetched partition', {
        topic: topicName,
        partition,
        reason: 'moved',
      })
      return false
    }

    // Nor is one held since the request was built, or validated since against another epoch than
    // the request carried: the answer may come from a leader the position was not checked with,
    // and not even an error answer may reset or reroute a position that is being checked.
    if (this.positionValidator.holdsFetch(topicName, partition, requested?.currentLeaderEpoch)) {
      this.logger.debug('Dropping fetched partition', {
        topic: topicName,
        partition,
        reason: 'awaiting-validation',
      })
      return false
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
        const requested = requestedPartitions.get(partition)
        await this.recoverFromOffsetOutOfRange(
          { topic: topicName, partition, offset: requested && String(requested.fetchOffset) },
          context.fence,
          this.fetchedResetContext(context.prepared, context.requestClock, context.topicId)
        )
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
          nodeEndpoints,
          context.fence
        )

        // A partition whose hint was usable already knows where to go next round; one whose hint
        // was dropped, or that never carried one, needs the refresh.
        return !applied
      }

      return false
    }

    const dropReason = fetchedPartitionDropReason(
      this.seekOffset.size !== 0 && this.seekOffset.has(topicName, partition),
      this.subscriptionState.isPaused(topicName, partition)
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
      if (currentPreferredReadReplica !== preferredReadReplica) this.routingChanged()
    }

    if (
      this.haltedPartitions.size !== 0 &&
      this.haltedPartitions.has(`${topicName}|${partition}`)
    ) {
      return false
    }

    const { corruption } = partitionData
    const halted = corruption != null && this.acceptHalt(topicName, partition, corruption, context)

    const partitionRequestData = requestedPartitions.get(partition)

    batches.push(new Batch(topicName, partitionRequestData.fetchOffset, partitionData))
    if (halted) this.reportHalt(topicName, partition, corruption)
    return false
  }

  /**
   * Halts a partition on a corruption marker, unless the answer is stale. The answer is stale when
   * the partition is already halted (a second marker for one episode), when the assignment the
   * round began under was replaced, when the partition is no longer this member's, when it was
   * acquired again, sought, reset or recreated after the request was built (its `changedAt`, set
   * on each of those, is later than the request's clock, so a seek back to the same offset still
   * counts), or when the topic id the answer is keyed by is not the one the cluster knows now. A
   * stale marker is dropped and changes nothing.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {{ offset: string, reason: string, error: Error }} corruption the decoder's marker
   * @param {object} context the round's state, see {@link ConsumerGroup#handleFetchedPartition}
   * @returns {boolean} whether the partition is now halted by this marker
   */
  acceptHalt(topic, partition, { offset, reason, error }, context) {
    const key = `${topic}|${partition}`
    const acquired = this.acquired.get(key)
    const knownTopicId = this.cluster.topicId(topic)

    if (
      this.haltedPartitions.has(key) ||
      this.assignmentEpoch !== context.assignmentEpoch ||
      !acquired ||
      isChangedSince(acquired, context.requestClock) ||
      (context.topicId !== undefined &&
        knownTopicId !== undefined &&
        context.topicId !== knownTopicId)
    ) {
      return false
    }

    this.haltedPartitions.set(key, { offset, reason, error })
    return true
  }

  /**
   * Captures, before any await, the identity a reset resolution is asked under: the manager, the
   * run's guard, the membership token, the assignment epoch and the acquisition clock. A resolution
   * whose answer arrives later is judged by this, never by the identity current when it arrives.
   * @private
   * @param {string} topic
   * @param {import('./offsetManager')} manager the manager that asks
   * @param {FetchPreparation} [preparation] the fetch round the resolution belongs to
   * @returns {{manager: object, guard: Function|null, token: number, assignmentEpoch: number,
   *   requestClock: number, topicId: string|undefined}} an immutable record
   */
  captureResetContext(topic, manager, preparation) {
    return {
      manager,
      guard: preparation?.guard ?? null,
      token: preparation?.ownershipToken ?? this.ownership.token(),
      assignmentEpoch: preparation?.assignmentEpoch ?? this.assignmentEpoch,
      requestClock: this.acquireClock,
      topicId: this.cluster.topicId(topic),
    }
  }

  /**
   * The identity of a fetched answer, for a refusal raised from it. Every field was captured when
   * the request was built, so nothing here is read from the state after the response arrived.
   * @private
   * @param {FetchPreparation} prepared the round's preparation
   * @param {number} requestClock the acquisition clock when the request was built
   * @param {string} [topicId] the id the answer or the request keyed the topic with
   * @returns {object} the same shape `captureResetContext` returns
   */
  fetchedResetContext(prepared, requestClock, topicId) {
    return {
      manager: prepared.manager,
      guard: prepared.guard,
      token: prepared.ownershipToken,
      assignmentEpoch: prepared.assignmentEpoch,
      requestClock,
      topicId,
    }
  }

  /**
   * Whether a captured reset context still describes this member's partition: the same manager,
   * run and membership, the same assignment, and no seek, revoke, reacquisition or recreation of
   * the partition since the context's clock. Offset equality plays no part.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {object|null} context a record from `captureResetContext` or a fetched round
   * @returns {boolean}
   */
  isResetContextCurrent(topic, partition, context) {
    if (!context || this.isStopping() || context.manager !== this.offsetManager) return false
    const acquired = this.acquired.get(`${topic}|${partition}`)
    return (
      !context.guard?.() &&
      !this.ownership.refusal(context.token) &&
      this.assignmentEpoch === context.assignmentEpoch &&
      acquired !== undefined &&
      !isChangedSince(acquired, context.requestClock)
    )
  }

  /**
   * The identity a position-changing effect of an answer runs under: the caller's preparation
   * (guard and fence), refused as well once the partition was acquired again, sought, reset or
   * recreated after the request was built (`isChangedSince`), so an answer older than a seek
   * changes nothing, however late inside its own work the seek arrives.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {number} requestClock the acquisition clock when the request was built
   * @param {{guard: () => Error|null, fence?: import('../producer/generation')}} [preparation]
   * @returns {{guard: () => Error|null, fence?: import('../producer/generation')}}
   */
  boundToRequest(topic, partition, requestClock, preparation) {
    return {
      guard: () =>
        preparation?.guard() ||
        (this.isRequestStale(topic, partition, requestClock)
          ? new KafkaJSCommitFailedError('A seek superseded the answer', {
              reason: 'stale-assignment',
            })
          : null),
      fence: preparation?.fence,
    }
  }

  /**
   * Whether an answer to a request built at `requestClock` is older than the partition's current
   * position: the partition is no longer held, or was acquired again, sought, reset or recreated
   * since.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {number} requestClock
   * @returns {boolean}
   */
  isRequestStale(topic, partition, requestClock) {
    const acquired = this.acquired.get(`${topic}|${partition}`)
    return acquired === undefined || isChangedSince(acquired, requestClock)
  }

  /**
   * Halts a partition the none policy refused, when its captured identity is still current, with
   * the same store-first, one-event, one-log episode as a corruption halt. A stale refusal changes
   * nothing. No position is derived: the partition waits for an explicit seek, a revoke, a
   * recreation or a rebuild.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {{offset: string|null, reason: 'no-offset'|'offset-out-of-range', error: Error}} marker
   * @param {object|null} context the identity the refusal was raised under
   * @returns {boolean} whether a current halt is installed
   */
  installReset(topic, partition, marker, context) {
    if (!this.isResetContextCurrent(topic, partition, context)) return false
    if (this.acceptHalt(topic, partition, marker, context)) {
      this.reportHalt(topic, partition, marker)
    }
    return this.haltedPartitions.has(`${topic}|${partition}`)
  }

  /**
   * Installs every refusal a manager's resolution has produced since the last call. A refusal
   * whose captured identity is no longer current is dropped and retired in the manager, so the
   * partition is resolved again under the identity that holds now instead of staying skipped.
   * @private
   * @param {import('./offsetManager')} manager the manager that resolved
   * @returns {void}
   */
  installResetRefusals(manager) {
    for (const refusal of manager.takeResetRefusals()) {
      const { topic, partition, offset, reason, error, context } = refusal
      if (this.isResetContextCurrent(topic, partition, context)) {
        this.installReset(topic, partition, { offset, reason, error }, context)
      } else {
        manager.retireResetRefusals(topic, partition)
      }
    }
  }

  /**
   * Reports an accepted halt with optional nextOffset, codec and limit copied from its error.
   * Emits one `consumer.partition_halted` event and one error log. Neither can throw: `Consumer.on`
   * contains its listeners and the logger contains its sink, so the halt stands and the other
   * partitions of the round go on.
   * @private
   * @param {string} topic
   * @param {number} partition
   * @param {{ offset: string, reason: string, error: Error }} corruption
   * @returns {void}
   */
  reportHalt(topic, partition, { offset, reason, error }) {
    const { groupId, memberId } = this
    const { nextOffset, codec, limit } = error
    const payload = { groupId: groupId ?? null, memberId, topic, partition, offset, reason, error }
    if (nextOffset !== undefined) payload.nextOffset = nextOffset
    if (codec !== undefined) payload.codec = codec
    if (limit !== undefined) payload.limit = limit
    this.instrumentationEmitter.emit(PARTITION_HALTED, payload)

    try {
      this.logger.error('Halted a partition; seek it to resume', {
        groupId,
        memberId,
        topic,
        partition,
        offset,
        reason,
        error: error.message,
      })
    } catch {}
  }

  /**
   * Recovers from an error thrown by a fetch round, or rethrows it.
   * @param {Error} e
   * @param {import('../producer/generation')} [fence] the run that failed: its refreshes are bound
   *   to it, and it is checked before a rejoin is requested, so a retired run asks for nothing
   * @param {object} [context] the identity of the round that failed, for a none refusal
   * @returns {Promise<void>}
   * @throws {Error} `e` itself when it is not one of the errors this method knows how to recover
   *   from, or the fence's close error
   */
  async recoverFromFetch(e, fence, context) {
    if (STALE_METADATA_ERRORS.includes(e.type) || e.name === 'KafkaJSTopicMetadataNotLoaded') {
      this.logger.debug('Stale cluster metadata, refreshing...', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: e.message,
      })

      await this.refreshCluster({ fence })
      fence?.check()
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
      await this.recoverFromOffsetOutOfRange(e, ...(fence ? [fence, context] : []))
      return
    }

    if (e.name === 'KafkaJSConnectionClosedError') {
      this.cluster.removeBroker({ host: e.host, port: e.port, source: failureSourceOf(e) })
      return
    }

    if (e.name === 'KafkaJSBrokerNotFound' || e.name === 'KafkaJSConnectionClosedError') {
      this.logger.debug(`${e.message}, refreshing metadata and retrying...`)
      await this.refreshCluster({ fence })
      fence?.check()
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
    if (this.kind === 'manual') {
      this.logger.debug('A manual assignment is fixed; no resync was requested', {
        error: error.message,
      })
      return
    }

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
   * @param {import('../producer/generation')} [fence] the run that received the response: its
   *   close stops the endpoint work and a late answer is neither installed nor published
   * @returns {Promise<boolean>} true when the hint was applied and no refresh is needed for it
   * @throws {Error} the fence's close error
   */
  async applyFetchLeaderHint(topic, partition, currentLeader, nodeEndpoints, fence) {
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

    return await this.cluster.applyLeaderHint(
      { topic, partition, leaderId, leaderEpoch, nodeEndpoints: endpoints },
      ...boundTo(fence)
    )
  }

  /**
   * Recovers one partition from `OFFSET_OUT_OF_RANGE`: drops a preferred read replica, or else
   * resets the partition to its topic's default offset. Under the none policy the leader's answer
   * halts the partition instead, with the offset that was requested, and resets nothing.
   * @param {{topic: string, partition: number, offset?: string}} e the partition that was out of
   *   range, with the decimal offset the request asked for when it is known
   * @param {import('../producer/generation')} [fence] the run that saw it: the reset commit is
   *   bound to it, and a retired run neither commits nor marks the partition changed
   * @param {object} [context] the identity of the answer, captured when its request was built
   * @returns {Promise<void>}
   * @throws {Error} the fence's close error
   */
  async recoverFromOffsetOutOfRange(e, fence, context) {
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
      this.routingChanged()
    } else if (this.topicConfigurations[e.topic]?.resetPolicy?.kind === 'none') {
      this.installReset(
        e.topic,
        e.partition,
        {
          offset: e.offset ?? null,
          reason: 'offset-out-of-range',
          error: new KafkaJSNoOffsetError('Offset out of range; seek the partition to resume', {
            topic: e.topic,
            partition: e.partition,
            reason: 'offset-out-of-range',
          }),
        },
        context
      )
    } else {
      const stale = () => context && this.isRequestStale(e.topic, e.partition, context.requestClock)
      if (stale()) return
      this.logger.error('Offset out of range, resetting to default offset', {
        topic: e.topic,
        partition: e.partition,
        groupId: this.groupId,
        memberId: this.memberId,
      })

      const caller = fence ? { guard: fence.guard, fence } : undefined
      await this.offsetManager.setDefaultOffset(
        { topic: e.topic, partition: e.partition },
        ...(context
          ? [this.boundToRequest(e.topic, e.partition, context.requestClock, caller)]
          : caller
            ? [caller]
            : [])
      )
      fence?.check()
      if (stale()) return
      this.markChanged(e.topic, e.partition)
    }
  }

  /**
   * @param {string[]} [names] the subscription to map: the applied names, or a round's pinned ones
   * @returns {Map<string, number[]>} each name's known partition ids, sorted
   */
  generatePartitionsPerSubscribedTopic(names = this.topicsSubscribed) {
    const map = new Map()

    for (const topic of names) {
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
   * A leader also reports a topic that only other members subscribe to once its union watch has
   * seen that topic gain partitions.
   *
   * Classic only: a manual assignment is fixed, and under the consumer protocol the coordinator
   * sees a topic's partitions change and sends the new assignment itself, so a rejoin would only
   * repeat the round it already has (a topic that was missing at the last round and is back, say).
   *
   * @returns {void}
   * @throws {KafkaJSStaleTopicMetadataAssignment} when a subscribed or union-only topic gained
   *   partitions
   */
  checkForStaleAssignment() {
    if (
      this.kind !== 'classic' ||
      !this.partitionsPerSubscribedTopic ||
      this.staleAssignmentPending
    ) {
      return
    }

    const newPartitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()

    for (const [topic, partitions] of newPartitionsPerSubscribedTopic) {
      const known = this.partitionsPerSubscribedTopic.get(topic)
      if (known === undefined) continue
      const diff = arrayDiff(partitions, known)

      if (diff.length > 0) {
        this.staleAssignmentPending = true
        throw new KafkaJSStaleTopicMetadataAssignment('Topic has been updated', {
          topic,
          unknownPartitions: diff,
        })
      }
    }

    const { drift } = this.leaderUnionWatch
    if (drift) {
      this.staleAssignmentPending = true
      throw new KafkaJSStaleTopicMetadataAssignment('Topic has been updated', drift)
    }
  }

  /**
   * Lets the leader's union watch reread the topics only other members subscribe to, so that a
   * partition added to one of them reaches `checkForStaleAssignment`. A member that is no longer
   * the leader stops watching instead.
   * @private
   * @param {Generation} fence the run's fence; a stop or restart cancels the read
   * @returns {Promise<void>}
   * @throws {Error} the fence's close error
   */
  async pollLeaderUnion(fence) {
    if (!this.isLeader()) return this.leaderUnionWatch.clear()
    await this.leaderUnionWatch.poll(fence)
  }

  /**
   * Resolves pending seek/reset work through the ordinary resolver, then reads the latest position.
   * A seek changes position without changing acquisition; revoke/reacquire and rebuild retire it.
   * A rebalance callback may read while a stop is under way: it then applies and waits for no
   * seek and returns the established position, and a queued seek dies with the run. Otherwise,
   * while a seek of the partition is queued or in flight (`isSeekGated`) and its cell's
   * reservation is held, the read waits for the release with no timer (`awaitSeekRelease`), so
   * it never returns the position a seek in flight is replacing; a halt is reported only once no
   * seek gates the partition, so a queued seek that repairs it is waited for. A read from a
   * callback carries that invocation's guard and fence through seek, resolution and the answer,
   * so a callback that settled, expired or was retired meanwhile publishes nothing and gets its
   * refusal; a lost caller's read is refused too.
   * With a public-call `operation`, its one deadline and identity guard are carried the same way: its
   * close ends the reads and delays, and an expired call publishes and answers nothing.
   * @param {{topic: string, partition: number}} entry validated public partition
   * @param {{ operation?: import('./groupProtocol/interface').ConsumerOperation }} [context]
   * @returns {Promise<string>} current nonnegative decimal next position, including sparse offsets;
   *   a committed offset the cache holds as a `Long` (a deliberate commit) is read as its decimal
   * @throws {Error} on ownership retirement, reset refusal, the operation's refusal or unresolved
   *   bounded resolution
   */
  async position(entry, { operation } = {}) {
    ManualGroupProtocol.validateTopicPartition(entry)
    const { topic, partition } = entry
    const key = `${topic}|${partition}`
    const acquiredAt = this.acquired.get(key)?.acquiredAt
    const manager = this.offsetManager
    const epoch = this.assignmentEpoch
    const token = this.ownership.token()
    const topicId = this.cluster.topicId(topic)
    const invocation = RebalanceHooks.current()
    const current = () =>
      acquiredAt !== undefined &&
      manager &&
      (invocation !== undefined || !this.isStopping()) &&
      this.offsetManager === manager &&
      this.assignmentEpoch === epoch &&
      this.acquired.get(key)?.acquiredAt === acquiredAt &&
      this.cluster.topicId(topic) === topicId &&
      !this.ownership.refusal(token)
    const refusal = () =>
      operation?.guard() ||
      RebalanceHooks.refusal(invocation) ||
      (current() ? null : consumerStoppingError())
    const preparation =
      invocation || operation
        ? new InvocationPreparation({
            manager,
            fence: operation ?? RebalanceHooks.fence(invocation),
            ownershipToken: token,
            assignmentEpoch: epoch,
            guard: refusal,
          })
        : undefined
    const check = () => {
      const error = refusal()
      if (error) throw error
    }
    check()
    const quiescent = invocation !== undefined && this.isStopping()
    for (let pass = 0; pass < MAX_RESOLUTION_PASSES;) {
      if (!quiescent) await this.seekOffsets([{ topic, partitions: [partition] }], preparation)
      check()
      if (!quiescent && this.isSeekGated(topic, partition)) {
        const held = this.operations.reservation(topic, partition)
        if (held !== null) await this.awaitSeekRelease(entry, held, preparation?.fence, check)
        else pass++
        continue
      }
      const halted = this.haltedPartitions.get(key)
      if (halted && ['no-offset', 'offset-out-of-range'].includes(halted.reason)) throw halted.error
      pass++
      const offset = String(
        manager.resolvedOffsetAt(topic, partition) ?? manager.heldOffsetAt(topic, partition)
      )
      if (/^\d+$/.test(offset)) return offset
    }
    throw new KafkaJSNonRetriableError(`Position is not yet resolved for ${topic}[${partition}]`)
  }

  /**
   * Waits, with no timer, until the reservation a seek of the partition holds or is blocked on is
   * released, or the run, the caller or the partition's authority changes.
   * @private
   * @param {{topic: string, partition: number}} entry the partition `position()` reads
   * @param {import('./offsetManager/offsetOperations').Reservation} held the observed reservation
   * @param {import('../producer/generation')|undefined} fence the caller's lifetime, if any
   * @param {() => void} check throws the caller's refusal
   * @returns {Promise<void>} once the wait ended; the caller rechecks before it reads
   * @throws {Error} the caller's refusal, or the stopping error once the run is disposed
   */
  async awaitSeekRelease({ topic, partition }, held, fence, check) {
    const run = this.idleWait
    if (run.disposed) throw consumerStoppingError()
    await run.wait(
      Infinity,
      () => {
        try {
          check()
        } catch {
          return false
        }
        return this.operations.reservation(topic, partition) === held
      },
      fence
    )
    if (run.disposed) throw consumerStoppingError()
    check()
  }

  /**
   * Applies every pending `seek()` for the given topic/partitions, then resolves offsets for the
   * rest. A seek applied here that lands on a topic with a pending recreation reset (set by
   * `invalidateTopic()` earlier in this same fetch cycle) wins over that reset for its own
   * partition for its local position; the reset obligation settles only when a confirmed
   * current-incarnation commit is applied, so a seek with auto-commit off or a sentinel boundary
   * leaves it owed. An applied seek also clears the partition's halt, which is how an
   * application resumes a halted partition.
   *
   * A resolution that dropped an answer (a seek, commit or recreation reached the cell while the
   * request was in flight) is repeated at once, after a yield to the event loop, at most
   * `MAX_RESOLUTION_PASSES` times, instead of leaving the broker's round to wait out an empty
   * fetch. No pass can spin: each needs a fresh competing operation to drop again. After the last
   * pass the round proceeds as it always did. A stop request ends the passes: stopping is checked
   * before each pass and after a pass returns, so no further pass starts once the round is about to
   * be discarded. A pass already in flight is not cancelled.
   *
   * A popped seek is attempted once, as in kafkajs 2.2.4: whatever happens to its commit, it is
   * settled and never sent again. One the broker accepted is settled as applied (clearing its
   * halt), whatever refused the caller afterwards or whichever newer commit took its cell. One that
   * failed (refused before its write, rejected, or written with no answer) changes no position:
   * the partition keeps its cursor, committed cache, pending sentinel and halt, nothing is looked
   * up because of it, one WARN is logged and the error reaches the caller (`position()`, or the
   * fetch loop's error policy). The application seeks again if it still wants the position. A
   * popped seek that is no longer current (`isCurrentSeek`) is refused before its write and
   * settles nothing; one popped with no acquisition to compare (a partition assigned without one)
   * is never refused for it. Its identity is captured in the step that pops it, before any
   * observer runs, and every later step, its DEBUG log included, ends in its settlement, so a
   * throwing log is a failed seek and a seek admitted from inside the log supersedes it: the
   * attempt's admission is checked once more after the log, before any local or wire effect. The
   * log gets a copy of the popped seek, so a log creator that edits it changes nothing applied.
   *
   * A seek is popped only together with its cell's reservation in the group's ledger, taken in
   * the same synchronous step; while another seek of the cell holds it (any consumer, acquisition
   * or run of the group) the intent stays queued and this call moves on to the next partition.
   * The reservation is released when the seek's one commit request settles on the wire, before
   * any recovery or diagnostic, and at the latest when this attempt ends. A seek the broker
   * accepted is published (its position, committed offset and halt) in that same step, before the
   * release, so whatever the release wakes reads the seek's position and nothing is read back.
   *
   * Given a preparation, every step uses its captured offset manager and checks its guard before
   * popping an intent, after every await, before the resolution retry and its yield, and before
   * any local effect, so a late continuation of a retired or replaced preparation changes nothing.
   * The retry yield is bound to the run's fence. Without one, nothing changes.
   * @param {import("../../types").TopicPartitions[]} topicPartitions
   * @param {FetchPreparation} [preparation] the fetch round's identity
   * @returns {Promise<void>}
   * @throws {Error} the preparation's refusal
   */
  async seekOffsets(topicPartitions, preparation) {
    const manager = preparation ? preparation.manager : this.offsetManager
    for (const { topic, partitions } of topicPartitions) {
      for (const partition of partitions) {
        preparation?.check()
        if (!this.hasSeekOffset({ topic, partition })) continue
        const key = `${topic}|${partition}`
        const acquired = this.acquired.get(key)
        const admitted = acquired?.soughtAt
        const reservation = this.operations.reserve(topic, partition, {
          acquired,
          admitted,
          run: this.idleWait,
        })
        if (reservation === null) continue
        const seekEntry = this.seekOffset.pop(topic, partition)
        const popped = { acquired, admitted, run: this.idleWait, manager, settled: false }
        const attempt = {
          release: () => this.operations.release(reservation),
          refusal: () => {
            if (acquired === undefined || this.isCurrentSeek(key, acquired, admitted)) return null
            const message = 'A newer seek superseded this one'
            attempt.superseded ??= new KafkaJSCommitFailedError(message, {
              reason: 'stale-assignment',
            })
            return attempt.superseded
          },
          mayTakeEffect: () => this.mayTakeEffect(key, popped),
          settle: () => this.settleSeek(key, seekEntry, popped),
        }
        let applied = false
        let failure = null
        try {
          this.logger.debug('Seek offset', {
            groupId: this.groupId,
            memberId: this.memberId,
            seek: { ...seekEntry },
          })
          if (attempt.refusal() === null) {
            applied = await manager.seek(seekEntry, preparation, attempt)
          }
          if (applied) attempt.settle()
        } catch (error) {
          failure = { error }
        } finally {
          attempt.release()
        }
        if (failure) {
          if (attempt.accepted !== true) this.warnFailedSeek(seekEntry, failure.error)
          throw failure.error
        }
        preparation?.check()
      }
    }

    for (let pass = 1; pass <= MAX_RESOLUTION_PASSES && !this.isStopping(); pass++) {
      const resolved = await manager.resolveOffsets(preparation)
      preparation?.check()
      this.installResetRefusals(manager)
      if (resolved || pass === MAX_RESOLUTION_PASSES || this.isStopping()) {
        break
      }

      await (preparation ? fencedDelay(preparation.fence) : new Promise(setImmediate))
      preparation?.check()
    }

    // Guarded, not just level-checked: this runs once per fetch round, and most rounds resolve
    // nothing, so the per-topic payload must not be built to be thrown away.
    if (this.logger.debugEnabled()) {
      const committedOffsets = manager.committedOffsets()
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
   * uses the same two triggers and no others. Validation is a **blocking gate**: a partition that
   * is awaiting a reply or held is absent from the request built afterwards, and this method never
   * returns leaving one awaiting.
   *
   * Each leader is asked once per call, as the Java client does. A partition whose validation does
   * not complete - a failed request, a retriable or unexpected answer, a failed reposition - is
   * held: it keeps its position and tracked epoch, the next round's metadata refresh is a full one,
   * and it is asked again once its backoff passes (at once under a new leader epoch). The first
   * failure of a held partition is warned, then at most once a minute. Other partitions, other
   * leaders' included, are validated and fetched throughout. Only an ACL refusal
   * (`TOPIC_AUTHORIZATION_FAILED`) reaches the caller, after every leader was asked, as the same
   * code on a Fetch does.
   *
   * Positions are read without seeding (`peekNextOffset`): validation observes a position and
   * never makes the committed offset count as resolved progress, which a seek being applied has
   * just forgotten.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation
   * @param {import("../../types").TopicPartitions[]} topicPartitions the node-filtered assignment,
   *   which is a superset of what the request keeps: it also contains partitions with no committed
   *   offset, which are skipped here - there is no position to check.
   * @param {FetchPreparation} [preparation] the fetch round's identity. Its refusal is rethrown,
   *   and the entries this call put in flight are returned to unvalidated by identity, also at
   *   once when the run's fence closes, so a held broker reply is not needed to remove them.
   *   Without one, nothing changes.
   * @returns {Promise<void>}
   * @throws {Error} the preparation's refusal, or a `TOPIC_AUTHORIZATION_FAILED` answer
   */
  async validatePositions(topicPartitions, preparation) {
    let awaiting
    let unregister
    let failure
    let authorization
    const manager = preparation ? preparation.manager : this.offsetManager
    const now = performance.now()

    try {
      for (const { topic, partitions } of topicPartitions) {
        for (const partition of partitions) {
          // A user seek, queued or still being applied, is newer, more specific intent than
          // anything validation could conclude, and the position it replaces is not worth asking
          // about.
          if (this.hasSeekGates() && this.isSeekGated(topic, partition)) {
            continue
          }

          const currentLeaderEpoch = this.cluster.leaderEpoch(topic, partition)

          // Asked before anything is read or allocated: the steady state of a round is that every
          // partition is already validated against this epoch, and that round must cost nothing.
          if (!this.positionValidator.needsValidation(topic, partition, currentLeaderEpoch, now)) {
            continue
          }

          // The same filter the request build applies: with no committed offset there is no
          // position to check.
          if (manager.heldOffsetAt(topic, partition) == null) {
            continue
          }

          const trackedEpoch = manager.lastConsumedEpoch(topic, partition)
          // `request()` answers 'validated' rather than 'unchanged' for a partition whose leader
          // epoch moved while an epoch is unknown, and the preferred-read-replica clearing below
          // still has to happen - a replica chosen under the old leader is not a safe place to
          // keep reading from, whether or not this client can ask the leader about its position.
          const { action, generation } = this.positionValidator.request(topic, partition, {
            offset: manager.peekNextOffset(topic, partition).toString(),
            trackedEpoch,
            currentLeaderEpoch,
          })

          if (action === 'unchanged') {
            continue
          }

          const replicas = this.preferredReadReplicasPerTopicPartition[topic]
          if (replicas && replicas[partition] !== undefined) {
            delete replicas[partition]
            this.routingChanged()
          }

          if (action === 'awaiting') {
            awaiting = awaiting || []
            awaiting.push({
              topic,
              partition,
              trackedEpoch,
              currentLeaderEpoch,
              generation,
              requestClock: this.acquireClock,
              entry: this.positionValidator.entryOf(topic, partition),
            })
          }
        }
      }

      if (awaiting) {
        const cancelled = awaiting
        unregister = preparation?.fence.register(() => this.positionValidator.release(cancelled))
        await this.runValidationRequests(awaiting, preparation)
      }
    } catch (e) {
      const refusal = preparation?.guard()
      if (refusal) throw refusal
      if (e.code === TOPIC_AUTHORIZATION_FAILED_CODE) authorization = e
      else failure = e
      if (!awaiting) {
        this.logger.warn('Position validation failed', {
          groupId: this.groupId,
          memberId: this.memberId,
          error: e.message,
        })
      }
    } finally {
      // The invariant: no partition may be left awaiting with no request in flight. An entry is
      // held by identity, so a sweep that runs after a rebalance cannot touch another fetcher's
      // fresh entry. A refused preparation cancels its own entries instead of holding anything.
      unregister?.()
      if (preparation?.guard()) {
        this.positionValidator.release(awaiting || [])
      } else if (awaiting) {
        this.holdUnvalidated(
          awaiting.filter(({ topic, partition }) =>
            this.positionValidator.isAwaiting(topic, partition)
          ),
          (failure ?? authorization)?.message ?? 'no usable answer'
        )
      }
    }

    if (authorization) throw authorization
  }

  /**
   * Holds partitions whose validation did not complete (`PositionValidator#fail`), asks for a full
   * metadata refresh before the next round's validation, and warns about each one when it is
   * first held, then at most once per `HELD_WARNING_INTERVAL`. Reads the clock when the failure is
   * recorded, not when the round began, so a slow round cannot shorten the backoff.
   * @private
   * @param {{topic: string, partition: number, entry: object, currentLeaderEpoch: number}[]} items
   *   the awaiting items that failed
   * @param {string} error what failed, for the warning
   * @returns {void}
   */
  holdUnvalidated(items, error) {
    const now = performance.now()
    const held = this.positionValidator.fail(items, now)
    if (held.length === 0) return
    this.validationRefresh = Symbol('validation refresh')

    for (const { topic, partition, entry } of held) {
      if (entry.warnedAt !== undefined && now - entry.warnedAt < HELD_WARNING_INTERVAL) continue
      entry.warnedAt = now
      this.logger.warn(
        'Position validation did not complete; the partition is not fetched until it validates',
        {
          groupId: this.groupId,
          memberId: this.memberId,
          topic,
          partition,
          error,
          failures: entry.failures,
          heldMs: now - entry.heldSince,
        }
      )
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
   * A leader whose request fails holds only its own partitions; the next leader is still asked.
   *
   * @private
   * @param {{topic: string, partition: number, trackedEpoch: number, currentLeaderEpoch: number,
   *   generation: number, entry: object}[]} awaiting
   * @param {FetchPreparation} [preparation] the fetch round's identity, see `validatePositions`
   * @returns {Promise<void>}
   * @throws {Error} the preparation's refusal, or the first `TOPIC_AUTHORIZATION_FAILED` answer
   *   once every leader was asked
   */
  async runValidationRequests(awaiting, preparation) {
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

    let authorization = null
    for (const [nodeId, topics] of perLeader) {
      preparation?.check()
      try {
        const refused = await this.validateOnLeader(nodeId, topics, preparation)
        authorization ??= refused
      } catch (e) {
        preparation?.check()
        if (e.code === TOPIC_AUTHORIZATION_FAILED_CODE) authorization ??= e
        const unanswered = topics.flatMap(({ entries }) => entries)
        this.holdUnvalidated(
          unanswered.filter(({ topic, partition }) =>
            this.positionValidator.isAwaiting(topic, partition)
          ),
          e.message
        )
      }
      preparation?.check()
    }

    if (authorization) throw authorization
  }

  /**
   * Validates one leader's share of this round with one request. A partition the answer does not
   * settle is held and asked again in a later round, after a metadata refresh: resending it to
   * the same leader at the same epoch could not succeed.
   *
   * Whether the leader can serve the API at all is decided from its advertised versions, never by
   * catching `KafkaJSServerDoesNotSupportApiKey`: that error is non-retriable and would reach the
   * fetch's own recovery, crashing the consumer once per round.
   *
   * @private
   * @param {number} nodeId the leader of every partition in `topics`
   * @param {{topic: string, entries: object[]}[]} topics
   * @param {FetchPreparation} [preparation] the fetch round's identity: checked after the broker
   *   lookup and the reply, and forwarded to the request
   * @returns {Promise<Error|null>} the first `TOPIC_AUTHORIZATION_FAILED` answer, for the caller to
   *   raise once every leader was asked
   * @throws {Error} the preparation's refusal, or the failure of the request itself
   */
  async validateOnLeader(nodeId, topics, preparation) {
    const broker = await this.cluster.findBroker({ nodeId, fence: preparation?.fence })
    preparation?.check()
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

      this.confirmAll(topics)
      return null
    }

    const response = await broker.offsetForLeaderEpoch({
      topics: topics.map(({ topic, entries }) => ({
        topic,
        partitions: entries.map(({ partition, currentLeaderEpoch, trackedEpoch }) => ({
          partition,
          currentLeaderEpoch,
          leaderEpoch: trackedEpoch,
        })),
      })),
      guard: preparation?.guard,
    })
    preparation?.check()

    return this.applyValidationResponse(topics, response, preparation)
  }

  /**
   * Classifies every partition of one OffsetForLeaderEpoch reply and applies its outcome.
   *
   * Each partition is handled on its own: any error code holds only that partition, and
   * `UNKNOWN_LEADER_EPOCH` also forgets the cached epoch, which the forced refresh brings back; a
   * truncation or reset outcome holds it until its reposition is applied (`confirm`), and a failed
   * reposition keeps it held.
   *
   * @private
   * @param {{topic: string, entries: object[]}[]} pending the partitions the request asked about
   * @param {object} response the decoded reply
   * @param {FetchPreparation} [preparation] the fetch round's identity, checked before each
   *   partition's outcome is applied; a refusal is rethrown after the partition it interrupted is
   *   returned to unvalidated
   * @returns {Promise<Error|null>} the first `TOPIC_AUTHORIZATION_FAILED` error, which the caller
   *   raises once every leader was asked. A partition the reply does not mention is left to the
   *   caller's awaiting sweep: the broker answered without it.
   * @throws {Error} the preparation's refusal
   */
  async applyValidationResponse(pending, response, preparation) {
    const manager = preparation ? preparation.manager : this.offsetManager
    const replies = new Map()
    for (const { topic, partitions } of response.topics) {
      for (const reply of partitions) {
        replies.set(`${topic}|${reply.partition}`, reply)
      }
    }

    let authorization = null

    for (const { topic, entries } of pending) {
      for (const entry of entries) {
        preparation?.check()
        const reply = replies.get(`${topic}|${entry.partition}`)

        if (!reply) {
          continue
        }

        // The partition lost its committed offset while the request was in flight - a concurrent
        // OffsetOutOfRange recovery cleared it. There is no position left to compare the reply
        // with. Left to the caller's awaiting sweep, like a partition the reply never mentioned.
        if (manager.heldOffsetAt(topic, entry.partition) == null) {
          continue
        }

        // Read again now, not the pair captured when the request was made: comparing the captured
        // pair with itself could never detect a position that moved under the request.
        const offset = manager.peekNextOffset(topic, entry.partition).toString()
        const outcome = this.positionValidator.complete(
          topic,
          entry.partition,
          { offset, trackedEpoch: manager.lastConsumedEpoch(topic, entry.partition) },
          {
            errorCode: reply.errorCode,
            endOffset: reply.endOffset,
            leaderEpoch: reply.leaderEpoch,
            seekQueued:
              this.seekOffset.has(topic, entry.partition) ||
              this.isSoughtAfter(topic, entry.partition, entry.requestClock),
            generation: entry.generation,
            requestEntry: entry.entry,
          }
        )

        if (outcome.action === 'retry' || outcome.action === 'fatal') {
          // Resending an epoch the leader does not know cannot succeed: it is forgotten, and the
          // forced refresh brings the current one.
          if (reply.errorCode === UNKNOWN_LEADER_EPOCH_CODE) {
            this.cluster.forgetLeaderEpoch(topic, entry.partition)
          }
          const error = createErrorFromCode(reply.errorCode)
          if (reply.errorCode === TOPIC_AUTHORIZATION_FAILED_CODE) authorization ??= error
          this.holdUnvalidated([entry], error.message)
        } else if (outcome.action === 'truncated' || outcome.action === 'reset') {
          try {
            const applied = await this.applyValidationOutcome(
              topic,
              entry.partition,
              offset,
              outcome,
              preparation,
              entry.requestClock
            )
            preparation?.check()
            if (applied) this.positionValidator.confirm(entry)
            else this.holdUnvalidated([entry], 'position change was not applied')
          } catch (e) {
            // The partition stays held, so the position it was told to leave is not fetched; the
            // next round decides it again. A refused preparation releases only the entry it
            // decided, never a replacement's.
            if (preparation?.guard()) {
              this.positionValidator.release([entry])
              preparation.check()
            }
            this.holdUnvalidated([entry], e.message)
          }
        }
      }
    }

    return authorization
  }

  /**
   * Records every captured entry as validated against the epoch it was requested under, without
   * a reply, leaving tracked epochs alone: the exit for a leader that cannot serve the API. An
   * entry replaced, forgotten or cleared since is left alone (`PositionValidator#confirm`).
   *
   * @private
   * @param {{topic: string, entries: object[]}[]} topics
   * @returns {void}
   */
  confirmAll(topics) {
    for (const { entries } of topics) {
      for (const entry of entries) this.positionValidator.confirm(entry)
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
   * @param {FetchPreparation} [preparation] the fetch round's identity: forwarded to the offset
   *   work and checked before the repositioning is marked and announced
   * @param {number} [requestClock] the acquisition clock when the validation request was built;
   *   with a preparation, it is the identity a none refusal of an unknown divergence carries
   * @returns {Promise<boolean>} whether reposition, reset or a halt was applied
   * @throws {Error} the preparation's refusal
   */
  async applyValidationOutcome(topic, partition, offset, outcome, preparation, requestClock) {
    const manager = preparation?.manager ?? this.offsetManager
    const bound =
      requestClock === undefined
        ? preparation
        : this.boundToRequest(topic, partition, requestClock, preparation)
    const stale = () =>
      requestClock !== undefined && this.isRequestStale(topic, partition, requestClock)
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
      const applied = await manager.reposition(
        { topic, partition, offset: outcome.divergingOffset },
        bound
      )
      preparation?.check()
      if (!applied || stale()) return false
      this.markChanged(topic, partition)
    } else if (outcome.action === 'reset') {
      this.logger.warn('Log truncation detected but the divergence point is unknown, resetting', {
        groupId: this.groupId,
        topic,
        partition,
        offset,
      })
      if (this.topicConfigurations[topic]?.resetPolicy?.kind === 'none') {
        const halted = this.installReset(
          topic,
          partition,
          {
            offset,
            reason: 'offset-out-of-range',
            error: new KafkaJSNoOffsetError('Log truncated at an unknown point; seek to resume', {
              topic,
              partition,
              reason: 'offset-out-of-range',
            }),
          },
          preparation && this.fetchedResetContext(preparation, requestClock)
        )
        if (!halted || stale()) return false
      } else {
        const applied = await manager.setDefaultOffset({ topic, partition }, bound)
        preparation?.check()
        if (!applied || stale()) return false
        this.markChanged(topic, partition)
      }
    } else {
      return false
    }

    this.instrumentationEmitter.emit(LOG_TRUNCATION, {
      topic,
      partition,
      offset,
      divergingOffset: outcome.divergingOffset,
      leaderEpoch: outcome.leaderEpoch,
    })
    return true
  }

  /**
   * For each of the partitions find the best nodeId to read it from. A partition whose leader is
   * negative (`-1`) has none and is read from nowhere, its preferred read replica included, as the
   * Java consumer reads it from nowhere and asks for new metadata. It is left out of the result,
   * never raised as an error, so the node's other partitions are still fetched; the caller learns
   * of it through `routing.leaderless` and refreshes the metadata. A partition with no metadata
   * row is left out too, and a row whose leader is `null` or `undefined` is invalid.
   *
   * A preferred read replica is used while its lease has not expired and the replica itself is not
   * in the partition's `offlineReplicas`; otherwise the leader is. Only the preferred replica's
   * state matters: a leader listed as offline does not exclude an online preferred follower.
   *
   * The metadata rows are indexed by id for this call only, so the cost is linear in the topic's
   * partitions and nothing outlives the call: the next call reads the metadata as it is then.
   *
   * @param {string} topic
   * @param {number[]} partitions
   * @param {{expiresAt?: number, leaderless?: boolean}} [routing] an optional observation:
   *   `expiresAt` is lowered to the earliest preferred-replica expiry that decided a placement
   *   here, and `leaderless` is set when a partition had no leader. The no-preference path does no
   *   extra work for it.
   * @returns {{[nodeId: number]: number[]}} per-node assignment of partitions
   * @throws {import('../errors').KafkaJSError} for a metadata row whose leader is absent
   * @see Cluster~findLeaderForPartitions
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractFetch.java
   */
  // Invariant: The resulting object has each partition referenced exactly once
  findReadReplicaForPartitions(topic, partitions, routing) {
    const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic)
    // A short array is searched faster than it is indexed.
    const rows =
      partitionMetadata.length > SHORT_METADATA
        ? indexFirstBy(partitionMetadata, 'partitionId')
        : null
    const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topic]
    const result = {}
    for (const id of partitions) {
      const partitionId = parseInt(id, 10)
      const metadata = rows
        ? rows.get(partitionId)
        : partitionMetadata.find((p) => p.partitionId === partitionId)
      if (!metadata) {
        continue
      }

      if (metadata.leader == null) {
        throw new KafkaJSError('Invalid partition metadata', { topic, partitionId, metadata })
      }

      if (metadata.leader < 0) {
        if (routing) routing.leaderless = true
        continue
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
          this.routingChanged()
        } else if (preferredReadReplica != null) {
          // Valid entry, check whether it is not offline
          // Note that we don't delete the preference here, and rather hope that eventually that replica comes online again
          const offlineReplicas = metadata.offlineReplicas
          if (Array.isArray(offlineReplicas) && offlineReplicas.includes(preferredReadReplica)) {
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
            if (routing && expireAt < routing.expiresAt) routing.expiresAt = expireAt
          }
        }
      }
      ;(result[nodeId] ||= []).push(partitionId)
    }
    return result
  }

  /**
   * @param {number} nodeId the broker a fetch round asks
   * @param {import("../../types").TopicPartitions[]} topicPartitions the assignment
   * @param {{expiresAt?: number, leaderless?: boolean}} [routing] see `findReadReplicaForPartitions`
   * @returns {import("../../types").TopicPartitions[]} the partitions routed to that node
   */
  filterPartitionsByNode(nodeId, topicPartitions, routing) {
    return topicPartitions.map(({ topic, partitions }) => ({
      topic,
      partitions: this.findReadReplicaForPartitions(topic, partitions, routing)[nodeId] || [],
    }))
  }

  /**
   * Retires the pending refresh for unavailable leaders with its run, so a replacement run starts
   * with none pending and no spacing owed. The old refresh settles on its own fence: it publishes
   * nothing and cannot clear a replacement's entry.
   * @private
   * @returns {void}
   */
  retireLeaderlessRefresh() {
    this.leaderlessRefresh = null
    this.leaderlessRefreshAt = -Infinity
  }

  /**
   * Refreshes the metadata because a partition has no leader or a topic has no id, once for all
   * the nodes' fetchers of a run. Callers share the pending refresh, and a new one starts at most
   * once per initial retry interval, measured start to start; a due caller waits for its own
   * refresh, an early one returns at once. There is no retry timer: the next round that finds a
   * leader or an id missing and the interval elapsed asks again.
   *
   * The refresh is bound to the run's fence, so a stop or restart settles every caller and the
   * refresh publishes nothing afterwards. A failure is logged at debug level and never becomes a
   * membership verdict by itself; it is returned to every caller that waited for it, so a caller
   * can judge it for its own reason.
   * @private
   * @param {IdleWait} run the run that owns the calling round, captured before any await
   * @returns {Promise<Error|undefined>} the failure of the refresh this caller waited for, if it
   *   failed; undefined when it succeeded or was not due
   */
  async refreshLeaderlessMetadata(run) {
    if (run.fence.closed) return
    let attempt = this.leaderlessRefresh
    if (!attempt) {
      const now = performance.now()
      if (now - this.leaderlessRefreshAt < this.leaderlessRefreshInterval) return
      this.leaderlessRefreshAt = now
      attempt = { promise: null }
      attempt.promise = this.refreshCluster({ fence: run.fence })
        .catch((error) => {
          if (!run.fence.closed) {
            this.logger.debug('Metadata refresh for unavailable leaders failed', {
              groupId: this.groupId,
              memberId: this.memberId,
              error: describeThrown(error),
            })
          }
          attempt.error = error
        })
        .finally(() => {
          if (this.leaderlessRefresh === attempt) this.leaderlessRefresh = null
        })
      this.leaderlessRefresh = attempt
    }
    await run.fence.wait(attempt.promise).catch(() => {})
    return attempt.error
  }

  /**
   * Refreshes the metadata for a round that left a topic out for a missing id, and fails the
   * round when the refresh learned a refusal that will not clear (`TOPIC_AUTHORIZATION_FAILED`,
   * `INVALID_TOPIC_EXCEPTION`), as a conditional refresh before the round would. Other failures
   * are waited out: the next refresh may bring the id.
   * @private
   * @param {IdleWait} run the run that owns the calling round
   * @returns {Promise<void>}
   * @throws {Error} the refresh's terminal topic refusal
   */
  async refreshMissingTopicIds(run) {
    const error = await this.refreshLeaderlessMetadata(run)
    if (isFatalTopicError(error)) throw error
  }
}
