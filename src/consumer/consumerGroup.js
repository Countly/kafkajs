const sleep = require('../utils/sleep')
const websiteUrl = require('../utils/websiteUrl')
const arrayDiff = require('../utils/arrayDiff')
const createRetry = require('../retry')
const sharedPromiseTo = require('../utils/sharedPromiseTo')

const OffsetManager = require('./offsetManager')
const Batch = require('./batch')
const SeekOffsets = require('./seekOffsets')
const SubscriptionState = require('./subscriptionState')
const PositionValidator = require('./positionValidator')
const {
  events: { GROUP_JOIN, HEARTBEAT, CONNECT, RECEIVED_UNSUBSCRIBED_TOPICS, LOG_TRUNCATION },
} = require('./instrumentationEvents')
const { MemberAssignment } = require('./assignerProtocol')
const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSStaleTopicMetadataAssignment,
  isRebalancing,
} = require('../errors')
const { failure, createErrorFromCode } = require('../protocol/error')
const {
  partitionFetchAction,
  topLevelFetchAction,
  fetchedPartitionDropReason,
} = require('./fetchErrorContract')
const apiKeys = require('../protocol/requests/apiKeys')
const { requests } = require('../protocol/requests')

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

const PRIVATE = {
  JOIN: Symbol('private:ConsumerGroup:join'),
  SYNC: Symbol('private:ConsumerGroup:sync'),
  SHARED_HEARTBEAT: Symbol('private:ConsumerGroup:sharedHeartbeat'),
}

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
   * @param {string} [options.groupInstanceId=null] Static membership id (KIP-345); sent on
   *                                JoinGroup/SyncGroup/Heartbeat/LeaveGroup so the coordinator
   *                                recognizes this member across restarts.
   * @param {number} options.metadataMaxAge
   * @param {Map<string, string>} [options.pendingTopicResets] Topics deleted and recreated while
   *                                subscribed, each keyed by topic and stamped with its new topic
   *                                id (owned by the long-lived `Consumer`; passed through to every
   *                                `OffsetManager` this group builds, since the group itself is
   *                                rebuilt on every crash restart).
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
    metadataMaxAge,
    pendingTopicResets = new Map(),
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
    this.assigners = assigners
    this.sessionTimeout = sessionTimeout
    this.rebalanceTimeout = rebalanceTimeout
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
    this.groupInstanceId = groupInstanceId
    this.metadataMaxAge = metadataMaxAge
    this.pendingTopicResets = pendingTopicResets
    this.staticMembershipInactiveWarned = false

    this.seekOffset = new SeekOffsets()
    this.coordinator = null
    this.generationId = null
    this.leaderId = null
    this.memberId = null
    this.members = null
    this.groupProtocol = null

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

    this.lastRequest = Date.now()

    this[PRIVATE.SHARED_HEARTBEAT] = sharedPromiseTo(async ({ interval }) => {
      const { groupId, generationId, memberId } = this
      const now = Date.now()

      if (memberId && now >= this.lastRequest + interval) {
        const payload = {
          groupId,
          memberId,
          groupGenerationId: generationId,
          groupInstanceId: this.groupInstanceId,
        }

        await this.coordinator.heartbeat(payload)
        this.instrumentationEmitter.emit(HEARTBEAT, payload)
        this.lastRequest = Date.now()
      }
    })
  }

  isLeader() {
    return this.leaderId && this.memberId === this.leaderId
  }

  getNodeIds() {
    return this.cluster.getNodeIds()
  }

  async connect() {
    await this.cluster.connect()
    this.instrumentationEmitter.emit(CONNECT)
    await this.cluster.refreshMetadataIfNecessary()
  }

  async [PRIVATE.JOIN]() {
    const { groupId, sessionTimeout, rebalanceTimeout, groupInstanceId } = this

    this.coordinator = await this.cluster.findGroupCoordinator({ groupId })

    if (groupInstanceId) {
      this.warnIfStaticMembershipInactive()
    }

    const groupData = await this.coordinator.joinGroup({
      groupId,
      sessionTimeout,
      rebalanceTimeout,
      memberId: this.memberId || '',
      groupInstanceId,
      groupProtocols: this.assigners.map((assigner) =>
        assigner.protocol({
          topics: this.topicsSubscribed,
        })
      ),
    })

    this.generationId = groupData.generationId
    this.leaderId = groupData.leaderId
    this.memberId = groupData.memberId
    this.members = groupData.members
    this.groupProtocol = groupData.groupProtocol
    // JoinGroup v5's response carries no group_instance_id field for this member: it only
    // appears per-entry in `members`, populated for the leader. this.groupInstanceId is the
    // value the caller configured and must not be overwritten with undefined here.
  }

  /**
   * Warn once (per instance) when `groupInstanceId` is configured but the coordinator negotiated a
   * JoinGroup version below 5: only v5+ carries `group_instance_id` on the wire (KIP-345), so
   * static membership is silently inactive on an older negotiated version.
   *
   * The version comes from the protocol layer's own selection rule rather than being recomputed
   * here. A coordinator that advertises no usable JoinGroup at all answers `null` and this returns
   * quietly, matching the previous behaviour — `lookupRequest` would have thrown, inside the sync.
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
   * @returns {void}
   */
  warnIfStaticMembershipInactive() {
    if (this.staticMembershipInactiveWarned) {
      return
    }

    const negotiatedVersion = this.coordinator.negotiatedVersion(
      apiKeys.JoinGroup,
      requests.JoinGroup
    )

    if (negotiatedVersion == null) {
      return
    }

    if (negotiatedVersion < 5) {
      this.logger.warn(
        `groupInstanceId is set but the broker negotiated JoinGroup v${negotiatedVersion} - static membership is inactive`,
        { groupId: this.groupId, groupInstanceId: this.groupInstanceId, negotiatedVersion }
      )
    }

    this.staticMembershipInactiveWarned = true
  }

  async leave() {
    const { groupId, memberId, groupInstanceId } = this
    if (memberId) {
      await this.coordinator.leaveGroup({ groupId, memberId, groupInstanceId })
      this.memberId = null
    }
  }

  async [PRIVATE.SYNC]() {
    let assignment = []
    const {
      groupId,
      generationId,
      memberId,
      members,
      groupProtocol,
      topics,
      topicsSubscribed,
      coordinator,
      sessionTimeout,
      rebalanceTimeout,
      groupInstanceId,
    } = this

    if (this.isLeader()) {
      this.logger.debug('Chosen as group leader', { groupId, generationId, memberId, topics })
      const assigner = this.assigners.find(({ name }) => name === groupProtocol)

      if (!assigner) {
        throw new KafkaJSNonRetriableError(
          `Unsupported partition assigner "${groupProtocol}", the assigner wasn't found in the assigners list`
        )
      }

      await this.cluster.refreshMetadata()
      assignment = await assigner.assign({ members, topics: topicsSubscribed })

      this.logger.debug('Group assignment', {
        groupId,
        generationId,
        groupProtocol,
        assignment,
        topics: topicsSubscribed,
      })
    }

    // Keep track of the partitions for the subscribed topics
    this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
    const { memberAssignment } = await this.coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupInstanceId,
      groupAssignment: assignment,
      sessionTimeout,
      rebalanceTimeout,
    })

    const decodedMemberAssignment = MemberAssignment.decode(memberAssignment)
    const decodedAssignment =
      decodedMemberAssignment != null ? decodedMemberAssignment.assignment : {}

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
    this.subscriptionState.assign(currentMemberAssignment)
    this.offsetManager = new OffsetManager({
      cluster: this.cluster,
      topicConfigurations: this.topicConfigurations,
      instrumentationEmitter: this.instrumentationEmitter,
      memberAssignment: currentMemberAssignment.reduce(
        (partitionsByTopic, { topic, partitions }) => ({
          ...partitionsByTopic,
          [topic]: partitions,
        }),
        {}
      ),
      autoCommit: this.autoCommit,
      autoCommitInterval: this.autoCommitInterval,
      autoCommitThreshold: this.autoCommitThreshold,
      coordinator,
      groupId,
      generationId,
      memberId,
      groupInstanceId: this.groupInstanceId,
      pendingTopicResets: this.pendingTopicResets,
    })
    this.consecutiveFetchErrors.clear()
    this.positionValidator.clear()
    this.validationUnusableLogged.clear()
  }

  joinAndSync() {
    const startJoin = Date.now()
    return this.retrier(async (bail) => {
      try {
        await this[PRIVATE.JOIN]()
        await this[PRIVATE.SYNC]()

        const memberAssignment = this.assigned().reduce(
          (result, { topic, partitions }) => ({ ...result, [topic]: partitions }),
          {}
        )

        const payload = {
          groupId: this.groupId,
          memberId: this.memberId,
          leaderId: this.leaderId,
          isLeader: this.isLeader(),
          memberAssignment,
          groupProtocol: this.groupProtocol,
          duration: Date.now() - startJoin,
        }

        this.instrumentationEmitter.emit(GROUP_JOIN, payload)
        this.logger.info('Consumer has joined the group', payload)
      } catch (e) {
        if (isRebalancing(e)) {
          // Rebalance in progress isn't a retriable protocol error since the consumer
          // has to go through find coordinator and join again before it can
          // actually retry the operation. We wrap the original error in a retriable error
          // here instead in order to restart the join + sync sequence using the retrier.
          throw new KafkaJSError(e)
        }

        if (e.type === 'UNKNOWN_MEMBER_ID') {
          this.memberId = null
          throw new KafkaJSError(e)
        }

        bail(e)
      }
    })
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
   * corresponding one-shot reset is applied at the next offset resolution.
   *
   * A no-op before the first sync (`offsetManager` is null until then) and for a topic outside
   * the current member assignment - the second guard keeps this member from writing a key it
   * does not own into the committed-offsets map, which is shared by every consumer built from
   * the same client with the same group id.
   *
   * Precedence with `seek()`: a seek pending here (queued but not yet applied) targets the
   * *old* incarnation and is dropped along with everything else, via `seekOffset.deleteTopic()`.
   * A seek issued *after* this call returns, applied in the same `seekOffsets()` cycle, targets
   * the current incarnation and wins over the reset for that partition - see
   * `ConsumerGroup#seekOffsets()` and `OffsetManager#drainPendingTopicResets()`.
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

  async commitOffsetsIfNecessary() {
    await this.offsetManager.commitOffsetsIfNecessary()
  }

  async commitOffsets(offsets) {
    await this.offsetManager.commitOffsets(offsets)
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

  async heartbeat({ interval }) {
    return this[PRIVATE.SHARED_HEARTBEAT]({ interval })
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
    try {
      await this.cluster.refreshMetadataIfNecessary()
      this.checkForStaleAssignment()

      let topicPartitions = this.subscriptionState.assigned()
      topicPartitions = this.filterPartitionsByNode(nodeId, topicPartitions)

      await this.seekOffsets(topicPartitions)
      await this.validatePositions(topicPartitions)

      const committedOffsets = this.offsetManager.committedOffsets()
      const activeTopicPartitions = this.getActiveTopicPartitions()

      const requests = topicPartitions
        .map(({ topic, partitions }) => ({
          topic,
          partitions: partitions
            .filter(
              (partition) =>
                /**
                 * When recovering from OffsetOutOfRange, each partition can recover
                 * concurrently, which invalidates resolved and committed offsets as part
                 * of the recovery mechanism (see OffsetManager.clearOffsets). In concurrent
                 * scenarios this can initiate a new fetch with invalid offsets.
                 *
                 * This was further highlighted by https://github.com/tulios/kafkajs/pull/570,
                 * which increased concurrency, making this more likely to happen.
                 *
                 * This is solved by only making requests for partitions with initialized offsets.
                 *
                 * See the following pull request which explains the context of the problem:
                 * @issue https://github.com/tulios/kafkajs/pull/578
                 */
                committedOffsets[topic][partition] != null &&
                activeTopicPartitions[topic].has(partition) &&
                // A partition another fetcher put into validation is left out of this request:
                // its position is the thing being checked, so fetching from it would be reading
                // from an offset this client is not yet sure still exists.
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

      if (!requests.length) {
        await sleep(this.maxWaitTime)
        return []
      }

      // Topic ids are all-or-nothing per request: a fetch goes out keyed by id only when every
      // topic in it has one, otherwise the whole request falls back to the last version keyed by
      // name. This is the rule the Java consumer applies, and it avoids splitting one round into
      // two requests at two versions. No extra metadata refresh is issued for a missing id: the
      // round already refreshed if necessary, and an id is only missing inside a delete/recreate
      // window.
      let topicIds = new Map()
      let maxVersion
      for (const { topic } of requests) {
        const topicId = this.cluster.topicId(topic)
        if (topicId === undefined) {
          topicIds = undefined
          maxVersion = FETCH_TOPIC_NAMES_MAX_VERSION
          break
        }
        topicIds.set(topic, topicId)
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

      let needsRefresh = false

      // The top-level code is only decoded from Fetch v7 up, so an older response carries none -
      // and `failure(undefined)` is true, which is why the guard is on the field's presence.
      if (topLevelErrorCode != null && failure(topLevelErrorCode)) {
        if (topLevelFetchAction(topLevelErrorCode) === 'throw') {
          // Everything not in the table keeps the recovery path a request-level fetch error has
          // always taken: refresh and rejoin for stale metadata, crash for anything else.
          throw createErrorFromCode(topLevelErrorCode)
        }

        this.logger.warn('Fetch response carried a top-level error, refreshing metadata', {
          groupId: this.groupId,
          memberId: this.memberId,
          errorCode: topLevelErrorCode,
        })
        await this.cluster.refreshMetadata()
        return []
      }

      const batches = []

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

        for (const partitionData of partitions) {
          const { partition, errorCode, preferredReadReplica } = partitionData

          if (failure(errorCode)) {
            const action = partitionFetchAction(errorCode)

            if (action === 'throw') {
              throw createErrorFromCode(errorCode)
            }

            this.recordFetchError(topicName, partition, errorCode)

            if (action === 'offset-out-of-range') {
              // The existing recovery, for this partition only: drop the preferred replica if one
              // is set, otherwise reset per the topic's own fromBeginning.
              await this.recoverFromOffsetOutOfRange({ topic: topicName, partition })
            } else if (action === 'skip-and-forget-epoch') {
              // The client is ahead of this broker. Defending the cached epoch would make every
              // retry resend the same rejected value; forgetting it makes the next request carry
              // -1 and the next metadata response converge. Warned every time, because this
              // rewrites cached cluster state and usually clears on the very next round.
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

              // A partition whose hint was usable already knows where to go next round; one whose
              // hint was dropped, or that never carried one, needs the refresh.
              needsRefresh = needsRefresh || !applied
            }

            continue
          }

          const dropReason = fetchedPartitionDropReason({
            seeked: this.seekOffset.has(topicName, partition),
            paused: this.subscriptionState.isPaused(topicName, partition),
            awaitingValidation: this.positionValidator.isAwaiting(topicName, partition),
          })

          if (dropReason != null) {
            continue
          }

          // Guarded on the map being non-empty: in a healthy steady state it always is empty, and
          // this is the per-partition success path of every fetch round.
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
        }
      }

      if (needsRefresh) {
        await this.cluster.refreshMetadata()
      }

      return batches
    } catch (e) {
      await this.recoverFromFetch(e)
      return []
    }
  }

  async recoverFromFetch(e) {
    if (STALE_METADATA_ERRORS.includes(e.type) || e.name === 'KafkaJSTopicMetadataNotLoaded') {
      this.logger.debug('Stale cluster metadata, refreshing...', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: e.message,
      })

      await this.cluster.refreshMetadata()
      await this.joinAndSync()
      return
    }

    if (e.name === 'KafkaJSStaleTopicMetadataAssignment') {
      this.logger.warn(`${e.message}, resync group`, {
        groupId: this.groupId,
        memberId: this.memberId,
        topic: e.topic,
        unknownPartitions: e.unknownPartitions,
      })

      await this.joinAndSync()
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

  checkForStaleAssignment() {
    if (!this.partitionsPerSubscribedTopic) {
      return
    }

    const newPartitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()

    for (const [topic, partitions] of newPartitionsPerSubscribedTopic) {
      const diff = arrayDiff(partitions, this.partitionsPerSubscribedTopic.get(topic))

      if (diff.length > 0) {
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
   * partition: `resolveOffsets()` is told which partitions were just seeked and leaves them
   * alone. No `Set` is allocated when nothing was seeked.
   * @param {import("../../types").TopicPartitions[]} topicPartitions
   * @returns {Promise<void>}
   */
  async seekOffsets(topicPartitions) {
    let seekedPartitions
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

        const applied = await this.offsetManager.seek(seekEntry)
        if (applied) {
          seekedPartitions = seekedPartitions || new Set()
          seekedPartitions.add(
            OffsetManager.seekedPartitionKey(seekEntry.topic, seekEntry.partition)
          )
        }
      }
    }

    await this.offsetManager.resolveOffsets(seekedPartitions)
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
   *   offset, whose tracked epoch was cleared with that offset, so they answer 'validated' with no
   *   request and are safe to touch.
   * @returns {Promise<void>}
   */
  async validatePositions(topicPartitions) {
    let awaiting

    try {
      for (const { topic, partitions } of topicPartitions) {
        for (const partition of partitions) {
          // A queued user seek is newer, more specific intent than anything validation could
          // conclude. seekOffsets() has just popped this round's seeks, so the only entries this
          // can find are seeks that arrived while validation itself was awaiting.
          if (this.seekOffset.has(topic, partition)) {
            continue
          }

          const currentLeaderEpoch = this.cluster.leaderEpoch(topic, partition)
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
      // The invariant: no partition may be left awaiting with no request in flight.
      for (const { topic, partition, currentLeaderEpoch } of awaiting || []) {
        if (this.positionValidator.isAwaiting(topic, partition)) {
          this.positionValidator.markValidated(topic, partition, currentLeaderEpoch)
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
        for (const { partition, currentLeaderEpoch } of entries) {
          if (!this.positionValidator.isAwaiting(topic, partition)) {
            continue
          }

          // The epoch is cleared and the offset left alone: the partition is fetched from where it
          // is, unvalidated, and validates again once it consumes a record.
          this.offsetManager.forgetConsumedEpoch(topic, partition)
          this.positionValidator.markValidated(topic, partition, currentLeaderEpoch)
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
   * @private
   * @param {{topic: string, entries: object[]}[]} topics
   * @returns {void}
   */
  markAllValidated(topics) {
    for (const { topic, entries } of topics) {
      for (const { partition, currentLeaderEpoch } of entries) {
        this.positionValidator.markValidated(topic, partition, currentLeaderEpoch)
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
   * For each of the partitions find the best nodeId to read it from
   *
   * @param {string} topic
   * @param {number[]} partitions
   * @returns {{[nodeId: number]: number[]}} per-node assignment of partitions
   * @see Cluster~findLeaderForPartitions
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
