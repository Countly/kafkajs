const sleep = require('../utils/sleep')
const websiteUrl = require('../utils/websiteUrl')
const arrayDiff = require('../utils/arrayDiff')
const createRetry = require('../retry')
const sharedPromiseTo = require('../utils/sharedPromiseTo')

const OffsetManager = require('./offsetManager')
const Batch = require('./batch')
const SeekOffsets = require('./seekOffsets')
const SubscriptionState = require('./subscriptionState')
const {
  events: { GROUP_JOIN, HEARTBEAT, CONNECT, RECEIVED_UNSUBSCRIBED_TOPICS },
} = require('./instrumentationEvents')
const { MemberAssignment } = require('./assignerProtocol')
const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSOffsetOutOfRange,
  KafkaJSStaleTopicMetadataAssignment,
  isRebalancing,
} = require('../errors')
const { failure, createErrorFromCode, errorCodes } = require('../protocol/error')
const apiKeys = require('../protocol/requests/apiKeys')
const { requests } = require('../protocol/requests')

// The one error code `fetch()` below special-cases, the same way fetch/v4/response.js's `parse`
// does for every Fetch version whose parse throws on a partition error.
const { code: OFFSET_OUT_OF_RANGE_ERROR_CODE } = errorCodes.find(
  (e) => e.type === 'OFFSET_OUT_OF_RANGE'
)

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
   * At most one metadata refresh happens here, after the response has been walked, and only when
   * the broker answered with a topic id this request never asked for.
   *
   * A partition-level error code is surfaced by throwing the first one found, which is what the
   * fetch codecs below v12 do inside their own `parse`; from v12 the codec returns the codes as
   * data and this method throws them instead, so the behaviour a caller sees is the same.
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
                activeTopicPartitions[topic].has(partition)
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

      const { responses } = await broker.fetch({
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

      /**
       * Partition errors are surfaced here for the versions whose parse does not throw. Below
       * this, `broker.fetch` has already thrown for a failing partition and this is unreached;
       * from here, the first failing partition across every response is thrown exactly as
       * fetch/v4/response.js's `parse` throws it, so everything downstream - including
       * `recoverFromFetch`/`recoverFromOffsetOutOfRange` - sees the same error it always has.
       */
      const failedPartition = responses
        .flatMap(({ topicName, partitions }) =>
          partitions
            .filter((partition) => failure(partition.errorCode))
            .map((partition) => Object.assign({}, partition, { topic: topicName }))
        )
        .shift()

      if (failedPartition) {
        const { errorCode, topic, partition } = failedPartition
        if (errorCode === OFFSET_OUT_OF_RANGE_ERROR_CODE) {
          throw new KafkaJSOffsetOutOfRange(createErrorFromCode(errorCode), { topic, partition })
        }

        throw createErrorFromCode(errorCode)
      }

      let needsRefresh = false

      const batches = responses.flatMap(({ topicName, topicId, partitions }) => {
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
          return []
        }

        const topicRequestData = requests.find(({ topic }) => topic === topicName)

        let preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topicName]
        if (!preferredReadReplicas) {
          this.preferredReadReplicasPerTopicPartition[topicName] = preferredReadReplicas = {}
        }

        return partitions
          .filter(
            ({ partition }) =>
              !this.seekOffset.has(topicName, partition) &&
              !this.subscriptionState.isPaused(topicName, partition)
          )
          .map((partitionData) => {
            const { partition, preferredReadReplica } = partitionData

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
              ({ partition }) => partition === partitionData.partition
            )

            const fetchedOffset = partitionRequestData.fetchOffset
            return new Batch(topicName, fetchedOffset, partitionData)
          })
      })

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
