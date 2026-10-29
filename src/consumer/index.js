const { failureSourceOf } = require('../broker/failureSource')
const Long = require('../utils/long')
const dictionary = require('../utils/dictionary')
const { initialRetryTime } = require('../retry/defaults')
const ConsumerGroup = require('./consumerGroup')
const ResetObligations = require('./offsetManager/resetObligations')
const { createGroupDescriptions } = require('../admin/groupDescriptions')
const resolveGroupTimeouts = require('./groupTimeouts')
const { selectRebalanceProtocol } = require('./groupProtocol/interface')
const Runner = require('./runner')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const { KafkaJSNonRetriableError, isFencedInstance } = require('../errors')
const { isConsumerProtocolFatal } = require('./rebalanceErrorContract')
const { roundRobin } = require('./assigners')
const {
  EARLIEST_OFFSET,
  LATEST_OFFSET,
  DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES,
} = require('../constants')
const ISOLATION_LEVEL = require('../protocol/isolationLevel')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const { registerDisposer } = require('./disposal')

const { keys, values } = Object
const { CONNECT, DISCONNECT, STOP, CRASH } = events

const eventNames = values(events)
const eventKeys = keys(events)
  .map((key) => `consumer.events.${key}`)
  .join(', ')

const specialOffsets = [
  Long.fromValue(EARLIEST_OFFSET).toString(),
  Long.fromValue(LATEST_OFFSET).toString(),
]

/**
 * Keys the classic protocol owns, in the order the Java client lists them, so a rejection naming
 * several reads the same way theirs does.
 */
const CLASSIC_ONLY_KEYS = ['partitionAssigners', 'heartbeatInterval', 'sessionTimeout']

/**
 * Whether the user actually set this option.
 *
 * Presence alone carries no intent: `Kafka#consumer()` rebuilds its argument as a fresh object
 * literal, so every option it forwards is an own key holding `undefined` when nothing was set.
 * @param {object} params
 * @param {string} key
 * @returns {boolean}
 */
const isExplicit = (params, key) => Object.hasOwn(params, key) && params[key] !== undefined

/**
 * Whether the user actually set a NULLABLE option.
 *
 * `groupRemoteAssignor`'s documented default is `null` - "let the coordinator pick its first
 * configured assignor" - so `null` has to read as "not set", both for the classic rejection and
 * for the non-empty-string rule. Threading an optional config value that is `null` when absent
 * must not make a consumer unconstructible.
 * @param {object} params
 * @param {string} key
 * @returns {boolean}
 */
const isExplicitAndNotNull = (params, key) => isExplicit(params, key) && params[key] !== null

/**
 * Rejects a configuration that mixes options from the two group protocols, and a prefetch depth
 * this release does not implement.
 *
 * The broker owns the session timeout, the heartbeat interval and the assignment under the consumer
 * protocol, so accepting those options would silently ignore them; and a `maxPrefetchBatches` above
 * 1 is rejected rather than clamped, so a value whose semantics do not exist yet cannot freeze the
 * wrong contract when deeper prefetch lands.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @param {object} params the raw options object, before any default is applied
 * @returns {void}
 * @throws {KafkaJSNonRetriableError}
 */
const validateGroupProtocolOptions = (params) => {
  const groupProtocol = isExplicit(params, 'groupProtocol') ? params.groupProtocol : 'classic'

  if (groupProtocol !== 'classic' && groupProtocol !== 'consumer') {
    throw new KafkaJSNonRetriableError('groupProtocol must be one of "classic" or "consumer"')
  }

  if (groupProtocol === 'consumer') {
    const offending = CLASSIC_ONLY_KEYS.filter((key) => isExplicit(params, key))

    if (offending.length) {
      throw new KafkaJSNonRetriableError(
        `${offending.join(', ')} cannot be set when groupProtocol=consumer`
      )
    }
  } else if (isExplicitAndNotNull(params, 'groupRemoteAssignor')) {
    throw new KafkaJSNonRetriableError(
      'groupRemoteAssignor cannot be set when groupProtocol=classic'
    )
  }

  if (
    isExplicitAndNotNull(params, 'groupRemoteAssignor') &&
    (typeof params.groupRemoteAssignor !== 'string' || params.groupRemoteAssignor.length === 0)
  ) {
    throw new KafkaJSNonRetriableError('Argument "groupRemoteAssignor" must be a non-empty string')
  }

  if (isExplicit(params, 'maxPrefetchBatches') && ![0, 1].includes(params.maxPrefetchBatches)) {
    throw new KafkaJSNonRetriableError(
      'maxPrefetchBatches must be 0 or 1; deeper prefetch is not supported yet'
    )
  }
}

/**
 * Reads a `disconnect()` argument: an options object, `{ leaveGroup?: boolean }`, or nothing.
 *
 * Anything that is not an object is ignored, exactly as kafkajs 2.2.4's `disconnect()` ignored
 * every argument: `process.on('SIGTERM', consumer.disconnect)` passes the signal name, and
 * rejecting it would surface as an unhandled rejection inside a signal handler.
 * @param {*} [options]
 * @returns {{ leaveGroup?: boolean }|undefined} what to hand to the stop: `undefined` when
 *   `options` is absent (`null` included) or is not an object, so everything below sees an object
 *   or nothing
 * @throws {KafkaJSNonRetriableError} when `options` is an object whose `leaveGroup` is neither
 *   `undefined` nor a boolean
 */
const parseDisconnectOptions = (options) => {
  if (options == null || typeof options !== 'object') {
    return undefined
  }

  if (options.leaveGroup !== undefined && typeof options.leaveGroup !== 'boolean') {
    throw new KafkaJSNonRetriableError('Argument "leaveGroup" must be a boolean')
  }

  return options
}

/**
 * @param {Object} params
 * @param {import("../../types").Cluster} params.cluster
 * @param {String} params.groupId
 * @param {import('../../types').RetryOptions} [params.retry]
 * @param {import('../../types').Logger} params.logger
 * @param {import('../../types').PartitionAssigner[]} [params.partitionAssigners]
 * @param {number} [params.sessionTimeout=45000] Integer from 1 to 2147483647; classic only.
 * @param {number} [params.maxPollInterval=300000] The maximum time between two batches handed to
 *                                your handler, and the rebalance timeout sent on the wire. A
 *                                handler that exceeds it loses its partitions and the consumer
 *                                rejoins, as in the Java client's `max.poll.interval.ms`.
 * @param {number} [params.rebalanceTimeout] Deprecated alias for the wire rebalance timeout only.
 *                                It is sent as configured, but never lowers the poll deadline,
 *                                which stays at least 300000 ms.
 * @param {number} [params.heartbeatInterval=3000] Positive integer below sessionTimeout; classic only.
 * @param {number} [params.maxBytesPerPartition]
 * @param {number} [params.minBytes]
 * @param {number} [params.maxBytes]
 * @param {number} [params.maxWaitTimeInMs]
 * @param {number} [params.isolationLevel]
 * @param {string} [params.rackId]
 * @param {boolean} [params.checkCrcs=true] Verify the CRC32C checksum of every record batch
 *                                received (Java's `check.crcs`). A mismatch halts the partition
 *                                like any corrupt batch (`consumer.partition_halted`, reason
 *                                `checksum`); its other partitions keep delivering.
 * @param {number} [params.maxDecompressedBatchBytes=1073741824] The most a record batch may
 *                                decompress to, in bytes, for every codec; it also sets a ceiling of
 *                                `max(1, floor(bytes / 512))` records per batch. A batch past
 *                                either halts the partition.
 * @param {String} [params.groupInstanceId=null] Static membership id (KIP-345). When set, this
 *                                consumer keeps its group slot across a `disconnect()`/reconnect
 *                                within `sessionTimeout` instead of triggering a rebalance, and
 *                                does not send LeaveGroup on a normal shutdown. A duplicate
 *                                `groupInstanceId` in the same group admits the new joiner and
 *                                fences the *existing* member instead (`FENCED_INSTANCE_ID`, code
 *                                82); the fenced consumer crashes without restarting rather than
 *                                loop against a fencing broker.
 * @param {InstrumentationEventEmitter} [params.instrumentationEmitter]
 * @param {number} params.metadataMaxAge
 * @param {'classic'|'consumer'} [params.groupProtocol='classic'] The group membership protocol.
 *                                `'consumer'` (KIP-848) lets the coordinator compute the
 *                                assignment, so `partitionAssigners`, `heartbeatInterval` and
 *                                `sessionTimeout` are rejected with it.
 * @param {string|null} [params.groupRemoteAssignor=null] The server-side assignor to ask for under
 *                                the consumer protocol; `null` lets the coordinator choose.
 *                                Rejected under the classic protocol.
 * @param {0|1} [params.maxPrefetchBatches=0] How many fetch responses may be in flight ahead of the
 *                                batch being processed. Anything else is rejected rather than
 *                                clamped.
 *
 * @returns {import("../../types").Consumer}
 */
module.exports = (params) => {
  validateGroupProtocolOptions(params)

  const {
    cluster,
    groupId,
    retry,
    logger: rootLogger,
    partitionAssigners = [roundRobin],
    sessionTimeout = 45000,
    maxPollInterval,
    rebalanceTimeout,
    heartbeatInterval = 3000,
    maxBytesPerPartition = 1048576, // 1MB
    minBytes = 1,
    maxBytes = 10485760, // 10MB
    maxWaitTimeInMs = 5000,
    isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
    rackId = '',
    checkCrcs = true,
    maxDecompressedBatchBytes = DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES,
    groupInstanceId = null,
    instrumentationEmitter: rootInstrumentationEmitter,
    metadataMaxAge,
    groupProtocol = 'classic',
    groupRemoteAssignor = null,
    maxPrefetchBatches = 0,
  } = params

  if (!groupId) {
    throw new KafkaJSNonRetriableError('Consumer groupId must be a non-empty string.')
  }

  const logger = rootLogger.namespace('Consumer')
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()
  const isConsumerProtocol = groupProtocol === 'consumer'

  // The coordinator computes the assignment under the consumer protocol, so there is nothing to
  // build and no protocol to select from an empty list.
  const assigners = isConsumerProtocol
    ? []
    : partitionAssigners.map((createAssigner) => createAssigner({ groupId, logger, cluster }))

  /** @type {Record<string, { fromBeginning?: boolean }>} */
  const topics = dictionary()
  /** @type {string|null} the RE2 pattern a regex subscription sent, verbatim */
  let subscribedTopicRegex = null
  /** @type {{ fromBeginning: boolean }|null} the options that subscription was made with */
  let regexSubscription = null
  let runner = null
  /** @type {ConsumerGroup} */
  let consumerGroup = null
  let restartTimeout = null
  // Bumped on every stop() and disconnect() invocation (including the disconnect onCrash triggers
  // internally as part of its own cleanup), so onCrash can tell whether a *separate* stop happened
  // while it was still deciding whether to restart (e.g. a slow restartOnFailure callback).
  let stopGeneration = 0

  /**
   * The generation and member id of this consumer's last completed join, or null before its
   * first. `groupMetadata()` reports the classic pair from here because the live classic
   * membership drops both when a generation is lost (a poll-interval overrun, UNKNOWN_MEMBER_ID),
   * and a classic group accepts a transactional offset commit carrying no generation and no member
   * id without fencing it. Java's classic consumer keeps its group metadata from the last join the
   * same way.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java
   * @see https://github.com/apache/kafka/blob/4.3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/classic/ClassicGroup.java
   * @type {{ generationId: number, memberId: string }|null}
   */
  let lastJoin = null

  /**
   * Records the pair of the join that just completed. The runner calls it before `GROUP_JOIN`
   * is delivered, so a `kafkajs.consumer.group_join` subscriber and an `on()` listener both read
   * the new pair from `groupMetadata()`.
   * @returns {void}
   */
  const recordJoin = () => {
    if (consumerGroup) {
      lastJoin = { generationId: consumerGroup.generationId, memberId: consumerGroup.memberId }
    }
  }

  const groupTimeouts = resolveGroupTimeouts({
    maxPollInterval,
    rebalanceTimeout,
    sessionTimeout,
    heartbeatInterval,
    logger,
  })

  const rebalanceProtocol = isConsumerProtocol ? undefined : selectRebalanceProtocol(assigners)

  if (!isConsumerProtocol) {
    logger.info('Rebalance protocol selected', {
      groupId,
      rebalanceProtocol,
      assigners: assigners.map(({ name }) => name),
      reason: 'the highest protocol every configured assigner supports',
    })
  }

  /**
   * The resets this consumer owes the topics deleted and recreated while it was subscribed. Owned
   * here rather than on the ConsumerGroup or the OffsetManager because both of those are rebuilt -
   * the group on every crash restart, the manager on every sync - and a rebalance is exactly what
   * a recreation provokes. A crash restart unlinks them from the group's ledger and keeps them; a
   * user disconnect empties them.
   * @type {ResetObligations}
   */
  const obligations = new ResetObligations()
  let removeTopicRecreatedListener = null
  let pendingStart = Promise.resolve()

  /**
   * Subscribes to the cluster's topic-recreation event, at most once. Called at construction and
   * again from connect(), run() and start(), so a consumer reused after disconnect() - including
   * the restart onCrash schedules, which reaches start() alone - re-arms rather than running
   * with the reset silently inert.
   *
   * The handler body is wrapped in try/catch: it runs synchronously inside the cluster's
   * `emit()`, called from `BrokerPool#refreshMetadata` - an uncaught throw here would escape as
   * a metadata-refresh failure and could itself trigger a rebootstrap, turning a topic
   * recreation into a connection storm.
   * @returns {void}
   */
  const ensureTopicRecreatedSubscription = () => {
    if (removeTopicRecreatedListener) {
      return
    }

    removeTopicRecreatedListener = cluster.onTopicRecreated(({ payload }) => {
      try {
        const { topic, topicId } = payload

        if (!Object.hasOwn(topics, topic)) {
          return
        }

        obligations.owe(topic, topicId)
        cluster.offsetOperations({ groupId }).touchTopic(topic)
        consumerGroup && consumerGroup.invalidateTopic(topic)
      } catch (e) {
        logger.error(`Failed to handle topic recreation: ${e.message}`, {
          groupId,
          stack: e.stack,
          payload,
        })
      }
    })
  }

  ensureTopicRecreatedSubscription()

  /** @type {import("../../types").Consumer["connect"]} */
  const connect = async () => {
    ensureTopicRecreatedSubscription()
    await cluster.connect()
    instrumentationEmitter.emit(CONNECT)
  }

  /**
   * Stops the runner and disconnects the cluster, for `disconnect()` and for the crash handler.
   * A user disconnect ends the consumer, so its reset obligations are emptied. A crash restart
   * unlinks them and keeps them: the topics still owe those resets to the group it builds next.
   * @param {{ leaveGroup?: boolean }} [stopOptions]
   * @param {{ keepResets?: boolean }} [shutDownOptions] `true` for the crash handler
   * @returns {Promise<void>}
   */
  const shutDown = async (stopOptions, { keepResets = false } = {}) => {
    stopGeneration++

    try {
      await stopShared(stopOptions)
      logger.debug('consumer has stopped, disconnecting', { groupId })
      await cluster.disconnect()
      instrumentationEmitter.emit(DISCONNECT)
    } catch (e) {
      logger.error(`Caught error when disconnecting the consumer: ${e.message}`, {
        stack: e.stack,
        groupId,
      })
      throw e
    } finally {
      if (keepResets) {
        obligations.unlink()
      } else {
        removeTopicRecreatedListener && removeTopicRecreatedListener()
        removeTopicRecreatedListener = null
        obligations.clear()
      }
    }
  }

  /**
   * Stops the consumer and disconnects the cluster. `leaveGroup` (KIP-1092) overrides whether the
   * member leaves its group: `true` removes it at once, whatever its kind, `false` keeps it in the
   * group until its session expires, and `undefined` is the default (a dynamic member leaves, a
   * static one stays). A call that joins a stop already in flight keeps the first caller's choice.
   * Crashes and restarts always use the default, and so does a call whose argument is not an
   * options object.
   *
   * @type {import("../../types").Consumer["disconnect"]}
   * @param {{ leaveGroup?: boolean }} [options]
   * @throws {KafkaJSNonRetriableError} before anything is stopped, when `options` is an object
   *   whose `leaveGroup` is neither `undefined` nor a boolean
   */
  const disconnect = async (options) => shutDown(parseDisconnectOptions(options))

  const stopShared = sharedPromiseTo(async (options) => {
    try {
      if (runner) {
        await runner.stop(options)
        runner = null
        consumerGroup = null
        instrumentationEmitter.emit(STOP)
      }

      clearTimeout(restartTimeout)
      logger.info('Stopped', { groupId })
    } catch (e) {
      logger.error(`Caught error when stopping the consumer: ${e.message}`, {
        stack: e.stack,
        groupId,
      })

      throw e
    }
  })

  /**
   * Every call bumps the stop generation, even when it joins an in-flight stop, so a crash
   * handler deciding whether to restart can tell that the user asked for a stop meanwhile. It takes
   * no options: only `disconnect()` can override how the member leaves its group.
   * @type {import("../../types").Consumer["stop"]}
   */
  const stop = () => {
    stopGeneration++
    return stopShared()
  }

  /**
   * Ends a consumer that borrows its creator's cluster (see `disposeConsumer`): waits for a start
   * that is still in flight, since a `stop()` that lands before the runner is running does
   * nothing and the member would join and stay; stops it as `stop()` does; then gives up what it
   * registered on the cluster, the topic-recreation listener and its reset obligations. Unlike
   * `disconnect()` it leaves the cluster connected, because the cluster is shared with the
   * operations around it. The consumer is not reused. Idempotent. The listener is released even
   * when the stop fails, and the failure is rethrown.
   * @returns {Promise<void>}
   */
  const dispose = async () => {
    stopGeneration++

    try {
      await pendingStart
      await stopShared()
    } finally {
      removeTopicRecreatedListener && removeTopicRecreatedListener()
      removeTopicRecreatedListener = null
      obligations.clear()
    }
  }

  /** @type {import("../../types").Consumer["subscribe"]} */
  const subscribe = async (subscription = {}) => {
    const { topic, topics: subscriptionTopics, regex, fromBeginning = false } = subscription

    if (consumerGroup) {
      throw new KafkaJSNonRetriableError('Cannot subscribe to topic while consumer is running')
    }

    const wantsRegex = isExplicit(subscription, 'regex')
    const wantsNames = topic != null || subscriptionTopics != null

    // The combination rule comes first, so a call that is wrong several ways reports the thing the
    // user has to change: you subscribe one way or the other, not both.
    if (
      (wantsRegex && (wantsNames || keys(topics).length > 0)) ||
      (wantsNames && subscribedTopicRegex != null)
    ) {
      throw new KafkaJSNonRetriableError('Subscribe to topics or to a regex, not both')
    }

    if (wantsRegex) {
      if (!isConsumerProtocol) {
        throw new KafkaJSNonRetriableError(
          "A regex subscription requires groupProtocol: 'consumer'"
        )
      }

      if (typeof regex !== 'string' || regex.length === 0) {
        throw new KafkaJSNonRetriableError('Argument "regex" must be a non-empty string')
      }

      // Sent to the coordinator verbatim: RE2 is the broker's dialect, not JavaScript's, so there
      // is nothing this client can usefully validate about the pattern itself.
      subscribedTopicRegex = regex
      regexSubscription = { fromBeginning }
      return
    }

    if (!topic && !subscriptionTopics) {
      throw new KafkaJSNonRetriableError('Missing required argument "topics"')
    }

    if (subscriptionTopics != null && !Array.isArray(subscriptionTopics)) {
      throw new KafkaJSNonRetriableError('Argument "topics" must be an array')
    }

    const subscriptions = subscriptionTopics || [topic]

    for (const subscription of subscriptions) {
      if (typeof subscription !== 'string' && !(subscription instanceof RegExp)) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${subscription} (${typeof subscription}), the topic name has to be a String or a RegExp`
        )
      }
    }

    const hasRegexSubscriptions = subscriptions.some(
      (subscription) => subscription instanceof RegExp
    )
    const metadata = hasRegexSubscriptions ? await cluster.metadata() : undefined

    const topicsToSubscribe = []
    for (const subscription of subscriptions) {
      const isRegExp = subscription instanceof RegExp
      if (isRegExp) {
        const topicRegExp = subscription
        const matchedTopics = metadata.topicMetadata
          .map(({ topic: topicName }) => topicName)
          .filter((topicName) => topicRegExp.test(topicName))

        logger.debug('Subscription based on RegExp', {
          groupId,
          topicRegExp: topicRegExp.toString(),
          matchedTopics,
        })

        topicsToSubscribe.push(...matchedTopics)
      } else {
        topicsToSubscribe.push(subscription)
      }
    }

    for (const t of topicsToSubscribe) {
      topics[t] = { fromBeginning }
    }

    await cluster.addMultipleTargetTopics(topicsToSubscribe)
  }

  /** @type {import("../../types").Consumer["run"]} */
  const run = async ({
    autoCommit = true,
    autoCommitInterval = null,
    autoCommitThreshold = null,
    eachBatchAutoResolve = true,
    partitionsConsumedConcurrently: concurrency = 1,
    eachBatch = null,
    eachMessage = null,
  } = {}) => {
    ensureTopicRecreatedSubscription()

    if (consumerGroup) {
      logger.warn('consumer#run was called, but the consumer is already running', { groupId })
      return
    }

    const start = async (onCrash) => {
      ensureTopicRecreatedSubscription()
      logger.info('Starting', { groupId })

      consumerGroup = new ConsumerGroup({
        logger: rootLogger,
        topics: keys(topics),
        topicConfigurations: topics,
        retry,
        cluster,
        groupId,
        assigners,
        sessionTimeout: groupTimeouts.sessionTimeout,
        rebalanceTimeout: groupTimeouts.rebalanceTimeout,
        // The classic membership owns the heartbeat cadence and the timer reads it from there, so
        // without this line every classic consumer would silently drop to the membership's own
        // 3000 ms default whatever the user configured.
        heartbeatInterval: groupTimeouts.heartbeatInterval,
        maxBytesPerPartition,
        minBytes,
        maxBytes,
        maxWaitTimeInMs,
        instrumentationEmitter,
        isolationLevel,
        rackId,
        checkCrcs,
        maxDecompressedBatchBytes,
        groupInstanceId,
        rebalanceProtocol,
        groupProtocol,
        groupRemoteAssignor,
        subscribedTopicRegex,
        regexSubscription,
        maxPrefetchBatches,
        metadataMaxAge,
        autoCommit,
        autoCommitInterval,
        autoCommitThreshold,
        operations: cluster.offsetOperations({ groupId }),
        obligations,
      })

      runner = new Runner({
        logger: rootLogger,
        consumerGroup,
        instrumentationEmitter,
        heartbeatInterval: groupTimeouts.heartbeatInterval,
        maxPollInterval: groupTimeouts.pollDeadline,
        sessionTimeout: groupTimeouts.sessionTimeout,
        retry,
        autoCommit,
        eachBatchAutoResolve,
        eachBatch,
        eachMessage,
        onCrash,
        onJoinComplete: recordJoin,
        concurrency,
        maxPrefetchBatches,
      })

      const started = runner.start()
      pendingStart = started.catch(() => {})
      await started
    }

    const onCrash = async (e) => {
      logger.error(`Crash: ${e.name}: ${e.message}`, {
        groupId,
        retryCount: e.retryCount,
        stack: e.stack,
      })

      if (e.name === 'KafkaJSConnectionClosedError') {
        cluster.removeBroker({ host: e.host, port: e.port, source: failureSourceOf(e) })
      }

      // Cleaning up must never suppress the CRASH event: it is the only supervision signal a
      // user gets, because consumer.run() has already resolved by the time the runner dies.
      // shutDown() already logs this failure itself (and rethrows), so there is nothing
      // further to log here; just make sure it cannot stop the CRASH event from firing.
      const crashStopGeneration = stopGeneration + 1
      try {
        await shutDown(undefined, { keepResets: true })
      } catch {}

      const getOriginalCause = (error) => {
        if (error.cause) {
          return getOriginalCause(error.cause)
        }

        return error
      }

      const isErrorRetriable =
        e.name === 'KafkaJSNumberOfRetriesExceeded' || getOriginalCause(e).retriable === true

      let shouldRestart = isErrorRetriable
      if (retry && retry.restartOnFailure) {
        // The user callback is authoritative for every terminal death, retriable or not.
        try {
          shouldRestart = await retry.restartOnFailure(e)
        } catch (callbackError) {
          logger.error(
            'Caught error when invoking user-provided "restartOnFailure" callback. Defaulting to restarting.',
            {
              error: callbackError.message || callbackError,
              cause: e.message || e,
              groupId,
            }
          )
          shouldRestart = true
        }
      }

      if (isFencedInstance(e) || (isConsumerProtocol && isConsumerProtocolFatal(e))) {
        // A fenced static member (82) has been evicted by a duplicate groupInstanceId elsewhere,
        // and a fatal membership error - unsupported assignor, invalid subscription regex,
        // unreleased instance id, group at capacity, a revoked group ACL, a malformed request, a
        // cluster without the consumer protocol - will answer the same way to every rejoin. Both
        // override restartOnFailure, unlike every other terminal error. The predicate is the
        // error contract's own consumer-protocol set, so the CRASH event's `restart` flag agrees
        // with the verdict that produced it.
        shouldRestart = false
      }

      if (shouldRestart && stopGeneration !== crashStopGeneration) {
        // The user called disconnect()/stop() during the crash cleanup or while restartOnFailure
        // was deciding: do not resurrect a consumer they believe is gone.
        logger.warn(
          'Consumer was stopped while deciding whether to restart after a crash; not restarting',
          { groupId }
        )
        shouldRestart = false
      }

      instrumentationEmitter.emit(CRASH, {
        error: e,
        groupId,
        restart: shouldRestart,
      })

      if (shouldRestart) {
        const retryTime = e.retryTime || (retry && retry.initialRetryTime) || initialRetryTime
        logger.error(`Restarting the consumer in ${retryTime}ms`, {
          retryCount: e.retryCount,
          retryTime,
          groupId,
        })

        restartTimeout = setTimeout(() => start(onCrash), retryTime)
      }
    }

    await start(onCrash)
  }

  /** @type {import("../../types").Consumer["on"]} */
  const on = (eventName, listener) => {
    if (!eventNames.includes(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${eventKeys}`)
    }

    return instrumentationEmitter.addListener(unwrapEvent(eventName), (event) => {
      event.type = wrapEvent(event.type)
      Promise.resolve(listener(event)).catch((e) => {
        logger.error(`Failed to execute listener: ${e.message}`, {
          eventName,
          stack: e.stack,
        })
      })
    })
  }

  /**
   * @type {import("../../types").Consumer["commitOffsets"]}
   * @param topicPartitions
   *   Example: [{ topic: 'topic-name', partition: 0, offset: '1', metadata: 'event-id-3' }]
   */
  const commitOffsets = async (topicPartitions = []) => {
    const commitsByTopic = topicPartitions.reduce(
      (payload, { topic, partition, offset, metadata = null }) => {
        if (!topic) {
          throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
        }

        if (isNaN(partition)) {
          throw new KafkaJSNonRetriableError(
            `Invalid partition, expected a number received ${partition}`
          )
        }

        let commitOffset
        try {
          commitOffset = Long.fromValue(offset)
        } catch (_) {
          throw new KafkaJSNonRetriableError(`Invalid offset, expected a long received ${offset}`)
        }

        if (commitOffset.lessThan(0)) {
          throw new KafkaJSNonRetriableError('Offset must not be a negative number')
        }

        if (metadata !== null && typeof metadata !== 'string') {
          throw new KafkaJSNonRetriableError(
            `Invalid offset metadata, expected string or null, received ${metadata}`
          )
        }

        const topicCommits = payload[topic] || []

        topicCommits.push({ partition, offset: commitOffset, metadata })

        return Object.assign(dictionary(), payload, { [topic]: topicCommits })
      },
      dictionary()
    )

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    const topics = Object.keys(commitsByTopic)

    return runner.commitOffsets({
      topics: topics.map((topic) => {
        return {
          topic,
          partitions: commitsByTopic[topic],
        }
      }),
    })
  }

  /** @type {import("../../types").Consumer["seek"]} */
  const seek = ({ topic, partition, offset }) => {
    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`)
    }

    if (isNaN(partition)) {
      throw new KafkaJSNonRetriableError(
        `Invalid partition, expected a number received ${partition}`
      )
    }

    let seekOffset
    try {
      seekOffset = Long.fromValue(offset)
    } catch (_) {
      throw new KafkaJSNonRetriableError(`Invalid offset, expected a long received ${offset}`)
    }

    if (seekOffset.lessThan(0) && !specialOffsets.includes(seekOffset.toString())) {
      throw new KafkaJSNonRetriableError('Offset must not be a negative number')
    }

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.seek({ topic, partition, offset: seekOffset.toString() })
  }

  /**
   * Describes the group this consumer belongs to, whatever protocol runs it.
   *
   * Routed through the same describer `admin.describeGroups` uses, so a KIP-848 group is described
   * through ConsumerGroupDescribe and a classic one through DescribeGroups. Asking DescribeGroups
   * directly - which is what this did - answers `GROUP_ID_NOT_FOUND` for a consumer-protocol group
   * and reports it as `Dead` with no members.
   *
   * @type {import("../../types").Consumer["describeGroup"]}
   */
  const describeGroup = async () => {
    const { groups } = await createGroupDescriptions({
      cluster,
      retry,
      logger: rootLogger,
    }).describe([groupId])

    return groups.find((group) => group.groupId === groupId)
  }

  /**
   * @type {import("../../types").Consumer["pause"]}
   * @param topicPartitions
   *   Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  const pause = (topicPartitions = []) => {
    for (const topicPartition of topicPartitions) {
      if (!topicPartition || !topicPartition.topic) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${(topicPartition && topicPartition.topic) || topicPartition}`
        )
      } else if (
        typeof topicPartition.partitions !== 'undefined' &&
        (!Array.isArray(topicPartition.partitions) || topicPartition.partitions.some(isNaN))
      ) {
        throw new KafkaJSNonRetriableError(
          `Array of valid partitions required to pause specific partitions instead of ${topicPartition.partitions}`
        )
      }
    }

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.pause(topicPartitions)
  }

  /**
   * Returns the list of topic partitions paused on this consumer
   *
   * @type {import("../../types").Consumer["paused"]}
   */
  const paused = () => {
    if (!consumerGroup) {
      return []
    }

    return consumerGroup.paused()
  }

  /**
   * @type {import("../../types").Consumer["resume"]}
   * @param topicPartitions
   *  Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  const resume = (topicPartitions = []) => {
    for (const topicPartition of topicPartitions) {
      if (!topicPartition || !topicPartition.topic) {
        throw new KafkaJSNonRetriableError(
          `Invalid topic ${(topicPartition && topicPartition.topic) || topicPartition}`
        )
      } else if (
        typeof topicPartition.partitions !== 'undefined' &&
        (!Array.isArray(topicPartition.partitions) || topicPartition.partitions.some(isNaN))
      ) {
        throw new KafkaJSNonRetriableError(
          `Array of valid partitions required to resume specific partitions instead of ${topicPartition.partitions}`
        )
      }
    }

    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    consumerGroup.resume(topicPartitions)
  }

  /**
   * This consumer's group membership, for a transactional offset commit the group coordinator can
   * fence (KIP-447): pass it to `transaction.sendOffsets({ consumerGroupMetadata, topics })`. It is
   * a snapshot taken at the call, so read it again after every rebalance.
   *
   * Before the consumer's first completed join it reports generation -1 and an empty member id,
   * as the Java client does. Under the classic protocol it then reports the generation and member
   * id of the last completed join, which a lost generation does not erase. Under
   * `groupProtocol: 'consumer'` `generationId` is the live member epoch and `memberId` the live
   * member id, the same pair OffsetCommit v9 carries. The last joined pair is kept across
   * `stop()`/`run()` and a crash restart until the next join; under `groupProtocol: 'consumer'`
   * the new membership is read live. A `GROUP_JOIN` observer, on its channel or through `on()`,
   * reads the pair of the join that event reports.
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
   * @returns {{ groupId: string, generationId: number, memberId: string, groupInstanceId: string|null }}
   * @throws {KafkaJSNonRetriableError} before `run()` and after `stop()`
   */
  const groupMetadata = () => {
    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    const { groupInstanceId } = consumerGroup
    if (lastJoin === null) {
      return { groupId, generationId: -1, memberId: '', groupInstanceId }
    }

    if (consumerGroup.kind === 'consumer') {
      return {
        groupId,
        generationId: consumerGroup.memberEpoch,
        memberId: consumerGroup.memberId,
        groupInstanceId,
      }
    }

    return { groupId, ...lastJoin, groupInstanceId }
  }

  /**
   * @return {Object} logger
   */
  const getLogger = () => logger

  const consumer = {
    connect,
    disconnect,
    subscribe,
    stop,
    run,
    commitOffsets,
    seek,
    describeGroup,
    pause,
    paused,
    resume,
    groupMetadata,
    on,
    events,
    logger: getLogger,
  }

  registerDisposer(consumer, dispose)
  return consumer
}
