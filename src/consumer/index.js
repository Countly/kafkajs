const { performance } = require('node:perf_hooks')
const { failureSourceOf } = require('../broker/failureSource')
const Long = require('../utils/long')
const dictionary = require('../utils/dictionary')
const { initialRetryTime } = require('../retry/defaults')
const ConsumerGroup = require('./consumerGroup')
const ManualGroupProtocol = require('./groupProtocol/manual')
const CommittedOffsetReader = require('./committedOffsets')
const handlerContext = require('./handlerContext')
const OffsetOperations = require('./offsetManager/offsetOperations')
const Subscription = require('./subscription')
const ControlLifetime = require('./controlLifetime')
const { normalizeResetPolicy } = require('./offsetReset')
const { createGroupDescriptions } = require('../admin/groupDescriptions')
const resolveGroupTimeouts = require('./groupTimeouts')
const {
  selectRebalanceProtocol,
  ConsumerOperation,
  markCallDeadline,
} = require('./groupProtocol/interface')
const Runner = require('./runner')
const RebalanceHooks = require('./rebalanceHooks')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const {
  KafkaJSNonRetriableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSDeliveryTimeout,
  isFencedInstance,
} = require('../errors')
const {
  isConsumerProtocolFatal,
  isJoinDeadline,
  consumerStoppingError,
  stampOwnership,
} = require('./rebalanceErrorContract')
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

const GROUP_REQUIRED = 'Consumer groupId must be a non-empty string.'

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
 * Reads a `disconnect()` argument: an options object with leaveGroup and timeout, or nothing.
 *
 * Anything that is not an object is ignored, exactly as kafkajs 2.2.4's `disconnect()` ignored
 * every argument: `process.on('SIGTERM', consumer.disconnect)` passes the signal name, and
 * rejecting it would surface as an unhandled rejection inside a signal handler.
 * @param {*} [options]
 * @returns {{ leaveGroup?: boolean, timeout?: number }|undefined} what to hand to the stop: `undefined` when
 *   `options` is absent (`null` included) or is not an object, so everything below sees an object
 *   or nothing
 * @throws {KafkaJSNonRetriableError} when `options` is an object whose `leaveGroup` is neither
 *   `undefined` nor a boolean
 * @throws {KafkaJSInvalidConfigurationError} when timeout is not an integer from 0 to 2147483647
 */
const parseDisconnectOptions = (options) => {
  if (options == null || typeof options !== 'object') {
    return undefined
  }

  const { leaveGroup, timeout } = options
  if (leaveGroup !== undefined && typeof leaveGroup !== 'boolean') {
    throw new KafkaJSNonRetriableError('Argument "leaveGroup" must be a boolean')
  }

  if (
    timeout !== undefined &&
    (!Number.isInteger(timeout) || timeout < 0 || timeout > 2147483647)
  ) {
    throw new KafkaJSInvalidConfigurationError(
      'Argument "timeout" must be an integer from 0 to 2147483647'
    )
  }

  return { leaveGroup, timeout }
}

/**
 * @param {Object} params
 * @param {import("../../types").Cluster} params.cluster
 * @param {String} [params.groupId] required to subscribe and for every group offset API; a
 *                                consumer that only assigns partitions may omit it, and then keeps
 *                                its offsets private, never contacts a coordinator and defaults
 *                                `autoCommit` to false
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
 * @param {boolean} [params.rackAwareAssignment=false] opt-in range rack matching; other assigners ignore it
 * @param {string} [params.rackId]
 * @param {boolean} [params.fetchSessions=false] Enable incremental Fetch sessions (KIP-227). The
 *   default is false, which preserves full requests.
 * @param {number} [params.requestTimeout=30000] The client's request timeout, which caps a session
 *   close that has no disconnect deadline.
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
  if (params.fetchSessions !== undefined && typeof params.fetchSessions !== 'boolean') {
    throw new KafkaJSNonRetriableError('fetchSessions must be a boolean')
  }
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
    rackAwareAssignment = false,
    checkCrcs = true,
    maxDecompressedBatchBytes = DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES,
    groupInstanceId = null,
    instrumentationEmitter: rootInstrumentationEmitter,
    metadataMaxAge,
    groupProtocol = 'classic',
    groupRemoteAssignor = null,
    maxPrefetchBatches = 0,
    fetchSessions = false,
    requestTimeout = 30000,
  } = params

  if (groupId !== undefined && (typeof groupId !== 'string' || groupId.length === 0)) {
    throw new KafkaJSNonRetriableError(GROUP_REQUIRED)
  }

  const logger = rootLogger.namespace('Consumer')
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()
  const isConsumerProtocol = groupProtocol === 'consumer'

  // The coordinator computes the assignment under the consumer protocol, so there is nothing to
  // build and no protocol to select from an empty list.
  const assigners = isConsumerProtocol
    ? []
    : partitionAssigners.map((createAssigner) =>
        createAssigner({ groupId, logger, cluster, rackAware: rackAwareAssignment })
      )

  /** @type {Record<string, { fromBeginning?: boolean, resetPolicy?: object }>} */
  const topics = dictionary()
  /**
   * Names subscribed under the consumer protocol while their topic did not exist, so the cluster
   * could not register them as targets. The group registers each once the coordinator assigns it.
   * @type {Set<string>}
   */
  const unregisteredTopics = new Set()
  /**
   * Every subscription declaration for the life of this consumer: names, cloned patterns and the
   * policy each carried. A client-side pattern makes discovery re-evaluate it at metadata
   * refreshes; literal-only consumers keep their names in `topics`.
   */
  const declared = new Subscription()
  /** @type {string|null} the RE2 pattern a regex subscription sent, verbatim */
  let subscribedTopicRegex = null
  /** @type {{ fromBeginning: boolean, resetPolicy: object }|null} the options that subscription was made with */
  let regexSubscription = null
  let runner = null
  /** @type {ConsumerGroup} */
  let consumerGroup = null
  // Bumped on every stop() and disconnect() invocation (including the disconnect onCrash triggers
  // internally as part of its own cleanup), so onCrash can tell whether a *separate* stop happened
  // while it was still deciding whether to restart (e.g. a slow restartOnFailure callback).
  let stopGeneration = 0
  /**
   * The close in progress. Concurrent disconnects join its whole cleanup, so the first call's
   * timeout and leave choice govern; it is cleared when the cleanup settles.
   * `joins` counts the later `disconnect()` calls that joined it.
   * @type {{ operation: ConsumerOperation, promise: Promise<void>, joins: number }|null}
   */
  let closing = null
  /**
   * What the current explicit connection owns. An explicit disconnect marks it ending; the next
   * connect, run or start then begins a new one, so a close settling late never touches it.
   * @type {ControlLifetime}
   */
  let life = new ControlLifetime()

  const hasGroup = groupId !== undefined
  /**
   * The offsets and ledger of a consumer without a group, kept for the consumer's lifetime across
   * restarts and never registered on the cluster, so two such consumers cannot see each other.
   * @type {{ offsets: Record<string, object>, operations: OffsetOperations }|null}
   */
  const privateState = hasGroup
    ? null
    : { offsets: dictionary(), operations: new OffsetOperations() }
  /** @returns {OffsetOperations} the private ledger, or the group's shared one */
  const offsetOperations = () =>
    privateState ? privateState.operations : cluster.offsetOperations({ groupId })

  /**
   * The manual selection, or null: its normalized partitions and per-topic reset configuration.
   * Kept apart from `topics`, so `subscription()` stays empty for a manual consumer.
   * @type {{ partitions: {topic: string, partitions: number[]}[], configurations: Record<string, object> }|null}
   */
  let manual = null
  /** @type {Map<string, {topic: string, partition: number, offset: string}>} seeks queued before the first start */
  const queuedSeeks = new Map()
  /** The token of the current run, or null; each start, restart included, replaces it. */
  let runSlot = null
  /** Advances on every accepted selection change, so an older staged candidate cannot publish. */
  let selectionRevision = 0

  /**
   * @throws {KafkaJSNonRetriableError} when this consumer was built without a group
   * @returns {void}
   */
  const requireGroup = () => {
    if (!hasGroup) throw new KafkaJSNonRetriableError(GROUP_REQUIRED)
  }

  /**
   * Refuses a call made from a handler of a run that has since been retired, or from a rebalance
   * callback invocation that is settled, expired or retired, before it can read or change a
   * replacement run's state. Detached callers, the current run's handlers and a current callback pass.
   * @throws {Error} `CONSUMER_STOPPING` or the callback's own refusal
   * @returns {void}
   */
  const refuseRetiredCaller = () => {
    const refusal = RebalanceHooks.refusal()
    if (refusal) throw refusal
    const identity = handlerContext.current()?.runIdentity
    if (identity && !identity.isCurrent()) throw consumerStoppingError()
  }

  /**
   * Opens the one deadline scope of a public call, captured at its entry before any lookup: it ends
   * at `retry.maxTotalTime` when that is set, at the calling rebalance callback's own deadline when
   * the call is made from one, and when a forced close retires the call or the calling handler's
   * run is retired. With none of these it never expires on its own, so an unset deadline keeps the
   * call's natural wait. The caller must `finish` it in a `finally`.
   * @param {Set<ConsumerOperation>} calls the in-flight set of the lifetime the call belongs to, so
   *   only that lifetime's forced close retires it
   * @returns {ConsumerOperation} the call's scope
   */
  const beginCall = (calls) => {
    const invocation = RebalanceHooks.current()
    const identity = handlerContext.current()?.runIdentity
    const maxTotalTime = retry?.maxTotalTime
    const operation = new ConsumerOperation({
      deadlineAt: Math.min(
        maxTotalTime == null ? Infinity : performance.now() + maxTotalTime,
        invocation?.context.deadlineAt ?? Infinity
      ),
      guard: () =>
        RebalanceHooks.refusal(invocation) ||
        (identity && !identity.isCurrent() ? consumerStoppingError() : null),
      onTimeout: (cause) =>
        markCallDeadline(
          new KafkaJSDeliveryTimeout('The consumer call exceeded retry.maxTotalTime', { cause })
        ),
    })
    calls.add(operation)
    return operation
  }

  /**
   * Runs a public call's work under its own deadline scope and releases the scope however it ends.
   * @template T
   * @param {(operation: ConsumerOperation) => T|Promise<T>} work the call's body
   * @param {ControlLifetime} [owned] the lifetime the call belongs to, the current one by default
   * @returns {Promise<T>} the body's result, or the scope's refusal if it expired first
   * @throws {Error} the body's failure or the scope's refusal
   */
  const withinCall = async (work, owned = life) => {
    const { calls } = owned
    const operation = beginCall(calls)
    try {
      return await operation.wait(() => work(operation))
    } finally {
      calls.delete(operation)
      operation.finish()
    }
  }

  /**
   * Subscribed mode exists from the declaration on: a client pattern counts whether or not it
   * currently matches a topic, because matches are observations that come and go.
   * @returns {boolean} whether any topic, broker regex or client pattern is subscribed
   */
  const isSubscribed = () =>
    keys(topics).length > 0 || subscribedTopicRegex != null || declared.hasPatterns()

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
   * the new pair from `groupMetadata()`. A join of a run that has since been replaced records nothing.
   * @param {ConsumerGroup} group the group of the run that joined
   * @returns {void}
   */
  const recordJoin = (group) => {
    if (consumerGroup && consumerGroup === group) {
      lastJoin = { generationId: group.generationId, memberId: group.memberId }
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
  let pendingStart = Promise.resolve()

  /**
   * Returns the lifetime that serves a connect, run or start, beginning a new one when an explicit
   * disconnect has started on the current one, and subscribes it to the cluster's topic-recreation
   * event, at most once per lifetime. Called at construction and again from connect(), run() and
   * start(), so a consumer reused after disconnect() - including the restart onCrash schedules,
   * which reaches start() alone - re-arms rather than running with the reset silently inert.
   *
   * The handler body is wrapped in try/catch: it runs synchronously inside the cluster's
   * `emit()`, called from `BrokerPool#refreshMetadata` - an uncaught throw here would escape as
   * a metadata-refresh failure and could itself trigger a rebootstrap, turning a topic
   * recreation into a connection storm.
   * @returns {ControlLifetime} the lifetime now serving the caller
   */
  const acquireLife = () => {
    if (life.ending) life = new ControlLifetime(life.released ? life.obligations : undefined)
    const owned = life
    if (owned.removeListener) return owned

    owned.removeListener = cluster.onTopicRecreated(({ payload }) => {
      try {
        const { topic, topicId } = payload

        if (
          !Object.hasOwn(topics, topic) &&
          !declared.desired.topicConfigurations.has(topic) &&
          !(manual && Object.hasOwn(manual.configurations, topic))
        ) {
          return
        }

        owned.obligations.owe(topic, topicId)
        offsetOperations().touchTopic(topic)
        consumerGroup && consumerGroup.invalidateTopic(topic)
      } catch (e) {
        logger.error(`Failed to handle topic recreation: ${e.message}`, {
          groupId,
          stack: e.stack,
          payload,
        })
      }
    })
    return owned
  }

  acquireLife()

  /**
   * Makes this consumer's subscription the default metadata scope of its cluster while it holds a
   * client-side pattern, so every refresh on the cluster asks for all topics and creates none.
   * Idempotent; a literal-only consumer, which may borrow its creator's cluster, registers nothing.
   * @returns {void}
   */
  const registerDiscoveryScope = () => {
    if (declared.hasPatterns()) cluster.useConsumerMetadataScope(declared)
  }

  /**
   * Gives the cluster's default scope back, only if this consumer registered it.
   * @returns {void}
   */
  const releaseDiscoveryScope = () => {
    if (declared.hasPatterns()) cluster.releaseConsumerMetadataScope(declared)
  }

  /**
   * Ends a lifetime when its explicit disconnect or disposal settles: its connect fence retires, and
   * its listener, debt and restart timer are released. The discovery scope goes back only while no
   * replacement lifetime has begun, because a replacement registered the same declarations.
   * @param {ControlLifetime} owned the lifetime the close began on
   * @returns {void}
   */
  const endLife = (owned) => {
    owned.ending = true
    owned.fence.retire(consumerStoppingError())
    owned.release()
    if (life === owned) releaseDiscoveryScope()
  }

  /**
   * The wait a connect, run or start owes the close that was already running when it began, so
   * that close can never tear down the connection that replaces it: its cluster disconnect and
   * group-registry cleanup finish first. A forced close bounds the wait by its own deadline; a
   * plain one waits naturally, which is the order `disconnect()` then `connect()` always had. A
   * close's own failure is its caller's to see, not the replacement's. A caller that is part of
   * the run being closed (a handler or a rebalance callback) does not wait, because that close is
   * waiting for it. Returns null with no close to wait for, so the caller stays synchronous up to
   * the lifetime it acquires and a `disconnect()` made right after still retires it.
   * @returns {Promise<void>|null} settles when that close has; rejects `CONSUMER_STOPPING` when a
   *   `disconnect()` joined the close meanwhile, because it was asked after this call
   */
  const earlierClose = () => {
    const earlier = closing
    if (!earlier || RebalanceHooks.current() || handlerContext.current()) return null
    const { joins } = earlier
    return earlier.promise
      .catch(() => {})
      .then(() => {
        if (earlier.joins !== joins) throw consumerStoppingError()
      })
  }

  /**
   * @type {import("../../types").Consumer["connect"]}
   * @throws {Error} `CONSUMER_STOPPING` from a handler of a retired run, or when a close ended this
   *   connect's lifetime before it completed; no `CONNECT` is published then
   */
  const connect = async () => {
    refuseRetiredCaller()
    const waiting = earlierClose()
    if (waiting) await waiting
    const owned = acquireLife()
    registerDiscoveryScope()
    await cluster.connect({ fence: owned.fence })
    owned.fence.check()
    instrumentationEmitter.emit(CONNECT)
  }

  /**
   * Stops the runner and disconnects the cluster, for `disconnect()` and for the crash handler.
   * A user disconnect ends the consumer, so its reset obligations are emptied. A crash restart
   * unlinks them and keeps them: the topics still owe those resets to the group it builds next.
   * @param {{ leaveGroup?: boolean }} [stopOptions]
   * @param {{ keepResets?: boolean }} [shutDownOptions] `true` for the crash handler
   * @param {ConsumerOperation} [operation] the close that bounds this shutdown; absent, it waits
   *   naturally
   * @returns {Promise<void>}
   */
  const shutDown = async (stopOptions, { keepResets = false } = {}, operation) => {
    stopGeneration++
    const owned = life

    try {
      if (operation) {
        await closeWithin(operation, stopOptions, owned)
      } else {
        await stopShared(stopOptions)
        logger.debug('consumer has stopped, disconnecting', { groupId })
        await cluster.disconnect()
      }
      instrumentationEmitter.emit(DISCONNECT)
    } catch (e) {
      logger.error(`Caught error when disconnecting the consumer: ${e.message}`, {
        stack: e.stack,
        groupId,
      })
      throw e
    } finally {
      if (keepResets) owned.obligations.unlink()
      else endLife(owned)
    }
  }

  /**
   * The stop and the cluster teardown of one close, under that close's operation. At its deadline,
   * or at once for zero, the close is forced in one synchronous step: the captured run is retired
   * (timers, rounds, hooks and waits end, and later commits and heartbeats of its handlers are
   * refused), the public calls in flight are retired, and the connections close without draining.
   * The close then resolves once the connections have closed, whatever failed on the way; what a
   * forced close settles claims no remote outcome, because a transmitted commit can still apply.
   * @param {ConsumerOperation} operation the close scope
   * @param {{ leaveGroup?: boolean }} [stopOptions]
   * @param {ControlLifetime} owned the lifetime this close ends; its calls and connect are retired
   * @returns {Promise<void>} rejects only for an ordinary failure of a close that was not forced
   */
  const closeWithin = async (operation, stopOptions, owned) => {
    const captured = runner
    let disconnecting = null
    /** Starts the close's one cluster disconnect; later calls share it. */
    const disconnectCluster = (drainTimeout) =>
      (disconnecting ??= cluster.disconnect(
        ...(drainTimeout === undefined ? [] : [{ drainTimeout }])
      ))
    let forced = false
    let forcedDone
    const forcedSettled = new Promise((resolve) => {
      forcedDone = resolve
    })
    const unregister = operation.register(() => {
      forced = true
      captured?.retire(consumerStoppingError())
      owned.retire(consumerStoppingError())
      disconnectCluster(0).then(forcedDone, (error) => {
        logger.warn('Disconnecting the cluster failed', { error: error.message })
        forcedDone()
      })
    })

    try {
      await Promise.race([stopShared(stopOptions, operation), forcedSettled])
      logger.debug('consumer has stopped, disconnecting', { groupId })
      const { deadlineAt } = operation
      await disconnectCluster(deadlineAt === Infinity ? undefined : operation.remaining()).catch(
        (error) => {
          if (!forced) throw error
        }
      )
      if (forced) await forcedSettled
    } finally {
      unregister()
    }
  }

  /**
   * Starts the one close of this consumer, or joins the one in progress. The first call captures
   * the deadline from its `timeout` (none when omitted) and its leave choice; a later call, with
   * or without options, neither renews nor shortens them.
   * @param {{ leaveGroup?: boolean, timeout?: number }|undefined} options the validated options
   * @returns {Promise<void>} the shared close
   */
  const closeShared = (options) => {
    if (closing) {
      stopGeneration++
      closing.joins++
      return closing.promise
    }

    const timeout = options?.timeout
    const operation = new ConsumerOperation({
      deadlineAt: timeout === undefined ? Infinity : performance.now() + timeout,
    })
    const record = { operation, promise: null, joins: 0 }
    life.ending = true
    record.promise = shutDown(options, {}, operation).finally(() => {
      if (closing === record) closing = null
      operation.finish()
    })
    closing = record
    return record.promise
  }

  /**
   * Accepts a lifecycle request made from inside a rebalance callback. The cleanup keeps running
   * and its failure is logged. The caller returns without awaiting it, so an async API settles on
   * the next microtask instead of awaiting the round that is itself waiting for that callback.
   * @param {Promise<void>} cleanup the shared cleanup the request started
   * @returns {void}
   */
  const acceptFromHook = (cleanup) => {
    cleanup.catch((e) => {
      logger.error(`Caught error while stopping from a rebalance callback: ${e.message}`, {
        stack: e.stack,
        groupId,
      })
    })
  }

  /**
   * Stops the consumer and disconnects the cluster. `leaveGroup` (KIP-1092) overrides whether the
   * member leaves its group: `true` removes it at once, whatever its kind, `false` keeps it in the
   * group until its session expires, and `undefined` is the default (a dynamic member leaves, a
   * static one stays). A call that joins a stop already in flight keeps the first caller's choice.
   * Crashes and restarts always use the default, and so does a call whose argument is not an
   * options object.
   *
   * The first concurrent call governs the whole close, `timeout` included: with `timeout` (ms,
   * integer 0 to 2147483647) the close is bounded, and at the deadline the captured run is retired
   * and the call resolves after the connections close; zero starts no final commit or leave, and
   * omitted keeps the natural wait. A transmitted commit can still apply after the deadline.
   *
   * @type {import("../../types").Consumer["disconnect"]}
   * @param {{ leaveGroup?: boolean, timeout?: number }} [options]
   * @throws {KafkaJSNonRetriableError} before anything is stopped, when `options` is an object
   *   whose `leaveGroup` is neither `undefined` nor a boolean
   * @throws {KafkaJSInvalidConfigurationError} before anything is stopped, when `timeout` is not an
   *   integer from 0 to 2147483647
   * @throws {Error} `CONSUMER_STOPPING` from a handler of a retired run, before anything is stopped
   */
  const disconnect = async (options) => {
    refuseRetiredCaller()
    const cleanup = closeShared(parseDisconnectOptions(options))
    if (RebalanceHooks.current()) return acceptFromHook(cleanup)
    return cleanup
  }

  /**
   * The one stop in flight, shared by every caller. It captures the runner it stops: the factory's
   * references are cleared only if they are still that run's, so a replacement started meanwhile
   * is never nulled by an old stop settling late.
   * @param {{ leaveGroup?: boolean }} [options] the first caller's leave choice
   * @param {ConsumerOperation} [operation] the close that bounds the stop, absent for a natural wait
   * @returns {Promise<void>}
   */
  const stopShared = sharedPromiseTo(async (options, operation) => {
    runSlot = null
    life.cancelRestart()
    const stopping = runner
    try {
      if (stopping) {
        await stopping.stop(options, operation)
        if (runner === stopping) {
          runner = null
          consumerGroup = null
          declared.retireRun()
        }
        instrumentationEmitter.emit(STOP)
      }

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
   * no options: only `disconnect()` can override how the member leaves its group. A call from a
   * handler of a retired run rejects with `CONSUMER_STOPPING` and changes nothing, so it cannot end
   * the replacement run.
   * @type {import("../../types").Consumer["stop"]}
   */
  const stop = () => {
    try {
      refuseRetiredCaller()
    } catch (error) {
      return Promise.reject(error)
    }
    stopGeneration++
    const cleanup = stopShared()
    if (!RebalanceHooks.current()) return cleanup
    acceptFromHook(cleanup)
    return Promise.resolve()
  }

  /**
   * Ends a consumer that borrows its creator's cluster (see `disposeConsumer`): requests the stop
   * first, so a start that is still connecting joins nothing, then waits for that start to settle;
   * then gives up what it registered on the cluster, the topic-recreation listener and its reset
   * obligations. Unlike `disconnect()` it leaves the cluster connected, because the cluster is
   * shared with the operations around it. The consumer is not reused. Idempotent. The listener is
   * released even when the stop fails, and the failure is rethrown.
   * @returns {Promise<void>}
   */
  const dispose = async () => {
    stopGeneration++
    const owned = life
    owned.ending = true

    try {
      await Promise.all([stopShared(), pendingStart])
    } finally {
      endLife(owned)
    }
  }

  /**
   * Declares a call that holds a client-side pattern, or follows one, and evaluates the
   * declarations against one all-topic metadata refresh. The cluster's targets become empty: while
   * a pattern is declared every refresh asks for all topics and creates none. A failure puts the
   * declarations back as they were.
   * @param {Array<string|RegExp>} subscriptions the validated names and patterns of the call
   * @param {{kind: string, durationMs?: number}} policy the call's normalized reset policy
   * @param {ControlLifetime} owned the lifetime the call began under; once its close is forced the
   *   refresh stops, nothing reconnects and the targets are not replaced
   * @returns {Promise<void>}
   * @throws {Error} the refresh's own failure, or the close's refusal
   */
  const declarePatterns = async (subscriptions, policy, owned) => {
    const checkpoint = declared.checkpoint()
    declared.add({ topics: subscriptions, policy })
    cluster.useConsumerMetadataScope(declared)

    try {
      await cluster.refreshMetadata({
        fence: owned.fence,
        metadataScope: declared,
        onMetadata: (metadata) => declared.evaluate(metadata),
      })
      const replaced = await cluster.resetTargetTopics([], {
        isCurrent: () => !owned.fence.closed,
      })
      if (!replaced) owned.fence.check()
    } catch (error) {
      declared.restore(checkpoint)
      if (!declared.hasPatterns()) cluster.releaseConsumerMetadataScope(declared)
      throw error
    }

    logger.debug('Subscription based on RegExp', {
      groupId,
      patterns: subscriptions.filter((entry) => entry instanceof RegExp).map(String),
      matchedTopics: declared.desired.names,
    })
  }

  /**
   * Refuses an API that no rebalance callback may call, even a current one, because it would
   * replace the run that is waiting for that callback; a retired or expired caller is refused first.
   * @param {string} api the public method name, for the message
   * @returns {void}
   * @throws {KafkaJSNonRetriableError} when called from inside a rebalance callback
   * @throws {Error} `CONSUMER_STOPPING` or the callback refusal from a retired caller
   */
  const refuseFromHook = (api) => {
    refuseRetiredCaller()
    if (RebalanceHooks.current()) {
      throw new KafkaJSNonRetriableError(
        `consumer#${api} cannot be called from a rebalance callback`
      )
    }
  }

  /**
   * A subscription that reaches the cluster belongs to the control lifetime it starts under: once a
   * forced close retires that lifetime, the call stops, so it neither reconnects nor changes the
   * targets afterwards.
   * @type {import("../../types").Consumer["subscribe"]}
   * @throws {Error} `CONSUMER_STOPPING` from a handler of a retired run, or when a forced close
   *   ended the call's lifetime meanwhile
   */
  const subscribe = async (subscription = {}) => {
    refuseFromHook('subscribe')
    requireGroup()
    const { topic, topics: subscriptionTopics, regex } = subscription
    const resetPolicy = normalizeResetPolicy(subscription)
    const fromBeginning = resetPolicy.kind === 'earliest'

    if (consumerGroup) {
      throw new KafkaJSNonRetriableError('Cannot subscribe to topic while consumer is running')
    }

    if (manual) {
      throw new KafkaJSNonRetriableError(
        'Cannot subscribe while a manual assignment is active; call unsubscribe() first'
      )
    }

    const wantsRegex = isExplicit(subscription, 'regex')
    const wantsNames = topic != null || subscriptionTopics != null

    // The combination rule comes first, so a call that is wrong several ways reports the thing the
    // user has to change: you subscribe one way or the other, not both.
    if (
      (wantsRegex && (wantsNames || keys(topics).length > 0 || declared.hasPatterns())) ||
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
      regexSubscription = { fromBeginning, resetPolicy }
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

    const owned = acquireLife()
    if (!subscriptions.some((entry) => entry instanceof RegExp) && !declared.hasPatterns()) {
      declared.add({ topics: subscriptions, policy: resetPolicy })
      for (const t of subscriptions) {
        topics[t] = { fromBeginning, resetPolicy }
      }

      try {
        await cluster.addMultipleTargetTopics(subscriptions, { fence: owned.fence })
        for (const name of subscriptions) unregisteredTopics.delete(name)
      } catch (error) {
        if (!isConsumerProtocol || error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
        for (const name of subscriptions) unregisteredTopics.add(name)
      }
      return
    }

    await declarePatterns(subscriptions, resetPolicy, owned)
  }

  /**
   * @type {import("../../types").Consumer["run"]}
   * @throws {Error} `CONSUMER_STOPPING` from a handler of a retired run
   * @throws {KafkaJSNonRetriableError} from a rebalance callback, or for an invalid
   *   `rebalanceListener` (a manual consumer, a missing group, a non-object, a non-function hook)
   */
  const run = async (runOptions = {}) => {
    refuseFromHook('run')
    const {
      autoCommitInterval = null,
      autoCommitThreshold = null,
      eachBatchAutoResolve = true,
      partitionsConsumedConcurrently: concurrency = 1,
      eachBatch = null,
      eachMessage = null,
      rebalanceListener,
    } = runOptions
    // Presence, not truthiness: a consumer without a group commits nothing unless told otherwise,
    // and is refused when it asks to.
    const autoCommit = runOptions.autoCommit === undefined ? hasGroup : runOptions.autoCommit
    if (!hasGroup && autoCommit) {
      throw new KafkaJSNonRetriableError(
        'autoCommit requires a groupId; a consumer without a group keeps its offsets private'
      )
    }

    const listener = RebalanceHooks.validate(
      rebalanceListener,
      manual ? 'manual' : groupProtocol,
      hasGroup
    )
    const waiting = earlierClose()
    if (waiting) await waiting
    acquireLife()

    if (consumerGroup) {
      logger.warn('consumer#run was called, but the consumer is already running', { groupId })
      return
    }

    /** The classic member id a crash by a round's join deadline hands to the restart. */
    let retainedMemberId = null

    const start = async (onCrash) => {
      const owned = acquireLife()
      logger.info('Starting', { groupId })

      registerDiscoveryScope()
      const discovering = declared.hasPatterns()

      const token = {}
      runSlot = token
      selectionRevision++
      const runIdentity = { isCurrent: () => runSlot === token }
      let group = null
      // Taken before the first await, so a crash restart cannot replay a queued seek.
      const seeks = [...queuedSeeks.values()]
      queuedSeeks.clear()

      group = consumerGroup = new ConsumerGroup({
        logger: rootLogger,
        topics: manual
          ? manual.partitions.map(({ topic }) => topic)
          : discovering
            ? declared.desired.names
            : keys(topics),
        topicConfigurations: manual
          ? manual.configurations
          : discovering
            ? Subscription.configurationsOf(declared.desired)
            : topics,
        subscription: discovering ? declared : null,
        manual: manual ? manual.partitions : null,
        runIdentity,
        privateOffsets: privateState?.offsets,
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
        fetchSessions,
        requestTimeout,
        groupInstanceId,
        retainedMemberId,
        rebalanceProtocol,
        groupProtocol,
        groupRemoteAssignor,
        subscribedTopicRegex,
        regexSubscription,
        unregisteredTopics,
        maxPrefetchBatches,
        metadataMaxAge,
        autoCommit,
        autoCommitInterval,
        autoCommitThreshold,
        operations: offsetOperations(),
        obligations: owned.obligations,
        rebalanceListener: listener,
      })
      retainedMemberId = null
      for (const seekEntry of seeks) consumerGroup.seek(seekEntry)

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
        onCrash: (error) => onCrash(error, owned),
        onJoinComplete: () => recordJoin(group),
        concurrency,
        maxPrefetchBatches,
      })

      const started = runner.start()
      pendingStart = started.catch(() => {})
      await started
    }

    const onCrash = async (e, owned) => {
      logger.error(`Crash: ${e.name}: ${e.message}`, {
        groupId,
        retryCount: e.retryCount,
        stack: e.stack,
      })

      if (owned.fence.closed) {
        logger.warn('A run of a closed connection crashed late; nothing more is done for it', {
          groupId,
        })
        return
      }

      if (e.name === 'KafkaJSConnectionClosedError') {
        cluster.removeBroker({ host: e.host, port: e.port, source: failureSourceOf(e) })
      }

      const getOriginalCause = (error) => {
        if (error.cause) {
          return getOriginalCause(error.cause)
        }

        return error
      }

      // The restart rejoins under the id the expired round held, read before the shutdown forgets it.
      if (isJoinDeadline(getOriginalCause(e)))
        retainedMemberId = consumerGroup?.retainableMemberId()

      // Cleaning up must never suppress the CRASH event: it is the only supervision signal a
      // user gets, because consumer.run() has already resolved by the time the runner dies.
      // shutDown() already logs this failure itself (and rethrows), so there is nothing
      // further to log here; just make sure it cannot stop the CRASH event from firing.
      const crashStopGeneration = stopGeneration + 1
      try {
        await shutDown(undefined, { keepResets: true })
      } catch {}

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

      if (shouldRestart && (stopGeneration !== crashStopGeneration || owned.fence.closed)) {
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
        groupId: groupId ?? null,
        restart: shouldRestart,
      })

      if (shouldRestart) {
        const retryTime = e.retryTime || (retry && retry.initialRetryTime) || initialRetryTime
        logger.error(`Restarting the consumer in ${retryTime}ms`, {
          retryCount: e.retryCount,
          retryTime,
          groupId,
        })

        // Created outside any handler or callback store: the restart must not inherit its authority.
        owned.restartTimer = handlerContext.run(null, () =>
          RebalanceHooks.outside(() =>
            setTimeout(() => {
              owned.restartTimer = null
              if (stopGeneration === crashStopGeneration && !owned.fence.closed) start(onCrash)
            }, retryTime)
          )
        )
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
    requireGroup()
    refuseRetiredCaller()
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
    const offsets = {
      topics: topics.map((topic) => {
        return {
          topic,
          partitions: commitsByTopic[topic],
        }
      }),
    }
    const committing = runner

    return withinCall((operation) => committing.commitOffsets(offsets, undefined, { operation }))
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

    refuseRetiredCaller()

    if (!consumerGroup) {
      if (!manual) {
        throw new KafkaJSNonRetriableError(
          'Consumer group was not initialized, consumer#run must be called first'
        )
      }

      if (
        !manual.partitions.some((row) => row.topic === topic && row.partitions.includes(partition))
      ) {
        throw new KafkaJSNonRetriableError(
          `Cannot seek ${topic}[${partition}]: it is not in the manual assignment`
        )
      }

      queuedSeeks.set(JSON.stringify([topic, partition]), {
        topic,
        partition,
        offset: seekOffset.toString(),
      })
      return
    }

    consumerGroup.seek({ topic, partition, offset: seekOffset.toString() })
  }

  /**
   * Requests one classic rejoin without waiting for it: an eager member commits and stops the world,
   * a cooperative one runs a background round, and a call while a round runs is a no-op. The
   * assigners' `protocol()` is rebuilt by the round itself, so changed user data is sent.
   * @param {string} [reason='enforced-rebalance'] reported as the rebalance reason
   * @returns {Promise<void>} resolves once the request is accepted, not when the round ends
   * @throws {KafkaJSNonRetriableError} for a non-string reason, the consumer protocol, a manual
   *   assignment, or a consumer that is not running
   * @throws {Error} `CONSUMER_STOPPING` or the callback's refusal from a retired caller
   */
  const enforceRebalance = async (reason = 'enforced-rebalance') => {
    refuseRetiredCaller()
    if (typeof reason !== 'string') {
      throw new KafkaJSNonRetriableError('enforceRebalance reason must be a string')
    }
    if (isConsumerProtocol) {
      throw new KafkaJSNonRetriableError(
        `enforceRebalance is not supported with groupProtocol 'consumer' (reason: ${reason})`
      )
    }
    if (manual) {
      throw new KafkaJSNonRetriableError(
        `enforceRebalance is unavailable with manual assignment (reason: ${reason})`
      )
    }
    if (!consumerGroup || !runner) {
      throw new KafkaJSNonRetriableError(
        `enforceRebalance requires a running group consumer (reason: ${reason})`
      )
    }
    const { rebalanceCoordinator } = runner
    if (rebalanceCoordinator.isRebalancing()) return
    const error = stampOwnership(
      new KafkaJSNonRetriableError(reason),
      consumerGroup.ownership.token()
    )
    rebalanceCoordinator.requestRejoin({ kind: 'revoked', reason, error })
  }

  /**
   * Describes the group this consumer belongs to, whatever protocol runs it.
   *
   * Routed through the same describer `admin.describeGroups` uses, so a KIP-848 group is described
   * through ConsumerGroupDescribe and a classic one through DescribeGroups. Asking DescribeGroups
   * directly - which is what this did - answers `GROUP_ID_NOT_FOUND` for a consumer-protocol group
   * and reports it as `Dead` with no members.
   *
   * With `retry.maxTotalTime` set, one deadline from the call's entry covers the lookup, fallback,
   * refresh and retries, and an expired call sends and installs nothing more.
   *
   * @type {import("../../types").Consumer["describeGroup"]}
   */
  const describeGroup = async () => {
    requireGroup()
    const { groups } = await withinCall((operation) =>
      createGroupDescriptions({
        cluster,
        retry,
        logger: rootLogger,
      }).describe([groupId], { operation })
    )

    return groups.find((group) => group.groupId === groupId)
  }

  /**
   * @type {import("../../types").Consumer["pause"]}
   * @throws {Error} `CONSUMER_STOPPING` from a handler of a retired run
   * @param topicPartitions
   *   Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  const pause = (topicPartitions = []) => {
    refuseRetiredCaller()
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
   * @throws {Error} `CONSUMER_STOPPING` from a handler of a retired run
   * @param topicPartitions
   *  Example: [{ topic: 'topic-name', partitions: [1, 2] }]
   */
  const resume = (topicPartitions = []) => {
    refuseRetiredCaller()
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
    requireGroup()
    if (!consumerGroup) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }

    if (consumerGroup.kind === 'manual') {
      return { groupId, generationId: -1, memberId: '', groupInstanceId: null }
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
   * Replaces the manual selection before `run()`. The whole input is validated and snapshotted
   * first. The topics' metadata is then read without installing anything, fenced by a metadata
   * ticket of the lifetime subscription, and every named partition is checked against it. A
   * candidate that is still current then publishes, under the cluster's target lock and with no
   * await between the steps: the targets are replaced (the old ones pruned), the old metadata scope
   * retired, and the manual selection, its reset policy dictionary and the revision change
   * together. Topics that left the selection lose their recreation debt, queued seeks and private
   * offsets; the selected ones keep them. A rejected or superseded candidate changes nothing, so
   * the previous selection stays usable. Exclusive with a subscription until `unsubscribe()`.
   *
   * The call belongs to the control lifetime it starts under, after any close already running has
   * settled, so it is one of that lifetime's calls in flight: a forced close ends it with the
   * close's refusal, and nothing it was still waiting for (a broker, a connection, the Metadata
   * write, the target lock) reconnects or publishes afterwards.
   * @type {import("../../types").Consumer["assign"]}
   * @throws {KafkaJSNonRetriableError} for malformed input, a running consumer, an active
   *   subscription, an unknown partition, or a candidate superseded while it was validated
   * @throws {Error} `CONSUMER_STOPPING` when a forced close ended its lifetime meanwhile
   */
  const assign = async (topicPartitions, options) => {
    refuseRetiredCaller()
    if (
      options !== undefined &&
      (options === null || typeof options !== 'object' || Array.isArray(options))
    ) {
      throw new KafkaJSNonRetriableError('assign options must be an object')
    }
    if (options?.fromBeginning !== undefined && typeof options.fromBeginning !== 'boolean') {
      throw new KafkaJSNonRetriableError('Argument "fromBeginning" must be a boolean')
    }
    ManualGroupProtocol.validateAssignment(topicPartitions)
    const partitions = ManualGroupProtocol.snapshot(topicPartitions)
    const resetPolicy = normalizeResetPolicy(options || {})
    const fromBeginning = resetPolicy.kind === 'earliest'
    const names = partitions.map(({ topic }) => topic)

    const refusal = () => {
      if (consumerGroup) {
        return new KafkaJSNonRetriableError(
          'Cannot assign while the consumer is running; call stop() first'
        )
      }
      if (isSubscribed()) {
        return new KafkaJSNonRetriableError(
          'Cannot assign while subscribed; call unsubscribe() first'
        )
      }
      return null
    }
    const first = refusal()
    if (first) throw first

    const waiting = earlierClose()
    if (waiting) await waiting
    const owned = acquireLife()
    return withinCall(async (operation) => {
      const revision = selectionRevision
      const ticket = declared.captureMetadata()
      const stale = () =>
        refusal() ||
        operation.guard() ||
        owned.fence.guard() ||
        (revision !== selectionRevision || !ticket.isCurrent()
          ? new KafkaJSNonRetriableError('The assignment was superseded while it was validated')
          : null)

      const metadata = await cluster.stageMetadata({ topics: names, guard: stale })
      for (const { topic, partitions: wanted } of partitions) {
        const known = new Set(
          metadata.topicMetadata
            .filter((entry) => entry.topic === topic)
            .flatMap((entry) => entry.partitionMetadata.map(({ partitionId }) => partitionId))
        )
        const unknown = wanted.filter((partition) => !known.has(partition))
        if (unknown.length > 0) {
          throw new KafkaJSNonRetriableError(
            `Topic ${topic} has no partition ${unknown.join(', ')}`
          )
        }
      }

      const published = await cluster.resetTargetTopics(names, {
        isCurrent: () => stale() === null,
        publish: () => {
          declared.retireMetadata()
          manual = {
            partitions,
            configurations: Subscription.configurationsOf({
              topicConfigurations: new Map(
                names.map((name) => [name, { fromBeginning, offsetReset: resetPolicy }])
              ),
            }),
          }
          for (const [key, { topic, partition }] of queuedSeeks) {
            const selected = partitions.some(
              (row) => row.topic === topic && row.partitions.includes(partition)
            )
            if (!selected) queuedSeeks.delete(key)
          }
          retainTopics(new Set(names))
          lastJoin = null
          selectionRevision++
        },
      })
      if (published === false) throw stale() || consumerStoppingError()
    }, owned)
  }

  /**
   * Drops the recreation debt and the private offsets of every topic outside `keep`, the topic-local
   * state a consumer holds while stopped. A name that returns owes anew, as after a subscription change.
   * @param {Set<string>} keep the names that keep their state
   * @returns {void}
   */
  const retainTopics = (keep) => {
    life.obligations.retain(keep)
    if (privateState) {
      for (const topic of keys(privateState.offsets)) {
        if (!keep.has(topic)) delete privateState.offsets[topic]
      }
    }
  }

  /**
   * Stops the run first, then clears the subscription or manual selection with its targets, queued
   * seeks, reset debts and metadata scope, keeping the connection. A failed stop propagates and
   * leaves the selection as it was. Idempotent. From a current rebalance callback it requests that
   * cleanup and settles without awaiting it, as `stop()` does. The clearing happens only if no
   * later selection change or run start replaced the control this call began under.
   * @type {import("../../types").Consumer["unsubscribe"]}
   * @throws {Error} what the stop failed with
   */
  const unsubscribe = async () => {
    refuseRetiredCaller()
    const revision = ++selectionRevision
    stopGeneration++
    const cleanup = stopShared().then(() =>
      cluster.resetTargetTopics([], {
        isCurrent: () => selectionRevision === revision,
        publish: () => {
          releaseDiscoveryScope()
          declared.clear()
          manual = null
          queuedSeeks.clear()
          for (const name of keys(topics)) delete topics[name]
          unregisteredTopics.clear()
          subscribedTopicRegex = null
          regexSubscription = null
          retainTopics(new Set())
          lastJoin = null
          selectionRevision++
        },
      })
    )
    if (RebalanceHooks.current()) return acceptFromHook(cleanup)
    return cleanup
  }

  /**
   * The acquired assignment, sorted, as a defensive copy. Empty before the first acquisition and
   * from the moment a stop begins.
   * @type {import("../../types").Consumer["assignment"]}
   */
  const assignment = () => {
    if (!runSlot || !consumerGroup) return []
    return consumerGroup
      .assigned()
      .map(({ topic, partitions }) => ({
        topic,
        partitions: [...partitions].sort((a, b) => a - b),
      }))
      .sort((a, b) => (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0))
  }

  /**
   * The subscribed topic names, sorted, as a defensive copy: the literals and the names a client
   * pattern currently matches. Empty for a manual consumer.
   * @type {import("../../types").Consumer["subscription"]}
   */
  const subscription = () =>
    manual ? [] : [...new Set([...keys(topics), ...declared.desired.names])].sort()

  /**
   * The next position of one acquired partition after pending seek and reset work resolves.
   * @type {import("../../types").Consumer["position"]}
   * @throws {KafkaJSNonRetriableError} for an invalid argument or a consumer that is not running
   * @throws {Error} `CONSUMER_STOPPING` when the acquisition is retired meanwhile, or the stored
   *   reset refusal
   */
  const position = async (entry) => {
    ManualGroupProtocol.validateTopicPartition(entry)
    refuseRetiredCaller()
    const group = consumerGroup
    if (!group) {
      throw new KafkaJSNonRetriableError(
        'Consumer group was not initialized, consumer#run must be called first'
      )
    }
    return withinCall((operation) =>
      group.position({ topic: entry.topic, partition: entry.partition }, { operation })
    )
  }

  /**
   * Reads the group's stable committed offsets from the broker for any valid pairs, assigned or
   * not. An observation only: it moves no local position and fills no cache.
   * @type {import("../../types").Consumer["committed"]}
   * @throws {KafkaJSNonRetriableError} without a group, or for invalid or duplicate pairs
   */
  const committed = async (topicPartitions) => {
    requireGroup()
    refuseRetiredCaller()
    if (!Array.isArray(topicPartitions)) {
      throw new KafkaJSNonRetriableError('committed requires an array of topic partitions')
    }
    const seen = new Set()
    const entries = topicPartitions.map((entry) => {
      ManualGroupProtocol.validateTopicPartition(entry)
      const key = JSON.stringify([entry.topic, entry.partition])
      if (seen.has(key)) {
        throw new KafkaJSNonRetriableError('committed requires unique topic-partition pairs')
      }
      seen.add(key)
      return { topic: entry.topic, partition: entry.partition }
    })
    if (entries.length === 0) return []
    return withinCall((operation) =>
      new CommittedOffsetReader({ cluster, groupId, retry }).read(entries, { operation })
    )
  }

  /**
   * @return {Object} logger
   */
  const getLogger = () => logger

  const consumer = {
    connect,
    disconnect,
    subscribe,
    assign,
    unsubscribe,
    assignment,
    subscription,
    position,
    committed,
    stop,
    run,
    enforceRebalance,
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
