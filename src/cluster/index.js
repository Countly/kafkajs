const describeThrown = require('../network/describeThrown')
const BrokerPool = require('./brokerPool')
const MetadataCache = require('./metadataCache')
const { joinDeadlineError } = require('../consumer/rebalanceErrorContract')
const { CLUSTER_TOPIC_RECREATED } = require('./instrumentationEvents')
const Lock = require('../utils/lock')
const { isConnectionFailure } = require('../network/connectionFailure')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const createRetry = require('../retry')
const connectionPoolBuilder = require('./connectionPoolBuilder')
const GroupRegistry = require('./groupRegistry')
const { EARLIEST_OFFSET, LATEST_OFFSET } = require('../constants')
const {
  KafkaJSError,
  KafkaJSBrokerNotFound,
  KafkaJSConnectionError,
  KafkaJSRequestTimeoutError,
  KafkaJSMetadataNotLoaded,
  KafkaJSTopicMetadataNotLoaded,
  KafkaJSGroupCoordinatorNotFound,
} = require('../errors')
const COORDINATOR_TYPES = require('../protocol/coordinatorTypes')
const apiKeys = require('../protocol/requests/apiKeys')
const { requests } = require('../protocol/requests')
const { staleLeaderEpoch, createErrorFromCode } = require('../protocol/error')

const dictionary = require('../utils/dictionary')

const { keys } = Object

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const LEADER_NOT_AVAILABLE = 5

const mergeTopics = (obj, { topic, partitions }) =>
  Object.assign(dictionary(), obj, {
    [topic]: [...(obj[topic] || []), ...partitions],
  })

/**
 * Metadata error codes that describe ONE topic rather than the request: the response carries them
 * per topic, and the client's `parse` throws on the first one it meets, taking the whole batch
 * with it.
 * @see clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
 */
const PER_TOPIC_METADATA_ERRORS = [
  'UNKNOWN_TOPIC_ID',
  'TOPIC_AUTHORIZATION_FAILED',
  'UNKNOWN_TOPIC_OR_PARTITION',
]

/**
 * @param {Error} e
 * @returns {boolean} whether the failure is about one topic in the request, not the request
 */
const isPerTopicError = (e) => PER_TOPIC_METADATA_ERRORS.includes(e.type)

/** A broker was asked by id and its answer reached the metadata cache. */
const RESOLVED = 'resolved'
/** The negotiated Metadata version cannot carry a topic id, so the ids need another route. */
const TOO_OLD = 'too-old'
/** The broker was asked, but nothing usable came back. */
const FAILED = 'failed'

/**
 * A producer lifecycle as the trailing argument of a call, or no argument at all: an unfenced
 * call keeps the shape it always had.
 * @param {import('../producer/generation')} [fence]
 * @returns {[{ fence: import('../producer/generation') }]|[]}
 */
const fenceArgs = (fence) => (fence ? [{ fence }] : [])

/**
 * A refresh's context as the trailing argument of a broker-pool call, or no argument at all: only
 * the fields the caller set are forwarded, so an ordinary call keeps the shape it always had.
 * @param {object} [context] fence, metadataScope, isCurrent, allowAutoTopicCreation, onMetadata
 * @returns {[object]|[]}
 */
const refreshArgs = (context = {}) => {
  const set = Object.fromEntries(Object.entries(context).filter(([, value]) => value !== undefined))
  return Object.keys(set).length ? [set] : []
}

const PRIVATE = {
  CONNECT: Symbol('private:Cluster:connect'),
  REFRESH_METADATA: Symbol('private:Cluster:refreshMetadata'),
  REFRESH_METADATA_IF_NECESSARY: Symbol('private:Cluster:refreshMetadataIfNecessary'),
  FIND_CONTROLLER_BROKER: Symbol('private:Cluster:findControllerBroker'),
}

module.exports = class Cluster {
  /**
   * Owns broker discovery and routing, with producer-only pins protecting live topic work.
   * @param {Object} options
   * @param {Array<string>} options.brokers example: ['127.0.0.1:9092', '127.0.0.1:9094']
   * @param {Object} options.ssl
   * @param {Object} options.sasl
   * @param {string} options.clientId
   * @param {number} options.connectionTimeout - in milliseconds
   * @param {number} options.authenticationTimeout - in milliseconds
   * @param {number} options.reauthenticationThreshold - in milliseconds
   * @param {number} [options.requestTimeout=30000] - in milliseconds
   * @param {boolean} [options.enforceRequestTimeout]
   * @param {number} options.metadataMaxAge - in milliseconds
   * @param {number} [options.metadataMaxIdle] producer-only idle expiry; undefined disables tracking
   * @param {boolean} options.allowAutoTopicCreation
   * @param {number|null} options.maxInFlightRequests
   * @param {number} options.isolationLevel
   * @param {import("../../types").RetryOptions} options.retry
   * @param {import("../../types").Logger} options.logger
   * @param {import("../../types").ISocketFactory} options.socketFactory
   * @param {Map} [options.offsets]
   * @param {Map} [options.operations] the per-group store of offset operations the `Kafka`
   *   instance shares between its clusters; kept as `operationsByGroup`, the way `offsets` is kept
   *   as `committedOffsetsByGroup`.
   * @param {number} [options.maxResponseSize] the response-size limit handed to every connection
   *   pool unchanged.
   * @param {'rebootstrap'|'none'} [options.metadataRecoveryStrategy] forwarded to `BrokerPool`
   *   unchanged; `Cluster` stores neither this nor the trigger window.
   * @param {number} [options.metadataRecoveryRebootstrapTriggerMs] forwarded to `BrokerPool`
   *   unchanged.
   * @param {import("../instrumentation/emitter")} [options.instrumentationEmitter=null] stored on
   *   the instance (`this.instrumentationEmitter`) and shared with the `MetadataCache` and the
   *   `BrokerPool`, so a topic recreation seen by either surfaces on the same emitter the client
   *   already wraps for its public `on()`.
   */
  constructor({
    logger: rootLogger,
    socketFactory,
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    authenticationTimeout,
    reauthenticationThreshold,
    requestTimeout = 30000,
    enforceRequestTimeout,
    metadataMaxAge,
    metadataMaxIdle,
    retry,
    allowAutoTopicCreation,
    maxInFlightRequests,
    isolationLevel,
    instrumentationEmitter = null,
    offsets = new Map(),
    operations = new Map(),
    maxResponseSize,
    metadataRecoveryStrategy,
    metadataRecoveryRebootstrapTriggerMs,
  }) {
    this.rootLogger = rootLogger
    this.logger = rootLogger.namespace('Cluster')
    this.retryOptions = { ...retry }
    this.retrier = createRetry(this.retryOptions)
    this.connectionPoolBuilder = connectionPoolBuilder({
      logger: rootLogger,
      instrumentationEmitter,
      socketFactory,
      brokers,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      requestTimeout,
      drainTimeout: requestTimeout,
      enforceRequestTimeout,
      maxInFlightRequests,
      maxResponseSize,
      reauthenticationThreshold,
    })

    this.targetTopics = new Set()
    this.metadataMaxIdle = metadataMaxIdle
    this.targetTopicState = metadataMaxIdle === undefined ? null : new Map()
    this.topicPins = metadataMaxIdle === undefined ? null : new Map()
    this.nextTopicIncarnation = 0
    /** @type {WeakMap<object, {topics: object[], byName: Map<string, object>}>} lookup index per accepted metadata object */
    this.topicMetadataIndexes = new WeakMap()
    this.mutatingTargetTopics = new Lock({
      description: `updating target topics`,
      timeout: requestTimeout,
    })
    this.isolationLevel = isolationLevel
    // Exposed so callers (e.g. Admin#fetchTopicMetadata) can decide whether an
    // UNKNOWN_TOPIC_OR_PARTITION response is a still-propagating auto-created topic worth
    // retrying, rather than a real "no such topic".
    this.allowAutoTopicCreation = allowAutoTopicCreation
    this.instrumentationEmitter = instrumentationEmitter
    this.metadataCache = new MetadataCache({
      logger: rootLogger,
      instrumentationEmitter,
    })
    this.brokerPool = new BrokerPool({
      connectionPoolBuilder: this.connectionPoolBuilder,
      logger: this.rootLogger,
      metadataCache: this.metadataCache,
      captureTopics:
        this.targetTopicState === null ? undefined : (topics) => this.captureTopics(topics),
      instrumentationEmitter,
      retry,
      allowAutoTopicCreation,
      authenticationTimeout,
      metadataMaxAge,
      metadataRecoveryStrategy,
      metadataRecoveryRebootstrapTriggerMs,
      drainTimeout: requestTimeout,
    })
    this.committedOffsetsByGroup = offsets
    this.operationsByGroup = operations
    this.groupRegistry = GroupRegistry.forStores(offsets, operations)
    this.groupEntries = new Map()
    this.groupScopes = new Map()

    // A producer lifecycle's call shares nothing with another lifecycle's: each is scoped by its fence.
    const fenceOf = ({ fence } = {}) => fence

    this[PRIVATE.CONNECT] = sharedPromiseTo(async ({ fence } = {}) => {
      return await this.brokerPool.connect(...fenceArgs(fence))
    }, fenceOf)

    // A refresh joins only calls of its own scope: a producer's fence, else the consumer's
    // subscription key (it moves with the generation and the declaration revision), else the
    // registration generation, so nothing joins a refresh begun before a mode change.
    const shareScopeOf = ({ fence, metadataScope } = {}) =>
      fence ??
      (metadataScope ?? this.brokerPool.consumerMetadataScope)?.shareKey() ??
      this.brokerPool.consumerMetadataGeneration ??
      0

    this[PRIVATE.REFRESH_METADATA] = sharedPromiseTo(async ({ fence, ...scope } = {}) => {
      fence?.check()
      this.pruneTargetTopics()
      if (this.targetTopicState !== null && this.targetTopics.size === 0) return
      return await this.brokerPool.refreshMetadata(
        Array.from(this.targetTopics),
        ...refreshArgs({ fence, ...scope })
      )
    }, shareScopeOf)

    this[PRIVATE.REFRESH_METADATA_IF_NECESSARY] = sharedPromiseTo(
      async ({ fence, ...scope } = {}) => {
        fence?.check()
        this.pruneTargetTopics()
        if (this.targetTopicState !== null && this.targetTopics.size === 0) return
        return await this.brokerPool.refreshMetadataIfNecessary(
          Array.from(this.targetTopics),
          ...refreshArgs({ fence, ...scope })
        )
      },
      shareScopeOf
    )

    this[PRIVATE.FIND_CONTROLLER_BROKER] = sharedPromiseTo(async () => {
      const { metadata } = this.brokerPool

      if (!metadata || metadata.controllerId == null) {
        throw new KafkaJSMetadataNotLoaded('Topic metadata not loaded')
      }

      const broker = await this.findBroker({ nodeId: metadata.controllerId })

      if (!broker) {
        throw new KafkaJSBrokerNotFound(
          `Controller broker with id ${metadata.controllerId} not found in the cached metadata`
        )
      }

      return broker
    })
  }

  isConnected() {
    return this.brokerPool.hasConnectedBrokers()
  }

  /**
   * The retrier of work that belongs to one producer lifecycle: it stops at once, with the
   * lifecycle's close error, when the lifecycle closes. Counts and delays are the cluster's own.
   * @public
   * @param {import('../producer/generation')} [fence] the captured producer lifecycle
   * @returns {import('../retry').Retrier} the cluster's own retrier when there is no lifecycle
   */
  retryFor(fence) {
    return fence ? createRetry({ ...this.retryOptions, fence }) : this.retrier
  }

  /**
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: its
   *   close ends the retries and opens no socket afterwards
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   */
  async connect({ fence } = {}) {
    await this[PRIVATE.CONNECT]({ fence })
  }

  /**
   * @public
   * @param {object} [options]
   * @param {number} [options.drainTimeout] ms each connection waits for its requests, 0 closing
   *   at once; the pools' own bound when omitted, which keeps a reusable client's close as it was
   * @returns {Promise<void>}
   */
  async disconnect(options) {
    try {
      await this.brokerPool.disconnect(...(options ? [options] : []))
    } finally {
      for (const groupId of this.groupEntries.keys()) this.groupRegistry.release(groupId, this)
      this.groupEntries.clear()
    }
  }

  /**
   * Forwards a failed broker's private provenance unchanged. A sourced stale failure never
   * falls back to address matching; the pool controller owns the current-generation fence.
   * @public
   * @param {object} destination failed endpoint and optional private source
   * @param {string} destination.host endpoint host
   * @param {number} destination.port endpoint port
   * @param {{broker: object, generation: number}} [destination.source] captured failure source
   * @returns {void}
   */
  removeBroker({ host, port, source }) {
    this.brokerPool.removeBroker({ host, port, source })
  }

  /**
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, the retries stop, no socket opens and a late response publishes nothing
   * @param {() => Error|null} [options.guard] a guarded refresh does not share an unguarded promise
   * @param {object} [options.metadataScope] the subscription that issues scope and sequence tickets;
   *   a registered default applies when omitted
   * @param {() => boolean} [options.isCurrent] additional membership, target or lifecycle guard
   * @param {boolean} [options.allowAutoTopicCreation] per-call creation override
   * @param {(metadata: object) => void} [options.onMetadata] synchronous accepted-snapshot observer
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   * @throws {Error} original refresh failure or captured scope refusal
   */
  async refreshMetadata({ fence, guard, ...scope } = {}) {
    if (guard) {
      return this.brokerPool.refreshMetadata(Array.from(this.targetTopics), {
        fence,
        guard,
        ...scope,
      })
    }
    await this[PRIVATE.REFRESH_METADATA]({ fence, ...scope })
  }

  /**
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] see `refreshMetadata`
   * @param {object} [options.metadataScope] see `refreshMetadata`
   * @param {() => boolean} [options.isCurrent] see `refreshMetadata`
   * @param {boolean} [options.allowAutoTopicCreation] see `refreshMetadata`
   * @param {(metadata: object) => void} [options.onMetadata] see `refreshMetadata`
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   */
  async refreshMetadataIfNecessary({ fence, ...scope } = {}) {
    await this[PRIVATE.REFRESH_METADATA_IF_NECESSARY]({ fence, ...scope })
  }

  /**
   * Fetches metadata for topics identified only by their id, so their names can be resolved.
   *
   * An assignment computed by the group coordinator names topics by uuid, and nothing in this
   * client can fetch from a topic it cannot name. `TopicId` appears in the request schema at v10,
   * but `MetadataRequest.json` says the server never implemented it there: "Versions 10 and 11
   * should not use the topicId field or set topic name to null. Version 12 supports topic Id." So
   * v12 is the floor, and on an older negotiated version this falls back to a full refresh by name
   * and leaves the ids to the caller's own retry.
   *
   * Deliberately NOT routed through `BrokerPool#refreshMetadata`: that method replaces
   * `brokerPool.metadata` with its response, and `findTopicPartitionMetadata` reads that object
   * for every topic - so a two-id refresh would make the client believe the cluster has two
   * topics. The response feeds `MetadataCache#updateTopicsById`, the entry point for a response that
   * routes nowhere, which records the named topics without disturbing the write-through of any
   * other one; the names it resolves are registered as target topics by the caller, and that
   * registration triggers an ordinary complete refresh.
   *
   * Not wrapped in `sharedPromiseTo` either, unlike every other refresh here: those all ask the
   * same question, this one takes an argument, and de-duplicating two different id sets would
   * silently drop one.
   *
   * Best effort by design: it never throws, whatever happened. It is called from the heartbeat
   * path, where a throw would take the membership down over a metadata hiccup the next tick
   * retries anyway. Two consequences of "best effort" worth stating: a *response* failure is not
   * retried against a second broker (the callback returns rather than throws, so `withBroker`'s
   * fan-out never runs), and `unresolved` says only that the cache still cannot name the id - not
   * whether the cluster refused it or the client never managed to ask.
   *
   * @see https://kafka.apache.org/43/design/protocol
   * @see clients/src/main/resources/common/message/MetadataRequest.json
   * @public
   * @param {string[]} topicIds canonical uuid strings
   * @param {{ isCurrent?: () => boolean }} [options] checked before requests and cache installs;
   *   guarded old-version discovery reads names without installing global routing metadata
   * @returns {Promise<{ resolved: string[], unresolved: string[] }>} the requested ids, split by
   *   whether the cache can name them now. Duplicates and nulls are dropped from both lists.
   */
  async refreshMetadataForTopicIds(topicIds, { isCurrent = () => true } = {}) {
    const ids = [...new Set(topicIds)].filter((topicId) => topicId != null)

    if (!isCurrent() || ids.length === 0) {
      return { resolved: [], unresolved: [] }
    }

    const guarded = arguments[1]?.isCurrent !== undefined
    let outcome = null

    try {
      outcome = await this.brokerPool.withBroker(async ({ broker }) => {
        if (!isCurrent()) return FAILED
        const version = broker.negotiatedVersion(apiKeys.Metadata, requests.Metadata)

        if (version == null || version < 12) {
          if (!guarded) return TOO_OLD
          const response = await broker.metadata()
          if (!isCurrent()) return FAILED
          this.metadataCache.updateTopicsById(response)
          return RESOLVED
        }

        return await this.fetchMetadataByTopicIds(broker, ids, { isCurrent })
      })

      if (outcome === TOO_OLD && isCurrent()) {
        this.logger.debug('Cannot request metadata by topic id; refreshing by name instead', {
          topicIds: ids,
        })
        await this.refreshMetadata()
      }
    } catch (e) {
      // `withBroker` throws KafkaJSBrokerNotFound on an empty pool (before the first connect, and
      // after a rebootstrap), and `refreshMetadata` can reject for any transport reason. Both are
      // reachable from the heartbeat path.
      this.logger.debug('Could not refresh metadata for topic ids', {
        topicIds: ids,
        error: e.message,
      })
    }

    if (!isCurrent()) return { resolved: [], unresolved: [] }

    const resolution = {
      resolved: ids.filter((topicId) => this.metadataCache.topicName(topicId) != null),
      unresolved: ids.filter((topicId) => this.metadataCache.topicName(topicId) == null),
    }

    if (resolution.unresolved.length > 0) {
      this.logger.debug('Some topic ids could not be resolved to a name', {
        ...resolution,
        outcome,
      })
    }

    return resolution
  }

  /**
   * One broker's answer to a by-id metadata request, with a per-id retry on a topic-level failure.
   *
   * `MetadataResponse`'s `parse` throws on the **first** topic carrying an error code, so a batch
   * holding one id the cluster will never resolve - a deleted topic (`UNKNOWN_TOPIC_ID`), one the
   * member may not describe (`TOPIC_AUTHORIZATION_FAILED`) - discards the ids that decoded
   * perfectly alongside it. Left alone, that one id would block every other id in the set for as
   * long as the membership holds it. So a topic-level failure is re-asked one id at a time: the
   * healthy ids resolve, the bad ones fail on their own and are simply left unnamed. The extra
   * round trips are paid only on that path.
   *
   * A transport failure is not retried per id - it says nothing about any particular id - and is
   * logged and swallowed like everything else here.
   *
   * @private
   * @param {import("../broker")} broker
   * @param {string[]} ids
   * @param {{ isCurrent?: () => boolean }} [options] stale responses have no cache effects
   * @returns {Promise<'resolved'|'failed'>} whether the client managed to ask at all
   */
  async fetchMetadataByTopicIds(broker, ids, { isCurrent = () => true } = {}) {
    try {
      if (!isCurrent()) return FAILED
      const response = await broker.metadata(ids.map((topicId) => ({ topicId })))
      if (!isCurrent()) return FAILED
      this.metadataCache.updateTopicsById(response)
      return RESOLVED
    } catch (e) {
      if (!isCurrent()) return FAILED
      if (ids.length === 1 || !isPerTopicError(e)) {
        this.logger.debug('Metadata by topic id failed; leaving the ids unresolved', {
          topicIds: ids,
          error: e.message,
        })
        return FAILED
      }

      this.logger.debug('A topic id in the batch failed; asking for each one separately', {
        topicIds: ids,
        error: e.message,
      })

      for (const topicId of ids) {
        if (!isCurrent()) return FAILED
        try {
          const response = await broker.metadata([{ topicId }])
          if (!isCurrent()) return FAILED
          this.metadataCache.updateTopicsById(response)
        } catch (topicError) {
          if (!isCurrent()) return FAILED
          this.logger.debug('Topic id could not be resolved to a name', {
            topicId,
            error: topicError.message,
          })
        }
      }

      return RESOLVED
    }
  }

  /**
   * Reads a one-broker leader snapshot without installing metadata or changing targets.
   * @param {object} options per-attempt scope
   * @param {string[]} options.topics missing union names
   * @param {boolean} options.allowAutoTopicCreation effective subscription policy
   * @param {number} options.deadlineAt absolute performance.now() deadline
   * @param {() => Error|null} options.guard captured run and leader-attempt refusal
   * @returns {Promise<object>} decoded snapshot, with topic/partition errors retained
   * @throws {Error} original acquisition/auth/transport failure, or retriable join expiry
   */
  async groupMetadata({ topics, allowAutoTopicCreation, deadlineAt, guard }) {
    const startedAt = performance.now()
    const expired = joinDeadlineError({
      rebalanceTimeoutMs: Math.max(0, deadlineAt - startedAt),
    })
    let finished = false
    const refusal = () => guard() || (finished || performance.now() >= deadlineAt ? expired : null)
    const check = () => {
      const error = refusal()
      if (error) throw error
    }
    check()
    const brokers = Object.values(this.brokerPool.brokers)
    const broker =
      brokers.find((candidate) => candidate.isConnected()) ||
      brokers[0] ||
      this.brokerPool.seedBroker
    if (!broker) throw new KafkaJSBrokerNotFound('No broker available for group metadata')
    let timer
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(expired), Math.max(0, deadlineAt - performance.now()))
    })
    const read = async () => {
      check()
      await broker.connect()
      check()
      const result = await broker.metadata(topics, {
        allowAutoTopicCreation,
        lenient: true,
        guard: refusal,
      })
      check()
      return result
    }
    try {
      return await Promise.race([read(), timeout])
    } finally {
      finished = true
      clearTimeout(timer)
    }
  }

  /**
   * The cluster's metadata for `topics` (every topic when empty), as a broker answers it now. In
   * three stages:
   * 1. The refresh pre-step, `refreshMetadataIfNecessary`, which writes the metadata cache and
   *    emits `TOPIC_RECREATED` as any other route does, but only when the cache is missing,
   *    expired or lacks a named topic. The refresh retries `LEADER_NOT_AVAILABLE` on its own
   *    `retry` budget; once that is spent it throws a non-retriable `KafkaJSNumberOfRetriesExceeded`
   *    (`type` `LEADER_NOT_AVAILABLE`), which this method does not retry: it rejects at once with a
   *    `KafkaJSNonRetriableError` whose `cause` is the broker's error. A raw `LEADER_NOT_AVAILABLE`
   *    that escapes the refresh would be retried here. Any other refresh error is thrown as it is.
   * 2. Broker selection, `withBroker`: a broker that cannot be found or connected to raises a
   *    retriable error, which this method retries within the `retry` budget, ending in
   *    `KafkaJSNumberOfRetriesExceeded`, or `KafkaJSDeliveryTimeout` when `retry.maxTotalTime`
   *    passes first. A selection or connection error that cannot succeed, a SASL authentication
   *    failure for example, rejects at once with a `KafkaJSNonRetriableError`.
   * 3. The call on each broker in turn: a failure moves on to the next broker, and the first
   *    answer is returned. When every broker is connected to and every call fails, the last
   *    broker's error is thrown as it is (class and cause kept) after that one pass, never retried
   *    here. When some calls fail and a later broker cannot be connected to, the attempt ends as
   *    in stage 2 (`KafkaJSNumberOfRetriesExceeded` after the budget) and the call errors are lost.
   *
   * The answer is a snapshot: it is never merged into the metadata cache, never emits
   * `TOPIC_RECREATED` and never changes the target topics.
   * @public
   * @param {{ topics?: string[] }} [options]
   * @returns {Promise<import("../../types").BrokerMetadata>}
   * @throws {Error} the last broker error, once every broker is connected to and has failed; or a
   *   refresh error other than `LEADER_NOT_AVAILABLE`, as it is
   * @throws {import("../errors").KafkaJSNumberOfRetriesExceeded} when brokers cannot be found or
   *   connected to for the whole `retry` budget
   * @throws {import("../errors").KafkaJSDeliveryTimeout} when `retry.maxTotalTime` passes before
   *   that budget is spent
   * @throws {import("../errors").KafkaJSNonRetriableError} when the refresh spent its budget on
   *   `LEADER_NOT_AVAILABLE`, or broker selection fails in a way that cannot succeed
   */
  async metadata({ topics = [] } = {}) {
    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.brokerPool.refreshMetadataIfNecessary(topics)
      } catch (e) {
        if (e.type === 'LEADER_NOT_AVAILABLE') {
          throw e
        }

        return bail(e)
      }

      let lastError
      const metadata = await this.brokerPool.withBroker(async ({ broker }) => {
        try {
          return await broker.metadata(topics)
        } catch (e) {
          lastError = e
          throw e
        }
      })

      if (metadata === null) {
        return bail(lastError)
      }

      return metadata
    })
  }

  /**
   * Reads the metadata of `topics` as a broker answers now, installing nothing: no cache write, no
   * `TOPIC_RECREATED`, no target change. It skips `metadata()`'s refresh pre-step, so a candidate
   * that is later rejected leaves no effect behind. Unknown topics follow the client's configured
   * auto-creation setting, and a topic-level error rejects.
   * @public
   * @param {object} options
   * @param {string[]} options.topics the names to describe
   * @param {() => Error|null} [options.guard] a caller's currentness check, evaluated before the
   *   request is written and after it answers; its refusal replaces the answer
   * @returns {Promise<import("../../types").BrokerMetadata>} the decoded snapshot
   * @throws {Error} the guard's refusal, or the last broker error once the retry budget is spent
   */
  async stageMetadata({ topics, guard }) {
    return this.retrier(async (bail) => {
      const before = guard?.()
      if (before) return bail(before)

      const brokers = Object.values(this.brokerPool.brokers)
      const broker =
        brokers.find((candidate) => candidate.isConnected()) ||
        brokers[0] ||
        this.brokerPool.seedBroker
      if (!broker) throw new KafkaJSBrokerNotFound('No broker available for staged metadata')

      await broker.connect()
      const metadata = await broker.metadata(topics, { guard: guard && (() => guard()) })
      const after = guard?.()
      if (after) return bail(after)
      return metadata
    })
  }

  /**
   * @public
   * @param {string} topic
   * @return {Promise}
   */
  async addTargetTopic(topic) {
    return this.addMultipleTargetTopics([topic])
  }

  /**
   * @public
   * @param {string[]} topics
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, nothing is added, and a failed add rolls nothing back, so it never touches the
   *   state of a lifecycle that began after it; its own lock is released either way
   * @return {Promise}
   * @throws {Error} the lifecycle's close error
   */
  async addMultipleTargetTopics(topics, { fence } = {}) {
    fence?.check()
    await this.mutatingTargetTopics.acquire()

    try {
      fence?.check()
      const previousSize = this.targetTopics.size
      const previousTopics = new Set(this.targetTopics)
      for (const topic of topics) {
        this.targetTopics.add(topic)
        this.touchTargetTopic(topic)
      }

      const hasChanged = previousSize !== this.targetTopics.size || !this.brokerPool.metadata

      if (hasChanged) {
        try {
          await this.refreshMetadata(...fenceArgs(fence))
        } catch (e) {
          fence?.check()
          if (
            e.type === 'INVALID_TOPIC_EXCEPTION' ||
            e.type === 'UNKNOWN_TOPIC_OR_PARTITION' ||
            e.type === 'TOPIC_AUTHORIZATION_FAILED'
          ) {
            if (this.targetTopicState === null) {
              this.targetTopics = previousTopics
            } else {
              for (const topic of this.targetTopics) {
                if (previousTopics.has(topic)) continue
                this.targetTopics.delete(topic)
                this.targetTopicState.delete(topic)
                this.metadataCache.forget(topic)
                this.brokerPool.forgetTopic(topic)
              }
            }
          }

          throw e
        }
      }
    } finally {
      await this.mutatingTargetTopics.release()
    }
  }

  /**
   * Stamps an active producer target, allocating an incarnation only on its first use.
   * Other client kinds keep no tracking state.
   * @param {string} topic active target name
   * @returns {void}
   */
  touchTargetTopic(topic) {
    if (this.targetTopicState === null || !this.targetTopics.has(topic)) return
    const previous = this.targetTopicState.get(topic)
    this.targetTopicState.set(topic, {
      lastUsed: Date.now(),
      incarnation: previous?.incarnation ?? ++this.nextTopicIncarnation,
    })
  }

  /**
   * Protects a producer topic while an owner holds charged storage or awaits admission: one
   * count per call, ended by one `releaseTopic`. Other client kinds keep no pins.
   * @param {string} topic name owned by the send
   * @returns {void}
   */
  holdTopic(topic) {
    if (this.topicPins === null) return
    this.topicPins.set(topic, (this.topicPins.get(topic) ?? 0) + 1)
  }

  /**
   * Ends one `holdTopic` count; the owner calls it once per hold. Issues no metadata request.
   * @param {string} topic name the owner held
   * @returns {void}
   */
  releaseTopic(topic) {
    const pins = this.topicPins?.get(topic)
    if (pins === undefined) return
    if (pins === 1) this.topicPins.delete(topic)
    else this.topicPins.set(topic, pins - 1)
  }

  /**
   * Forgets only idle, unpinned producer targets and both cached representations.
   * Runs once before a refresh; no timer or retired-name history is kept.
   * @returns {void}
   */
  pruneTargetTopics() {
    if (this.targetTopicState === null) return
    const now = Date.now()
    for (const [topic, { lastUsed }] of this.targetTopicState) {
      if (this.topicPins.has(topic) || now - lastUsed < this.metadataMaxIdle) continue
      this.targetTopics.delete(topic)
      this.targetTopicState.delete(topic)
      this.metadataCache.forget(topic)
      this.brokerPool.forgetTopic(topic)
    }
  }

  /**
   * Captures the active incarnations requested by one scoped refresh before it awaits.
   * A late answer cannot publish a forgotten topic or overwrite its replacement.
   * @param {string[]} topics requested names
   * @returns {(topic: string) => boolean} synchronous publication predicate
   */
  captureTopics(topics) {
    const captured = new Map(
      topics.map((topic) => [topic, this.targetTopicState.get(topic)?.incarnation])
    )
    return (topic) => {
      const incarnation = captured.get(topic)
      return (
        incarnation !== undefined && this.targetTopicState.get(topic)?.incarnation === incarnation
      )
    }
  }

  /**
   * Registers the first client-pattern scope on its owning consumer cluster. This also fences
   * earlier unscoped snapshot pre-steps. Literal-only temporary admin consumers do not register.
   * @param {object} scope the consumer's stable subscription owner
   * @returns {void}
   */
  useConsumerMetadataScope(scope) {
    if (this.brokerPool.consumerMetadataScope === scope) return
    this.brokerPool.consumerMetadataScope = scope
    this.brokerPool.consumerMetadataGeneration =
      (this.brokerPool.consumerMetadataGeneration ?? 0) + 1
  }

  /**
   * Releases only the captured owner's default scope and fences every earlier refresh.
   * A late old consumer cannot detach a replacement owner's scope.
   * @param {object} scope the owner being disposed
   * @returns {void}
   */
  releaseConsumerMetadataScope(scope) {
    if (this.brokerPool.consumerMetadataScope !== scope) return
    this.brokerPool.consumerMetadataScope = null
    this.brokerPool.consumerMetadataGeneration =
      (this.brokerPool.consumerMetadataGeneration ?? 0) + 1
  }

  /**
   * Replaces consumer targets and prunes removed decoded/id state without issuing a request.
   * Pattern callers pass an empty list. Captured guards are rechecked after the target lock.
   * `publish` runs under that lock right after the replacement with no await between them, so a
   * caller can make its own state and the targets change together or not at all.
   * @param {string[]} names accepted desired target names
   * @param {{isCurrent?: () => boolean, publish?: () => void}} [options]
   * @returns {Promise<boolean>} whether the replacement and `publish` ran; false when `isCurrent`
   *   refused before or after the lock, in which case nothing changed
   * @throws {Error} when acquiring the existing target lock fails
   */
  async resetTargetTopics(names, { isCurrent = () => true, publish } = {}) {
    const next = new Set(names)
    if (!isCurrent()) return false
    await this.mutatingTargetTopics.acquire()
    try {
      if (!isCurrent()) return false
      for (const topic of this.targetTopics) {
        if (next.has(topic)) continue
        this.metadataCache.forget(topic)
        this.brokerPool.forgetTopic(topic)
      }
      this.targetTopics = next
      this.topicMetadataIndexes = new WeakMap()
      publish?.()
      return true
    } finally {
      await this.mutatingTargetTopics.release()
    }
  }

  /** @type {() => string[]} */
  getNodeIds() {
    return this.brokerPool.getNodeIds()
  }

  /**
   * Calls `refreshMetadata()` for its side effect only, swallowing whatever it rejects with.
   *
   * Every caller of this helper is already inside its own `catch` block, refreshing metadata as a
   * best-effort attempt to fix staleness before its *own* error is rethrown (or before a
   * surrounding retrier tries again). If the refresh itself fails (e.g.
   * `KafkaJSNumberOfRetriesExceeded` while the pool cannot reach anything right now), that failure
   * must never replace the original error the caller is already handling - an unguarded `await`
   * here would let it do exactly that, silently changing the error class, `retryCount`/`retryTime`
   * and any `instanceof`/name check downstream depends on.
   *
   * @private
   * @param {import('../producer/generation')} [fence] the producer lifecycle asking
   * @returns {Promise<void>}
   */
  async refreshMetadataBestEffort(fence) {
    // `Promise.resolve(...)` tolerates a test double that returns `undefined` instead of a
    // promise (several specs stub `refreshMetadata` with a bare `jest.fn()`); the real,
    // production `refreshMetadata()` is an `async` method and always returns a genuine promise,
    // so this changes nothing there.
    await Promise.resolve(this.refreshMetadata(...fenceArgs(fence))).catch((refreshError) => {
      this.logger.debug('Best-effort metadata refresh failed, continuing with the original error', {
        error: refreshError.message,
      })
    })
  }

  /**
   * @public
   * @param {object} options
   * @param {string} options.nodeId
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: its
   *   close ends the connection attempts and the refresh a failure would start
   * @returns {Promise<import("../../types").Broker>}
   * @throws {Error} the lifecycle's close error
   */
  async findBroker({ nodeId, fence }) {
    try {
      return await this.brokerPool.findBroker({ nodeId, fence })
    } catch (e) {
      fence?.check()
      // The client probably has stale metadata
      if (
        describeThrown(e, 'name') === 'KafkaJSBrokerNotFound' ||
        describeThrown(e, 'name') === 'KafkaJSLockTimeout' ||
        isConnectionFailure(e)
      ) {
        await this.refreshMetadataBestEffort(fence)
      }

      throw e
    }
  }

  /**
   * @public
   * @returns {Promise<import("../../types").Broker>}
   */
  async findControllerBroker() {
    return await this[PRIVATE.FIND_CONTROLLER_BROKER]()
  }

  /**
   * Any connected broker: a cached one already connected, else the first cached one that
   * connects, else the seed broker. Needs no metadata, so it serves a request any broker answers,
   * such as a non-transactional InitProducerId, which the Java producer sends to its least loaded
   * node.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @public
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] the producer lifecycle asking: once
   *   it closed, no broker is tried and no socket opens
   * @returns {Promise<import("../../types").Broker>}
   * @throws {Error} the seed broker's connection error, when no broker connects, or the
   *   lifecycle's close error
   */
  async findConnectedBroker({ fence } = {}) {
    return await this.brokerPool.findConnectedBroker(...fenceArgs(fence))
  }

  /**
   * @public
   * @param {string} topic
   * @returns {import("../../types").PartitionMetadata[]} Example:
   *                   [{
   *                     isr: [2],
   *                     leader: 2,
   *                     partitionErrorCode: 0,
   *                     partitionId: 0,
   *                     replicas: [2],
   *                   }]
   */
  findTopicPartitionMetadata(topic) {
    const { metadata } = this.brokerPool
    if (!metadata || !metadata.topicMetadata) {
      throw new KafkaJSTopicMetadataNotLoaded('Topic metadata not loaded', {
        topic,
      })
    }

    this.topicMetadataIndexes ??= new WeakMap()
    let index = this.topicMetadataIndexes.get(metadata)
    if (!index || index.topics !== metadata.topicMetadata) {
      const byName = new Map()
      for (const entry of metadata.topicMetadata) {
        if (!byName.has(entry.topic)) byName.set(entry.topic, entry)
      }
      index = { topics: metadata.topicMetadata, byName }
      this.topicMetadataIndexes.set(metadata, index)
    }
    return index.byName.get(topic)?.partitionMetadata ?? []
  }

  /**
   * The current id of a topic, as of the last Metadata response that mentioned it.
   *
   * Internal: this is the seam the consumer's topic-recreation reset uses to tell one
   * incarnation of a topic from another. It is not part of the public Cluster surface.
   *
   * @param {string} topic
   * @returns {string|undefined} the topic id, or undefined when the cache has not seen it
   */
  topicId(topic) {
    return this.metadataCache.topicId(topic)
  }

  /**
   * The name of a topic, as of the last Metadata response that mentioned it.
   *
   * Internal: the read half of the id-to-name seam, mirroring `topicId(topic)` above. A
   * broker-computed assignment names topics by uuid, and this is how the consumer resolves one
   * without reaching into the metadata cache itself.
   *
   * @param {string} topicId
   * @returns {string|undefined} the topic's name, or undefined when the cache has not seen it
   */
  topicName(topicId) {
    return this.metadataCache.topicName(topicId)
  }

  /**
   * The cached leader epoch of one partition, as of the last Metadata response or accepted leader
   * hint. Internal seam for the consumer's fetch path; not on the public Cluster surface.
   * @param {string} topic
   * @param {number} partition
   * @returns {number} the epoch, or -1 when unknown
   */
  leaderEpoch(topic, partition) {
    return this.metadataCache.leaderEpoch(topic, partition)
  }

  /**
   * The cached leader of one partition, as of the last Metadata response or accepted leader hint:
   * a map lookup, never a refresh. Internal seam for the producer's accumulator; not on the public
   * Cluster surface.
   * @param {string} topic
   * @param {number} partition
   * @returns {number} the node id, or -1 when the partition is unknown or has no leader
   */
  cachedLeader(topic, partition) {
    return this.metadataCache.leader(topic, partition)
  }

  /**
   * Forgets one partition's cached leader epoch. Called when a broker reports the client is ahead
   * of it (`UNKNOWN_LEADER_EPOCH`), so the next request stops fencing on an epoch the broker does
   * not have and the next metadata response converges.
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean} true when a cached entry existed and was forgotten
   */
  forgetLeaderEpoch(topic, partition) {
    return this.metadataCache.forgetLeaderEpoch(topic, partition)
  }

  /**
   * Subscribes to routing changes, synchronously after each is visible: an accepted metadata
   * install, an accepted leader hint, a by-ID write-through or an invalidation. Internal, and
   * independent of the instrumentation events.
   * @param {() => void} handler called once per change
   * @returns {() => void} idempotent unsubscribe
   */
  onRoutingChange(handler) {
    return this.metadataCache.onRoutingChange(handler)
  }

  /**
   * Whether the broker table currently holds a node with this id. Used before trusting a leader
   * hint that carries no endpoint for the node it names.
   * @param {number|string} nodeId
   * @returns {boolean}
   */
  hasNode(nodeId) {
    return this.brokerPool.hasNode(nodeId)
  }

  /**
   * Applies a leader hint a produce error carried (KIP-951): the broker that now owns the
   * partition, and the endpoints of the nodes it named.
   *
   * The epoch check runs first, so a hint that does not advance the partition's leader epoch
   * costs nothing (unless the partition itself is unknown to the cache - see below). The endpoint
   * work runs **before** the cache write, because the cache write is synchronous and immediately
   * visible to routing: writing it first would publish a node id for which no broker object
   * exists yet, and a concurrent send would take the broker-not-found path and force a metadata
   * refresh - the exact latency this hint exists to avoid. That ordering only closes the window
   * when the hint actually carries an endpoint for `leaderId`; a hint with no `nodeEndpoints` (the
   * broker believes the client already knows the node) can still publish a node id the broker
   * table has never heard of, which takes the broker-not-found path regardless of ordering here.
   *
   * The pre-check mirrors `MetadataCache#applyLeaderHint`'s own **epoch** guard exactly (an
   * unknown cached epoch, `-1`, never rejects) so it can only short-circuit a hint the cache would
   * reject anyway, never one it would accept; the cache stays the single authority, since it also
   * knows about topics and partitions this pre-check cannot see. A hint for a partition the cache
   * does not know at all is one such case the pre-check cannot short-circuit: the epoch compares
   * as advancing (nothing cached to compare against), so the endpoint work still runs, and only
   * `MetadataCache#applyLeaderHint`'s own `!cached` branch rejects it afterwards.
   *
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
   * @public
   * @param {object} hint
   * @param {string} hint.topic
   * @param {number} hint.partition
   * @param {number} hint.leaderId
   * @param {number} hint.leaderEpoch
   * @param {{ nodeId: number, host: string, port: number, rack: string|null }[]} [hint.nodeEndpoints]
   * @param {object} [options]
   * @param {import('../producer/generation')} [options.fence] captured producer lifecycle; its
   *   close prevents endpoint installation and a later cache update
   * @returns {Promise<boolean>} `false` when the hint was rejected - a non-advancing epoch, a
   *   `leaderId` of `-1` (no hint offered), or an unknown topic/partition - `true` when it was
   *   applied
   * @throws {Error} the lifecycle's close error
   */
  async applyLeaderHint(
    { topic, partition, leaderId, leaderEpoch, nodeEndpoints },
    { fence } = {}
  ) {
    fence?.check()
    // Mirrors MetadataCache#applyLeaderHint's own no-hint-offered guard, so this pre-check can
    // never itself pay for the endpoint work below on the broker's `{ -1, -1 }` default.
    if (leaderId === -1) {
      return false
    }

    const cachedLeaderEpoch = this.metadataCache.leaderEpoch(topic, partition)

    if (cachedLeaderEpoch > -1 && leaderEpoch <= cachedLeaderEpoch) {
      return false
    }

    await this.brokerPool.applyNodeEndpoints(nodeEndpoints, ...fenceArgs(fence))
    fence?.check()
    return this.metadataCache.applyLeaderHint(topic, partition, { leaderId, leaderEpoch })
  }

  /**
   * Subscribes to the cluster-level "a known topic name came back with a different id" event.
   *
   * Internal: the consumer uses it to schedule a one-shot offset reset. Applications observe the
   * same event through `consumer.events.TOPIC_RECREATED` and its producer/admin equivalents.
   *
   * @param {(event: import('../instrumentation/event')) => void} handler
   * @returns {() => void} removes the listener
   */
  onTopicRecreated(handler) {
    if (!this.instrumentationEmitter) {
      return () => {}
    }

    return this.instrumentationEmitter.addListener(CLUSTER_TOPIC_RECREATED, handler)
  }

  /**
   * The leader of each partition, from the cached metadata; a partition the cache does not know is
   * left out. A negative leader (`-1`) means the partition has none: an answer to a request for
   * every topic keeps such a partition, so the cache can hold one. It is reported as the broker
   * reports it, with the retriable `LEADER_NOT_AVAILABLE` every caller refreshes the metadata on,
   * and never routed to a node that does not exist, as the Java client looks a partition with a
   * negative leader up again.
   * @public
   * @param {string} topic
   * @param {(number|string)[]} partitions
   * @returns {Object} Object with leader and partitions. For partitions 0 and 5
   *                   the result could be:
   *                     { '0': [0], '2': [5] }
   *
   *                   where the key is the nodeId.
   * @throws {import('../errors').KafkaJSProtocolError} `LEADER_NOT_AVAILABLE`, for a requested
   *   partition whose leader is negative
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/internals/PartitionLeaderStrategy.java
   */
  findLeaderForPartitions(topic, partitions) {
    const partitionMetadata = this.findTopicPartitionMetadata(topic)
    return partitions.reduce((result, id) => {
      const partitionId = parseInt(id, 10)
      const metadata = partitionMetadata.find((p) => p.partitionId === partitionId)

      if (!metadata) {
        return result
      }

      if (metadata.leader === null || metadata.leader === undefined) {
        throw new KafkaJSError('Invalid partition metadata', {
          topic,
          partitionId,
          metadata,
        })
      }

      if (metadata.leader < 0) {
        throw createErrorFromCode(LEADER_NOT_AVAILABLE)
      }

      const { leader } = metadata
      const current = result[leader] || []
      return { ...result, [leader]: [...current, partitionId] }
    }, {})
  }

  /**
   * The broker coordinating a group or a transactional id. A coordinator the broker table does
   * not hold yet is reached at the address FindCoordinator answered with, as the Java client
   * reaches it, so no Metadata response is needed first. A coordinator the table holds is
   * reached through its cached broker.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @public
   * @param {object} params
   * @param {string} params.groupId
   * @param {import("../protocol/coordinatorTypes").CoordinatorType} [params.coordinatorType=0]
   * @param {import('../producer/generation')} [params.fence] the producer lifecycle asking: its
   *   close ends the retries, and an answer that arrives after it is neither installed nor used
   * @returns {Promise<import("../../types").Broker>}
   * @throws {Error} the lifecycle's close error
   */
  async findGroupCoordinator({ groupId, coordinatorType = COORDINATOR_TYPES.GROUP, fence }) {
    return this.retryFor(fence)(async (bail, retryCount, retryTime) => {
      try {
        const { coordinator } = await this.findGroupCoordinatorMetadata({
          groupId,
          coordinatorType,
          fence,
        })
        fence?.check()

        if (!this.brokerPool.hasNode(coordinator.nodeId)) {
          await this.brokerPool.applyNodeEndpoints(
            [{ ...coordinator, rack: null }],
            ...fenceArgs(fence)
          )
          fence?.check()
        }

        return await this.findBroker({ nodeId: coordinator.nodeId, fence })
      } catch (e) {
        fence?.check()
        // A new broker can join the cluster before we have the chance
        // to refresh metadata
        if (e.name === 'KafkaJSBrokerNotFound' || e.type === 'GROUP_COORDINATOR_NOT_AVAILABLE') {
          this.logger.debug(`${e.message}, refreshing metadata and trying again...`, {
            groupId,
            retryCount,
            retryTime,
          })

          await this.refreshMetadataBestEffort(fence)
          throw e
        }

        if (e.code === 'ECONNREFUSED') {
          // During maintenance the current coordinator can go down; findBroker will
          // refresh metadata and re-throw the error. findGroupCoordinator has to re-throw
          // the error to go through the retry cycle.
          throw e
        }

        bail(e)
      }
    })
  }

  /**
   * One FindCoordinator answer, from the first cached broker that gives one. Before the first
   * Metadata response the broker table is empty, and the seed broker is asked instead; a
   * connection failure or a timeout there is retriable, as the next cached broker would be.
   * @public
   * @param {object} params
   * @param {string} params.groupId
   * @param {import("../protocol/coordinatorTypes").CoordinatorType} [params.coordinatorType=0]
   * @param {import('../producer/generation')} [params.fence] the producer lifecycle asking: once
   *   it closed, no broker is asked and no socket opens
   * @returns {Promise<Object>}
   * @throws {KafkaJSBrokerNotFound} when the seed broker's connection failed or timed out
   * @throws {KafkaJSGroupCoordinatorNotFound} when no broker answered
   * @throws {Error} the lifecycle's close error
   */
  async findGroupCoordinatorMetadata({ groupId, coordinatorType, fence }) {
    const findCoordinator = async ({ nodeId, broker }) => {
      return await this.retryFor(fence)(async (bail, retryCount, retryTime) => {
        try {
          fence?.check()
          const brokerMetadata = await broker.findGroupCoordinator({
            groupId,
            coordinatorType,
            ...(fence ? { guard: fence.guard } : {}),
          })
          fence?.check()
          this.logger.debug('Found group coordinator', {
            broker: brokerMetadata.host,
            nodeId: brokerMetadata.coordinator.nodeId,
          })
          return brokerMetadata
        } catch (e) {
          fence?.check()
          this.logger.debug('Tried to find group coordinator', {
            nodeId,
            groupId,
            error: e,
          })

          if (e.type === 'GROUP_COORDINATOR_NOT_AVAILABLE') {
            this.logger.debug('Group coordinator not available, retrying...', {
              nodeId,
              retryCount,
              retryTime,
            })

            throw e
          }

          bail(e)
        }
      })
    }

    let brokerMetadata
    if (this.brokerPool.getNodeIds().length > 0) {
      brokerMetadata = await this.brokerPool.withBroker(findCoordinator, ...fenceArgs(fence))
    } else {
      const broker = await this.brokerPool.findConnectedBroker(...fenceArgs(fence))
      brokerMetadata = await findCoordinator({ nodeId: broker.nodeId, broker }).catch((e) => {
        fence?.check()
        if (e instanceof KafkaJSConnectionError || e instanceof KafkaJSRequestTimeoutError) {
          const message = `FindCoordinator failed on the seed broker: ${e.message}`
          throw new KafkaJSBrokerNotFound(message, { cause: e })
        }

        return null
      })
    }

    if (brokerMetadata) {
      return brokerMetadata
    }

    throw new KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator')
  }

  /**
   * @param {object} topicConfiguration
   * @returns {number}
   */
  defaultOffset({ fromBeginning }) {
    return fromBeginning ? EARLIEST_OFFSET : LATEST_OFFSET
  }

  /**
   * @public
   * @param {Array<Object>} topics
   *                          [
   *                            {
   *                              topic: 'my-topic-name',
   *                              partitions: [{ partition: 0, timestamp: -2 }],
   *                              fromBeginning: false
   *                            }
   *                          ]
   * @returns {Promise<import("../../types").TopicOffsets[]>} example:
   *                          [
   *                            {
   *                              topic: 'my-topic-name',
   *                              partitions: [
   *                                { partition: 0, offset: '1' },
   *                                { partition: 1, offset: '2' },
   *                                { partition: 2, offset: '1' },
   *                              ],
   *                            },
   *                          ]
   *
   * An optional per-partition `timestamp` overrides the topic's timestamp/default, so one request
   * can resolve explicit earliest and latest seeks independently. Zero remains a valid timestamp.
   * Each requested partition carries `currentLeaderEpoch`, the client's current view of that
   * partition's leader epoch from `MetadataCache#leaderEpoch` (-1 when the cache has not seen
   * the partition), letting the broker fence a request built on metadata that has since moved.
   *
   * When the broker does fence it (`FENCED_LEADER_EPOCH`/`UNKNOWN_LEADER_EPOCH`, both retriable),
   * metadata is refreshed here and the original error is rethrown, so the caller's own retrier
   * makes the next attempt with the epoch that refresh produced. The refresh is best-effort: a
   * failing refresh must not replace the retriable error the caller is about to retry on.
   *
   * @param {object[]} topics see above
   * @param {object} [options] private, for a consumer's preparation round
   * @param {import('../producer/generation')} [options.fence] the caller's terminal fence, for the broker lookups
   *   and the metadata refresh a stale leader epoch starts
   * @param {() => Error|null} [options.guard] the caller's immutable identity: checked before each
   *   request is written and before any side effect of a failure, so a refused caller causes none
   * @returns {Promise<object[]>} the offsets per topic
   * @throws {Error} the guard's refusal, when it refuses
   */
  async fetchTopicsOffset(topics, { fence, guard } = {}) {
    const partitionsPerBroker = {}
    const topicConfigurations = dictionary()

    const addDefaultOffset = (topic) => (partition) => {
      const { timestamp } = topicConfigurations[topic]
      return {
        ...partition,
        timestamp: partition.timestamp ?? timestamp,
        currentLeaderEpoch: this.metadataCache.leaderEpoch(topic, partition.partition),
      }
    }

    // Index all topics and partitions per leader (nodeId)
    for (const topicData of topics) {
      const { topic, partitions, fromBeginning, fromTimestamp } = topicData
      const partitionsPerLeader = this.findLeaderForPartitions(
        topic,
        partitions.map((p) => p.partition)
      )
      const timestamp =
        fromTimestamp != null ? fromTimestamp : this.defaultOffset({ fromBeginning })

      topicConfigurations[topic] = { timestamp }

      keys(partitionsPerLeader).forEach((nodeId) => {
        partitionsPerBroker[nodeId] = partitionsPerBroker[nodeId] || dictionary()
        partitionsPerBroker[nodeId][topic] = partitions.filter((p) =>
          partitionsPerLeader[nodeId].includes(p.partition)
        )
      })
    }

    // Create a list of requests to fetch the offset of all partitions
    const offsetRequests = keys(partitionsPerBroker).map(async (nodeId) => {
      const broker = await this.findBroker({ nodeId, fence })
      const partitions = partitionsPerBroker[nodeId]

      const { responses: topicOffsets } = await broker.listOffsets({
        guard,
        isolationLevel: this.isolationLevel,
        topics: keys(partitions).map((topic) => ({
          topic,
          partitions: partitions[topic].map(addDefaultOffset(topic)),
        })),
      })

      return topicOffsets
    })

    // Execute all requests, merge and normalize the responses
    const responses = await Promise.all(offsetRequests).catch(async (e) => {
      const refusal = guard?.()
      if (refusal) throw refusal

      if (staleLeaderEpoch(e)) {
        // UNKNOWN_LEADER_EPOCH means this client is *ahead* of the broker, so defending the
        // cached epoch makes every retry resend the same rejected value. FENCED_LEADER_EPOCH is
        // the opposite - the client is behind - and a refresh converges on its own, so the epoch
        // is kept there. This catch runs on a Promise.all and has no per-partition information,
        // so it forgets the epoch of every partition in the request; the cost is one unfenced
        // round for partitions that were not at fault, and the next metadata response
        // repopulates them.
        if (e.type === 'UNKNOWN_LEADER_EPOCH') {
          for (const topicData of topics) {
            for (const { partition } of topicData.partitions) {
              this.metadataCache.forgetLeaderEpoch(topicData.topic, partition)
            }
          }
        }

        await this.refreshMetadataBestEffort(fence)
      }

      throw e
    })
    const partitionsPerTopic = responses.flat().reduce(mergeTopics, dictionary())

    return keys(partitionsPerTopic).map((topic) => ({
      topic,
      partitions: partitionsPerTopic[topic].map(({ partition, offset }) => ({
        partition,
        offset,
      })),
    }))
  }

  /**
   * Retrieve the object mapping for committed offsets for a single consumer group
   * @param {object} options
   * @param {string} options.groupId
   * @returns {Object}
   */
  committedOffsets({ groupId }) {
    return this.groupEntry(groupId).offsets
  }

  /**
   * Retrieve the ledger that orders every write to a consumer group's committed offsets. It is
   * shared by every cluster the same `Kafka` instance creates, so a producer's transaction and a
   * consumer's seek or commit for one group see each other.
   * @param {object} options
   * @param {string} options.groupId
   * @returns {OffsetOperations}
   */
  offsetOperations({ groupId }) {
    return this.groupEntry(groupId).ledger
  }

  /**
   * Keeps admin offset work on one shared pair until the last overlapping operation settles.
   * Ordinary consumer and producer cluster holders retain their existing lifetime.
   * @param {string} groupId validated consumer group
   * @param {() => Promise<*>} work offset work including temporary-member cleanup
   * @returns {Promise<*>} original result
   * @throws {Error} original work failure; registry release is synchronous
   */
  async withGroup(groupId, work) {
    let scope = this.groupScopes.get(groupId)
    if (!scope) {
      scope = { count: 0, entry: null }
      scope.entry = this.groupRegistry.acquire(groupId, scope)
      this.groupScopes.set(groupId, scope)
    }
    scope.count += 1
    try {
      return await work()
    } finally {
      if (--scope.count === 0) {
        if (this.groupScopes.get(groupId) === scope) this.groupScopes.delete(groupId)
        if (this.groupEntries.get(groupId) === scope.entry) {
          this.groupEntries.delete(groupId)
          this.groupRegistry.release(groupId, this)
        }
        this.groupRegistry.release(groupId, scope)
      }
    }
  }

  /**
   * Acquires the cache/ledger pair once for this Cluster and group lifetime.
   * @param {string} groupId consumer group
   * @returns {object} the shared pair
   */
  groupEntry(groupId) {
    let entry = this.groupEntries.get(groupId)
    if (!entry) {
      entry = this.groupRegistry.acquire(groupId, this)
      this.groupEntries.set(groupId, entry)
    }
    return entry
  }

  /**
   * Mark offset as committed for a single consumer group's topic-partition
   * @param {object} options
   * @param {string} options.groupId
   * @param {string} options.topic
   * @param {string|number} options.partition
   * @param {string} options.offset
   */
  markOffsetAsCommitted({ groupId, topic, partition, offset }) {
    const committedOffsets = this.committedOffsets({ groupId })

    committedOffsets[topic] = committedOffsets[topic] || {}
    committedOffsets[topic][partition] = offset
  }
}
