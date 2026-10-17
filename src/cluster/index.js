const BrokerPool = require('./brokerPool')
const MetadataCache = require('./metadataCache')
const { CLUSTER_TOPIC_RECREATED } = require('./instrumentationEvents')
const Lock = require('../utils/lock')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const createRetry = require('../retry')
const connectionPoolBuilder = require('./connectionPoolBuilder')
const OffsetOperations = require('../consumer/offsetManager/offsetOperations')
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

const { keys } = Object

/** @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java */
const LEADER_NOT_AVAILABLE = 5

const mergeTopics = (obj, { topic, partitions }) => ({
  ...obj,
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

const PRIVATE = {
  CONNECT: Symbol('private:Cluster:connect'),
  REFRESH_METADATA: Symbol('private:Cluster:refreshMetadata'),
  REFRESH_METADATA_IF_NECESSARY: Symbol('private:Cluster:refreshMetadataIfNecessary'),
  FIND_CONTROLLER_BROKER: Symbol('private:Cluster:findControllerBroker'),
}

module.exports = class Cluster {
  /**
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
    this.retrier = createRetry(retry)
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
      enforceRequestTimeout,
      maxInFlightRequests,
      maxResponseSize,
      reauthenticationThreshold,
    })

    this.targetTopics = new Set()
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
    this.metadataCache = new MetadataCache({ logger: rootLogger, instrumentationEmitter })
    this.brokerPool = new BrokerPool({
      connectionPoolBuilder: this.connectionPoolBuilder,
      logger: this.rootLogger,
      metadataCache: this.metadataCache,
      instrumentationEmitter,
      retry,
      allowAutoTopicCreation,
      authenticationTimeout,
      metadataMaxAge,
      metadataRecoveryStrategy,
      metadataRecoveryRebootstrapTriggerMs,
    })
    this.committedOffsetsByGroup = offsets
    this.operationsByGroup = operations

    this[PRIVATE.CONNECT] = sharedPromiseTo(async () => {
      return await this.brokerPool.connect()
    })

    this[PRIVATE.REFRESH_METADATA] = sharedPromiseTo(async () => {
      return await this.brokerPool.refreshMetadata(Array.from(this.targetTopics))
    })

    this[PRIVATE.REFRESH_METADATA_IF_NECESSARY] = sharedPromiseTo(async () => {
      return await this.brokerPool.refreshMetadataIfNecessary(Array.from(this.targetTopics))
    })

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
   * @public
   * @returns {Promise<void>}
   */
  async connect() {
    await this[PRIVATE.CONNECT]()
  }

  /**
   * @public
   * @returns {Promise<void>}
   */
  async disconnect() {
    await this.brokerPool.disconnect()
  }

  /**
   * @public
   * @param {object} destination
   * @param {String} destination.host
   * @param {Number} destination.port
   */
  removeBroker({ host, port }) {
    this.brokerPool.removeBroker({ host, port })
  }

  /**
   * @public
   * @returns {Promise<void>}
   */
  async refreshMetadata() {
    await this[PRIVATE.REFRESH_METADATA]()
  }

  /**
   * @public
   * @returns {Promise<void>}
   */
  async refreshMetadataIfNecessary() {
    await this[PRIVATE.REFRESH_METADATA_IF_NECESSARY]()
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
   * @returns {Promise<{ resolved: string[], unresolved: string[] }>} the requested ids, split by
   *   whether the cache can name them now. Duplicates and nulls are dropped from both lists.
   */
  async refreshMetadataForTopicIds(topicIds) {
    const ids = [...new Set(topicIds)].filter((topicId) => topicId != null)

    if (ids.length === 0) {
      return { resolved: [], unresolved: [] }
    }

    let outcome = null

    try {
      outcome = await this.brokerPool.withBroker(async ({ broker }) => {
        const version = broker.negotiatedVersion(apiKeys.Metadata, requests.Metadata)

        if (version == null || version < 12) {
          return TOO_OLD
        }

        return await this.fetchMetadataByTopicIds(broker, ids)
      })

      if (outcome === TOO_OLD) {
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
   * @returns {Promise<'resolved'|'failed'>} whether the client managed to ask at all
   */
  async fetchMetadataByTopicIds(broker, ids) {
    try {
      this.metadataCache.updateTopicsById(
        await broker.metadata(ids.map((topicId) => ({ topicId })))
      )
      return RESOLVED
    } catch (e) {
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
        try {
          this.metadataCache.updateTopicsById(await broker.metadata([{ topicId }]))
        } catch (topicError) {
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
   * @return {Promise}
   */
  async addMultipleTargetTopics(topics) {
    await this.mutatingTargetTopics.acquire()

    try {
      const previousSize = this.targetTopics.size
      const previousTopics = new Set(this.targetTopics)
      for (const topic of topics) {
        this.targetTopics.add(topic)
      }

      const hasChanged = previousSize !== this.targetTopics.size || !this.brokerPool.metadata

      if (hasChanged) {
        try {
          await this.refreshMetadata()
        } catch (e) {
          if (
            e.type === 'INVALID_TOPIC_EXCEPTION' ||
            e.type === 'UNKNOWN_TOPIC_OR_PARTITION' ||
            e.type === 'TOPIC_AUTHORIZATION_FAILED'
          ) {
            this.targetTopics = previousTopics
          }

          throw e
        }
      }
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
   * @returns {Promise<void>}
   */
  async refreshMetadataBestEffort() {
    // `Promise.resolve(...)` tolerates a test double that returns `undefined` instead of a
    // promise (several specs stub `refreshMetadata` with a bare `jest.fn()`); the real,
    // production `refreshMetadata()` is an `async` method and always returns a genuine promise,
    // so this changes nothing there.
    await Promise.resolve(this.refreshMetadata()).catch((refreshError) => {
      this.logger.debug('Best-effort metadata refresh failed, continuing with the original error', {
        error: refreshError.message,
      })
    })
  }

  /**
   * @public
   * @param {object} options
   * @param {string} options.nodeId
   * @returns {Promise<import("../../types").Broker>}
   */
  async findBroker({ nodeId }) {
    try {
      return await this.brokerPool.findBroker({ nodeId })
    } catch (e) {
      // The client probably has stale metadata
      if (
        e.name === 'KafkaJSBrokerNotFound' ||
        e.name === 'KafkaJSLockTimeout' ||
        e.name === 'KafkaJSConnectionError'
      ) {
        await this.refreshMetadataBestEffort()
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
   * @returns {Promise<import("../../types").Broker>}
   * @throws {Error} the seed broker's connection error, when no broker connects
   */
  async findConnectedBroker() {
    return await this.brokerPool.findConnectedBroker()
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
      throw new KafkaJSTopicMetadataNotLoaded('Topic metadata not loaded', { topic })
    }

    const topicMetadata = metadata.topicMetadata.find((t) => t.topic === topic)
    return topicMetadata ? topicMetadata.partitionMetadata : []
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
   * @returns {Promise<boolean>} `false` when the hint was rejected - a non-advancing epoch, a
   *   `leaderId` of `-1` (no hint offered), or an unknown topic/partition - `true` when it was
   *   applied
   */
  async applyLeaderHint({ topic, partition, leaderId, leaderEpoch, nodeEndpoints }) {
    // Mirrors MetadataCache#applyLeaderHint's own no-hint-offered guard, so this pre-check can
    // never itself pay for the endpoint work below on the broker's `{ -1, -1 }` default.
    if (leaderId === -1) {
      return false
    }

    const cachedLeaderEpoch = this.metadataCache.leaderEpoch(topic, partition)

    if (cachedLeaderEpoch > -1 && leaderEpoch <= cachedLeaderEpoch) {
      return false
    }

    await this.brokerPool.applyNodeEndpoints(nodeEndpoints)
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
        throw new KafkaJSError('Invalid partition metadata', { topic, partitionId, metadata })
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
   * @returns {Promise<import("../../types").Broker>}
   */
  async findGroupCoordinator({ groupId, coordinatorType = COORDINATOR_TYPES.GROUP }) {
    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        const { coordinator } = await this.findGroupCoordinatorMetadata({
          groupId,
          coordinatorType,
        })

        if (!this.brokerPool.hasNode(coordinator.nodeId)) {
          await this.brokerPool.applyNodeEndpoints([{ ...coordinator, rack: null }])
        }

        return await this.findBroker({ nodeId: coordinator.nodeId })
      } catch (e) {
        // A new broker can join the cluster before we have the chance
        // to refresh metadata
        if (e.name === 'KafkaJSBrokerNotFound' || e.type === 'GROUP_COORDINATOR_NOT_AVAILABLE') {
          this.logger.debug(`${e.message}, refreshing metadata and trying again...`, {
            groupId,
            retryCount,
            retryTime,
          })

          await this.refreshMetadataBestEffort()
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
   * @returns {Promise<Object>}
   * @throws {KafkaJSBrokerNotFound} when the seed broker's connection failed or timed out
   * @throws {KafkaJSGroupCoordinatorNotFound} when no broker answered
   */
  async findGroupCoordinatorMetadata({ groupId, coordinatorType }) {
    const findCoordinator = async ({ nodeId, broker }) => {
      return await this.retrier(async (bail, retryCount, retryTime) => {
        try {
          const brokerMetadata = await broker.findGroupCoordinator({ groupId, coordinatorType })
          this.logger.debug('Found group coordinator', {
            broker: brokerMetadata.host,
            nodeId: brokerMetadata.coordinator.nodeId,
          })
          return brokerMetadata
        } catch (e) {
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
      brokerMetadata = await this.brokerPool.withBroker(findCoordinator)
    } else {
      const broker = await this.brokerPool.findConnectedBroker()
      brokerMetadata = await findCoordinator({ nodeId: broker.nodeId, broker }).catch((e) => {
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
   *                              partitions: [{ partition: 0 }],
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
   * Each requested partition carries `currentLeaderEpoch`, the client's current view of that
   * partition's leader epoch from `MetadataCache#leaderEpoch` (-1 when the cache has not seen
   * the partition), letting the broker fence a request built on metadata that has since moved.
   *
   * When the broker does fence it (`FENCED_LEADER_EPOCH`/`UNKNOWN_LEADER_EPOCH`, both retriable),
   * metadata is refreshed here and the original error is rethrown, so the caller's own retrier
   * makes the next attempt with the epoch that refresh produced. The refresh is best-effort: a
   * failing refresh must not replace the retriable error the caller is about to retry on.
   */
  async fetchTopicsOffset(topics) {
    const partitionsPerBroker = {}
    const topicConfigurations = {}

    const addDefaultOffset = (topic) => (partition) => {
      const { timestamp } = topicConfigurations[topic]
      return {
        ...partition,
        timestamp,
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
        partitionsPerBroker[nodeId] = partitionsPerBroker[nodeId] || {}
        partitionsPerBroker[nodeId][topic] = partitions.filter((p) =>
          partitionsPerLeader[nodeId].includes(p.partition)
        )
      })
    }

    // Create a list of requests to fetch the offset of all partitions
    const offsetRequests = keys(partitionsPerBroker).map(async (nodeId) => {
      const broker = await this.findBroker({ nodeId })
      const partitions = partitionsPerBroker[nodeId]

      const { responses: topicOffsets } = await broker.listOffsets({
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

        await this.refreshMetadataBestEffort()
      }

      throw e
    })
    const partitionsPerTopic = responses.flat().reduce(mergeTopics, {})

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
    if (!this.committedOffsetsByGroup.has(groupId)) {
      this.committedOffsetsByGroup.set(groupId, {})
    }

    return this.committedOffsetsByGroup.get(groupId)
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
    if (!this.operationsByGroup.has(groupId)) {
      this.operationsByGroup.set(groupId, new OffsetOperations())
    }

    return this.operationsByGroup.get(groupId)
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
