const BrokerPool = require('./brokerPool')
const MetadataCache = require('./metadataCache')
const { CLUSTER_TOPIC_RECREATED } = require('./instrumentationEvents')
const Lock = require('../utils/lock')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const createRetry = require('../retry')
const connectionPoolBuilder = require('./connectionPoolBuilder')
const { EARLIEST_OFFSET, LATEST_OFFSET } = require('../constants')
const {
  KafkaJSError,
  KafkaJSBrokerNotFound,
  KafkaJSMetadataNotLoaded,
  KafkaJSTopicMetadataNotLoaded,
  KafkaJSGroupCoordinatorNotFound,
} = require('../errors')
const COORDINATOR_TYPES = require('../protocol/coordinatorTypes')

const { keys } = Object

const mergeTopics = (obj, { topic, partitions }) => ({
  ...obj,
  [topic]: [...(obj[topic] || []), ...partitions],
})

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
   * @public
   * @returns {Promise<import("../../types").BrokerMetadata>}
   */
  async metadata({ topics = [] } = {}) {
    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.brokerPool.refreshMetadataIfNecessary(topics)
        return this.brokerPool.withBroker(async ({ broker }) => broker.metadata(topics))
      } catch (e) {
        if (e.type === 'LEADER_NOT_AVAILABLE') {
          throw e
        }

        bail(e)
      }
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
   * @public
   * @param {string} topic
   * @param {(number|string)[]} partitions
   * @returns {Object} Object with leader and partitions. For partitions 0 and 5
   *                   the result could be:
   *                     { '0': [0], '2': [5] }
   *
   *                   where the key is the nodeId.
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

      const { leader } = metadata
      const current = result[leader] || []
      return { ...result, [leader]: [...current, partitionId] }
    }, {})
  }

  /**
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
   * @public
   * @param {object} params
   * @param {string} params.groupId
   * @param {import("../protocol/coordinatorTypes").CoordinatorType} [params.coordinatorType=0]
   * @returns {Promise<Object>}
   */
  async findGroupCoordinatorMetadata({ groupId, coordinatorType }) {
    const brokerMetadata = await this.brokerPool.withBroker(async ({ nodeId, broker }) => {
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
    })

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
   */
  async fetchTopicsOffset(topics) {
    const partitionsPerBroker = {}
    const topicConfigurations = {}

    const addDefaultOffset = (topic) => (partition) => {
      const { timestamp } = topicConfigurations[topic]
      return { ...partition, timestamp }
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
    const requests = keys(partitionsPerBroker).map(async (nodeId) => {
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
    const responses = await Promise.all(requests)
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
