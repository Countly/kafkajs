const createRetry = require('../../retry')
const Lock = require('../../utils/lock')
const { KafkaJSNonRetriableError, isStaleCoordinator } = require('../../errors')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const ProducerStateManager = require('../state')
const assert = require('assert')

const STATES = require('./transactionStates')
const INIT_PRODUCER_RETRIABLE_PROTOCOL_ERRORS = [
  'NOT_COORDINATOR_FOR_GROUP',
  'GROUP_COORDINATOR_NOT_AVAILABLE',
  'GROUP_LOAD_IN_PROGRESS',
  /**
   * The producer might have crashed and never committed the transaction; retry the
   * request so Kafka can abort the current transaction
   * @see https://github.com/apache/kafka/blob/201da0542726472d954080d54bc585b111aaf86f/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L1001-L1002
   */
  'CONCURRENT_TRANSACTIONS',
]
const COMMIT_RETRIABLE_PROTOCOL_ERRORS = ['UNKNOWN_TOPIC_OR_PARTITION']
const COORDINATOR_LOAD_IN_PROGRESS_CODE = 14
const CONCURRENT_TRANSACTIONS_CODE = 51

/** Codes 14/15/16 are emitted with legacy type names (see src/protocol/error.js), match on code. */
const isCommitRetriable = (e) =>
  COMMIT_RETRIABLE_PROTOCOL_ERRORS.includes(e.type) || e.code === COORDINATOR_LOAD_IN_PROGRESS_CODE

const isAddOffsetsRetriable = (e) => e.code === CONCURRENT_TRANSACTIONS_CODE

/**
 * @typedef {Object} EosManager
 */

/**
 * Manage behavior for an idempotent producer and transactions: a facade over the producer's
 * `ProducerStateManager`, which owns the producer id and epoch, the sequences, the transaction
 * state and its version. The facade keeps the coordinator lookups, the retries of its own
 * requests and the per-broker mutex of the send path.
 *
 * @param {Object} options
 * @param {import('../../../types').Logger} options.logger
 * @param {import('../../../types').Cluster} options.cluster
 * @param {number} [options.transactionTimeout=60000] Passed as `InitProducerId`'s
 *   `transactionTimeout`.
 * @param {boolean} options.transactional Whether this manager is for a transactional (vs. plain
 *   idempotent) producer; gates the transactional methods (`beginTransaction`, `commit`, ...).
 * @param {string} [options.transactionalId] Required when `transactional` is true.
 * @param {import('../../../types').RetryOptions} [options.retry=cluster.retry] the producer's
 *   retry options: they bound every batch's re-sends and the requests the state manager makes on
 *   the send path
 * @param {import('../../instrumentation/emitter')} [options.instrumentationEmitter] Passed to
 *   the per-broker mutex (see `acquireBrokerLock`) so a waiter timing out emits
 *   `NETWORK_LOCK_TIMEOUT` instead of failing silently.
 * @returns {EosManager}
 */
module.exports = ({
  logger,
  cluster,
  transactionTimeout = 60000,
  transactional,
  transactionalId,
  retry = cluster.retry,
  instrumentationEmitter = null,
}) => {
  if (transactional && !transactionalId) {
    throw new KafkaJSNonRetriableError('Cannot manage transactions without a transactionalId')
  }

  const retrier = createRetry(cluster.retry)

  /**
   * Idempotent production requires a mutex lock per broker to serialize requests with sequence number handling
   */
  let brokerMutexLocks = {}

  const findTransactionCoordinator = () => {
    return cluster.findGroupCoordinator({
      groupId: transactionalId,
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    })
  }

  const stateManager = new ProducerStateManager({
    logger,
    transactional,
    transactionalId,
    transactionTimeout,
    retry,
    findBroker: async () => {
      await cluster.refreshMetadataIfNecessary()
      // If non-transactional we can request the PID from any broker
      return transactional ? findTransactionCoordinator() : cluster.findControllerBroker()
    },
    addPartitionsToTxn: async (topics) => {
      const broker = await findTransactionCoordinator()
      await broker.addPartitionsToTxn({
        transactionalId,
        producerId: stateManager.producerId,
        producerEpoch: stateManager.producerEpoch,
        topics,
      })
    },
  })

  const transactionalGuard = () => {
    if (!transactional) {
      throw new KafkaJSNonRetriableError('Method unavailable if non-transactional')
    }
  }

  /**
   * Runs one InitProducerId - a first one, or a KIP-360 re-initialization - under the facade's
   * retrier, retrying the coordinator answers that clear by themselves and handing every other
   * error to the caller unchanged.
   * @param {() => Promise<void>} request
   * @returns {Promise<void>}
   */
  const retryingInitProducerId = (request) =>
    retrier(async (bail, retryCount, retryTime) => {
      try {
        await request()
      } catch (e) {
        if (INIT_PRODUCER_RETRIABLE_PROTOCOL_ERRORS.includes(e.type)) {
          if (e.type === 'CONCURRENT_TRANSACTIONS') {
            logger.debug('There is an ongoing transaction on this transactionId, retrying', {
              error: e.message,
              stack: e.stack,
              transactionalId,
              retryCount,
              retryTime,
            })
          }

          throw e
        }

        bail(e)
      }
    })

  const eosManager = {
    /**
     * The producer's state: producer id and epoch, sequences, transaction state and version.
     * Read-only; the send path reaches the sequencer through it.
     * @returns {ProducerStateManager}
     */
    get stateManager() {
      return stateManager
    },

    /**
     * Get the current producer id
     * @returns {string|number} -1 before the first `initProducerId()`
     */
    getProducerId() {
      return stateManager.producerId
    },

    /**
     * Get the current producer epoch
     * @returns {number} 0 before the first `initProducerId()`, the epoch a non-idempotent
     *   Produce request carries
     */
    getProducerEpoch() {
      return stateManager.isInitialized() ? stateManager.producerEpoch : 0
    },

    getTransactionalId() {
      return transactionalId
    },

    /**
     * Initialize the idempotent producer by making an `InitProducerId` request.
     * Overwrites any existing state in this transaction manager. A call made with no producer id
     * held keeps one that another call acquired while it was waiting to retry, instead of
     * replacing it, as the Java producer shares one InitProducerId between its callers.
     * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
     * @returns {Promise<void>}
     * @throws {import('../../errors').KafkaJSInvalidConfigurationError} CLUSTER_AUTHORIZATION_FAILED,
     *   naming both remedies
     */
    async initProducerId() {
      stateManager.transaction.guard('initProducerId')
      const held = stateManager.isInitialized()
      await retryingInitProducerId(async () => {
        if (held || !stateManager.isInitialized()) {
          await stateManager.initialize()
        }
      })
      stateManager.transitionTo(STATES.READY)
      brokerMutexLocks = {}

      logger.debug('Initialized producer id & epoch', {
        producerId: stateManager.producerId,
        producerEpoch: stateManager.producerEpoch,
      })
    },

    /**
     * Get the current sequence for a given Topic-Partition. Defaults to 0.
     *
     * @param {string} topic
     * @param {string} partition
     * @returns {number}
     */
    getSequence(topic, partition) {
      if (!eosManager.isInitialized()) {
        return 0
      }

      return stateManager.nextSequence(topic, partition)
    },

    /**
     * Update the sequence for a given Topic-Partition.
     *
     * Do nothing if not yet initialized (not idempotent)
     * @param {string} topic
     * @param {string} partition
     * @param {number} increment
     */
    updateSequence(topic, partition, increment) {
      if (!eosManager.isInitialized()) {
        return
      }

      stateManager.advance(topic, partition, increment)
    },

    /**
     * Begin a transaction. Resolves the transaction coordinator first, so a failed lookup rejects
     * the call and leaves the producer `READY`.
     * @returns {Promise<void>}
     */
    async beginTransaction() {
      stateManager.transaction.guard('beginTransaction')
      transactionalGuard()
      await findTransactionCoordinator()
      stateManager.transitionTo(STATES.IN_TRANSACTION)
    },

    /**
     * Add partitions to a transaction if they are not already marked as participating.
     *
     * Should be called prior to sending any messages during a transaction
     * @param {TopicData[]} topicData
     *
     * @typedef {Object} TopicData
     * @property {string} topic
     * @property {object[]} partitions
     * @property {number} partitions[].partition
     */
    async addPartitionsToTransaction(topicData) {
      stateManager.transaction.guard('addPartitionsToTransaction')
      transactionalGuard()
      const pending = []
      for (const { topic, partitions } of topicData) {
        for (const { partition } of partitions) {
          const added = stateManager.addPartitionToTransaction(topic, partition)
          if (added !== null) {
            pending.push(added)
          }
        }
      }

      await Promise.all(pending)
    },

    /**
     * Commit the ongoing transaction
     */
    async commit() {
      stateManager.transaction.guard('commit')
      transactionalGuard()
      stateManager.transitionTo(STATES.COMMITTING)

      if (!stateManager.transactionStarted()) {
        logger.debug('No partitions or offsets registered, not sending EndTxn')

        stateManager.transitionTo(STATES.READY)
        return
      }

      const broker = await findTransactionCoordinator()
      await broker.endTxn({
        producerId: stateManager.producerId,
        producerEpoch: stateManager.producerEpoch,
        transactionalId,
        transactionResult: true,
      })

      stateManager.transitionTo(STATES.READY)
    },

    /**
     * Abort the ongoing transaction. An abort that ends `ABORTABLE_ERROR` under transaction
     * version 1 then bumps the producer epoch (KIP-360), as the Java producer does, so the next
     * transaction starts with no sequence left over from the failed one.
     * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
     */
    async abort() {
      stateManager.transaction.guard('abort')
      transactionalGuard()
      const recovering = stateManager.state() === STATES.ABORTABLE_ERROR
      stateManager.transitionTo(STATES.ABORTING)

      if (stateManager.transactionStarted()) {
        const broker = await findTransactionCoordinator()
        await broker.endTxn({
          producerId: stateManager.producerId,
          producerEpoch: stateManager.producerEpoch,
          transactionalId,
          transactionResult: false,
        })
      } else {
        logger.debug('No partitions or offsets registered, not sending EndTxn')
      }

      stateManager.transitionTo(STATES.READY)

      if (recovering && stateManager.transactionVersion() === 1) {
        await retryingInitProducerId(() => stateManager.reinitialize())
      }
    },

    /**
     * Whether the producer id has already been initialized
     */
    isInitialized() {
      return stateManager.isInitialized()
    },

    isTransactional() {
      return transactional
    },

    isInTransaction() {
      return stateManager.state() === STATES.IN_TRANSACTION
    },

    /**
     * Acquire this broker's mutex, serializing idempotent requests against it so sequence
     * numbers stay ordered. A wait longer than 65535ms rejects with `KafkaJSLockTimeout` and,
     * if `instrumentationEmitter` was provided, emits `NETWORK_LOCK_TIMEOUT` first.
     * @param {import('../../broker')} broker
     * @returns {Promise<void>}
     */
    async acquireBrokerLock(broker) {
      if (eosManager.isInitialized()) {
        brokerMutexLocks[broker.nodeId] =
          brokerMutexLocks[broker.nodeId] ||
          new Lock({
            timeout: 0xffff,
            description: broker.brokerAddress,
            instrumentationEmitter,
          })
        await brokerMutexLocks[broker.nodeId].acquire()
      }
    },

    releaseBrokerLock(broker) {
      if (eosManager.isInitialized()) brokerMutexLocks[broker.nodeId].release()
    },

    /**
     * Mark the provided offsets as participating in the transaction for the given consumer group.
     *
     * This allows us to commit an offset as consumed only if the transaction passes.
     * @param {string} consumerGroupId The unique group identifier
     * @param {OffsetCommitTopic[]} topics The unique group identifier
     * @returns {Promise}
     *
     * @typedef {Object} OffsetCommitTopic
     * @property {string} topic
     * @property {OffsetCommitTopicPartition[]} partitions
     *
     * @typedef {Object} OffsetCommitTopicPartition
     * @property {number} partition
     * @property {number} offset
     */
    async sendOffsets({ consumerGroupId, topics } = {}) {
      stateManager.transaction.guard('sendOffsets')
      assert(consumerGroupId, 'Missing consumerGroupId')
      assert(topics, 'Missing offset topics')

      // Kafka answers CONCURRENT_TRANSACTIONS while a previous transaction on the same
      // transactional id is still completing and expects the client to retry, the same as
      // initProducerId above. The coordinator lookup happens inside the retrier so a
      // coordinator move is picked up on retry.
      await retrier(async (bail, retryCount, retryTime) => {
        const transactionCoordinator = await findTransactionCoordinator()

        try {
          await transactionCoordinator.addOffsetsToTxn({
            transactionalId,
            producerId: stateManager.producerId,
            producerEpoch: stateManager.producerEpoch,
            groupId: consumerGroupId,
          })
        } catch (e) {
          if (isAddOffsetsRetriable(e)) {
            logger.debug('There is an ongoing transaction on this transactionId, retrying', {
              error: e.message,
              stack: e.stack,
              transactionalId,
              retryCount,
              retryTime,
            })

            throw e
          }

          bail(e)
        }
      })

      stateManager.markOffsetsAdded()

      let groupCoordinator = await cluster.findGroupCoordinator({
        groupId: consumerGroupId,
        coordinatorType: COORDINATOR_TYPES.GROUP,
      })

      return retrier(async (bail, retryCount, retryTime) => {
        try {
          await groupCoordinator.txnOffsetCommit({
            transactionalId,
            producerId: stateManager.producerId,
            producerEpoch: stateManager.producerEpoch,
            groupId: consumerGroupId,
            topics,
          })
        } catch (e) {
          if (isCommitRetriable(e)) {
            logger.debug('Group coordinator is not ready yet, retrying', {
              error: e.message,
              stack: e.stack,
              transactionalId,
              retryCount,
              retryTime,
            })

            throw e
          }

          if (isStaleCoordinator(e) || e.code === 'ECONNREFUSED') {
            logger.debug('Invalid group coordinator, finding new group coordinator and retrying', {
              error: e.message,
              stack: e.stack,
              transactionalId,
              retryCount,
              retryTime,
            })

            groupCoordinator = await cluster.findGroupCoordinator({
              groupId: consumerGroupId,
              coordinatorType: COORDINATOR_TYPES.GROUP,
            })

            throw e
          }

          bail(e)
        }
      })
    },
  }

  return eosManager
}
