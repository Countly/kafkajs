const createRetry = require('../../retry')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const ProducerStateManager = require('../state')
const { transactionStateFor } = require('../state/errorClassifier')
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

/**
 * The error a request made inside a transaction surfaces with, and arms: a retriable error that ran
 * out of retries, and an error that is not this client's own - a `TypeError` while encoding a
 * record or an offset, say - become a `KafkaJSTransactionAbortableError` with it as the cause: the
 * transaction can only be aborted now, and the producer stays usable (KIP-1050). Every other error
 * is surfaced as it is, so its category decides, and an invariant violation stays fatal. The Java
 * producer encodes a record before it appends it; this client encodes once the batch was queued
 * and numbered, so such a failure can leave a hole in the transaction.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1050%3A+Consistent+error+handling+for+Transactions
 * @param {Error} error
 * @returns {Error}
 */
const surfaceInTransaction = (error) => {
  if (error instanceof KafkaJSNumberOfRetriesExceeded) {
    return new KafkaJSTransactionAbortableError(
      `Ran out of retries inside a transaction, which can now only be aborted: ${error.message}`,
      { cause: error }
    )
  }

  if (!(error instanceof KafkaJSError)) {
    return new KafkaJSTransactionAbortableError(
      `Failed inside a transaction, which can now only be aborted: ${error.message}`,
      { cause: error }
    )
  }

  return error
}

/**
 * @param {{ partitions: unknown[] }[]} topics offsets as given to `sendOffsets`
 * @returns {boolean} whether any topic holds a partition
 */
const hasPartitions = (topics) => topics.some(({ partitions }) => partitions.length > 0)

/**
 * The error a commit whose `EndTxn` outcome is unknown rejects with: the last retriable error of a
 * request whose retries ran out, or whose `retry.maxTotalTime` passed, unwrapped from the retrier's
 * error - a timed-out request, a lost connection, a retriable answer. The coordinator may have
 * committed, so the commit is to be sent again, as the Java producer's `commitTransaction()` is
 * retried after a timeout.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java
 * @param {Error} error what the `EndTxn` retrier rejected with
 * @returns {Error|null} null for an error that settles the commit
 */
const unknownOutcome = (error) =>
  (error instanceof KafkaJSNumberOfRetriesExceeded || error instanceof KafkaJSDeliveryTimeout) &&
  error.cause?.retriable === true
    ? error.cause
    : null

/**
 * The consumer group a transactional offset commit is for: the membership `consumerGroupMetadata`
 * names (KIP-447), or a bare `consumerGroupId`, which commits with no member to fence.
 * @param {{ consumerGroupId?: string, consumerGroupMetadata?: { groupId: string,
 *   generationId: number, memberId: string, groupInstanceId: string|null } }} options
 * @returns {{ groupId: string, generationId: number, memberId: string,
 *   groupInstanceId: string|null }}
 * @throws {KafkaJSNonRetriableError} when both are given, or neither
 */
const consumerGroupOf = ({ consumerGroupId, consumerGroupMetadata }) => {
  if ((consumerGroupId == null) === (consumerGroupMetadata == null)) {
    throw new KafkaJSNonRetriableError(
      'sendOffsets takes exactly one of consumerGroupMetadata and consumerGroupId'
    )
  }

  return (
    consumerGroupMetadata ?? {
      groupId: consumerGroupId,
      generationId: -1,
      memberId: '',
      groupInstanceId: null,
    }
  )
}

/**
 * @typedef {Object} EosManager
 */

/**
 * Manage behavior for an idempotent producer and transactions: a facade over the producer's
 * `ProducerStateManager`, which owns the producer id and epoch, the sequences, the transaction
 * state and its version. The facade keeps the coordinator lookups and the retries of its own
 * requests.
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
 *   retry options: they bound every batch's re-sends, the requests the state manager makes on
 *   the send path, and this facade's own retries (InitProducerId, EndTxn, AddOffsetsToTxn,
 *   TxnOffsetCommit)
 * @returns {EosManager}
 */
const createEosManager = ({
  logger,
  cluster,
  transactionTimeout = 60000,
  transactional,
  transactionalId,
  retry = cluster.retry,
}) => {
  if (transactional && !transactionalId) {
    throw new KafkaJSNonRetriableError('Cannot manage transactions without a transactionalId')
  }

  const retrier = createRetry(retry)

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
    // Asks for no metadata first, as the Java producer: a non-transactional InitProducerId goes to
    // any connected broker, and a transactional one to its coordinator.
    findBroker: () =>
      transactional ? findTransactionCoordinator() : cluster.findConnectedBroker(),
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

  /** Whether a KIP-360 epoch bump is owed before the next transaction; kept until one succeeds. */
  let bumpOwed = false
  let bumping = null

  /**
   * Performs the epoch bump owed, if any: one re-initialization shared by concurrent callers. A
   * failure keeps the bump owed, so the next transaction tries it again.
   * @returns {Promise<void>}
   */
  const bumpIfOwed = () => {
    if (!bumpOwed) {
      return Promise.resolve()
    }

    bumping =
      bumping ||
      retryingInitProducerId(() => stateManager.reinitialize())
        .then(() => {
          bumpOwed = false
        })
        .finally(() => {
          bumping = null
        })
    return bumping
  }

  /**
   * Arms the state a failed transactional request leaves, from its already classified error, and
   * returns the error the request rejects with: the armed one - a `KafkaJSTransactionAbortableError`
   * or a `KafkaJSApplicationRecoverableError` with the request's error as its cause - or, when
   * nothing could be armed, the request's own.
   * @param {Error} error
   * @returns {Error}
   */
  const armWith = (error) => {
    const surfaced = surfaceInTransaction(error)
    stateManager.arm(surfaced)
    return stateManager.transaction.armedError() || surfaced
  }

  /**
   * Swallows a failure inside `abort()` - an abort always ends the transaction (KIP-1050) - unless
   * it leaves the producer unusable, which arms `FATAL_ERROR` and is thrown.
   * @param {Error} error
   * @returns {void}
   * @throws {import('../../errors').KafkaJSApplicationRecoverableError} the armed error
   */
  const swallowUnlessFatal = (error) => {
    stateManager.classify(error, 'txn')
    if (transactionStateFor(error) === STATES.FATAL_ERROR) {
      stateManager.arm(error)
      throw stateManager.transaction.armedError()
    }

    logger.warn(`Ignoring a failure while aborting the transaction: ${error.message}`, {
      transactionalId,
    })
  }

  /**
   * Ends the transaction with one EndTxn at the version the transaction began under, retried while
   * the coordinator moves or is busy, and under transaction version 2 adopts the producer id and
   * epoch it returns (KIP-890), which restarts every sequence. A transaction that never produced
   * and never sent offsets ends locally with no EndTxn, under either version. No EndTxn goes out
   * while a batch is in flight, as the Java producer holds it until every batch completed: the
   * caller waited for the transaction's sends, so only a fault in the send loop leaves one. The
   * coordinator is looked up again on every attempt, and a lookup whose own retries ran out ends
   * the attempts with that error as it is.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {boolean} committed
   * @returns {Promise<boolean>} whether a new producer id and epoch were adopted
   * @throws {KafkaJSInvariantViolation} while a batch is in flight; nothing is sent
   * @throws {Error} the EndTxn's error, classified; `KafkaJSNumberOfRetriesExceeded` once the
   *   retries run out
   */
  const endTransaction = async (committed) => {
    if (!stateManager.transactionStarted()) {
      logger.debug('No partitions or offsets registered, not sending EndTxn')
      return false
    }

    if (stateManager.sequencer.hasInFlight()) {
      throw new KafkaJSInvariantViolation('EndTxn with a batch of the transaction in flight')
    }

    const transactionVersion = stateManager.transactionVersion()
    const ended = await retrier(async (bail) => {
      try {
        const coordinator = await findTransactionCoordinator()
        return await coordinator.endTxn({
          producerId: stateManager.producerId,
          producerEpoch: stateManager.producerEpoch,
          transactionalId,
          transactionResult: committed,
          transactionVersion,
        })
      } catch (e) {
        stateManager.classify(e, 'txn')
        if (e.retriable) {
          throw e
        }
        bail(e)
      }
    })

    if (transactionVersion === 2 && ended.producerId != null && String(ended.producerId) !== '-1') {
      stateManager.adopt({ producerId: ended.producerId, producerEpoch: ended.producerEpoch })
      return true
    }
    return false
  }

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

    /**
     * @returns {string|undefined} the transactional id this manager was built with
     */
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
     * Begin a transaction. Resolves the transaction coordinator and reads the transaction version
     * it finalizes, fixed for this transaction. When that version moved from 1 to 2, or an epoch
     * bump is still owed from an earlier abort, the producer id is re-initialized first (KIP-360);
     * a failure there rejects the call, leaves the producer `READY` and keeps the bump owed for the
     * next call. A failed lookup rejects the call the same way.
     * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
     * @returns {Promise<void>}
     * @throws {import('../../errors').KafkaJSNonRetriableError} outside `READY`, or on a producer
     *   that is not transactional
     * @throws {Error} the coordinator lookup's error, or the re-initialization's; the producer
     *   stays `READY` (`FATAL_ERROR` when the re-initialization failed fatally)
     */
    async beginTransaction() {
      stateManager.transaction.guard('beginTransaction')
      transactionalGuard()
      const coordinator = await findTransactionCoordinator()
      if (stateManager.version.begin(coordinator).bumpRequired) {
        bumpOwed = true
      }
      await bumpIfOwed()
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
     * Commit the ongoing transaction, once every AddPartitionsToTxn it started has settled. The
     * caller waits for the transaction's own sends first: nothing of it may still be in flight. A
     * partition whose sequence is still unresolved - a batch the broker may or may not hold - makes
     * the transaction abort-only first, so no EndTxn commits over it.
     *
     * An EndTxn whose outcome is unknown - its retries ran out on a retriable answer, a timeout or
     * a lost connection - leaves the commit pending: the transaction stays `COMMITTING` and the
     * next `commit()` sends the same EndTxn again, with the same producer id and epoch. A commit
     * that had landed is answered with success then (under transaction version 2 with the epoch to
     * adopt); one the coordinator aborted on its timeout meanwhile is refused with
     * `INVALID_TXN_STATE` (48) or `PRODUCER_FENCED` (90), depending on the transaction version and
     * on whether the broker has KAFKA-20785 (fixed in 4.3.2 and 4.5.0), which makes the producer
     * unusable.
     * @see https://github.com/apache/kafka/blob/4.3/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala
     * @returns {Promise<void>}
     * @throws {Error} the last retriable error itself (`retriable: true`) when the outcome is
     *   unknown; the commit is pending
     * @throws {import('../../errors').KafkaJSTransactionAbortableError} the armed error when the
     *   transaction can only be aborted; an abortable EndTxn answer arms `ABORTABLE_ERROR` and
     *   rejects with it
     * @throws {import('../../errors').KafkaJSApplicationRecoverableError} the armed error once the
     *   producer is unusable, a fatal EndTxn answer included
     */
    async commit() {
      stateManager.transaction.guard('commit')
      transactionalGuard()
      await stateManager.whenPartitionsAdded()
      stateManager.resolveSequences()
      stateManager.transaction.guard('commit')
      stateManager.transitionTo(STATES.COMMITTING)

      try {
        await endTransaction(true)
      } catch (error) {
        const unknown = unknownOutcome(error)
        if (unknown !== null) {
          stateManager.transaction.holdCommit()
          throw unknown
        }
        throw armWith(error)
      }

      stateManager.offsets.publish(cluster)
      stateManager.transitionTo(STATES.READY)
    },

    /**
     * Abort the ongoing transaction, once every AddPartitionsToTxn it started has settled. The
     * caller first rejects the transaction's queued batches and waits for those in flight. A
     * partition whose sequence is still unresolved makes the transaction abort-only before the
     * abort decides on the epoch, so it bumps rather than leave the gap. An abort always ends the
     * transaction: a failure inside it is logged and swallowed, unless it leaves the producer
     * unusable (`FATAL_ERROR`), and it never rejects with `KafkaJSTransactionAbortableError`
     * (KIP-1050). An abort that ends `ABORTABLE_ERROR`, or whose caller dropped queued batches that
     * already carried a sequence, then bumps the producer epoch (KIP-360), so the next transaction
     * starts with no sequence left over from this one - unless its EndTxn handed back a new epoch
     * (transaction version 2), which restarts every sequence already. An abort whose EndTxn failed
     * bumps the epoch under either version: the coordinator
     * may still hold the transaction, or have fenced its epoch, and an InitProducerId with the
     * current id and epoch aborts what is left of it. A bump that fails is owed to the next
     * transaction, and the abort resolves with the producer `READY`; only a bump that fails fatally
     * rejects it, after a successful EndTxn, with the producer in `FATAL_ERROR`.
     * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
     * @param {object} [options]
     * @param {boolean} [options.droppedSequences=false] whether the caller dropped queued batches
     *   that had already been sent, so their sequences are spent
     * @returns {Promise<void>}
     * @throws {import('../../errors').KafkaJSApplicationRecoverableError} when the abort itself
     *   failed fatally, or a fatal answer armed `FATAL_ERROR` while it waited
     */
    async abort({ droppedSequences = false } = {}) {
      stateManager.transaction.guard('abort')
      transactionalGuard()
      stateManager.resolveSequences()
      const bump = droppedSequences || stateManager.state() === STATES.ABORTABLE_ERROR
      stateManager.transitionTo(STATES.ABORTING)
      await stateManager.whenPartitionsAdded()
      if (stateManager.state() === STATES.FATAL_ERROR) {
        throw stateManager.transaction.armedError()
      }

      let ended = true
      let adopted = false
      try {
        adopted = await endTransaction(false)
      } catch (error) {
        swallowUnlessFatal(error)
        ended = false
      }

      stateManager.transitionTo(STATES.READY)

      if (!ended || (bump && !adopted)) {
        bumpOwed = true
        await bumpIfOwed().catch(swallowUnlessFatal)
      }
    },

    /**
     * @returns {boolean} whether a producer id is held
     */
    isInitialized() {
      return stateManager.isInitialized()
    },

    /**
     * @returns {boolean} whether this manager was built for a transactional producer
     */
    isTransactional() {
      return transactional
    },

    /**
     * @returns {boolean} whether a transaction is open and usable: `IN_TRANSACTION`, so false
     *   while it commits or aborts and in either error state
     */
    isInTransaction() {
      return stateManager.state() === STATES.IN_TRANSACTION
    },

    /**
     * Mark the provided offsets as participating in the transaction for the given consumer group,
     * so they are committed only if the transaction is.
     *
     * Under transaction version 1 an AddOffsetsToTxn goes to the transaction coordinator first;
     * under version 2 the TxnOffsetCommit (v5) adds the offsets itself (KIP-890). The TxnOffsetCommit
     * carries the consumer's generation, member id and instance id from `consumerGroupMetadata`
     * (KIP-447), so the group coordinator fences a stale or zombie member; `consumerGroupId` alone
     * sends -1, "" and null, which commits with no member to fence, as 2.2.4 did. Each request is
     * retried while it fails retriably, a moved group coordinator looked up again. Any other failure
     * arms the state its category calls for - a stale generation or member (ILLEGAL_GENERATION,
     * UNKNOWN_MEMBER_ID, FENCED_INSTANCE_ID, STALE_MEMBER_EPOCH) arms `ABORTABLE_ERROR`: abort, let
     * the consumer rejoin, reprocess - and rejects with it.
     *
     * Under version 2 offsets holding no partition are skipped, as the Java producer skips them:
     * nothing is sent and the transaction is not marked started, since the broker starts none for
     * them and an `EndTxn` on an empty transaction fails. Under version 1 the AddOffsetsToTxn is
     * still sent, as in 2.2.4.
     * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
     * @param {object} options
     * @param {import('../../../types').ConsumerGroupMetadata} [options.consumerGroupMetadata]
     * @param {string} [options.consumerGroupId]
     * @param {OffsetCommitTopic[]} options.topics
     * @returns {Promise<void>}
     * @throws {KafkaJSNonRetriableError} when both `consumerGroupMetadata` and `consumerGroupId`
     *   are given, or neither; nothing is armed then
     *
     * @typedef {Object} OffsetCommitTopic
     * @property {string} topic
     * @property {OffsetCommitTopicPartition[]} partitions
     *
     * @typedef {Object} OffsetCommitTopicPartition
     * @property {number} partition
     * @property {string} offset
     * @property {number} [leaderEpoch] the committed leader epoch; -1 when absent
     * @property {string|null} [metadata]
     */
    async sendOffsets({ consumerGroupId, consumerGroupMetadata, topics } = {}) {
      stateManager.transaction.guard('sendOffsets')
      const { groupId, generationId, memberId, groupInstanceId } = consumerGroupOf({
        consumerGroupId,
        consumerGroupMetadata,
      })
      assert(topics, 'Missing offset topics')
      const snapshot = stateManager.offsets.snapshot(
        cluster.offsetOperations({ groupId }),
        groupId,
        topics
      )
      return stateManager.offsets.enqueue(snapshot, async () => {
        const transactionVersion = stateManager.transactionVersion()

        try {
          stateManager.transaction.guard('sendOffsets')
          if (transactionVersion === 2 && !hasPartitions(snapshot.topics)) {
            stateManager.offsets.stage(snapshot)
            return
          }

          if (transactionVersion === 1) {
            await retrier(async (bail, retryCount, retryTime) => {
              const transactionCoordinator = await findTransactionCoordinator()
              try {
                await transactionCoordinator.addOffsetsToTxn({
                  transactionalId,
                  producerId: stateManager.producerId,
                  producerEpoch: stateManager.producerEpoch,
                  groupId,
                })
              } catch (e) {
                stateManager.classify(e, 'txn')
                if (e.retriable) {
                  logger.debug('AddOffsetsToTxn failed retriably, retrying', {
                    error: e.message,
                    transactionalId,
                    retryCount,
                    retryTime,
                  })
                  throw e
                }
                bail(e)
              }
            })
          }

          stateManager.markOffsetsAdded()

          let groupCoordinator = await cluster.findGroupCoordinator({
            groupId,
            coordinatorType: COORDINATOR_TYPES.GROUP,
          })
          await retrier(async (bail, retryCount, retryTime) => {
            try {
              await groupCoordinator.txnOffsetCommit({
                transactionalId,
                producerId: stateManager.producerId,
                producerEpoch: stateManager.producerEpoch,
                groupId,
                topics: snapshot.topics,
                generationId,
                memberId,
                groupInstanceId,
                transactionVersion,
              })
            } catch (e) {
              const { category } = stateManager.classify(e, 'offsets')
              if (e.retriable) {
                logger.debug('TxnOffsetCommit failed retriably, retrying', {
                  error: e.message,
                  transactionalId,
                  retryCount,
                  retryTime,
                })
                if (category === 'refreshRetriable' || e.code === 'ECONNREFUSED') {
                  groupCoordinator = await cluster.findGroupCoordinator({
                    groupId,
                    coordinatorType: COORDINATOR_TYPES.GROUP,
                  })
                }
                throw e
              }
              bail(e)
            }
          })
          stateManager.offsets.stage(snapshot)
        } catch (error) {
          stateManager.offsets.abandon(snapshot)
          throw armWith(error)
        }
      })
    },
  }

  return eosManager
}

createEosManager.surfaceInTransaction = surfaceInTransaction

module.exports = createEosManager
