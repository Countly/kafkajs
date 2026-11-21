const createRetry = require('../../retry')
const handlerContext = require('../../consumer/handlerContext')
const { ON_WRITE } = require('../../broker/privateOptions')
const {
  KafkaJSBrokerNotFound,
  KafkaJSCommitFailedError,
  KafkaJSDeliveryTimeout,
  KafkaJSError,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProducerClosedError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { usableTopicIds } = require('../../protocol/requests/topicIds')
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

const COMMIT_PENDING =
  'The transaction coordinator could not be found; the commit is pending, call commit() again'

/**
 * The not-found links of failed coordinator lookups' cause chains. A retrier's re-wrap keeps such a
 * link in its own chain, so a boundary can still tell that discovery ran out, and an unrelated
 * `KafkaJSBrokerNotFound` is never mistaken for it. Weak: nothing is retained.
 * @type {WeakSet<Error>}
 */
const exhaustedLookups = new WeakSet()

/**
 * @param {Error} error
 * @returns {Error[]} the error and its `cause` links, outermost first; a loop ends the chain
 */
const causeChain = (error) => {
  const chain = []
  for (let link = error; link != null && !chain.includes(link); link = link.cause) {
    chain.push(link)
  }
  return chain
}

/**
 * Records a failed coordinator lookup whose discovery ran out of its budget: a
 * `KafkaJSGroupCoordinatorNotFound` or a `KafkaJSBrokerNotFound` in its cause chain.
 * @param {Error} error what the lookup rejected with
 * @returns {Error} the same error
 */
const markLookupFailure = (error) => {
  for (const link of causeChain(error)) {
    if (link instanceof KafkaJSGroupCoordinatorNotFound || link instanceof KafkaJSBrokerNotFound) {
      exhaustedLookups.add(link)
    }
  }
  return error
}

/**
 * @param {Error} error
 * @returns {boolean} whether `error` stands for a coordinator lookup whose discovery ran out
 */
const isDiscoveryExhausted = (error) => causeChain(error).some((link) => exhaustedLookups.has(link))

/**
 * The error a call rejects with when coordinator discovery ran out and the call can be made again:
 * retriable, with the discovery error as its `cause`, its class unchanged. It is no category class,
 * so its category is set here: it never arms `FATAL_ERROR`.
 * @param {string} message
 * @param {Error} cause
 * @returns {KafkaJSError}
 */
const discoveryUnavailable = (message, cause) => {
  const error = new KafkaJSError(message, { retriable: true, cause })
  error.category = 'retriable'
  return error
}

/**
 * The error a transactional request rejects with when its coordinator could not be found: the
 * transaction can only be aborted, and the producer stays usable.
 * @param {Error} cause the discovery error
 * @returns {KafkaJSTransactionAbortableError}
 */
const abortOnly = (cause) =>
  new KafkaJSTransactionAbortableError(
    `The transaction can no longer be committed, its coordinator could not be found (${cause.message}); call abort() on it before starting another`,
    { cause }
  )

/**
 * Whether an abort's INVALID_TXN_STATE means there is nothing to abort: no add request of the
 * transaction was confirmed, so the coordinator never started it (EMPTY, or the previous
 * transaction's COMPLETE_COMMIT), as `endTransactionWithTV1` answers. For a confirmed transaction
 * the answer stays a fatal error.
 * @see https://github.com/apache/kafka/blob/4.4.0/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala
 * @param {Error} error what the abort's EndTxn rejected with
 * @param {boolean} confirmed whether the transaction was confirmed to have started
 * @returns {boolean}
 */
const nothingToAbort = (error, confirmed) => !confirmed && error.type === 'INVALID_TXN_STATE'

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
 * The refusal of a transaction that carries offsets of a consumer batch a seek, a topic recreation
 * or the end of its acquisition superseded: the transaction can only be aborted, and the records
 * processed again in a new one. The cause is the commit refusal the consumer gives such a batch.
 * @param {Pick<import('../../consumer/handlerContext').BatchIdentity, 'acquisition'|'acquireClock'>} input
 *   the superseded input
 * @returns {KafkaJSTransactionAbortableError}
 */
const supersededInputError = (input) =>
  new KafkaJSTransactionAbortableError(
    'A consumer batch whose offsets this transaction carries was superseded; call abort() on it and process the records again in a new transaction',
    {
      cause: new KafkaJSCommitFailedError(
        handlerContext.isSought(input)
          ? 'A seek superseded the handler batch'
          : 'The handler outlived its assignment',
        { reason: 'stale-assignment' }
      ),
    }
  )

/**
 * @param {ReadonlyArray<{ topic: string, partitions: ReadonlyArray<{ partition: number|string }> }>} topics
 * @param {{ topic: string, partition: number }} cell
 * @returns {boolean} whether the offsets name the cell's partition
 */
const carries = (topics, { topic, partition }) =>
  topics.some(
    (entry) =>
      entry.topic === topic &&
      entry.partitions.some((offset) => Number(offset.partition) === partition)
  )

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
 *   retry options for the requests of the transaction protocol: the state manager's on the send
 *   path and this facade's own retries (InitProducerId, EndTxn, AddOffsetsToTxn, TxnOffsetCommit,
 *   and the transaction coordinator lookup of a begin and of InitProducerId)
 * @param {number} [options.deliveryRetries] how many times a batch is sent again; `retry.retries`
 *   when absent
 * @param {() => (import('../generation')|undefined)} [options.getGeneration] the producer's current
 *   lifecycle. Each operation reads it once, when it starts, and never again after an await: its
 *   retries stop, and it opens no socket and changes no state, once that lifecycle is closed.
 * @param {(armed: Error) => void} [options.onFenced] private notice that an `InitProducerId` was
 *   fenced (47 or 90), called with the armed error this manager is about to reject with, so the
 *   producer can make its other identity and its lifecycle fatal too
 * @returns {EosManager}
 */
const createEosManager = ({
  logger,
  cluster,
  transactionTimeout = 60000,
  transactional,
  transactionalId,
  retry = cluster.retry,
  deliveryRetries,
  getGeneration = () => undefined,
  onFenced,
}) => {
  if (transactional && !transactionalId) {
    throw new KafkaJSNonRetriableError('Cannot manage transactions without a transactionalId')
  }

  const retrier = createRetry(retry)

  /**
   * @param {import('../generation')} [generation] the producer lifecycle an operation captured
   * @returns {import('../../retry').Retrier} a retrier that stops, with the lifecycle's close
   *   error, when the lifecycle closes; the facade's own without one
   */
  const retrierFor = (generation) =>
    generation ? createRetry({ ...retry, fence: generation }) : retrier

  /** The private options a broker request takes for a captured lifecycle; none without one. */
  const guardOf = (generation) => (generation ? { guard: generation.guard } : {})

  /**
   * @returns {KafkaJSTransactionAbortableError|null} the refusal of this transaction once one of
   *   its input batches was superseded (`TransactionOffsets#supersededInput`)
   */
  const inputRefusal = () => {
    const input = stateManager.offsets.supersededInput()
    return input && supersededInputError(input)
  }

  /**
   * The private options of an AddOffsetsToTxn or TxnOffsetCommit attempt: the lifecycle's guard,
   * and, while the transaction holds input dependencies, their refusal too, so a request is not
   * written once an input was superseded.
   * @param {import('../generation')} [generation]
   * @returns {{ guard?: (frameLength?: number) => Error|null }}
   */
  const offsetGuardOf = (generation) =>
    stateManager.offsets.inputs === null
      ? guardOf(generation)
      : { guard: () => generation?.guard() || inputRefusal() }

  /**
   * Runs immediately before a commit EndTxn that carries input dependencies is written: refuses
   * the write once an input was superseded, else drops the dependencies, since from this write on
   * the commit may have happened and a later seek must not turn it, or its retry, into an abort.
   * @returns {void}
   * @throws {KafkaJSTransactionAbortableError} the refusal; the request is not written
   */
  const sealInputs = () => {
    const refusal = inputRefusal()
    if (refusal) throw refusal
    stateManager.offsets.sealInputs()
  }

  /**
   * Re-reads the metadata the producer's cluster tracks after the broker answered UNKNOWN_TOPIC_ID,
   * so the next attempt reads the id a recreated topic has now from the cache `cluster.topicId`
   * reads. It never creates a topic and registers none: an offset commit's topics are not targets
   * of this producer, and one that became a target here would stay eligible for automatic creation
   * in every later refresh. A topic whose id the cache holds is already a target, so it is
   * refreshed with the rest. The refresh is guarded, so it shares no promise with the send path's
   * refreshes, which may create a topic. Best effort: a failed refresh leaves the original error
   * to the retrier, but a closed lifecycle ends the attempt with its close error and publishes
   * nothing.
   * @param {import('../generation')} [generation] the lifecycle the call captured
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   */
  const refreshWithoutCreating = async (generation) => {
    try {
      generation?.check()
      await cluster.refreshMetadata({
        allowAutoTopicCreation: false,
        ...(generation ? { fence: generation } : {}),
        ...guardOf(generation),
      })
    } catch (error) {
      generation?.check()
      logger.debug('Could not refresh the metadata after an unknown topic id', {
        error: error.message,
      })
    }
    generation?.check()
  }

  /**
   * One coordinator lookup under the cluster's own retries. A failure whose discovery ran out is
   * recorded on a side branch, so the boundary that sees it decides; the caller gets the lookup's
   * own promise, which keeps its timing.
   * @param {string} groupId the group, or the transactional id
   * @param {import('../../protocol/coordinatorTypes').CoordinatorType} coordinatorType
   * @param {import('../generation')} [generation] the lifecycle the caller captured
   * @returns {Promise<import('../../../types').Broker>} the coordinator
   * @throws {Error} the lookup's error, as it is
   */
  const findCoordinator = (groupId, coordinatorType, generation) => {
    const lookup = cluster.findGroupCoordinator({
      groupId,
      coordinatorType,
      ...(generation ? { fence: generation } : {}),
    })
    lookup?.catch?.(markLookupFailure)
    return lookup
  }

  /**
   * One lookup of this producer's transaction coordinator, as `findCoordinator` makes it.
   * @param {import('../generation')} [generation] the lifecycle the caller captured
   * @returns {Promise<import('../../../types').Broker>} the coordinator
   * @throws {Error} the lookup's error, as it is
   */
  const findTransactionCoordinator = (generation) =>
    findCoordinator(transactionalId, COORDINATOR_TYPES.TRANSACTION, generation)

  /**
   * Carries on a transaction coordinator lookup of a begin or an InitProducerId whose first
   * attempt, made by the caller, failed. A retriable failure, a connection reset in the lookup's
   * own connect step above all, is retried under the producer's retry settings, as `endTransaction`
   * retries its lookup; 2.2.4 retried this lookup inside `send()`. The caller makes the first
   * attempt itself, so a lookup that succeeds allocates nothing for the retrier; that attempt's
   * failure stands for the retrier's first, and the `maxTotalTime` clock starts here.
   * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {Error} failure what the first attempt rejected with
   * @param {import('../generation')} [generation] the lifecycle the caller captured: its close
   *   ends the retries
   * @returns {Promise<import('../../../types').Broker>} the coordinator
   * @throws {Error} a failure that is not retriable, as it is - the lookup's own spent retries
   *   and a coordinator that was not found included, so the budgets do not nest; the lifecycle's
   *   close error once it closed; `KafkaJSNumberOfRetriesExceeded` (`cause`: the last error) or
   *   `KafkaJSDeliveryTimeout` once this retrier gave up
   */
  const retryTransactionCoordinator = async (failure, generation) => {
    generation?.check()
    if (!failure.retriable) throw failure

    let failed = failure
    return retrierFor(generation)(async (bail) => {
      try {
        if (failed) throw failed
        return await findTransactionCoordinator(generation)
      } catch (e) {
        failed = null
        generation?.check()
        if (e.retriable) throw e
        bail(e)
      }
    })
  }

  /**
   * The transaction coordinator for an InitProducerId - a first one or an epoch bump - its lookup
   * retried as `retryTransactionCoordinator` does. A discovery that ran out rejects with a
   * retriable error before the state manager sees it, so it arms nothing: the producer stays as it
   * was, an owed bump stays owed, and the next call tries again.
   * @param {import('../generation')} [generation] the lifecycle the caller captured
   * @returns {Promise<import('../../../types').Broker>} the coordinator
   * @throws {KafkaJSError} retriable, `cause` the discovery error, when discovery ran out
   * @throws {Error} any other failure, as `retryTransactionCoordinator` does
   */
  const lookUpTransactionCoordinator = async (generation) => {
    try {
      return await findTransactionCoordinator(generation)
    } catch (error) {
      return retryTransactionCoordinator(error, generation).catch((failure) => {
        throw isDiscoveryExhausted(failure)
          ? discoveryUnavailable(
              'The transaction coordinator could not be found; no producer id was requested, try again',
              failure
            )
          : failure
      })
    }
  }

  const stateManager = new ProducerStateManager({
    logger,
    transactional,
    transactionalId,
    transactionTimeout,
    retry,
    deliveryRetries,
    // Asks for no metadata first, as the Java producer: a non-transactional InitProducerId goes to
    // any connected broker, and a transactional one to its coordinator.
    findBroker: (generation) =>
      transactional
        ? lookUpTransactionCoordinator(generation)
        : cluster.findConnectedBroker(...(generation ? [{ fence: generation }] : [])),
    // A coordinator that could not be found makes the transaction abort-only, never fatal.
    addPartitionsToTxn: async (topics, generation) => {
      let broker
      try {
        broker = await findTransactionCoordinator(generation)
      } catch (error) {
        throw isDiscoveryExhausted(error) ? abortOnly(error) : error
      }
      generation?.check()
      await broker.addPartitionsToTxn({
        transactionalId,
        producerId: stateManager.producerId,
        producerEpoch: stateManager.producerEpoch,
        topics,
        ...guardOf(generation),
      })
    },
    getGeneration,
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
   * @param {import('../generation')} [generation] the lifecycle the caller captured
   * @returns {Promise<void>}
   */
  const retryingInitProducerId = (request, generation) =>
    retrierFor(generation)(async (bail, retryCount, retryTime) => {
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

  /**
   * Runs one task at a time per producer lifecycle: concurrent callers of one lifecycle share it,
   * a caller of another lifecycle starts its own.
   * @returns {(generation: ?import('../generation'), task: () => Promise<void>) => Promise<void>}
   */
  const sharedPerLifecycle = () => {
    /** @type {?{ generation: ?import('../generation'), promise: Promise<void> }} */
    let running = null
    return (generation, task) => {
      if (running === null || running.generation !== generation) {
        const current = { generation, promise: null }
        current.promise = task().finally(() => {
          if (running === current) running = null
        })
        running = current
      }
      return running.promise
    }
  }

  /** Whether a KIP-360 epoch bump is owed before the next transaction; kept until one succeeds. */
  let bumpOwed = false
  const bumping = sharedPerLifecycle()

  /**
   * Performs the epoch bump owed, if any: one re-initialization shared by the concurrent callers
   * of one producer lifecycle. A failure keeps the bump owed, so the next transaction tries it
   * again, and so does a bump whose lifecycle closed before it answered.
   * @param {import('../generation')} [generation] the lifecycle the caller captured
   * @returns {Promise<void>}
   */
  const bumpIfOwed = (generation) => {
    if (!bumpOwed) {
      return Promise.resolve()
    }

    return bumping(generation, () =>
      retryingInitProducerId(() => stateManager.reinitialize(), generation).then(() => {
        if (!generation?.closed) bumpOwed = false
      })
    )
  }

  /**
   * An abort whose outcome is unknown - its EndTxn failed, or a close cut it short - and whose
   * EndTxn is sent again before the next transaction: its transaction version, and whether the
   * transaction was confirmed to have started; null when none is owed. Kept until the coordinator
   * answers that EndTxn.
   * @type {?{ version: 1|2, confirmed: boolean }}
   */
  let abortOwed = null
  const resending = sharedPerLifecycle()

  /**
   * Sends the owed abort again: EndTxn(ABORT) with the producer id and epoch held, shared
   * by the concurrent callers of one producer lifecycle. A coordinator that still holds the
   * transaction aborts it, and one that already aborted it answers success. Under transaction
   * version 2 the producer adopts the epoch the answer carries; under version 1 an epoch bump is
   * then owed. PRODUCER_FENCED or INVALID_PRODUCER_ID_MAPPING - another producer took over the
   * transactional id, or (version 1) the coordinator aborted the transaction on its timeout -
   * makes the producer fatal. INVALID_TXN_STATE for a transaction never confirmed to have started
   * means nothing to abort (`nothingToAbort`), handled as success. Any other failure keeps the
   * abort owed.
   * @param {import('../generation')} [generation] the lifecycle the caller captured
   * @returns {Promise<void>}
   * @throws {import('../../errors').KafkaJSApplicationRecoverableError} the armed fatal error
   * @throws {Error} the EndTxn's other error, as `endTransaction` throws it
   */
  const resendAbortOwed = (generation) =>
    resending(generation, async () => {
      let adopted = false
      try {
        adopted = await sendEndTxn(false, abortOwed.version, generation)
      } catch (error) {
        generation?.check()
        if (!nothingToAbort(error, abortOwed.confirmed)) {
          stateManager.arm(error)
          if (stateManager.state() === STATES.FATAL_ERROR) {
            throw stateManager.transaction.armedError()
          }
          throw error
        }
      }
      abortOwed = null
      bumpOwed = !adopted
    })

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
   * it leaves the producer unusable, which arms `FATAL_ERROR` and is thrown. A coordinator that
   * could not be found is swallowed, unless a fatal error was armed while the abort waited.
   * @param {Error} error
   * @returns {void}
   * @throws {import('../../errors').KafkaJSApplicationRecoverableError} the armed error
   */
  const swallowUnlessFatal = (error) => {
    stateManager.classify(error, 'txn')
    if (stateManager.state() === STATES.FATAL_ERROR) {
      throw stateManager.transaction.armedError()
    }
    if (!isDiscoveryExhausted(error) && transactionStateFor(error) === STATES.FATAL_ERROR) {
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
   * epoch it returns (KIP-890), which restarts every sequence. A transaction that sent no add
   * request and holds nothing ends locally with no EndTxn, under either version; one whose
   * AddPartitionsToTxn or AddOffsetsToTxn went unanswered sends it, since the coordinator may
   * have applied the add. No EndTxn goes out
   * while a batch is in flight, as the Java producer holds it until every batch completed: the
   * caller waited for the transaction's sends, so only a fault in the send loop leaves one. The
   * coordinator is looked up again on every attempt, and a lookup whose own retries ran out ends
   * the attempts with that error as it is.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {boolean} committed
   * @param {import('../generation')} [generation] the lifecycle the caller captured: its close
   *   ends the retries, and an answer after it is not adopted
   * @returns {Promise<boolean>} whether a new producer id and epoch were adopted
   * @throws {KafkaJSInvariantViolation} while a batch is in flight; nothing is sent
   * @throws {Error} the EndTxn's error, classified; `KafkaJSNumberOfRetriesExceeded` once the
   *   retries run out; the lifecycle's close error once it closed
   */
  const endTransaction = async (committed, generation) => {
    if (!stateManager.transactionReached()) {
      logger.debug('No partitions or offsets registered, not sending EndTxn')
      return false
    }

    if (stateManager.sequencer.hasInFlight()) {
      throw new KafkaJSInvariantViolation('EndTxn with a batch of the transaction in flight')
    }

    return sendEndTxn(committed, stateManager.transactionVersion(), generation)
  }

  /**
   * Sends one EndTxn with the producer id and epoch held, as `endTransaction` describes, at
   * `transactionVersion`.
   * @param {boolean} committed
   * @param {1|2} transactionVersion
   * @param {import('../generation')} [generation]
   * @returns {Promise<boolean>} whether a new producer id and epoch were adopted
   * @throws {Error} as `endTransaction`
   */
  const sendEndTxn = async (committed, transactionVersion, generation) => {
    const ended = await retrierFor(generation)(async (bail) => {
      try {
        generation?.check()
        const coordinator = await findTransactionCoordinator(generation)
        generation?.check()
        return await coordinator.endTxn({
          producerId: stateManager.producerId,
          producerEpoch: stateManager.producerEpoch,
          transactionalId,
          transactionResult: committed,
          transactionVersion,
          ...guardOf(generation),
          ...(committed && stateManager.offsets.inputs !== null ? { [ON_WRITE]: sealInputs } : {}),
        })
      } catch (e) {
        generation?.check()
        stateManager.classify(e, 'txn')
        if (e.retriable) {
          throw e
        }
        bail(e)
      }
    })
    generation?.check()

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
     * replacing it, as the Java producer shares one InitProducerId between its callers. A
     * transactional producer's lookup of its coordinator is retried while it fails retriably, a
     * connection reset above all; the request itself is retried only on the codes that clear
     * by themselves.
     * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
     * @returns {Promise<void>}
     * @throws {import('../../errors').KafkaJSInvalidConfigurationError} CLUSTER_AUTHORIZATION_FAILED,
     *   naming both remedies
     * @throws {import('../../errors').KafkaJSApplicationRecoverableError} initial fencing codes
     *   47 or 90; the protocol error remains its cause and the producer stays fatal
     * @throws {KafkaJSError} retriable, `cause` the discovery error, when the transaction
     *   coordinator could not be found; nothing is armed and the call can be made again
     */
    async initProducerId() {
      const generation = getGeneration()
      stateManager.transaction.guard('initProducerId')
      const held = stateManager.isInitialized()
      try {
        await retryingInitProducerId(async () => {
          if (held || !stateManager.isInitialized()) {
            await stateManager.initialize()
          }
        }, generation)
      } catch (error) {
        if (error.code === 47 || error.code === 90) {
          generation?.check()
          stateManager.classify(error, 'txn')
          stateManager.arm(error)
          const armed = stateManager.transaction.armedError()
          onFenced?.(armed)
          throw armed
        }
        throw error
      }
      generation?.check()
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
     * it finalizes, fixed for this transaction. An abort whose outcome was unknown has its EndTxn
     * sent again first (`resendAbortOwed`). When the version moved from 1 to 2, or an epoch bump
     * is still owed, the producer id is then re-initialized (KIP-360). A failure of either rejects
     * the call, leaves the producer `READY` - `FATAL_ERROR` when it was fatal - and keeps what was
     * owed for the next call. A lookup that fails retriably, a connection reset above all, is
     * retried under the producer's `retry` settings; one that is not retriable, or whose retries
     * ran out, rejects the call the same way.
     * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
     * @returns {Promise<void>}
     * @throws {import('../../errors').KafkaJSNonRetriableError} outside `READY`, or on a producer
     *   that is not transactional
     * @throws {Error} the coordinator lookup's error - `KafkaJSNumberOfRetriesExceeded` once its
     *   retries ran out, or `KafkaJSDeliveryTimeout` once `retry.maxTotalTime` did - or the
     *   re-initialization's; the producer stays `READY` (`FATAL_ERROR` when the re-initialization
     *   failed fatally)
     */
    async beginTransaction() {
      const generation = getGeneration()
      stateManager.transaction.guard('beginTransaction')
      transactionalGuard()
      let coordinator
      // The first attempt is inline, so a lookup that succeeds costs no wrapper promise.
      try {
        coordinator = await findTransactionCoordinator(generation)
      } catch (error) {
        coordinator = await retryTransactionCoordinator(error, generation)
      }
      generation?.check()
      if (stateManager.version.begin(coordinator).bumpRequired) {
        bumpOwed = true
      }
      if (abortOwed !== null) {
        await resendAbortOwed(generation)
        generation?.check()
      }
      await bumpIfOwed(generation)
      generation?.check()
      stateManager.transitionTo(STATES.IN_TRANSACTION)
    },

    /**
     * Add partitions to a transaction if they are not already marked as participating. A
     * transaction coordinator that could not be found arms `ABORTABLE_ERROR`.
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
     * the transaction abort-only first, so no EndTxn commits over it. Once the EndTxn is confirmed,
     * the offsets this transaction's `sendOffsets` calls staged are published to the shared
     * committed-offset cache, each partition once unless a newer offset operation fenced it; an
     * unknown outcome keeps them staged for the next `commit()`.
     *
     * An EndTxn whose outcome is unknown - its retries ran out on a retriable answer, a timeout or
     * a lost connection - leaves the commit pending: the transaction stays `COMMITTING` and the
     * next `commit()` sends the same EndTxn again, with the same producer id and epoch. A commit
     * that had landed is answered with success then (under transaction version 2 with the epoch to
     * adopt); one the coordinator aborted on its timeout meanwhile is refused with
     * `INVALID_TXN_STATE` (48) or `PRODUCER_FENCED` (90), depending on the transaction version and
     * on whether the broker has KAFKA-20785 (fixed in 4.3.2 and 4.5.0), which makes the producer
     * unusable.
     *
     * A transaction coordinator that could not be found - its discovery budget spent, before or
     * after an unacknowledged EndTxn - leaves the commit pending the same way, and is not looked up
     * again inside this call. A fatal error armed meanwhile stays.
     * @see https://github.com/apache/kafka/blob/4.3/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala
     * @returns {Promise<void>}
     * @throws {Error} the last retriable error itself (`retriable: true`) when the outcome is
     *   unknown; the commit is pending
     * @throws {KafkaJSError} retriable, `cause` the discovery error, when the transaction
     *   coordinator could not be found; the commit is pending
     * @throws {import('../../errors').KafkaJSTransactionAbortableError} the armed error when the
     *   transaction can only be aborted; an abortable EndTxn answer arms `ABORTABLE_ERROR` and
     *   rejects with it
     * @throws {import('../../errors').KafkaJSApplicationRecoverableError} the armed error once the
     *   producer is unusable, a fatal EndTxn answer included, and a fenced first `InitProducerId`
     *   of the producer's other identity that landed while the EndTxn was outstanding
     */
    async commit() {
      const generation = getGeneration()
      stateManager.transaction.guard('commit')
      transactionalGuard()
      await stateManager.whenPartitionsAdded()
      generation?.check()
      stateManager.resolveSequences()
      stateManager.transaction.guard('commit')
      const refusal = inputRefusal()
      if (refusal) throw armWith(refusal)
      stateManager.transitionTo(STATES.COMMITTING)

      try {
        await endTransaction(true, generation)
        generation?.check()
      } catch (error) {
        if (generation?.closed) {
          if (stateManager.state() === STATES.FATAL_ERROR) {
            throw stateManager.transaction.armedError()
          }
          stateManager.transaction.holdCommit()
          throw new KafkaJSProducerClosedError(
            'The producer was closed with a commit unconfirmed',
            {
              outcome: 'unknown',
            }
          )
        }
        if (isDiscoveryExhausted(error)) {
          if (stateManager.state() !== STATES.COMMITTING) {
            throw stateManager.transaction.armedError()
          }
          stateManager.transaction.holdCommit()
          throw discoveryUnavailable(COMMIT_PENDING, error)
        }
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
     * (transaction version 2), which restarts every sequence already. A bump that fails is owed to
     * the next transaction, and the abort resolves with the producer `READY`; only a bump that
     * fails fatally rejects it, after a successful EndTxn, with the producer in `FATAL_ERROR`. A
     * transaction coordinator that could not be found is never fatal here, for the EndTxn or for
     * the bump. An abort whose outcome is unknown - its EndTxn failed, or the producer was closed
     * while the abort ran - ends the transaction locally and leaves the producer `READY` with the
     * EndTxn owed when the transaction reached the coordinator: the next transaction sends it again
     * with the producer id and epoch held, never a fresh InitProducerId, so it cannot fence another
     * producer that took over the transactional id. A close rejects the abort with the close error.
     * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
     * @param {object} [options]
     * @param {boolean} [options.droppedSequences=false] whether the caller dropped queued batches
     *   that had already been sent, so their sequences are spent
     * @returns {Promise<void>}
     * @throws {import('../../errors').KafkaJSApplicationRecoverableError} when the abort itself
     *   failed fatally, or a fatal answer armed `FATAL_ERROR` while it waited
     * @throws {import('../../errors').KafkaJSProducerClosedError} the lifecycle's close error once
     *   the producer was closed; the producer is `READY`, the EndTxn owed
     */
    async abort({ droppedSequences = false } = {}) {
      const generation = getGeneration()
      stateManager.transaction.guard('abort')
      transactionalGuard()
      stateManager.resolveSequences()
      const bump = droppedSequences || stateManager.state() === STATES.ABORTABLE_ERROR
      stateManager.transitionTo(STATES.ABORTING)

      /** Ends the transaction locally, owing its EndTxn when it reached the coordinator. */
      const endUnknown = () => {
        if (stateManager.transactionReached()) {
          abortOwed = {
            version: stateManager.transactionVersion(),
            confirmed: stateManager.transactionStarted(),
          }
        }
        if (bump) bumpOwed = true
        stateManager.transitionTo(STATES.READY)
      }

      let adopted = false
      try {
        await stateManager.whenPartitionsAdded()
        generation?.check()
        if (stateManager.state() === STATES.FATAL_ERROR) {
          throw stateManager.transaction.armedError()
        }

        try {
          adopted = await endTransaction(false, generation)
        } catch (error) {
          generation?.check()
          if (!nothingToAbort(error, stateManager.transactionStarted())) {
            swallowUnlessFatal(error)
            endUnknown()
            return
          }
        }

        generation?.check()
      } catch (error) {
        if (generation?.closed && stateManager.state() === STATES.ABORTING) endUnknown()
        throw error
      }
      stateManager.transitionTo(STATES.READY)

      if (bump && !adopted) {
        bumpOwed = true
        await bumpIfOwed(generation).catch((error) => {
          generation?.check()
          swallowUnlessFatal(error)
        })
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
     * the consumer rejoin, reprocess - and rejects with it. So does a v6 answer that cannot be
     * matched to its request, and a transaction or group coordinator that could not be found. An UNKNOWN_TOPIC_ID answer (v6) refreshes the metadata the cluster
     * tracks before the retry, and never creates a topic.
     *
     * Under version 2 offsets holding no partition are skipped, as the Java producer skips them:
     * nothing is sent and the transaction is not marked started, since the broker starts none for
     * them and an `EndTxn` on an empty transaction fails. Under version 1 the AddOffsetsToTxn is
     * still sent, as in 2.2.4.
     *
     * The offsets are copied, and their ledger fence taken, when the method is called, before any
     * await; the calls of one transaction then run one at a time in call order, each with its
     * retries. Accepted offsets are staged, the last call winning for a partition, and reach the
     * shared committed-offset cache only when `commit()` is confirmed; a failed call releases its
     * own fence.
     * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
     * @param {object} options
     * @param {import('../../../types').ConsumerGroupMetadata} [options.consumerGroupMetadata]
     * @param {string} [options.consumerGroupId]
     * @param {OffsetCommitTopic[]} options.topics
     * @returns {Promise<void>}
     * @throws {KafkaJSNonRetriableError} when both `consumerGroupMetadata` and `consumerGroupId`
     *   are given, or neither; nothing is armed then
     * @throws {TypeError} when `topics`, or the `partitions` of one, is not an array; nothing is
     *   sent and nothing is armed
     * @throws {import('../../errors').KafkaJSTransactionAbortableError} armed, `cause` a
     *   `KafkaJSCommitFailedError` (`stale-assignment`), when an input batch of the transaction was
     *   superseded (a seek, a topic recreation, the end of its acquisition); nothing more of the
     *   call is sent. Called from a consumer handler with offsets of its own group and partition,
     *   the call ties the transaction to that batch until the commit EndTxn is written
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
      const generation = getGeneration()
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
      const identity = handlerContext.current()
      if (
        identity?.acquisition !== undefined &&
        identity.groupId === groupId &&
        carries(snapshot.topics, identity)
      ) {
        stateManager.offsets.enlist(identity.acquisition, identity.acquireClock)
      }
      const refusal = inputRefusal()
      if (refusal) {
        stateManager.offsets.abandon(snapshot)
        throw armWith(refusal)
      }
      return stateManager.offsets.enqueue(snapshot, async () => {
        const transactionVersion = stateManager.transactionVersion()

        try {
          generation?.check()
          stateManager.transaction.guard('sendOffsets')
          if (transactionVersion === 2 && !hasPartitions(snapshot.topics)) {
            stateManager.offsets.stage(snapshot)
            return
          }

          if (transactionVersion === 1) {
            stateManager.markAddSent()
            await retrierFor(generation)(async (bail, retryCount, retryTime) => {
              generation?.check()
              const transactionCoordinator = await findTransactionCoordinator(generation)
              generation?.check()
              try {
                await transactionCoordinator.addOffsetsToTxn({
                  transactionalId,
                  producerId: stateManager.producerId,
                  producerEpoch: stateManager.producerEpoch,
                  groupId,
                  ...offsetGuardOf(generation),
                })
              } catch (e) {
                generation?.check()
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

          generation?.check()
          stateManager.markOffsetsAdded()

          let groupCoordinator = await findCoordinator(groupId, COORDINATOR_TYPES.GROUP, generation)
          generation?.check()
          await retrierFor(generation)(async (bail, retryCount, retryTime) => {
            try {
              generation?.check()
              const topicIds = usableTopicIds(
                snapshot.topics.map(({ topic }) => topic),
                (name) => cluster.topicId(name)
              )
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
                ...(topicIds ? { topicIds } : {}),
                ...offsetGuardOf(generation),
              })
            } catch (e) {
              generation?.check()
              const { category } = stateManager.classify(e, 'offsets')
              if (e.retriable) {
                logger.debug('TxnOffsetCommit failed retriably, retrying', {
                  error: e.message,
                  transactionalId,
                  retryCount,
                  retryTime,
                })
                if (category === 'refreshRetriable' || e.code === 'ECONNREFUSED') {
                  groupCoordinator = await findCoordinator(
                    groupId,
                    COORDINATOR_TYPES.GROUP,
                    generation
                  )
                }
                if (e.type === 'UNKNOWN_TOPIC_ID') {
                  await refreshWithoutCreating(generation)
                }
                throw e
              }
              bail(e)
            }
          })
          generation?.check()
          stateManager.offsets.stage(snapshot)
        } catch (error) {
          stateManager.offsets.abandon(snapshot)
          if (generation?.closed) {
            throw error
          }
          throw armWith(isDiscoveryExhausted(error) ? abortOnly(error) : error)
        }
      })
    },
  }

  return eosManager
}

createEosManager.surfaceInTransaction = surfaceInTransaction

module.exports = createEosManager
