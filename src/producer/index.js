const createRetry = require('../retry')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const { DefaultPartitioner } = require('./partitioners/')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const createEosManager = require('./eosManager')
const createMessageProducer = require('./messageProducer')
const Idempotence = require('./idempotence')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const {
  KafkaJSNonRetriableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSTransactionAbortableError,
} = require('../errors')
const { IN_TRANSACTION, isArmed } = require('./eosManager/transactionStates')
const TransactionState = require('./state/transactionState')

const { values, keys } = Object
const eventNames = values(events)
const eventKeys = keys(events)
  .map((key) => `producer.events.${key}`)
  .join(', ')

const { CONNECT, INITIALIZED, DISCONNECT } = events

/**
 * The error `transaction()` rejects with while another transaction is ongoing.
 * @returns {KafkaJSNonRetriableError}
 */
const ongoingTransactionError = () =>
  new KafkaJSNonRetriableError(
    'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.'
  )

/**
 * The error a transaction's methods reject with once it has ended.
 * @returns {KafkaJSNonRetriableError}
 */
const endedTransactionError = () =>
  new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')

/**
 * The code a warning names for a failed commit: the error's own, or else its cause's, which is the
 * Kafka error code of the answer an armed error wraps.
 * @param {Error} error
 * @returns {number|string|undefined}
 */
const codeOf = (error) => error.code ?? error.cause?.code

/**
 * The accumulator's defaults: the Java client's 4.x `linger.ms` and `batch.size`.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java
 */
const DEFAULTS = Object.freeze({ linger: 5, batchSize: 16384 })

/**
 *
 * @param {Object} params
 * @param {import('../../types').Cluster} params.cluster
 * @param {import('../../types').Logger} params.logger
 * @param {import('../../types').ICustomPartitioner} [params.createPartitioner]
 * @param {import('../../types').RetryOptions} [params.retry] merged over `cluster.retry`; a
 *   producer that sets neither keeps the retrier's default budget
 * @param {boolean} [params.idempotent] idempotent unless falsy (`undefined` and `null` mean not
 *   passed); a value passed, or a non-empty `transactionalId`, makes a conflicting `retry.retries`
 *   or `acks` an error instead of a reason to turn idempotence off with a warning
 * @param {string} [params.transactionalId]
 * @param {number} [params.transactionTimeout]
 * @param {number} [params.linger=5] ms a batch waits for more records; 0 sends it at once
 * @param {number} [params.batchSize=16384] bytes of keys and values at which a batch is sent
 * @param {number} [params.deliveryTimeout] ms one send may take, retries included; past it the
 *   send rejects with `KafkaJSDeliveryTimeout`
 * @param {InstrumentationEventEmitter} [params.instrumentationEmitter]
 *
 * @returns {import('../../types').Producer}
 * @throws {KafkaJSInvalidConfigurationError} `retry.retries` below 1 on a producer that asked for
 *   idempotence
 */
const createProducer = ({
  cluster,
  logger: rootLogger,
  createPartitioner = DefaultPartitioner,
  retry = {},
  idempotent,
  transactionalId,
  transactionTimeout,
  linger = DEFAULTS.linger,
  batchSize = DEFAULTS.batchSize,
  deliveryTimeout,
  instrumentationEmitter: rootInstrumentationEmitter,
}) => {
  let connectionStatus = CONNECTION_STATUS.DISCONNECTED
  /**
   * @type {Promise<void>|null} while `disconnect()` delivers the accumulated batches: every send,
   *   the producer's own and its transactions', is refused, and concurrent calls share it
   */
  let closing = null
  const isClosing = () => closing !== null
  const logger = rootLogger.namespace('Producer')
  const transactional = Boolean(transactionalId)
  const idempotence = new Idempotence({
    enabled: transactional || idempotent == null || Boolean(idempotent),
    explicit: transactional || idempotent != null,
    logger,
  })

  if (idempotence.isEnabled() && retry.retries < 1) {
    if (idempotence.isExplicit()) {
      throw new KafkaJSInvalidConfigurationError(
        'Idempotent producer must allow retries to protect against transient errors'
      )
    }

    idempotence.disable(
      'retries',
      `Idempotence disabled: retry.retries is ${retry.retries}, and an idempotent producer must ` +
        'retry. Pass retry.retries of 1 or more to keep idempotence, or idempotent: false to ' +
        'silence this warning.'
    )
  }

  // A user who deliberately picked a generous, finite retry budget has already made the
  // trade-off; only warn below the non-idempotent default, where exhaustion is plausible.
  const MIN_SAFE_IDEMPOTENT_RETRIES = 5
  if (idempotence.isEnabled() && retry.retries < MIN_SAFE_IDEMPOTENT_RETRIES) {
    logger.warn('Limiting retries for the idempotent producer may invalidate EoS guarantees', {
      retries: retry.retries,
    })
  }

  const partitioner = createPartitioner()
  const retryOptions = Object.assign({}, cluster.retry, retry)
  const retrier = createRetry({
    ...retryOptions,
    maxTotalTime: deliveryTimeout ?? retryOptions.maxTotalTime,
  })
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()
  const idempotentEosManager = createEosManager({
    logger,
    cluster,
    transactionTimeout,
    transactional: false,
    transactionalId,
    retry: retryOptions,
  })

  const { send, sendBatch, ensureInitialized, close } = createMessageProducer({
    logger,
    cluster,
    partitioner,
    eosManager: idempotentEosManager,
    idempotence,
    retrier,
    retry: retryOptions,
    deliveryTimeout,
    linger,
    batchSize,
    getConnectionStatus: () => connectionStatus,
    isClosing,
    instrumentationEmitter,
    INITIALIZED,
  })

  let transactionalEosManager
  let transactionalMessageProducer
  /**
   * @type {Promise<void>|null} the commit under way - a transaction's `commit()`, or the one
   *   `disconnect()` sends again - which a further `commit()` and `disconnect()` join; only one
   *   transaction can be committing at a time
   */
  let committing = null
  /**
   * @type {() => void} ends the last transaction object; called when the next transaction begins,
   *   because a commit `disconnect()` settled never ran through that object
   */
  let endPrevious = () => {}

  /**
   * Makes `commit` the commit under way until it settles.
   * @param {Promise<void>} commit
   * @returns {Promise<void>} commit
   */
  const joinable = (commit) => {
    committing = commit
    const settled = () => {
      committing = null
    }
    commit.then(settled, settled)
    return commit
  }

  /** @type {import("../../types").Producer["on"]} */
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
   * Begin a transaction. The returned object contains methods to send messages
   * to the transaction and end the transaction by committing or aborting.
   *
   * Only messages sent on the transaction object will participate in the transaction.
   *
   * Calling any of the transactional methods after the transaction has ended
   * will raise an exception (use `isActive` to ascertain if ended).
   *
   * A send or `sendOffsets` that fails inside the transaction leaves it abort-only, or the producer
   * unusable, as the error's category decides: from then on `send`, `sendBatch`, `sendOffsets` and
   * `commit` reject with that armed error, `abort()` still ends the transaction, and
   * `producer.transaction()` rejects with it until the transaction is aborted. `commit()` and
   * `abort()` first wait for every send and `sendOffsets` of the transaction to settle, as the Java
   * producer flushes before it commits; `abort()` rejects the batches still queued first.
   *
   * A `commit()` whose outcome is unknown rejects with the last retriable error and leaves the
   * commit pending: calling `commit()` again sends it again, and meanwhile `send`, `sendBatch`,
   * `sendOffsets`, `abort()` and `producer.transaction()` throw a `KafkaJSNonRetriableError` saying
   * so. A `commit()` called while another is under way joins it, as the Java producer hands a
   * repeated `commitTransaction()` the result already pending.
   * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1050%3A+Consistent+error+handling+for+Transactions
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @returns {Promise<Transaction>}
   * @throws {import('../errors').KafkaJSTransactionAbortableError} while the previous transaction
   *   is abort-only
   * @throws {import('../errors').KafkaJSApplicationRecoverableError} once the producer is unusable
   * @throws {import('../errors').KafkaJSNonRetriableError} while the previous transaction's commit
   *   is pending
   *
   * @typedef {Object} Transaction
   * @property {Function} send  Identical to the producer "send" method
   * @property {Function} sendBatch Identical to the producer "sendBatch" method
   * @property {Function} sendOffsets Add consumed offsets to the transaction
   * @property {Function} abort Abort the transaction
   * @property {Function} commit  Commit the transaction
   * @property {Function} isActive  Whether the transaction is active
   */
  const transaction = async () => {
    if (!transactionalId) {
      throw new KafkaJSNonRetriableError('Must provide transactional id for transactional producer')
    }

    transactionalEosManager =
      transactionalEosManager ||
      createEosManager({
        logger,
        cluster,
        transactionTimeout,
        transactional: true,
        transactionalId,
        retry: retryOptions,
      })
    transactionalMessageProducer =
      transactionalMessageProducer ||
      createMessageProducer({
        logger,
        cluster,
        partitioner,
        retrier,
        retry: retryOptions,
        deliveryTimeout,
        linger,
        batchSize,
        eosManager: transactionalEosManager,
        idempotence,
        getConnectionStatus: () => connectionStatus,
        isClosing,
        instrumentationEmitter,
        INITIALIZED,
      })
    const { stateManager } = transactionalEosManager

    if (transactionalEosManager.isInTransaction()) {
      throw ongoingTransactionError()
    }

    try {
      // We only initialize the producer id once
      if (!transactionalEosManager.isInitialized()) {
        await transactionalEosManager.initProducerId()
      }
      await transactionalEosManager.beginTransaction()
    } catch (error) {
      // Another transaction() began while this one was initializing or finding the coordinator.
      if (transactionalEosManager.isInTransaction()) {
        throw ongoingTransactionError()
      }
      throw error
    }

    const { send: sendTxn, sendBatch: sendBatchTxn } = transactionalMessageProducer

    let transactionDidEnd = false
    endPrevious()
    endPrevious = () => {
      transactionDidEnd = true
    }
    /** @type {Promise<void>|null} the last `commit()` or `abort()` called; the next one waits for it */
    let ending = null
    /** @type {Set<Promise<unknown>>} the sends and `sendOffsets` not settled yet */
    const pending = new Set()

    /**
     * Keeps an operation of this transaction until it settles: `commit()` and `abort()` wait for
     * every one of them.
     * @param {Promise<unknown>} operation
     * @returns {Promise<unknown>} the operation
     */
    const track = (operation) => {
      pending.add(operation)
      const forget = () => pending.delete(operation)
      operation.then(forget, forget)
      return operation
    }

    /**
     * Wraps `send`, `sendBatch` or `sendOffsets`: refused once the transaction has ended, with the
     * armed error while it can only be aborted or the producer is unusable, while its commit is
     * pending, and once it is ending.
     * @param {string} method the name the pending-commit error gives the call
     * @param {Function} fn
     * @returns {Function}
     */
    const transactionGuard =
      (method, fn) =>
      (...args) => {
        if (transactionDidEnd) {
          return Promise.reject(endedTransactionError())
        }
        const state = stateManager.state()
        if (isArmed(state)) {
          return Promise.reject(stateManager.transaction.armedError())
        }
        if (stateManager.transaction.commitPending()) {
          return Promise.reject(TransactionState.pendingCommitError(method))
        }
        if (ending !== null || state !== IN_TRANSACTION) {
          return Promise.reject(endedTransactionError())
        }

        return track(fn(...args))
      }

    /**
     * Ends the transaction once its own operations have settled. A `commit()` or `abort()` called
     * while another one is under way waits for it, and is refused if it ended the transaction: so a
     * failed `commit()` can still be followed by `abort()`, and two ends never race.
     * @param {() => Promise<void>} end
     * @returns {Promise<void>}
     */
    const endWith = (end) => {
      const previous = ending === null ? Promise.resolve() : ending.catch(() => {})
      ending = previous.then(async () => {
        if (transactionDidEnd) {
          throw endedTransactionError()
        }

        await Promise.allSettled(pending)
        await end()
        transactionDidEnd = true
      })
      return ending
    }

    const isActive = () => transactionalEosManager.isInTransaction() && !transactionDidEnd

    return {
      sendBatch: transactionGuard('sendBatch', sendBatchTxn),
      send: transactionGuard('send', sendTxn),
      /**
       * Abort the ongoing transaction: rejects the batches still queued - accumulated ones
       * included - waits for the rest of the transaction's operations, then aborts it. It ends the
       * transaction even when the abort fails, unless the producer is unusable. A rejected batch
       * that was already sent once leaves its sequence spent, so the abort then moves the producer
       * to a new epoch (see `abort()` on the eosManager). Called while a `commit()` of the
       * transaction is under way, it rejects no batch and waits for the commit: the commit keeps
       * every record, and the abort is refused if the commit ended the transaction.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended
       * @throws {import('../errors').KafkaJSApplicationRecoverableError} once the producer is
       *   unusable
       */
      abort: () => {
        // The accumulated batches join the queue, so the abort rejects them with the rest.
        transactionalMessageProducer.flush()
        const dropped =
          ending === null && stateManager.state() === IN_TRANSACTION
            ? stateManager.sequencer.abortQueued(
                new KafkaJSTransactionAbortableError(
                  'The transaction was aborted while this batch was waiting to be sent'
                )
              )
            : []
        const droppedSequences = dropped.some(({ firstSequence }) => firstSequence !== null)
        const ended = endWith(() => transactionalEosManager.abort({ droppedSequences }))
        transactionalMessageProducer.flush(ended)
        return ended
      },
      /**
       * Commit the ongoing transaction, once every send and `sendOffsets` of it has settled. The
       * transaction's accumulated batches are sent at once rather than after their `linger`, and so
       * is every batch its sends form until the commit ends. A call made while the commit is under
       * way joins it: one EndTxn, and the same outcome for both.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended
       * @throws {import('../errors').KafkaJSTransactionAbortableError} the armed error, when an
       *   operation of the transaction failed; nothing is sent to the broker then
       * @throws {Error} the last retriable error itself (`retriable: true`) when the commit's
       *   outcome is unknown: call `commit()` again, which sends it again
       * @throws {import('../errors').KafkaJSApplicationRecoverableError} when the commit sent again
       *   finds the transaction aborted by the coordinator on its timeout: `INVALID_TXN_STATE` (48)
       *   or `PRODUCER_FENCED` (90), depending on the transaction version and on whether the broker
       *   has KAFKA-20785 (fixed in 4.3.2 and 4.5.0); replace the producer
       */
      commit: () => {
        if (transactionDidEnd) {
          return Promise.reject(endedTransactionError())
        }
        if (committing === null) {
          transactionalMessageProducer.flush(
            joinable(endWith(() => transactionalEosManager.commit()))
          )
        }
        return committing
      },
      /**
       * Sends a list of specified offsets to the consumer group coordinator, and also marks those
       * offsets as part of the current transaction. Takes the consumer's `consumerGroupMetadata`
       * (`consumer.groupMetadata()`), so a stale or zombie member is fenced (KIP-447), or a
       * `consumerGroupId`, which commits with no member to fence; exactly one of them. The offsets
       * are marked committed in this client's cache, for that group, once they are sent.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended, or when both or neither of
       *   `consumerGroupMetadata` and `consumerGroupId` are given
       */
      sendOffsets: transactionGuard(
        'sendOffsets',
        async ({ consumerGroupMetadata, consumerGroupId, topics }) => {
          await transactionalEosManager.sendOffsets({
            consumerGroupMetadata,
            consumerGroupId,
            topics,
          })
          const groupId = consumerGroupId ?? consumerGroupMetadata.groupId

          for (const topicOffsets of topics) {
            const { topic, partitions } = topicOffsets
            for (const { partition, offset } of partitions) {
              cluster.markOffsetAsCommitted({
                groupId,
                topic,
                partition,
                offset,
              })
            }
          }
        }
      ),
      isActive,
    }
  }

  /**
   * @returns {Object} logger
   */
  const getLogger = () => logger

  /**
   * Settles the transaction's commit before the connections close, as the Java producer's close
   * lets a transaction that is completing finish. It waits, in order, for the commit `disconnect()`
   * found under way when it began (one that ended pending is not sent again), else for the commit
   * under way now (a concurrent `disconnect()`'s resend), else sends a pending commit once more,
   * through one more retry budget; with none of them it sends nothing. An outcome still unknown, or
   * an answer that fails the commit, is logged as a warning naming the transactional id and the
   * error's code; the coordinator decides an unknown outcome, and an answer that makes the producer
   * unusable leaves it so.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @param {Promise<void>|null} joined the commit under way when `disconnect()` began
   * @returns {Promise<void>} never rejects
   */
  const settleCommit = async (joined) => {
    const commit =
      joined ??
      committing ??
      (transactionalEosManager?.stateManager.transaction.commitPending()
        ? joinable(transactionalEosManager.commit())
        : null)
    if (commit === null) {
      return
    }

    try {
      await commit
    } catch (error) {
      logger.warn(
        error.retriable
          ? 'Disconnecting with the commit unconfirmed; the transaction coordinator completes or aborts the transaction'
          : 'The commit failed while the producer was disconnecting',
        { transactionalId, code: codeOf(error), error: error.message }
      )
    }
  }

  /**
   * Delivers every accumulated batch, the producer's own and its transaction's, while every send
   * is refused, then leaves the producer `DISCONNECTING`. Concurrent calls share one delivery.
   * @returns {Promise<void>} never rejects
   */
  const deliverAccumulated = () =>
    (closing ??= Promise.all([close(), transactionalMessageProducer?.close()]).then(() => {
      // In one step, so no send passes between the gate and the status.
      connectionStatus = CONNECTION_STATUS.DISCONNECTING
      closing = null
    }))

  return {
    /**
     * @returns {Promise}
     */
    connect: async () => {
      await cluster.connect()
      connectionStatus = CONNECTION_STATUS.CONNECTED
      // CONNECT stays exactly where it is: it means "the cluster connection is up", and existing
      // listeners depend on that ordering. INITIALIZED is the new "safe to send()" signal.
      instrumentationEmitter.emit(CONNECT)

      // Shares the same deduped ensureInitialized() as send()/sendBatch(), so an un-awaited
      // connect() racing a concurrent send() issues exactly one InitProducerId request instead
      // of two (which would otherwise allocate two producer ids and reset sequences underneath
      // in-flight sends).
      await ensureInitialized()
    },
    /**
     * Disconnects the producer. It first delivers every batch still lingering, its own and its
     * transaction's, and the sends begun before it, refusing new sends meanwhile; nothing
     * accumulated is lost. A transaction's commit under way when it begins is waited for, and not
     * sent again if it ends pending; a pending one that is not under way is sent once more first.
     * It then waits for any commit another `disconnect()` sends, and for every commit the
     * application starts while it waits, so the connections never close while a commit is under
     * way; each such commit extends the wait. The call resolves whatever they answer.
     * @return {Promise}
     */
    disconnect: async () => {
      const joined = committing
      await deliverAccumulated()
      await settleCommit(joined)
      // eslint-disable-next-line no-unmodified-loop-condition -- joinable() clears it while the loop awaits
      while (committing !== null) {
        await committing.catch(() => {})
      }
      await cluster.disconnect()
      connectionStatus = CONNECTION_STATUS.DISCONNECTED
      instrumentationEmitter.emit(DISCONNECT)
    },
    /**
     * @returns {boolean} whether the producer sends idempotently; false once a configuration
     *   that does not allow it has turned idempotence off
     */
    isIdempotent: () => idempotence.isEnabled(),
    events,
    on,
    send,
    sendBatch,
    transaction,
    logger: getLogger,
  }
}

/** The accumulator's defaults, `linger` and `batchSize`, for every producer. */
createProducer.defaults = DEFAULTS

module.exports = createProducer
