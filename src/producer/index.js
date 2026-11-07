const { inspect } = require('util')
const createRetry = require('../retry')
const Generation = require('./generation')
const { MAX_DELAY } = require('./deadline')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const { UniformStickyPartitioner } = require('./partitioners/')
const { attach: attachBuiltIn } = require('./partitioners/builtIn')
const RackPreference = require('./partitioners/rackPreference')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const createEosManager = require('./eosManager')
const createMessageProducer = require('./messageProducer')
const Idempotence = require('./idempotence')
const { Admission } = require('./state/admission')
const { RequestBudget } = require('./state/recordSize')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const {
  KafkaJSNonRetriableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSProducerClosedError,
  KafkaJSTimeout,
  KafkaJSTransactionAbortableError,
} = require('../errors')
const {
  IN_TRANSACTION,
  READY,
  ABORTING,
  ABORTABLE_ERROR,
  isArmed,
} = require('./eosManager/transactionStates')
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
 * The `timeout` of a `flush` or `disconnect` call, validated before the call changes anything.
 * @param {unknown} options the call's argument
 * @param {boolean} ignoreNonObject whether a non-object argument means no options, as for a signal
 *   handler's argument (`disconnect`), instead of an error (`flush`)
 * @returns {number|undefined} ms, an integer from 0 to 2147483647; undefined when omitted
 * @throws {KafkaJSInvalidConfigurationError} an options object whose `timeout` is anything else,
 *   or a non-object argument that is not ignored
 */
const timeoutOf = (options, ignoreNonObject) => {
  if (
    options === undefined ||
    (ignoreNonObject && (typeof options !== 'object' || options === null))
  ) {
    return undefined
  }
  if (typeof options !== 'object' || options === null) {
    throw new KafkaJSInvalidConfigurationError(
      `The options must be an object with an optional timeout, got ${inspect(options)}`
    )
  }

  const { timeout } = options
  if (timeout === undefined) {
    return undefined
  }
  if (!Number.isInteger(timeout) || timeout < 0 || timeout > MAX_DELAY) {
    throw new KafkaJSInvalidConfigurationError(
      `timeout must be an integer from 0 to ${MAX_DELAY}, got ${inspect(timeout)}`
    )
  }
  return timeout
}

/**
 * The accumulator's, the delivery deadline's and the buffer's defaults: the Java client's 4.x
 * `linger.ms`, `batch.size`, `delivery.timeout.ms`, `buffer.memory` and `max.block.ms`.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java
 */
const DEFAULTS = Object.freeze({
  linger: 5,
  batchSize: 16384,
  deliveryTimeout: 120000,
  bufferMemory: 33554432,
  maxBlockMs: 60000,
  maxRequestSize: 1048576,
  // KIP-794's adaptive weighting stays off by default until its measured gate passes.
  adaptive: false,
})

/**
 * The retry options of a send's delivery: the Java client's `retry.backoff.ms` and
 * `retry.backoff.max.ms`, and a count that never ends a send before its delivery deadline does.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java
 */
const DELIVERY_RETRY = Object.freeze({
  initialRetryTime: 100,
  maxRetryTime: 1000,
  retries: 2147483647,
})

/**
 * @param {object} options
 * @returns {object} a copy without the keys whose value is `undefined`, which a retrier would take
 *   for a setting and not for an absent one
 */
const definedKeys = (options) =>
  Object.fromEntries(Object.entries(options).filter(([, value]) => value !== undefined))

/**
 *
 * @param {Object} params
 * @param {import('../../types').Cluster} params.cluster
 * @param {import('../telemetry/factory').TelemetryFactory|null} [params.telemetry=null] private
 *   telemetry binding: its three public methods are added to the producer, it resumes after a
 *   usable connect and joins the one close; transaction handles have no telemetry surface
 * @param {import('../../types').Logger} params.logger
 * @param {import('../../types').ICustomPartitioner} [params.createPartitioner] the uniform sticky
 *   partitioner, sticking for `batchSize` bytes, when absent; a built-in partitioner with an
 *   `attach` method is attached once to the producer's queues and racks
 * @param {import('../../types').RetryOptions} [params.retry] merged over `cluster.retry`; a
 *   producer that sets neither keeps the retrier's default budget
 * @param {boolean} [params.idempotent] idempotent unless falsy (`undefined` and `null` mean not
 *   passed); a value passed, or a non-empty `transactionalId`, makes a conflicting `retry.retries`
 *   or `acks` an error instead of a reason to turn idempotence off with a warning
 * @param {string} [params.transactionalId]
 * @param {number} [params.transactionTimeout]
 * @param {number} [params.linger=5] ms a batch waits for more records; 0 sends it at once
 * @param {number} [params.batchSize=16384] bytes of keys and values at which a batch is sent
 * @param {{ gzip?: number, zstd?: number }} [params.compressionLevels] per-codec levels
 * @param {number} [params.deliveryTimeout] ms one send may take, retries included; past it the
 *   send rejects with `KafkaJSDeliveryTimeout`
 * @param {number} [params.bufferMemory=33554432] bytes the producer's sends may hold at once (see
 *   `Admission`), its own and its transactions' together
 * @param {number} [params.maxBlockMs=60000] ms a send waits for room in the buffer
 * @param {Admission} [params.admission] the buffer itself, built from `bufferMemory` and
 *   `maxBlockMs` when absent
 * @param {string|null} [params.clientId] the client's id, which every request carries
 * @param {number} [params.maxRequestSize=1048576] the largest Produce request in bytes, its whole
 *   frame counted, for its own sends and its transactions' (see `RequestBudget`)
 * @param {InstrumentationEventEmitter} [params.instrumentationEmitter]
 * @param {string} [params.rackId=''] the producer's rack (KIP-1123)
 * @param {boolean} [params.rackAware=false] whether the built-in partitioners prefer the partitions
 *   whose leader is in `rackId` for records without a key; needs a non-empty `rackId`, and a custom
 *   partitioner is never given either option
 *
 * @returns {import('../../types').Producer}
 * @throws {KafkaJSInvalidConfigurationError} `retry.retries` below 1 on a producer that asked for
 *   idempotence, a `rackId` that is not a string, a `rackAware` that is not a boolean, or
 *   `rackAware` with an empty `rackId`
 */
const createProducer = ({
  cluster,
  telemetry = null,
  logger: rootLogger,
  createPartitioner,
  retry = {},
  idempotent,
  transactionalId,
  transactionTimeout,
  linger = DEFAULTS.linger,
  batchSize = DEFAULTS.batchSize,
  deliveryTimeout,
  compressionLevels,
  bufferMemory = DEFAULTS.bufferMemory,
  maxBlockMs = DEFAULTS.maxBlockMs,
  admission = new Admission({ capacity: bufferMemory, maxBlockMs }),
  clientId,
  maxRequestSize = DEFAULTS.maxRequestSize,
  instrumentationEmitter: rootInstrumentationEmitter,
  rackId = '',
  rackAware = false,
}) => {
  RackPreference.validate({ rackId, rackAware })
  let connectionStatus = CONNECTION_STATUS.DISCONNECTED
  /**
   * @type {Promise<void>|null} while `disconnect()` cleans up, from its first step to its last:
   *   every send, the producer's own and its transactions', is refused, and concurrent calls
   *   share it
   */
  let closing = null
  /** Whether a `disconnect()` has completed since the last `connect()`: later calls do nothing. */
  let closed = false
  const isClosing = () => closing !== null
  /**
   * The lifecycle every operation of this producer captures when it starts and checks after each
   * await; a forced close closes it, and a `connect()` after a close replaces it.
   */
  let generation = new Generation()
  const getGeneration = () => generation
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

  const partitioner = (
    createPartitioner ??
    UniformStickyPartitioner({ stickyBytes: batchSize, adaptive: DEFAULTS.adaptive })
  )()
  const requestBudget = new RequestBudget({ maxRequestSize, clientId, transactionalId })
  // The retry options the producer was given, and no others: the requests of the transaction
  // protocol retry with these and the retrier's defaults.
  const retryOptions = definedKeys(Object.assign({}, cluster.retry, retry))
  // The retry options of a send's delivery: the delivery defaults under each key given.
  const deliveryRetry = { ...DELIVERY_RETRY, ...retryOptions }
  const budget = deliveryTimeout ?? deliveryRetry.maxTotalTime
  const retrier = createRetry(
    budget === undefined
      ? deliveryRetry
      : { ...deliveryRetry, maxTotalTime: Math.min(MAX_DELAY, Math.max(1, budget)) }
  )
  const instrumentationEmitter = rootInstrumentationEmitter || new InstrumentationEventEmitter()

  /**
   * Makes a fenced initial `InitProducerId` fatal for the whole producer, as Java's one
   * `TransactionManager` is: both identities are armed with the same error, the lifecycle is
   * revoked with it, so every retry, continuation and initialization waiting on it ends with that
   * error and nothing is written again, and every send still held, written or not, rejects with it.
   * The first fencing wins: later answers find the lifecycle closed. `disconnect()` stays usable.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   * @param {import('../errors').KafkaJSApplicationRecoverableError} armed the error the fenced
   *   identity armed
   * @returns {void}
   */
  const fence = (armed) => {
    for (const manager of [idempotentEosManager, transactionalEosManager]) {
      manager?.stateManager.arm(armed)
    }
    generation.close(armed)
    admission.cancel(armed)
    ownMessageProducer.forceClose(armed)
    transactionalMessageProducer?.forceClose(armed)
  }

  const idempotentEosManager = createEosManager({
    logger,
    cluster,
    transactionTimeout,
    transactional: false,
    transactionalId,
    retry: retryOptions,
    deliveryRetries: deliveryRetry.retries,
    getGeneration,
    onFenced: fence,
  })

  const ownMessageProducer = createMessageProducer({
    logger,
    cluster,
    partitioner,
    eosManager: idempotentEosManager,
    idempotence,
    retrier,
    retry: deliveryRetry,
    deliveryTimeout,
    compressionLevels,
    linger,
    batchSize,
    admission,
    budget: requestBudget,
    getConnectionStatus: () => connectionStatus,
    isClosing,
    instrumentationEmitter,
    INITIALIZED,
    getGeneration,
  })
  const { send, sendBatch, ensureInitialized, depthOf } = ownMessageProducer

  let transactionalEosManager
  let transactionalMessageProducer
  /**
   * @type {{ abort: () => Promise<void>, ending: () => (Promise<void>|null), ended: () => boolean }|null}
   *   the abort of the last transaction object, with its end under way and whether it has ended:
   *   `disconnect()` aborts a transaction left open through it
   */
  let lastTransaction = null

  /**
   * Refuses work if either identity is fatal, preserving the armed object and its cause.
   * Cleanup and event observations remain usable; reconnect never resets fatal state.
   * @returns {void} when neither identity is fatal
   * @throws {import('../errors').KafkaJSApplicationRecoverableError} the armed fatal error
   */
  const throwIfFatal = () => {
    for (const manager of [idempotentEosManager, transactionalEosManager]) {
      if (manager?.stateManager.state() === 'FATAL_ERROR') {
        throw manager.stateManager.transaction.armedError()
      }
    }
  }

  /**
   * Checks fatal state and forwards synchronously, preserving admission and snapshot timing. The
   * operation's own promise is returned, so a pending send holds no wrapper of its own.
   * @param {(...args: any[]) => Promise<any>} operation public producer operation, which rejects
   *   instead of throwing
   * @returns {(...args: any[]) => Promise<any>} guarded operation, forwarding arguments and result
   * @throws {Error} through the returned promise; the armed error or the operation's error
   */
  const guardFatal =
    (operation) =>
    (...args) => {
      try {
        throwIfFatal()
      } catch (error) {
        return Promise.reject(error)
      }
      return operation(...args)
    }

  attachBuiltIn(partitioner, {
    loadOf: (topic, partition) =>
      depthOf(topic, partition) + (transactionalMessageProducer?.depthOf(topic, partition) ?? 0),
    rackOf: (nodeId) => cluster.brokerPool?.brokers[nodeId]?.connectionPool.rack ?? undefined,
    rackId,
    rackAware,
  })
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
      if (committing === commit) committing = null
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
   * will raise an exception. `isActive()` is false once the transaction has ended, and also while a
   * commit whose outcome is unknown is pending. A pending commit has not ended: call `commit()`
   * again while `error.retriable` is set.
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
   * @throws {Error} the close error when the producer closed while the transaction was beginning.
   *   A begin that had reached `IN_TRANSACTION` locally is rolled back to `READY` without EndTxn,
   *   so a later `transaction()` is not blocked by it
   *
   * @typedef {Object} Transaction
   * @property {Function} send  Identical to the producer "send" method
   * @property {Function} sendBatch Identical to the producer "sendBatch" method
   * @property {Function} sendOffsets Add consumed offsets to the transaction
   * @property {Function} abort Abort the transaction
   * @property {Function} commit  Commit the transaction
   * @property {Function} isActive  Whether the transaction is active: false once it has ended and
   *   while a commit of unknown outcome is pending
   */
  const transaction = async () => {
    const lifecycle = generation
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
        deliveryRetries: deliveryRetry.retries,
        getGeneration,
        onFenced: fence,
      })
    transactionalMessageProducer =
      transactionalMessageProducer ||
      createMessageProducer({
        logger,
        cluster,
        partitioner,
        retrier,
        retry: deliveryRetry,
        deliveryTimeout,
        compressionLevels,
        linger,
        batchSize,
        admission,
        budget: requestBudget,
        eosManager: transactionalEosManager,
        idempotence,
        getConnectionStatus: () => connectionStatus,
        isClosing,
        instrumentationEmitter,
        INITIALIZED,
        getGeneration,
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
    // IN_TRANSACTION has no edge to READY. Begin sent no EndTxn and published no facade,
    // so step ABORTING then READY before the close error. abort() would write.
    if (lifecycle.closed && transactionalEosManager.isInTransaction()) {
      stateManager.transitionTo(ABORTING)
      stateManager.transitionTo(READY)
    }
    lifecycle.check()
    throwIfFatal()

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
      const lifecycle = getGeneration()
      const previous = ending === null ? Promise.resolve() : ending.catch(() => {})
      ending = previous.then(async () => {
        if (transactionDidEnd) {
          throw endedTransactionError()
        }

        lifecycle.check()
        await Promise.allSettled(pending)
        lifecycle.check()
        await end()
        transactionDidEnd = true
      })
      return ending
    }

    /**
     * Abort the ongoing transaction: rejects the batches still queued - accumulated ones
     * included - waits for the rest of the transaction's operations, then aborts it. It ends the
     * transaction even when the abort fails, unless the producer is unusable. A rejected batch
     * that was already sent once leaves its sequence spent, so the abort then moves the producer
     * to a new epoch (see `abort()` on the eosManager). Called while a `commit()` of the
     * transaction is under way, it rejects no batch and waits for the commit: the commit keeps
     * every record, and the abort is refused if the commit ended the transaction. `disconnect()`
     * calls it for a transaction left open.
     *
     * @returns {Promise<void>}
     * @throws {KafkaJSNonRetriableError} If transaction has ended
     * @throws {import('../errors').KafkaJSApplicationRecoverableError} once the producer is
     *   unusable
     */
    const abort = () => {
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
    }
    lastTransaction = { abort, ending: () => ending, ended: () => transactionDidEnd }

    const isActive = () => transactionalEosManager.isInTransaction() && !transactionDidEnd

    return {
      sendBatch: transactionGuard('sendBatch', sendBatchTxn),
      send: transactionGuard('send', sendTxn),
      abort,
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
       * enter the shared committed cache only after this transaction's commit is confirmed.
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
   * is refused, then leaves the producer `DISCONNECTING`.
   * @returns {Promise<void>} never rejects
   */
  const deliverAccumulated = () =>
    Promise.all([ownMessageProducer.close(), transactionalMessageProducer?.close()]).then(() => {
      connectionStatus = CONNECTION_STATUS.DISCONNECTING
    })

  /**
   * Aborts a transaction left open - `IN_TRANSACTION` once its sends settled, or an armed
   * `ABORTABLE_ERROR` - as the Java producer's shutdown does, or waits for the abort already under
   * way. A transaction that is committing is never aborted. A failed abort is logged.
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
   * @returns {Promise<void>} never rejects
   */
  const abortOpenTransaction = async () => {
    const state = transactionalEosManager?.stateManager.state()
    if (lastTransaction === null || lastTransaction.ended()) {
      return
    }

    try {
      if (state === IN_TRANSACTION || state === ABORTABLE_ERROR) {
        await lastTransaction.abort()
      } else if (state === ABORTING) {
        await lastTransaction.ending()
      }
    } catch (error) {
      logger.warn('Aborting the open transaction failed while the producer was disconnecting', {
        transactionalId,
        code: codeOf(error),
        error: error.message,
      })
    }
  }

  /**
   * The graceful half of a close: delivers what is accumulated, settles the commit and aborts an
   * open transaction, each only while the lifecycle is open. Operational failures are logged.
   * @param {Generation} lifecycle the lifecycle the close began under
   * @returns {Promise<void>} never rejects
   */
  const closeGracefully = async (lifecycle) => {
    try {
      const joined = committing
      await deliverAccumulated()
      if (lifecycle.closed) return
      await settleCommit(joined)
      // eslint-disable-next-line no-unmodified-loop-condition -- joinable() clears it while the loop awaits
      while (committing !== null && !lifecycle.closed) {
        await committing.catch(() => {})
      }
      if (lifecycle.closed) return
      await abortOpenTransaction()
      transactionalEosManager?.stateManager.offsets.release()
    } catch (error) {
      logger.warn('The producer could not finish delivering while it was disconnecting', {
        error: error.message,
      })
    }
  }

  /**
   * One close episode. The `timeout` runs from `startedAt`; at its end, or at once for 0, the close
   * is forced in one synchronous step: the lifecycle is closed, which cancels the retry timers and
   * fences every continuation, the admission waiters, the sends not yet written and the ones on
   * the wire are ended locally, the staged offsets are forfeited, and the connections close
   * without draining. Nothing it settles claims a remote outcome. The episode ends when the
   * connections have closed, whatever failed on the way.
   * @param {number} startedAt epoch ms of the call that began it
   * @param {number|undefined} timeout ms from `startedAt`; undefined waits for natural completion
   * @returns {Promise<void>} never rejects
   */
  const close = async (startedAt, timeout) => {
    const lifecycle = generation
    const remaining = () =>
      timeout === undefined ? Infinity : Math.max(0, startedAt + timeout - Date.now())
    // Runs concurrently with the core close and never shortens or extends its budget.
    const finalTelemetry =
      telemetry?.close({
        deadline: timeout === undefined ? Infinity : startedAt + timeout,
        remaining,
        zero: timeout === 0,
      }) ?? null
    let cleanup = null
    /** Starts the one cluster disconnect of the episode; later calls share it. */
    const disconnectCluster = (drainTimeout) =>
      (cleanup ??= (async () =>
        cluster.disconnect(...(drainTimeout === undefined ? [] : [{ drainTimeout }])))().catch(
        (error) => {
          logger.warn('Disconnecting the cluster failed', { error: error.message })
        }
      ))
    let forced = false
    let forcedDone
    const forcedSettled = new Promise((resolve) => {
      forcedDone = resolve
    })
    const force = () => {
      if (forced) return
      forced = true
      lifecycle.close(
        new KafkaJSProducerClosedError('The producer was closed', { outcome: 'unknown' })
      )
      admission.cancel(
        new KafkaJSProducerClosedError('The producer was closed before admission completed', {
          outcome: 'unsent',
        })
      )
      ownMessageProducer.forceClose()
      transactionalMessageProducer?.forceClose()
      transactionalEosManager?.stateManager.offsets.release()
      disconnectCluster(0).then(forcedDone)
    }

    let timer = null
    if (timeout === 0) {
      force()
    } else if (timeout !== undefined) {
      timer = setTimeout(force, remaining())
    }

    try {
      await Promise.race([forced ? forcedSettled : closeGracefully(lifecycle), forcedSettled])
      await disconnectCluster(timeout === undefined ? undefined : remaining())
    } finally {
      clearTimeout(timer)
      if (finalTelemetry) await finalTelemetry
      lifecycle.close(
        new KafkaJSProducerClosedError('The producer was disconnected', { outcome: 'unknown' })
      )
      connectionStatus = CONNECTION_STATUS.DISCONNECTED
      closing = null
      closed = true
      instrumentationEmitter.emit(DISCONNECT)
    }
  }

  return {
    ...telemetry?.methods(),
    /**
     * Connects the producer, starting a new lifecycle after a close: it first joins a close still
     * under way, and opens no connection while that close owns the cleanup. Operations of an
     * earlier lifecycle stay fenced.
     * @returns {Promise}
     * @throws {import('../errors').KafkaJSApplicationRecoverableError} as a rejection, once the
     *   producer is unusable: a connect never revives it
     */
    connect: guardFatal(async () => {
      // eslint-disable-next-line no-unmodified-loop-condition -- the close clears it when it ends
      while (closing !== null) {
        await closing
      }
      if (closed || generation.closed) {
        generation = new Generation()
        closed = false
      }
      const lifecycle = generation
      await cluster.connect({ fence: lifecycle })
      lifecycle.check()
      connectionStatus = CONNECTION_STATUS.CONNECTED
      // CONNECT stays exactly where it is: it means "the cluster connection is up", and existing
      // listeners depend on that ordering. INITIALIZED is the new "safe to send()" signal.
      instrumentationEmitter.emit(CONNECT)

      // Shares the same deduped ensureInitialized() as send()/sendBatch(), so an un-awaited
      // connect() racing a concurrent send() issues exactly one InitProducerId request instead
      // of two (which would otherwise allocate two producer ids and reset sequences underneath
      // in-flight sends).
      await lifecycle.wait(ensureInitialized())
      if (!lifecycle.closed) telemetry?.resume()
    }),
    /**
     * Resolves once every send made before the call, the producer's own and its transactions',
     * has settled, after handing the accumulated batches on at once. A send that failed does not
     * reject it: its own promise carries that. Sends made later are not waited for.
     * @param {{ timeout?: number }} [options] `timeout` in ms, an integer from 0 to 2147483647: when
     *   it passes first the call rejects, and every send keeps running; omitted, there is no limit
     * @returns {Promise<void>}
     * @throws {KafkaJSInvalidConfigurationError} as a rejection, before anything changes, for an
     *   argument that is not an object or whose `timeout` is not valid
     * @throws {KafkaJSTimeout} as a rejection, when `timeout` passes first
     * @throws {import('../errors').KafkaJSApplicationRecoverableError} as a rejection, once the
     *   producer is unusable, for valid options and whatever is unsettled
     */
    flush: (options) => {
      let timeout
      try {
        timeout = timeoutOf(options, false)
        throwIfFatal()
      } catch (error) {
        return Promise.reject(error)
      }

      const snapshot = [
        ...ownMessageProducer.unsettled(),
        ...(transactionalMessageProducer?.unsettled() || []),
      ]
      if (snapshot.length === 0) return Promise.resolve()
      const until = Promise.allSettled(snapshot)
      ownMessageProducer.flush(until)
      transactionalMessageProducer?.flush(until)
      if (timeout === undefined) return until.then(() => {})
      if (timeout === 0) return Promise.reject(new KafkaJSTimeout('Producer flush timed out'))

      let timer
      return new Promise((resolve, reject) => {
        timer = setTimeout(() => reject(new KafkaJSTimeout('Producer flush timed out')), timeout)
        until.then(() => resolve())
      }).finally(() => clearTimeout(timer))
    },
    /**
     * Disconnects the producer. It first delivers every batch still lingering, its own and its
     * transaction's, and the sends begun before it, refusing new sends meanwhile; nothing
     * accumulated is lost. A transaction's commit under way when it begins is waited for, and not
     * sent again if it ends pending; a pending one that is not under way is sent once more first.
     * It then waits for any commit the application starts while it waits, so the connections never
     * close while a commit is under way, and aborts a transaction left open. Offsets staged by a
     * transaction's `sendOffsets` that no confirmed commit has published by then are forfeited.
     *
     * With `timeout`, the close is forced when it passes (at once for 0): sends not yet written
     * reject with `KafkaJSProducerClosedError` `outcome: 'unsent'`, sends written at least once
     * with `'unknown'`, pending retries are cancelled, no old operation opens a connection or
     * writes, an unknown commit stays unknown and is not aborted, and the connections close. The
     * first call's `timeout` governs concurrent ones, which share its one cleanup; a call after
     * the close completed does nothing until `connect()`. The call resolves whatever the
     * cleanup answers.
     * @param {{ timeout?: number }} [options] `timeout` in ms, an integer from 0 to 2147483647; an
     *   argument that is not an object is ignored, so the method can be a signal handler
     * @returns {Promise<void>}
     * @throws {KafkaJSInvalidConfigurationError} as a rejection, before anything changes, for an
     *   options object whose `timeout` is not valid
     */
    disconnect: (options) => {
      const startedAt = Date.now()
      let timeout
      try {
        timeout = timeoutOf(options, true)
      } catch (error) {
        return Promise.reject(error)
      }

      if (closing !== null) return closing
      if (closed) return Promise.resolve()
      closing = close(startedAt, timeout)
      return closing
    },
    /**
     * @returns {boolean} whether the producer sends idempotently; false once a configuration
     *   that does not allow it has turned idempotence off
     */
    isIdempotent: () => idempotence.isEnabled(),
    events,
    on,
    /**
     * @param {object} request send options
     * @returns {Promise<*>} metadata
     * @throws {Error} a delivery error or the producer's armed fatal error
     */
    send: guardFatal(send),
    /**
     * @param {object} request batch options
     * @returns {Promise<*>} metadata
     * @throws {Error} a delivery error or the producer's armed fatal error
     */
    sendBatch: guardFatal(sendBatch),
    /**
     * @returns {Promise<object>} new transaction
     * @throws {Error} a transaction error or the producer's armed fatal error
     */
    transaction: guardFatal(transaction),
    logger: getLogger,
  }
}

/** The accumulator's defaults, `linger` and `batchSize`, for every producer. */
createProducer.defaults = DEFAULTS

module.exports = createProducer
