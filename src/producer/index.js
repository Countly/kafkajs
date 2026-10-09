const createRetry = require('../retry')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const { DefaultPartitioner } = require('./partitioners/')
const InstrumentationEventEmitter = require('../instrumentation/emitter')
const createEosManager = require('./eosManager')
const createMessageProducer = require('./messageProducer')
const Idempotence = require('./idempotence')
const { events, wrap: wrapEvent, unwrap: unwrapEvent } = require('./instrumentationEvents')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('../errors')

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
 * @param {number} [params.linger] ms an accumulated batch waits for more records; set together
 *   with `batchSize`, or neither
 * @param {number} [params.batchSize] bytes at which an accumulated batch is sent
 * @param {number} [params.deliveryTimeout] ms one send may take, retries included; past it the
 *   send rejects with `KafkaJSDeliveryTimeout`
 * @param {InstrumentationEventEmitter} [params.instrumentationEmitter]
 *
 * @returns {import('../../types').Producer}
 * @throws {KafkaJSInvalidConfigurationError} `retry.retries` below 1 on a producer that asked for
 *   idempotence
 */
module.exports = ({
  cluster,
  logger: rootLogger,
  createPartitioner = DefaultPartitioner,
  retry = {},
  idempotent,
  transactionalId,
  transactionTimeout,
  linger,
  batchSize,
  deliveryTimeout,
  instrumentationEmitter: rootInstrumentationEmitter,
}) => {
  let connectionStatus = CONNECTION_STATUS.DISCONNECTED
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
    instrumentationEmitter,
  })

  const { send, sendBatch, ensureInitialized } = createMessageProducer({
    logger,
    cluster,
    partitioner,
    eosManager: idempotentEosManager,
    idempotence,
    retrier,
    retry: retryOptions,
    deliveryTimeout,
    getConnectionStatus: () => connectionStatus,
    instrumentationEmitter,
    INITIALIZED,
  })

  let transactionalEosManager

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
   * @returns {Promise<Transaction>}
   *
   * @typedef {Object} Transaction
   * @property {Function} send  Identical to the producer "send" method
   * @property {Function} sendBatch Identical to the producer "sendBatch" method
   * @property {Function} abort Abort the transaction
   * @property {Function} commit  Commit the transaction
   * @property {Function} isActive  Whether the transaction is active
   */
  const transaction = async () => {
    if (!transactionalId) {
      throw new KafkaJSNonRetriableError('Must provide transactional id for transactional producer')
    }

    let transactionDidEnd = false
    transactionalEosManager =
      transactionalEosManager ||
      createEosManager({
        logger,
        cluster,
        transactionTimeout,
        transactional: true,
        transactionalId,
        retry: retryOptions,
        instrumentationEmitter,
      })

    if (transactionalEosManager.isInTransaction()) {
      throw ongoingTransactionError()
    }

    // We only initialize the producer id once
    if (!transactionalEosManager.isInitialized()) {
      await transactionalEosManager.initProducerId()
    }
    try {
      await transactionalEosManager.beginTransaction()
    } catch (error) {
      // Another transaction() began while this one was finding the coordinator.
      if (transactionalEosManager.isInTransaction()) {
        throw ongoingTransactionError()
      }
      throw error
    }

    const { send: sendTxn, sendBatch: sendBatchTxn } = createMessageProducer({
      logger,
      cluster,
      partitioner,
      retrier,
      retry: retryOptions,
      deliveryTimeout,
      eosManager: transactionalEosManager,
      idempotence,
      getConnectionStatus: () => connectionStatus,
      instrumentationEmitter,
      INITIALIZED,
    })

    const isActive = () => transactionalEosManager.isInTransaction() && !transactionDidEnd

    const transactionGuard =
      (fn) =>
      (...args) => {
        if (!isActive()) {
          return Promise.reject(
            new KafkaJSNonRetriableError('Cannot continue to use transaction once ended')
          )
        }

        return fn(...args)
      }

    return {
      sendBatch: transactionGuard(sendBatchTxn),
      send: transactionGuard(sendTxn),
      /**
       * Abort the ongoing transaction.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended
       */
      abort: transactionGuard(async () => {
        await transactionalEosManager.abort()
        transactionDidEnd = true
      }),
      /**
       * Commit the ongoing transaction.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended
       */
      commit: transactionGuard(async () => {
        await transactionalEosManager.commit()
        transactionDidEnd = true
      }),
      /**
       * Sends a list of specified offsets to the consumer group coordinator, and also marks those offsets as part of the current transaction.
       *
       * @throws {KafkaJSNonRetriableError} If transaction has ended
       */
      sendOffsets: transactionGuard(async ({ consumerGroupId, topics }) => {
        await transactionalEosManager.sendOffsets({ consumerGroupId, topics })

        for (const topicOffsets of topics) {
          const { topic, partitions } = topicOffsets
          for (const { partition, offset } of partitions) {
            cluster.markOffsetAsCommitted({
              groupId: consumerGroupId,
              topic,
              partition,
              offset,
            })
          }
        }
      }),
      isActive,
    }
  }

  /**
   * @returns {Object} logger
   */
  const getLogger = () => logger

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
     * @return {Promise}
     */
    disconnect: async () => {
      connectionStatus = CONNECTION_STATUS.DISCONNECTING
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
