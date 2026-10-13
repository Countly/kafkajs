const createSendMessages = require('./sendMessages')
const createEosManager = require('./eosManager')
const Accumulator = require('./state/accumulator')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('../errors')
const sharedPromiseTo = require('../utils/sharedPromiseTo')

/**
 * `send()`/`sendBatch()` over one eosManager: validation, the `acks` rule of an idempotent
 * producer, the shared lazy `InitProducerId`, and the accumulator every send goes through.
 * @param {object} options
 * @param {import('../../types').Logger} options.logger
 * @param {import('../../types').Cluster} options.cluster
 * @param {Function} options.partitioner
 * @param {object} options.eosManager
 * @param {import('./idempotence')} options.idempotence the producer's idempotence, shared by its
 *   own sends and its transactions'
 * @param {import('../retry').Retrier} options.retrier
 * @param {import('../../types').RetryOptions} options.retry the producer's retry options, merged
 *   over the client's
 * @param {number} [options.deliveryTimeout] the producer's delivery deadline in ms
 * @param {number} options.linger ms a batch waits for more records; 0 sends it at once
 * @param {number} options.batchSize bytes of keys and values at which a batch is sent
 * @param {() => string} options.getConnectionStatus
 * @param {() => boolean} options.isClosing whether the producer's `disconnect()` is delivering
 *   the accumulated batches: sends are refused and batches no longer linger
 * @param {object} options.instrumentationEmitter
 * @param {string} options.INITIALIZED
 */
module.exports = ({
  logger,
  cluster,
  partitioner,
  eosManager,
  idempotence,
  retrier,
  retry,
  deliveryTimeout,
  linger,
  batchSize,
  getConnectionStatus,
  isClosing,
  instrumentationEmitter,
  INITIALIZED,
}) => {
  const idempotentSend = createSendMessages({
    logger,
    cluster,
    retrier,
    partitioner,
    eosManager,
    idempotence,
    retry,
    deliveryTimeout,
    getConnectionStatus,
  })
  let plainSend = null
  /** How many `flush(until)` calls are waiting for their `until`: batches linger only at 0. */
  let hurried = 0

  /**
   * The send path for a send starting now. Once idempotence is off for a producer whose producer
   * id was already allocated, the sends go through a manager that never allocates one, so they
   * carry the non-idempotent producer id -1, epoch 0 and sequence 0 of an `idempotent: false`
   * producer - never that producer id without a sequence.
   * @returns {ReturnType<typeof createSendMessages>}
   */
  const sendPath = () => {
    if (idempotence.isEnabled() || !eosManager.isInitialized()) {
      return idempotentSend
    }

    plainSend =
      plainSend ||
      createSendMessages({
        logger,
        cluster,
        retrier,
        retry,
        deliveryTimeout,
        partitioner,
        eosManager: createEosManager({ logger, cluster, transactional: false }),
      })
    return plainSend
  }

  const validateConnectionStatus = () => {
    const status = isClosing() ? CONNECTION_STATUS.DISCONNECTING : getConnectionStatus()
    const error = createSendMessages.connectionStatusError(status)
    if (error !== null) {
      throw error
    }
  }

  const accumulator = new Accumulator({
    linger,
    batchSize,
    partition: async (topicMessages) => {
      await ensureInitialized()
      return idempotentSend.partition(topicMessages)
    },
    sendPartitioned: (request) => {
      // The idempotent path refuses the batches of a producer that is not connected itself, and
      // arms a transaction with the refusal; the plain path would send them.
      const error = idempotence.isEnabled()
        ? null
        : createSendMessages.connectionStatusError(getConnectionStatus())
      return error === null ? sendPath().sendPartitioned(request) : Promise.reject(error)
    },
    lingers: () =>
      !isClosing() && hurried === 0 && getConnectionStatus() === CONNECTION_STATUS.CONNECTED,
    holds: () => idempotence.isEnabled(),
    // What the client already knows about the leader, never fetched
    leaderOf: (topic, partition) => cluster.cachedLeader(topic, partition),
    deadline: deliveryTimeout ?? retry.maxTotalTime,
  })

  /**
   * Hands every accumulated batch to the send path now and, until `until` settles, every batch
   * that forms meanwhile as soon as it forms - the sends of a transaction that is ending wait for
   * no `linger`.
   * @param {Promise<unknown>} [until]
   * @returns {Promise<void>} settles once the batches pending now have settled; never rejects
   */
  const flush = (until) => {
    if (until !== undefined) {
      hurried += 1
      const done = () => {
        hurried -= 1
      }
      until.then(done, done)
    }
    return accumulator.flush()
  }

  /**
   * Delivers every accumulated batch before the producer disconnects: hands them on now and
   * waits until nothing is left to deliver, the batches of sends that began before the close and
   * join one only now included. The producer refuses new sends meanwhile (`isClosing`).
   * @returns {Promise<void>} never rejects
   */
  const close = () => accumulator.close()

  /**
   * A transient InitProducerId failure during connect() must not poison the producer for the
   * rest of the process lifetime: re-attempt it on the next send instead (Countly patch 2g).
   * Shared (via `sharedPromiseTo`) between connect() and send()/sendBatch(), so whichever one
   * gets there first is the only one to issue an InitProducerId request; the others await the
   * same in-flight promise instead of racing separate ones. Emits INITIALIZED once the request
   * that actually initialized the producer succeeds, regardless of which caller triggered it.
   */
  const ensureInitialized = sharedPromiseTo(async () => {
    if (idempotence.isEnabled() && !eosManager.isInitialized()) {
      logger.debug('Idempotent producer is not initialized, requesting a producer id')
      await eosManager.initProducerId()
      instrumentationEmitter.emit(INITIALIZED)
    }
  })

  /**
   * @typedef {Object} TopicMessages
   * @property {string} topic
   * @property {Array} messages An array of objects with "key" and "value", example:
   *                         [{ key: 'my-key', value: 'my-value'}]
   *
   * @typedef {Object} SendBatchRequest
   * @property {Array<TopicMessages>} topicMessages
   * @property {number} [acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   *
   * @property {number} [timeout=30000] The time to await a response in ms
   * @property {Compression.Types} [compression=Compression.Types.None] Compression codec
   *
   * @param {SendBatchRequest}
   * @returns {Promise}
   */
  const sendBatch = async ({ acks = -1, timeout, compression, topicMessages = [] }) => {
    if (topicMessages.some(({ topic }) => !topic)) {
      throw new KafkaJSNonRetriableError(`Invalid topic`)
    }

    if (idempotence.isEnabled() && acks !== -1) {
      if (idempotence.isExplicit()) {
        throw new KafkaJSInvalidConfigurationError(
          `Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees`
        )
      }

      // Accumulated batches still carry the producer id: they go out idempotently first.
      flush()
      idempotence.disable(
        'acks',
        `Idempotence disabled: a send asked for acks ${acks}, and an idempotent producer needs ` +
          'acks -1 (all). Send with acks -1 to keep idempotence, or create the producer with ' +
          'idempotent: false to silence this warning.'
      )
    }

    for (const { topic, messages } of topicMessages) {
      if (!messages) {
        throw new KafkaJSNonRetriableError(
          `Invalid messages array [${messages}] for topic "${topic}"`
        )
      }

      const messageWithoutValue = messages.find((message) => message.value === undefined)
      if (messageWithoutValue) {
        throw new KafkaJSNonRetriableError(
          `Invalid message without value for topic "${topic}": ${JSON.stringify(
            messageWithoutValue
          )}`
        )
      }
    }

    validateConnectionStatus()

    const mergedTopicMessages = topicMessages.reduce((merged, { topic, messages }) => {
      const index = merged.findIndex(({ topic: mergedTopic }) => topic === mergedTopic)

      if (index === -1) {
        merged.push({ topic, messages })
      } else {
        merged[index].messages = [...merged[index].messages, ...messages]
      }

      return merged
    }, [])

    // The accumulator acquires the producer id itself, so `close()` waits for this send from here.
    return await accumulator.append({
      acks,
      timeout,
      compression,
      topicMessages: mergedTopicMessages,
    })
  }

  /**
   * @param {ProduceRequest} ProduceRequest
   * @returns {Promise}
   *
   * @typedef {Object} ProduceRequest
   * @property {string} topic
   * @property {Array} messages An array of objects with "key" and "value", example:
   *                         [{ key: 'my-key', value: 'my-value'}]
   * @property {number} [acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   * @property {number} [timeout=30000] The time to await a response in ms
   * @property {Compression.Types} [compression=Compression.Types.None] Compression codec
   */
  const send = async ({ acks, timeout, compression, topic, messages }) => {
    const topicMessage = { topic, messages }
    return sendBatch({
      acks,
      timeout,
      compression,
      topicMessages: [topicMessage],
    })
  }

  return {
    send,
    sendBatch,
    ensureInitialized,
    flush,
    close,
  }
}
