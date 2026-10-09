const createSendMessages = require('./sendMessages')
const createEosManager = require('./eosManager')
const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSInvalidConfigurationError,
} = require('../errors')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const sharedPromiseTo = require('../utils/sharedPromiseTo')

/**
 * `send()`/`sendBatch()` over one eosManager: validation, the `acks` rule of an idempotent
 * producer, the shared lazy `InitProducerId`, and the send itself.
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
 * @param {() => string} options.getConnectionStatus
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
  getConnectionStatus,
  instrumentationEmitter,
  INITIALIZED,
}) => {
  const idempotentSend = createSendMessages({ logger, cluster, retrier, partitioner, eosManager })
  let plainSend = null

  /**
   * The send for this call. Once idempotence is off for a producer whose producer id was already
   * allocated, the sends go through a manager that never allocates one, so they carry the
   * non-idempotent producer id -1, epoch 0 and sequence 0 of an `idempotent: false` producer.
   * @param {object} request
   * @returns {Promise<object[]>}
   */
  const sendMessages = (request) => {
    if (idempotence.isEnabled() || !eosManager.isInitialized()) {
      return idempotentSend(request)
    }

    plainSend =
      plainSend ||
      createSendMessages({
        logger,
        cluster,
        retrier,
        partitioner,
        eosManager: createEosManager({ logger, cluster, transactional: false }),
      })
    return plainSend(request)
  }

  const validateConnectionStatus = () => {
    const connectionStatus = getConnectionStatus()

    switch (connectionStatus) {
      case CONNECTION_STATUS.DISCONNECTING:
        throw new KafkaJSNonRetriableError(
          `The producer is disconnecting; therefore, it can't safely accept messages anymore`
        )
      case CONNECTION_STATUS.DISCONNECTED:
        throw new KafkaJSError('The producer is disconnected')
    }
  }

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
    await ensureInitialized()

    const mergedTopicMessages = topicMessages.reduce((merged, { topic, messages }) => {
      const index = merged.findIndex(({ topic: mergedTopic }) => topic === mergedTopic)

      if (index === -1) {
        merged.push({ topic, messages })
      } else {
        merged[index].messages = [...merged[index].messages, ...messages]
      }

      return merged
    }, [])

    return await sendMessages({
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
  }
}
