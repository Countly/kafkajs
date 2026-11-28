const createSendMessages = require('./sendMessages')
const createEosManager = require('./eosManager')
const Accumulator = require('./state/accumulator')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('../errors')
const sharedPromiseTo = require('../utils/sharedPromiseTo')
const { classifyAcks, classifyTimeout, classifyCompression } = require('./sendOptions')

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
 * @param {{ gzip?: number, zstd?: number }} [options.compressionLevels] per-codec levels
 * @param {number} [options.deliveryTimeout] the producer's delivery deadline in ms
 * @param {number} options.linger ms a batch waits for more records; 0 sends it at once
 * @param {number} options.batchSize bytes of keys and values at which a batch is sent
 * @param {import('./state/admission').Admission} options.admission the producer's buffer, shared
 *   by its own sends and its transactions'
 * @param {import('./state/recordSize').RequestBudget} options.budget the producer's request-size
 *   plan (`maxRequestSize`), shared the same way
 * @param {() => string} options.getConnectionStatus
 * @param {() => boolean} options.isClosing whether the producer's `disconnect()` is delivering
 *   the accumulated batches: sends are refused and batches no longer linger
 * @param {object} options.instrumentationEmitter
 * @param {string} options.INITIALIZED
 * @param {() => import('./generation')|undefined} [options.getGeneration] current producer lifecycle
 * @returns {{ send: Function, sendBatch: Function, sendersWith: Function,
 *   ensureInitialized: Function, flush: Function, close: Function, unsettled: () => Promise<void>[], forceClose: (surfaced?: Error) => void,
 *   depthOf: (topic: string, partition: number) => number }} `depthOf` counts the batches
 *   waiting for a partition (see `Accumulator#depthOf`)
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
  compressionLevels,
  linger,
  batchSize,
  admission,
  budget,
  getConnectionStatus,
  isClosing,
  instrumentationEmitter,
  INITIALIZED,
  getGeneration = () => undefined,
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
    compressionLevels,
    getConnectionStatus,
    budget,
    getGeneration,
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
        compressionLevels,
        partitioner,
        budget,
        getGeneration,
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
    partition: async (send) => {
      const generation = getGeneration()
      await ensureInitialized()
      return idempotentSend.partition(send, generation)
    },
    sendPartitioned: (request) => {
      // The idempotent path refuses the batches of a producer that is not connected itself, and
      // arms a transaction with the refusal; the plain path would send them.
      const error = idempotence.isEnabled()
        ? null
        : createSendMessages.connectionStatusError(getConnectionStatus())
      if (error === null) {
        return sendPath().sendPartitioned(request)
      }
      request.batches.forEach((batch) => batch.retire())
      return Promise.reject(error)
    },
    lingers: () =>
      !isClosing() && hurried === 0 && getConnectionStatus() === CONNECTION_STATUS.CONNECTED,
    holds: () => idempotence.isEnabled(),
    // What the client already knows about the leader, never fetched
    leaderOf: (topic, partition) => cluster.cachedLeader(topic, partition),
    deadline: deliveryTimeout ?? retry.maxTotalTime,
    expired: (error) => idempotentSend.inTransaction(error),
    admission,
    budget,
    pinner: cluster.releaseTopic ? cluster : undefined,
    getGeneration,
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
   * The sends begun so far that have not settled, as the snapshot a `flush` waits for.
   * @returns {Promise<void>[]} at most one promise, which settles after the promise of every send
   *   it covers; none when no send is unsettled (see `Accumulator#unsettled`)
   */
  const unsettled = () => accumulator.unsettled()

  /**
   * Ends every send this producer still holds locally with `outcome: 'unsent'` or `'unknown'`
   * (see `Accumulator#forceClose` and the send paths' `forceClose`), for a forced close. A request
   * already on the wire keeps its memory until its terminal callback.
   * @param {Error} [surfaced] what the sends reject with instead, for a producer made unusable
   * @returns {void}
   */
  const forceClose = (surfaced) => {
    accumulator.forceClose(surfaced)
    idempotentSend.forceClose(surfaced)
    plainSend?.forceClose(surfaced)
  }

  /**
   * A transient InitProducerId failure during connect() must not poison the producer for the
   * rest of the process lifetime: re-attempt it on the next send instead (Countly patch 2g).
   * Shared (via `sharedPromiseTo`) between connect() and send()/sendBatch(), so whichever one
   * gets there first is the only one to issue an InitProducerId request; the others await the
   * same in-flight promise instead of racing separate ones. Emits INITIALIZED once the request
   * that actually initialized the producer succeeds, regardless of which caller triggered it.
   * The promise is shared only within one producer lifecycle: a closed lifecycle's request, still
   * held, is never joined by the next one, and its answer emits nothing.
   */
  const initialize = sharedPromiseTo(
    async (generation) => {
      generation?.check()
      if (idempotence.isEnabled() && !eosManager.isInitialized()) {
        logger.debug('Idempotent producer is not initialized, requesting a producer id')
        await eosManager.initProducerId()
        generation?.check()
        instrumentationEmitter.emit(INITIALIZED)
      }
    },
    (generation) => generation
  )
  const ensureInitialized = () => initialize(getGeneration())

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
   * Each option is read once and converted once, here, before anything is admitted (see
   * `sendOptions.js`); the codec factory is taken from the registry here and pinned to the send.
   * The records are read once, at this call (`Accumulator.snapshot`): changing a record, its
   * headers or a `Date` or `Long` timestamp afterwards changes neither what is sent nor what is
   * charged against `bufferMemory`. The bytes of a Buffer are not copied, except a small view of a
   * large backing store: all of those in the send are copied into one store (`SendCopy`).
   * @param {SendBatchRequest} request
   * @returns {Promise}
   * @throws {KafkaJSInvalidConfigurationError} as a rejection, an option `sendOptions.js` refuses
   */
  const sendBatch = (request) => sendBatchBy(request, undefined)

  /**
   * `sendBatch` for a caller that can lose the right to send while the call's own option and
   * record conversions run: its `refusal` is asked again once they are done.
   * @param {SendBatchRequest} request
   * @param {(() => ?Error)|undefined} refusal
   * @returns {Promise}
   */
  const sendBatchBy = async (request, refusal) => {
    // The delivery deadline counts from here, the option reads included.
    const startedAt = Date.now()
    const { acks, timeout, compression, topicMessages = [] } = request
    return produce(startedAt, acks, timeout, compression, topicMessages, refusal)
  }

  /**
   * Converts a send's options, validates it and appends it to the accumulator.
   * @param {number} startedAt when the public call began, in epoch ms
   * @param {*} acksOption
   * @param {*} timeoutOption
   * @param {*} compressionOption
   * @param {TopicMessages[]} given the caller's records
   * @param {(() => ?Error)|undefined} refusal asked once the caller's code has run (the options'
   *   conversions, the records' getters) and before anything changes: the error the send is
   *   refused with, or null
   * @returns {Promise<object[]>}
   * @throws {Error} what `refusal` returned
   */
  const produce = (startedAt, acksOption, timeoutOption, compressionOption, given, refusal) => {
    const acks = classifyAcks(acksOption)
    const timeout = classifyTimeout(timeoutOption)
    const { bits: compression, factory: codec } = classifyCompression(compressionOption)
    // Every check below, the charge and the encoder read this copy, never the caller's records.
    const topicMessages = Accumulator.snapshot(given)

    const refused = refusal?.() ?? null
    if (refused !== null) {
      throw refused
    }

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

      const index = messages.findIndex((message) => message.value === undefined)
      if (index >= 0) {
        throw new KafkaJSNonRetriableError(
          `Invalid message without value for topic "${topic}" at index ${index}`
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
    return accumulator.append({
      acks,
      timeout,
      compression,
      codec,
      topicMessages: mergedTopicMessages,
      startedAt,
    })
  }

  /**
   * One topic's `sendBatch`, its options read and converted the same way.
   * @param {ProduceRequest} request
   * @returns {Promise}
   * @throws {KafkaJSInvalidConfigurationError} as a rejection, an option `sendOptions.js` refuses
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
  const send = (request) => sendBy(request, undefined)

  /**
   * `send` for a caller that can lose the right to send while the call's own conversions run.
   * @param {ProduceRequest} request
   * @param {(() => ?Error)|undefined} refusal see `produce`
   * @returns {Promise}
   */
  const sendBy = async (request, refusal) => {
    const startedAt = Date.now()
    const { acks, timeout, compression, topic, messages } = request
    return produce(startedAt, acks, timeout, compression, [{ topic, messages }], refusal)
  }

  /**
   * The two sends of a caller that must be asked again before a send is admitted, such as a
   * transaction, which can end while a send's option conversions run. They take exactly the
   * request: nothing a caller of the public methods passes can reach `refusal`.
   * @param {(method: 'send'|'sendBatch') => ?Error} refusal the error the named send must be
   *   refused with now, or null
   * @returns {{ send: (request: ProduceRequest) => Promise, sendBatch: (request: SendBatchRequest) => Promise }}
   */
  const sendersWith = (refusal) => {
    const sendRefusal = () => refusal('send')
    const sendBatchRefusal = () => refusal('sendBatch')
    return {
      send: (request) => sendBy(request, sendRefusal),
      sendBatch: (request) => sendBatchBy(request, sendBatchRefusal),
    }
  }

  return {
    send,
    sendBatch,
    sendersWith,
    ensureInitialized,
    flush,
    close,
    unsettled,
    forceClose,
    depthOf: (topic, partition) => accumulator.depthOf(topic, partition),
  }
}
