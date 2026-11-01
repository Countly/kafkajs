const { inspect } = require('util')
const {
  createLogger,
  LEVELS: { INFO },
} = require('./loggers')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('./errors')

const InstrumentationEventEmitter = require('./instrumentation/emitter')
const channels = require('./instrumentation/channels')
const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const { ConnectionSetupPolicy } = require('./network/connectionSetup')
const createProducer = require('./producer')
const { minimumRequestSize } = require('./producer/state/recordSize')
const { MAX_DELAY } = require('./producer/deadline')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')
const defaultSocketFactory = require('./network/socketFactory')
const {
  INT_32_MAX_VALUE,
  DEFAULT_MAX_RESPONSE_SIZE,
  DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES,
} = require('./constants')

const PRIVATE = {
  CREATE_CLUSTER: Symbol('private:Kafka:createCluster'),
  CLUSTER_RETRY: Symbol('private:Kafka:clusterRetry'),
  LOGGER: Symbol('private:Kafka:logger'),
  OFFSETS: Symbol('private:Kafka:offsets'),
  OPERATIONS: Symbol('private:Kafka:operations'),
  MAX_RESPONSE_SIZE: Symbol('private:Kafka:maxResponseSize'),
  REQUEST_TIMEOUT: Symbol('private:Kafka:requestTimeout'),
  CLIENT_ID: Symbol('private:Kafka:clientId'),
}

const DEFAULT_METADATA_MAX_AGE = 300000
const DEFAULT_MAX_IN_FLIGHT_REQUESTS = 5
// The Cluster's own default for `requestTimeout`.
const DEFAULT_REQUEST_TIMEOUT = 30000
// Java's MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE.
// @see clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java (Apache Kafka 4.3)
const MAX_IN_FLIGHT_REQUESTS_FOR_IDEMPOTENCE = 5
// A response body holds at least its 4-byte correlation id.
const MIN_MAX_RESPONSE_SIZE = 4
const DEFAULT_METADATA_RECOVERY_STRATEGY = 'rebootstrap'
const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS = 300000
const METADATA_RECOVERY_STRATEGIES = ['rebootstrap', 'none']

/**
 * Accept the documented values for maxInFlightRequests: null (unbounded) or an integer >= 1.
 * Zero would never send anything, so it is rejected up front instead of deadlocking later.
 * @param {number|null|undefined} value
 * @returns {number|null}
 * @throws {KafkaJSNonRetriableError} on 0, negative, fractional or non-numeric values
 */
const validateMaxInFlightRequests = (value) => {
  if (value === null) return null
  if (Number.isInteger(value) && value >= 1) return value
  throw new KafkaJSNonRetriableError(
    `maxInFlightRequests must be null (unbounded) or an integer >= 1, got ${inspect(value)}`
  )
}

/**
 * Accept the documented values for maxResponseSize: an integer from 4 (a correlation id) to
 * 2147483647 (the largest size a frame's signed length prefix can hold).
 * @param {number} value
 * @returns {number}
 * @throws {KafkaJSInvalidConfigurationError} on anything else, naming the option
 */
const validateMaxResponseSize = (value) => {
  if (Number.isInteger(value) && value >= MIN_MAX_RESPONSE_SIZE && value <= INT_32_MAX_VALUE) {
    return value
  }

  throw new KafkaJSInvalidConfigurationError(
    `maxResponseSize must be an integer from ${MIN_MAX_RESPONSE_SIZE} to ${INT_32_MAX_VALUE}, got ${inspect(value)}`
  )
}

/**
 * Accept the documented values for maxDecompressedBatchBytes: an integer from 1 to 2147483647.
 * @param {number} value
 * @returns {number}
 * @throws {KafkaJSInvalidConfigurationError} on anything else, naming the option
 */
const validateMaxDecompressedBatchBytes = (value) => {
  if (Number.isInteger(value) && value >= 1 && value <= INT_32_MAX_VALUE) {
    return value
  }

  throw new KafkaJSInvalidConfigurationError(
    `maxDecompressedBatchBytes must be an integer from 1 to ${INT_32_MAX_VALUE}, got ${inspect(value)}`
  )
}

/**
 * Accept the documented values for metadataRecoveryStrategy. Java's client uses the same two
 * names and the same default.
 * @param {string} value
 * @returns {string}
 * @throws {KafkaJSNonRetriableError} on anything else
 */
const validateMetadataRecoveryStrategy = (value) => {
  if (METADATA_RECOVERY_STRATEGIES.includes(value)) return value
  throw new KafkaJSNonRetriableError(
    `metadataRecoveryStrategy must be one of ${METADATA_RECOVERY_STRATEGIES.join(', ')}, got ${inspect(value)}`
  )
}

/**
 * Zero is rejected, not accepted as "rebootstrap immediately": with 0 the window comparison is
 * trivially true on the second consecutive failure, so one unreachable cluster would produce a
 * full teardown per refresh - the storm the armed-once-per-cycle rules exist to prevent. Same
 * shape as `maxInFlightRequests: 0`.
 * @param {number} value
 * @returns {number}
 * @throws {KafkaJSNonRetriableError} on 0, negative, fractional or non-numeric values
 */
const validateMetadataRecoveryRebootstrapTriggerMs = (value) => {
  if (Number.isInteger(value) && value >= 1) return value
  throw new KafkaJSNonRetriableError(
    `metadataRecoveryRebootstrapTriggerMs must be an integer >= 1, got ${inspect(value)}`
  )
}

/**
 * The idempotence a producer is created with once `maxInFlightRequests` has had its say: more
 * than 5 requests in flight per connection - or `null`, unbounded - is more than idempotence
 * allows. A producer that did not ask for idempotence runs without it and says so once; one that
 * did, with `idempotent: true` or a `transactionalId`, cannot be created. This deviates from the
 * Java client, which refuses the combination whether or not idempotence was asked for, so that a
 * 2.2.4 configuration with `maxInFlightRequests: null` keeps working. `idempotent` and
 * `transactionalId` are read by truthiness, as 2.2.4 read them.
 * @param {object} options
 * @param {boolean|undefined} options.idempotent as the caller passed it
 * @param {string} [options.transactionalId]
 * @param {number|null} options.maxInFlightRequests already validated; `null` is unbounded
 * @param {import("../types").Logger} options.logger
 * @returns {boolean|undefined} `idempotent` unchanged, or `false` when it was turned off
 * @throws {KafkaJSInvalidConfigurationError} for a producer that asked for idempotence
 */
const resolveIdempotent = ({
  idempotent,
  transactionalId,
  maxInFlightRequests: inFlight,
  logger,
}) => {
  const transactional = Boolean(transactionalId)
  const requested = transactional || idempotent == null || Boolean(idempotent)
  if (!requested || (inFlight !== null && inFlight <= MAX_IN_FLIGHT_REQUESTS_FOR_IDEMPOTENCE)) {
    return idempotent
  }

  if (transactional) {
    throw new KafkaJSInvalidConfigurationError(
      `maxInFlightRequests must be 5 or lower for a transactional producer, got ${inFlight}; pass a value <= 5`
    )
  }

  if (idempotent != null) {
    throw new KafkaJSInvalidConfigurationError(
      'maxInFlightRequests must be 5 or lower when idempotent: true is set; pass a value <= 5, ' +
        'or drop idempotent: true to run non-idempotent'
    )
  }

  logger.warn(
    `Idempotence disabled: maxInFlightRequests is ${inFlight === null ? 'null (unbounded)' : inFlight}, ` +
      'above the maximum of 5 for an idempotent producer. Pass maxInFlightRequests <= 5 to keep ' +
      'idempotence, or idempotent: false to silence this warning.',
    { rule: 'maxInFlightRequests' }
  )
  return false
}

/**
 * Checks the producer's accumulator options and its delivery deadline, and resolves the
 * accumulator's defaults, `createProducer.defaults`, for the options not passed. An explicit
 * deadline must leave room for one request after the linger, `deliveryTimeout >= linger +
 * requestTimeout` (Java's `KafkaProducer.configureDeliveryTimeout`). Without one, the deadline is
 * `retry.maxTotalTime` when that is set, and not held to that sum, so no configuration that
 * worked before is refused; otherwise it is the default, raised to the sum when the sum is larger.
 * `linger` and `deliveryTimeout` are at most 2147483647 ms, the longest timer delay; a default
 * raised above it is kept, as the timers of a longer deadline fire in steps.
 * @param {object} options
 * @param {number} [options.linger]
 * @param {number} [options.batchSize]
 * @param {number} [options.deliveryTimeout]
 * @param {number} [options.maxTotalTime] the producer's `retry.maxTotalTime`
 * @param {number} options.requestTimeout
 * @returns {{ linger: number, batchSize: number, deliveryTimeout: number, raised: boolean }}
 *   `raised` says that the default was raised to `linger + requestTimeout`
 * @throws {KafkaJSInvalidConfigurationError} naming the option at fault
 */
const resolveDelivery = ({ linger, batchSize, deliveryTimeout, maxTotalTime, requestTimeout }) => {
  if (linger !== undefined && !(Number.isInteger(linger) && linger >= 0 && linger <= MAX_DELAY)) {
    throw new KafkaJSInvalidConfigurationError(
      `linger must be an integer >= 0 and <= ${MAX_DELAY}, got ${inspect(linger)}`
    )
  }

  if (batchSize !== undefined && !(Number.isInteger(batchSize) && batchSize >= 1)) {
    throw new KafkaJSInvalidConfigurationError(
      `batchSize must be an integer >= 1, got ${inspect(batchSize)}`
    )
  }

  const resolved = {
    linger: linger ?? createProducer.defaults.linger,
    batchSize: batchSize ?? createProducer.defaults.batchSize,
    deliveryTimeout,
    raised: false,
  }

  if (deliveryTimeout != null) {
    if (!(
      Number.isInteger(deliveryTimeout) &&
      deliveryTimeout >= 1 &&
      deliveryTimeout <= MAX_DELAY
    )) {
      throw new KafkaJSInvalidConfigurationError(
        `deliveryTimeout must be an integer >= 1 and <= ${MAX_DELAY}, got ${inspect(deliveryTimeout)}`
      )
    }

    const floor = resolved.linger + requestTimeout
    if (deliveryTimeout < floor) {
      throw new KafkaJSInvalidConfigurationError(
        `deliveryTimeout must be at least linger + requestTimeout (${floor} ms), got ${deliveryTimeout}`
      )
    }
  } else if (maxTotalTime != null) {
    resolved.deliveryTimeout = maxTotalTime
  } else {
    const floor = resolved.linger + requestTimeout
    resolved.deliveryTimeout = Math.max(createProducer.defaults.deliveryTimeout, floor)
    resolved.raised = floor > createProducer.defaults.deliveryTimeout
  }

  return resolved
}

/**
 * Checks the producer's buffer options (`Admission`): `bufferMemory` an integer from 1 to
 * `Number.MAX_SAFE_INTEGER` bytes, `maxBlockMs` an integer from 0 to 2147483647 ms, the longest
 * timer delay. An option not passed keeps its default.
 * @param {{ bufferMemory?: number, maxBlockMs?: number }} options
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} naming the option at fault
 */
const validateBuffer = ({ bufferMemory, maxBlockMs }) => {
  if (bufferMemory !== undefined && !(Number.isSafeInteger(bufferMemory) && bufferMemory >= 1)) {
    throw new KafkaJSInvalidConfigurationError(
      `bufferMemory must be an integer >= 1 and <= ${Number.MAX_SAFE_INTEGER}, got ${inspect(bufferMemory)}`
    )
  }

  if (
    maxBlockMs !== undefined &&
    !(Number.isInteger(maxBlockMs) && maxBlockMs >= 0 && maxBlockMs <= MAX_DELAY)
  ) {
    throw new KafkaJSInvalidConfigurationError(
      `maxBlockMs must be an integer >= 0 and <= ${MAX_DELAY}, got ${inspect(maxBlockMs)}`
    )
  }
}

/**
 * Checks a producer's `maxRequestSize`: an integer from the smallest Produce request this
 * producer's identifiers allow (its framing, one batch header and one empty record) to 2147483647.
 * @param {number|undefined} maxRequestSize absent keeps the default
 * @param {string|null|undefined} clientId
 * @param {string|null|undefined} transactionalId
 * @returns {void}
 * @throws {KafkaJSInvalidConfigurationError} naming the minimum
 */
const validateMaxRequestSize = (maxRequestSize, clientId, transactionalId) => {
  const value = maxRequestSize ?? createProducer.defaults.maxRequestSize
  const minimum = minimumRequestSize(clientId, transactionalId)
  if (!(Number.isInteger(value) && value >= minimum && value <= INT_32_MAX_VALUE)) {
    throw new KafkaJSInvalidConfigurationError(
      `maxRequestSize must be an integer from ${minimum} (the smallest request this producer's ` +
        `clientId and transactionalId allow) to ${INT_32_MAX_VALUE}, got ${inspect(maxRequestSize)}`
    )
  }
}

/**
 * Validates and snapshots the supported per-codec producer levels.
 * @param {{ gzip?: number, zstd?: number }|undefined} levels requested levels
 * @returns {{ gzip?: number, zstd?: number }|undefined} owned options
 * @throws {KafkaJSInvalidConfigurationError} for unsupported keys or non-integer levels
 */
const resolveCompressionLevels = (levels) => {
  if (levels === undefined) return undefined
  if (levels === null || typeof levels !== 'object' || Array.isArray(levels)) {
    throw new KafkaJSInvalidConfigurationError('compressionLevels must be an object')
  }
  const resolved = {}
  for (const [key, level] of Object.entries(levels)) {
    if (key === 'lz4') {
      throw new KafkaJSInvalidConfigurationError(
        'compressionLevels.lz4: lz4-napi exposes no compression level'
      )
    }
    const valid =
      Number.isInteger(level) &&
      (key === 'gzip'
        ? level === -1 || (level >= 1 && level <= 9)
        : key === 'zstd' && level >= -131072 && level <= 22)
    if (!valid)
      throw new KafkaJSInvalidConfigurationError(
        `Invalid compressionLevels.${key}: ${inspect(level)}`
      )
    resolved[key] = level
  }
  return resolved
}

module.exports = class Client {
  /**
   * @param {Object} options
   * @param {Array<string>} options.brokers example: ['127.0.0.1:9092', '127.0.0.1:9094']
   * @param {Object} options.ssl
   * @param {Object} options.sasl
   * @param {string} options.clientId
   * @param {number} [options.connectionTimeout=1000] - in milliseconds
   * @param {number} options.authenticationTimeout - in milliseconds
   * @param {number} options.reauthenticationThreshold - in milliseconds
   * @param {number} [options.requestTimeout=30000] - in milliseconds
   * @param {boolean} [options.enforceRequestTimeout]
   * @param {number} [options.maxResponseSize=104857600] the response-size limit handed to every
   *   connection, in bytes, the 4-byte length prefix excluded: an integer from 4 to 2147483647.
   *   It is not the consumer's `maxBytes`.
   * @param {import("../types").RetryOptions} [options.retry]
   * @param {import("../types").ISocketFactory} [options.socketFactory]
   * @param {'rebootstrap'|'none'} [options.metadataRecoveryStrategy='rebootstrap'] KIP-899/1102:
   *   `'rebootstrap'` rebuilds the client's connections and metadata from the configured broker
   *   list when metadata is unreachable; `'none'` restores pre-3.0 behavior.
   * @param {number} [options.metadataRecoveryRebootstrapTriggerMs=300000] how long metadata must
   *   fail *continuously* before a rebootstrap is triggered. Must be an integer >= 1.
   * @throws {KafkaJSInvalidConfigurationError} for an invalid `maxResponseSize`
   */
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    connectionTimeoutMax,
    reconnectBackoff,
    reconnectBackoffMax,
    authenticationTimeout,
    reauthenticationThreshold,
    requestTimeout,
    enforceRequestTimeout = true,
    maxResponseSize = DEFAULT_MAX_RESPONSE_SIZE,
    retry,
    socketFactory = defaultSocketFactory(),
    logLevel = INFO,
    logCreator = LoggerConsole,
    metadataRecoveryStrategy = DEFAULT_METADATA_RECOVERY_STRATEGY,
    metadataRecoveryRebootstrapTriggerMs = DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
  }) {
    this[PRIVATE.OFFSETS] = new Map()
    this[PRIVATE.OPERATIONS] = new Map()
    this[PRIVATE.LOGGER] = createLogger({ level: logLevel, logCreator })
    this[PRIVATE.CLUSTER_RETRY] = retry
    this[PRIVATE.REQUEST_TIMEOUT] = requestTimeout ?? DEFAULT_REQUEST_TIMEOUT
    this[PRIVATE.CLIENT_ID] = clientId

    const setupPolicy = ConnectionSetupPolicy.resolve({
      connectionTimeout,
      connectionTimeoutMax,
      reconnectBackoff,
      reconnectBackoffMax,
      authenticationTimeout,
    })
    connectionTimeout = setupPolicy.timeoutBase
    connectionTimeoutMax = setupPolicy.timeoutCeiling
    reconnectBackoff = setupPolicy.waitBase
    reconnectBackoffMax = setupPolicy.waitCeiling
    authenticationTimeout = setupPolicy.authenticationTimeout
    const validatedMaxResponseSize = validateMaxResponseSize(maxResponseSize)
    this[PRIVATE.MAX_RESPONSE_SIZE] = validatedMaxResponseSize
    const validatedMetadataRecoveryStrategy =
      validateMetadataRecoveryStrategy(metadataRecoveryStrategy)
    const validatedMetadataRecoveryRebootstrapTriggerMs =
      validateMetadataRecoveryRebootstrapTriggerMs(metadataRecoveryRebootstrapTriggerMs)

    this[PRIVATE.CREATE_CLUSTER] = ({
      metadataMaxAge,
      metadataMaxIdle,
      allowAutoTopicCreation = true,
      // Java's max.in.flight.requests.per.connection default. Pass null for the pre-3.0
      // unbounded behavior.
      maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS,
      instrumentationEmitter = null,
      isolationLevel,
    }) =>
      new Cluster({
        maxInFlightRequests: validateMaxInFlightRequests(maxInFlightRequests),
        logger: this[PRIVATE.LOGGER],
        retry: this[PRIVATE.CLUSTER_RETRY],
        offsets: this[PRIVATE.OFFSETS],
        operations: this[PRIVATE.OPERATIONS],
        maxResponseSize: validatedMaxResponseSize,
        socketFactory,
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        connectionTimeoutMax,
        reconnectBackoff,
        reconnectBackoffMax,
        setupPolicy,
        authenticationTimeout,
        reauthenticationThreshold,
        requestTimeout,
        enforceRequestTimeout,
        metadataMaxAge,
        metadataMaxIdle,
        instrumentationEmitter,
        allowAutoTopicCreation,
        isolationLevel,
        metadataRecoveryStrategy: validatedMetadataRecoveryStrategy,
        metadataRecoveryRebootstrapTriggerMs: validatedMetadataRecoveryRebootstrapTriggerMs,
      })
  }

  /**
   * Creates a producer. It is idempotent unless `idempotent: false` is passed; a setting that
   * does not allow idempotence - `maxInFlightRequests` above 5 or `null` here, `retry.retries` of
   * 0, or `acks` other than -1 on a send - turns it off with a warning, or is refused when
   * `idempotent: true` or a `transactionalId` asked for it.
   * @public
   * @param {object} [options]
   * @param {number} [options.linger=5] ms a batch waits for more records; 0 sends it at once
   * @param {number} [options.batchSize=16384] bytes of keys and values at which a batch is sent
   * @param {number} [options.deliveryTimeout] ms bounding the waiting and the retries of one send,
   *   at least `linger + requestTimeout`. A request already sent settles on its answer or on
   *   `requestTimeout`, so a send can end up to `requestTimeout` after `deliveryTimeout`
   * @param {number} [options.maxRequestSize=1048576] maximum encoded Produce frame bytes,
   *   from the minimum for this client and transaction id to 2147483647
   * @param {{ gzip?: number, zstd?: number }} [options.compressionLevels] gzip (-1 or 1..9)
   *   and zstd (-131072..22) levels, copied at construction; omission keeps codec defaults
   * @param {number} [options.bufferMemory=33554432] bytes the producer's sends may hold at once
   * @param {number} [options.maxBlockMs=60000] ms a send waits for room in that buffer
   * @param {number} [options.metadataMaxIdle=300000] idle unpinned topic lifetime, 5000..2147483647 ms
   * @returns {import("../types").Producer} the producer and its transaction factory
   * @throws {KafkaJSInvalidConfigurationError} for a setting idempotence asked for explicitly
   *   does not allow, and for invalid delivery, buffer, request-size or compression-level options
   */
  producer({
    createPartitioner,
    retry,
    metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
    metadataMaxIdle = 300000,
    allowAutoTopicCreation,
    idempotent,
    transactionalId,
    transactionTimeout,
    maxInFlightRequests,
    linger,
    batchSize,
    deliveryTimeout,
    compressionLevels,
    bufferMemory,
    maxBlockMs,
    maxRequestSize,
  } = {}) {
    if (!(
      Number.isInteger(metadataMaxIdle) &&
      metadataMaxIdle >= 5000 &&
      metadataMaxIdle <= MAX_DELAY
    )) {
      throw new KafkaJSInvalidConfigurationError(
        `metadataMaxIdle must be an integer >= 5000 and <= ${MAX_DELAY}, got ${inspect(metadataMaxIdle)}`
      )
    }
    const levels = resolveCompressionLevels(compressionLevels)
    validateBuffer({ bufferMemory, maxBlockMs })
    validateMaxRequestSize(maxRequestSize, this[PRIVATE.CLIENT_ID], transactionalId)
    const producerRetry = { ...this[PRIVATE.CLUSTER_RETRY], ...retry }
    const delivery = resolveDelivery({
      linger,
      batchSize,
      deliveryTimeout,
      maxTotalTime: producerRetry.maxTotalTime,
      requestTimeout: this[PRIVATE.REQUEST_TIMEOUT],
    })
    const inFlight = validateMaxInFlightRequests(
      maxInFlightRequests === undefined ? DEFAULT_MAX_IN_FLIGHT_REQUESTS : maxInFlightRequests
    )
    const resolvedIdempotent = resolveIdempotent({
      idempotent,
      transactionalId,
      maxInFlightRequests: inFlight,
      logger: this[PRIVATE.LOGGER].namespace('Producer'),
    })
    const instrumentationEmitter = new InstrumentationEventEmitter({ channels: channels.producer })
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      metadataMaxIdle,
      allowAutoTopicCreation,
      maxInFlightRequests: inFlight,
      instrumentationEmitter,
    })

    if (delivery.raised) {
      this[PRIVATE.LOGGER]
        .namespace('Producer')
        .warn(
          `deliveryTimeout was not set and defaults to ${createProducer.defaults.deliveryTimeout} ms, ` +
            `which is below linger + requestTimeout: it is raised to ${delivery.deliveryTimeout} ms. ` +
            'Set deliveryTimeout to choose it.',
          { rule: 'deliveryTimeout' }
        )
    }

    return createProducer({
      retry: producerRetry,
      logger: this[PRIVATE.LOGGER],
      cluster,
      createPartitioner,
      idempotent: resolvedIdempotent,
      transactionalId,
      transactionTimeout,
      linger: delivery.linger,
      batchSize: delivery.batchSize,
      deliveryTimeout: delivery.deliveryTimeout,
      compressionLevels: levels,
      bufferMemory,
      maxBlockMs,
      clientId: this[PRIVATE.CLIENT_ID],
      maxRequestSize,
      instrumentationEmitter,
    })
  }

  /**
   * Creates a consumer.
   *
   * `maxPollInterval` (default 300000 ms) is both the rebalance timeout sent on the wire and the
   * local deadline a handler has to return within; `rebalanceTimeout` is its deprecated alias for
   * the wire half only.
   *
   * `groupProtocol` selects the group membership protocol: `'classic'` (the default) or
   * `'consumer'` (KIP-848, broker-driven assignment). Under `'consumer'`, `groupRemoteAssignor`
   * names the server-side assignor to ask for. `maxPrefetchBatches` (`0` or `1`, default `0`) is
   * accepted and inert in this release.
   *
   * A `maxBytes` above the client's `maxResponseSize` logs one warning: a Fetch response of that
   * size would be refused with `KafkaJSInvalidFrameError`.
   *
   * `maxDecompressedBatchBytes` (default 1 GiB, an integer from 1 to 2147483647) is the most a
   * record batch may decompress to, for every codec; it also sets a ceiling of
   * `max(1, floor(bytes / 512))` records per batch. A batch past either halts its partition.
   *
   * `fetchSessions` (default `false`) opts in to incremental Fetch sessions (KIP-227); `false`
   * keeps every Fetch a full request.
   * @throws {KafkaJSNonRetriableError} for a non-boolean `rackAwareAssignment` or `fetchSessions`
   * @throws {KafkaJSInvalidConfigurationError} for an invalid `maxDecompressedBatchBytes`
   * @public
   */
  consumer({
    groupId,
    partitionAssigners,
    metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
    sessionTimeout,
    rebalanceTimeout,
    maxPollInterval,
    heartbeatInterval,
    maxBytesPerPartition,
    minBytes,
    maxBytes = 10485760,
    maxWaitTimeInMs,
    retry = { retries: 5 },
    allowAutoTopicCreation,
    maxInFlightRequests,
    readUncommitted = false,
    rackId = '',
    rackAwareAssignment = false,
    checkCrcs = true,
    maxDecompressedBatchBytes = DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES,
    groupInstanceId = null,
    groupProtocol,
    groupRemoteAssignor,
    maxPrefetchBatches,
    fetchSessions = false,
  } = {}) {
    if (typeof fetchSessions !== 'boolean') {
      throw new KafkaJSNonRetriableError('fetchSessions must be a boolean')
    }
    if (typeof rackAwareAssignment !== 'boolean') {
      throw new KafkaJSNonRetriableError('rackAwareAssignment must be a boolean')
    }
    validateMaxDecompressedBatchBytes(maxDecompressedBatchBytes)

    const isolationLevel = readUncommitted
      ? ISOLATION_LEVEL.READ_UNCOMMITTED
      : ISOLATION_LEVEL.READ_COMMITTED

    if (maxBytes > this[PRIVATE.MAX_RESPONSE_SIZE]) {
      this[PRIVATE.LOGGER]
        .namespace('Consumer')
        .warn(
          `maxBytes (${maxBytes}) is above maxResponseSize (${this[PRIVATE.MAX_RESPONSE_SIZE]}): a Fetch response that large would be refused with KafkaJSInvalidFrameError. Raise maxResponseSize in the Kafka options, or lower maxBytes.`,
          { groupId }
        )
    }

    const instrumentationEmitter = new InstrumentationEventEmitter({
      channels: channels.consumer,
    })
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      isolationLevel,
      instrumentationEmitter,
    })

    return createConsumer({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      groupId,
      partitionAssigners,
      sessionTimeout,
      rebalanceTimeout,
      maxPollInterval,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      isolationLevel,
      instrumentationEmitter,
      rackId,
      rackAwareAssignment,
      checkCrcs,
      maxDecompressedBatchBytes,
      groupInstanceId,
      metadataMaxAge,
      groupProtocol,
      groupRemoteAssignor,
      maxPrefetchBatches,
      fetchSessions,
      requestTimeout: this[PRIVATE.REQUEST_TIMEOUT],
    })
  }

  /**
   * @public
   */
  admin({ retry } = {}) {
    const instrumentationEmitter = new InstrumentationEventEmitter({
      channels: channels.admin,
    })
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      allowAutoTopicCreation: false,
      instrumentationEmitter,
    })

    return createAdmin({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
      logger: this[PRIVATE.LOGGER],
      instrumentationEmitter,
      cluster,
    })
  }

  /**
   * @public
   */
  logger() {
    return this[PRIVATE.LOGGER]
  }
}
