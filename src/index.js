const {
  createLogger,
  LEVELS: { INFO },
} = require('./loggers')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('./errors')

const InstrumentationEventEmitter = require('./instrumentation/emitter')
const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')
const defaultSocketFactory = require('./network/socketFactory')
const once = require('./utils/once')
const websiteUrl = require('./utils/websiteUrl')

const PRIVATE = {
  CREATE_CLUSTER: Symbol('private:Kafka:createCluster'),
  CLUSTER_RETRY: Symbol('private:Kafka:clusterRetry'),
  LOGGER: Symbol('private:Kafka:logger'),
  OFFSETS: Symbol('private:Kafka:offsets'),
  REQUEST_TIMEOUT: Symbol('private:Kafka:requestTimeout'),
}

const DEFAULT_METADATA_MAX_AGE = 300000
const DEFAULT_MAX_IN_FLIGHT_REQUESTS = 5
// The Cluster's own default for `requestTimeout`.
const DEFAULT_REQUEST_TIMEOUT = 30000
// Java's MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE, and its 4.x defaults for an
// accumulator that is switched on (linger.ms, batch.size).
// @see clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java (Apache Kafka 4.3)
const MAX_IN_FLIGHT_REQUESTS_FOR_IDEMPOTENCE = 5
const DEFAULT_LINGER = 5
const DEFAULT_BATCH_SIZE = 16384
// The longest delay Node's `setTimeout` honours; a longer one runs after 1 ms.
// @see https://nodejs.org/api/timers.html
const MAX_TIMER_DELAY = 2147483647
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
    `maxInFlightRequests must be null (unbounded) or an integer >= 1, got ${value}`
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
    `metadataRecoveryStrategy must be one of ${METADATA_RECOVERY_STRATEGIES.join(', ')}, got ${value}`
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
    `metadataRecoveryRebootstrapTriggerMs must be an integer >= 1, got ${value}`
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
 * accumulator's defaults: it is off unless `linger` or `batchSize` is passed, and then the unset
 * one takes the Java 4.x default. The deadline must leave room for one request after the linger,
 * `deliveryTimeout >= linger + requestTimeout` (Java's `KafkaProducer.configureDeliveryTimeout`),
 * with `linger` 0 while the accumulator is off. `linger` and `deliveryTimeout` are at most
 * 2147483647 ms, the longest timer delay.
 * @param {object} options
 * @param {number} [options.linger]
 * @param {number} [options.batchSize]
 * @param {number} [options.deliveryTimeout]
 * @param {number} options.requestTimeout
 * @returns {{ linger: number|undefined, batchSize: number|undefined }} both undefined while the
 *   accumulator is off
 * @throws {KafkaJSInvalidConfigurationError} naming the option at fault
 */
const resolveDelivery = ({ linger, batchSize, deliveryTimeout, requestTimeout }) => {
  if (
    linger !== undefined &&
    !(Number.isInteger(linger) && linger >= 0 && linger <= MAX_TIMER_DELAY)
  ) {
    throw new KafkaJSInvalidConfigurationError(
      `linger must be an integer >= 0 and <= ${MAX_TIMER_DELAY}, got ${linger}`
    )
  }

  if (batchSize !== undefined && !(Number.isInteger(batchSize) && batchSize >= 1)) {
    throw new KafkaJSInvalidConfigurationError(
      `batchSize must be an integer >= 1, got ${batchSize}`
    )
  }

  const accumulating = linger !== undefined || batchSize !== undefined
  const resolved = {
    linger: accumulating ? (linger ?? DEFAULT_LINGER) : undefined,
    batchSize: accumulating ? (batchSize ?? DEFAULT_BATCH_SIZE) : undefined,
  }

  if (deliveryTimeout != null) {
    if (!(
      Number.isInteger(deliveryTimeout) &&
      deliveryTimeout >= 1 &&
      deliveryTimeout <= MAX_TIMER_DELAY
    )) {
      throw new KafkaJSInvalidConfigurationError(
        `deliveryTimeout must be an integer >= 1 and <= ${MAX_TIMER_DELAY}, got ${deliveryTimeout}`
      )
    }

    const floor = (resolved.linger ?? 0) + requestTimeout
    if (deliveryTimeout < floor) {
      throw new KafkaJSInvalidConfigurationError(
        `deliveryTimeout must be at least linger + requestTimeout (${floor} ms), got ${deliveryTimeout}`
      )
    }
  }

  return resolved
}

const warnOfDefaultPartitioner = once((logger) => {
  if (process.env.KAFKAJS_NO_PARTITIONER_WARNING == null) {
    logger.warn(
      `KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior as in previous versions, create the producer with the option "createPartitioner: Partitioners.LegacyPartitioner". See the migration guide at ${websiteUrl(
        'docs/migration-guide-v2.0.0',
        'producer-new-default-partitioner'
      )} for details. Silence this warning by setting the environment variable "KAFKAJS_NO_PARTITIONER_WARNING=1"`
    )
  }
})

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
   * @param {import("../types").RetryOptions} [options.retry]
   * @param {import("../types").ISocketFactory} [options.socketFactory]
   * @param {'rebootstrap'|'none'} [options.metadataRecoveryStrategy='rebootstrap'] KIP-899/1102:
   *   `'rebootstrap'` rebuilds the client's connections and metadata from the configured broker
   *   list when metadata is unreachable; `'none'` restores pre-3.0 behavior.
   * @param {number} [options.metadataRecoveryRebootstrapTriggerMs=300000] how long metadata must
   *   fail *continuously* before a rebootstrap is triggered. Must be an integer >= 1.
   */
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout = 1000,
    authenticationTimeout,
    reauthenticationThreshold,
    requestTimeout,
    enforceRequestTimeout = true,
    retry,
    socketFactory = defaultSocketFactory(),
    logLevel = INFO,
    logCreator = LoggerConsole,
    metadataRecoveryStrategy = DEFAULT_METADATA_RECOVERY_STRATEGY,
    metadataRecoveryRebootstrapTriggerMs = DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
  }) {
    this[PRIVATE.OFFSETS] = new Map()
    this[PRIVATE.LOGGER] = createLogger({ level: logLevel, logCreator })
    this[PRIVATE.CLUSTER_RETRY] = retry
    this[PRIVATE.REQUEST_TIMEOUT] = requestTimeout ?? DEFAULT_REQUEST_TIMEOUT

    const validatedMetadataRecoveryStrategy =
      validateMetadataRecoveryStrategy(metadataRecoveryStrategy)
    const validatedMetadataRecoveryRebootstrapTriggerMs =
      validateMetadataRecoveryRebootstrapTriggerMs(metadataRecoveryRebootstrapTriggerMs)

    this[PRIVATE.CREATE_CLUSTER] = ({
      metadataMaxAge,
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
        socketFactory,
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        authenticationTimeout,
        reauthenticationThreshold,
        requestTimeout,
        enforceRequestTimeout,
        metadataMaxAge,
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
   * @param {number} [options.linger] the opt-in accumulator: ms a batch waits for more records
   * @param {number} [options.batchSize] the opt-in accumulator: bytes at which a batch is sent
   * @param {number} [options.deliveryTimeout] ms one send may take, retries included; at least
   *   `linger + requestTimeout`
   * @throws {KafkaJSInvalidConfigurationError} for a setting idempotence asked for explicitly
   *   does not allow, and for an invalid `linger`, `batchSize` or `deliveryTimeout`
   */
  producer({
    createPartitioner,
    retry,
    metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
    allowAutoTopicCreation,
    idempotent,
    transactionalId,
    transactionTimeout,
    maxInFlightRequests,
    linger,
    batchSize,
    deliveryTimeout,
  } = {}) {
    const delivery = resolveDelivery({
      linger,
      batchSize,
      deliveryTimeout,
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
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests: inFlight,
      instrumentationEmitter,
    })

    if (createPartitioner == null) {
      warnOfDefaultPartitioner(this[PRIVATE.LOGGER])
    }

    return createProducer({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      createPartitioner,
      idempotent: resolvedIdempotent,
      transactionalId,
      transactionTimeout,
      linger: delivery.linger,
      batchSize: delivery.batchSize,
      deliveryTimeout,
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
    maxBytes,
    maxWaitTimeInMs,
    retry = { retries: 5 },
    allowAutoTopicCreation,
    maxInFlightRequests,
    readUncommitted = false,
    rackId = '',
    checkCrcs = true,
    groupInstanceId = null,
    groupProtocol,
    groupRemoteAssignor,
    maxPrefetchBatches,
  } = {}) {
    const isolationLevel = readUncommitted
      ? ISOLATION_LEVEL.READ_UNCOMMITTED
      : ISOLATION_LEVEL.READ_COMMITTED

    const instrumentationEmitter = new InstrumentationEventEmitter()
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
      checkCrcs,
      groupInstanceId,
      metadataMaxAge,
      groupProtocol,
      groupRemoteAssignor,
      maxPrefetchBatches,
    })
  }

  /**
   * @public
   */
  admin({ retry } = {}) {
    const instrumentationEmitter = new InstrumentationEventEmitter()
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
