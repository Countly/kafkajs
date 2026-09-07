const {
  createLogger,
  LEVELS: { INFO },
} = require('./loggers')
const { KafkaJSNonRetriableError } = require('./errors')

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
}

const DEFAULT_METADATA_MAX_AGE = 300000
const DEFAULT_MAX_IN_FLIGHT_REQUESTS = 5
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
   * @public
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
  } = {}) {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
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
      idempotent,
      transactionalId,
      transactionTimeout,
      instrumentationEmitter,
    })
  }

  /**
   * Creates a consumer.
   *
   * `maxPollInterval` (default 300000 ms) is both the rebalance timeout sent on the wire and the
   * local deadline a handler has to return within; `rebalanceTimeout` is its deprecated alias for
   * the wire half only.
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
