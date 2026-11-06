const { inspect } = require('util')
const { KafkaJSInvalidConfigurationError, KafkaJSNonRetriableError } = require('../../errors')
const { ShareRunner } = require('./shareRunner')

const MAX_INT = 2147483647
const REJECTED = Object.freeze({
  fromBeginning: 'use the group config share.auto.offset.reset',
  readUncommitted: 'use the group config share.isolation.level',
  groupInstanceId: 'a share group has no static membership',
  partitionAssigners: 'the broker assigns',
  sessionTimeout: 'use the group config share.session.timeout.ms',
  rebalanceTimeout: 'use the group config share.session.timeout.ms',
  heartbeatInterval: 'use the group config share.heartbeat.interval.ms',
  groupProtocol: 'the broker assigns',
  groupRemoteAssignor: 'the broker assigns',
  maxBytesPerPartition: 'the share fetch has no per-partition limit',
  maxPrefetchBatches: 'prefetch is not offered',
})
const DEFAULTS = Object.freeze({
  groupId: undefined,
  acknowledgementMode: 'implicit',
  acquireMode: 'record_limit',
  maxRecords: 500,
  minBytes: 1,
  maxBytes: 10485760,
  maxWaitTimeInMs: 500,
  metadataMaxAge: 300000,
  maxPollInterval: 300000,
  rackId: '',
  retry: { retries: 5 },
  allowAutoTopicCreation: true,
  checkCrcs: true,
  maxInFlightRequests: 5,
})
const INTEGER_RANGES = [
  ['maxRecords', 1],
  ['minBytes', 0],
  ['maxBytes', 1],
  ['maxWaitTimeInMs', 0],
  ['metadataMaxAge', 1],
  ['maxPollInterval', 1],
]

/**
 * Validates and copies recognized options before a Cluster or emitter exists.
 * Getters are read once; their original exceptions propagate without constructing client state.
 * @param {object} input User config; unknown properties are not read.
 * @param {object} [inheritedRetry] Already-owned Kafka retry configuration.
 * @returns {object} Normalized independent config, with copied merged retry options.
 * @throws {KafkaJSInvalidConfigurationError} For an invalid option other than maxInFlightRequests.
 * @throws {KafkaJSNonRetriableError} For invalid maxInFlightRequests, as for consumer().
 */
const validateShareConsumerOptions = (input, inheritedRetry = {}) => {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    throw new KafkaJSInvalidConfigurationError(
      'shareConsumer config must be an object with groupId'
    )
  }
  for (const [key, equivalent] of Object.entries(REJECTED)) {
    if (input[key] !== undefined) {
      throw new KafkaJSInvalidConfigurationError(`${key} is not supported: ${equivalent}`)
    }
  }
  const config = {}
  for (const [key, fallback] of Object.entries(DEFAULTS)) {
    const value = input[key]
    config[key] = value === undefined ? fallback : value
  }
  const invalid = (key, range) => {
    throw new KafkaJSInvalidConfigurationError(
      `${key} must be ${range}, got ${inspect(config[key])}`
    )
  }
  if (typeof config.groupId !== 'string' || config.groupId.length === 0)
    invalid('groupId', 'a non-empty string')
  if (!['implicit', 'explicit'].includes(config.acknowledgementMode))
    invalid('acknowledgementMode', 'implicit or explicit')
  if (!['batch_optimized', 'record_limit'].includes(config.acquireMode))
    invalid('acquireMode', 'batch_optimized or record_limit')
  for (const [key, minimum] of INTEGER_RANGES) {
    if (!Number.isInteger(config[key]) || config[key] < minimum || config[key] > MAX_INT) {
      invalid(key, `an integer from ${minimum} to ${MAX_INT}`)
    }
  }
  if (typeof config.rackId !== 'string') invalid('rackId', 'a string')
  for (const key of ['allowAutoTopicCreation', 'checkCrcs']) {
    if (typeof config[key] !== 'boolean') invalid(key, 'a boolean')
  }
  const inFlight = config.maxInFlightRequests
  if (inFlight !== null && !(Number.isInteger(inFlight) && inFlight >= 1)) {
    throw new KafkaJSNonRetriableError(
      'maxInFlightRequests must be null (unbounded) or an integer >= 1'
    )
  }
  config.retry = { ...inheritedRetry, ...config.retry }
  const maxTotalTime = config.retry.maxTotalTime
  if (
    maxTotalTime != null &&
    !(Number.isInteger(maxTotalTime) && maxTotalTime >= 1 && maxTotalTime <= MAX_INT)
  ) {
    throw new KafkaJSInvalidConfigurationError(
      'retry.maxTotalTime must be an integer from 1 to 2147483647 or absent'
    )
  }
  return config
}

/**
 * Creates the public share consumer from validated factory dependencies.
 * @param {object} options Cluster, emitter, logger, requestTimeout, and normalized config.
 * @returns {import('../../../types').ShareConsumer} An independent public client facade.
 */
const createShareConsumer = (options) =>
  new ShareRunner({ ...options, logger: options.logger.namespace('ShareConsumer') }).api()

module.exports = { createShareConsumer, validateShareConsumerOptions }
