const flexibleVersions = require('./flexibleVersions')
const apiKeys = require('./requests/apiKeys')
const { KafkaJSNonRetriableError } = require('../errors')

/** Classic request header: api_key, api_version, correlation_id, client_id. */
const REQUEST_HEADER_V1 = 1
/** Flexible request header: v1 plus a tagged-field buffer. client_id stays classic. */
const REQUEST_HEADER_V2 = 2
/** Classic response header: correlation_id only. */
const RESPONSE_HEADER_V0 = 0
/** Flexible response header: v0 plus a tagged-field buffer. */
const RESPONSE_HEADER_V1 = 1

/**
 * Whether `(apiKey, apiVersion)` uses the KIP-482 flexible encoding — compact strings, bytes
 * and arrays, and a tagged-field section terminating every structure.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
 * @param {number} apiKey
 * @param {number} apiVersion
 * @returns {boolean}
 * @throws {KafkaJSNonRetriableError} when `apiKey` is missing from the generated flexible-versions table
 */
const isFlexible = (apiKey, apiVersion) => {
  const flexibleFrom = flexibleVersions[apiKey]

  if (flexibleFrom === undefined) {
    throw new KafkaJSNonRetriableError(
      `No flexible-version information for API key ${apiKey}; regenerate src/protocol/flexibleVersions.js`
    )
  }

  return flexibleFrom !== null && apiVersion >= flexibleFrom
}

/**
 * @param {number} apiKey
 * @param {number} apiVersion
 * @returns {number} REQUEST_HEADER_V1 or REQUEST_HEADER_V2
 */
const requestHeaderVersion = (apiKey, apiVersion) =>
  isFlexible(apiKey, apiVersion) ? REQUEST_HEADER_V2 : REQUEST_HEADER_V1

/**
 * ApiVersions responses keep header v0 forever: a broker answering UNSUPPORTED_VERSION replies
 * with a v0 body, and the client must be able to parse the header before it knows the version,
 * so the header length must never change.
 * @see https://kafka.apache.org/43/design/protocol
 * @param {number} apiKey
 * @param {number} apiVersion
 * @returns {number} RESPONSE_HEADER_V0 or RESPONSE_HEADER_V1
 */
const responseHeaderVersion = (apiKey, apiVersion) => {
  if (apiKey === apiKeys.ApiVersions) {
    return RESPONSE_HEADER_V0
  }

  return isFlexible(apiKey, apiVersion) ? RESPONSE_HEADER_V1 : RESPONSE_HEADER_V0
}

module.exports = {
  REQUEST_HEADER_V1,
  REQUEST_HEADER_V2,
  RESPONSE_HEADER_V0,
  RESPONSE_HEADER_V1,
  isFlexible,
  requestHeaderVersion,
  responseHeaderVersion,
}
