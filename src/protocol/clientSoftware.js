const { version } = require('../../package.json')

/**
 * Reported to the broker in ApiVersionsRequest v3+ and surfaced by the
 * `kafka.server:type=socket-server-metrics,clientSoftwareName=…` gauge.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers
 */
const CLIENT_SOFTWARE_NAME = 'countly-kafkajs'

/** The broker rejects anything outside this set with INVALID_REQUEST (42). */
const ILLEGAL_CHARACTERS = /[^.\-a-zA-Z0-9]/g

/**
 * @param {string} value a semver string, possibly with `+build` metadata
 * @returns {string} a value the broker's `([\.\-a-zA-Z0-9])+` check accepts
 */
const sanitize = (value) => {
  const withoutBuildMetadata = String(value).split('+')[0]
  const cleaned = withoutBuildMetadata.replace(ILLEGAL_CHARACTERS, '-')
  return cleaned.length === 0 ? 'unknown' : cleaned
}

/** @returns {string} this package's version, sanitized for the broker */
const clientSoftwareVersion = () => sanitize(version)

module.exports = { CLIENT_SOFTWARE_NAME, clientSoftwareVersion, sanitize }
