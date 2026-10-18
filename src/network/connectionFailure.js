const describeThrown = require('./describeThrown')

/**
 * Whether an error says the broker at an address could not be reached or did not speak the
 * protocol: a connection error (refused, reset, timed out while connecting, "Not connected") or an
 * invalid response frame. The callers refresh metadata or rotate the seed broker on it. A
 * connection that was open and closed under a request (`KafkaJSConnectionClosedError`) is not one:
 * the request's own retrier decides about that.
 * @param {*} error
 * @returns {boolean}
 */
const isConnectionFailure = (error) =>
  describeThrown(error, 'name') === 'KafkaJSConnectionError' ||
  describeThrown(error, 'name') === 'KafkaJSInvalidFrameError'

module.exports = { isConnectionFailure }
