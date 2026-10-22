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

/** The names of the errors a lost connection raises. */
const LOSSES = new Set([
  'KafkaJSConnectionError',
  'KafkaJSConnectionClosedError',
  'KafkaJSRequestTimeoutError',
])

/**
 * Whether an error says a connection to a broker was lost: it could not be opened (refused, reset,
 * timed out while connecting), it closed under a request, or a request on it timed out. A retrier
 * that gave up on such an error counts as one. An invalid frame (`KafkaJSInvalidFrameError`) does
 * not: the peer answered, and asking again gets the same answer.
 * @param {*} error
 * @returns {boolean}
 */
const isConnectionLoss = (error) => {
  const name = describeThrown(error, 'name')
  if (name !== 'KafkaJSNumberOfRetriesExceeded') return LOSSES.has(name)
  try {
    return LOSSES.has(describeThrown(error.cause, 'name'))
  } catch {
    return false
  }
}

module.exports = { isConnectionFailure, isConnectionLoss }
