const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSNonRetriableError, KafkaJSShareGroupsNotSupportedError } = require('../../errors')

/**
 * What a share membership does with a failed heartbeat, by protocol code. The action classes:
 * - `coordinator`: forget the cached coordinator, keep the membership, retry under backoff;
 * - `fence`: the member was removed, so rejoin at epoch zero with the same member ID;
 * - `unsupported`: the broker refused the API at runtime, a terminal capability failure;
 * - `fatal`: terminal for this run, never retried;
 * - `groupNotFound`: GROUP_ID_NOT_FOUND, whose meaning depends on what it answers. It ends a
 *   current membership as terminal, but is the expected answer to a member's own leave and is
 *   ignored once the membership is retired, so the membership decides, not this table.
 *
 * A code not listed follows its own `retriable` flag. Independent of transport retries and of the
 * fetch and acknowledgement policy.
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/ShareGroupHeartbeatResponse.json
 */
const MEMBERSHIP_ERRORS = Object.freeze({
  14: 'coordinator',
  15: 'coordinator',
  16: 'coordinator',
  25: 'fence',
  110: 'fence',
  35: 'unsupported',
  30: 'fatal',
  29: 'fatal',
  81: 'fatal',
  42: 'fatal',
  69: 'groupNotFound',
})

/**
 * Classifies a membership failure. Only a current membership may call this: a caller holding the
 * response of a retired membership must discard it first.
 * @param {Error} error Decoded protocol or transport error.
 * @returns {'coordinator'|'fence'|'unsupported'|'fatal'|'groupNotFound'|'retry'} Membership action.
 */
const membershipAction = (error) =>
  MEMBERSHIP_ERRORS[error.code] || (error.retriable === false ? 'fatal' : 'retry')

/**
 * Builds the terminal error of a membership, keeping the protocol code and the cause.
 * @param {Error} error Original failure.
 * @returns {Error} A non-retriable failure; code 35 becomes the share capability error naming the
 *   ShareGroupHeartbeat API, every other code a plain non-retriable wrapper.
 */
const fatalMembershipError = (error) =>
  error.code === 35
    ? new KafkaJSShareGroupsNotSupportedError(error, {
        apiKey: 76,
        apiName: 'ShareGroupHeartbeat',
        reason: 'refused-by-broker',
      })
    : new KafkaJSNonRetriableError(error, { cause: error })

/**
 * Classifies a session-level reply without borrowing the membership table. Code 133 resets only a
 * ShareFetch session; on ShareAcknowledge it is an ordinary error that still advances the epoch.
 * @param {78|79} apiKey Actual request API.
 * @param {number} code Top-level broker code.
 * @returns {{action: 'ok'|'reset'|'fence'|'fatal'|'retry', backoff: boolean, error: Error|null}}
 *   `reset` reopens at epoch zero and always asks for backoff here (the session decides whether a
 *   first reset is immediate); `fence` is for the membership to judge; `fatal` is terminal and
 *   never retried; `retry` is paced. Every non-reset answer still advances the epoch.
 */
const sessionAction = (apiKey, code) => {
  if (code === 0) return { action: 'ok', backoff: false, error: null }
  const error = createErrorFromCode(code)
  if (code === 122 || code === 123 || (code === 133 && apiKey === 78))
    return { action: 'reset', backoff: true, error }
  if (code === 25) return { action: 'fence', backoff: false, error }
  if (code === 35) {
    return {
      action: 'fatal',
      backoff: false,
      error: new KafkaJSShareGroupsNotSupportedError(error, {
        apiKey,
        apiName: apiKey === 78 ? 'ShareFetch' : 'ShareAcknowledge',
        reason: 'refused-by-broker',
      }),
    }
  }
  const fatal = code === 30 || (code !== -1 && error.retriable === false)
  return { action: fatal ? 'fatal' : 'retry', backoff: true, error }
}

/**
 * Creates the terminal partition failure while keeping its protocol identity.
 * @param {Error} error Classified partition failure.
 * @returns {Error} A `KafkaJSProtocolError` of the same code with `retriable` forced to false, which
 *   also overrides the table flag of code 2.
 */
const fatalFetchError = (error) => {
  const protocol = createErrorFromCode(error.code)
  protocol.retriable = false
  return protocol
}

/**
 * Classifies a ShareFetch partition error. The acknowledgement result of the same partition is
 * judged separately by {@link acknowledgementAction}.
 * @param {number} code Partition fetch error code.
 * @returns {{action: 'ok'|'move'|'refresh'|'retry'|'fatal', error: Error|null}} `move` follows a
 *   leader change, `refresh` reloads metadata and retries under backoff, `retry` only retries
 *   under backoff (unknown error -1), `fatal` ends the run.
 */
const fetchAction = (code) => {
  if (code === 0) return { action: 'ok', error: null }
  const error = createErrorFromCode(code)
  if (code === 6 || code === 74) return { action: 'move', error }
  if ([3, 100, 103, 9, 56, 78].includes(code)) return { action: 'refresh', error }
  if (code === -1) return { action: 'retry', error }
  if (code === 2 || code === 29 || error.retriable === false)
    return { action: 'fatal', error: fatalFetchError(error) }
  return { action: 'refresh', error }
}

/**
 * Classifies one acquisition's acknowledgement result, read from its own field and never inferred
 * from a fetch error.
 * @param {number} code ShareFetch `acknowledgeErrorCode` or ShareAcknowledge partition `errorCode`.
 * @returns {{result: 'ok'|'session-lost'|'rejected-by-broker', cause: Error|undefined}} Only code 0
 *   confirms; 6 and 74 mean the session lost its leader; every other code is a rejection.
 */
const acknowledgementAction = (code) => ({
  result: code === 0 ? 'ok' : code === 6 || code === 74 ? 'session-lost' : 'rejected-by-broker',
  cause: code === 0 ? undefined : createErrorFromCode(code),
})

/**
 * Classifies a send failure, timeout or closed connection without guessing whether the broker
 * applied the request.
 * @param {Error} error Original transport failure.
 * @returns {{action: 'reset', result: 'session-lost', reason: 'connection', error: Error}} Old
 *   acquisitions settle session-lost and are never accepted or resent.
 */
const transportAction = (error) => ({
  action: 'reset',
  result: 'session-lost',
  reason: 'connection',
  error,
})

module.exports = {
  MEMBERSHIP_ERRORS,
  membershipAction,
  fatalMembershipError,
  sessionAction,
  transportAction,
  fetchAction,
  acknowledgementAction,
  fatalFetchError,
  createErrorFromCode,
}
