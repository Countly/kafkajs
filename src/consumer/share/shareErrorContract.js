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

module.exports = {
  MEMBERSHIP_ERRORS,
  membershipAction,
  fatalMembershipError,
  createErrorFromCode,
}
