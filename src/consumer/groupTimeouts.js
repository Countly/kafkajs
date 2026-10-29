const { inspect } = require('util')
const { KafkaJSNonRetriableError } = require('../errors')

/**
 * Java's `max.poll.interval.ms` default. Both the wire rebalance timeout and the local poll
 * deadline start here.
 * @see https://kafka.apache.org/documentation/#consumerconfigs_max.poll.interval.ms
 */
const DEFAULT_MAX_POLL_INTERVAL = 300000

/**
 * The client-side ceiling on how long a SyncGroup request may block. Mirrors `MAX_TIMEOUT` in
 * `../protocol/requests/syncGroup/index.js`, which is where the codec actually derives the
 * request timeout; the value reported here is a prediction of that derivation, so the two
 * constants must move together.
 */
const SYNC_GROUP_MAX_TIMEOUT = 60000

/** Slack added to every derived request timeout, matching the protocol layer's own constant. */
const NETWORK_DELAY = 5000

/**
 * Resolves the group's timeout model from what the user configured.
 *
 * One number governs both halves in Java: how long the group waits for this member during a
 * rebalance (the wire `RebalanceTimeoutMs`) and how long the member may spend inside a handler
 * before it is evicted. `maxPollInterval` is that number. `rebalanceTimeout` remains as a
 * deprecated alias for the wire half ONLY: it never lowers the poll deadline, so an existing
 * configuration cannot silently acquire a short handler-eviction deadline.
 *
 * @param {object} options
 * @param {number} [options.maxPollInterval] explicit; validated, and it wins over the alias
 * @param {number} [options.rebalanceTimeout] the deprecated alias; sent on the wire when it is
 *   the only one given, degenerate values (0, non-integers) grandfathered
 * @param {number} [options.sessionTimeout=45000]
 * @param {number} [options.heartbeatInterval=3000]
 * @param {import('../../types').Logger} options.logger
 * @returns {{ rebalanceTimeout: number, pollDeadline: number, sessionTimeout: number,
 *   heartbeatInterval: number, syncGroupTimeout: number }} `rebalanceTimeout` is the wire value;
 *   `syncGroupTimeout` reports the capped SyncGroup request timeout the protocol layer derives
 *   from it — it is logged, never sent, and the codec remains the authority on that number
 * @throws {KafkaJSNonRetriableError} when either timeout is invalid or `heartbeatInterval`
 *   is not lower than `sessionTimeout`,
 *   or when an explicit `maxPollInterval` is not an integer of at least 1
 */
const resolveGroupTimeouts = ({
  maxPollInterval,
  rebalanceTimeout,
  sessionTimeout = 45000,
  heartbeatInterval = 3000,
  logger,
}) => {
  if (!Number.isInteger(sessionTimeout) || sessionTimeout < 1 || sessionTimeout > 2147483647) {
    throw new KafkaJSNonRetriableError(
      `Consumer sessionTimeout must be an integer from 1 to 2147483647, got ${inspect(sessionTimeout)}`
    )
  }

  if (!Number.isInteger(heartbeatInterval) || heartbeatInterval < 1) {
    throw new KafkaJSNonRetriableError(
      `Consumer heartbeatInterval must be an integer of at least 1, got ${inspect(heartbeatInterval)}`
    )
  }

  if (heartbeatInterval >= sessionTimeout) {
    throw new KafkaJSNonRetriableError(
      `Consumer heartbeatInterval (${heartbeatInterval}) must be lower than sessionTimeout (${sessionTimeout}). It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.`
    )
  }

  if (heartbeatInterval > sessionTimeout / 3) {
    logger.warn(
      'Consumer heartbeatInterval is more than a third of sessionTimeout; the heartbeat timer drifts by one request duration per tick, so the member may be evicted',
      { heartbeatInterval, sessionTimeout }
    )
  }

  const hasMaxPollInterval = maxPollInterval !== undefined
  const hasRebalanceTimeout = rebalanceTimeout !== undefined

  if (hasMaxPollInterval && (!Number.isInteger(maxPollInterval) || maxPollInterval < 1)) {
    throw new KafkaJSNonRetriableError(
      `Consumer maxPollInterval must be an integer of at least 1, got ${inspect(maxPollInterval)}`
    )
  }

  const wire = hasMaxPollInterval
    ? maxPollInterval
    : hasRebalanceTimeout
      ? rebalanceTimeout
      : DEFAULT_MAX_POLL_INTERVAL

  // The floor is the whole point of the alias rule: a legacy 20 s rebalance timeout must not
  // become a 20 s handler-eviction deadline.
  const pollDeadline = hasMaxPollInterval
    ? maxPollInterval
    : Math.max(hasRebalanceTimeout ? rebalanceTimeout : 0, DEFAULT_MAX_POLL_INTERVAL)

  // `||`, matching the two shipped request-timeout helpers: a configured 0 falls back to
  // sessionTimeout rather than producing a 5 s request timeout.
  const syncGroupTimeout = Math.min(wire || sessionTimeout, SYNC_GROUP_MAX_TIMEOUT) + NETWORK_DELAY

  if (hasRebalanceTimeout && hasMaxPollInterval && rebalanceTimeout !== maxPollInterval) {
    logger.warn(
      'Both maxPollInterval and the deprecated rebalanceTimeout are set and disagree; maxPollInterval wins',
      { maxPollInterval, rebalanceTimeout, rebalanceTimeoutOnTheWire: wire, pollDeadline }
    )
  } else if (hasRebalanceTimeout) {
    logger.warn(
      'rebalanceTimeout is deprecated; use maxPollInterval, which is both the wire rebalance timeout and the handler deadline',
      { rebalanceTimeout, rebalanceTimeoutOnTheWire: wire, pollDeadline, syncGroupTimeout }
    )
  }

  return {
    rebalanceTimeout: wire,
    pollDeadline,
    sessionTimeout,
    heartbeatInterval,
    syncGroupTimeout,
  }
}

module.exports = resolveGroupTimeouts
