const {
  KafkaJSError,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
  isStaleCoordinator,
  isPollIntervalExceeded,
} = require('../errors')

/**
 * The four places a rebalance-class error can arrive. They agree on the action today; the
 * parameter exists so each column of the contract has a named case and so a typo throws instead
 * of being classified as something.
 */
const REBALANCE_PATHS = {
  MEMBERSHIP: 'membership',
  HEARTBEAT: 'heartbeat',
  OFFSET_COMMIT: 'offset-commit',
  FETCH: 'fetch',
}

const PATH_VALUES = new Set(Object.values(REBALANCE_PATHS))

/**
 * Classifies one rebalance-class error.
 *
 * Every code decision is delegated to the predicates in `src/errors.js`, which stay the single
 * source of truth: this module holds no code list of its own, so a predicate that grows a code
 * grows this contract with it.
 *
 * `clearsMemberId` distinguishes the two lost codes: `UNKNOWN_MEMBER_ID` (25) means the
 * coordinator does not recognise the id, so it must be dropped, while `ILLEGAL_GENERATION` (22)
 * invalidates only the generation - keeping the id avoids an extra MEMBER_ID_REQUIRED round trip.
 *
 * @param {Error} error
 * @param {string} path one of `REBALANCE_PATHS`
 * @returns {{ action: 'lost'|'revoked'|'retry-coordinator'|null, fatal: boolean,
 *   clearsMemberId: boolean }} `action` is null for an error that is not a rebalance;
 *   `fatal` marks a fenced static member, which must not rejoin into the same fencing loop
 * @throws {Error} when `path` is not one of the four known paths
 */
const rebalanceAction = (error, path) => {
  if (!PATH_VALUES.has(path)) {
    throw new Error(`Unknown rebalance path "${path}"`)
  }

  if (isPollIntervalExceeded(error)) {
    return { action: 'lost', fatal: false, clearsMemberId: true }
  }

  if (isLostMember(error)) {
    return {
      action: 'lost',
      fatal: isFencedInstance(error),
      clearsMemberId: isUnknownMemberId(error),
    }
  }

  if (isRevoked(error)) {
    return { action: 'revoked', fatal: false, clearsMemberId: false }
  }

  if (isStaleCoordinator(error)) {
    return { action: 'retry-coordinator', fatal: false, clearsMemberId: false }
  }

  return { action: null, fatal: false, clearsMemberId: false }
}

/**
 * The `reason` a rebalance triggered by this error reports, total for every rebalance-class
 * error so an emit can never be skipped for want of a reason.
 *
 * The order matters. `isLostMember` is code-only, so a bare `{ type: 'ILLEGAL_GENERATION' }`
 * falls through to the coordinator-changed branch, which is what the shipped runner already did
 * with it.
 *
 * @param {Error} error
 * @returns {'lost'|'poll-interval-exceeded'|'rebalance-in-progress'|'coordinator-changed'|null}
 *   null for an error that is not a rebalance at all
 */
const rebalanceReason = (error) => {
  if (isPollIntervalExceeded(error)) {
    return 'poll-interval-exceeded'
  }

  if (isLostMember(error)) {
    return 'lost'
  }

  if (error.type === 'REBALANCE_IN_PROGRESS') {
    return 'rebalance-in-progress'
  }

  if (isRevoked(error) || isStaleCoordinator(error)) {
    return 'coordinator-changed'
  }

  return null
}

/**
 * The internal error the poll-interval guard raises. Not a public class (none is added by this
 * release) and never thrown at a user: it exists so the verdict paths, all of which are
 * error-shaped, can carry the guard like any broker error.
 * @param {{ elapsedMs: number, maxPollInterval: number }} details
 * @returns {KafkaJSError}
 */
const pollIntervalExceededError = ({ elapsedMs, maxPollInterval }) => {
  const error = new KafkaJSError(
    `The consumer took ${elapsedMs} ms between handler invocations, longer than maxPollInterval (${maxPollInterval} ms)`,
    { retriable: false }
  )
  error.type = 'POLL_INTERVAL_EXCEEDED'
  error.elapsedMs = elapsedMs
  error.maxPollInterval = maxPollInterval
  return error
}

/**
 * The generation a group request was sent under, remembered on the error its answer produced.
 *
 * Symbol-keyed, so it never reaches a user's logger or a JSON dump of the error - it exists only
 * so a lost-membership answer can be matched against the generation the member holds now.
 */
const SENT_GENERATION = Symbol('rebalance:sentGenerationId')

/**
 * Records the generation a failing group request was sent under.
 * @param {Error} error
 * @param {number|null} generationId
 * @returns {Error} the same error
 */
const stampGeneration = (error, generationId) => {
  if (error && generationId != null) {
    error[SENT_GENERATION] = generationId
  }

  return error
}

/**
 * @param {Error} error
 * @returns {number|undefined} the generation the request behind this error was sent under
 */
const sentGeneration = (error) => error && error[SENT_GENERATION]

module.exports = {
  REBALANCE_PATHS,
  rebalanceAction,
  rebalanceReason,
  pollIntervalExceededError,
  stampGeneration,
  sentGeneration,
}
