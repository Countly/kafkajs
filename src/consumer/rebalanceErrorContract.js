const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
  isStaleCoordinator,
  isPollIntervalExceeded,
  isFatalMembership,
  CONSUMER_PROTOCOL_UNSUPPORTED,
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

const COORDINATOR_LOAD_IN_PROGRESS = 14
const GROUP_AUTHORIZATION_FAILED = 30
const INVALID_REQUEST = 42
const UNKNOWN_MEMBER_ID = 25
const FENCED_MEMBER_EPOCH = 110
const GROUP_ID_NOT_FOUND = 69

/**
 * Marks an error as one an OffsetCommit produced.
 *
 * Symbol-keyed for the same reason `SENT_GENERATION` is: it never reaches a user's logger or a
 * JSON dump. It exists because a commit failure is not classified where it is raised - an
 * auto-commit's error travels out through the fetch round and is classified on the FETCH path -
 * so the request that drew it has to be recorded on the error itself.
 */
const OFFSET_COMMIT_ORIGIN = Symbol('rebalance:offsetCommit')

/**
 * Records that this error answered an OffsetCommit.
 * @param {Error} error
 * @returns {Error} the same error
 */
const stampOffsetCommit = (error) => {
  if (error) {
    error[OFFSET_COMMIT_ORIGIN] = true
  }

  return error
}

/**
 * Whether a vanished group answered this member's OffsetCommit.
 *
 * Apache Kafka's `OffsetMetadataManager.validateOffsetCommit` answers `GROUP_ID_NOT_FOUND` (69) at
 * OffsetCommit v9 and above where v8 answered `ILLEGAL_GENERATION` (22) - the version branch is
 * explicit in the broker, and this client negotiates v9 against every Kafka 4.x broker. The two
 * codes mean the same thing to a member (the group it committed under is gone), so 69 takes the
 * verdict 22 has always taken: give up the uncommitted offsets and rejoin, rather than crash.
 * Scoped to the commit path by the stamp, so 69 keeps its own meaning everywhere else - it is
 * still a non-retriable "no such group" for `admin.fetchOffsets`, and still the signal that routes
 * a `ConsumerGroupDescribe` row to the classic API.
 * @see https://github.com/apache/kafka/blob/4.3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java
 * @param {Error} error
 * @returns {boolean}
 */
const isDeletedGroupCommit = (error) =>
  error.code === GROUP_ID_NOT_FOUND && Boolean(error[OFFSET_COMMIT_ORIGIN])

/**
 * Whether an error came from an OffsetCommit this client sent: its coordinator lookup, the request
 * or its answer.
 * @param {unknown} error
 * @returns {boolean}
 */
const isOffsetCommitError = (error) =>
  error !== null && typeof error === 'object' && Boolean(error[OFFSET_COMMIT_ORIGIN])

/** The message the Java client gives when the cluster cannot speak the new group protocol. */
const CONSUMER_PROTOCOL_UNSUPPORTED_MESSAGE =
  "The cluster does not support the new CONSUMER group protocol. Set groupProtocol: 'classic' on the consumer until the cluster is upgraded."

/**
 * Every error that ends a consumer-protocol membership for good.
 *
 * Wider than the exported `isFatalMembership` by two codes, on purpose: a fenced instance id, a
 * group authorization failure and a malformed request are all fatal to a member of a consumer-type
 * group, but the exported predicate is also honoured on the classic path - where adding them would
 * change behaviour no decision asked to change.
 * @param {Error} error
 * @returns {boolean}
 */
const isConsumerProtocolFatal = (error) =>
  isFatalMembership(error) ||
  isFencedInstance(error) ||
  error.code === GROUP_AUTHORIZATION_FAILED ||
  error.code === INVALID_REQUEST

/**
 * Classifies one rebalance-class error.
 *
 * The consumer-protocol rows are the coordinator's own handling of the same codes: a loading
 * coordinator is retried by the heartbeat itself rather than being treated as a rebalance, a moved
 * coordinator is re-found, and the group-level failures are not rejoined into.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java
 * @param {Error} error
 * @param {string} path one of `REBALANCE_PATHS`
 * @param {{ kind?: 'classic' | 'consumer' }} [membership] which membership protocol raised it. The
 *   default keeps every classic caller's answer identical; three codes classify differently under
 *   the consumer protocol, where a coordinator move is not a rebalance and the group's fatal errors
 *   must not be rejoined into
 * @returns {{ action: 'lost'|'revoked'|'retry-coordinator'|null, fatal: boolean,
 *   clearsMemberId: boolean }}
 * @throws {Error} when `path` is not one of the four known paths
 */
const rebalanceAction = (error, path, { kind = 'classic' } = {}) => {
  if (!PATH_VALUES.has(path)) {
    throw new Error(`Unknown rebalance path "${path}"`)
  }

  if (isPollIntervalExceeded(error)) {
    return { action: 'lost', fatal: false, clearsMemberId: true }
  }

  // Before every other row, and for both protocols: the member id is still valid (only the group
  // is gone), so this is the ILLEGAL_GENERATION verdict, not the UNKNOWN_MEMBER_ID one.
  if (isDeletedGroupCommit(error)) {
    return { action: 'lost', fatal: false, clearsMemberId: false }
  }

  if (kind === 'consumer') {
    // A loading coordinator is retried by the membership itself and is not a rebalance at all;
    // a coordinator that moved is simply re-found. Both are matched by `isRebalancing` below,
    // which is why they have to be answered first.
    if (error.code === COORDINATOR_LOAD_IN_PROGRESS) {
      return { action: null, fatal: false, clearsMemberId: false }
    }

    if (isStaleCoordinator(error)) {
      return { action: 'retry-coordinator', fatal: false, clearsMemberId: false }
    }

    if (isConsumerProtocolFatal(error)) {
      return { action: 'lost', fatal: true, clearsMemberId: false }
    }

    if (isLostMember(error)) {
      // Never clears the member id, 25 included: this protocol generates its id once per membership
      // and keeps it across fencings, so a caller acting on `clearsMemberId` would be told to do
      // something the membership refuses to do anyway. Saying `false` here is what makes the
      // contract's answer and the membership's behaviour the same statement.
      return { action: 'lost', fatal: isFencedInstance(error), clearsMemberId: false }
    }
  }

  if (isLostMember(error)) {
    return {
      action: 'lost',
      fatal: isFencedInstance(error),
      clearsMemberId: isUnknownMemberId(error),
    }
  }

  // Before the revoked row: a coordinator that moved or is unavailable (15, 16) is looked up again
  // and the member keeps its membership, as the Java client marks it unknown and rejoins nothing.
  // @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
  if (isStaleCoordinator(error)) {
    return { action: 'retry-coordinator', fatal: false, clearsMemberId: false }
  }

  if (isRevoked(error)) {
    return { action: 'revoked', fatal: false, clearsMemberId: false }
  }

  return { action: null, fatal: false, clearsMemberId: false }
}

/**
 * The reason string a rebalance round reports for one error.
 *
 * The consumer-protocol rows follow the coordinator's own vocabulary: an unknown member id and a
 * stale member epoch both mean the coordinator has stopped recognising this member, which it calls
 * fencing rather than loss.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java
 * @param {Error} error
 * @param {{ kind?: 'classic' | 'consumer' }} [membership]
 * @returns {'lost'|'fenced'|'poll-interval-exceeded'|'rebalance-in-progress'|'coordinator-changed'|null}
 *   null for an error that is not a rebalance at all. `assignment-changed` is never returned: it is
 *   supplied by whoever raises the verdict, because no error describes it
 */
const rebalanceReason = (error, { kind = 'classic' } = {}) => {
  if (isPollIntervalExceeded(error)) {
    return 'poll-interval-exceeded'
  }

  if (isDeletedGroupCommit(error)) {
    return 'lost'
  }

  if (kind === 'consumer') {
    if (error.code === UNKNOWN_MEMBER_ID || error.code === FENCED_MEMBER_EPOCH) {
      return 'fenced'
    }

    if (isConsumerProtocolFatal(error)) {
      return 'lost'
    }
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
 * The verdict a new target assignment raises. Never surfaces to a user: the verdict paths are all
 * error-shaped, and this one carries no failure at all.
 * @returns {KafkaJSError}
 */
const assignmentChangedError = () => {
  const error = new KafkaJSError('The group coordinator assigned a different set of partitions', {
    retriable: false,
  })
  error.type = 'ASSIGNMENT_CHANGED'
  return error
}

/**
 * Fails whatever the membership is waiting on because the consumer is stopping. The round loop
 * returns rather than rethrowing when it sees this while a stop is in progress, so a normal
 * shutdown reports no crash.
 * @returns {KafkaJSError}
 */
const consumerStoppingError = () => {
  const error = new KafkaJSError('The consumer is stopping', { retriable: false })
  error.type = 'CONSUMER_STOPPING'
  return error
}

/**
 * The deadline a join is allowed to take. Retriable on purpose: a coordinator that is still loading
 * answers an error the membership swallows, so without a deadline the round would never settle and
 * a stop would never complete - and the right response to it is another attempt, not a crash.
 * @param {{ rebalanceTimeoutMs: number }} details
 * @returns {KafkaJSError}
 */
const joinDeadlineError = ({ rebalanceTimeoutMs }) => {
  const error = new KafkaJSError(`Timed out joining the group after ${rebalanceTimeoutMs} ms`, {
    retriable: true,
  })
  error.type = 'JOIN_DEADLINE_EXCEEDED'
  return error
}

/**
 * The cluster cannot speak the new group protocol: it answered UNSUPPORTED_VERSION to a heartbeat,
 * or it advertises no version of the heartbeat API at all.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @param {Error} [cause] the answer that proved it, when there was one
 * @returns {KafkaJSNonRetriableError}
 */
const unsupportedConsumerProtocolError = (cause) => {
  const error = new KafkaJSNonRetriableError(CONSUMER_PROTOCOL_UNSUPPORTED_MESSAGE)
  error.type = CONSUMER_PROTOCOL_UNSUPPORTED

  if (cause && cause.code != null) {
    error.code = cause.code
  }

  return error
}

/**
 * The generation a group request was sent under, remembered on the error its answer produced.
 *
 * Symbol-keyed, so it never reaches a user's logger or a JSON dump of the error - it exists only
 * so a lost-membership answer can be matched against the generation the member holds now.
 */
const SENT_GENERATION = Symbol('rebalance:sentGenerationId')
const SENT_OWNERSHIP = Symbol('rebalance:sentOwnership')

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

/**
 * Records the membership identity captured before a group operation awaited anything.
 * @param {Error} error the operation's error
 * @param {number} token its membership identity
 * @returns {Error} the same error, with a private stamp
 */
const stampOwnership = (error, token) => {
  if (error && token != null && error[SENT_OWNERSHIP] == null) error[SENT_OWNERSHIP] = token
  return error
}

/** @param {Error} error @returns {number|undefined} its original membership identity */
const sentOwnership = (error) => error && error[SENT_OWNERSHIP]

/**
 * Whether an answer belongs to a membership already replaced. Unstamped local errors use current.
 * @param {object|null} ownership the run's GroupOwnership, absent on isolated collaborators
 * @param {Error} error the answer to inspect before any state change
 * @returns {boolean} true only for a stamped obsolete answer
 */
const obsoleteOwnership = (ownership, error) => {
  const token = sentOwnership(error)
  return Boolean(ownership && token != null && token !== ownership.token())
}

/**
 * Learns a path-aware loss before cleanup or a retry can await anything.
 * @param {object|null} ownership the consumer's membership fence
 * @param {Error} error the original, stamped operation error
 * @param {string} path one of REBALANCE_PATHS
 * @param {'classic'|'consumer'} kind the group protocol
 * @returns {boolean} whether this current operation learned a path-aware loss
 */
const learnOwnershipLoss = (ownership, error, path, kind) => {
  if (!ownership || obsoleteOwnership(ownership, error)) return false
  if (rebalanceAction(error, path, { kind }).action === 'lost') {
    ownership.lose(error, sentOwnership(error) ?? ownership.token())
    return true
  }
  return false
}

module.exports = {
  REBALANCE_PATHS,
  rebalanceAction,
  rebalanceReason,
  isConsumerProtocolFatal,
  stampOffsetCommit,
  isDeletedGroupCommit,
  isOffsetCommitError,
  pollIntervalExceededError,
  assignmentChangedError,
  consumerStoppingError,
  joinDeadlineError,
  unsupportedConsumerProtocolError,
  stampGeneration,
  sentGeneration,
  stampOwnership,
  sentOwnership,
  obsoleteOwnership,
  learnOwnershipLoss,
}
