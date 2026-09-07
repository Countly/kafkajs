const pkgJson = require('../package.json')
const { bugs } = pkgJson

class KafkaJSError extends Error {
  constructor(e, { retriable = true, cause } = {}) {
    super(e, { cause })
    Error.captureStackTrace(this, this.constructor)
    this.message = e.message || e
    this.name = 'KafkaJSError'
    this.retriable = retriable
    this.helpUrl = e.helpUrl
    this.cause = cause

    // Protocol errors are frequently re-wrapped on the consumer paths (rebalance, unknown
    // member, retry exhaustion). Losing `type`/`code` forces callers to string-match the
    // message, so carry them across every wrap. Only set when the source actually has them.
    if (e != null && e.type !== undefined) {
      this.type = e.type
    }
    if (e != null && e.code !== undefined) {
      this.code = e.code
    }
  }
}

class KafkaJSNonRetriableError extends KafkaJSError {
  constructor(e, { cause } = {}) {
    super(e, { retriable: false, cause })
    this.name = 'KafkaJSNonRetriableError'
  }
}

class KafkaJSProtocolError extends KafkaJSError {
  constructor(e, { retriable = e.retriable } = {}) {
    super(e, { retriable })
    this.type = e.type
    this.code = e.code
    this.name = 'KafkaJSProtocolError'
  }
}

class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  constructor(e, { topic, partition }) {
    super(e)
    this.topic = topic
    this.partition = partition
    this.name = 'KafkaJSOffsetOutOfRange'
  }
}

class KafkaJSMemberIdRequired extends KafkaJSProtocolError {
  constructor(e, { memberId }) {
    super(e)
    this.memberId = memberId
    this.name = 'KafkaJSMemberIdRequired'
  }
}

class KafkaJSNumberOfRetriesExceeded extends KafkaJSNonRetriableError {
  constructor(e, { retryCount, retryTime }) {
    super(e, { cause: e })
    this.stack = `${this.name}\n  Caused by: ${e.stack}`
    this.retryCount = retryCount
    this.retryTime = retryTime
    this.name = 'KafkaJSNumberOfRetriesExceeded'

    // The wrapped error may itself be a wrapper (KafkaJSError around a protocol error), so
    // walk the chain for the first typed link.
    let source = e
    while (source && source.type === undefined && source.cause) {
      source = source.cause
    }
    if (source && source.type !== undefined) {
      this.type = source.type
      this.code = source.code
    }
  }
}

class KafkaJSConnectionError extends KafkaJSError {
  /**
   * @param {string} e
   * @param {object} options
   * @param {string} [options.broker]
   * @param {string} [options.code]
   */
  constructor(e, { broker, code } = {}) {
    super(e)
    this.broker = broker
    this.code = code
    this.name = 'KafkaJSConnectionError'
  }
}

class KafkaJSConnectionClosedError extends KafkaJSConnectionError {
  constructor(e, { host, port } = {}) {
    super(e, { broker: `${host}:${port}` })
    this.host = host
    this.port = port
    this.name = 'KafkaJSConnectionClosedError'
  }
}

class KafkaJSRequestTimeoutError extends KafkaJSError {
  constructor(e, { broker, correlationId, createdAt, sentAt, pendingDuration } = {}) {
    super(e)
    this.broker = broker
    this.correlationId = correlationId
    this.createdAt = createdAt
    this.sentAt = sentAt
    this.pendingDuration = pendingDuration
    this.name = 'KafkaJSRequestTimeoutError'
  }
}

class KafkaJSMetadataNotLoaded extends KafkaJSError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSMetadataNotLoaded'
  }
}
class KafkaJSTopicMetadataNotLoaded extends KafkaJSMetadataNotLoaded {
  constructor(e, { topic } = {}) {
    super(e)
    this.topic = topic
    this.name = 'KafkaJSTopicMetadataNotLoaded'
  }
}
class KafkaJSStaleTopicMetadataAssignment extends KafkaJSError {
  constructor(e, { topic, unknownPartitions } = {}) {
    super(e)
    this.topic = topic
    this.unknownPartitions = unknownPartitions
    this.name = 'KafkaJSStaleTopicMetadataAssignment'
  }
}

class KafkaJSDeleteGroupsError extends KafkaJSError {
  constructor(e, groups = []) {
    super(e)
    this.groups = groups
    this.name = 'KafkaJSDeleteGroupsError'
  }
}

class KafkaJSServerDoesNotSupportApiKey extends KafkaJSNonRetriableError {
  constructor(e, { apiKey, apiName } = {}) {
    super(e)
    this.apiKey = apiKey
    this.apiName = apiName
    this.name = 'KafkaJSServerDoesNotSupportApiKey'
  }
}

class KafkaJSBrokerNotFound extends KafkaJSError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSBrokerNotFound'
  }
}

class KafkaJSPartialMessageError extends KafkaJSNonRetriableError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSPartialMessageError'
  }
}

class KafkaJSSASLAuthenticationError extends KafkaJSNonRetriableError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSSASLAuthenticationError'
  }
}

class KafkaJSGroupCoordinatorNotFound extends KafkaJSNonRetriableError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSGroupCoordinatorNotFound'
  }
}

class KafkaJSNotImplemented extends KafkaJSNonRetriableError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSNotImplemented'
  }
}

class KafkaJSTimeout extends KafkaJSNonRetriableError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSTimeout'
  }
}

class KafkaJSLockTimeout extends KafkaJSTimeout {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSLockTimeout'
  }
}

class KafkaJSUnsupportedMagicByteInMessageSet extends KafkaJSNonRetriableError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSUnsupportedMagicByteInMessageSet'
  }
}

class KafkaJSDeleteTopicRecordsError extends KafkaJSError {
  constructor({ partitions }) {
    /*
     * This error is retriable if all the errors were retriable
     */
    const retriable = partitions
      .filter(({ error }) => error != null)
      .every(({ error }) => error.retriable === true)

    super('Error while deleting records', { retriable })
    this.name = 'KafkaJSDeleteTopicRecordsError'
    this.partitions = partitions
  }
}

const issueUrl = bugs ? bugs.url : null

class KafkaJSInvariantViolation extends KafkaJSNonRetriableError {
  constructor(e) {
    const message = e.message || e
    super(`Invariant violated: ${message}. This is likely a bug and should be reported.`)
    this.name = 'KafkaJSInvariantViolation'

    if (issueUrl !== null) {
      const issueTitle = encodeURIComponent(`Invariant violation: ${message}`)
      this.helpUrl = `${issueUrl}/new?assignees=&labels=bug&template=bug_report.md&title=${issueTitle}`
    }
  }
}

class KafkaJSInvalidVarIntError extends KafkaJSNonRetriableError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSNonRetriableError'
  }
}

class KafkaJSInvalidLongError extends KafkaJSNonRetriableError {
  constructor() {
    super(...arguments)
    this.name = 'KafkaJSNonRetriableError'
  }
}

class KafkaJSCreateTopicError extends KafkaJSProtocolError {
  constructor(e, topicName) {
    super(e)
    this.topic = topicName
    this.name = 'KafkaJSCreateTopicError'
  }
}

class KafkaJSAlterPartitionReassignmentsError extends KafkaJSProtocolError {
  constructor(e, topicName, partition) {
    super(e)
    this.topic = topicName
    this.partition = partition
    this.name = 'KafkaJSAlterPartitionReassignmentsError'
  }
}

class KafkaJSAggregateError extends Error {
  constructor(message, errors) {
    super(message)
    this.errors = errors
    this.name = 'KafkaJSAggregateError'
  }
}

class KafkaJSFetcherRebalanceError extends Error {}

class KafkaJSNoBrokerAvailableError extends KafkaJSError {
  constructor() {
    super('No broker available')
    this.name = 'KafkaJSNoBrokerAvailableError'
  }
}

/**
 * Group-coordinator error codes. The error table still emits the legacy type names for
 * 14/15/16 (GROUP_LOAD_IN_PROGRESS / GROUP_COORDINATOR_NOT_AVAILABLE / NOT_COORDINATOR_FOR_GROUP)
 * for compatibility, so callers must match on the code, not the type string.
 */
const COORDINATOR_LOAD_IN_PROGRESS_CODE = 14
const COORDINATOR_NOT_AVAILABLE_CODE = 15
const NOT_COORDINATOR_CODE = 16

/** The cached group coordinator is wrong or gone: re-run FindCoordinator before retrying. */
const isStaleCoordinator = (e) =>
  e.code === COORDINATOR_NOT_AVAILABLE_CODE || e.code === NOT_COORDINATOR_CODE

const isRebalancing = (e) =>
  e.type === 'REBALANCE_IN_PROGRESS' ||
  e.type === 'NOT_COORDINATOR_FOR_GROUP' ||
  e.type === 'ILLEGAL_GENERATION' ||
  e.code === COORDINATOR_LOAD_IN_PROGRESS_CODE

const ILLEGAL_GENERATION_CODE = 22
const UNKNOWN_MEMBER_ID_CODE = 25
const FENCED_INSTANCE_ID_CODE = 82

/**
 * The member is no longer part of the group: its generation, member id or static instance id
 * has been invalidated by the coordinator. Uncommitted offsets belong to a generation the
 * coordinator has already discarded, so committing them fails forever (upstream #1712, #1466,
 * #1527, #1009). Discard the uncommitted offsets and rejoin instead.
 */
const isLostMember = (e) =>
  e.code === ILLEGAL_GENERATION_CODE ||
  e.code === UNKNOWN_MEMBER_ID_CODE ||
  e.code === FENCED_INSTANCE_ID_CODE

/** True for a member fenced out of the group by a duplicate static instance id (code 82). */
const isFencedInstance = (e) => e.code === FENCED_INSTANCE_ID_CODE

/** True for a member the coordinator no longer recognizes by member id (code 25). */
const isUnknownMemberId = (e) => e.code === UNKNOWN_MEMBER_ID_CODE

/**
 * The member is still valid: every rebalance-class error isRebalancing() matches (partitions
 * revoked, the coordinator moved or is still loading, ...) minus the LOST codes above. The
 * generation is good in all of these, so a final commit of already-resolved offsets is allowed
 * before rejoining. Defined this way (rather than an explicit code list) so this set stays
 * byte-identical to isRebalancing()'s, minus whatever isLostMember() claims - no code that
 * isRebalancing() covers today or in the future is silently dropped from the commit-allowed set.
 */
const isRevoked = (e) => isRebalancing(e) && !isLostMember(e)

/**
 * The consumer exceeded its own poll-interval deadline. This is the client's own condition, not
 * a broker error code, and it deliberately has no public error class: it exists so the rebalance
 * paths - all of which are error-shaped - can carry it like any other verdict.
 */
const isPollIntervalExceeded = (e) => e.type === 'POLL_INTERVAL_EXCEEDED'

const isKafkaJSError = (e) => e instanceof KafkaJSError

module.exports = {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSPartialMessageError,
  KafkaJSBrokerNotFound,
  KafkaJSProtocolError,
  KafkaJSConnectionError,
  KafkaJSConnectionClosedError,
  KafkaJSRequestTimeoutError,
  KafkaJSSASLAuthenticationError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSOffsetOutOfRange,
  KafkaJSMemberIdRequired,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSNotImplemented,
  KafkaJSMetadataNotLoaded,
  KafkaJSTopicMetadataNotLoaded,
  KafkaJSStaleTopicMetadataAssignment,
  KafkaJSDeleteGroupsError,
  KafkaJSTimeout,
  KafkaJSLockTimeout,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSUnsupportedMagicByteInMessageSet,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSInvariantViolation,
  KafkaJSInvalidVarIntError,
  KafkaJSInvalidLongError,
  KafkaJSCreateTopicError,
  KafkaJSAggregateError,
  KafkaJSFetcherRebalanceError,
  KafkaJSNoBrokerAvailableError,
  KafkaJSAlterPartitionReassignmentsError,
  isRebalancing,
  isStaleCoordinator,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
  isPollIntervalExceeded,
  isKafkaJSError,
}
