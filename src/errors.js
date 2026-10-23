const { isNativeError } = require('util').types
const pkgJson = require('../package.json')
const { bugs } = pkgJson

const { categoryOf } = require('./protocol/errorCategories')

/**
 * @param {*} source an error, or whatever a constructor was given in its place
 * @param {boolean} retriable the `retriable` flag of the error being built
 * @returns {string|undefined} the source's category, when it carries one and the same flag
 */
const keptCategory = (source, retriable) => {
  try {
    return source != null && typeof source.category === 'string' && source.retriable === retriable
      ? source.category
      : undefined
  } catch {
    return undefined
  }
}

/**
 * The category a new error starts with (KIP-1050): the one it keeps from the error it wraps — the
 * constructor's first argument, else its `cause` — when that error has the same `retriable` flag;
 * otherwise the one the flag implies. Keeping a category only when the flag is kept is what makes
 * `retriable === true` hold exactly for the `retriable` and `refreshRetriable` categories.
 * @param {*} e the constructor's first argument: a message, or the error being wrapped
 * @param {Error|undefined} cause the constructor's `cause` option
 * @param {boolean} retriable the new error's `retriable` flag
 * @returns {string}
 */
const initialCategory = (e, cause, retriable) =>
  keptCategory(e, retriable) ??
  (isNativeError(cause) ? keptCategory(cause, retriable) : undefined) ??
  (retriable === true ? 'retriable' : 'applicationRecoverable')

/**
 * Base class of the errors this client raises.
 *
 * `retriable` says whether repeating the operation may succeed. `category` (KIP-1050) says what
 * the caller should do: `retriable` and `refreshRetriable` errors are retried by the client,
 * `abortable` ends the current transaction but keeps the producer, `applicationRecoverable` needs a
 * new producer, and `invalidConfiguration` needs the configuration, the record or an ACL fixed.
 * `retriable === true` exactly when the category is `retriable` or `refreshRetriable`. An error
 * built from another one keeps its `type` and `code`, and keeps its `category` — taken from the
 * first argument or, for a message, from `cause` — when the two errors agree on `retriable`.
 * `category` is own and enumerable, so it appears in `JSON.stringify(error)`.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1050:+Consistent+error+handling+for+Transactions
 */
class KafkaJSError extends Error {
  /**
   * @param {Error|string|object} e the message, or the error this one is built from
   * @param {object} [options]
   * @param {boolean} [options.retriable=true]
   * @param {Error} [options.cause]
   */
  constructor(e, { retriable = true, cause } = {}) {
    super(e, { cause })
    Error.captureStackTrace(this, this.constructor)
    this.message = e.message || e
    this.name = 'KafkaJSError'
    this.retriable = retriable
    this.category = initialCategory(e, cause, retriable)
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

/**
 * An error the broker reported by its Kafka error code. Its `category` is the one the code has on
 * every request; a category that depends on the request (INVALID_RECORD on Produce, say) is
 * stamped later by the producer's classifier, which knows which request failed. An error rebuilt
 * from another protocol error keeps that error's category.
 */
class KafkaJSProtocolError extends KafkaJSError {
  /**
   * @param {{ type: string, code: number, retriable: boolean, message: string, category?: string }} e
   *   a row of the protocol error table, or an error built from one
   * @param {{ retriable?: boolean }} [options] overrides the row's `retriable` flag
   */
  constructor(e, { retriable = e.retriable } = {}) {
    super(e, { retriable })
    this.type = e.type
    this.code = e.code
    this.name = 'KafkaJSProtocolError'

    if (typeof e.code === 'number' && e.category === undefined) {
      this.category = categoryOf(e.code, { retriable: this.retriable })
    }
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
    try {
      while (source && source.type === undefined && source.cause) {
        source = source.cause
      }
      if (source && source.type !== undefined) {
        this.type = source.type
        this.code = source.code
      }
    } catch {}
  }
}

class KafkaJSConnectionError extends KafkaJSError {
  /**
   * @param {string} e
   * @param {object} options
   * @param {string} [options.broker]
   * @param {string} [options.code]
   * @param {*} [options.cause] the original failure, retained by identity
   */
  constructor(e, { broker, code, cause } = {}) {
    super(e, { cause })
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

/**
 * Whether every child of an aggregate is retriable, a missing one (a hole) counting as not.
 * Reads by index and returns at the first child that is not, so a long sparse list is neither
 * copied nor walked past that point.
 * @param {Array<Error|string>} errors a non-empty array
 * @returns {boolean}
 */
const everyChildRetriable = (errors) => {
  for (let index = 0; index < errors.length; index++) {
    if (errors[index]?.retriable !== true) return false
  }
  return true
}

/**
 * The category an aggregate of `errors` has.
 * @param {Array<Error|string>} errors the aggregate's children
 * @param {boolean} retriable whether every child is retriable (and there is at least one)
 * @returns {'refreshRetriable'|'retriable'|'applicationRecoverable'}
 */
const aggregateCategory = (errors, retriable) => {
  if (!retriable) return 'applicationRecoverable'
  return errors.every((child) => child.category === 'refreshRetriable')
    ? 'refreshRetriable'
    : 'retriable'
}

/**
 * Several errors from one request, reported together (one per failed topic or partition). It
 * extends `Error` directly, so it sets `retriable` and `category` itself, both derived from its
 * children: it is retriable only when it has children and every one is, so a caller's own retrier
 * never repeats an aggregate that holds a terminal failure. A string child, a missing child (a
 * hole in a sparse list counts), a missing list and a list that is not an array are not retriable.
 * Constructing never throws, as in 2.2.4, however long the list. `category` is `refreshRetriable`
 * when every child is, else `retriable` when every child is retriable, else
 * `applicationRecoverable`: the caller has to inspect `errors`.
 */
class KafkaJSAggregateError extends Error {
  /**
   * @param {string} message
   * @param {Array<Error|string>} [errors]
   */
  constructor(message, errors) {
    super(message)
    this.errors = errors
    this.name = 'KafkaJSAggregateError'
    this.retriable = Array.isArray(errors) && errors.length > 0 && everyChildRetriable(errors)
    this.category = aggregateCategory(errors, this.retriable)
  }
}

/**
 * Raised inside the fetch loop to make a fetcher stop and start again when the group rebalances.
 * Flow control rather than a failure; it extends `Error` directly and keeps the `name` `'Error'`
 * it has always had, so it only gains the `category` every error carries.
 */
class KafkaJSFetcherRebalanceError extends Error {
  constructor() {
    super(...arguments)
    this.category = 'applicationRecoverable'
  }
}

class KafkaJSNoBrokerAvailableError extends KafkaJSError {
  constructor() {
    super('No broker available')
    this.name = 'KafkaJSNoBrokerAvailableError'
  }
}

/**
 * A transient error the client recovers from by refreshing what it knows — topic metadata, or the
 * coordinator — and retrying. Retriable, like every `KafkaJSError` whose `retriable` is true.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/errors/RefreshRetriableException.java
 */
class KafkaJSRefreshRetriableError extends KafkaJSError {
  /**
   * @param {Error|string} e
   * @param {{ cause?: Error }} [options]
   */
  constructor(e, { cause } = {}) {
    super(e, { retriable: true, cause })
    this.name = 'KafkaJSRefreshRetriableError'
    this.category = 'refreshRetriable'
  }
}

/**
 * The current transaction cannot complete and has to be aborted; the producer stays usable for
 * the next one. `commit()` rejects with it and `abort()` succeeds.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/errors/TransactionAbortableException.java
 */
class KafkaJSTransactionAbortableError extends KafkaJSNonRetriableError {
  /**
   * @param {Error|string} e
   * @param {{ cause?: Error }} [options]
   */
  constructor(e, { cause } = {}) {
    super(e, { cause })
    this.name = 'KafkaJSTransactionAbortableError'
    this.category = 'abortable'
  }
}

/**
 * The producer cannot continue — it was fenced, or its producer id is no longer valid — and has
 * to be replaced by a new instance; every later call on it rejects.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/errors/ApplicationRecoverableException.java
 */
class KafkaJSApplicationRecoverableError extends KafkaJSNonRetriableError {
  /**
   * @param {Error|string} e
   * @param {{ cause?: Error }} [options]
   */
  constructor(e, { cause } = {}) {
    super(e, { cause })
    this.name = 'KafkaJSApplicationRecoverableError'
    this.category = 'applicationRecoverable'
  }
}

/**
 * The configuration, a record or an ACL is wrong: retrying, aborting or re-creating the producer
 * does not help until it is fixed.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1050:+Consistent+error+handling+for+Transactions
 */
class KafkaJSInvalidConfigurationError extends KafkaJSNonRetriableError {
  /**
   * @param {Error|string} e
   * @param {{ cause?: Error }} [options]
   */
  constructor(e, { cause } = {}) {
    super(e, { cause })
    this.name = 'KafkaJSInvalidConfigurationError'
    this.category = 'invalidConfiguration'
  }
}

/**
 * A send did not complete within `deliveryTimeout` ms (KIP-91); `cause` is the last error it
 * met. Abortable: inside a transaction the transaction has to be aborted, outside one the producer
 * stays usable.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer
 */
class KafkaJSDeliveryTimeout extends KafkaJSNonRetriableError {
  /**
   * @param {Error|string} e
   * @param {{ cause?: Error, outcome?: 'unsent'|'unknown' }} [options]
   */
  constructor(e, { cause, outcome } = {}) {
    super(e, { cause })
    this.name = 'KafkaJSDeliveryTimeout'
    this.category = 'abortable'
    if (outcome !== undefined) this.outcome = outcome
    for (const key of ['type', 'code']) {
      try {
        const value = cause?.[key]
        if (value !== undefined) this[key] = value
      } catch {}
    }
  }
}

/**
 * A response frame was negative, shorter than a correlation id, or larger than `maxResponseSize`,
 * or could not be processed: the connection it arrived on is closed and every request on it fails
 * with this error. Retriable, like every connection error: a caller whose retrier retries it sends
 * the request again on a new connection. `reason` says which rule the frame broke, and `size` is
 * the length its prefix declared; `size` is `null` for `reason: 'unreadable'`, a frame whose
 * handling failed with `cause`. The message names `maxResponseSize` for a frame that was too large.
 */
class KafkaJSInvalidFrameError extends KafkaJSConnectionError {
  /**
   * @param {Error|string} e
   * @param {object} options
   * @param {string} [options.broker]
   * @param {number|null} [options.size]
   * @param {'negative'|'too-short'|'too-large'|'unreadable'} options.reason
   * @param {Error} [options.cause]
   */
  constructor(e, { broker, size = null, reason, cause } = {}) {
    super(e, { broker })
    this.name = 'KafkaJSInvalidFrameError'
    this.size = size
    this.reason = reason
    this.cause = cause
  }
}

/**
 * A Fetch record batch is corrupt or breaks a decoding bound: its length, magic byte, record count
 * or control record is impossible, its checksum does not match, its compressed body is malformed,
 * or it decompresses past `maxDecompressedBatchBytes`. Non-retriable; repeating the fetch returns
 * the same bytes. `offset` is the first offset of the batch as a decimal string, when it could be
 * read. `nextOffset`, present only when the batch boundary was validated (a readable, non-negative
 * `lastOffsetDelta` and, with `checkCrcs`, a matching checksum), is where `consumer.seek` goes to
 * skip the batch. `codec` names the compression codec of a compressed batch, and `limit` the bound
 * in force: bytes for `decompressed-size` and `decompression`, records for a `count` over the
 * derived record ceiling.
 */
class KafkaJSCorruptRecordError extends KafkaJSNonRetriableError {
  /**
   * @param {Error|string} e
   * @param {object} options
   * @param {'length'|'magic'|'count'|'control'|'checksum'|'decompressed-size'|'decompression'} options.reason
   * @param {string} [options.offset]
   * @param {string} [options.nextOffset]
   * @param {'gzip'|'snappy'|'lz4'|'zstd'} [options.codec]
   * @param {number} [options.limit]
   * @param {Error} [options.cause]
   */
  constructor(e, { reason, offset, nextOffset, codec, limit, cause } = {}) {
    super(e, { cause })
    this.name = 'KafkaJSCorruptRecordError'
    this.reason = reason
    this.offset = offset

    if (nextOffset !== undefined) this.nextOffset = nextOffset
    if (codec !== undefined) this.codec = codec
    if (limit !== undefined) this.limit = limit
  }
}

/**
 * An offset commit was refused because the consumer lost its group membership, or because a
 * handler or a retry outlived a rejoin. `cause` is the loss. For a loss the broker reported,
 * `type`, `code` and `helpUrl` are the cause's; a synthetic loss carries no code, a poll-interval
 * loss keeps the type `POLL_INTERVAL_EXCEEDED`, and a stale handler has the type
 * `STALE_ASSIGNMENT` and no `code` or `helpUrl`, whatever its cause carries. `isLostMember` checks
 * only the broker codes; it does not recognize every reason of this class: test `instanceof` and
 * `reason`.
 */
class KafkaJSCommitFailedError extends KafkaJSNonRetriableError {
  /**
   * @param {Error|string} e
   * @param {object} options
   * @param {'membership-lost'|'poll-interval-exceeded'|'stale-assignment'} options.reason
   * @param {Error} options.cause
   */
  constructor(e, { reason, cause } = {}) {
    super(e, { cause })
    this.name = 'KafkaJSCommitFailedError'
    this.reason = reason
    this.category = 'applicationRecoverable'

    if (reason === 'stale-assignment') {
      this.type = 'STALE_ASSIGNMENT'
      this.code = undefined
      this.helpUrl = undefined
    } else {
      for (const key of ['type', 'code', 'helpUrl']) {
        if (this[key] === undefined && cause != null && cause[key] !== undefined) {
          this[key] = cause[key]
        }
      }
    }
  }
}

/**
 * A send could not be admitted to the producer's buffer (`bufferMemory`): `reason` is `'timeout'`
 * when no room came within `maxBlockMs`, and `'queue-full'` when the sends already waiting for room
 * hold so many bytes between them that this one would take them past `bufferMemory`. Nothing of the
 * send was queued. Retriable: back off and send again. Inside a transaction the refusal leaves the
 * transaction able only to abort. `requested` is the send's charge, `available` and `capacity` the
 * buffer's free bytes and size when it was refused, and `waitedMs` how long it waited.
 */
class KafkaJSBufferExhaustedError extends KafkaJSError {
  /**
   * @param {Error|string} e
   * @param {object} options
   * @param {'timeout'|'queue-full'} options.reason
   * @param {number} options.requested
   * @param {number} options.available
   * @param {number} options.capacity
   * @param {number} options.waitedMs
   * @param {Error} [options.cause]
   */
  constructor(e, { reason, requested, available, capacity, waitedMs, cause } = {}) {
    super(e, { cause })
    this.name = 'KafkaJSBufferExhaustedError'
    this.reason = reason
    this.requested = requested
    this.available = available
    this.capacity = capacity
    this.waitedMs = waitedMs
  }
}

/**
 * A record, or a whole send, can never fit: with `reason: 'bufferMemory'` its charge is above
 * `bufferMemory`; with `reason: 'maxRequestSize'` no Produce request under `maxRequestSize` can
 * carry it. Raised by the client: at admission, before anything of the send is queued; or when a
 * request is refused at its write, for that write only - and for a batch that was already
 * transmitted, which keeps its records together and is not sent again, it does not establish that
 * the earlier attempts were unsent (it carries no `outcome`). Non-retriable. It carries the `type`
 * and `code` of the broker's own MESSAGE_TOO_LARGE (10), so a check on either catches both, and
 * that error's category on a Produce request, `invalidConfiguration`: inside a transaction the
 * refusal leaves the transaction able only to abort. `scope` says whether one record (`topic`
 * names its topic) or more is too large; `size` is what was found above `limit`: a charge, a
 * record's wire size against the bytes of records one request carries, or a request's frame.
 */
class KafkaJSRecordTooLargeError extends KafkaJSNonRetriableError {
  /**
   * @param {Error|string} e
   * @param {object} options
   * @param {'bufferMemory'|'maxRequestSize'} options.reason
   * @param {'record'|'send'} options.scope
   * @param {number} options.size
   * @param {number} options.limit
   * @param {string} [options.topic]
   * @param {Error} [options.cause]
   */
  constructor(e, { reason, scope, size, limit, topic, cause } = {}) {
    super(e, { cause })
    this.name = 'KafkaJSRecordTooLargeError'
    this.type = 'MESSAGE_TOO_LARGE'
    this.code = 10
    this.category = 'invalidConfiguration'
    this.reason = reason
    this.scope = scope
    this.size = size
    this.limit = limit
    if (topic !== undefined) this.topic = topic
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
const FENCED_MEMBER_EPOCH_CODE = 110

/**
 * The member is no longer part of the group: its generation, member id, static instance id or
 * member epoch has been invalidated by the coordinator. Uncommitted offsets belong to a generation
 * the coordinator has already discarded, so committing them fails forever (upstream #1712, #1466,
 * #1527, #1009). Discard the uncommitted offsets and rejoin instead.
 *
 * 110 is raised only for a group the coordinator runs under the consumer protocol, so recognising
 * it here cannot change what a classic member does with any answer it can actually receive.
 */
const isLostMember = (e) =>
  e.code === ILLEGAL_GENERATION_CODE ||
  e.code === UNKNOWN_MEMBER_ID_CODE ||
  e.code === FENCED_INSTANCE_ID_CODE ||
  e.code === FENCED_MEMBER_EPOCH_CODE

/** True for a member fenced out of the group by a duplicate static instance id (code 82). */
const isFencedInstance = (e) => e.code === FENCED_INSTANCE_ID_CODE

/** True for a member the coordinator no longer recognizes by member id (code 25). */
const isUnknownMemberId = (e) => e.code === UNKNOWN_MEMBER_ID_CODE

/**
 * The internal type of the error raised when the cluster cannot speak the new group protocol -
 * either it answered UNSUPPORTED_VERSION or it offers no version of the heartbeat API at all.
 * Exported so the predicate below and whatever builds the error agree on one string.
 */
const CONSUMER_PROTOCOL_UNSUPPORTED = 'CONSUMER_PROTOCOL_UNSUPPORTED'

const FATAL_MEMBERSHIP_CODES = new Set([
  81, // GROUP_MAX_SIZE_REACHED
  111, // UNRELEASED_INSTANCE_ID
  112, // UNSUPPORTED_ASSIGNOR
  128, // INVALID_REGULAR_EXPRESSION
])

/**
 * The membership can never be recovered by rejoining: the group is full, the instance id belongs to
 * someone else, the assignor or the pattern is one the cluster will not accept, or the cluster does
 * not speak the protocol at all. Restarting into any of them repeats the same failure forever, so
 * this overrides `restartOnFailure`.
 *
 * UNSUPPORTED_VERSION is deliberately matched by the internal type rather than by code 35: every
 * API can raise 35, and only the membership's own error means "this cluster cannot do this at all".
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java
 * @param {Error} e
 * @returns {boolean}
 */
const isFatalMembership = (e) =>
  FATAL_MEMBERSHIP_CODES.has(e.code) || e.type === CONSUMER_PROTOCOL_UNSUPPORTED

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
  KafkaJSRefreshRetriableError,
  KafkaJSTransactionAbortableError,
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidFrameError,
  KafkaJSCorruptRecordError,
  KafkaJSCommitFailedError,
  KafkaJSBufferExhaustedError,
  KafkaJSRecordTooLargeError,
  isRebalancing,
  isStaleCoordinator,
  COORDINATOR_LOAD_IN_PROGRESS_CODE,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
  isPollIntervalExceeded,
  isFatalMembership,
  CONSUMER_PROTOCOL_UNSUPPORTED,
  isKafkaJSError,
}
