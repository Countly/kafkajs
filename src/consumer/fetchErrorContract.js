/**
 * How the consumer reacts to the error codes a Fetch or OffsetForLeaderEpoch response carries.
 *
 * Three lookups, all keyed by **numeric** code and built once at module load. Keying by error
 * *type* would mean constructing an error object (which builds a stack) or scanning the error
 * table for every failing partition of every fetch round, on the consume hot path; the type names
 * live here as comments instead.
 *
 * Nothing in this module performs I/O or touches consumer state, which is what lets the codes
 * that cannot occur against a healthy cluster be covered directly rather than by simulating one.
 *
 * @see https://kafka.apache.org/43/design/protocol
 */

/**
 * @typedef {'offset-out-of-range'|'stale-metadata'|'skip'|'skip-and-forget-epoch'|'throw'} PartitionFetchAction
 */

/** @type {Map<number, PartitionFetchAction>} */
const PARTITION_ACTIONS = new Map([
  [1, 'offset-out-of-range'], // OFFSET_OUT_OF_RANGE
  [3, 'stale-metadata'], // UNKNOWN_TOPIC_OR_PARTITION
  [5, 'stale-metadata'], // LEADER_NOT_AVAILABLE
  [6, 'stale-metadata'], // NOT_LEADER_FOR_PARTITION (NOT_LEADER_OR_FOLLOWER)
  [9, 'stale-metadata'], // REPLICA_NOT_AVAILABLE
  [56, 'stale-metadata'], // KAFKA_STORAGE_ERROR
  [74, 'stale-metadata'], // FENCED_LEADER_EPOCH - the client is behind
  [75, 'skip-and-forget-epoch'], // UNKNOWN_LEADER_EPOCH - the client is ahead
  [78, 'skip'], // OFFSET_NOT_AVAILABLE - not yet committed on this replica (KIP-392)
  [100, 'stale-metadata'], // UNKNOWN_TOPIC_ID
  [103, 'stale-metadata'], // INCONSISTENT_TOPIC_ID
  [106, 'stale-metadata'], // FETCH_SESSION_TOPIC_ID_ERROR - defensive; see TOP_LEVEL_ACTIONS
  [109, 'throw'], // OFFSET_MOVED_TO_TIERED_STORAGE - non-retriable, no follower path here
])

/**
 * Only the three fetch-session codes can appear at the top level in practice, and none of them can
 * appear at all while every fetch is sessionless. They are mapped anyway so that a code which was
 * invisible before this version - the top-level code was decoded and never read - cannot become a
 * consumer crash now that it is read.
 * @type {Map<number, 'stale-metadata'>}
 */
const TOP_LEVEL_ACTIONS = new Map([
  [70, 'stale-metadata'], // FETCH_SESSION_ID_NOT_FOUND
  [71, 'stale-metadata'], // INVALID_FETCH_SESSION_EPOCH
  [106, 'stale-metadata'], // FETCH_SESSION_TOPIC_ID_ERROR
])

/**
 * Retriable per-partition codes on an OffsetForLeaderEpoch response. Anything else is fatal for
 * the assignment - `TOPIC_AUTHORIZATION_FAILED` (29) is the one to expect, since v3 is the version
 * that relaxes the ACL to TOPIC Describe.
 * @type {Set<number>}
 */
const RETRIABLE_VALIDATION_CODES = new Set([
  3, // UNKNOWN_TOPIC_OR_PARTITION
  5, // LEADER_NOT_AVAILABLE
  6, // NOT_LEADER_FOR_PARTITION
  9, // REPLICA_NOT_AVAILABLE
  56, // KAFKA_STORAGE_ERROR
  74, // FENCED_LEADER_EPOCH
  75, // UNKNOWN_LEADER_EPOCH
  78, // OFFSET_NOT_AVAILABLE
])

/**
 * Classifies a failing partition-level fetch error code. Only meaningful for a non-zero code:
 * a successful partition (code 0) is never classified, it is delivered.
 * @param {number} errorCode
 * @returns {PartitionFetchAction} `'throw'` for every code not listed, which preserves the
 *   previous behaviour: the error is thrown from the fetch and handled by the existing recovery.
 */
const partitionFetchAction = (errorCode) => PARTITION_ACTIONS.get(errorCode) || 'throw'

/**
 * Classifies a failing top-level fetch error code. Only meaningful for a non-zero code: a
 * response whose top-level code is 0 is walked partition by partition instead.
 * @param {number} errorCode
 * @returns {'stale-metadata'|'throw'}
 */
const topLevelFetchAction = (errorCode) => TOP_LEVEL_ACTIONS.get(errorCode) || 'throw'

/**
 * @param {number} errorCode
 * @returns {'ok'|'retry'|'fatal'} `'fatal'` means a non-retriable per-partition code. It is
 *   distinct from "the broker does not advertise this API at all", which is decided from the
 *   broker's advertised versions before any request and never reaches this function.
 */
const validationPartitionAction = (errorCode) => {
  if (errorCode === 0) {
    return 'ok'
  }

  return RETRIABLE_VALIDATION_CODES.has(errorCode) ? 'retry' : 'fatal'
}

/**
 * Why a fetched partition is being dropped instead of delivered, or `null` when it is not.
 *
 * The three checks are evaluated in this order, which is the precedence the drop log reports: a
 * queued seek is the newest user intent, a pause is standing user intent, and a partition awaiting
 * position validation should not have been in the request at all (that check is defensive).
 *
 * Positional rather than named, because it is called once per fetched partition per round and a
 * named-argument object would be one allocation per call on the consume hot path.
 *
 * @param {boolean} seeked a `seek()` is queued for this partition
 * @param {boolean} paused the partition is paused
 * @param {boolean} awaitingValidation the partition's position is being validated
 * @returns {'seeked'|'paused'|'awaiting-validation'|null}
 */
const fetchedPartitionDropReason = (seeked, paused, awaitingValidation) => {
  if (seeked) return 'seeked'
  if (paused) return 'paused'
  if (awaitingValidation) return 'awaiting-validation'
  return null
}

module.exports = {
  partitionFetchAction,
  topLevelFetchAction,
  validationPartitionAction,
  fetchedPartitionDropReason,
}
