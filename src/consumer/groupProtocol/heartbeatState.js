/**
 * What the coordinator already knows, and what one heartbeat therefore has to carry.
 *
 * The protocol sends nine optional fields on a diff: full data on the joining heartbeat, and after
 * that only what changed. `null` on the wire means "unchanged since the last heartbeat", which is
 * why a field that did not change is omitted rather than nulled, and why a member with no topic
 * names still has to send an EMPTY LIST to say so.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerHeartbeatRequestManager.java
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/resources/common/message/ConsumerGroupHeartbeatRequest.json
 */

/**
 * Carries the local assignment a request was built from. Symbol-keyed because the local epoch is
 * part of the comparison but never part of the wire form, and because a symbol reaches neither the
 * encoder nor a logger nor `JSON.stringify`.
 */
const LOCAL_ASSIGNMENT = Symbol('heartbeat:localAssignment')

/**
 * Two topic-name lists, compared the way the coordinator compares them: as sets, order-insensitive.
 * @param {string[]|null} a
 * @param {string[]|null} b
 * @returns {boolean}
 */
const equalsAsSortedSet = (a, b) => {
  if (a == null || b == null) {
    return a === b
  }

  if (a.length !== b.length) {
    return false
  }

  const left = [...a].sort()
  const right = [...b].sort()
  return left.every((value, index) => value === right[index])
}

/**
 * Two local assignments, compared by local epoch AND by partitions. The epoch is part of the key on
 * purpose: re-acking the same partition set under a bumped epoch is how a member reports progress
 * on an assignment it could only resolve in part, so that heartbeat must still carry
 * `TopicPartitions`.
 * @param {{ localEpoch: number, byTopicId: Map<string, number[]> }|null} a
 * @param {{ localEpoch: number, byTopicId: Map<string, number[]> }|null} b
 * @returns {boolean}
 */
const localAssignmentEquals = (a, b) => {
  if (a == null || b == null) {
    return a === b
  }

  if (a.localEpoch !== b.localEpoch || a.byTopicId.size !== b.byTopicId.size) {
    return false
  }

  for (const [topicId, partitions] of a.byTopicId) {
    const other = b.byTopicId.get(topicId)

    if (!other || other.length !== partitions.length) {
      return false
    }

    if (!partitions.every((partition, index) => partition === other[index])) {
      return false
    }
  }

  return true
}

/**
 * Copies a local assignment, so what is remembered cannot be changed from underneath by whoever
 * owns the live one. Aliasing here would make the next comparison compare an object with itself,
 * and `TopicPartitions` would silently never be re-sent.
 * @param {{ localEpoch: number, byTopicId: Map<string, number[]> }} assignment
 * @returns {{ localEpoch: number, byTopicId: Map<string, number[]> }} a deep copy
 */
const snapshotLocalAssignment = ({ localEpoch, byTopicId }) => ({
  localEpoch,
  byTopicId: new Map([...byTopicId].map(([topicId, partitions]) => [topicId, [...partitions]])),
})

/**
 * The subset of the last request this member sent, as far as the diff cares about it.
 *
 * Reset on every failed heartbeat - a transport failure and an error response alike - and on a poll
 * expiry, so the next attempt re-states everything the coordinator may have lost.
 */
class SentFields {
  constructor() {
    this.reset()
  }

  /**
   * Forgets everything except the identity fields, which are never diffed.
   * @returns {void}
   */
  reset() {
    /** @type {string[]|null} */
    this.subscribedTopicNames = null
    /** −1 is the wire's "unknown", and is never a legal rebalance timeout. */
    this.rebalanceTimeoutMs = -1
    /** @type {string|null} */
    this.serverAssignor = null
    /** @type {string|null} */
    this.subscribedTopicRegex = null
    /** @type {{ localEpoch: number, byTopicId: Map<string, number[]> }|null} */
    this.localAssignment = null
  }

  /**
   * Records a request that actually went out. Only the fields the request carried are updated: a
   * field the diff decided to omit is still known by the coordinator, so forgetting it here would
   * re-send it forever.
   * @param {Object} request the object `buildRequest` returned
   * @returns {void}
   */
  remember(request) {
    if (request.SubscribedTopicNames !== undefined) {
      this.subscribedTopicNames = [...request.SubscribedTopicNames]
    }

    if (request.RebalanceTimeoutMs !== undefined) {
      this.rebalanceTimeoutMs = request.RebalanceTimeoutMs
    }

    if (request.ServerAssignor !== undefined) {
      this.serverAssignor = request.ServerAssignor
    }

    if (request.SubscribedTopicRegex !== undefined) {
      this.subscribedTopicRegex = request.SubscribedTopicRegex
    }

    if (request[LOCAL_ASSIGNMENT] !== undefined) {
      this.localAssignment = request[LOCAL_ASSIGNMENT]
    }
  }
}

/**
 * Builds one ConsumerGroupHeartbeat request from the membership's current state.
 *
 * Pure: it reads the protocol and its `sentFields` and writes neither. The caller sends the result
 * and then hands it to `sentFields.remember`, so a request that never left is never remembered.
 * The caller also owns the pattern latch: after a request carrying `SubscribedTopicRegex` went out,
 * it clears `regexUpdated`. Clearing it here would make this function impure and would clear the
 * latch for a request that failed to leave.
 *
 * `protocol.localAssignment` must always be an object - `{ localEpoch: 0, byTopicId: new Map() }`
 * for a member that owns nothing - because the joining heartbeat always reads it.
 *
 * @param {Object} protocol the membership, read for `groupId`, `memberId`, `memberEpoch`,
 *   `groupInstanceId`, `rackId`, `rebalanceTimeoutMs`, `subscribedTopicNames`,
 *   `subscribedTopicRegex`, `regexUpdated`, `serverAssignor`, `localAssignment` and `sentFields`
 * @param {{ sendAllFields?: boolean, leaveEpoch?: number|null }} [options] `sendAllFields` is true
 *   exactly while the member is joining; `leaveEpoch` replaces the member epoch with −1 (dynamic)
 *   or −2 (static) for a leave
 * @returns {Object} the request, in wire field names, carrying only the fields that must be sent
 */
const buildRequest = (protocol, { sendAllFields = false, leaveEpoch = null } = {}) => {
  const request = {
    GroupId: protocol.groupId,
    MemberId: protocol.memberId,
    MemberEpoch: leaveEpoch != null ? leaveEpoch : protocol.memberEpoch,
  }

  if (protocol.groupInstanceId != null) {
    request.InstanceId = protocol.groupInstanceId
  }

  if (request.MemberEpoch === -1 || request.MemberEpoch === -2) {
    return request
  }

  const { sentFields } = protocol

  if (sendAllFields && protocol.rackId != null) {
    request.RackId = protocol.rackId
  }

  if (sendAllFields || sentFields.rebalanceTimeoutMs !== protocol.rebalanceTimeoutMs) {
    request.RebalanceTimeoutMs = protocol.rebalanceTimeoutMs
  }

  if (
    sendAllFields ||
    !equalsAsSortedSet(sentFields.subscribedTopicNames, protocol.subscribedTopicNames)
  ) {
    request.SubscribedTopicNames = [...protocol.subscribedTopicNames]
  }

  const regex = protocol.subscribedTopicRegex

  if (
    protocol.regexUpdated ||
    (regex != null && (sendAllFields || sentFields.subscribedTopicRegex !== regex))
  ) {
    // The empty string is how a previously sent pattern is CLEARED; null would mean "unchanged".
    // The second clause makes the remembered value the comparison basis, so a reset re-states the
    // pattern like every other row; the `regex != null` guard is what keeps a member that never
    // had one from announcing an empty pattern it never set.
    request.SubscribedTopicRegex = regex == null ? '' : regex
  }

  if (
    protocol.serverAssignor != null &&
    (sendAllFields || sentFields.serverAssignor !== protocol.serverAssignor)
  ) {
    request.ServerAssignor = protocol.serverAssignor
  }

  if (
    sendAllFields ||
    !localAssignmentEquals(sentFields.localAssignment, protocol.localAssignment)
  ) {
    request.TopicPartitions = [...protocol.localAssignment.byTopicId].map(
      ([TopicId, Partitions]) => ({ TopicId, Partitions: [...Partitions] })
    )
    request[LOCAL_ASSIGNMENT] = snapshotLocalAssignment(protocol.localAssignment)
  }

  return request
}

module.exports = {
  SentFields,
  buildRequest,
  equalsAsSortedSet,
  localAssignmentEquals,
  LOCAL_ASSIGNMENT,
}
