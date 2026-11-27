const { createErrorFromCode } = require('../error')
const { KafkaJSError } = require('../../errors')
const Encoder = require('../encoder')

const INCONSISTENT_GROUP_PROTOCOL = 23
const MALFORMED_GROUP_ANSWER = 'MALFORMED_GROUP_ANSWER'

/**
 * Compares a protocol type or name the coordinator reported with the one this client sent
 * (JoinGroup v7 and later, SyncGroup v5). A null on either side is not a mismatch: a coordinator
 * that does not report a value, or a caller that sent none, leaves nothing to compare.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java
 * @param {string|null|undefined} received what the coordinator reported
 * @param {string|null|undefined} sent what this client sent
 * @returns {void}
 * @throws {import('../../errors').KafkaJSProtocolError} INCONSISTENT_GROUP_PROTOCOL (23), which is
 *   not retriable, when both are set and differ
 */
const assertSameProtocol = (received, sent) => {
  if (received != null && sent != null && received !== sent) {
    throw createErrorFromCode(INCONSISTENT_GROUP_PROTOCOL)
  }
}

/**
 * The refusal of a JoinGroup answer no coordinator sends, or of a SyncGroup assignment this member
 * cannot use (the coordinator relays the leader's bytes unread). Retriable, with the type
 * `MALFORMED_GROUP_ANSWER`, so the consumer's join retrier joins again instead of using it.
 * @param {string} message names the API and what is wrong
 * @returns {KafkaJSError}
 */
const malformedGroupAnswer = (message) => {
  const error = new KafkaJSError(message, { retriable: true })
  error.type = MALFORMED_GROUP_ANSWER
  return error
}

/**
 * @param {Error} error
 * @returns {boolean} whether `error` is a refusal from `malformedGroupAnswer`
 */
const isMalformedGroupAnswer = (error) => error?.type === MALFORMED_GROUP_ANSWER

/**
 * Refuses a successful JoinGroup answer no Kafka 4.x coordinator sends: it always names a member
 * id, a generation of 0 or more and one of the protocols the member offered, as the request wrote
 * its name, and gives the leader every member, itself included, once each. Followers get no member
 * list, so theirs is not read.
 * @see https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java
 * @param {{ memberId: string|null, generationId: number, groupProtocol: string|null,
 *   leaderId: string|null, members: { memberId: string|null }[] }} answer the decoded answer
 * @param {{ name: string }[]} offered the protocols the request offered
 * @returns {void}
 * @throws {KafkaJSError} retriable, from `malformedGroupAnswer`
 */
const assertJoinAnswer = (
  { memberId, generationId, groupProtocol, leaderId, members },
  offered
) => {
  if (typeof memberId !== 'string' || memberId === '') {
    throw malformedGroupAnswer('JoinGroup response has no member id')
  }
  if (!Number.isInteger(generationId) || generationId < 0) {
    throw malformedGroupAnswer(`JoinGroup response names generation ${generationId}`)
  }
  if (!offered.some(({ name }) => Encoder.isEchoOf(groupProtocol, name))) {
    throw malformedGroupAnswer(
      `JoinGroup response names protocol ${groupProtocol}, which this member did not offer`
    )
  }
  if (leaderId !== memberId) return

  const ids = new Set()
  for (const { memberId: id } of members) {
    if (typeof id !== 'string' || id === '') {
      throw malformedGroupAnswer('JoinGroup response to the leader lists a member with no id')
    }
    if (ids.has(id)) {
      throw malformedGroupAnswer(`JoinGroup response to the leader lists member ${id} twice`)
    }
    ids.add(id)
  }
  if (!ids.has(leaderId)) {
    throw malformedGroupAnswer(
      `JoinGroup response to the leader does not list the leader ${leaderId}`
    )
  }
}

module.exports = {
  assertSameProtocol,
  assertJoinAnswer,
  malformedGroupAnswer,
  isMalformedGroupAnswer,
}
