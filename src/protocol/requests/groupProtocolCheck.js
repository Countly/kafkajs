const { createErrorFromCode } = require('../error')

const INCONSISTENT_GROUP_PROTOCOL = 23

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

module.exports = { assertSameProtocol }
