const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/consumerGroupHeartbeat/fixtures/java/consumerGroupHeartbeatResponse-v1-assignment.json')

const encodeTopicPartitions = ({ topicId, partitions }) =>
  new Encoder().writeUuid(topicId).writeUVarIntArray(partitions, 'int32').writeTaggedFields()

/** An assignment: a nullable struct holding the compact array of its topic partitions. */
const encodeAssignment = (assignment) =>
  assignment === null
    ? new Encoder().writeInt8(-1)
    : new Encoder()
        .writeInt8(1)
        .writeUVarIntArray(assignment.topicPartitions.map(encodeTopicPartitions))
        .writeTaggedFields()

const readTopicPartitions = (decoder) => {
  const entry = {
    topicId: decoder.readUuid(),
    partitions: decoder.readUVarIntArray((partition) => partition.readInt32()),
  }
  decoder.readTaggedFields()
  return entry
}

/**
 * ConsumerGroupHeartbeat v1 (flexible), the KIP-848 heartbeat that is also the join, the leave and
 * the acknowledgement of an assignment. The request names the group and member, the epoch the
 * member holds (0 to join, -1 or -2 to leave), its subscription (topic names, or a regular
 * expression from v1) and the partitions it owns; the response gives the member id, the epoch and
 * heartbeat interval the coordinator set and, when it changed, the assignment (by topic id).
 *
 * Evidence: Java's structured vectors `consumerGroupHeartbeatRequest-v1-*.json` and
 * `consumerGroupHeartbeatResponse-v1-*.json` (`hex` and `fields`); the unvaried response is the
 * `assignment` vector.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ConsumerGroupHeartbeatResponse.json
 */
module.exports = {
  api: 'ConsumerGroupHeartbeat',
  apiKey: apiKeys.ConsumerGroupHeartbeat,
  version: 1,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `errorCode`, `errorMessage`,
   *   `memberId`, `memberEpoch`, `heartbeatIntervalMs` and `assignment` (`{ topicPartitions: [{
   *   topicId, partitions }] }`, or null when it did not change)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeUVarIntString(response.errorMessage)
      .writeUVarIntString(response.memberId)
      .writeInt32(response.memberEpoch)
      .writeInt32(response.heartbeatIntervalMs)
      .writeEncoder(encodeAssignment(response.assignment))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ groupId: string, memberId: string, memberEpoch: number, instanceId: string|null,
   *   rackId: string|null, rebalanceTimeoutMs: number, subscribedTopicNames: string[]|null,
   *   subscribedTopicRegex: string|null, serverAssignor: string|null, topicPartitions:
   *   Array<{ topicId: string, partitions: number[] }>|null }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      groupId: decoder.readUVarIntString(),
      memberId: decoder.readUVarIntString(),
      memberEpoch: decoder.readInt32(),
      instanceId: decoder.readUVarIntString(),
      rackId: decoder.readUVarIntString(),
      rebalanceTimeoutMs: decoder.readInt32(),
      subscribedTopicNames: decoder.readUVarIntArray((name) => name.readUVarIntString()),
      subscribedTopicRegex: decoder.readUVarIntString(),
      serverAssignor: decoder.readUVarIntString(),
      topicPartitions: decoder.readUVarIntArray(readTopicPartitions),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('ConsumerGroupHeartbeat v1 request has a byte after its tag buffer')
    }

    return request
  },
}
