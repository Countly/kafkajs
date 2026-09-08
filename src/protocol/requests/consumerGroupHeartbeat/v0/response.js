const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')
const { KafkaJSProtocolError, KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * The one-byte presence markers Kafka's generator writes for a nullable, non-tagged struct field:
 * INT8 -1 when the field is null, INT8 1 followed by the struct's body when it is present. No
 * other value is legal, so anything else means the frame is not the message it claims to be.
 * @see https://github.com/apache/kafka/blob/4.3/generator/src/main/java/org/apache/kafka/message/MessageDataGenerator.java
 */
const NULL_STRUCT = -1
const PRESENT_STRUCT = 1

/**
 * @param {Decoder} decoder
 * @returns {{ topicId: string|null, partitions: number[] }}
 */
const decodeTopicPartitions = (decoder) => {
  const entry = {
    topicId: decoder.readUuid(),
    partitions: decoder.readUVarIntArray((partition) => partition.readInt32()),
  }

  decoder.readTaggedFields()
  return entry
}

/**
 * @param {Decoder} decoder
 * @returns {{ topicPartitions: object[] }|null} null when the broker sent the null marker; an
 *   assignment with no partitions is a present, empty target and is NOT null
 * @throws {KafkaJSPartialMessageError} on any marker byte other than 1 or -1 - reading on would
 *   decode the rest of the response from the wrong offset and report plausible nonsense
 */
const decodeAssignment = (decoder) => {
  const marker = decoder.readInt8()

  if (marker === NULL_STRUCT) {
    return null
  }

  if (marker !== PRESENT_STRUCT) {
    throw new KafkaJSPartialMessageError(
      `Invalid ConsumerGroupHeartbeatResponse Assignment presence marker ${marker}, expected 1 or -1`
    )
  }

  const assignment = { topicPartitions: decoder.readUVarIntArray(decodeTopicPartitions) }
  decoder.readTaggedFields()
  return assignment
}

/**
 * ConsumerGroupHeartbeatResponse (Version: 0) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * ErrorMessage => COMPACT_NULLABLE_STRING
 * MemberId => COMPACT_NULLABLE_STRING
 * MemberEpoch => INT32
 * HeartbeatIntervalMs => INT32
 * Assignment => nullable struct: INT8 -1 when absent, INT8 1 then the struct when present
 *   TopicPartitions => COMPACT_ARRAY
 *     TopicId => UUID
 *     Partitions => COMPACT_ARRAY of INT32
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The one-byte presence marker is how Kafka encodes a nullable, non-tagged struct field: the
 * generator emits `writeByte(-1)` for null and `writeByte(1)` plus the body otherwise.
 * @see https://github.com/apache/kafka/blob/4.3/generator/src/main/java/org/apache/kafka/message/MessageDataGenerator.java
 * @see clients/src/main/resources/common/message/ConsumerGroupHeartbeatResponse.json (Apache Kafka 4.3.1)
 *
 * A null assignment and an empty one are different answers: null is "nothing new to reconcile
 * toward", an empty assignment is a target that happens to hold no partitions.
 *
 * @param {Buffer} rawData
 * @returns {Promise<object>} the decoded response; `throttleTime` is pinned at 0 and the broker's
 *   value is reported as `clientSideThrottleTime` (KIP-219), as every quota-aware codec here does
 * @throws {KafkaJSProtocolError} on UNSUPPORTED_VERSION, so a version mismatch is reported by the
 *   decode rather than travelling on as data
 * @throws {KafkaJSPartialMessageError} on an illegal `Assignment` presence marker
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const response = {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    memberId: decoder.readUVarIntString(),
    memberEpoch: decoder.readInt32(),
    heartbeatIntervalMs: decoder.readInt32(),
    assignment: decodeAssignment(decoder),
  }

  decoder.readTaggedFields()
  failIfVersionNotSupported(response.errorCode)
  return response
}

/**
 * @param {object} data decoded response
 * @returns {Promise<object>} the response, unchanged, when the broker reported no error
 * @throws {KafkaJSProtocolError} carrying the broker's own `errorMessage` when it sent one - the
 *   membership error table keys on the code, but the message is what a user reads for an invalid
 *   regular expression or an unsupported assignor
 */
const parse = async (data) => {
  if (failure(data.errorCode)) {
    const error = createErrorFromCode(data.errorCode)

    if (data.errorMessage) {
      throw new KafkaJSProtocolError(
        { ...error, message: `${error.message}: ${data.errorMessage}` },
        { retriable: error.retriable }
      )
    }

    throw error
  }

  return data
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ConsumerGroupHeartbeat',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'MemberId', type: 'string', versions: '0+', nullable: true },
    { name: 'MemberEpoch', type: 'int32', versions: '0+' },
    { name: 'HeartbeatIntervalMs', type: 'int32', versions: '0+' },
    { name: 'Assignment', type: 'Assignment', versions: '0+', nullable: true },
    { name: 'Assignment.TopicPartitions', type: '[]TopicPartitions', versions: '0+' },
    { name: 'Assignment.TopicPartitions.TopicId', type: 'uuid', versions: '0+' },
    { name: 'Assignment.TopicPartitions.Partitions', type: '[]int32', versions: '0+' },
  ],
}
