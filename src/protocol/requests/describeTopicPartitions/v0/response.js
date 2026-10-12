const Decoder = require('../../../decoder')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * The one-byte presence markers Kafka's generator writes for a nullable, non-tagged struct field.
 * Any other value means the frame is not the message it claims to be, and reading on would decode
 * the rest from the wrong offset.
 * @see https://github.com/apache/kafka/blob/4.3/generator/src/main/java/org/apache/kafka/message/MessageDataGenerator.java
 */
const NULL_STRUCT = -1
const PRESENT_STRUCT = 1

/** @param {Decoder} decoder @returns {number} */
const decodeInt32 = (decoder) => decoder.readInt32()

/**
 * @param {Decoder} decoder
 * @returns {{ errorCode: number, partitionIndex: number, leaderId: number, leaderEpoch: number,
 *   replicaNodes: number[], isrNodes: number[], eligibleLeaderReplicas: number[]|null,
 *   lastKnownElr: number[]|null, offlineReplicas: number[] }} one partition. The two lists of
 *   eligible leader replicas keep their three states: `null` (the cluster does not track them),
 *   `[]` (it does, and none are eligible) and the ids.
 */
const decodePartition = (decoder) => {
  const partition = {
    errorCode: decoder.readInt16(),
    partitionIndex: decoder.readInt32(),
    leaderId: decoder.readInt32(),
    leaderEpoch: decoder.readInt32(),
    replicaNodes: decoder.readUVarIntArray(decodeInt32) || [],
    isrNodes: decoder.readUVarIntArray(decodeInt32) || [],
    eligibleLeaderReplicas: decoder.readUVarIntArray(decodeInt32),
    lastKnownElr: decoder.readUVarIntArray(decodeInt32),
    offlineReplicas: decoder.readUVarIntArray(decodeInt32) || [],
  }

  decoder.readTaggedFields()
  return partition
}

/**
 * @param {Decoder} decoder
 * @returns {{ errorCode: number, name: string|null, topicId: string|null, isInternal: boolean,
 *   partitions: object[], topicAuthorizedOperations: number }} one topic; `topicId` is `null` for
 *   the zero uuid an unauthorized or unknown topic carries
 */
const decodeTopic = (decoder) => {
  const topic = {
    errorCode: decoder.readInt16(),
    name: decoder.readUVarIntString(),
    topicId: decoder.readUuid(),
    isInternal: decoder.readBoolean(),
    partitions: decoder.readUVarIntArray(decodePartition) || [],
    topicAuthorizedOperations: decoder.readInt32(),
  }

  decoder.readTaggedFields()
  return topic
}

/**
 * @param {Decoder} decoder
 * @returns {{ topicName: string, partitionIndex: number }|null} `null` when the broker sent the
 *   marker -1: there is nothing more to read
 * @throws {KafkaJSPartialMessageError} on a marker other than -1 and 1
 */
const decodeNextCursor = (decoder) => {
  const marker = decoder.readInt8()

  if (marker === NULL_STRUCT) {
    return null
  }

  if (marker !== PRESENT_STRUCT) {
    throw new KafkaJSPartialMessageError(
      `Invalid DescribeTopicPartitionsResponse NextCursor presence marker ${marker}, expected 1 or -1`
    )
  }

  const cursor = { topicName: decoder.readUVarIntString(), partitionIndex: decoder.readInt32() }
  decoder.readTaggedFields()
  return cursor
}

/**
 * DescribeTopicPartitions Response (Version: 0) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * Topics => COMPACT_ARRAY
 *   ErrorCode => INT16
 *   Name => COMPACT_NULLABLE_STRING
 *   TopicId => UUID
 *   IsInternal => BOOLEAN
 *   Partitions => COMPACT_ARRAY
 *     ErrorCode => INT16
 *     PartitionIndex => INT32
 *     LeaderId => INT32
 *     LeaderEpoch => INT32
 *     ReplicaNodes => COMPACT_ARRAY of INT32
 *     IsrNodes => COMPACT_ARRAY of INT32
 *     EligibleLeaderReplicas => COMPACT_ARRAY of INT32 (nullable)
 *     LastKnownElr => COMPACT_ARRAY of INT32 (nullable)
 *     OfflineReplicas => COMPACT_ARRAY of INT32
 *     TAG_BUFFER
 *   TopicAuthorizedOperations => INT32
 *   TAG_BUFFER
 * NextCursor => nullable struct: INT8 -1 when absent, INT8 1 then the struct when present
 *   TopicName => COMPACT_STRING
 *   PartitionIndex => INT32
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json (Apache Kafka 4.3.1)
 *
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, topics: object[],
 *   nextCursor: { topicName: string, partitionIndex: number }|null }>} `throttleTime` is pinned at
 *   0 and the broker's value is reported as `clientSideThrottleTime` (KIP-219), as every
 *   quota-aware codec here does
 * @throws {KafkaJSPartialMessageError} on a truncated body or an illegal cursor marker
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const topics = decoder.readUVarIntArray(decodeTopic) || []
  const nextCursor = decodeNextCursor(decoder)

  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, topics, nextCursor }
}

/**
 * Deliberately does NOT throw on a topic-level error code. An unknown topic, an unauthorized one
 * and a name the broker refuses are answers about one topic among the requested ones, and the
 * caller decides what each costs (`admin.fetchTopicMetadata` fails that topic only).
 * @param {object} data the decoded response
 * @returns {Promise<object>} `data` unchanged
 */
const parse = async (data) => data

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'DescribeTopicPartitions',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Topics', type: '[]DescribeTopicPartitionsResponseTopic', versions: '0+' },
    { name: 'Topics.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+', nullable: true },
    { name: 'Topics.TopicId', type: 'uuid', versions: '0+' },
    { name: 'Topics.IsInternal', type: 'bool', versions: '0+' },
    {
      name: 'Topics.Partitions',
      type: '[]DescribeTopicPartitionsResponsePartition',
      versions: '0+',
    },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.LeaderId', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.LeaderEpoch', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ReplicaNodes', type: '[]int32', versions: '0+' },
    { name: 'Topics.Partitions.IsrNodes', type: '[]int32', versions: '0+' },
    {
      name: 'Topics.Partitions.EligibleLeaderReplicas',
      type: '[]int32',
      versions: '0+',
      nullable: true,
    },
    { name: 'Topics.Partitions.LastKnownElr', type: '[]int32', versions: '0+', nullable: true },
    { name: 'Topics.Partitions.OfflineReplicas', type: '[]int32', versions: '0+' },
    { name: 'Topics.TopicAuthorizedOperations', type: 'int32', versions: '0+' },
    { name: 'NextCursor', type: 'Cursor', versions: '0+', nullable: true },
    { name: 'NextCursor.TopicName', type: 'string', versions: '0+' },
    { name: 'NextCursor.PartitionIndex', type: 'int32', versions: '0+' },
  ],
}
