const Encoder = require('../../../encoder')
const { DescribeTopicPartitions: apiKey } = require('../../apiKeys')

/**
 * The one-byte presence markers Kafka's generator writes for a nullable, non-tagged struct field:
 * INT8 -1 when the field is null, INT8 1 followed by the struct's body when it is present.
 * @see https://github.com/apache/kafka/blob/4.3/generator/src/main/java/org/apache/kafka/message/MessageDataGenerator.java
 */
const NULL_STRUCT = -1
const PRESENT_STRUCT = 1

/**
 * @param {string} name
 * @returns {Encoder} one `Topics` element: COMPACT_STRING, TAG_BUFFER
 */
const encodeTopic = (name) => new Encoder().writeUVarIntString(name).writeTaggedFields()

/**
 * @param {{ topicName: string, partitionIndex: number }|null|undefined} cursor
 * @returns {Encoder} the nullable `Cursor` struct: the marker -1 for none, else the marker 1 and
 *   the body. A cursor on partition 0 is a present cursor, not a null one.
 */
const encodeCursor = (cursor) =>
  cursor == null
    ? new Encoder().writeInt8(NULL_STRUCT)
    : new Encoder()
        .writeInt8(PRESENT_STRUCT)
        .writeUVarIntString(cursor.topicName)
        .writeInt32(cursor.partitionIndex)
        .writeTaggedFields()

/**
 * DescribeTopicPartitions Request (Version: 0) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   TAG_BUFFER
 * ResponsePartitionLimit => INT32
 * Cursor => nullable struct: INT8 -1 when absent, INT8 1 then the struct when present
 *   TopicName => COMPACT_STRING
 *   PartitionIndex => INT32
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * An empty `topics` list asks the broker for every topic it can describe, so a caller that means
 * "these topics" never sends one. The broker pages through the topics in name order, at most
 * `responsePartitionLimit` partitions per response (it raises a limit below 1 to 1 and lowers one
 * above its own `max.request.partition.size.limit`); `cursor` names the topic and partition to
 * resume from and must be one of the listed topics.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeTopicPartitionsRequest.json (Apache Kafka 4.3.1)
 * @see https://github.com/apache/kafka/blob/4.3.1/core/src/main/java/kafka/server/handlers/DescribeTopicPartitionsRequestHandler.java
 *
 * @param {object} options
 * @param {string[]} options.topics the topic names to describe
 * @param {number} [options.responsePartitionLimit=2000] the most partitions one response holds
 * @param {{ topicName: string, partitionIndex: number }|null} [options.cursor=null] where to resume
 * @returns {object} the protocol request
 */
module.exports = ({ topics, responsePartitionLimit = 2000, cursor = null }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DescribeTopicPartitions',
  encode: async () =>
    new Encoder()
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeInt32(responsePartitionLimit)
      .writeEncoder(encodeCursor(cursor))
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'DescribeTopicPartitions',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]TopicRequest', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'ResponsePartitionLimit', type: 'int32', versions: '0+' },
    { name: 'Cursor', type: 'Cursor', versions: '0+', nullable: true },
    { name: 'Cursor.TopicName', type: 'string', versions: '0+' },
    { name: 'Cursor.PartitionIndex', type: 'int32', versions: '0+' },
  ],
}
