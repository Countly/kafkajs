const Encoder = require('../../encoder')

/**
 * The two nested encoders `ConsumerGroupHeartbeatRequest` needs, shared by every version: the
 * field layouts are identical at v0 and v1, so a single definition keeps the two codecs from
 * drifting apart one line at a time.
 * @see clients/src/main/resources/common/message/ConsumerGroupHeartbeatRequest.json (Apache Kafka 4.3.1)
 */

/**
 * One `SubscribedTopicNames` element: COMPACT_STRING.
 * @param {string} name
 * @returns {Encoder}
 */
const encodeName = (name) => new Encoder().writeUVarIntString(name)

/**
 * One `TopicPartitions` element: UUID, COMPACT_ARRAY of INT32, TAG_BUFFER.
 * @param {{ topicId: string, partitions: number[] }} entry
 * @returns {Encoder}
 */
const encodeTopicPartitions = ({ topicId, partitions }) =>
  new Encoder().writeUuid(topicId).writeUVarIntArray(partitions, 'int32').writeTaggedFields()

module.exports = { encodeName, encodeTopicPartitions }
