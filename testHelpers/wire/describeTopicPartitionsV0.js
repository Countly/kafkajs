const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/describeTopicPartitions/fixtures/java/describeTopicPartitionsResponse-v0-noNextCursor.json')

const encodeNodes = (nodes) => new Encoder().writeUVarIntArray(nodes, 'int32')

const encodePartition = (partition) =>
  new Encoder()
    .writeInt16(partition.errorCode)
    .writeInt32(partition.partitionIndex)
    .writeInt32(partition.leaderId)
    .writeInt32(partition.leaderEpoch)
    .writeEncoder(encodeNodes(partition.replicaNodes))
    .writeEncoder(encodeNodes(partition.isrNodes))
    .writeEncoder(encodeNodes(partition.eligibleLeaderReplicas))
    .writeEncoder(encodeNodes(partition.lastKnownElr))
    .writeEncoder(encodeNodes(partition.offlineReplicas))
    .writeTaggedFields()

const encodeTopic = (topic) =>
  new Encoder()
    .writeInt16(topic.errorCode)
    .writeUVarIntString(topic.name)
    .writeUuid(topic.topicId)
    .writeBoolean(topic.isInternal)
    .writeUVarIntArray(topic.partitions.map(encodePartition))
    .writeInt32(topic.topicAuthorizedOperations)
    .writeTaggedFields()

/** A nullable struct: -1 for none, 1 and its fields otherwise. */
const encodeCursor = (cursor) =>
  cursor === null
    ? new Encoder().writeInt8(-1)
    : new Encoder()
        .writeInt8(1)
        .writeUVarIntString(cursor.topicName)
        .writeInt32(cursor.partitionIndex)
        .writeTaggedFields()

const readCursor = (decoder) => {
  if (decoder.readInt8() === -1) {
    return null
  }

  const cursor = { topicName: decoder.readUVarIntString(), partitionIndex: decoder.readInt32() }
  decoder.readTaggedFields()
  return cursor
}

/**
 * DescribeTopicPartitions v0 (flexible), the KIP-966 paged read of topic partitions. The request
 * names the topics, how many partitions one answer may hold and where to resume (a cursor); the
 * response lists each topic with its partitions and, when the limit cut it short, the cursor to
 * continue from. A zero topic id (an unauthorized topic) is read and written as `null`, and the two
 * eligible-leader lists are `null` when the broker sends none.
 *
 * Evidence: Java's structured vectors `describeTopicPartitionsRequest-v0-{noCursor,cursor}.json` and
 * `describeTopicPartitionsResponse-v0-*.json` (`hex` and `fields`); the unvaried response is the
 * `noNextCursor` vector.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json
 */
module.exports = {
  api: 'DescribeTopicPartitions',
  apiKey: apiKeys.DescribeTopicPartitions,
  version: 0,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `topics` (`{ errorCode, name,
   *   topicId, isInternal, partitions, topicAuthorizedOperations }`, a partition being `{
   *   errorCode, partitionIndex, leaderId, leaderEpoch, replicaNodes, isrNodes,
   *   eligibleLeaderReplicas, lastKnownElr, offlineReplicas }`) and `nextCursor` (`{ topicName,
   *   partitionIndex }` or null)
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeUVarIntArray(response.topics.map(encodeTopic))
      .writeEncoder(encodeCursor(response.nextCursor))
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ topics: string[], responsePartitionLimit: number, cursor: { topicName: string,
   *   partitionIndex: number }|null }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      topics: decoder.readUVarIntArray((topic) => {
        const name = topic.readUVarIntString()
        topic.readTaggedFields()
        return name
      }),
      responsePartitionLimit: decoder.readInt32(),
      cursor: readCursor(decoder),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('DescribeTopicPartitions v0 request has a byte after its tag buffer')
    }

    return request
  },
}
