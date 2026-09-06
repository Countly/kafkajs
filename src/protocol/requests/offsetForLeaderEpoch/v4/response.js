const Decoder = require('../../../decoder')
const { parse } = require('../v3/response')

/**
 * OffsetForLeaderEpochResponse (Version: 4) - flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * Topics => COMPACT_ARRAY
 *   Topic => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY
 *     ErrorCode => INT16
 *     Partition => INT32
 *     LeaderEpoch => INT32
 *     EndOffset => INT64
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * Decodes to exactly the shape v3 produces. `parse` is v3's and throws on nothing.
 * @see clients/src/main/resources/common/message/OffsetForLeaderEpochResponse.json (Apache Kafka 4.3.1)
 */
const decodePartition = (decoder) => {
  const partition = {
    errorCode: decoder.readInt16(),
    partition: decoder.readInt32(),
    leaderEpoch: decoder.readInt32(),
    endOffset: decoder.readInt64().toString(),
  }

  decoder.readTaggedFields()
  return partition
}

const decodeTopic = (decoder) => {
  const topic = {
    topic: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartition),
  }

  decoder.readTaggedFields()
  return topic
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<object>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const clientSideThrottleTime = decoder.readInt32()
  const topics = decoder.readUVarIntArray(decodeTopic)

  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime, topics }
}

module.exports = { decode, parse }
module.exports.schema = {
  api: 'OffsetForLeaderEpoch',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '2+' },
    { name: 'Topics', type: '[]OffsetForLeaderTopicResult', versions: '0+' },
    { name: 'Topics.Topic', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]EpochEndOffset', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Partitions.Partition', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.LeaderEpoch', type: 'int32', versions: '1+' },
    { name: 'Topics.Partitions.EndOffset', type: 'int64', versions: '0+' },
  ],
}
