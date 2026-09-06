const Decoder = require('../../../decoder')

/**
 * OffsetForLeaderEpochResponse (Version: 3)
 *
 * ThrottleTimeMs => INT32
 * Topics => ARRAY
 *   Topic => STRING
 *   Partitions => ARRAY
 *     ErrorCode => INT16
 *     Partition => INT32
 *     LeaderEpoch => INT32
 *     EndOffset => INT64
 *
 * @see clients/src/main/resources/common/message/OffsetForLeaderEpochResponse.json (Apache Kafka 4.3.1)
 */
const decodePartition = (decoder) => ({
  errorCode: decoder.readInt16(),
  partition: decoder.readInt32(),
  leaderEpoch: decoder.readInt32(),
  endOffset: decoder.readInt64().toString(),
})

const decodeTopic = (decoder) => ({
  topic: decoder.readString(),
  partitions: decoder.readArray(decodePartition),
})

/**
 * @param {Buffer} rawData
 * @returns {Promise<object>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const clientSideThrottleTime = decoder.readInt32()
  const topics = decoder.readArray(decodeTopic)

  // Report a throttleTime of 0: the broker has not throttled this request, but expects the
  // client to honour clientSideThrottleTime itself.
  return { throttleTime: 0, clientSideThrottleTime, topics }
}

/**
 * Per-partition error codes are returned rather than thrown. Position validation classifies them:
 * a retriable code is retried against the leader within a bounded budget, a non-retriable one
 * turns validation off for the rest of the assignment, and neither may escape into the consumer's
 * fetch loop as an exception.
 * @param {object} data
 * @returns {Promise<object>} the same object
 */
const parse = async (data) => data

module.exports = { decode, parse }
module.exports.schema = {
  api: 'OffsetForLeaderEpoch',
  version: 3,
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
