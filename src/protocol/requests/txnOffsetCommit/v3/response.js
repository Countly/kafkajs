const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * TxnOffsetCommit Response (Version: 3) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * Topics => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY
 *     PartitionIndex => INT32
 *     ErrorCode => INT16
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * Decoded into v1's shape,
 * `{ throttleTime: 0, clientSideThrottleTime, topics: [{ topic, partitions: [{ partition, errorCode }] }] }`;
 * `parse` throws the first failed partition's error, as at every earlier version. v3 lets the
 * coordinator answer ILLEGAL_GENERATION, UNKNOWN_MEMBER_ID and FENCED_INSTANCE_ID.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitResponse.json (Apache Kafka 4.3.1)
 *
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, topics: { topic: string, partitions: { partition: number, errorCode: number }[] }[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const topics = Decoder.requireArray(
    decoder.readUVarIntArray(decodeTopic),
    'TxnOffsetCommitResponse.Topics'
  )
  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, topics }
}

const decodeTopic = (decoder) => {
  const topic = {
    topic: decoder.readUVarIntString(),
    partitions: Decoder.requireArray(
      decoder.readUVarIntArray(decodePartition),
      'TxnOffsetCommitResponse.Topics.Partitions'
    ),
  }
  decoder.readTaggedFields()
  return topic
}

const decodePartition = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
  }
  decoder.readTaggedFields()
  return partition
}

module.exports = { decode, parse }

module.exports.schema = {
  api: 'TxnOffsetCommit',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Topics', type: '[]TxnOffsetCommitResponseTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0-5' },
    { name: 'Topics.Partitions', type: '[]TxnOffsetCommitResponsePartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
