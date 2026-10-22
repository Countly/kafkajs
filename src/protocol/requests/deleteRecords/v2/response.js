const Decoder = require('../../../decoder')
const responseV0 = require('../v0/response')

/**
 * DeleteRecords Response (Version: 2) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * Topics => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   Partitions => COMPACT_ARRAY (0+)
 *     PartitionIndex => INT32 (0+)
 *     LowWatermark => INT64 (0+)
 *     ErrorCode => INT16 (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing, decoded to v1's keys: the topics sorted by name, `lowWatermark`
 * a bigint, and the broker's throttle time reported as `clientSideThrottleTime` (KIP-219).
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteRecordsResponse.json (Apache Kafka 4.3.1)
 */

const topicNameComparator = (a, b) => (a.topic ?? '').localeCompare(b.topic ?? '')

const decodePartition = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
    lowWatermark: decoder.readInt64(),
    errorCode: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return partition
}

const decodeTopic = (decoder) => {
  const topic = {
    topic: decoder.readUVarIntString(),
    partitions: Decoder.requireArray(
      decoder.readUVarIntArray(decodePartition),
      'DeleteRecordsResponse.Topics.Partitions'
    ),
  }

  decoder.readTaggedFields()
  return topic
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, topics: object[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  // A compact null count (0x00) reads as an empty list, as v0's classic -1 does
  const listed = decoder.readUVarIntArray(decodeTopic) || []

  decoder.readTaggedFields()
  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    topics: listed.sort(topicNameComparator),
  }
}

/**
 * @param {object} options
 * @param {{ topic: string, partitions: { partition: number, offset: * }[] }[]} options.topics the
 *   topics of the request, whose offsets `parse` attaches to a failed partition
 * @returns {{ decode: function, parse: function }} `parse` is v0's: it throws a
 *   `KafkaJSDeleteTopicRecordsError` naming the failed partitions
 */
module.exports = ({ topics }) => ({ decode, parse: responseV0({ topics }).parse })

module.exports.schema = {
  api: 'DeleteRecords',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Topics', type: '[]DeleteRecordsTopicResult', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]DeleteRecordsPartitionResult', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.LowWatermark', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
