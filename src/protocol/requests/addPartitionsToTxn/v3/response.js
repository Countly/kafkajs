const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * AddPartitionsToTxn Response (Version: 3) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ResultsByTopicV3AndBelow => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   ResultsByPartition => COMPACT_ARRAY
 *     PartitionIndex => INT32
 *     PartitionErrorCode => INT16
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * Decoded into v1's shape — `{ throttleTime: 0, clientSideThrottleTime, errors: [{ topic,
 * partitionErrors: [{ partition, errorCode }] }] }`; the broker's throttle is reported as
 * `clientSideThrottleTime` (KIP-219). `parse` is v0's: it throws the first failed partition's error.
 * @see clients/src/main/resources/common/message/AddPartitionsToTxnResponse.json (Apache Kafka 4.3.1)
 *
 * @param {Buffer} rawData
 * @returns {Promise<object>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errors = decoder.readUVarIntArray(decodeTopicResult) || []
  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, errors }
}

const decodeTopicResult = (decoder) => {
  const result = {
    topic: decoder.readUVarIntString(),
    partitionErrors: decoder.readUVarIntArray(decodePartitionResult) || [],
  }
  decoder.readTaggedFields()
  return result
}

const decodePartitionResult = (decoder) => {
  const result = { partition: decoder.readInt32(), errorCode: decoder.readInt16() }
  decoder.readTaggedFields()
  return result
}

module.exports = { decode, parse }

module.exports.schema = {
  api: 'AddPartitionsToTxn',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ResultsByTopicV3AndBelow', type: '[]AddPartitionsToTxnTopicResult', versions: '0-3' },
    { name: 'ResultsByTopicV3AndBelow.Name', type: 'string', versions: '0+' },
    {
      name: 'ResultsByTopicV3AndBelow.ResultsByPartition',
      type: '[]AddPartitionsToTxnPartitionResult',
      versions: '0+',
    },
    {
      name: 'ResultsByTopicV3AndBelow.ResultsByPartition.PartitionIndex',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'ResultsByTopicV3AndBelow.ResultsByPartition.PartitionErrorCode',
      type: 'int16',
      versions: '0+',
    },
  ],
}
