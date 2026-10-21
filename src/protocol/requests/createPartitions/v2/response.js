const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * CreatePartitions Response (Version: 2) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * Results => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_NULLABLE_STRING (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing, decoded to v1's keys: the topics sorted by name, with the
 * broker's throttle time reported as `clientSideThrottleTime` (KIP-219). `parse` is v0's: it
 * throws the error of the first failed topic.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/CreatePartitionsResponse.json (Apache Kafka 4.3.1)
 */

const topicNameComparator = (a, b) => (a.topic ?? '').localeCompare(b.topic ?? '')

const decodeTopicError = (decoder) => {
  const topicError = {
    topic: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return topicError
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, topicErrors:
 *   { topic: string, errorCode: number, errorMessage: string|null }[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  // A compact null count (0x00) reads as an empty list, as v0's classic -1 does
  const listed = decoder.readUVarIntArray(decodeTopicError) || []

  decoder.readTaggedFields()
  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    topicErrors: listed.sort(topicNameComparator),
  }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'CreatePartitions',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Results', type: '[]CreatePartitionsTopicResult', versions: '0+' },
    { name: 'Results.Name', type: 'string', versions: '0+' },
    { name: 'Results.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Results.ErrorMessage', type: 'string', versions: '0+', nullable: true },
  ],
}
