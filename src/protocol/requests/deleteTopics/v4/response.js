const Decoder = require('../../../decoder')
const { parse } = require('../v1/response')

/**
 * DeleteTopics Response (Versions: 4-6) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (1+)
 * Responses => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0-5), COMPACT_NULLABLE_STRING (6+)
 *   TopicId => UUID (6+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_NULLABLE_STRING (5+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * Each result is `{ topic, errorCode }` as v1 has it; v5 adds `errorMessage` and v6 adds `topicId`
 * (between `Name` and `ErrorCode`): the topic's id, or null for the zero uuid the controller sends
 * for a name it could not delete. `topic` is null for a result of a deletion by id (an unknown or
 * duplicate id, or one the caller may not describe) and for an entry that names nothing, never for
 * a deletion by name.
 * @see core/src/main/scala/kafka/server/ControllerApis.scala (Apache Kafka 4.3.1)
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteTopicsResponse.json (Apache Kafka 4.3.1)
 */

// A null name sorts first, so a result the broker could not name never breaks the sort.
const topicNameComparator = (a, b) => (a.topic ?? '').localeCompare(b.topic ?? '')

/**
 * @param {number} version 4, 5 or 6
 * @returns {(decoder: Decoder) => object} a reader for one topic result at that version
 */
const decodeTopic = (version) => (decoder) => {
  const result = { topic: decoder.readUVarIntString() }

  if (version >= 6) result.topicId = decoder.readUuid()
  result.errorCode = decoder.readInt16()
  if (version >= 5) result.errorMessage = decoder.readUVarIntString()

  decoder.readTaggedFields()
  return result
}

/**
 * @param {number} version 4, 5 or 6; v5 adds `errorMessage` and v6 adds `topicId`
 * @returns {(rawData: Buffer) => Promise<object>} the decoder for that version: `throttleTime` is
 *   pinned at 0 and the broker's value is reported as `clientSideThrottleTime` (KIP-219), and the
 *   topics are sorted by name as v1's are
 */
const buildDecode = (version) => async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  // A compact null count (0x00) reads as an empty list, as v1's classic -1 does
  const listed = decoder.readUVarIntArray(decodeTopic(version)) || []

  decoder.readTaggedFields()
  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    topicErrors: listed.sort(topicNameComparator),
  }
}

module.exports = { buildDecode, decode: buildDecode(4), parse }

module.exports.schema = {
  api: 'DeleteTopics',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'Responses', type: '[]DeletableTopicResult', versions: '0+' },
    { name: 'Responses.Name', type: 'string', versions: '0+' },
    { name: 'Responses.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
