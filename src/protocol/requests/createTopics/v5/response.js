const Decoder = require('../../../decoder')
const { parse } = require('../v2/response')

/**
 * CreateTopics Response (Versions: 5-7) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (2+)
 * Topics => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   TopicId => UUID (7+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_NULLABLE_STRING (1+)
 *   NumPartitions => INT32 (5+)
 *   ReplicationFactor => INT16 (5+)
 *   Configs => COMPACT_NULLABLE_ARRAY (5+)
 *     Name => COMPACT_STRING (5+)
 *     Value => COMPACT_NULLABLE_STRING (5+)
 *     ReadOnly => BOOLEAN (5+)
 *     ConfigSource => INT8 (5+)
 *     IsSensitive => BOOLEAN (5+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 *     tag 0 TopicConfigErrorCode => INT16 (5+, default 0)
 * TAG_BUFFER
 *
 * `topicConfigErrorCode` is not 0 when the topic was created but its configs could not be read
 * back; it is decoded, never thrown, and `configs` is then null. `topicId` (v7) sits between
 * `Name` and `ErrorCode` and is null for a topic that was not created.
 * @see https://kafka.apache.org/43/design/protocol
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-525+-+Return+topic+metadata+and+configs+in+CreateTopics+response
 * @see clients/src/main/resources/common/message/CreateTopicsResponse.json (Apache Kafka 4.3.1)
 */

const TOPIC_CONFIG_ERROR_CODE_TAG = 0

const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

const decodeConfig = (decoder) => {
  const config = {
    name: decoder.readUVarIntString(),
    value: decoder.readUVarIntString(),
    readOnly: decoder.readBoolean(),
    configSource: decoder.readInt8(),
    isSensitive: decoder.readBoolean(),
  }

  decoder.readTaggedFields()
  return config
}

/**
 * @param {number} version 5, 6 or 7
 * @returns {(decoder: Decoder) => object} a reader for one topic result at that version
 */
const decodeTopic = (version) => (decoder) => {
  const name = decoder.readUVarIntString()
  const topicId = version >= 7 ? decoder.readUuid() : undefined
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const numPartitions = decoder.readInt32()
  const replicationFactor = decoder.readInt16()
  const configs = decoder.readUVarIntArray(decodeConfig)
  const configError = decoder.readTaggedFields().get(TOPIC_CONFIG_ERROR_CODE_TAG)

  const result = {
    topic: name,
    errorCode,
    errorMessage,
    topicConfigErrorCode: configError === undefined ? 0 : new Decoder(configError).readInt16(),
    numPartitions,
    replicationFactor,
    configs,
  }

  if (version >= 7) result.topicId = topicId
  return result
}

/**
 * @param {number} version 5, 6 or 7; v6 has the bytes of v5 and v7 adds `topicId`
 * @returns {(rawData: Buffer) => Promise<object>} the decoder for that version: `throttleTime` is
 *   pinned at 0 and the broker's value is reported as `clientSideThrottleTime` (KIP-219), and the
 *   topics are sorted by name as v2's are
 */
const buildDecode = (version) => async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  // A compact null count (0x00) reads as an empty list, as v2's classic -1 does
  const listed = decoder.readUVarIntArray(decodeTopic(version)) || []

  decoder.readTaggedFields()
  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    topicErrors: listed.sort(topicNameComparator),
  }
}

module.exports = { buildDecode, decode: buildDecode(5), parse }

module.exports.schema = {
  api: 'CreateTopics',
  version: 5,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '2+' },
    { name: 'Topics', type: '[]CreatableTopicResult', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.ErrorMessage', type: 'string', versions: '1+', nullable: true },
    { name: 'Topics.TopicConfigErrorCode', type: 'int16', versions: '5+', tag: 0 },
    { name: 'Topics.NumPartitions', type: 'int32', versions: '5+' },
    { name: 'Topics.ReplicationFactor', type: 'int16', versions: '5+' },
    { name: 'Topics.Configs', type: '[]CreatableTopicConfigs', versions: '5+', nullable: true },
    { name: 'Topics.Configs.Name', type: 'string', versions: '5+' },
    { name: 'Topics.Configs.Value', type: 'string', versions: '5+', nullable: true },
    { name: 'Topics.Configs.ReadOnly', type: 'bool', versions: '5+' },
    { name: 'Topics.Configs.ConfigSource', type: 'int8', versions: '5+' },
    { name: 'Topics.Configs.IsSensitive', type: 'bool', versions: '5+' },
  ],
}
