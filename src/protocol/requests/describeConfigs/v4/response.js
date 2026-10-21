const Decoder = require('../../../decoder')
const { DEFAULT_CONFIG } = require('../../../configSource')
const { parse } = require('../v1/response')
const { schema } = require('../v3/response')

/**
 * DescribeConfigs Response (Version: 4) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * Results => COMPACT_ARRAY (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_NULLABLE_STRING (0+)
 *   ResourceType => INT8 (0+)
 *   ResourceName => COMPACT_STRING (0+)
 *   Configs => COMPACT_ARRAY (0+)
 *     Name => COMPACT_STRING (0+)
 *     Value => COMPACT_NULLABLE_STRING (0+)
 *     ReadOnly => BOOLEAN (0+)
 *     ConfigSource => INT8 (1+)
 *     IsSensitive => BOOLEAN (0+)
 *     Synonyms => COMPACT_ARRAY (1+)
 *       Name => COMPACT_STRING (1+)
 *       Value => COMPACT_NULLABLE_STRING (1+)
 *       Source => INT8 (1+)
 *       TAG_BUFFER
 *     ConfigType => INT8 (3+)
 *     Documentation => COMPACT_NULLABLE_STRING (3+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v3 in compact framing, decoded to v3's keys.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeConfigsResponse.json (Apache Kafka 4.3.1)
 */

const decodeSynonyms = (decoder) => {
  const synonym = {
    configName: decoder.readUVarIntString(),
    configValue: decoder.readUVarIntString(),
    configSource: decoder.readInt8(),
  }

  decoder.readTaggedFields()
  return synonym
}

const decodeConfigEntries = (decoder) => {
  const configName = decoder.readUVarIntString()
  const configValue = decoder.readUVarIntString()
  const readOnly = decoder.readBoolean()
  const configSource = decoder.readInt8()
  const isSensitive = decoder.readBoolean()
  const configSynonyms = decoder.readUVarIntArray(decodeSynonyms)
  const configType = decoder.readInt8()
  const documentation = decoder.readUVarIntString()

  decoder.readTaggedFields()
  return {
    configName,
    configValue,
    readOnly,
    isDefault: configSource === DEFAULT_CONFIG,
    configSource,
    isSensitive,
    configSynonyms,
    configType,
    documentation,
  }
}

const decodeResources = (decoder) => {
  const resource = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    resourceType: decoder.readInt8(),
    resourceName: decoder.readUVarIntString(),
    configEntries: decoder.readUVarIntArray(decodeConfigEntries),
  }

  decoder.readTaggedFields()
  return resource
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, resources: object[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const resources = decoder.readUVarIntArray(decodeResources) || []

  decoder.readTaggedFields()
  return { throttleTime: 0, clientSideThrottleTime: throttleTime, resources }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = { ...schema, version: 4 }
