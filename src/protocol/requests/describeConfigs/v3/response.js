const Decoder = require('../../../decoder')
const { DEFAULT_CONFIG } = require('../../../configSource')
const { parse } = require('../v1/response')

/**
 * DescribeConfigs Response (Version: 3) => throttle_time_ms [results]
 *   throttle_time_ms => INT32
 *   results => error_code error_message resource_type resource_name [configs]
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 *     configs => name value read_only config_source is_sensitive [synonyms] config_type documentation
 *       name => STRING
 *       value => NULLABLE_STRING
 *       read_only => BOOLEAN
 *       config_source => INT8
 *       is_sensitive => BOOLEAN
 *       synonyms => name value source
 *         name => STRING
 *         value => NULLABLE_STRING
 *         source => INT8
 *       config_type => INT8
 *       documentation => NULLABLE_STRING
 *
 * v2's entry plus `configType` (the raw byte; see `ConfigTypes`) and `documentation`, which is
 * null unless the request asked for it. `isDefault` is still derived from `configSource`.
 * @see clients/src/main/resources/common/message/DescribeConfigsResponse.json (Apache Kafka 4.3.1)
 */

const decodeSynonyms = (decoder) => ({
  configName: decoder.readString(),
  configValue: decoder.readString(),
  configSource: decoder.readInt8(),
})

const decodeConfigEntries = (decoder) => {
  const configName = decoder.readString()
  const configValue = decoder.readString()
  const readOnly = decoder.readBoolean()
  const configSource = decoder.readInt8()
  const isSensitive = decoder.readBoolean()
  const configSynonyms = decoder.readArray(decodeSynonyms)
  const configType = decoder.readInt8()
  const documentation = decoder.readString()

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

const decodeResources = (decoder) => ({
  errorCode: decoder.readInt16(),
  errorMessage: decoder.readString(),
  resourceType: decoder.readInt8(),
  resourceName: decoder.readString(),
  configEntries: decoder.readArray(decodeConfigEntries),
})

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, resources: object[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const resources = decoder.readArray(decodeResources)

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, resources }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'DescribeConfigs',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Results', type: '[]DescribeConfigsResult', versions: '0+' },
    { name: 'Results.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Results.ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Results.ResourceType', type: 'int8', versions: '0+' },
    { name: 'Results.ResourceName', type: 'string', versions: '0+' },
    { name: 'Results.Configs', type: '[]DescribeConfigsResourceResult', versions: '0+' },
    { name: 'Results.Configs.Name', type: 'string', versions: '0+' },
    { name: 'Results.Configs.Value', type: 'string', versions: '0+', nullable: true },
    { name: 'Results.Configs.ReadOnly', type: 'bool', versions: '0+' },
    { name: 'Results.Configs.ConfigSource', type: 'int8', versions: '1+' },
    { name: 'Results.Configs.IsSensitive', type: 'bool', versions: '0+' },
    { name: 'Results.Configs.Synonyms', type: '[]DescribeConfigsSynonym', versions: '1+' },
    { name: 'Results.Configs.Synonyms.Name', type: 'string', versions: '1+' },
    { name: 'Results.Configs.Synonyms.Value', type: 'string', versions: '1+', nullable: true },
    { name: 'Results.Configs.Synonyms.Source', type: 'int8', versions: '1+' },
    { name: 'Results.Configs.ConfigType', type: 'int8', versions: '3+' },
    { name: 'Results.Configs.Documentation', type: 'string', versions: '3+', nullable: true },
  ],
}
