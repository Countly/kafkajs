const Encoder = require('../../../encoder')
const { DescribeConfigs: apiKey } = require('../../apiKeys')

/**
 * DescribeConfigs Request (Version: 3) => [resources] include_synonyms include_documentation
 *   resources => resource_type resource_name [configuration_keys]
 *     resource_type => INT8
 *     resource_name => STRING
 *     configuration_keys => NULLABLE_ARRAY of STRING
 *   include_synonyms => BOOLEAN
 *   include_documentation => BOOLEAN
 *
 * An absent, null or empty `configNames` is written as the null list, which asks for every key.
 * @see clients/src/main/resources/common/message/DescribeConfigsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ type: number, name: string, configNames?: string[]|null }[]} options.resources
 * @param {boolean} [options.includeSynonyms=false]
 * @param {boolean} [options.includeDocumentation=false]
 * @returns {object} the protocol request
 */
module.exports = ({ resources, includeSynonyms = false, includeDocumentation = false }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'DescribeConfigs',
  encode: async () => {
    return new Encoder()
      .writeArray(resources.map(encodeResource))
      .writeBoolean(includeSynonyms)
      .writeBoolean(includeDocumentation)
  },
})

const encodeResource = ({ type, name, configNames }) => {
  return new Encoder()
    .writeInt8(type)
    .writeString(name)
    .writeNullableArray(configNames || [])
}

module.exports.schema = {
  api: 'DescribeConfigs',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'Resources', type: '[]DescribeConfigsResource', versions: '0+' },
    { name: 'Resources.ResourceType', type: 'int8', versions: '0+' },
    { name: 'Resources.ResourceName', type: 'string', versions: '0+' },
    { name: 'Resources.ConfigurationKeys', type: '[]string', versions: '0+', nullable: true },
    { name: 'IncludeSynonyms', type: 'bool', versions: '1+' },
    { name: 'IncludeDocumentation', type: 'bool', versions: '3+' },
  ],
}
