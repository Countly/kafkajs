const Encoder = require('../../../encoder')
const { DescribeConfigs: apiKey } = require('../../apiKeys')
const { schema } = require('../v3/request')

/**
 * DescribeConfigs Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * Resources => COMPACT_ARRAY (0+)
 *   ResourceType => INT8 (0+)
 *   ResourceName => COMPACT_STRING (0+)
 *   ConfigurationKeys => COMPACT_NULLABLE_ARRAY of COMPACT_STRING (0+)
 *   TAG_BUFFER
 * IncludeSynonyms => BOOLEAN (1+)
 * IncludeDocumentation => BOOLEAN (3+)
 * TAG_BUFFER
 *
 * v3 in compact framing. An absent, null or empty `configNames` is written as the compact null
 * (0x00), which asks for every key; `writeUVarIntArray([])` would write an empty list instead.
 * @see https://kafka.apache.org/43/design/protocol
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
  apiVersion: 4,
  apiName: 'DescribeConfigs',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(resources.map(encodeResource))
      .writeBoolean(includeSynonyms)
      .writeBoolean(includeDocumentation)
      .writeTaggedFields()
  },
})

const encodeResource = ({ type, name, configNames }) => {
  return new Encoder()
    .writeInt8(type)
    .writeUVarIntString(name)
    .writeUVarIntArray(configNames && configNames.length > 0 ? configNames.map(encodeKey) : null)
    .writeTaggedFields()
}

const encodeKey = (name) => new Encoder().writeUVarIntString(name)

module.exports.schema = { ...schema, version: 4 }
