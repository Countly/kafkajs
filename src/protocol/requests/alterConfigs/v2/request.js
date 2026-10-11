const Encoder = require('../../../encoder')
const { AlterConfigs: apiKey } = require('../../apiKeys')

/**
 * AlterConfigs Request (Version: 2) — flexible (KIP-482), request header v2
 *
 * Resources => COMPACT_ARRAY (0+)
 *   ResourceType => INT8 (0+)
 *   ResourceName => COMPACT_STRING (0+)
 *   Configs => COMPACT_ARRAY (0+)
 *     Name => COMPACT_STRING (0+)
 *     Value => COMPACT_NULLABLE_STRING (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * ValidateOnly => BOOLEAN (0+)
 * TAG_BUFFER
 *
 * The v1 fields in compact framing: v2 adds none. AlterConfigs replaces the resource's whole
 * dynamic config, so an entry left out is deleted; IncrementalAlterConfigs changes only the
 * entries it names.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/AlterConfigsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ type: number, name: string,
 *   configEntries: { name: string, value: string|null }[] }[]} options.resources
 * @param {boolean} [options.validateOnly=false] validate the change without applying it
 * @returns {object} the protocol request
 */
module.exports = ({ resources, validateOnly = false }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'AlterConfigs',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(resources.map(encodeResource))
      .writeBoolean(validateOnly)
      .writeTaggedFields()
  },
})

const encodeResource = ({ type, name, configEntries }) => {
  return new Encoder()
    .writeInt8(type)
    .writeUVarIntString(name)
    .writeUVarIntArray(configEntries.map(encodeConfigEntry))
    .writeTaggedFields()
}

const encodeConfigEntry = ({ name, value }) => {
  return new Encoder().writeUVarIntString(name).writeUVarIntString(value).writeTaggedFields()
}

module.exports.schema = {
  api: 'AlterConfigs',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'Resources', type: '[]AlterConfigsResource', versions: '0+' },
    { name: 'Resources.ResourceType', type: 'int8', versions: '0+' },
    { name: 'Resources.ResourceName', type: 'string', versions: '0+' },
    { name: 'Resources.Configs', type: '[]AlterableConfig', versions: '0+' },
    { name: 'Resources.Configs.Name', type: 'string', versions: '0+' },
    { name: 'Resources.Configs.Value', type: 'string', versions: '0+', nullable: true },
    { name: 'ValidateOnly', type: 'bool', versions: '0+' },
  ],
}
