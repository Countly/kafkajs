const Encoder = require('../../../encoder')
const { IncrementalAlterConfigs: apiKey } = require('../../apiKeys')
const { schema, entryValue } = require('../v0/request')

/**
 * IncrementalAlterConfigs Request (Version: 1) — flexible (KIP-482), request header v2
 *
 * Resources => COMPACT_ARRAY (0+)
 *   ResourceType => INT8 (0+)
 *   ResourceName => COMPACT_STRING (0+)
 *   Configs => COMPACT_ARRAY (0+)
 *     Name => COMPACT_STRING (0+)
 *     ConfigOperation => INT8 (0+)
 *     Value => COMPACT_NULLABLE_STRING (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * ValidateOnly => BOOLEAN (0+)
 * TAG_BUFFER
 *
 * The v0 fields in compact framing. A DELETE writes the compact null (0x00) whatever `value` it
 * carries, because the broker ignores the value of a DELETE; every other operation writes `value`
 * as given. Entries are refused as in v0: an `operation` that is not an `AlterConfigOpTypes` value,
 * or a missing `value` on anything but a DELETE, rejects `encode` with `KafkaJSNonRetriableError`.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/IncrementalAlterConfigsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ type: number, name: string,
 *   configEntries: { name: string, operation: number, value?: string|null }[] }[]} options.resources
 * @param {boolean} [options.validateOnly=false] validate the change without applying it
 * @returns {object} the protocol request
 */
module.exports = ({ resources, validateOnly = false }) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'IncrementalAlterConfigs',
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

const encodeConfigEntry = (entry) => {
  return new Encoder()
    .writeUVarIntString(entry.name)
    .writeInt8(entry.operation)
    .writeUVarIntString(entryValue(entry))
    .writeTaggedFields()
}

module.exports.schema = { ...schema, version: 1 }
