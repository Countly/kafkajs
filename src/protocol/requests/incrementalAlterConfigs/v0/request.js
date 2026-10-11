const Encoder = require('../../../encoder')
const AlterConfigOpTypes = require('../../../alterConfigOpTypes')
const { KafkaJSNonRetriableError } = require('../../../../errors')
const { IncrementalAlterConfigs: apiKey } = require('../../apiKeys')

/**
 * IncrementalAlterConfigs Request (Version: 0) => [resources] validate_only
 *   resources => resource_type resource_name [configs]
 *     resource_type => INT8
 *     resource_name => STRING
 *     configs => name config_operation value
 *       name => STRING
 *       config_operation => INT8
 *       value => NULLABLE_STRING
 *   validate_only => BOOLEAN
 *
 * A DELETE writes a null value whatever `value` it carries, because the broker ignores the value
 * of a DELETE; every other operation writes `value` as given. An entry whose `operation` is not an
 * `AlterConfigOpTypes` value, or that has no `value` and is not a DELETE, is refused before any
 * byte is written: the controller applies a SET with a null value as the removal of the config.
 * @see clients/src/main/resources/common/message/IncrementalAlterConfigsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ type: number, name: string,
 *   configEntries: { name: string, operation: number, value?: string|null }[] }[]} options.resources
 * @param {boolean} [options.validateOnly=false] validate the change without applying it
 * @returns {object} the protocol request; its `encode` rejects with `KafkaJSNonRetriableError` for
 *   an entry that is refused
 */
module.exports = ({ resources, validateOnly = false }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'IncrementalAlterConfigs',
  encode: async () => {
    return new Encoder().writeArray(resources.map(encodeResource)).writeBoolean(validateOnly)
  },
})

const encodeResource = ({ type, name, configEntries }) => {
  return new Encoder()
    .writeInt8(type)
    .writeString(name)
    .writeArray(configEntries.map(encodeConfigEntry))
}

/**
 * @param {{ name: string, operation: number, value?: string|null }} entry
 * @returns {string|null} the value to write: null for a DELETE, `value` for any other operation
 * @throws {KafkaJSNonRetriableError} when `operation` is not an `AlterConfigOpTypes` value (a
 *   missing one would be written as 0, a SET), or when a SET, APPEND or SUBTRACT has no value
 */
const entryValue = ({ name, operation, value }) => {
  if (!Object.values(AlterConfigOpTypes).includes(operation)) {
    throw new KafkaJSNonRetriableError(
      `Invalid operation ${operation} for the config "${name}": use an AlterConfigOpTypes value`
    )
  }

  if (operation === AlterConfigOpTypes.DELETE) return null
  if (value == null) {
    throw new KafkaJSNonRetriableError(
      `The config "${name}" needs a value: only a DELETE may leave it out`
    )
  }

  return value
}

const encodeConfigEntry = (entry) => {
  return new Encoder()
    .writeString(entry.name)
    .writeInt8(entry.operation)
    .writeString(entryValue(entry))
}

module.exports.entryValue = entryValue

module.exports.schema = {
  api: 'IncrementalAlterConfigs',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'Resources', type: '[]AlterConfigsResource', versions: '0+' },
    { name: 'Resources.ResourceType', type: 'int8', versions: '0+' },
    { name: 'Resources.ResourceName', type: 'string', versions: '0+' },
    { name: 'Resources.Configs', type: '[]AlterableConfig', versions: '0+' },
    { name: 'Resources.Configs.Name', type: 'string', versions: '0+' },
    { name: 'Resources.Configs.ConfigOperation', type: 'int8', versions: '0+' },
    { name: 'Resources.Configs.Value', type: 'string', versions: '0+', nullable: true },
    { name: 'ValidateOnly', type: 'bool', versions: '0+' },
  ],
}
