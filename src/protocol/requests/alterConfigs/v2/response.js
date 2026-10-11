const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * AlterConfigs Response (Version: 2) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * Responses => COMPACT_ARRAY (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_NULLABLE_STRING (0+)
 *   ResourceType => INT8 (0+)
 *   ResourceName => COMPACT_STRING (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing, decoded to v1's keys. `parse` is v0's: it throws the error of
 * the first failed resource.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/AlterConfigsResponse.json (Apache Kafka 4.3.1)
 */

const decodeResources = (decoder) => {
  const resource = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    resourceType: decoder.readInt8(),
    resourceName: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return resource
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number,
 *   resources: { errorCode: number, errorMessage: string|null, resourceType: number,
 *   resourceName: string }[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const resources = decoder.readUVarIntArray(decodeResources)

  decoder.readTaggedFields()
  return { throttleTime: 0, clientSideThrottleTime: throttleTime, resources }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'AlterConfigs',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Responses', type: '[]AlterConfigsResourceResponse', versions: '0+' },
    { name: 'Responses.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Responses.ResourceType', type: 'int8', versions: '0+' },
    { name: 'Responses.ResourceName', type: 'string', versions: '0+' },
  ],
}
