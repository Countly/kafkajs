const Decoder = require('../../../decoder')
const { parse, schema } = require('../v0/response')

/**
 * IncrementalAlterConfigs Response (Version: 1) — flexible (KIP-482), response header v1
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
 * The v0 fields in compact framing, decoded to v0's keys.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/IncrementalAlterConfigsResponse.json (Apache Kafka 4.3.1)
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

module.exports.schema = { ...schema, version: 1 }
