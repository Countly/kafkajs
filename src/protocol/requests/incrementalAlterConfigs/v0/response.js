const Decoder = require('../../../decoder')

/**
 * IncrementalAlterConfigs Response (Version: 0) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => error_code error_message resource_type resource_name
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 *
 * The response has no top-level error code: each resource carries its own, and a caller reads it
 * from `resources`. The broker's throttle time is reported as `clientSideThrottleTime` (KIP-219).
 * @see clients/src/main/resources/common/message/IncrementalAlterConfigsResponse.json (Apache Kafka 4.3.1)
 */

const decodeResources = (decoder) => ({
  errorCode: decoder.readInt16(),
  errorMessage: decoder.readString(),
  resourceType: decoder.readInt8(),
  resourceName: decoder.readString(),
})

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number,
 *   resources: { errorCode: number, errorMessage: string|null, resourceType: number,
 *   resourceName: string }[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const resources = decoder.readArray(decodeResources)

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, resources }
}

/**
 * @param {object} data the decoded response
 * @returns {Promise<object>} `data` unchanged: a per-resource error is a result, never thrown
 */
const parse = async (data) => data

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'IncrementalAlterConfigs',
  version: 0,
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
