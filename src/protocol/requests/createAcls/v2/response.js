const Decoder = require('../../../decoder')
const { parse } = require('../v1/response')

/**
 * CreateAcls Response (Version: 2) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * Results => COMPACT_ARRAY (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_NULLABLE_STRING (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing, decoded to v1's keys; `parse` is v1's and throws the error of
 * the first failed creation. The broker's throttle time is reported as `clientSideThrottleTime`
 * (KIP-219).
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/CreateAclsResponse.json (Apache Kafka 4.3.1)
 */

const decodeCreationResponse = (decoder) => {
  const creationResponse = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return creationResponse
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number,
 *   creationResponses: { errorCode: number, errorMessage: string|null }[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const creationResponses = decoder.readUVarIntArray(decodeCreationResponse) || []

  decoder.readTaggedFields()
  return { throttleTime: 0, clientSideThrottleTime: throttleTime, creationResponses }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'CreateAcls',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Results', type: '[]AclCreationResult', versions: '0+' },
    { name: 'Results.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Results.ErrorMessage', type: 'string', versions: '0+', nullable: true },
  ],
}
