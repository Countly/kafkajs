const Decoder = require('../../../decoder')
const { parse } = require('../v1/response')

/**
 * DeleteAcls Response (Version: 2) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * FilterResults => COMPACT_ARRAY (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_NULLABLE_STRING (0+)
 *   MatchingAcls => COMPACT_ARRAY (0+)
 *     ErrorCode => INT16 (0+)
 *     ErrorMessage => COMPACT_NULLABLE_STRING (0+)
 *     ResourceType => INT8 (0+)
 *     ResourceName => COMPACT_STRING (0+)
 *     PatternType => INT8 (1+)
 *     Principal => COMPACT_STRING (0+)
 *     Host => COMPACT_STRING (0+)
 *     Operation => INT8 (0+)
 *     PermissionType => INT8 (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing, decoded to v1's keys; `parse` is v1's and throws the error of
 * the first filter or matching ACL that failed. The broker's throttle time is reported as
 * `clientSideThrottleTime` (KIP-219).
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteAclsResponse.json (Apache Kafka 4.3.1)
 */

const decodeMatchingAcls = (decoder) => {
  const matchingAcl = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    resourceType: decoder.readInt8(),
    resourceName: decoder.readUVarIntString(),
    resourcePatternType: decoder.readInt8(),
    principal: decoder.readUVarIntString(),
    host: decoder.readUVarIntString(),
    operation: decoder.readInt8(),
    permissionType: decoder.readInt8(),
  }

  decoder.readTaggedFields()
  return matchingAcl
}

const decodeFilterResponse = (decoder) => {
  const filterResponse = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    matchingAcls: decoder.readUVarIntArray(decodeMatchingAcls),
  }

  decoder.readTaggedFields()
  return filterResponse
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number,
 *   filterResponses: object[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const filterResponses = decoder.readUVarIntArray(decodeFilterResponse)

  decoder.readTaggedFields()
  return { throttleTime: 0, clientSideThrottleTime: throttleTime, filterResponses }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'DeleteAcls',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'FilterResults', type: '[]DeleteAclsFilterResult', versions: '0+' },
    { name: 'FilterResults.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'FilterResults.ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'FilterResults.MatchingAcls', type: '[]DeleteAclsMatchingAcl', versions: '0+' },
    { name: 'FilterResults.MatchingAcls.ErrorCode', type: 'int16', versions: '0+' },
    {
      name: 'FilterResults.MatchingAcls.ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    { name: 'FilterResults.MatchingAcls.ResourceType', type: 'int8', versions: '0+' },
    { name: 'FilterResults.MatchingAcls.ResourceName', type: 'string', versions: '0+' },
    { name: 'FilterResults.MatchingAcls.PatternType', type: 'int8', versions: '1+' },
    { name: 'FilterResults.MatchingAcls.Principal', type: 'string', versions: '0+' },
    { name: 'FilterResults.MatchingAcls.Host', type: 'string', versions: '0+' },
    { name: 'FilterResults.MatchingAcls.Operation', type: 'int8', versions: '0+' },
    { name: 'FilterResults.MatchingAcls.PermissionType', type: 'int8', versions: '0+' },
  ],
}
