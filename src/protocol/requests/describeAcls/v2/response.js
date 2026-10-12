const Decoder = require('../../../decoder')
const { parse } = require('../v1/response')

/**
 * DescribeAcls Response (Version: 2) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * ErrorCode => INT16 (0+)
 * ErrorMessage => COMPACT_NULLABLE_STRING (0+)
 * Resources => COMPACT_ARRAY (0+)
 *   ResourceType => INT8 (0+)
 *   ResourceName => COMPACT_STRING (0+)
 *   PatternType => INT8 (1+)
 *   Acls => COMPACT_ARRAY (0+)
 *     Principal => COMPACT_STRING (0+)
 *     Host => COMPACT_STRING (0+)
 *     Operation => INT8 (0+)
 *     PermissionType => INT8 (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing, decoded to v1's keys; `parse` is v1's and throws a top-level
 * error. The broker's throttle time is reported as `clientSideThrottleTime` (KIP-219).
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeAclsResponse.json (Apache Kafka 4.3.1)
 */

const decodeAcls = (decoder) => {
  const acl = {
    principal: decoder.readUVarIntString(),
    host: decoder.readUVarIntString(),
    operation: decoder.readInt8(),
    permissionType: decoder.readInt8(),
  }

  decoder.readTaggedFields()
  return acl
}

const decodeResources = (decoder) => {
  const resource = {
    resourceType: decoder.readInt8(),
    resourceName: decoder.readUVarIntString(),
    resourcePatternType: decoder.readInt8(),
    acls: decoder.readUVarIntArray(decodeAcls),
  }

  decoder.readTaggedFields()
  return resource
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, errorCode: number,
 *   errorMessage: string|null, resources: object[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const resources = decoder.readUVarIntArray(decodeResources)

  decoder.readTaggedFields()
  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    errorMessage,
    resources,
  }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'DescribeAcls',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Resources', type: '[]DescribeAclsResource', versions: '0+' },
    { name: 'Resources.ResourceType', type: 'int8', versions: '0+' },
    { name: 'Resources.ResourceName', type: 'string', versions: '0+' },
    { name: 'Resources.PatternType', type: 'int8', versions: '1+' },
    { name: 'Resources.Acls', type: '[]AclDescription', versions: '0+' },
    { name: 'Resources.Acls.Principal', type: 'string', versions: '0+' },
    { name: 'Resources.Acls.Host', type: 'string', versions: '0+' },
    { name: 'Resources.Acls.Operation', type: 'int8', versions: '0+' },
    { name: 'Resources.Acls.PermissionType', type: 'int8', versions: '0+' },
  ],
}
