const Encoder = require('../../../encoder')
const { CreateAcls: apiKey } = require('../../apiKeys')

/**
 * CreateAcls Request (Version: 2) — flexible (KIP-482), request header v2
 *
 * Creations => COMPACT_ARRAY (0+)
 *   ResourceType => INT8 (0+)
 *   ResourceName => COMPACT_STRING (0+)
 *   ResourcePatternType => INT8 (1+)
 *   Principal => COMPACT_STRING (0+)
 *   Host => COMPACT_STRING (0+)
 *   Operation => INT8 (0+)
 *   PermissionType => INT8 (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing; v3 sends the same bytes.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/CreateAclsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ resourceType: number, resourceName: string, resourcePatternType: number,
 *   principal: string, host: string, operation: number, permissionType: number }[]} options.creations
 * @returns {object} the protocol request
 */
module.exports = ({ creations }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'CreateAcls',
  encode: async () => {
    return new Encoder().writeUVarIntArray(creations.map(encodeCreations)).writeTaggedFields()
  },
})

const encodeCreations = ({
  resourceType,
  resourceName,
  resourcePatternType,
  principal,
  host,
  operation,
  permissionType,
}) => {
  return new Encoder()
    .writeInt8(resourceType)
    .writeUVarIntString(resourceName)
    .writeInt8(resourcePatternType)
    .writeUVarIntString(principal)
    .writeUVarIntString(host)
    .writeInt8(operation)
    .writeInt8(permissionType)
    .writeTaggedFields()
}

module.exports.schema = {
  api: 'CreateAcls',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'Creations', type: '[]AclCreation', versions: '0+' },
    { name: 'Creations.ResourceType', type: 'int8', versions: '0+' },
    { name: 'Creations.ResourceName', type: 'string', versions: '0+' },
    { name: 'Creations.ResourcePatternType', type: 'int8', versions: '1+' },
    { name: 'Creations.Principal', type: 'string', versions: '0+' },
    { name: 'Creations.Host', type: 'string', versions: '0+' },
    { name: 'Creations.Operation', type: 'int8', versions: '0+' },
    { name: 'Creations.PermissionType', type: 'int8', versions: '0+' },
  ],
}
