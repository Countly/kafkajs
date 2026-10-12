const Encoder = require('../../../encoder')
const { DeleteAcls: apiKey } = require('../../apiKeys')

/**
 * DeleteAcls Request (Version: 2) — flexible (KIP-482), request header v2
 *
 * Filters => COMPACT_ARRAY (0+)
 *   ResourceTypeFilter => INT8 (0+)
 *   ResourceNameFilter => COMPACT_NULLABLE_STRING (0+)
 *   PatternTypeFilter => INT8 (1+)
 *   PrincipalFilter => COMPACT_NULLABLE_STRING (0+)
 *   HostFilter => COMPACT_NULLABLE_STRING (0+)
 *   Operation => INT8 (0+)
 *   PermissionType => INT8 (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing; v3 sends the same bytes. A resource name, principal or host
 * that is left out is the compact null (0x00), which matches any.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteAclsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {{ resourceType: number, resourceName?: string|null, resourcePatternType: number,
 *   principal?: string|null, host?: string|null, operation: number,
 *   permissionType: number }[]} options.filters
 * @returns {object} the protocol request
 */
module.exports = ({ filters }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'DeleteAcls',
  encode: async () => {
    return new Encoder().writeUVarIntArray(filters.map(encodeFilters)).writeTaggedFields()
  },
})

const encodeFilters = ({
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
  api: 'DeleteAcls',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'Filters', type: '[]DeleteAclsFilter', versions: '0+' },
    { name: 'Filters.ResourceTypeFilter', type: 'int8', versions: '0+' },
    { name: 'Filters.ResourceNameFilter', type: 'string', versions: '0+', nullable: true },
    { name: 'Filters.PatternTypeFilter', type: 'int8', versions: '1+' },
    { name: 'Filters.PrincipalFilter', type: 'string', versions: '0+', nullable: true },
    { name: 'Filters.HostFilter', type: 'string', versions: '0+', nullable: true },
    { name: 'Filters.Operation', type: 'int8', versions: '0+' },
    { name: 'Filters.PermissionType', type: 'int8', versions: '0+' },
  ],
}
