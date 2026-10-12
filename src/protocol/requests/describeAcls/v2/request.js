const Encoder = require('../../../encoder')
const { DescribeAcls: apiKey } = require('../../apiKeys')

/**
 * DescribeAcls Request (Version: 2) — flexible (KIP-482), request header v2
 *
 * ResourceTypeFilter => INT8 (0+)
 * ResourceNameFilter => COMPACT_NULLABLE_STRING (0+)
 * PatternTypeFilter => INT8 (1+)
 * PrincipalFilter => COMPACT_NULLABLE_STRING (0+)
 * HostFilter => COMPACT_NULLABLE_STRING (0+)
 * Operation => INT8 (0+)
 * PermissionType => INT8 (0+)
 * TAG_BUFFER
 *
 * The v1 fields in compact framing. A resource name, principal or host that is left out is the
 * compact null (0x00), which matches any.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeAclsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} filter
 * @param {number} filter.resourceType an `AclResourceTypes` value
 * @param {string|null} [filter.resourceName]
 * @param {number} filter.resourcePatternType a `ResourcePatternTypes` value
 * @param {string|null} [filter.principal]
 * @param {string|null} [filter.host]
 * @param {number} filter.operation an `AclOperationTypes` value
 * @param {number} filter.permissionType an `AclPermissionTypes` value
 * @returns {object} the protocol request
 */
module.exports = ({
  resourceType,
  resourceName,
  resourcePatternType,
  principal,
  host,
  operation,
  permissionType,
}) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'DescribeAcls',
  encode: async () => {
    return new Encoder()
      .writeInt8(resourceType)
      .writeUVarIntString(resourceName)
      .writeInt8(resourcePatternType)
      .writeUVarIntString(principal)
      .writeUVarIntString(host)
      .writeInt8(operation)
      .writeInt8(permissionType)
      .writeTaggedFields()
  },
})

module.exports.schema = {
  api: 'DescribeAcls',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'ResourceTypeFilter', type: 'int8', versions: '0+' },
    { name: 'ResourceNameFilter', type: 'string', versions: '0+', nullable: true },
    { name: 'PatternTypeFilter', type: 'int8', versions: '1+' },
    { name: 'PrincipalFilter', type: 'string', versions: '0+', nullable: true },
    { name: 'HostFilter', type: 'string', versions: '0+', nullable: true },
    { name: 'Operation', type: 'int8', versions: '0+' },
    { name: 'PermissionType', type: 'int8', versions: '0+' },
  ],
}
