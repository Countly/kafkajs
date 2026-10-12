const requestV2 = require('../v2/request')

/**
 * DescribeAcls Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * The v2 body. Version 3 only lets the resource type be USER (7).
 * @see clients/src/main/resources/common/message/DescribeAclsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} filter the filter of the v2 request
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
}) =>
  Object.assign(
    requestV2({
      resourceType,
      resourceName,
      resourcePatternType,
      principal,
      host,
      operation,
      permissionType,
    }),
    { apiVersion: 3 }
  )

module.exports.schema = { ...requestV2.schema, version: 3 }
