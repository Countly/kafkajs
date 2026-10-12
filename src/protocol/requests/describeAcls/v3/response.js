const { decode, parse, schema } = require('../v2/response')

/**
 * DescribeAcls Response (Version: 3) — flexible (KIP-482), response header v1
 *
 * The v2 body, decoded and parsed as v2 is. Version 3 only lets a resource type be USER (7).
 * @see clients/src/main/resources/common/message/DescribeAclsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = { ...schema, version: 3 }
