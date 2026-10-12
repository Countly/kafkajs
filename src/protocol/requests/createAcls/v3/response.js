const { decode, parse, schema } = require('../v2/response')

/**
 * CreateAcls Response (Version: 3) — flexible (KIP-482), response header v1
 *
 * The v2 body, decoded and parsed as v2 is.
 * @see clients/src/main/resources/common/message/CreateAclsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = { ...schema, version: 3 }
