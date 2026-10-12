const { decode, parse, parseTopLevel, schema: schemaV4 } = require('../v4/response')

/**
 * LeaveGroup Response (Version: 5) — flexible (KIP-482), response header v1
 *
 * Byte-identical to v4, so v4's `decode`, `parse` and `parseTopLevel` are re-exported.
 * @see clients/src/main/resources/common/message/LeaveGroupResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse, parseTopLevel }

module.exports.schema = { ...schemaV4, version: 5 }
