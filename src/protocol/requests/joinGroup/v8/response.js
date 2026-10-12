const { decode, parse, schema: schemaV7 } = require('../v7/response')

/**
 * JoinGroup Response (Version: 8) — flexible (KIP-482), response header v1
 *
 * Byte-identical to v7, so v7's `decode` and `parse` are re-exported. (v9 adds `SkipAssignment`
 * and is deliberately not implemented: the classic leader path stays as it is.)
 * @see clients/src/main/resources/common/message/JoinGroupResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = { ...schemaV7, version: 8 }
