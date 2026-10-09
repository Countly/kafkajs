const v3 = require('../v3/response')

/**
 * TxnOffsetCommit Response (Version: 5) — flexible (KIP-482), response header v1
 *
 * The v3 body and decoded shape.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: v3.decode, parse: v3.parse }

module.exports.schema = { ...v3.schema, version: 5 }
