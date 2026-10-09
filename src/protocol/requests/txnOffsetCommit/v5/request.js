const requestV3 = require('../v3/request')

/**
 * TxnOffsetCommit Request (Version: 5) — flexible (KIP-482), request header v2
 *
 * The v3 body. Under transaction version 2 (KIP-890 part 2) it also adds the offsets to the
 * transaction, so no AddOffsetsToTxn precedes it.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options the v3 builder's
 * @returns {object} the protocol request
 */
module.exports = (options) => Object.assign(requestV3(options), { apiVersion: 5 })

module.exports.schema = { ...requestV3.schema, version: 5 }
