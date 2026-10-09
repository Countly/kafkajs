const requestV3 = require('../v3/request')

/**
 * TxnOffsetCommit Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * The v3 body. v4 only lets the broker answer TRANSACTION_ABORTABLE (KIP-890). It is the highest
 * version a transaction may use while its coordinator runs transaction version 1.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options the v3 builder's
 * @returns {object} the protocol request
 */
module.exports = (options) => Object.assign(requestV3(options), { apiVersion: 4 })

module.exports.schema = { ...requestV3.schema, version: 4 }
