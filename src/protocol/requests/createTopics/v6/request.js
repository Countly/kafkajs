const requestV5 = require('../v5/request')

/**
 * CreateTopics Request (Version: 6) — flexible (KIP-482), request header v2
 *
 * The v5 body. Version 6 only changes what the broker may answer: THROTTLING_QUOTA_EXCEEDED (KIP-599).
 * @see clients/src/main/resources/common/message/CreateTopicsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options the options of the v5 request
 * @returns {object} the protocol request
 */
module.exports = ({ topics, validateOnly, timeout }) =>
  Object.assign(requestV5({ topics, validateOnly, timeout }), { apiVersion: 6 })

module.exports.schema = { ...requestV5.schema, version: 6 }
