const requestV6 = require('../v6/request')

/**
 * CreateTopics Request (Version: 7) — flexible (KIP-482), request header v2
 *
 * The v6 body. Version 7 only changes the response, which gains the topic id.
 * @see clients/src/main/resources/common/message/CreateTopicsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options the options of the v5 request
 * @returns {object} the protocol request
 */
module.exports = ({ topics, validateOnly, timeout }) =>
  Object.assign(requestV6({ topics, validateOnly, timeout }), { apiVersion: 7 })

module.exports.schema = { ...requestV6.schema, version: 7 }
