const requestV4 = require('../v4/request')

/**
 * DeleteTopics Request (Version: 5) — flexible (KIP-482), request header v2
 *
 * The v4 body. Version 5 only adds `ErrorMessage` to the response.
 * @see clients/src/main/resources/common/message/DeleteTopicsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string[]} options.topics the names of the topics to delete
 * @param {number} [options.timeout=5000]
 * @returns {object} the protocol request
 */
module.exports = ({ topics, timeout }) =>
  Object.assign(requestV4({ topics, timeout }), { apiVersion: 5 })

module.exports.schema = { ...requestV4.schema, version: 5 }
