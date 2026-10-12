const requestV2 = require('../v2/request')

/**
 * DeleteTopics Request (Version: 3) => [topic_names] timeout_ms
 *   topic_names => STRING
 *   timeout_ms => INT32
 *
 * The v2 body. Version 3 only adds a code the broker may answer, TOPIC_DELETION_DISABLED.
 * @see clients/src/main/resources/common/message/DeleteTopicsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string[]} options.topics the names of the topics to delete
 * @param {number} [options.timeout=5000]
 * @returns {object} the protocol request
 */
module.exports = ({ topics, timeout }) =>
  Object.assign(requestV2({ topics, timeout }), { apiVersion: 3 })

module.exports.schema = { ...requestV2.schema, version: 3 }
