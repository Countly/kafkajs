const requestV1 = require('../v1/request')

/**
 * DeleteTopics Request (Version: 2) => [topic_names] timeout_ms
 *   topic_names => STRING
 *   timeout_ms => INT32
 *
 * The v1 body. Version 2 only changes when the broker answers a throttled request (KIP-219).
 * @see clients/src/main/resources/common/message/DeleteTopicsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string[]} options.topics the names of the topics to delete
 * @param {number} [options.timeout=5000]
 * @returns {object} the protocol request
 */
module.exports = ({ topics, timeout }) =>
  Object.assign(requestV1({ topics, timeout }), { apiVersion: 2 })

module.exports.schema = {
  api: 'DeleteTopics',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'TopicNames', type: '[]string', versions: '0-5' },
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
  ],
}
