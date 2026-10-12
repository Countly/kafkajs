const requestV2 = require('../v2/request')

/**
 * CreatePartitions Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * The v2 body, so an absent or empty `assignments` is still the compact null. Version 3 only adds
 * a code the broker may answer, THROTTLING_QUOTA_EXCEEDED (KIP-599).
 * @see clients/src/main/resources/common/message/CreatePartitionsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options the options of the v2 request
 * @returns {object} the protocol request
 */
module.exports = ({ topicPartitions, validateOnly, timeout }) =>
  Object.assign(requestV2({ topicPartitions, validateOnly, timeout }), { apiVersion: 3 })

module.exports.schema = { ...requestV2.schema, version: 3 }
