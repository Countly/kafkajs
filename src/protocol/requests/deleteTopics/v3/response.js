const { decode, parse, schema } = require('../v2/response')

/**
 * DeleteTopics Response (Version: 3) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => name error_code
 *     name => STRING
 *     error_code => INT16
 *
 * The v2 body, decoded and parsed as v2 is. `parse` throws TOPIC_DELETION_DISABLED, the code
 * version 3 may return, like any other topic error.
 * @see clients/src/main/resources/common/message/DeleteTopicsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = { ...schema, version: 3 }
