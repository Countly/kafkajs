const { decode, parse, schema } = require('../v5/response')

/**
 * CreateTopics Response (Version: 6) — flexible (KIP-482), response header v1
 *
 * The v5 body, decoded and parsed as v5 is. Version 6 only adds a code the broker may answer,
 * THROTTLING_QUOTA_EXCEEDED (KIP-599), which `parse` reports like any other topic error.
 * @see clients/src/main/resources/common/message/CreateTopicsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = { ...schema, version: 6 }
