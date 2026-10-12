const { decode, parse, schema } = require('../v2/response')

/**
 * CreatePartitions Response (Version: 3) — flexible (KIP-482), response header v1
 *
 * The v2 body, decoded and parsed as v2 is. `parse` throws THROTTLING_QUOTA_EXCEEDED (KIP-599),
 * the code version 3 may return, like any other topic error.
 * @see clients/src/main/resources/common/message/CreatePartitionsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = { ...schema, version: 3 }
