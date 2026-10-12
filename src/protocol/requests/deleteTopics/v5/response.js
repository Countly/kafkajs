const { buildDecode, parse } = require('../v4/response')

/**
 * DeleteTopics Response (Version: 5) — flexible (KIP-482), response header v1
 *
 * The v4 body plus `ErrorMessage` (COMPACT_NULLABLE_STRING) after the `ErrorCode` of every result.
 * Version 5 may also answer THROTTLING_QUOTA_EXCEEDED (KIP-599), which `parse` throws like any
 * other topic error.
 * @see clients/src/main/resources/common/message/DeleteTopicsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(5), parse }

module.exports.schema = {
  api: 'DeleteTopics',
  version: 5,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'Responses', type: '[]DeletableTopicResult', versions: '0+' },
    { name: 'Responses.Name', type: 'string', versions: '0+' },
    { name: 'Responses.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.ErrorMessage', type: 'string', versions: '5+', nullable: true },
  ],
}
