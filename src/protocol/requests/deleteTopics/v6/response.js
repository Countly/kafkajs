const { buildDecode, parse } = require('../v4/response')

/**
 * DeleteTopics Response (Version: 6) — flexible (KIP-482), response header v1
 *
 * The v5 body with `Name` nullable and a `TopicId` (UUID) between the `Name` and the `ErrorCode` of
 * every result.
 * @see clients/src/main/resources/common/message/DeleteTopicsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(6), parse }

module.exports.schema = {
  api: 'DeleteTopics',
  version: 6,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'Responses', type: '[]DeletableTopicResult', versions: '0+' },
    { name: 'Responses.Name', type: 'string', versions: '0+', nullable: true },
    { name: 'Responses.TopicId', type: 'uuid', versions: '6+' },
    { name: 'Responses.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.ErrorMessage', type: 'string', versions: '5+', nullable: true },
  ],
}
