const { decode, parse } = require('../v3/response')

/**
 * CreateTopics Response (Version: 4) => throttle_time_ms [topics]
 *   throttle_time_ms => INT32
 *   topics => name error_code error_message
 *     name => STRING
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *
 * The v3 body, decoded and parsed as v3 is: the topics sorted by name, and a failed topic thrown
 * as one `KafkaJSCreateTopicError` in a `KafkaJSAggregateError`.
 * @see clients/src/main/resources/common/message/CreateTopicsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = {
  api: 'CreateTopics',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '2+' },
    { name: 'Topics', type: '[]CreatableTopicResult', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.ErrorMessage', type: 'string', versions: '1+', nullable: true },
  ],
}
