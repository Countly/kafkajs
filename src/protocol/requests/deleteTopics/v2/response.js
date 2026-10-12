const { decode, parse } = require('../v1/response')

/**
 * DeleteTopics Response (Version: 2) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => name error_code
 *     name => STRING
 *     error_code => INT16
 *
 * The v1 body, decoded and parsed as v1 is: the topics sorted by name, and the error of the first
 * failed topic thrown.
 * @see clients/src/main/resources/common/message/DeleteTopicsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = {
  api: 'DeleteTopics',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'Responses', type: '[]DeletableTopicResult', versions: '0+' },
    { name: 'Responses.Name', type: 'string', versions: '0+' },
    { name: 'Responses.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
