const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')
const { KafkaJSAggregateError, KafkaJSCreateTopicError } = require('../../../../errors')

/**
 * CreateTopics Response (Version: 2) => throttle_time_ms [topic_errors]
 *   throttle_time_ms => INT32
 *   topic_errors => topic error_code error_message
 *     topic => STRING
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 */

const topicNameComparator = (a, b) => (a.topic ?? '').localeCompare(b.topic ?? '')

const topicErrors = (decoder) => ({
  topic: decoder.readString(),
  errorCode: decoder.readInt16(),
  errorMessage: decoder.readString(),
})

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  return {
    throttleTime: decoder.readInt32(),
    topicErrors: decoder.readArray(topicErrors).sort(topicNameComparator),
  }
}

const parse = async (data) => {
  const topicsWithError = data.topicErrors.filter(({ errorCode }) => failure(errorCode))
  if (topicsWithError.length > 0) {
    throw new KafkaJSAggregateError(
      'Topic creation errors',
      topicsWithError.map(
        (error) => new KafkaJSCreateTopicError(createErrorFromCode(error.errorCode), error.topic)
      )
    )
  }

  return data
}

module.exports = {
  decode,
  parse,
}
