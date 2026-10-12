const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/*
 * CreatePartitions Response (Version: 0) => throttle_time_ms [topic_errors]
 *   throttle_time_ms => INT32
 *   topic_errors => topic error_code error_message
 *     topic => STRING
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 */

const topicNameComparator = (a, b) => a.topic.localeCompare(b.topic)

const topicErrors = (decoder) => ({
  topic: decoder.readString(),
  errorCode: decoder.readInt16(),
  errorMessage: decoder.readString(),
})

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  return {
    throttleTime,
    topicErrors: decoder.readArray(topicErrors).sort(topicNameComparator),
  }
}

/**
 * @param {{ topicErrors: { topic: string, errorCode: number }[] }} data the decoded response
 * @returns {Promise<object>} `data` unchanged when no topic failed
 * @throws {KafkaJSProtocolError} for the first failed topic; the error carries every failed row as
 *   `topicErrors`, so a caller can tell which topics a refusal left undone
 */
const parse = async (data) => {
  const topicsWithError = data.topicErrors.filter(({ errorCode }) => failure(errorCode))
  if (topicsWithError.length > 0) {
    throw Object.assign(createErrorFromCode(topicsWithError[0].errorCode), {
      topicErrors: topicsWithError,
    })
  }

  return data
}

module.exports = {
  decode,
  parse,
}
