const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * DeleteTopics Response (Version: 1) => throttle_time_ms [topic_error_codes]
 *   throttle_time_ms => INT32
 *   topic_error_codes => topic error_code
 *     topic => STRING
 *     error_code => INT16
 */

const topicNameComparator = (a, b) => (a.topic ?? '').localeCompare(b.topic ?? '')

const topicErrors = (decoder) => ({
  topic: decoder.readString(),
  errorCode: decoder.readInt16(),
})

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
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
