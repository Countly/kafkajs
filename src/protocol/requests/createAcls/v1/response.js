const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * CreateAcls Response (Version: 1) => throttle_time_ms [creation_responses]
 *   throttle_time_ms => INT32
 *   creation_responses => error_code error_message
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 */

const decodeCreationResponse = (decoder) => ({
  errorCode: decoder.readInt16(),
  errorMessage: decoder.readString(),
})

const decodeResponse = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const creationResponses = decoder.readArray(decodeCreationResponse)

  return {
    throttleTime,
    creationResponses,
  }
}

const decode = async (rawData) => {
  const decoded = await decodeResponse(rawData)

  return {
    ...decoded,
    throttleTime: 0,
    clientSideThrottleTime: decoded.throttleTime,
  }
}

const parse = async (data) => {
  const creationResponsesWithError = data.creationResponses.filter(({ errorCode }) =>
    failure(errorCode)
  )

  if (creationResponsesWithError.length > 0) {
    throw createErrorFromCode(creationResponsesWithError[0].errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}
