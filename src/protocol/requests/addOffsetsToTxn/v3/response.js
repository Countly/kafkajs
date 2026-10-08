const Decoder = require('../../../decoder')
const { failIfVersionNotSupported } = require('../../../error')
const { parse } = require('../v0/response')

/**
 * AddOffsetsToTxn Response (Version: 3) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * TAG_BUFFER
 *
 * Decoded into v1's shape, `{ throttleTime: 0, clientSideThrottleTime, errorCode }`; the broker's
 * throttle is reported as `clientSideThrottleTime` (KIP-219). UNSUPPORTED_VERSION is thrown while
 * decoding, as at every earlier version; any other error code is `parse`'s to throw.
 * @see clients/src/main/resources/common/message/AddOffsetsToTxnResponse.json (Apache Kafka 4.3.1)
 *
 * @param {Buffer} rawData
 * @returns {Promise<object>}
 * @throws {import('../../../../errors').KafkaJSProtocolError} UNSUPPORTED_VERSION
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)
  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, errorCode }
}

module.exports = { decode, parse }

module.exports.schema = {
  api: 'AddOffsetsToTxn',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
  ],
}
