const Decoder = require('../../../decoder')
const { failIfVersionNotSupported } = require('../../../error')
const { parse } = require('../v0/response')

/**
 * Heartbeat Response (Version: 4) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * TAG_BUFFER
 *
 * v3's fields in compact framing, decoded to v3's keys.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/HeartbeatResponse.json (Apache Kafka 4.3.1)
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)
  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, errorCode }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Heartbeat',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
  ],
}
