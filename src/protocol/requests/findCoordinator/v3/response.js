const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * FindCoordinatorResponse (Version: 3) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (1+)
 * ErrorCode => INT16 (0-3)
 * ErrorMessage => COMPACT_NULLABLE_STRING (1-3)
 * NodeId => INT32 (0-3)
 * Host => COMPACT_STRING (0-3)
 * Port => INT32 (0-3)
 * TAG_BUFFER
 *
 * v3 re-encodes v2's fields as compact with a trailing tag buffer; the decoded shape is
 * unchanged from v2.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/FindCoordinatorResponse.json (Apache Kafka 4.3.1)
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const errorMessage = decoder.readUVarIntString()
  const coordinator = {
    nodeId: decoder.readInt32(),
    host: decoder.readUVarIntString(),
    port: decoder.readInt32(),
  }

  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    errorMessage,
    coordinator,
  }
}

const parse = async (data) => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'FindCoordinator',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '0-3' },
    { name: 'ErrorMessage', type: 'string', versions: '1-3', nullable: true },
    { name: 'NodeId', type: 'int32', versions: '0-3' },
    { name: 'Host', type: 'string', versions: '0-3' },
    { name: 'Port', type: 'int32', versions: '0-3' },
  ],
}
