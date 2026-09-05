const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * FindCoordinatorResponse (Version: 4) - flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (1+)
 * Coordinators => COMPACT_ARRAY (4+)
 *   Key => COMPACT_STRING, NodeId => INT32, Host => COMPACT_STRING, Port => INT32,
 *   ErrorCode => INT16, ErrorMessage => COMPACT_NULLABLE_STRING
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * From v4 the response is a list, because the request may ask for several coordinator keys. This
 * client asks for exactly one, so exactly one entry is expected, and the decoded shape is
 * flattened back to the single-coordinator form every caller in this client reads.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/FindCoordinatorResponse.json (Apache Kafka 4.3.1)
 */
const decodeCoordinator = (decoder) => {
  const coordinator = {
    key: decoder.readUVarIntString(),
    nodeId: decoder.readInt32(),
    host: decoder.readUVarIntString(),
    port: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return coordinator
}

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const coordinators = decoder.readUVarIntArray(decodeCoordinator) || []

  decoder.readTaggedFields()

  if (coordinators.length !== 1) {
    throw new KafkaJSPartialMessageError(
      `Expected exactly one coordinator in the response, received ${coordinators.length}`
    )
  }

  // `Key` is decoded and dropped: this client sent it, and adding it to the returned object
  // would be a new decoded key with no consumer.
  const [{ nodeId, host, port, errorCode, errorMessage }] = coordinators
  failIfVersionNotSupported(errorCode)

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    errorMessage,
    coordinator: { nodeId, host, port },
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
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'Coordinators', type: '[]Coordinator', versions: '4+' },
    { name: 'Coordinators.Key', type: 'string', versions: '4+' },
    { name: 'Coordinators.NodeId', type: 'int32', versions: '4+' },
    { name: 'Coordinators.Host', type: 'string', versions: '4+' },
    { name: 'Coordinators.Port', type: 'int32', versions: '4+' },
    { name: 'Coordinators.ErrorCode', type: 'int16', versions: '4+' },
    { name: 'Coordinators.ErrorMessage', type: 'string', versions: '4+', nullable: true },
  ],
}
