const Decoder = require('../../../decoder')
const { failure, createErrorFromCode, failIfVersionNotSupported } = require('../../../error')

/**
 * InitProducerId Response (Version: 5) — flexible (KIP-482), response header v1
 *
 * Same wire shape as v2/v3/v4. Admits the new `TRANSACTION_ABORTABLE` (120) error code; this
 * client's error table already marks it non-retriable, so `parse` surfaces it as today's
 * `KafkaJSProtocolError`. This is the highest version this client implements — v6 (2PC,
 * KIP-939) is still `latestVersionUnstable` upstream.
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * ProducerId => INT64
 * ProducerEpoch => INT16
 * TAG_BUFFER
 *
 * The decoded shape stays `{ throttleTime: 0, clientSideThrottleTime, errorCode, producerId,
 * producerEpoch }`, so `eosManager` reads it unchanged.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/InitProducerIdResponse.json
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const producerId = decoder.readInt64().toString()
  const producerEpoch = decoder.readInt16()
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    producerId,
    producerEpoch,
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
  api: 'InitProducerId',
  version: 5,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
  ],
}
