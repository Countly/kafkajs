const Decoder = require('../../../decoder')
const { failIfVersionNotSupported } = require('../../../error')
const { parse } = require('../v0/response')

/**
 * EndTxn Response (Version: 5) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * ProducerId => INT64
 * ProducerEpoch => INT16
 * TAG_BUFFER
 *
 * v4's shape plus `producerId` (a decimal string, as every int64 this client decodes) and
 * `producerEpoch`: the pair the coordinator moved the producer to when it ended the transaction
 * (KIP-890 part 2). Both fields are in every v5 body; a coordinator that has no pair to report
 * sends their defaults, -1 and -1, which the caller must not adopt.
 * @see clients/src/main/resources/common/message/EndTxnResponse.json (Apache Kafka 4.3.1)
 *
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTime: number, clientSideThrottleTime: number, errorCode: number,
 *   producerId: string, producerEpoch: number }>}
 * @throws {import('../../../../errors').KafkaJSProtocolError} UNSUPPORTED_VERSION
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

module.exports = { decode, parse }

module.exports.schema = {
  api: 'EndTxn',
  version: 5,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '5+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '5+' },
  ],
}
