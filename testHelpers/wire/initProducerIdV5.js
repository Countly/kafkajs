const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { initProducerIdV5: expected } = require('./expected')

/**
 * InitProducerId v5 (flexible). The request names the transactional id (null for an idempotent
 * producer that has none), the transaction timeout and the producer id and epoch the client holds
 * (-1 for none); the response grants a producer id and epoch.
 *
 * Evidence: the response is Java's own bytes (`fixtures/v5_response_java.json`, bytes only, with a
 * capture `v5_response_kafka431.json` of the same shape) and `./expected` holds the fields of its
 * generator; the request is checked against Java's structured vectors
 * `initProducerIdRequest-v5-*.json`.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/InitProducerIdResponse.json
 */
module.exports = {
  api: 'InitProducerId',
  apiKey: apiKeys.InitProducerId,
  version: 5,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `errorCode`, `producerId` (a decimal
   *   string) and `producerEpoch`
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...expected.response, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeInt64(response.producerId)
      .writeInt16(response.producerEpoch)
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ transactionalId: string|null, transactionTimeout: number, producerId: string,
   *   producerEpoch: number }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      transactionalId: decoder.readUVarIntString(),
      transactionTimeout: decoder.readInt32(),
      producerId: decoder.readInt64().toString(),
      producerEpoch: decoder.readInt16(),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('InitProducerId v5 request has a byte after its tag buffer')
    }

    return request
  },
}
