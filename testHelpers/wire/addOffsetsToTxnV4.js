const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/addOffsetsToTxn/fixtures/java/addOffsetsToTxnResponse-v4-ok.json')

/**
 * AddOffsetsToTxn v4 (flexible). Under transaction version 1 a transaction names the consumer
 * group whose offsets it is about to commit before it sends them; the response is an error code.
 *
 * Evidence: Java's structured vectors `addOffsetsToTxnRequest-v4-basic.json` and
 * `addOffsetsToTxnResponse-v4-{ok,error}.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/AddOffsetsToTxnResponse.json
 */
module.exports = {
  api: 'AddOffsetsToTxn',
  apiKey: apiKeys.AddOffsetsToTxn,
  version: 4,

  /**
   * @param {object} [fields] over the vector's: `throttleTime` and `errorCode`
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeTaggedFields().buffer
  },

  /**
   * @param {Buffer} body the request body, after its header
   * @returns {{ transactionalId: string, producerId: string, producerEpoch: number, groupId:
   *   string }}
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      transactionalId: decoder.readUVarIntString(),
      producerId: decoder.readInt64().toString(),
      producerEpoch: decoder.readInt16(),
      groupId: decoder.readUVarIntString(),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('AddOffsetsToTxn v4 request has a byte after its tag buffer')
    }

    return request
  },
}
