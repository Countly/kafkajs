const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const vector = require('../../src/protocol/requests/endTxn/fixtures/java/endTxnResponse-v4-ok.json')

/**
 * EndTxn v4 (flexible), the version a transaction under transaction version 1 sends. The request
 * names the transactional id, producer id and epoch and whether to commit (`true`) or abort; the
 * response is an error code.
 *
 * Evidence: Java's structured vectors `endTxnRequest-v4-{commit,abort}.json` and
 * `endTxnResponse-v4-{ok,error}.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/EndTxnResponse.json
 */
module.exports = {
  api: 'EndTxn',
  apiKey: apiKeys.EndTxn,
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
   * @returns {{ transactionalId: string, producerId: string, producerEpoch: number,
   *   transactionResult: boolean }} `transactionResult` is true for a commit
   * @throws {Error} when the body is cut short or a byte follows its tag buffer
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      transactionalId: decoder.readUVarIntString(),
      producerId: decoder.readInt64().toString(),
      producerEpoch: decoder.readInt16(),
      transactionResult: decoder.readBoolean(),
    }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1)) {
      throw new Error('EndTxn v4 and v5 request has a byte after its tag buffer')
    }

    return request
  },
}
