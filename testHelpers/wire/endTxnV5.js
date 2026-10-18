const Encoder = require('../../src/protocol/encoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { readRequest } = require('./endTxnV4')
const vector = require('../../src/protocol/requests/endTxn/fixtures/java/endTxnResponse-v5-defaults.json')

/**
 * EndTxn v5 (flexible), the version a transaction under transaction version 2 sends (KIP-890 part
 * 2). The request is v4's; the response also carries the producer id and epoch the broker bumped
 * to (`-1` when it did not).
 *
 * Evidence: Java's structured vectors `endTxnRequest-v5-{commit,abort}.json` and
 * `endTxnResponse-v5-{defaults,producerIdAndEpoch,error}.json` (`hex` and `fields`).
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/EndTxnResponse.json
 */
module.exports = {
  api: 'EndTxn',
  apiKey: apiKeys.EndTxn,
  version: 5,

  /**
   * @param {object} [fields] over the vector's: `throttleTime`, `errorCode`, `producerId` (a decimal
   *   string) and `producerEpoch`
   * @returns {Buffer} the response body
   */
  buildResponse(fields = {}) {
    const response = { ...vector.fields, ...fields }

    return new Encoder()
      .writeInt32(response.throttleTime)
      .writeInt16(response.errorCode)
      .writeInt64(response.producerId)
      .writeInt16(response.producerEpoch)
      .writeTaggedFields().buffer
  },

  readRequest,
}
