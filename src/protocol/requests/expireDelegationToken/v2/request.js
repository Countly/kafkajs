const Encoder = require('../../../encoder')
const { ExpireDelegationToken: apiKey } = require('../../apiKeys')

/**
 * ExpireDelegationTokenRequest (Version: 2) — flexible (KIP-482)
 *
 * Hmac => COMPACT_BYTES (0+)
 * ExpiryTimePeriodMs => INT64 (0+)
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ExpireDelegationTokenRequest.json (Apache Kafka 4.3.1)
 */
/**
 * Builds the negotiated wire request; validation and secret derivation belong to admin.
 * @param {object} fields normalized wire fields
 * @returns {object} request with an asynchronous encoder
 * @throws {Error} invalid wire values during encoding
 */
module.exports = ({ hmac, expiryTimePeriodMs }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'ExpireDelegationToken',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder().writeUVarIntBytes(hmac).writeInt64(expiryTimePeriodMs).writeTaggedFields()
  },
})

module.exports.schema = {
  api: 'ExpireDelegationToken',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'Hmac', type: 'bytes', versions: '0+' },
    { name: 'ExpiryTimePeriodMs', type: 'int64', versions: '0+' },
  ],
}
