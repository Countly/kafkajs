const Encoder = require('../../../encoder')
const { RenewDelegationToken: apiKey } = require('../../apiKeys')

/**
 * RenewDelegationTokenRequest (Version: 1)
 *
 * Hmac => BYTES (0+)
 * RenewPeriodMs => INT64 (0+)
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/RenewDelegationTokenRequest.json (Apache Kafka 4.3.1)
 */
/**
 * Builds the negotiated wire request; validation and secret derivation belong to admin.
 * @param {object} fields normalized wire fields
 * @returns {object} request with an asynchronous encoder
 * @throws {Error} invalid wire values during encoding
 */
module.exports = ({ hmac, renewPeriodMs }) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'RenewDelegationToken',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder().writeBytes(hmac).writeInt64(renewPeriodMs)
  },
})

module.exports.schema = {
  api: 'RenewDelegationToken',
  version: 1,
  direction: 'request',
  fields: [
    { name: 'Hmac', type: 'bytes', versions: '0+' },
    { name: 'RenewPeriodMs', type: 'int64', versions: '0+' },
  ],
}
