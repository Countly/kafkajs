const Encoder = require('../../../encoder')
const { DescribeDelegationToken: apiKey } = require('../../apiKeys')

/**
 * DescribeDelegationTokenRequest (Version: 1)
 *
 * Owners => ARRAY (0+)
 *   PrincipalType => STRING (0+)
 *   PrincipalName => STRING (0+)
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeDelegationTokenRequest.json (Apache Kafka 4.3.1)
 */
/**
 * Builds the negotiated wire request; validation and secret derivation belong to admin.
 * @param {object} fields normalized wire fields
 * @returns {object} request with an asynchronous encoder
 * @throws {Error} invalid wire values during encoding
 */
module.exports = ({ owners }) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'DescribeDelegationToken',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder().writeArray(
      owners == null ? [] : owners.map(encodeOwners),
      undefined,
      owners == null ? -1 : owners.length
    )
  },
})

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeOwners = ({ principalType, principalName }) => {
  return new Encoder().writeString(principalType).writeString(principalName)
}

module.exports.schema = {
  api: 'DescribeDelegationToken',
  version: 1,
  direction: 'request',
  fields: [
    { name: 'Owners', type: '[]DescribeDelegationTokenOwner', versions: '0+', nullable: true },
    { name: 'Owners.PrincipalType', type: 'string', versions: '0+' },
    { name: 'Owners.PrincipalName', type: 'string', versions: '0+' },
  ],
}
