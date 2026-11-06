const Encoder = require('../../../encoder')
const { CreateDelegationToken: apiKey } = require('../../apiKeys')

/**
 * CreateDelegationTokenRequest (Version: 1)
 *
 * Renewers => ARRAY (0+)
 *   PrincipalType => STRING (0+)
 *   PrincipalName => STRING (0+)
 * MaxLifetimeMs => INT64 (0+)
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/CreateDelegationTokenRequest.json (Apache Kafka 4.3.1)
 */
/**
 * Builds the negotiated wire request; validation and secret derivation belong to admin.
 * @param {object} fields normalized wire fields
 * @returns {object} request with an asynchronous encoder
 * @throws {Error} invalid wire values during encoding
 */
module.exports = ({ renewers, maxLifetimeMs }) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'CreateDelegationToken',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder().writeArray(renewers.map(encodeRenewers)).writeInt64(maxLifetimeMs)
  },
})

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeRenewers = ({ principalType, principalName }) => {
  return new Encoder().writeString(principalType).writeString(principalName)
}

module.exports.schema = {
  api: 'CreateDelegationToken',
  version: 1,
  direction: 'request',
  fields: [
    { name: 'Renewers', type: '[]CreatableRenewers', versions: '0+' },
    { name: 'Renewers.PrincipalType', type: 'string', versions: '0+' },
    { name: 'Renewers.PrincipalName', type: 'string', versions: '0+' },
    { name: 'MaxLifetimeMs', type: 'int64', versions: '0+' },
  ],
}
