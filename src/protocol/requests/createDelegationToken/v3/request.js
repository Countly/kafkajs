const Encoder = require('../../../encoder')
const { CreateDelegationToken: apiKey } = require('../../apiKeys')

/**
 * CreateDelegationTokenRequest (Version: 3) — flexible (KIP-482)
 *
 * OwnerPrincipalType => COMPACT_STRING (3+)
 * OwnerPrincipalName => COMPACT_STRING (3+)
 * Renewers => COMPACT_ARRAY (0+)
 *   PrincipalType => COMPACT_STRING (0+)
 *   PrincipalName => COMPACT_STRING (0+)
 *   TAG_BUFFER
 * MaxLifetimeMs => INT64 (0+)
 * TAG_BUFFER
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
module.exports = ({ ownerPrincipalType, ownerPrincipalName, renewers, maxLifetimeMs }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'CreateDelegationToken',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder()
      .writeUVarIntString(ownerPrincipalType)
      .writeUVarIntString(ownerPrincipalName)
      .writeUVarIntArray(renewers.map(encodeRenewers))
      .writeInt64(maxLifetimeMs)
      .writeTaggedFields()
  },
})

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeRenewers = ({ principalType, principalName }) => {
  return new Encoder()
    .writeUVarIntString(principalType)
    .writeUVarIntString(principalName)
    .writeTaggedFields()
}

module.exports.schema = {
  api: 'CreateDelegationToken',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'OwnerPrincipalType', type: 'string', versions: '3+', nullable: true },
    { name: 'OwnerPrincipalName', type: 'string', versions: '3+', nullable: true },
    { name: 'Renewers', type: '[]CreatableRenewers', versions: '0+' },
    { name: 'Renewers.PrincipalType', type: 'string', versions: '0+' },
    { name: 'Renewers.PrincipalName', type: 'string', versions: '0+' },
    { name: 'MaxLifetimeMs', type: 'int64', versions: '0+' },
  ],
}
