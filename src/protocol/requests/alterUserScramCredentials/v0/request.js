const Encoder = require('../../../encoder')
const { AlterUserScramCredentials: apiKey } = require('../../apiKeys')

/**
 * AlterUserScramCredentialsRequest (Version: 0) — flexible (KIP-482)
 *
 * Deletions => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   Mechanism => INT8 (0+)
 *   TAG_BUFFER
 * Upsertions => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   Mechanism => INT8 (0+)
 *   Iterations => INT32 (0+)
 *   Salt => COMPACT_BYTES (0+)
 *   SaltedPassword => COMPACT_BYTES (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/AlterUserScramCredentialsRequest.json (Apache Kafka 4.3.1)
 */
/**
 * Builds the negotiated wire request; validation and secret derivation belong to admin.
 * @param {object} fields normalized wire fields
 * @returns {object} request with an asynchronous encoder
 * @throws {Error} invalid wire values during encoding
 */
module.exports = ({ deletions, upsertions }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'AlterUserScramCredentials',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(deletions.map(encodeDeletions))
      .writeUVarIntArray(upsertions.map(encodeUpsertions))
      .writeTaggedFields()
  },
})

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeDeletions = ({ name, mechanism }) => {
  return new Encoder().writeUVarIntString(name).writeInt8(mechanism).writeTaggedFields()
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeUpsertions = ({ name, mechanism, iterations, salt, saltedPassword }) => {
  return new Encoder()
    .writeUVarIntString(name)
    .writeInt8(mechanism)
    .writeInt32(iterations)
    .writeUVarIntBytes(salt)
    .writeUVarIntBytes(saltedPassword)
    .writeTaggedFields()
}

module.exports.schema = {
  api: 'AlterUserScramCredentials',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'Deletions', type: '[]ScramCredentialDeletion', versions: '0+' },
    { name: 'Deletions.Name', type: 'string', versions: '0+' },
    { name: 'Deletions.Mechanism', type: 'int8', versions: '0+' },
    { name: 'Upsertions', type: '[]ScramCredentialUpsertion', versions: '0+' },
    { name: 'Upsertions.Name', type: 'string', versions: '0+' },
    { name: 'Upsertions.Mechanism', type: 'int8', versions: '0+' },
    { name: 'Upsertions.Iterations', type: 'int32', versions: '0+' },
    { name: 'Upsertions.Salt', type: 'bytes', versions: '0+' },
    { name: 'Upsertions.SaltedPassword', type: 'bytes', versions: '0+' },
  ],
}
