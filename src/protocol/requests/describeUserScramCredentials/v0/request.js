const Encoder = require('../../../encoder')
const { DescribeUserScramCredentials: apiKey } = require('../../apiKeys')

/**
 * DescribeUserScramCredentialsRequest (Version: 0) — flexible (KIP-482)
 *
 * Users => COMPACT_ARRAY (0+)
 *   Name => COMPACT_STRING (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeUserScramCredentialsRequest.json (Apache Kafka 4.3.1)
 */
/**
 * Builds the negotiated wire request; validation and secret derivation belong to admin.
 * @param {object} fields normalized wire fields
 * @returns {object} request with an asynchronous encoder
 * @throws {Error} invalid wire values during encoding
 */
module.exports = ({ users }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DescribeUserScramCredentials',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(users == null ? null : users.map(encodeUsers))
      .writeTaggedFields()
  },
})

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeUsers = ({ name }) => {
  return new Encoder().writeUVarIntString(name).writeTaggedFields()
}

module.exports.schema = {
  api: 'DescribeUserScramCredentials',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'Users', type: '[]UserName', versions: '0+', nullable: true },
    { name: 'Users.Name', type: 'string', versions: '0+' },
  ],
}
