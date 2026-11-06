const Encoder = require('../../../encoder')
const { DescribeClientQuotas: apiKey } = require('../../apiKeys')

/**
 * DescribeClientQuotasRequest (Version: 0)
 *
 * Components => ARRAY (0+)
 *   EntityType => STRING (0+)
 *   MatchType => INT8 (0+)
 *   Match => STRING (0+)
 * Strict => BOOLEAN (0+)
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeClientQuotasRequest.json (Apache Kafka 4.3.1)
 */
/**
 * Builds the negotiated wire request; validation and secret derivation belong to admin.
 * @param {object} fields normalized wire fields
 * @returns {object} request with an asynchronous encoder
 * @throws {Error} invalid wire values during encoding
 */
module.exports = ({ components, strict }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DescribeClientQuotas',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder().writeArray(components.map(encodeComponents)).writeBoolean(strict)
  },
})

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeComponents = ({ entityType, matchType, match }) => {
  return new Encoder().writeString(entityType).writeInt8(matchType).writeString(match)
}

module.exports.schema = {
  api: 'DescribeClientQuotas',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'Components', type: '[]ComponentData', versions: '0+' },
    { name: 'Components.EntityType', type: 'string', versions: '0+' },
    { name: 'Components.MatchType', type: 'int8', versions: '0+' },
    { name: 'Components.Match', type: 'string', versions: '0+', nullable: true },
    { name: 'Strict', type: 'bool', versions: '0+' },
  ],
}
