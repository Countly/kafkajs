const Encoder = require('../../../encoder')
const { DescribeGroups: apiKey } = require('../../apiKeys')

const encodeGroupId = (groupId) => new Encoder().writeUVarIntString(groupId)

/**
 * DescribeGroupsRequest (Version: 5) — flexible (KIP-482), request header v2
 *
 * Groups => COMPACT_ARRAY of COMPACT_STRING (0+)
 * IncludeAuthorizedOperations => BOOLEAN (3+)
 * TAG_BUFFER
 *
 * v5 is the first flexible version; the fields are v3's, only the framing changed.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeGroupsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} [options]
 * @param {string[]} [options.groupIds=[]] the groups to describe; defaults to none so the version
 *   map can build this request with `{}` to read its api key, name and version
 * @param {boolean} [options.includeAuthorizedOperations=false]
 * @returns {object} the protocol request
 */
module.exports = ({ groupIds = [], includeAuthorizedOperations = false } = {}) => ({
  apiKey,
  apiVersion: 5,
  apiName: 'DescribeGroups',
  encode: async () =>
    new Encoder()
      .writeUVarIntArray(groupIds.map(encodeGroupId))
      .writeBoolean(includeAuthorizedOperations)
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'DescribeGroups',
  version: 5,
  direction: 'request',
  fields: [
    { name: 'Groups', type: '[]string', versions: '0+' },
    { name: 'IncludeAuthorizedOperations', type: 'bool', versions: '3+' },
  ],
}
