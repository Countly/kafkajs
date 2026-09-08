const Encoder = require('../../../encoder')
const { DescribeGroups: apiKey } = require('../../apiKeys')

/**
 * DescribeGroupsRequest (Version: 3) — classic framing, request header v1
 *
 * Groups => ARRAY of STRING (0+)
 * IncludeAuthorizedOperations => BOOLEAN (3+)
 *
 * v3 is where the request gains its only field beyond the group list: the broker answers with the
 * caller's authorized operations on each group when it is set (KIP-430).
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
  apiVersion: 3,
  apiName: 'DescribeGroups',
  encode: async () => new Encoder().writeArray(groupIds).writeBoolean(includeAuthorizedOperations),
})

module.exports.schema = {
  api: 'DescribeGroups',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'Groups', type: '[]string', versions: '0+' },
    { name: 'IncludeAuthorizedOperations', type: 'bool', versions: '3+' },
  ],
}
