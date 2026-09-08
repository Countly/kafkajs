const requestV3 = require('../v3/request')

/**
 * DescribeGroupsRequest (Version: 4) — classic framing, request header v1
 *
 * v4 adds nothing to the request: the version exists so the broker knows it may report each
 * member's `GroupInstanceId` in the response (KIP-345). The bytes are v3's.
 * @see clients/src/main/resources/common/message/DescribeGroupsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} [options] see v3
 * @returns {object} the protocol request
 */
module.exports = (options = {}) => Object.assign(requestV3(options), { apiVersion: 4 })

module.exports.schema = {
  api: 'DescribeGroups',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'Groups', type: '[]string', versions: '0+' },
    { name: 'IncludeAuthorizedOperations', type: 'bool', versions: '3+' },
  ],
}
