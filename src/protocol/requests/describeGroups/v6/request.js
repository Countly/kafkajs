const requestV5 = require('../v5/request')

/**
 * DescribeGroupsRequest (Version: 6) — flexible (KIP-482), request header v2
 *
 * v6 adds nothing to the request: the version exists so the broker knows it may answer
 * `GROUP_ID_NOT_FOUND` with a message for a group that is not there (KIP-1043). The bytes are
 * v5's.
 * @see clients/src/main/resources/common/message/DescribeGroupsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} [options] see v5
 * @returns {object} the protocol request
 */
module.exports = (options = {}) => Object.assign(requestV5(options), { apiVersion: 6 })

module.exports.schema = {
  api: 'DescribeGroups',
  version: 6,
  direction: 'request',
  fields: [
    { name: 'Groups', type: '[]string', versions: '0+' },
    { name: 'IncludeAuthorizedOperations', type: 'bool', versions: '3+' },
  ],
}
