const requestV0 = require('../v0/request')

/**
 * ConsumerGroupDescribeRequest (Version: 1) — flexible (KIP-482), request header v2
 *
 * v1's body is byte-identical to v0's: KIP-1099 adds `MemberType` to the *response* only, and
 * the message definition says so in as many words ("version 1 is same as version 0"). The
 * version travels in the header, so this reuses v0's encoder rather than duplicating it.
 * @see clients/src/main/resources/common/message/ConsumerGroupDescribeRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string[]} [options.groupIds=[]] the groups to describe
 * @param {boolean} [options.includeAuthorizedOperations=false]
 */
module.exports = (options) => Object.assign(requestV0(options), { apiVersion: 1 })

module.exports.schema = {
  api: 'ConsumerGroupDescribe',
  version: 1,
  direction: 'request',
  fields: [
    { name: 'GroupIds', type: '[]string', versions: '0+' },
    { name: 'IncludeAuthorizedOperations', type: 'bool', versions: '0+' },
  ],
}
