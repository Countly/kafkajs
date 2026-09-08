const Encoder = require('../../../encoder')
const { ConsumerGroupDescribe: apiKey } = require('../../apiKeys')

/**
 * ConsumerGroupDescribeRequest (Version: 0) — flexible (KIP-482), request header v2
 *
 * GroupIds => COMPACT_ARRAY of COMPACT_STRING
 * IncludeAuthorizedOperations => BOOLEAN
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ConsumerGroupDescribeRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string[]} [options.groupIds=[]] the groups to describe; defaults to none so the
 *   protocol map can build this request with `{}` to read its api key, name and version
 * @param {boolean} [options.includeAuthorizedOperations=false]
 */
module.exports = ({ groupIds = [], includeAuthorizedOperations = false }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ConsumerGroupDescribe',
  encode: async () =>
    new Encoder()
      .writeUVarIntArray(groupIds.map((groupId) => new Encoder().writeUVarIntString(groupId)))
      .writeBoolean(includeAuthorizedOperations)
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'ConsumerGroupDescribe',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'GroupIds', type: '[]string', versions: '0+' },
    { name: 'IncludeAuthorizedOperations', type: 'bool', versions: '0+' },
  ],
}
