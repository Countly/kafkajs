const { ShareEncoder: Encoder } = require('../../shareFetch/codec')
const { ShareGroupDescribe: apiKey } = require('../../apiKeys')

/**
 * ShareGroupDescribeRequest version 1; all structures use compact fields and tag buffers.
 * Nested structures are explicit wire objects: supply all their fields, including empty arrays.
 * @param {object} [options] Wire fields; validation occurs when encode() runs.
 * @param {string[]} [options.groupIds=[]] The ids of the groups to describe.
 * @param {boolean} [options.includeAuthorizedOperations=false] Whether to include authorized operations.
 * @returns {object} Descriptor with asynchronous encode().
 * @throws {KafkaJSNonRetriableError} from encode() for invalid field values.
 * @see clients/src/main/resources/common/message/ShareGroupDescribeRequest.json (Apache Kafka 4.4.0)
 */
module.exports = ({ groupIds = [], includeAuthorizedOperations = false } = {}) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'ShareGroupDescribe',
  encode: async () =>
    new Encoder()
      .writeCompactArray(groupIds, (value) => new Encoder().writeCompactString(value))
      .writeBoolean(includeAuthorizedOperations)
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'ShareGroupDescribe',
  version: 1,
  direction: 'request',
  fields: [
    {
      name: 'GroupIds',
      type: '[]string',
      versions: '0+',
    },
    {
      name: 'IncludeAuthorizedOperations',
      type: 'bool',
      versions: '0+',
    },
  ],
}
