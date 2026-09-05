const Encoder = require('../../../encoder')
const { Metadata: apiKey } = require('../../apiKeys')

/**
 * MetadataRequest (Version: 8)
 *
 * Topics => ARRAY (0+, nullable)
 *   Name => STRING (0+)
 * AllowAutoTopicCreation => BOOLEAN (4+)
 * IncludeClusterAuthorizedOperations => BOOLEAN (8-10)
 * IncludeTopicAuthorizedOperations => BOOLEAN (8+)
 *
 * A null topic array means "every topic"; this client encodes an empty list that way, which is
 * the behaviour every earlier version of this codec has.
 * @see clients/src/main/resources/common/message/MetadataRequest.json
 *
 * @param {object} options
 * @param {string[]} options.topics
 * @param {boolean} [options.allowAutoTopicCreation=true]
 * @param {boolean} [options.includeClusterAuthorizedOperations=false]
 * @param {boolean} [options.includeTopicAuthorizedOperations=false]
 */
module.exports = ({
  topics,
  allowAutoTopicCreation = true,
  includeClusterAuthorizedOperations = false,
  includeTopicAuthorizedOperations = false,
}) => ({
  apiKey,
  apiVersion: 8,
  apiName: 'Metadata',
  encode: async () => {
    return new Encoder()
      .writeNullableArray(topics)
      .writeBoolean(allowAutoTopicCreation)
      .writeBoolean(includeClusterAuthorizedOperations)
      .writeBoolean(includeTopicAuthorizedOperations)
  },
})

module.exports.schema = {
  api: 'Metadata',
  version: 8,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]MetadataRequestTopic', versions: '0+', nullable: true },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'AllowAutoTopicCreation', type: 'bool', versions: '4+' },
    { name: 'IncludeClusterAuthorizedOperations', type: 'bool', versions: '8-10' },
    { name: 'IncludeTopicAuthorizedOperations', type: 'bool', versions: '8+' },
  ],
}
