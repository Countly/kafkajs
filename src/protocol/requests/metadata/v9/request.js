const Encoder = require('../../../encoder')
const { Metadata: apiKey } = require('../../apiKeys')

/**
 * MetadataRequest (Version: 9) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY (0+, nullable)
 *   Name => COMPACT_STRING (0+)
 *   TAG_BUFFER
 * AllowAutoTopicCreation => BOOLEAN (4+)
 * IncludeClusterAuthorizedOperations => BOOLEAN (8-10)
 * IncludeTopicAuthorizedOperations => BOOLEAN (8+)
 * TAG_BUFFER
 *
 * The first flexible Metadata version: every string and array becomes compact and the request
 * gains a top-level tag buffer plus one per topic entry. An empty topic list is still encoded as
 * the null compact array, which the broker reads as "every topic".
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
  apiVersion: 9,
  apiName: 'Metadata',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(topics.length === 0 ? null : topics.map(encodeTopic))
      .writeBoolean(allowAutoTopicCreation)
      .writeBoolean(includeClusterAuthorizedOperations)
      .writeBoolean(includeTopicAuthorizedOperations)
      .writeTaggedFields()
  },
})

const encodeTopic = (topic) => new Encoder().writeUVarIntString(topic).writeTaggedFields()

module.exports.schema = {
  api: 'Metadata',
  version: 9,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]MetadataRequestTopic', versions: '0+', nullable: true },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'AllowAutoTopicCreation', type: 'bool', versions: '4+' },
    { name: 'IncludeClusterAuthorizedOperations', type: 'bool', versions: '8-10' },
    { name: 'IncludeTopicAuthorizedOperations', type: 'bool', versions: '8+' },
  ],
}
