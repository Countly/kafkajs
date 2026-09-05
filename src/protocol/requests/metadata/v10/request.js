const Encoder = require('../../../encoder')
const { Metadata: apiKey } = require('../../apiKeys')

/**
 * MetadataRequest (Version: 10) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY (0+, nullable)
 *   TopicId => UUID (10+)
 *   Name => COMPACT_STRING (0+, nullable from 10)
 *   TAG_BUFFER
 * AllowAutoTopicCreation => BOOLEAN (4+)
 * IncludeClusterAuthorizedOperations => BOOLEAN (8-10)
 * IncludeTopicAuthorizedOperations => BOOLEAN (8+)
 * TAG_BUFFER
 *
 * This client always asks by topic name, so TopicId is written as the all-zero "no uuid"
 * sentinel and Name is never null. An empty topic list is encoded as the null array, which the
 * broker reads as "every topic" — the behaviour of every earlier version of this codec.
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
  apiVersion: 10,
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

const encodeTopic = (topic) =>
  new Encoder().writeUuid(null).writeUVarIntString(topic).writeTaggedFields()

module.exports.schema = {
  api: 'Metadata',
  version: 10,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]MetadataRequestTopic', versions: '0+', nullable: true },
    { name: 'Topics.TopicId', type: 'uuid', versions: '10+' },
    { name: 'Topics.Name', type: 'string', versions: '0+', nullable: true },
    { name: 'AllowAutoTopicCreation', type: 'bool', versions: '4+' },
    { name: 'IncludeClusterAuthorizedOperations', type: 'bool', versions: '8-10' },
    { name: 'IncludeTopicAuthorizedOperations', type: 'bool', versions: '8+' },
  ],
}
