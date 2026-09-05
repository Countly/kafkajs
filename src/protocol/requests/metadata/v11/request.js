const Encoder = require('../../../encoder')
const { Metadata: apiKey } = require('../../apiKeys')

/**
 * MetadataRequest (Version: 11) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY (0+, nullable)
 *   TopicId => UUID (10+)
 *   Name => COMPACT_STRING (0+, nullable from 10)
 *   TAG_BUFFER
 * AllowAutoTopicCreation => BOOLEAN (4+)
 * IncludeTopicAuthorizedOperations => BOOLEAN (8+)
 * TAG_BUFFER
 *
 * IncludeClusterAuthorizedOperations' range closed at v10 (KIP-700 moved cluster-authorized-ops
 * to DescribeCluster), so v11 no longer writes it. This client always asks by topic name, so
 * TopicId is written as the all-zero "no uuid" sentinel and Name is never null. An empty topic
 * list is encoded as the null array, which the broker reads as "every topic".
 * @see clients/src/main/resources/common/message/MetadataRequest.json
 *
 * @param {object} options
 * @param {string[]} options.topics
 * @param {boolean} [options.allowAutoTopicCreation=true]
 * @param {boolean} [options.includeTopicAuthorizedOperations=false]
 */
module.exports = ({
  topics,
  allowAutoTopicCreation = true,
  includeTopicAuthorizedOperations = false,
}) => ({
  apiKey,
  apiVersion: 11,
  apiName: 'Metadata',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(topics.length === 0 ? null : topics.map(encodeTopic))
      .writeBoolean(allowAutoTopicCreation)
      .writeBoolean(includeTopicAuthorizedOperations)
      .writeTaggedFields()
  },
})

const encodeTopic = (topic) =>
  new Encoder().writeUuid(null).writeUVarIntString(topic).writeTaggedFields()

module.exports.schema = {
  api: 'Metadata',
  version: 11,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]MetadataRequestTopic', versions: '0+', nullable: true },
    { name: 'Topics.TopicId', type: 'uuid', versions: '10+' },
    { name: 'Topics.Name', type: 'string', versions: '0+', nullable: true },
    { name: 'AllowAutoTopicCreation', type: 'bool', versions: '4+' },
    { name: 'IncludeTopicAuthorizedOperations', type: 'bool', versions: '8+' },
  ],
}
