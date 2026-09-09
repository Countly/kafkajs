const Encoder = require('../../../encoder')
const { KafkaJSNonRetriableError } = require('../../../../errors')
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
 * A topic is requested by NAME only: TopicId is written as the all-zero "no uuid" sentinel and
 * Name is set. `MetadataRequest.json` says of the `Topics` field that "version 10 adds topicId and
 * allows name field to be null. However, this functionality was not implemented on the server.
 * Versions 10 and 11 should not use the topicId field or set topic name to null. Version 12
 * supports topic Id." - so asking by id here would put a uuid on the wire that the server never
 * reads, and asking by id is refused rather than encoded. `Cluster#refreshMetadataForTopicIds`
 * falls back to a full refresh by name below v12. An empty topic list is encoded as the null
 * array, which the broker reads as "every topic" - the behaviour of every earlier version of this
 * codec.
 * @see clients/src/main/resources/common/message/MetadataRequest.json
 *
 * @param {object} options
 * @param {string[]} options.topics topic names; a topic id is not encodable at this version
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

/**
 * One requested topic, by name.
 *
 * Writes the all-zero "no uuid" sentinel and the name, which is what every caller of this codec
 * has always done and what the committed golden vector pins. An entry that is not a name is
 * refused: the server does not read `TopicId` below v12.
 *
 * @param {string} topic a topic name
 * @returns {Encoder}
 * @throws {KafkaJSNonRetriableError} when the entry identifies a topic by id
 */
const encodeTopic = (topic) => {
  if (typeof topic !== 'string') {
    throw new KafkaJSNonRetriableError(
      'Metadata v10 cannot request a topic by id; the server reads TopicId only from v12'
    )
  }

  return new Encoder().writeUuid(null).writeUVarIntString(topic).writeTaggedFields()
}

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
