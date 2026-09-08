const Encoder = require('../../../encoder')
const { Metadata: apiKey } = require('../../apiKeys')

/**
 * MetadataRequest (Version: 13) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY (0+, nullable)
 *   TopicId => UUID (10+)
 *   Name => COMPACT_STRING (0+, nullable from 10)
 *   TAG_BUFFER
 * AllowAutoTopicCreation => BOOLEAN (4+)
 * IncludeTopicAuthorizedOperations => BOOLEAN (8+)
 * TAG_BUFFER
 *
 * The request is unchanged from v12; only the response gains a top-level ErrorCode.
 * A topic is requested by NAME - TopicId written as the all-zero "no uuid" sentinel, Name set -
 * or by ID, which writes the uuid and a null Name. Asking by id is how a topic that arrived in a
 * broker-computed assignment as a uuid gets resolved to a name. An empty topic list is encoded as
 * the null array, which the broker reads as "every topic" - the behaviour of every earlier
 * version of this codec.
 *
 * A request whose topics are ALL identified by id writes AllowAutoTopicCreation as false: such a
 * request names no topic, and a broker only auto-creates topics requested by name, so the flag
 * has nothing it could act on.
 * @see clients/src/main/resources/common/message/MetadataRequest.json
 *
 * @param {object} options
 * @param {Array<string|{topicId: string}>} options.topics each entry is a topic name or an
 *   object carrying a `topicId`; no other value is accepted
 * @param {boolean} [options.allowAutoTopicCreation=true]
 * @param {boolean} [options.includeTopicAuthorizedOperations=false]
 */
module.exports = ({
  topics,
  allowAutoTopicCreation = true,
  includeTopicAuthorizedOperations = false,
}) => ({
  apiKey,
  apiVersion: 13,
  apiName: 'Metadata',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(topics.length === 0 ? null : topics.map(encodeTopic))
      .writeBoolean(allowAutoTopicCreation && !isTopicIdOnly(topics))
      .writeBoolean(includeTopicAuthorizedOperations)
      .writeTaggedFields()
  },
})

/**
 * One requested topic, by name or by id.
 *
 * Asking by name writes the all-zero "no uuid" sentinel and the name, which is what every caller
 * before the consumer protocol did and what the committed golden vectors pin. Asking by id writes
 * the uuid and a NULL name - the form Name became nullable at v10 - and the only way to
 * resolve an id that arrived in a broker-computed assignment.
 *
 * @param {string|{topicId: string}} topic a topic name, or a topic id
 * @returns {Encoder}
 */
const encodeTopic = (topic) =>
  typeof topic === 'string'
    ? new Encoder().writeUuid(null).writeUVarIntString(topic).writeTaggedFields()
    : new Encoder().writeUuid(topic.topicId).writeUVarIntString(null).writeTaggedFields()

/**
 * @param {Array<string|{topicId: string}>} topics
 * @returns {boolean} whether every requested topic is identified by id alone. Such a request
 *   names no topic, and a broker auto-creates only topics requested by name, so the
 *   auto-creation flag has nothing to act on and is written as false rather than carried through
 *   as a value that cannot apply.
 */
const isTopicIdOnly = (topics) =>
  topics.length > 0 && topics.every((topic) => typeof topic !== 'string')

module.exports.schema = {
  api: 'Metadata',
  version: 13,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]MetadataRequestTopic', versions: '0+', nullable: true },
    { name: 'Topics.TopicId', type: 'uuid', versions: '10+' },
    { name: 'Topics.Name', type: 'string', versions: '0+', nullable: true },
    { name: 'AllowAutoTopicCreation', type: 'bool', versions: '4+' },
    { name: 'IncludeTopicAuthorizedOperations', type: 'bool', versions: '8+' },
  ],
}
