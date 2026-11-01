const Encoder = require('../../../encoder')
const { DeleteTopics: apiKey } = require('../../apiKeys')

/**
 * DeleteTopics Request (Version: 6) — flexible (KIP-482), request header v2
 *
 * Topics => COMPACT_ARRAY (6+)
 *   Name => COMPACT_NULLABLE_STRING (6+)
 *   TopicId => UUID (6+)
 *   TAG_BUFFER
 * TimeoutMs => INT32 (0+)
 * TAG_BUFFER
 *
 * Names are sent with the zero
 * uuid, which is how the Java client deletes by name. A caller that needs a result for each name
 * reads it from the response's `topic`, never from its `topicId`, which is null (the zero uuid) for
 * a name the controller could not delete.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteTopicsRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string[]} [options.topics] the names of the topics to delete
 * @param {string[]} [options.topicIds] canonical UUIDs; each is sent with a null name
 * @param {number} [options.timeout=5000]
 * @returns {object} the protocol request
 */
module.exports = ({ topics, topicIds, timeout = 5000 }) => ({
  apiKey,
  apiVersion: 6,
  apiName: 'DeleteTopics',
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(
        topicIds === undefined
          ? topics.map(encodeTopic)
          : topicIds.map((id) =>
              new Encoder().writeUVarIntString(null).writeUuid(id).writeTaggedFields()
            )
      )
      .writeInt32(timeout)
      .writeTaggedFields()
  },
})

const encodeTopic = (name) => {
  return new Encoder().writeUVarIntString(name).writeUuid(null).writeTaggedFields()
}

module.exports.schema = {
  api: 'DeleteTopics',
  version: 6,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]DeleteTopicState', versions: '6+' },
    { name: 'Topics.Name', type: 'string', versions: '6+', nullable: true },
    { name: 'Topics.TopicId', type: 'uuid', versions: '6+' },
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
  ],
}
