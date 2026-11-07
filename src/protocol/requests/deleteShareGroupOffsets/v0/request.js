const { ShareEncoder: Encoder } = require('../../shareFetch/codec')
const { DeleteShareGroupOffsets: apiKey } = require('../../apiKeys')

/**
 * DeleteShareGroupOffsetsRequest version 0; all structures use compact fields and tag buffers.
 * Nested structures are explicit wire objects: supply all their fields, including empty arrays.
 * @param {object} [options] Wire fields; validation occurs when encode() runs.
 * @param {string} [options.groupId=''] The group identifier.
 * @param {object[]} [options.topics=[]] The topics to delete offsets for.
 * @param {string} options.topics[].topicName The topic name.
 * @returns {object} Descriptor with asynchronous encode().
 * @throws {KafkaJSNonRetriableError} from encode() for invalid field values.
 * @see clients/src/main/resources/common/message/DeleteShareGroupOffsetsRequest.json (Apache Kafka 4.4.0)
 */
module.exports = ({ groupId = '', topics = [] } = {}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DeleteShareGroupOffsets',
  encode: async () =>
    new Encoder()
      .writeCompactString(groupId)
      .writeCompactArray(topics, (value) => new Encoder().writeEncoder(encodeTopics(value)))
      .writeTaggedFields(),
})

/** @param {object} value @returns {Encoder} Encoded structure including its tag buffer. */
const encodeTopics = (value) =>
  new Encoder().writeCompactString(value.topicName).writeTaggedFields()

module.exports.schema = {
  api: 'DeleteShareGroupOffsets',
  version: 0,
  direction: 'request',
  fields: [
    {
      name: 'GroupId',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Topics',
      type: '[]DeleteShareGroupOffsetsRequestTopic',
      versions: '0+',
    },
    {
      name: 'Topics.TopicName',
      type: 'string',
      versions: '0+',
    },
  ],
}
