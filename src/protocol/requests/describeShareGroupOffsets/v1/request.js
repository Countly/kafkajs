const { ShareEncoder: Encoder } = require('../../shareFetch/codec')
const { DescribeShareGroupOffsets: apiKey } = require('../../apiKeys')

/**
 * DescribeShareGroupOffsetsRequest version 1; all structures use compact fields and tag buffers.
 * Nested structures are explicit wire objects: supply all their fields, including empty arrays.
 * @param {object} [options] Wire fields; validation occurs when encode() runs.
 * @param {object[]} [options.groups=[]] The groups to describe offsets for.
 * @param {string} options.groups[].groupId The group identifier.
 * @param {object[]|null} options.groups[].topics The topics to describe offsets for, or null for all topic-partitions.
 * @param {string} options.groups[].topics[].topicName The topic name.
 * @param {number[]} options.groups[].topics[].partitions The partitions.
 * @returns {object} Descriptor with asynchronous encode().
 * @throws {KafkaJSNonRetriableError} from encode() for invalid field values.
 * @see clients/src/main/resources/common/message/DescribeShareGroupOffsetsRequest.json (Apache Kafka 4.4.0)
 */
module.exports = ({ groups = [] } = {}) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'DescribeShareGroupOffsets',
  encode: async () =>
    new Encoder()
      .writeCompactArray(groups, (value) => new Encoder().writeEncoder(encodeGroups(value)))
      .writeTaggedFields(),
})

/** @param {object} value @returns {Encoder} Encoded structure including its tag buffer. */
const encodeTopics = (value) =>
  new Encoder()
    .writeCompactString(value.topicName)
    .writeCompactArray(value.partitions, (value) => new Encoder().writeInt32(value))
    .writeTaggedFields()

/** @param {object} value @returns {Encoder} Encoded structure including its tag buffer. */
const encodeGroups = (value) =>
  new Encoder()
    .writeCompactString(value.groupId)
    .writeCompactArray(
      value.topics,
      (value) => new Encoder().writeEncoder(encodeTopics(value)),
      true
    )
    .writeTaggedFields()

module.exports.schema = {
  api: 'DescribeShareGroupOffsets',
  version: 1,
  direction: 'request',
  fields: [
    {
      name: 'Groups',
      type: '[]DescribeShareGroupOffsetsRequestGroup',
      versions: '0+',
    },
    {
      name: 'Groups.GroupId',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Topics',
      type: '[]DescribeShareGroupOffsetsRequestTopic',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'Groups.Topics.TopicName',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.Partitions',
      type: '[]int32',
      versions: '0+',
    },
  ],
}
