const { ShareEncoder: Encoder } = require('../../shareFetch/codec')
const { ShareAcknowledge: apiKey } = require('../../apiKeys')

/**
 * ShareAcknowledgeRequest version 2; all structures use compact fields and tag buffers.
 * Nested structures are explicit wire objects: supply all their fields, including empty arrays.
 * @param {object} [options] Wire fields; validation occurs when encode() runs.
 * @param {string|null} [options.groupId=null] The group identifier.
 * @param {string|null} [options.memberId=null] The member ID.
 * @param {number} [options.shareSessionEpoch=0] The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
 * @param {boolean} [options.isRenewAck=false] Whether Renew type acknowledgements present in AcknowledgementBatches.
 * @param {object[]} [options.topics=[]] The topics containing records to acknowledge.
 * @param {string} options.topics[].topicId The unique topic ID.
 * @param {object[]} options.topics[].partitions The partitions containing records to acknowledge.
 * @param {number} options.topics[].partitions[].partitionIndex The partition index.
 * @param {object[]} options.topics[].partitions[].acknowledgementBatches Record batches to acknowledge.
 * @param {string|bigint|number} options.topics[].partitions[].acknowledgementBatches[].firstOffset First offset of batch of records to acknowledge.
 * @param {string|bigint|number} options.topics[].partitions[].acknowledgementBatches[].lastOffset Last offset (inclusive) of batch of records to acknowledge.
 * @param {number[]} options.topics[].partitions[].acknowledgementBatches[].acknowledgeTypes Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
 * @returns {object} Descriptor with asynchronous encode().
 * @throws {KafkaJSNonRetriableError} from encode() for invalid field values.
 * @see clients/src/main/resources/common/message/ShareAcknowledgeRequest.json (Apache Kafka 4.4.0)
 */
module.exports = ({
  groupId = null,
  memberId = null,
  shareSessionEpoch = 0,
  isRenewAck = false,
  topics = [],
} = {}) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'ShareAcknowledge',
  encode: async () =>
    new Encoder()
      .writeCompactString(groupId, true)
      .writeCompactString(memberId, true)
      .writeInt32(shareSessionEpoch)
      .writeBoolean(isRenewAck)
      .writeCompactArray(topics, (value) => new Encoder().writeEncoder(encodeTopics(value)))
      .writeTaggedFields(),
})

/** @param {object} value @returns {Encoder} Encoded structure including its tag buffer. */
const encodeAcknowledgementBatches = (value) =>
  new Encoder()
    .writeOffset(value.firstOffset)
    .writeOffset(value.lastOffset)
    .writeCompactArray(value.acknowledgeTypes, (value) => new Encoder().writeInt8(value))
    .writeTaggedFields()

/** @param {object} value @returns {Encoder} Encoded structure including its tag buffer. */
const encodePartitions = (value) =>
  new Encoder()
    .writeInt32(value.partitionIndex)
    .writeCompactArray(value.acknowledgementBatches, (value) =>
      new Encoder().writeEncoder(encodeAcknowledgementBatches(value))
    )
    .writeTaggedFields()

/** @param {object} value @returns {Encoder} Encoded structure including its tag buffer. */
const encodeTopics = (value) =>
  new Encoder()
    .writeUuid(value.topicId)
    .writeCompactArray(value.partitions, (value) =>
      new Encoder().writeEncoder(encodePartitions(value))
    )
    .writeTaggedFields()

module.exports.schema = {
  api: 'ShareAcknowledge',
  version: 2,
  direction: 'request',
  fields: [
    {
      name: 'GroupId',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'MemberId',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'ShareSessionEpoch',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'IsRenewAck',
      type: 'bool',
      versions: '2+',
    },
    {
      name: 'Topics',
      type: '[]AcknowledgeTopic',
      versions: '0+',
    },
    {
      name: 'Topics.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'Topics.Partitions',
      type: '[]AcknowledgePartition',
      versions: '0+',
    },
    {
      name: 'Topics.Partitions.PartitionIndex',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Topics.Partitions.AcknowledgementBatches',
      type: '[]AcknowledgementBatch',
      versions: '0+',
    },
    {
      name: 'Topics.Partitions.AcknowledgementBatches.FirstOffset',
      type: 'int64',
      versions: '0+',
    },
    {
      name: 'Topics.Partitions.AcknowledgementBatches.LastOffset',
      type: 'int64',
      versions: '0+',
    },
    {
      name: 'Topics.Partitions.AcknowledgementBatches.AcknowledgeTypes',
      type: '[]int8',
      versions: '0+',
    },
  ],
}
