const { ShareEncoder: Encoder } = require('../../shareFetch/codec')
const { ShareFetch: apiKey } = require('../../apiKeys')

/**
 * ShareFetchRequest version 2; all structures use compact fields and tag buffers.
 * Nested structures are explicit wire objects: supply all their fields, including empty arrays.
 * @param {object} [options] Wire fields; validation occurs when encode() runs.
 * @param {string|null} [options.groupId=null] The group identifier.
 * @param {string|null} [options.memberId=null] The member ID.
 * @param {number} [options.shareSessionEpoch=0] The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
 * @param {number} [options.maxWaitMs=0] The maximum time in milliseconds to wait for the response.
 * @param {number} [options.minBytes=0] The minimum bytes to accumulate in the response.
 * @param {number} [options.maxBytes=0x7fffffff] The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
 * @param {number} [options.maxRecords=0] The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
 * @param {number} [options.batchSize=0] The optimal number of records for batches of acquired records and acknowledgements.
 * @param {number} [options.shareAcquireMode=0] The acquire mode to control the fetch behavior - 0:batch-optimized,1:record-limit.
 * @param {boolean} [options.isRenewAck=false] Whether Renew type acknowledgements present in AcknowledgementBatches.
 * @param {object[]} [options.topics=[]] The topics to fetch.
 * @param {string} options.topics[].topicId The unique topic ID.
 * @param {object[]} options.topics[].partitions The partitions to fetch.
 * @param {number} options.topics[].partitions[].partitionIndex The partition index.
 * @param {object[]} options.topics[].partitions[].acknowledgementBatches Record batches to acknowledge.
 * @param {string|bigint|number} options.topics[].partitions[].acknowledgementBatches[].firstOffset First offset of batch of records to acknowledge.
 * @param {string|bigint|number} options.topics[].partitions[].acknowledgementBatches[].lastOffset Last offset (inclusive) of batch of records to acknowledge.
 * @param {number[]} options.topics[].partitions[].acknowledgementBatches[].acknowledgeTypes Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
 * @param {object[]} [options.forgottenTopicsData=[]] The partitions to remove from this share session.
 * @param {string} options.forgottenTopicsData[].topicId The unique topic ID.
 * @param {number[]} options.forgottenTopicsData[].partitions The partitions indexes to forget.
 * @returns {object} Descriptor with asynchronous encode().
 * @throws {KafkaJSNonRetriableError} from encode() for invalid field values.
 * @see clients/src/main/resources/common/message/ShareFetchRequest.json (Apache Kafka 4.4.0)
 */
module.exports = ({
  groupId = null,
  memberId = null,
  shareSessionEpoch = 0,
  maxWaitMs = 0,
  minBytes = 0,
  maxBytes = 0x7fffffff,
  maxRecords = 0,
  batchSize = 0,
  shareAcquireMode = 0,
  isRenewAck = false,
  topics = [],
  forgottenTopicsData = [],
} = {}) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'ShareFetch',
  encode: async () =>
    new Encoder()
      .writeCompactString(groupId, true)
      .writeCompactString(memberId, true)
      .writeInt32(shareSessionEpoch)
      .writeInt32(maxWaitMs)
      .writeInt32(minBytes)
      .writeInt32(maxBytes)
      .writeInt32(maxRecords)
      .writeInt32(batchSize)
      .writeInt8(shareAcquireMode)
      .writeBoolean(isRenewAck)
      .writeCompactArray(topics, (value) => new Encoder().writeEncoder(encodeTopics(value)))
      .writeCompactArray(forgottenTopicsData, (value) =>
        new Encoder().writeEncoder(encodeForgottenTopicsData(value))
      )
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

/** @param {object} value @returns {Encoder} Encoded structure including its tag buffer. */
const encodeForgottenTopicsData = (value) =>
  new Encoder()
    .writeUuid(value.topicId)
    .writeCompactArray(value.partitions, (value) => new Encoder().writeInt32(value))
    .writeTaggedFields()

module.exports.schema = {
  api: 'ShareFetch',
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
      name: 'MaxWaitMs',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'MinBytes',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'MaxBytes',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'MaxRecords',
      type: 'int32',
      versions: '1+',
    },
    {
      name: 'BatchSize',
      type: 'int32',
      versions: '1+',
    },
    {
      name: 'ShareAcquireMode',
      type: 'int8',
      versions: '2+',
    },
    {
      name: 'IsRenewAck',
      type: 'bool',
      versions: '2+',
    },
    {
      name: 'Topics',
      type: '[]FetchTopic',
      versions: '0+',
    },
    {
      name: 'Topics.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'Topics.Partitions',
      type: '[]FetchPartition',
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
    {
      name: 'ForgottenTopicsData',
      type: '[]ForgottenTopic',
      versions: '0+',
    },
    {
      name: 'ForgottenTopicsData.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'ForgottenTopicsData.Partitions',
      type: '[]int32',
      versions: '0+',
    },
  ],
}
