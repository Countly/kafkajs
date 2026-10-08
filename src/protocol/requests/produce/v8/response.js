const Decoder = require('../../../decoder')
const { parse } = require('../partitionResults')

/**
 * ProduceResponse (Version: 8)
 *
 * Responses => [Name [PartitionResponses]]
 *   Name => STRING
 *   PartitionResponses => Index ErrorCode BaseOffset LogAppendTimeMs LogStartOffset
 *                         [RecordErrors] ErrorMessage
 *     Index => INT32
 *     ErrorCode => INT16
 *     BaseOffset => INT64
 *     LogAppendTimeMs => INT64
 *     LogStartOffset => INT64
 *     RecordErrors => [BatchIndex BatchIndexErrorMessage]
 *       BatchIndex => INT32
 *       BatchIndexErrorMessage => NULLABLE_STRING
 *     ErrorMessage => NULLABLE_STRING
 * ThrottleTimeMs => INT32
 *
 * v8 adds `RecordErrors` and `ErrorMessage` (KIP-467); every earlier field is unchanged.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ProduceResponse.json (Apache Kafka 4.3.1)
 */

const decodeRecordError = (decoder) => ({
  batchIndex: decoder.readInt32(),
  batchIndexErrorMessage: decoder.readString(),
})

const decodePartition = (decoder) => ({
  partition: decoder.readInt32(),
  errorCode: decoder.readInt16(),
  baseOffset: decoder.readInt64().toString(),
  logAppendTime: decoder.readInt64().toString(),
  logStartOffset: decoder.readInt64().toString(),
  recordErrors: decoder.readArray(decodeRecordError),
  errorMessage: decoder.readString(),
})

/**
 * @param {Buffer} rawData
 * @returns {Promise<object>} decoded response, keyed the same as v7 plus additive
 *   `partition.recordErrors`/`partition.errorMessage`
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const topics = decoder.readArray((decoder) => ({
    topicName: decoder.readString(),
    partitions: decoder.readArray(decodePartition),
  }))

  const throttleTime = decoder.readInt32()

  return {
    topics,
    throttleTime,
  }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Produce',
  version: 8,
  direction: 'response',
  fields: [
    { name: 'Responses', type: '[]TopicProduceResponse', versions: '0+' },
    { name: 'Responses.Name', type: 'string', versions: '0-12' },
    { name: 'Responses.PartitionResponses', type: '[]PartitionProduceResponse', versions: '0+' },
    { name: 'Responses.PartitionResponses.Index', type: 'int32', versions: '0+' },
    { name: 'Responses.PartitionResponses.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.PartitionResponses.BaseOffset', type: 'int64', versions: '0+' },
    { name: 'Responses.PartitionResponses.LogAppendTimeMs', type: 'int64', versions: '2+' },
    { name: 'Responses.PartitionResponses.LogStartOffset', type: 'int64', versions: '5+' },
    {
      name: 'Responses.PartitionResponses.RecordErrors',
      type: '[]BatchIndexAndErrorMessage',
      versions: '8+',
    },
    { name: 'Responses.PartitionResponses.RecordErrors.BatchIndex', type: 'int32', versions: '8+' },
    {
      name: 'Responses.PartitionResponses.RecordErrors.BatchIndexErrorMessage',
      type: 'string',
      versions: '8+',
      nullable: true,
    },
    {
      name: 'Responses.PartitionResponses.ErrorMessage',
      type: 'string',
      versions: '8+',
      nullable: true,
    },
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
  ],
}
