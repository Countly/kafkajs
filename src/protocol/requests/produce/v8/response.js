const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

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

/**
 * Produce responses report at most one failure to the caller: the first failing partition across
 * every topic in the response, which is the rule every version of this codec has used. From v8
 * the broker also sends a human-readable message for that partition and, when it dropped
 * individual records, the batch index of each one (KIP-467); both are attached to the thrown
 * error rather than changing the shape of a successful response.
 *
 * `recordErrors[].batchIndex` indexes the records **as grouped for that partition**, not the
 * caller's flat message list; the producer resolves each index to its record.
 *
 * @param {object} data decoded response
 * @returns {Promise<object>} data, when every partition succeeded
 * @throws {import('../../../../errors').KafkaJSProtocolError} enriched with topic, partition,
 *   errorMessage and recordErrors
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-467%3A+Augment+ProduceResponse+error+messaging+for+specific+culprit+records
 */
const parse = async (data) => {
  for (const response of data.topics) {
    for (const partition of response.partitions) {
      if (!failure(partition.errorCode)) {
        continue
      }

      const error = createErrorFromCode(partition.errorCode)
      error.topic = response.topicName
      error.partition = partition.partition
      error.errorMessage = partition.errorMessage
      error.recordErrors = partition.recordErrors.map(({ batchIndex, batchIndexErrorMessage }) => ({
        batchIndex,
        errorMessage: batchIndexErrorMessage,
      }))
      throw error
    }
  }

  return data
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
