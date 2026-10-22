const Decoder = require('../../../decoder')
const { parse } = require('../v8/response')

/**
 * ProduceResponse (Version: 9) — flexible (KIP-482), response header v1
 *
 * Responses => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   PartitionResponses => COMPACT_ARRAY
 *     Index => INT32
 *     ErrorCode => INT16
 *     BaseOffset => INT64
 *     LogAppendTimeMs => INT64
 *     LogStartOffset => INT64
 *     RecordErrors => COMPACT_ARRAY
 *       BatchIndex => INT32
 *       BatchIndexErrorMessage => COMPACT_NULLABLE_STRING
 *       TAG_BUFFER
 *     ErrorMessage => COMPACT_NULLABLE_STRING
 *     TAG_BUFFER
 *   TAG_BUFFER
 * ThrottleTimeMs => INT32
 * TAG_BUFFER
 *
 * Only the framing changes from v8 (compact strings/arrays, tag buffers on every struct); the
 * decoded shape and `parse`'s error-selection rule are identical, so `parse` is reused from v8.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ProduceResponse.json (Apache Kafka 4.3.1)
 */

const decodeRecordError = (decoder) => {
  const recordError = {
    batchIndex: decoder.readInt32(),
    batchIndexErrorMessage: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return recordError
}

const decodePartition = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    baseOffset: decoder.readInt64().toString(),
    logAppendTime: decoder.readInt64().toString(),
    logStartOffset: decoder.readInt64().toString(),
    recordErrors: Decoder.requireArray(
      decoder.readUVarIntArray(decodeRecordError),
      'ProduceResponse.Responses.PartitionResponses.RecordErrors'
    ),
    errorMessage: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return partition
}

const decodeTopic = (decoder) => {
  const topic = {
    topicName: decoder.readUVarIntString(),
    partitions: Decoder.requireArray(
      decoder.readUVarIntArray(decodePartition),
      'ProduceResponse.Responses.PartitionResponses'
    ),
  }

  decoder.readTaggedFields()
  return topic
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<object>} decoded response, keyed the same as v8
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const topics = Decoder.requireArray(
    decoder.readUVarIntArray(decodeTopic),
    'ProduceResponse.Responses'
  )
  const throttleTime = decoder.readInt32()
  decoder.readTaggedFields()

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
  version: 9,
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
