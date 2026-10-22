const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * ListOffsets Response (Version: 6) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * Responses => COMPACT_ARRAY
 *   Topic => COMPACT_STRING
 *   PartitionResponses => COMPACT_ARRAY
 *     Partition => INT32
 *     ErrorCode => INT16
 *     Timestamp => INT64
 *     Offset => INT64
 *     LeaderEpoch => INT32 (default -1)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v6 is the flexible re-encoding of v5: compact strings/arrays and a tagged-field buffer at
 * every struct, no new fields. The decoded result stays byte-identical to v4/v5, including the
 * KIP-219 throttleTime/clientSideThrottleTime split.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
 * @see clients/src/main/resources/common/message/ListOffsetsResponse.json (Apache Kafka 4.3.1)
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const responses = Decoder.requireArray(
    decoder.readUVarIntArray(decodeResponses),
    'ListOffsetsResponse.Topics'
  )
  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    responses,
  }
}

const decodeResponses = (decoder) => {
  const response = {
    topic: decoder.readUVarIntString(),
    partitions: Decoder.requireArray(
      decoder.readUVarIntArray(decodePartitions),
      'ListOffsetsResponse.Topics.Partitions'
    ),
  }

  decoder.readTaggedFields()
  return response
}

const decodePartitions = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    timestamp: decoder.readInt64().toString(),
    offset: decoder.readInt64().toString(),
    leaderEpoch: decoder.readInt32(),
  }

  decoder.readTaggedFields()
  return partition
}

const parse = async (data) => {
  const partitionsWithError = data.responses.flatMap((response) =>
    response.partitions.filter((partition) => failure(partition.errorCode))
  )
  const partitionWithError = partitionsWithError[0]
  if (partitionWithError) {
    throw createErrorFromCode(partitionWithError.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ListOffsets',
  version: 6,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '2+' },
    { name: 'Topics', type: '[]ListOffsetsTopicResponse', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]ListOffsetsPartitionResponse', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Partitions.Timestamp', type: 'int64', versions: '1+' },
    { name: 'Topics.Partitions.Offset', type: 'int64', versions: '1+' },
    { name: 'Topics.Partitions.LeaderEpoch', type: 'int32', versions: '4+' },
  ],
}
