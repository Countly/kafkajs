const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * ListOffsets Response (Version: 4) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code timestamp offset leader_epoch
 *       partition => INT32
 *       error_code => INT16
 *       timestamp => INT64
 *       offset => INT64
 *       leader_epoch => INT32
 *
 * v4 adds LeaderEpoch after Offset - the epoch of the leader that produced the returned offset -
 * decoded additively; every other key stays byte-identical to v3. Quota-violation responses
 * still arrive before the broker throttles (KIP-219), so throttleTime is zeroed and the wire
 * value kept as clientSideThrottleTime, exactly as v3 does.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation
 * @see clients/src/main/resources/common/message/ListOffsetsResponse.json (Apache Kafka 4.3.1)
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    responses: decoder.readArray(decodeResponses),
  }
}

const decodeResponses = (decoder) => ({
  topic: decoder.readString(),
  partitions: decoder.readArray(decodePartitions),
})

const decodePartitions = (decoder) => ({
  partition: decoder.readInt32(),
  errorCode: decoder.readInt16(),
  timestamp: decoder.readInt64().toString(),
  offset: decoder.readInt64().toString(),
  leaderEpoch: decoder.readInt32(),
})

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
  version: 4,
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
