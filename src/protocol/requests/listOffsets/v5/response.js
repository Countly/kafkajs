const { decode, parse } = require('../v4/response')

/**
 * ListOffsets Response (Version: 5) => throttle_time_ms [responses]
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
 * v5 adds no fields; the wire shape and decoded result are byte-identical to v4.
 * @see clients/src/main/resources/common/message/ListOffsetsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ListOffsets',
  version: 5,
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
