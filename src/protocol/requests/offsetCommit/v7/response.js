const { parse, decode } = require('../v4/response')

/**
 * OffsetCommitResponse (Version: 7) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 *
 * v7 adds no response fields (GroupInstanceId is request-only); the wire shape and decoded
 * result are byte-identical to v4.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-345
 * @see clients/src/main/resources/common/message/OffsetCommitResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'OffsetCommit',
  version: 7,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '3+' },
    { name: 'Topics', type: '[]OffsetCommitResponseTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0-9' },
    { name: 'Topics.Partitions', type: '[]OffsetCommitResponsePartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
