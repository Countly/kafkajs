const { parse, decode } = require('../v4/response')

/**
 * OffsetCommitResponse (Version: 6) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 *
 * v6 adds no response fields (CommittedLeaderEpoch is request-only, for fencing); the wire
 * shape and decoded result are byte-identical to v4.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetCommitResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'OffsetCommit',
  version: 6,
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
