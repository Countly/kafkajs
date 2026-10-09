const { decode, parse } = require('../v1/response')

/**
 * TxnOffsetCommit Response (Version: 2) => throttle_time_ms [topics]
 *   throttle_time_ms => INT32
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index error_code
 *       partition_index => INT32
 *       error_code => INT16
 *
 * The v1 body, decoded into v1's shape,
 * `{ throttleTime: 0, clientSideThrottleTime, topics: [{ topic, partitions: [{ partition, errorCode }] }] }`.
 * @see clients/src/main/resources/common/message/TxnOffsetCommitResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = {
  api: 'TxnOffsetCommit',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Topics', type: '[]TxnOffsetCommitResponseTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]TxnOffsetCommitResponsePartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
