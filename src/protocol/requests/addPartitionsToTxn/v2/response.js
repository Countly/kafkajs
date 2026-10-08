const { decode, parse } = require('../v1/response')

/**
 * AddPartitionsToTxn Response (Version: 2) => throttle_time_ms [results_by_topic]
 *   throttle_time_ms => INT32
 *   results_by_topic => name [results_by_partition]
 *     name => STRING
 *     results_by_partition => partition_index partition_error_code
 *       partition_index => INT32
 *       partition_error_code => INT16
 *
 * The v1 body, decoded into v1's shape — `{ throttleTime: 0, clientSideThrottleTime, errors:
 * [{ topic, partitionErrors: [{ partition, errorCode }] }] }` — so a caller reads every version the
 * same way. v2 adds PRODUCER_FENCED to the codes a partition may carry.
 * @see clients/src/main/resources/common/message/AddPartitionsToTxnResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode, parse }

module.exports.schema = {
  api: 'AddPartitionsToTxn',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ResultsByTopicV3AndBelow', type: '[]AddPartitionsToTxnTopicResult', versions: '0-3' },
    { name: 'ResultsByTopicV3AndBelow.Name', type: 'string', versions: '0+' },
    {
      name: 'ResultsByTopicV3AndBelow.ResultsByPartition',
      type: '[]AddPartitionsToTxnPartitionResult',
      versions: '0+',
    },
    {
      name: 'ResultsByTopicV3AndBelow.ResultsByPartition.PartitionIndex',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'ResultsByTopicV3AndBelow.ResultsByPartition.PartitionErrorCode',
      type: 'int16',
      versions: '0+',
    },
  ],
}
