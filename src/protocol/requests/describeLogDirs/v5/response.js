const { decode } = require('../codec')
/** Decodes DescribeLogDirs v5, preserving numeric errors as data.
 *
 * @param {Buffer} bytes response body
 *
 * @returns {Promise<object>} decoded response
 *
 * @throws {Error} malformed body
 */
module.exports.decode = (bytes) => decode(5, bytes)
/** Preserves decoded protocol results for family classification.
 *
 * @param {object} data decoded result
 *
 * @returns {Promise<object>} unchanged result
 */
module.exports.parse = async (data) => data

module.exports.schema = {
  api: 'DescribeLogDirs',
  version: 5,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '3+' },
    { name: 'Results', type: '[]DescribeLogDirsResult', versions: '0+' },
    { name: 'Results.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Results.LogDir', type: 'string', versions: '0+' },
    { name: 'Results.Topics', type: '[]DescribeLogDirsTopic', versions: '0+' },
    { name: 'Results.Topics.Name', type: 'string', versions: '0+' },
    { name: 'Results.Topics.Partitions', type: '[]DescribeLogDirsPartition', versions: '0+' },
    { name: 'Results.Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Results.Topics.Partitions.PartitionSize', type: 'int64', versions: '0+' },
    { name: 'Results.Topics.Partitions.OffsetLag', type: 'int64', versions: '0+' },
    { name: 'Results.Topics.Partitions.IsFutureKey', type: 'bool', versions: '0+' },
    { name: 'Results.TotalBytes', type: 'int64', versions: '4+' },
    { name: 'Results.UsableBytes', type: 'int64', versions: '4+' },
    { name: 'Results.IsCordoned', type: 'bool', versions: '5+' },
  ],
}
