const { decode } = require('../codec')
/** Decodes AlterReplicaLogDirs v2, preserving numeric errors as data.
 *
 * @param {Buffer} bytes response body
 *
 * @returns {Promise<object>} decoded response
 *
 * @throws {Error} malformed body
 */
module.exports.decode = (bytes) => decode(2, bytes)
/** Preserves decoded protocol results for family classification.
 *
 * @param {object} data decoded result
 *
 * @returns {Promise<object>} unchanged result
 */
module.exports.parse = async (data) => data

module.exports.schema = {
  api: 'AlterReplicaLogDirs',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Results', type: '[]AlterReplicaLogDirTopicResult', versions: '0+' },
    { name: 'Results.TopicName', type: 'string', versions: '0+' },
    { name: 'Results.Partitions', type: '[]AlterReplicaLogDirPartitionResult', versions: '0+' },
    { name: 'Results.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Results.Partitions.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
