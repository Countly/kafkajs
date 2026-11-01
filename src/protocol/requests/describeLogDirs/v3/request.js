const { request } = require('../codec')
/** Encodes DescribeLogDirs v3.
 *
 * @param {object} options request fields
 *
 * @returns {object} versioned request
 *
 * @throws {Error} invalid fields
 */
module.exports = (options) => request(3, options)

module.exports.schema = {
  api: 'DescribeLogDirs',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]DescribableLogDirTopic', versions: '0+', nullable: true },
    { name: 'Topics.Topic', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]int32', versions: '0+' },
  ],
}
