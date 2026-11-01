const { request } = require('../codec')
/** Encodes DescribeLogDirs v5.
 *
 * @param {object} options request fields
 *
 * @returns {object} versioned request
 *
 * @throws {Error} invalid fields
 */
module.exports = (options) => request(5, options)

module.exports.schema = {
  api: 'DescribeLogDirs',
  version: 5,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]DescribableLogDirTopic', versions: '0+', nullable: true },
    { name: 'Topics.Topic', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]int32', versions: '0+' },
  ],
}
