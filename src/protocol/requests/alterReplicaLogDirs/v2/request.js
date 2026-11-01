const { request } = require('../codec')
/** Encodes AlterReplicaLogDirs v2.
 *
 * @param {object} options request fields
 *
 * @returns {object} versioned request
 *
 * @throws {Error} invalid fields
 */
module.exports = (options) => request(2, options)

module.exports.schema = {
  api: 'AlterReplicaLogDirs',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'Dirs', type: '[]AlterReplicaLogDir', versions: '0+' },
    { name: 'Dirs.Path', type: 'string', versions: '0+' },
    { name: 'Dirs.Topics', type: '[]AlterReplicaLogDirTopic', versions: '0+' },
    { name: 'Dirs.Topics.Name', type: 'string', versions: '0+' },
    { name: 'Dirs.Topics.Partitions', type: '[]int32', versions: '0+' },
  ],
}
