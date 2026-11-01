const { request } = require('../codec')
/** Encodes AlterReplicaLogDirs v1.
 *
 * @param {object} options request fields
 *
 * @returns {object} versioned request
 *
 * @throws {Error} invalid fields
 */
module.exports = (options) => request(1, options)

module.exports.schema = {
  api: 'AlterReplicaLogDirs',
  version: 1,
  direction: 'request',
  fields: [
    { name: 'Dirs', type: '[]AlterReplicaLogDir', versions: '0+' },
    { name: 'Dirs.Path', type: 'string', versions: '0+' },
    { name: 'Dirs.Topics', type: '[]AlterReplicaLogDirTopic', versions: '0+' },
    { name: 'Dirs.Topics.Name', type: 'string', versions: '0+' },
    { name: 'Dirs.Topics.Partitions', type: '[]int32', versions: '0+' },
  ],
}
