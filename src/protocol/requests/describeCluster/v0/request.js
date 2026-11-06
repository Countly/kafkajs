const Encoder = require('../../../encoder')
const { DescribeCluster: apiKey } = require('../../apiKeys')
/** Builds a versioned body; capability validation is owned by Broker. @param {object} options wire fields @returns {object} protocol request @throws {Error} invalid encoding */
module.exports = ({ includeClusterAuthorizedOperations = false } = {}) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'DescribeCluster',
  encode: async () =>
    new Encoder().writeBoolean(includeClusterAuthorizedOperations).writeTaggedFields(),
})

module.exports.schema = {
  api: 'DescribeCluster',
  version: 0,
  direction: 'request',
  fields: [
    {
      name: 'IncludeClusterAuthorizedOperations',
      type: 'bool',
      versions: '0+',
    },
  ],
}
