const Encoder = require('../../../encoder')
const { DescribeCluster: apiKey } = require('../../apiKeys')
/** Builds a versioned body; capability validation is owned by Broker. @param {object} options wire fields @returns {object} protocol request @throws {Error} invalid encoding */
module.exports = ({
  includeClusterAuthorizedOperations = false,
  endpointType = 1,
  includeFencedBrokers = false,
} = {}) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'DescribeCluster',
  encode: async () =>
    new Encoder()
      .writeBoolean(includeClusterAuthorizedOperations)
      .writeInt8(endpointType)
      .writeBoolean(includeFencedBrokers)
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'DescribeCluster',
  version: 2,
  direction: 'request',
  fields: [
    {
      name: 'IncludeClusterAuthorizedOperations',
      type: 'bool',
      versions: '0+',
    },
    {
      name: 'EndpointType',
      type: 'int8',
      versions: '1+',
    },
    {
      name: 'IncludeFencedBrokers',
      type: 'bool',
      versions: '2+',
    },
  ],
}
