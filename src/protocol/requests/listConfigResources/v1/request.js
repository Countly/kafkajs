const Encoder = require('../../../encoder')
const { ListConfigResources: apiKey } = require('../../apiKeys')
/** Builds a versioned body; capability validation is owned by Broker. @param {object} options wire fields @returns {object} protocol request @throws {Error} invalid encoding */
module.exports = ({ resourceTypes = [] } = {}) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'ListConfigResources',
  encode: async () =>
    new Encoder()
      .writeUVarIntArray(
        resourceTypes === null ? null : resourceTypes.map((item) => new Encoder().writeInt8(item))
      )
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'ListConfigResources',
  version: 1,
  direction: 'request',
  fields: [
    {
      name: 'ResourceTypes',
      type: '[]int8',
      versions: '1+',
    },
  ],
}
