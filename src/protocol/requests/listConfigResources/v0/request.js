const Encoder = require('../../../encoder')
const { ListConfigResources: apiKey } = require('../../apiKeys')
/** Builds a versioned body; capability validation is owned by Broker. @returns {object} protocol request @throws {Error} invalid encoding */
module.exports = () => ({
  apiKey,
  apiVersion: 0,
  apiName: 'ListConfigResources',
  encode: async () => new Encoder().writeTaggedFields(),
})

module.exports.schema = {
  api: 'ListConfigResources',
  version: 0,
  direction: 'request',
  fields: [],
}
