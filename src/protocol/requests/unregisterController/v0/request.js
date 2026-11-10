const Encoder = require('../../../encoder')
const { UnregisterController: apiKey } = require('../../apiKeys')

/**
 * UnregisterControllerRequest v0, flexible from v0 (request header 2, response header 1).
 * The id is not range checked here; `admin.unregisterController` validates it before any request.
 * @param {object} options Wire fields.
 * @param {number} options.controllerId The controller ID to unregister (int32).
 * @returns {{apiKey: number, apiVersion: number, apiName: string, encode: () => Promise<Encoder>}}
 *   Descriptor with asynchronous encode().
 * @see clients/src/main/resources/common/message/UnregisterControllerRequest.json (Apache Kafka 4.4.0)
 */
module.exports = ({ controllerId }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'UnregisterController',
  encode: async () => new Encoder().writeInt32(controllerId).writeTaggedFields(),
})

module.exports.schema = {
  api: 'UnregisterController',
  version: 0,
  direction: 'request',
  fields: [
    {
      name: 'ControllerId',
      type: 'int32',
      versions: '0+',
    },
  ],
}
