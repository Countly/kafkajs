const Encoder = require('../../../encoder')
const { Metadata: apiKey } = require('../../apiKeys')

/**
 * Metadata Request (Version: 0) => [topics]
 *   topics => STRING
 */

module.exports = ({ topics }) => ({
  apiKey,
  apiVersion: 0,
  apiName: 'Metadata',
  encode: async () => {
    return new Encoder().writeArray(topics)
  },
})

module.exports.schema = {
  api: 'Metadata',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'Topics', type: '[]MetadataRequestTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
  ],
}
