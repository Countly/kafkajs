const Encoder = require('../../../encoder')
const { ApiVersions: apiKey } = require('../../apiKeys')
const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion } = require('../../../clientSoftware')

/**
 * ApiVersionsRequest (Version: 3) — flexible (KIP-482), request header v2
 *
 * ClientSoftwareName => COMPACT_STRING (3+)
 * ClientSoftwareVersion => COMPACT_STRING (3+)
 * TAG_BUFFER
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers
 * @see clients/src/main/resources/common/message/ApiVersionsRequest.json
 *
 * @param {object} [options]
 * @param {string} [options.clientSoftwareName]
 * @param {string} [options.clientSoftwareVersion]
 */
module.exports = ({
  clientSoftwareName = CLIENT_SOFTWARE_NAME,
  clientSoftwareVersion: softwareVersion = clientSoftwareVersion(),
} = {}) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'ApiVersions',
  encode: async () => {
    return new Encoder()
      .writeUVarIntString(clientSoftwareName)
      .writeUVarIntString(softwareVersion)
      .writeTaggedFields()
  },
})

module.exports.schema = {
  api: 'ApiVersions',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'ClientSoftwareName', type: 'string', versions: '3+' },
    { name: 'ClientSoftwareVersion', type: 'string', versions: '3+' },
  ],
}
