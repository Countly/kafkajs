const Encoder = require('../../../encoder')
const { ApiVersions: apiKey } = require('../../apiKeys')
const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion } = require('../../../clientSoftware')

/**
 * ApiVersionsRequest (Version: 5) — flexible (KIP-482), request header v2, with the intended
 * broker identity (KIP-1242)
 *
 * ClientSoftwareName => COMPACT_STRING (3+)
 * ClientSoftwareVersion => COMPACT_STRING (3+)
 * ClusterId => COMPACT_NULLABLE_STRING (5+), default null
 * NodeId => INT32 (5+), default -1
 * TAG_BUFFER
 *
 * A broker accepts the identity only as a pair, so it is written only when both halves are known:
 * a cluster id and a node id of 0 or more. Anything else (no cluster id, an unknown or negative
 * node) writes the defaults for both, never one half; a half-known pair would be refused by the
 * broker as INVALID_REQUEST.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1242%3A+Rebootstrap+on+cluster+or+node+mismatch
 * @see clients/src/main/resources/common/message/ApiVersionsRequest.json (Apache Kafka 4.4.0)
 *
 * @param {object} [options]
 * @param {string} [options.clientSoftwareName]
 * @param {string} [options.clientSoftwareVersion]
 * @param {string|null} [options.clusterId=null] the cluster the client intends to reach
 * @param {number|null} [options.nodeId=-1] the node it intends to reach
 */
module.exports = ({
  clientSoftwareName = CLIENT_SOFTWARE_NAME,
  clientSoftwareVersion: softwareVersion = clientSoftwareVersion(),
  clusterId = null,
  nodeId = -1,
} = {}) => {
  const known = clusterId != null && Number.isInteger(nodeId) && nodeId >= 0

  return {
    apiKey,
    apiVersion: 5,
    apiName: 'ApiVersions',
    encode: async () => {
      return new Encoder()
        .writeUVarIntString(clientSoftwareName)
        .writeUVarIntString(softwareVersion)
        .writeUVarIntString(known ? clusterId : null)
        .writeInt32(known ? nodeId : -1)
        .writeTaggedFields()
    },
  }
}

module.exports.schema = {
  api: 'ApiVersions',
  version: 5,
  direction: 'request',
  fields: [
    { name: 'ClientSoftwareName', type: 'string', versions: '3+' },
    { name: 'ClientSoftwareVersion', type: 'string', versions: '3+' },
    { name: 'ClusterId', type: 'string', versions: '5+', nullable: true },
    { name: 'NodeId', type: 'int32', versions: '5+' },
  ],
}
