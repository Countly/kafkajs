const Encoder = require('../../../encoder')
const { GroupCoordinator: apiKey } = require('../../apiKeys')

/**
 * FindCoordinatorRequest (Version: 3) — flexible (KIP-482), request header v2
 *
 * Key => COMPACT_STRING (0-3)
 * KeyType => INT8 (1+)
 * TAG_BUFFER
 *
 * v3 re-encodes v2's fields as compact with a trailing tag buffer; nothing else changes.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/FindCoordinatorRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.coordinatorKey
 * @param {number} options.coordinatorType
 */
module.exports = ({ coordinatorKey, coordinatorType }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'GroupCoordinator',
  encode: async () => {
    return new Encoder()
      .writeUVarIntString(coordinatorKey)
      .writeInt8(coordinatorType)
      .writeTaggedFields()
  },
})

module.exports.schema = {
  api: 'FindCoordinator',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'Key', type: 'string', versions: '0-3' },
    { name: 'KeyType', type: 'int8', versions: '1+' },
  ],
}
