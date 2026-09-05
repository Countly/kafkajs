const Encoder = require('../../../encoder')
const { GroupCoordinator: apiKey } = require('../../apiKeys')

/**
 * FindCoordinatorRequest (Version: 4) — flexible (KIP-482), request header v2
 *
 * KeyType => INT8 (1+)
 * CoordinatorKeys => COMPACT_ARRAY of COMPACT_STRING (4+)
 * TAG_BUFFER
 *
 * From v4 the request can batch several coordinator lookups into one call (KIP-699). This
 * client always looks up a single key, so `CoordinatorKeys` carries exactly one element.
 * `CoordinatorKeys` is `[]string`, not an array of structs, so each element carries no tag
 * buffer of its own.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/FindCoordinatorRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.coordinatorKey
 * @param {number} options.coordinatorType
 */
module.exports = ({ coordinatorKey, coordinatorType }) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'GroupCoordinator',
  encode: async () => {
    return new Encoder()
      .writeInt8(coordinatorType)
      .writeUVarIntArray([new Encoder().writeUVarIntString(coordinatorKey)])
      .writeTaggedFields()
  },
})

module.exports.schema = {
  api: 'FindCoordinator',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'KeyType', type: 'int8', versions: '1+' },
    { name: 'CoordinatorKeys', type: '[]string', versions: '4+' },
  ],
}
