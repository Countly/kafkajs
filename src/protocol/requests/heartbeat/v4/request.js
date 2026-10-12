const Encoder = require('../../../encoder')
const { Heartbeat: apiKey } = require('../../apiKeys')

/**
 * Heartbeat Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * GroupId => COMPACT_STRING
 * GenerationId => INT32
 * MemberId => COMPACT_STRING
 * GroupInstanceId => COMPACT_NULLABLE_STRING
 * TAG_BUFFER
 *
 * v3's fields in compact framing.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/HeartbeatRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {number} options.groupGenerationId
 * @param {string} options.memberId
 * @param {string|null} [options.groupInstanceId=null] static membership id (KIP-345)
 * @returns {object} the protocol request
 */
module.exports = ({ groupId, groupGenerationId, memberId, groupInstanceId = null }) => ({
  apiKey,
  apiVersion: 4,
  apiName: 'Heartbeat',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(groupId)
      .writeInt32(groupGenerationId)
      .writeUVarIntString(memberId)
      .writeUVarIntString(groupInstanceId)
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'Heartbeat',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'GenerationId', type: 'int32', versions: '0+' },
    { name: 'MemberId', type: 'string', versions: '0+' },
    { name: 'GroupInstanceId', type: 'string', versions: '3+', nullable: true },
  ],
}
