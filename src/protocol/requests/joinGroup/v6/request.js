const Encoder = require('../../../encoder')
const { JoinGroup: apiKey } = require('../../apiKeys')

/**
 * JoinGroup Request (Version: 6) — flexible (KIP-482), request header v2
 *
 * GroupId => COMPACT_STRING
 * SessionTimeoutMs => INT32
 * RebalanceTimeoutMs => INT32
 * MemberId => COMPACT_STRING
 * GroupInstanceId => COMPACT_NULLABLE_STRING
 * ProtocolType => COMPACT_STRING
 * Protocols => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   Metadata => COMPACT_BYTES
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v5's fields in compact framing. v7 sends the same bytes; v8 adds a trailing `Reason`.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/JoinGroupRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {number} options.sessionTimeout
 * @param {number} options.rebalanceTimeout
 * @param {string} options.memberId
 * @param {string|null} [options.groupInstanceId=null] static membership id (KIP-345)
 * @param {string} options.protocolType
 * @param {{name: string, metadata?: Buffer}[]} options.groupProtocols
 * @returns {object} the protocol request
 */
module.exports = (options) => ({
  apiKey,
  apiVersion: 6,
  apiName: 'JoinGroup',
  encode: async () => body(options).writeTaggedFields(),
})

/**
 * Everything but the final tag buffer, which v8 writes after its `Reason`.
 * @param {object} options as the version builder
 * @returns {Encoder}
 */
const body = ({
  groupId,
  sessionTimeout,
  rebalanceTimeout,
  memberId,
  groupInstanceId = null,
  protocolType,
  groupProtocols,
}) =>
  new Encoder()
    .writeUVarIntString(groupId)
    .writeInt32(sessionTimeout)
    .writeInt32(rebalanceTimeout)
    .writeUVarIntString(memberId)
    .writeUVarIntString(groupInstanceId)
    .writeUVarIntString(protocolType)
    .writeUVarIntArray(groupProtocols.map(encodeProtocol))

const encodeProtocol = ({ name, metadata = Buffer.alloc(0) }) =>
  new Encoder().writeUVarIntString(name).writeUVarIntBytes(metadata).writeTaggedFields()

module.exports.body = body

module.exports.schema = {
  api: 'JoinGroup',
  version: 6,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'SessionTimeoutMs', type: 'int32', versions: '0+' },
    { name: 'RebalanceTimeoutMs', type: 'int32', versions: '1+' },
    { name: 'MemberId', type: 'string', versions: '0+' },
    { name: 'GroupInstanceId', type: 'string', versions: '5+', nullable: true },
    { name: 'ProtocolType', type: 'string', versions: '0+' },
    { name: 'Protocols', type: '[]JoinGroupRequestProtocol', versions: '0+' },
    { name: 'Protocols.Name', type: 'string', versions: '0+' },
    { name: 'Protocols.Metadata', type: 'bytes', versions: '0+' },
  ],
}
