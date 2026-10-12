const Encoder = require('../../../encoder')
const { SyncGroup: apiKey } = require('../../apiKeys')
const { schema: schemaV4 } = require('../v4/request')

/**
 * SyncGroup Request (Version: 5) — flexible (KIP-482), request header v2
 *
 * v4's fields with two more after `GroupInstanceId`:
 *
 * ProtocolType => COMPACT_NULLABLE_STRING
 * ProtocolName => COMPACT_NULLABLE_STRING
 *
 * The coordinator checks them against what it holds for the group and echoes both in the response.
 * Both are mandatory: a coordinator answers INCONSISTENT_GROUP_PROTOCOL to a v5 request with either
 * one null, so a caller sends v5 only with both set (the codec itself writes a missing one as null).
 * @see clients/src/main/java/org/apache/kafka/common/requests/SyncGroupRequest.java (Apache Kafka 4.3.1)
 * @see clients/src/main/resources/common/message/SyncGroupRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options as v4, plus the protocol the join selected
 * @param {string|null} [options.protocolType=null] the group's protocol type, `'consumer'`; a 4.x
 *   coordinator requires it
 * @param {string|null} [options.protocolName=null] the protocol name the JoinGroup selected; a 4.x
 *   coordinator requires it
 * @returns {object} the protocol request
 */
module.exports = ({
  groupId,
  generationId,
  memberId,
  groupInstanceId = null,
  protocolType = null,
  protocolName = null,
  groupAssignment,
}) => ({
  apiKey,
  apiVersion: 5,
  apiName: 'SyncGroup',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(groupId)
      .writeInt32(generationId)
      .writeUVarIntString(memberId)
      .writeUVarIntString(groupInstanceId)
      .writeUVarIntString(protocolType)
      .writeUVarIntString(protocolName)
      .writeUVarIntArray(groupAssignment.map(encodeAssignment))
      .writeTaggedFields(),
})

const encodeAssignment = ({ memberId, memberAssignment }) =>
  new Encoder().writeUVarIntString(memberId).writeUVarIntBytes(memberAssignment).writeTaggedFields()

module.exports.schema = {
  ...schemaV4,
  version: 5,
  fields: schemaV4.fields.flatMap((field) =>
    field.name === 'Assignments'
      ? [
          { name: 'ProtocolType', type: 'string', versions: '5+', nullable: true },
          { name: 'ProtocolName', type: 'string', versions: '5+', nullable: true },
          field,
        ]
      : [field]
  ),
}
