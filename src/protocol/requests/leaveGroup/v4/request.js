const Encoder = require('../../../encoder')
const { LeaveGroup: apiKey } = require('../../apiKeys')

/**
 * LeaveGroup Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * GroupId => COMPACT_STRING
 * Members => COMPACT_ARRAY
 *   MemberId => COMPACT_STRING
 *   GroupInstanceId => COMPACT_NULLABLE_STRING
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v3's fields in compact framing. v5 adds a per-member `Reason`, which this version never writes.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/LeaveGroupRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.groupId
 * @param {{memberId: string, groupInstanceId?: string|null, reason?: string|null}[]} options.members
 *   a member removed by its static instance id is `{ memberId: '', groupInstanceId }`
 * @returns {object} the protocol request
 */
module.exports = ({ groupId, members }) => buildRequest(4, groupId, members)

/**
 * @param {number} version 4 or 5
 * @param {string} groupId
 * @param {object[]} members as the version builder
 * @returns {object} the protocol request; `Reason` is written only at v5
 */
const buildRequest = (version, groupId, members) => ({
  apiKey,
  apiVersion: version,
  apiName: 'LeaveGroup',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(groupId)
      .writeUVarIntArray(members.map((member) => encodeMember(version, member)))
      .writeTaggedFields(),
})

const encodeMember = (version, { memberId, groupInstanceId = null, reason = null }) => {
  const member = new Encoder().writeUVarIntString(memberId).writeUVarIntString(groupInstanceId)

  if (version >= 5) {
    member.writeUVarIntString(reason)
  }

  return member.writeTaggedFields()
}

module.exports.buildRequest = buildRequest

module.exports.schema = {
  api: 'LeaveGroup',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'Members', type: '[]MemberIdentity', versions: '3+' },
    { name: 'Members.MemberId', type: 'string', versions: '3+' },
    { name: 'Members.GroupInstanceId', type: 'string', versions: '3+', nullable: true },
  ],
}
