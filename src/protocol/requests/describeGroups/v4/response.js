const { buildDecode, parse } = require('../v3/response')

/**
 * DescribeGroupsResponse (Version: 4) — classic framing, response header v0
 *
 * v4 adds exactly one field, `GroupInstanceId` (NULLABLE_STRING), after `MemberId` on every
 * `DescribedGroupMember`: the static membership id (KIP-345). Everything else is v3's body, so
 * this shares v3's reader at version 4.
 * @see clients/src/main/resources/common/message/DescribeGroupsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(4), parse }

module.exports.schema = {
  api: 'DescribeGroups',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'Groups', type: '[]DescribedGroup', versions: '0+' },
    { name: 'Groups.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Groups.GroupId', type: 'string', versions: '0+' },
    { name: 'Groups.GroupState', type: 'string', versions: '0+' },
    { name: 'Groups.ProtocolType', type: 'string', versions: '0+' },
    { name: 'Groups.ProtocolData', type: 'string', versions: '0+' },
    { name: 'Groups.Members', type: '[]DescribedGroupMember', versions: '0+' },
    { name: 'Groups.Members.MemberId', type: 'string', versions: '0+' },
    { name: 'Groups.Members.GroupInstanceId', type: 'string', versions: '4+', nullable: true },
    { name: 'Groups.Members.ClientId', type: 'string', versions: '0+' },
    { name: 'Groups.Members.ClientHost', type: 'string', versions: '0+' },
    { name: 'Groups.Members.MemberMetadata', type: 'bytes', versions: '0+' },
    { name: 'Groups.Members.MemberAssignment', type: 'bytes', versions: '0+' },
    { name: 'Groups.AuthorizedOperations', type: 'int32', versions: '3+' },
  ],
}
