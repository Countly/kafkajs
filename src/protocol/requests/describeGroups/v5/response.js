const { buildDecode, parse } = require('../v3/response')

/**
 * DescribeGroupsResponse (Version: 5) — flexible (KIP-482), response header v1
 *
 * v5 adds no field: it is the first flexible version, so the same values travel as compact
 * strings, bytes and arrays with a tag buffer terminating every struct. This shares v3's reader
 * at version 5, which is what selects the compact primitives.
 * @see clients/src/main/resources/common/message/DescribeGroupsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(5), parse }

module.exports.schema = {
  api: 'DescribeGroups',
  version: 5,
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
