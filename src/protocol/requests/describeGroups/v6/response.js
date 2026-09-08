const { buildDecode } = require('../v3/response')
const { failure, createErrorFromCode } = require('../../../error')

/** GROUP_ID_NOT_FOUND: at v6 this is an answer about one group, not a failed request. */
const GROUP_ID_NOT_FOUND = 69

/**
 * Throws for every group-level error code except `GROUP_ID_NOT_FOUND`.
 *
 * KIP-1043 made 69 the answer for a group that is not there, where earlier versions sent a `Dead`
 * row and no error at all. Reporting it on the row keeps `describeGroups(['missing'])` returning
 * one row per requested id, as it always has, and is what lets a caller route that id to another
 * API. Every OTHER code still throws for the batch, deliberately: both callers of this request in
 * this client wrap it in a retrier and depend on a retriable coordinator code arriving as a throw,
 * so swallowing one would replace a retry with a row nobody inspects.
 *
 * @param {object} data decoded response
 * @returns {Promise<object>} the response, unchanged, when every group either succeeded or was
 *   reported as not found
 * @throws {KafkaJSProtocolError} on any other group-level code, first one wins
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1043%3A+Administration+of+groups
 */
const parse = async (data) => {
  const errored = data.groups.find(
    (group) => failure(group.errorCode) && group.errorCode !== GROUP_ID_NOT_FOUND
  )

  if (errored) {
    throw createErrorFromCode(errored.errorCode)
  }

  return data
}

/**
 * DescribeGroupsResponse (Version: 6) — flexible (KIP-482), response header v1
 *
 * v6 adds exactly one field, `ErrorMessage` (COMPACT_NULLABLE_STRING), after `ErrorCode` on every
 * `DescribedGroup`, and makes `GROUP_ID_NOT_FOUND` the answer for a group that does not exist
 * (KIP-1043). A row carrying 69 is a normal row: the broker sends `Dead`, an empty protocol type
 * and data and no members with it, so the shape callers have always read is unchanged and simply
 * gains the code and the message.
 * @see clients/src/main/resources/common/message/DescribeGroupsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(6), parse }

module.exports.schema = {
  api: 'DescribeGroups',
  version: 6,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'Groups', type: '[]DescribedGroup', versions: '0+' },
    { name: 'Groups.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Groups.ErrorMessage', type: 'string', versions: '6+', nullable: true },
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
