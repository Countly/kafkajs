const { buildRequest, schema: schemaV4 } = require('../v4/request')

/**
 * LeaveGroup Request (Version: 5) — flexible (KIP-482), request header v2
 *
 * v4's fields with a per-member `Reason` (COMPACT_NULLABLE_STRING, KIP-800) after
 * `GroupInstanceId`. The broker only logs it; null when a member has none.
 * @see clients/src/main/resources/common/message/LeaveGroupRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options as v4; each member may carry a `reason`
 * @returns {object} the protocol request
 */
module.exports = ({ groupId, members }) => buildRequest(5, groupId, members)

module.exports.schema = {
  ...schemaV4,
  version: 5,
  fields: [
    ...schemaV4.fields,
    { name: 'Members.Reason', type: 'string', versions: '5+', nullable: true },
  ],
}
