const { buildDecode, parse } = require('../v3/response')

/**
 * ListGroups Response (Version: 4) — flexible (KIP-482), response header v1
 *
 * v4 adds exactly one field, `GroupState` (COMPACT_STRING), at the end of every `ListedGroup`
 * struct, so this shares v3's reader at version 4 (KIP-518).
 * @see clients/src/main/resources/common/message/ListGroupsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(4), parse }

module.exports.schema = {
  api: 'ListGroups',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Groups', type: '[]ListedGroup', versions: '0+' },
    { name: 'Groups.GroupId', type: 'string', versions: '0+' },
    { name: 'Groups.ProtocolType', type: 'string', versions: '0+' },
    { name: 'Groups.GroupState', type: 'string', versions: '4+' },
  ],
}
