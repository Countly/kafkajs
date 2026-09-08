const { buildDecode, parse } = require('../v3/response')

/**
 * ListGroups Response (Version: 5) — flexible (KIP-482), response header v1
 *
 * v5 adds exactly one field, `GroupType` (COMPACT_STRING), at the end of every `ListedGroup`
 * struct, so this shares v3's reader at version 5 (KIP-848). It is the field that tells a
 * consumer-protocol group apart from a classic one.
 * @see clients/src/main/resources/common/message/ListGroupsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = { decode: buildDecode(5), parse }

module.exports.schema = {
  api: 'ListGroups',
  version: 5,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Groups', type: '[]ListedGroup', versions: '0+' },
    { name: 'Groups.GroupId', type: 'string', versions: '0+' },
    { name: 'Groups.ProtocolType', type: 'string', versions: '0+' },
    { name: 'Groups.GroupState', type: 'string', versions: '4+' },
    { name: 'Groups.GroupType', type: 'string', versions: '5+' },
  ],
}
