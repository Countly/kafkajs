const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * @param {number} version 3, 4 or 5
 * @returns {(decoder: Decoder) => object} a reader for one `ListedGroup` struct at that version
 */
const decodeGroup = (version) => (decoder) => {
  const group = {
    groupId: decoder.readUVarIntString(),
    protocolType: decoder.readUVarIntString(),
    // Not `undefined`: a caller reads one row shape whatever version was negotiated, and the
    // empty string is what the broker itself sends for "no state".
    groupState: version >= 4 ? decoder.readUVarIntString() : '',
    groupType: version >= 5 ? decoder.readUVarIntString() : '',
  }

  decoder.readTaggedFields()
  return group
}

/**
 * ListGroups Response (Versions: 3-5) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * Groups => COMPACT_ARRAY
 *   GroupId => COMPACT_STRING
 *   ProtocolType => COMPACT_STRING
 *   GroupState => COMPACT_STRING (4+)
 *   GroupType => COMPACT_STRING (5+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see clients/src/main/resources/common/message/ListGroupsResponse.json (Apache Kafka 4.3.1)
 *
 * @param {number} version 3, 4 or 5; v4 adds GroupState (KIP-518), v5 adds GroupType (KIP-848)
 * @returns {(rawData: Buffer) => Promise<object>} the decoder for that version; `throttleTime` is
 *   pinned at 0 and the broker's value is reported as `clientSideThrottleTime` (KIP-219), as v2
 *   already does
 */
const buildDecode = (version) => async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const groups = decoder.readUVarIntArray(decodeGroup(version))

  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, errorCode, groups }
}

module.exports = { buildDecode, decode: buildDecode(3), parse }

module.exports.schema = {
  api: 'ListGroups',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Groups', type: '[]ListedGroup', versions: '0+' },
    { name: 'Groups.GroupId', type: 'string', versions: '0+' },
    { name: 'Groups.ProtocolType', type: 'string', versions: '0+' },
  ],
}
