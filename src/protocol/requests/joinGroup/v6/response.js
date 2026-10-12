const Decoder = require('../../../decoder')
const { failIfVersionNotSupported } = require('../../../error')
const { parse } = require('../v5/response')

/**
 * JoinGroup Response (Version: 6) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * GenerationId => INT32
 * ProtocolName => COMPACT_STRING
 * Leader => COMPACT_STRING
 * MemberId => COMPACT_STRING
 * Members => COMPACT_ARRAY
 *   MemberId => COMPACT_STRING
 *   GroupInstanceId => COMPACT_NULLABLE_STRING
 *   Metadata => COMPACT_BYTES
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v5's fields in compact framing, decoded to v5's keys. v7 adds `ProtocolType` after `GenerationId`
 * and makes `ProtocolName` nullable, which `buildDecode(true)` reads; a compact null count or
 * bytes decodes to an empty list or buffer so a caller never meets `null` where it iterates.
 * `parse` is v5's: MEMBER_ID_REQUIRED carries the member id, every other code throws.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/JoinGroupResponse.json (Apache Kafka 4.3.1)
 */
const decodeMember = (decoder) => {
  const member = {
    memberId: decoder.readUVarIntString(),
    groupInstanceId: decoder.readUVarIntString(),
    memberMetadata: decoder.readUVarIntBytes() || Buffer.alloc(0),
  }

  decoder.readTaggedFields()
  return member
}

/**
 * @param {boolean} withProtocolType whether the version carries `ProtocolType` (v7 and later)
 * @returns {(rawData: Buffer) => Promise<object>} the decoder of that version
 */
const buildDecode = (withProtocolType) => async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const decoded = { throttleTime: 0, clientSideThrottleTime: throttleTime, errorCode }
  decoded.generationId = decoder.readInt32()
  if (withProtocolType) {
    decoded.protocolType = decoder.readUVarIntString()
  }

  decoded.groupProtocol = decoder.readUVarIntString()
  decoded.leaderId = decoder.readUVarIntString()
  decoded.memberId = decoder.readUVarIntString()
  decoded.members = decoder.readUVarIntArray(decodeMember) || []
  decoder.readTaggedFields()
  return decoded
}

module.exports = {
  decode: buildDecode(false),
  buildDecode,
  parse,
}

module.exports.schema = {
  api: 'JoinGroup',
  version: 6,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '2+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'GenerationId', type: 'int32', versions: '0+' },
    { name: 'ProtocolName', type: 'string', versions: '0+' },
    { name: 'Leader', type: 'string', versions: '0+' },
    { name: 'MemberId', type: 'string', versions: '0+' },
    { name: 'Members', type: '[]JoinGroupResponseMember', versions: '0+' },
    { name: 'Members.MemberId', type: 'string', versions: '0+' },
    { name: 'Members.GroupInstanceId', type: 'string', versions: '5+', nullable: true },
    { name: 'Members.Metadata', type: 'bytes', versions: '0+' },
  ],
}
