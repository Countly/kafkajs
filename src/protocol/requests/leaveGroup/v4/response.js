const Decoder = require('../../../decoder')
const { failIfVersionNotSupported, failure, createErrorFromCode } = require('../../../error')
const { parse: parseTopLevel } = require('../v2/response')

/**
 * LeaveGroup Response (Version: 4) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * Members => COMPACT_ARRAY
 *   MemberId => COMPACT_STRING
 *   GroupInstanceId => COMPACT_NULLABLE_STRING
 *   ErrorCode => INT16
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v3's fields in compact framing, decoded to v3's keys. v5 has the same body. `parse` throws the
 * top-level error, then the first member's, which is what a single-member leave needs; the batch
 * form uses `parseTopLevel` instead and reads every member's own code.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/LeaveGroupResponse.json (Apache Kafka 4.3.1)
 */
const decodeMember = (decoder) => {
  const member = {
    memberId: decoder.readUVarIntString(),
    groupInstanceId: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return member
}

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const members = decoder.readUVarIntArray(decodeMember) || []
  decoder.readTaggedFields()

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, errorCode, members }
}

const parse = async (data) => {
  await parseTopLevel(data)

  const memberWithError = data.members.find((member) => failure(member.errorCode))
  if (memberWithError) {
    throw createErrorFromCode(memberWithError.errorCode)
  }

  return data
}

module.exports = {
  decode,
  parse,
  parseTopLevel,
}

module.exports.schema = {
  api: 'LeaveGroup',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Members', type: '[]MemberResponse', versions: '3+' },
    { name: 'Members.MemberId', type: 'string', versions: '3+' },
    { name: 'Members.GroupInstanceId', type: 'string', versions: '3+', nullable: true },
    { name: 'Members.ErrorCode', type: 'int16', versions: '3+' },
  ],
}
