const Decoder = require('../../../decoder')
const { failIfVersionNotSupported } = require('../../../error')
const { parse } = require('../v0/response')

/**
 * SyncGroup Response (Version: 4) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * ErrorCode => INT16
 * Assignment => COMPACT_BYTES
 * TAG_BUFFER
 *
 * v3's fields in compact framing, decoded to v3's keys. A compact null assignment decodes to an
 * empty buffer, which `MemberAssignment.decode` reads as "nothing assigned". v5 adds
 * `ProtocolType` and `ProtocolName` before `Assignment`, which `buildDecode(true)` reads.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/SyncGroupResponse.json (Apache Kafka 4.3.1)
 */

/**
 * @param {boolean} withProtocol whether the version carries `ProtocolType` and `ProtocolName` (v5)
 * @returns {(rawData: Buffer) => Promise<object>} the decoder of that version
 */
const buildDecode = (withProtocol) => async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()

  failIfVersionNotSupported(errorCode)

  const decoded = { throttleTime: 0, clientSideThrottleTime: throttleTime, errorCode }
  if (withProtocol) {
    decoded.protocolType = decoder.readUVarIntString()
    decoded.protocolName = decoder.readUVarIntString()
  }

  decoded.memberAssignment = decoder.readUVarIntBytes() || Buffer.alloc(0)
  decoder.readTaggedFields()
  return decoded
}

module.exports = {
  decode: buildDecode(false),
  buildDecode,
  parse,
}

module.exports.schema = {
  api: 'SyncGroup',
  version: 4,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Assignment', type: 'bytes', versions: '0+' },
  ],
}
