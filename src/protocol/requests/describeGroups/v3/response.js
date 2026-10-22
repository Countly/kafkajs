const Decoder = require('../../../decoder')
const { parse } = require('../v0/response')

/**
 * The primitives one version family reads with: classic length-prefixed below v5, compact with a
 * tag buffer per struct from v5 (KIP-482). Choosing them once per version keeps the readers free
 * of per-field version branches.
 * @param {number} version 3, 4, 5 or 6
 * @returns {{ string: Function, bytes: Function, array: Function, taggedFields: Function }} `array`
 *   reads `(decoder, reader, field)`, and from v5 refuses a null array, naming `field`
 */
const readersFor = (version) =>
  version >= 5
    ? {
        string: (decoder) => decoder.readUVarIntString(),
        bytes: (decoder) => decoder.readUVarIntBytes(),
        array: (decoder, reader, field) =>
          Decoder.requireArray(decoder.readUVarIntArray(reader), field),
        taggedFields: (decoder) => decoder.readTaggedFields(),
      }
    : {
        string: (decoder) => decoder.readString(),
        bytes: (decoder) => decoder.readBytes(),
        array: (decoder, reader) => decoder.readArray(reader),
        taggedFields: () => {},
      }

/**
 * @param {number} version 3, 4, 5 or 6
 * @param {object} read the primitives from `readersFor`
 * @returns {(decoder: Decoder) => object} a reader for one `DescribedGroupMember` struct
 */
const decodeMember = (version, read) => (decoder) => {
  const member = {
    memberId: read.string(decoder),
    // Static membership (KIP-345) arrives at v4. Null below it, and null on the wire for a member
    // that has no instance id, so one row shape reads at every version.
    groupInstanceId: version >= 4 ? read.string(decoder) : null,
    clientId: read.string(decoder),
    clientHost: read.string(decoder),
    memberMetadata: read.bytes(decoder),
    memberAssignment: read.bytes(decoder),
  }

  read.taggedFields(decoder)
  return member
}

/**
 * @param {number} version 3, 4, 5 or 6
 * @param {object} read the primitives from `readersFor`
 * @returns {(decoder: Decoder) => object} a reader for one `DescribedGroup` struct
 */
const decodeGroup = (version, read) => (decoder) => {
  const group = {
    errorCode: decoder.readInt16(),
    // v6 only (KIP-1043). Null below it, so a caller reads one shape and can tell "the broker said
    // nothing" from "the broker sent an empty message".
    errorMessage: version >= 6 ? read.string(decoder) : null,
    groupId: read.string(decoder),
    // `state` and `protocol` are the 2.2.4 key names for GroupState and ProtocolData; renaming
    // them to match the schema would break every reader of this response.
    state: read.string(decoder),
    protocolType: read.string(decoder),
    protocol: read.string(decoder),
    members: read.array(
      decoder,
      decodeMember(version, read),
      'DescribeGroupsResponse.Groups.Members'
    ),
    authorizedOperations: decoder.readInt32(),
  }

  read.taggedFields(decoder)
  return group
}

/**
 * DescribeGroupsResponse (Versions: 3-6)
 *
 * ThrottleTimeMs => INT32 (1+)
 * Groups => ARRAY / COMPACT_ARRAY (5+) (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => NULLABLE_STRING (6+)
 *   GroupId => STRING (0+)
 *   GroupState => STRING (0+)
 *   ProtocolType => STRING (0+)
 *   ProtocolData => STRING (0+)
 *   Members => ARRAY (0+)
 *     MemberId => STRING (0+)
 *     GroupInstanceId => NULLABLE_STRING (4+)
 *     ClientId => STRING (0+)
 *     ClientHost => STRING (0+)
 *     MemberMetadata => BYTES (0+)
 *     MemberAssignment => BYTES (0+)
 *     TAG_BUFFER (5+)
 *   AuthorizedOperations => INT32 (3+)
 *   TAG_BUFFER (5+)
 * TAG_BUFFER (5+)
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeGroupsResponse.json (Apache Kafka 4.3.1)
 *
 * @param {number} version 3 (AuthorizedOperations, KIP-430), 4 (GroupInstanceId, KIP-345),
 *   5 (flexible framing, KIP-482) or 6 (ErrorMessage and GROUP_ID_NOT_FOUND, KIP-1043)
 * @returns {(rawData: Buffer) => Promise<object>} the decoder for that version; `throttleTime` is
 *   pinned at 0 and the broker's value is reported as `clientSideThrottleTime` (KIP-219), as v2
 *   already does
 */
const buildDecode = (version) => async (rawData) => {
  const read = readersFor(version)
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const groups = read.array(decoder, decodeGroup(version, read), 'DescribeGroupsResponse.Groups')

  read.taggedFields(decoder)

  return { throttleTime: 0, clientSideThrottleTime: throttleTime, groups }
}

module.exports = { buildDecode, decode: buildDecode(3), parse }

module.exports.schema = {
  api: 'DescribeGroups',
  version: 3,
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
    { name: 'Groups.Members.ClientId', type: 'string', versions: '0+' },
    { name: 'Groups.Members.ClientHost', type: 'string', versions: '0+' },
    { name: 'Groups.Members.MemberMetadata', type: 'bytes', versions: '0+' },
    { name: 'Groups.Members.MemberAssignment', type: 'bytes', versions: '0+' },
    { name: 'Groups.AuthorizedOperations', type: 'int32', versions: '3+' },
  ],
}
