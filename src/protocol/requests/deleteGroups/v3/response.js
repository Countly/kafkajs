const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')
const { parse } = require('../v0/response')

/**
 * DeleteGroups Response (Version: 3) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * Results => COMPACT_ARRAY (0+)
 *   GroupId => COMPACT_STRING (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_NULLABLE_STRING (3+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v2's fields plus the broker's own message for each group, decoded to v2's keys and a per-group
 * `errorMessage`: `null` when the broker sent none, `''` when it sent an empty one. A failed
 * group's result carries its `error`; the broker's text is kept in `errorMessage` and never put
 * into that error. Unknown tagged fields are read and dropped.
 * @see clients/src/main/resources/common/message/DeleteGroupsResponse.json (Apache Kafka 4.4.0)
 */

/**
 * @param {Decoder} decoder positioned at one `Results` element
 * @returns {{ groupId: string, errorCode: number, errorMessage: string|null, error?: Error }}
 */
const decodeGroup = (decoder) => {
  const result = {
    groupId: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  if (failure(result.errorCode)) {
    result.error = createErrorFromCode(result.errorCode)
  }

  return result
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<{ throttleTimeMs: number, throttleTime: number, clientSideThrottleTime: number,
 *   results: { groupId: string, errorCode: number, errorMessage: string|null, error?: Error }[] }>}
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTimeMs = decoder.readInt32()
  const results = decoder.readUVarIntArray(decodeGroup)

  decoder.readTaggedFields()
  return { throttleTimeMs, throttleTime: 0, clientSideThrottleTime: throttleTimeMs, results }
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'DeleteGroups',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Results', type: '[]DeletableGroupResult', versions: '0+' },
    { name: 'Results.GroupId', type: 'string', versions: '0+' },
    { name: 'Results.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Results.ErrorMessage', type: 'string', versions: '3+', nullable: true },
  ],
}
