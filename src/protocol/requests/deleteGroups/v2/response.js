const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')
const { parse } = require('../v0/response')

/**
 * DeleteGroups Response (Version: 2) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (0+)
 * Results => COMPACT_ARRAY (0+)
 *   GroupId => COMPACT_STRING (0+)
 *   ErrorCode => INT16 (0+)
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The v1 fields in compact framing, decoded to v1's keys: a failed group's result carries its
 * `error`, `throttleTimeMs` is the broker's value and `clientSideThrottleTime` is the same number
 * (KIP-219).
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DeleteGroupsResponse.json (Apache Kafka 4.3.1)
 */

const decodeGroup = (decoder) => {
  const result = {
    groupId: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
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
 *   results: { groupId: string, errorCode: number, error?: Error }[] }>}
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
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Results', type: '[]DeletableGroupResult', versions: '0+' },
    { name: 'Results.GroupId', type: 'string', versions: '0+' },
    { name: 'Results.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
