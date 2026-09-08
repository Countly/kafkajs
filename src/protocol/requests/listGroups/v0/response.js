const Decoder = require('../../../decoder')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * ListGroups Response (Version: 0) => error_code [groups]
 *   error_code => INT16
 *   groups => group_id protocol_type
 *     group_id => STRING
 *     protocol_type => STRING
 */

const decodeGroup = (decoder) => ({
  groupId: decoder.readString(),
  protocolType: decoder.readString(),
  // Neither field exists before v4/v5. Reporting them as the empty string rather than leaving
  // them out keeps one row shape at every negotiated version, which is what lets a caller read
  // a row without asking which version answered it.
  groupState: '',
  groupType: '',
})

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const errorCode = decoder.readInt16()
  const groups = decoder.readArray(decodeGroup)

  return {
    errorCode,
    groups,
  }
}

const parse = async (data) => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

module.exports = {
  decodeGroup,
  decode,
  parse,
}
