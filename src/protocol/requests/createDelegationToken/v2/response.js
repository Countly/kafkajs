const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * CreateDelegationTokenResponse (Version: 2) — flexible (KIP-482)
 *
 * ErrorCode => INT16 (0+)
 * PrincipalType => COMPACT_STRING (0+)
 * PrincipalName => COMPACT_STRING (0+)
 * IssueTimestampMs => INT64 (0+)
 * ExpiryTimestampMs => INT64 (0+)
 * MaxTimestampMs => INT64 (0+)
 * TokenId => COMPACT_STRING (0+)
 * Hmac => COMPACT_BYTES (0+)
 * ThrottleTimeMs => INT32 (0+)
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/CreateDelegationTokenResponse.json (Apache Kafka 4.3.1)
 */
/**
 * Decodes a complete CreateDelegationToken response body, consuming all tags.
 * @param {Buffer} rawData body without the response header
 * @returns {Promise<object>} decoded wire fields and throttle time
 * @throws {Error} malformed/truncated body
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const errorCode = decoder.readInt16()
  const principalType = decoder.readUVarIntString()
  const principalName = decoder.readUVarIntString()
  const issueTimestampMs = decoder.readInt64().toString()
  const expiryTimestampMs = decoder.readInt64().toString()
  const maxTimestampMs = decoder.readInt64().toString()
  const tokenId = decoder.readUVarIntString()
  const hmac = decoder.readUVarIntBytes()
  const throttleTimeMs = decoder.readInt32()
  const taggedFields = decoder.readTaggedFields()

  if (decoder.canReadBytes(1)) throw new KafkaJSPartialMessageError('Unexpected response bytes')
  return {
    clientSideThrottleTime: throttleTimeMs,
    errorCode,
    principalType,
    principalName,
    issueTimestampMs,
    expiryTimestampMs,
    maxTimestampMs,
    tokenId,
    hmac,
    throttleTimeMs,
    taggedFields,
  }
}

/**
 * Returns item codes as data; throws only a top-level protocol failure.
 * @param {object} data decoded response
 * @returns {Promise<object>} decoded fields with lossless decimal timestamps
 * @throws {Error} fixed protocol error; broker text is never copied to errors
 */
const parse = async (data) => {
  if (data.errorCode !== 0) throw createErrorFromCode(data.errorCode)
  return data
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'CreateDelegationToken',
  version: 2,
  direction: 'response',
  fields: [
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'PrincipalType', type: 'string', versions: '0+' },
    { name: 'PrincipalName', type: 'string', versions: '0+' },
    { name: 'IssueTimestampMs', type: 'int64', versions: '0+' },
    { name: 'ExpiryTimestampMs', type: 'int64', versions: '0+' },
    { name: 'MaxTimestampMs', type: 'int64', versions: '0+' },
    { name: 'TokenId', type: 'string', versions: '0+' },
    { name: 'Hmac', type: 'bytes', versions: '0+' },
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
  ],
}
