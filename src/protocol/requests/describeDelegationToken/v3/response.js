const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * DescribeDelegationTokenResponse (Version: 3) — flexible (KIP-482)
 *
 * ErrorCode => INT16 (0+)
 * Tokens => COMPACT_ARRAY (0+)
 *   PrincipalType => COMPACT_STRING (0+)
 *   PrincipalName => COMPACT_STRING (0+)
 *   TokenRequesterPrincipalType => COMPACT_STRING (3+)
 *   TokenRequesterPrincipalName => COMPACT_STRING (3+)
 *   IssueTimestamp => INT64 (0+)
 *   ExpiryTimestamp => INT64 (0+)
 *   MaxTimestamp => INT64 (0+)
 *   TokenId => COMPACT_STRING (0+)
 *   Hmac => COMPACT_BYTES (0+)
 *   Renewers => COMPACT_ARRAY (0+)
 *     PrincipalType => COMPACT_STRING (0+)
 *     PrincipalName => COMPACT_STRING (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * ThrottleTimeMs => INT32 (0+)
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeDelegationTokenResponse.json (Apache Kafka 4.3.1)
 */
/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const decodeTokens = (decoder) => {
  const tokens = {
    principalType: decoder.readUVarIntString(),
    principalName: decoder.readUVarIntString(),
    tokenRequesterPrincipalType: decoder.readUVarIntString(),
    tokenRequesterPrincipalName: decoder.readUVarIntString(),
    issueTimestamp: decoder.readInt64().toString(),
    expiryTimestamp: decoder.readInt64().toString(),
    maxTimestamp: decoder.readInt64().toString(),
    tokenId: decoder.readUVarIntString(),
    hmac: decoder.readUVarIntBytes(),
    renewers: Decoder.requireArray(
      decoder.readUVarIntArray(decodeTokensRenewers),
      'DescribeDelegationTokenResponse.Tokens.Renewers'
    ),
  }

  decoder.readTaggedFields()
  return tokens
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const decodeTokensRenewers = (decoder) => {
  const renewers = {
    principalType: decoder.readUVarIntString(),
    principalName: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return renewers
}

/**
 * Decodes a complete DescribeDelegationToken response body, consuming all tags.
 * @param {Buffer} rawData body without the response header
 * @returns {Promise<object>} decoded wire fields and throttle time
 * @throws {Error} malformed/truncated body
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const errorCode = decoder.readInt16()
  const tokens = Decoder.requireArray(
    decoder.readUVarIntArray(decodeTokens),
    'DescribeDelegationTokenResponse.Tokens'
  )
  const throttleTimeMs = decoder.readInt32()
  const taggedFields = decoder.readTaggedFields()

  if (decoder.canReadBytes(1)) throw new KafkaJSPartialMessageError('Unexpected response bytes')
  return {
    clientSideThrottleTime: throttleTimeMs,
    errorCode,
    tokens,
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
  api: 'DescribeDelegationToken',
  version: 3,
  direction: 'response',
  fields: [
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Tokens', type: '[]DescribedDelegationToken', versions: '0+' },
    { name: 'Tokens.PrincipalType', type: 'string', versions: '0+' },
    { name: 'Tokens.PrincipalName', type: 'string', versions: '0+' },
    { name: 'Tokens.TokenRequesterPrincipalType', type: 'string', versions: '3+' },
    { name: 'Tokens.TokenRequesterPrincipalName', type: 'string', versions: '3+' },
    { name: 'Tokens.IssueTimestamp', type: 'int64', versions: '0+' },
    { name: 'Tokens.ExpiryTimestamp', type: 'int64', versions: '0+' },
    { name: 'Tokens.MaxTimestamp', type: 'int64', versions: '0+' },
    { name: 'Tokens.TokenId', type: 'string', versions: '0+' },
    { name: 'Tokens.Hmac', type: 'bytes', versions: '0+' },
    { name: 'Tokens.Renewers', type: '[]DescribedDelegationTokenRenewer', versions: '0+' },
    { name: 'Tokens.Renewers.PrincipalType', type: 'string', versions: '0+' },
    { name: 'Tokens.Renewers.PrincipalName', type: 'string', versions: '0+' },
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
  ],
}
