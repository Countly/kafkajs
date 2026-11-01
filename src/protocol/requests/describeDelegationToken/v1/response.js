const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/** Reads a classic array with the message's exact nullability.
 * @param {object} decoder positioned decoder
 * @param {Function} read nested struct reader
 * @param {boolean} nullable schema permits null
 * @returns {object[]|null} decoded values
 * @throws {Error} negative, null-forbidden or truncated framing
 */
const readArray = (decoder, read, nullable) => {
  const length = decoder.readInt32()
  if (length === -1 && nullable) return null
  if (length < 0) throw new KafkaJSPartialMessageError('Invalid array length')
  decoder.ensureReadable(length, 'array')
  const values = new Array(length)
  for (let i = 0; i < length; i++) values[i] = read(decoder)
  return values
}

/**
 * DescribeDelegationTokenResponse (Version: 1)
 *
 * ErrorCode => INT16 (0+)
 * Tokens => ARRAY (0+)
 *   PrincipalType => STRING (0+)
 *   PrincipalName => STRING (0+)
 *   IssueTimestamp => INT64 (0+)
 *   ExpiryTimestamp => INT64 (0+)
 *   MaxTimestamp => INT64 (0+)
 *   TokenId => STRING (0+)
 *   Hmac => BYTES (0+)
 *   Renewers => ARRAY (0+)
 *     PrincipalType => STRING (0+)
 *     PrincipalName => STRING (0+)
 * ThrottleTimeMs => INT32 (0+)
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
    principalType: decoder.readString(),
    principalName: decoder.readString(),
    issueTimestamp: decoder.readInt64().toString(),
    expiryTimestamp: decoder.readInt64().toString(),
    maxTimestamp: decoder.readInt64().toString(),
    tokenId: decoder.readString(),
    hmac: decoder.readBytes(),
    renewers: readArray(decoder, decodeTokensRenewers, false),
  }
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
    principalType: decoder.readString(),
    principalName: decoder.readString(),
  }
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
  const tokens = readArray(decoder, decodeTokens, false)
  const throttleTimeMs = decoder.readInt32()

  if (decoder.canReadBytes(1)) throw new KafkaJSPartialMessageError('Unexpected response bytes')
  return {
    clientSideThrottleTime: throttleTimeMs,
    errorCode,
    tokens,
    throttleTimeMs,
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
  version: 1,
  direction: 'response',
  fields: [
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Tokens', type: '[]DescribedDelegationToken', versions: '0+' },
    { name: 'Tokens.PrincipalType', type: 'string', versions: '0+' },
    { name: 'Tokens.PrincipalName', type: 'string', versions: '0+' },
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
