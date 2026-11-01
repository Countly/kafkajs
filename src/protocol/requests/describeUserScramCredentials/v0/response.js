const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * DescribeUserScramCredentialsResponse (Version: 0) — flexible (KIP-482)
 *
 * ThrottleTimeMs => INT32 (0+)
 * ErrorCode => INT16 (0+)
 * ErrorMessage => COMPACT_STRING (0+)
 * Results => COMPACT_ARRAY (0+)
 *   User => COMPACT_STRING (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_STRING (0+)
 *   CredentialInfos => COMPACT_ARRAY (0+)
 *     Mechanism => INT8 (0+)
 *     Iterations => INT32 (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeUserScramCredentialsResponse.json (Apache Kafka 4.3.1)
 */
/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const decodeResults = (decoder) => {
  const results = {
    user: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    credentialInfos: Decoder.requireArray(
      decoder.readUVarIntArray(decodeResultsCredentialInfos),
      'DescribeUserScramCredentialsResponse.Results.CredentialInfos'
    ),
  }

  decoder.readTaggedFields()
  return results
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const decodeResultsCredentialInfos = (decoder) => {
  const credentialInfos = {
    mechanism: decoder.readInt8(),
    iterations: decoder.readInt32(),
  }

  decoder.readTaggedFields()
  return credentialInfos
}

/**
 * Decodes a complete DescribeUserScramCredentials response body, consuming all tags.
 * @param {Buffer} rawData body without the response header
 * @returns {Promise<object>} decoded wire fields and throttle time
 * @throws {Error} malformed/truncated body
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTimeMs = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const results = Decoder.requireArray(
    decoder.readUVarIntArray(decodeResults),
    'DescribeUserScramCredentialsResponse.Results'
  )
  const taggedFields = decoder.readTaggedFields()

  if (decoder.canReadBytes(1)) throw new KafkaJSPartialMessageError('Unexpected response bytes')
  return {
    clientSideThrottleTime: throttleTimeMs,
    throttleTimeMs,
    errorCode,
    errorMessage,
    results,
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
  api: 'DescribeUserScramCredentials',
  version: 0,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Results', type: '[]DescribeUserScramCredentialsResult', versions: '0+' },
    { name: 'Results.User', type: 'string', versions: '0+' },
    { name: 'Results.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Results.ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Results.CredentialInfos', type: '[]CredentialInfo', versions: '0+' },
    { name: 'Results.CredentialInfos.Mechanism', type: 'int8', versions: '0+' },
    { name: 'Results.CredentialInfos.Iterations', type: 'int32', versions: '0+' },
  ],
}
