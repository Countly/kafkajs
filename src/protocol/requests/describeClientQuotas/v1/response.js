const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * DescribeClientQuotasResponse (Version: 1) — flexible (KIP-482)
 *
 * ThrottleTimeMs => INT32 (0+)
 * ErrorCode => INT16 (0+)
 * ErrorMessage => COMPACT_STRING (0+)
 * Entries => COMPACT_ARRAY (0+)
 *   Entity => COMPACT_ARRAY (0+)
 *     EntityType => COMPACT_STRING (0+)
 *     EntityName => COMPACT_STRING (0+)
 *     TAG_BUFFER
 *   Values => COMPACT_ARRAY (0+)
 *     Key => COMPACT_STRING (0+)
 *     Value => FLOAT64 (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/DescribeClientQuotasResponse.json (Apache Kafka 4.3.1)
 */
/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const decodeEntries = (decoder) => {
  const entries = {
    entity: Decoder.requireArray(
      decoder.readUVarIntArray(decodeEntriesEntity),
      'DescribeClientQuotasResponse.Entries.Entity'
    ),
    values: Decoder.requireArray(
      decoder.readUVarIntArray(decodeEntriesValues),
      'DescribeClientQuotasResponse.Entries.Values'
    ),
  }

  decoder.readTaggedFields()
  return entries
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const decodeEntriesEntity = (decoder) => {
  const entity = {
    entityType: decoder.readUVarIntString(),
    entityName: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return entity
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const decodeEntriesValues = (decoder) => {
  const values = {
    key: decoder.readUVarIntString(),
    value: decoder.readDouble(),
  }

  decoder.readTaggedFields()
  return values
}

/**
 * Decodes a complete DescribeClientQuotas response body, consuming all tags.
 * @param {Buffer} rawData body without the response header
 * @returns {Promise<object>} decoded wire fields and throttle time
 * @throws {Error} malformed/truncated body
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTimeMs = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readUVarIntString()
  const entries = decoder.readUVarIntArray(decodeEntries) || []
  const taggedFields = decoder.readTaggedFields()

  if (decoder.canReadBytes(1)) throw new KafkaJSPartialMessageError('Unexpected response bytes')
  return {
    clientSideThrottleTime: throttleTimeMs,
    throttleTimeMs,
    errorCode,
    errorMessage,
    entries,
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
  api: 'DescribeClientQuotas',
  version: 1,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Entries', type: '[]EntryData', versions: '0+', nullable: true },
    { name: 'Entries.Entity', type: '[]EntityData', versions: '0+' },
    { name: 'Entries.Entity.EntityType', type: 'string', versions: '0+' },
    { name: 'Entries.Entity.EntityName', type: 'string', versions: '0+', nullable: true },
    { name: 'Entries.Values', type: '[]ValueData', versions: '0+' },
    { name: 'Entries.Values.Key', type: 'string', versions: '0+' },
    { name: 'Entries.Values.Value', type: 'float64', versions: '0+' },
  ],
}
