const Decoder = require('../../../decoder')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/**
 * AlterClientQuotasResponse (Version: 1) — flexible (KIP-482)
 *
 * ThrottleTimeMs => INT32 (0+)
 * Entries => COMPACT_ARRAY (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => COMPACT_STRING (0+)
 *   Entity => COMPACT_ARRAY (0+)
 *     EntityType => COMPACT_STRING (0+)
 *     EntityName => COMPACT_STRING (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/AlterClientQuotasResponse.json (Apache Kafka 4.3.1)
 */
/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const decodeEntries = (decoder) => {
  const entries = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readUVarIntString(),
    entity: Decoder.requireArray(
      decoder.readUVarIntArray(decodeEntriesEntity),
      'AlterClientQuotasResponse.Entries.Entity'
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
 * Decodes a complete AlterClientQuotas response body, consuming all tags.
 * @param {Buffer} rawData body without the response header
 * @returns {Promise<object>} decoded wire fields and throttle time
 * @throws {Error} malformed/truncated body
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTimeMs = decoder.readInt32()
  const entries = Decoder.requireArray(
    decoder.readUVarIntArray(decodeEntries),
    'AlterClientQuotasResponse.Entries'
  )
  const taggedFields = decoder.readTaggedFields()

  if (decoder.canReadBytes(1)) throw new KafkaJSPartialMessageError('Unexpected response bytes')
  return {
    clientSideThrottleTime: throttleTimeMs,
    throttleTimeMs,
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
  return data
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'AlterClientQuotas',
  version: 1,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '0+' },
    { name: 'Entries', type: '[]EntryData', versions: '0+' },
    { name: 'Entries.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Entries.ErrorMessage', type: 'string', versions: '0+', nullable: true },
    { name: 'Entries.Entity', type: '[]EntityData', versions: '0+' },
    { name: 'Entries.Entity.EntityType', type: 'string', versions: '0+' },
    { name: 'Entries.Entity.EntityName', type: 'string', versions: '0+', nullable: true },
  ],
}
