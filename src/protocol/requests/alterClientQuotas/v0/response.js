const Decoder = require('../../../decoder')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/** Reads a classic array with the message's exact nullability, bounded by the decoder's count limit.
 * @param {Decoder} decoder positioned decoder
 * @param {Function} read nested struct reader
 * @param {boolean} nullable schema permits null
 * @returns {object[]|null} decoded values
 * @throws {Error} negative, null-forbidden, over-limit or truncated framing
 */
const readArray = (decoder, read, nullable) => {
  const length = decoder.readInt32()
  if (length === -1 && nullable) return null
  if (length < 0) throw new KafkaJSPartialMessageError('Invalid array length')
  decoder.assertCount(length, 'array')
  decoder.ensureReadable(length, 'array')
  const values = []
  for (let i = 0; i < length; i++) values.push(read(decoder))
  return values
}

/**
 * AlterClientQuotasResponse (Version: 0)
 *
 * ThrottleTimeMs => INT32 (0+)
 * Entries => ARRAY (0+)
 *   ErrorCode => INT16 (0+)
 *   ErrorMessage => STRING (0+)
 *   Entity => ARRAY (0+)
 *     EntityType => STRING (0+)
 *     EntityName => STRING (0+)
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
    errorMessage: decoder.readString(),
    entity: readArray(decoder, decodeEntriesEntity, false),
  }
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
    entityType: decoder.readString(),
    entityName: decoder.readString(),
  }
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
  const entries = readArray(decoder, decodeEntries, false)

  if (decoder.canReadBytes(1)) throw new KafkaJSPartialMessageError('Unexpected response bytes')
  return {
    clientSideThrottleTime: throttleTimeMs,
    throttleTimeMs,
    entries,
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
  version: 0,
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
