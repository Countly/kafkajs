const Encoder = require('../../../encoder')
const { AlterClientQuotas: apiKey } = require('../../apiKeys')

/**
 * AlterClientQuotasRequest (Version: 1) — flexible (KIP-482)
 *
 * Entries => COMPACT_ARRAY (0+)
 *   Entity => COMPACT_ARRAY (0+)
 *     EntityType => COMPACT_STRING (0+)
 *     EntityName => COMPACT_STRING (0+)
 *     TAG_BUFFER
 *   Ops => COMPACT_ARRAY (0+)
 *     Key => COMPACT_STRING (0+)
 *     Value => FLOAT64 (0+)
 *     Remove => BOOLEAN (0+)
 *     TAG_BUFFER
 *   TAG_BUFFER
 * ValidateOnly => BOOLEAN (0+)
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/AlterClientQuotasRequest.json (Apache Kafka 4.3.1)
 */
/**
 * Builds the negotiated wire request; validation and secret derivation belong to admin.
 * @param {object} fields normalized wire fields
 * @returns {object} request with an asynchronous encoder
 * @throws {Error} invalid wire values during encoding
 */
module.exports = ({ entries, validateOnly }) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'AlterClientQuotas',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder()
      .writeUVarIntArray(entries.map(encodeEntries))
      .writeBoolean(validateOnly)
      .writeTaggedFields()
  },
})

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeEntries = ({ entity, ops }) => {
  return new Encoder()
    .writeUVarIntArray(entity.map(encodeEntriesEntity))
    .writeUVarIntArray(ops.map(encodeEntriesOps))
    .writeTaggedFields()
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeEntriesEntity = ({ entityType, entityName }) => {
  return new Encoder()
    .writeUVarIntString(entityType)
    .writeUVarIntString(entityName)
    .writeTaggedFields()
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeEntriesOps = ({ key, value, remove }) => {
  return new Encoder()
    .writeUVarIntString(key)
    .writeDouble(value)
    .writeBoolean(remove)
    .writeTaggedFields()
}

module.exports.schema = {
  api: 'AlterClientQuotas',
  version: 1,
  direction: 'request',
  fields: [
    { name: 'Entries', type: '[]EntryData', versions: '0+' },
    { name: 'Entries.Entity', type: '[]EntityData', versions: '0+' },
    { name: 'Entries.Entity.EntityType', type: 'string', versions: '0+' },
    { name: 'Entries.Entity.EntityName', type: 'string', versions: '0+', nullable: true },
    { name: 'Entries.Ops', type: '[]OpData', versions: '0+' },
    { name: 'Entries.Ops.Key', type: 'string', versions: '0+' },
    { name: 'Entries.Ops.Value', type: 'float64', versions: '0+' },
    { name: 'Entries.Ops.Remove', type: 'bool', versions: '0+' },
    { name: 'ValidateOnly', type: 'bool', versions: '0+' },
  ],
}
