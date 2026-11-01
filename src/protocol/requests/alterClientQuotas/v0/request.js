const Encoder = require('../../../encoder')
const { AlterClientQuotas: apiKey } = require('../../apiKeys')

/**
 * AlterClientQuotasRequest (Version: 0)
 *
 * Entries => ARRAY (0+)
 *   Entity => ARRAY (0+)
 *     EntityType => STRING (0+)
 *     EntityName => STRING (0+)
 *   Ops => ARRAY (0+)
 *     Key => STRING (0+)
 *     Value => FLOAT64 (0+)
 *     Remove => BOOLEAN (0+)
 * ValidateOnly => BOOLEAN (0+)
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
  apiVersion: 0,
  apiName: 'AlterClientQuotas',
  /** Encodes normalized fields without retaining a second request payload.
   * @returns {Promise<object>} Encoder holding the complete request body
   * @throws {Error} invalid normalized wire values
   */
  encode: async () => {
    return new Encoder().writeArray(entries.map(encodeEntries)).writeBoolean(validateOnly)
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
    .writeArray(entity.map(encodeEntriesEntity))
    .writeArray(ops.map(encodeEntriesOps))
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeEntriesEntity = ({ entityType, entityName }) => {
  return new Encoder().writeString(entityType).writeString(entityName)
}

/**
 * Processes one nested wire struct in schema order, including its tag buffer.
 * @param {object} value struct fields or the decoder at their first byte
 * @returns {object} encoded struct or decoded fields
 * @throws {Error} invalid or truncated wire data
 */
const encodeEntriesOps = ({ key, value, remove }) => {
  return new Encoder().writeString(key).writeDouble(value).writeBoolean(remove)
}

module.exports.schema = {
  api: 'AlterClientQuotas',
  version: 0,
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
