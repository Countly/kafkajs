const { JoinGroup: apiKey } = require('../../apiKeys')
const { body, schema: schemaV6 } = require('../v6/request')

/**
 * JoinGroup Request (Version: 8) — flexible (KIP-482), request header v2
 *
 * v7's fields followed by
 *
 * Reason => COMPACT_NULLABLE_STRING
 * TAG_BUFFER
 *
 * `Reason` is why the member (re-)joins; the broker only logs it. The consumer sends none, so it is
 * null by default.
 * @see clients/src/main/resources/common/message/JoinGroupRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options as v6, plus `reason`
 * @param {string|null} [options.reason=null]
 * @returns {object} the protocol request
 */
module.exports = (options) => ({
  apiKey,
  apiVersion: 8,
  apiName: 'JoinGroup',
  encode: async () => body(options).writeUVarIntString(options.reason).writeTaggedFields(),
})

module.exports.schema = {
  ...schemaV6,
  version: 8,
  fields: [...schemaV6.fields, { name: 'Reason', type: 'string', versions: '8+', nullable: true }],
}
