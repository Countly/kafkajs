const { buildDecode, parse, schema: schemaV4 } = require('../v4/response')

/**
 * SyncGroup Response (Version: 5) — flexible (KIP-482), response header v1
 *
 * v4's fields with `ProtocolType` and `ProtocolName` (both COMPACT_NULLABLE_STRING) before
 * `Assignment`. The decoded object gains `protocolType` and `protocolName`, each `string|null`;
 * whether they match what was sent is the broker layer's check, not the codec's.
 * @see clients/src/main/resources/common/message/SyncGroupResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode: buildDecode(true),
  parse,
}

module.exports.schema = {
  ...schemaV4,
  version: 5,
  fields: schemaV4.fields.flatMap((field) =>
    field.name === 'Assignment'
      ? [
          { name: 'ProtocolType', type: 'string', versions: '5+', nullable: true },
          { name: 'ProtocolName', type: 'string', versions: '5+', nullable: true },
          field,
        ]
      : [field]
  ),
}
