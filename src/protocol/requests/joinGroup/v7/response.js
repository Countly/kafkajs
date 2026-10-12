const { buildDecode, parse, schema: schemaV6 } = require('../v6/response')

/**
 * JoinGroup Response (Version: 7) — flexible (KIP-482), response header v1
 *
 * v6's fields with `ProtocolType` (COMPACT_NULLABLE_STRING) inserted after `GenerationId` and
 * `ProtocolName` made nullable. The decoded object gains `protocolType` (`string|null`) and keeps
 * `groupProtocol`, now `string|null`. The coordinator reports the protocol type it holds for the
 * group; whether it matches the one sent is the broker layer's check, not the codec's.
 * @see clients/src/main/resources/common/message/JoinGroupResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode: buildDecode(true),
  parse,
}

module.exports.schema = {
  ...schemaV6,
  version: 7,
  fields: schemaV6.fields.flatMap((field) => {
    if (field.name === 'ProtocolName') {
      return [
        { name: 'ProtocolType', type: 'string', versions: '7+', nullable: true },
        { ...field, nullable: true },
      ]
    }

    return [field]
  }),
}
