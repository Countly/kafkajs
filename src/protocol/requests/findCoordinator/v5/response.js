const { decode, parse } = require('../v4/response')

/**
 * FindCoordinatorResponse (Version: 5) — flexible (KIP-482), response header v1
 *
 * Adds no fields over v4: v5 only admits the `TRANSACTION_ABORTABLE` (120) error code on the
 * coordinator lookup (KIP-890), which this client's shared error table already maps. The wire
 * shape and decoded result are byte-identical to v4.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890
 * @see clients/src/main/resources/common/message/FindCoordinatorResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'FindCoordinator',
  version: 5,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'Coordinators', type: '[]Coordinator', versions: '4+' },
    { name: 'Coordinators.Key', type: 'string', versions: '4+' },
    { name: 'Coordinators.NodeId', type: 'int32', versions: '4+' },
    { name: 'Coordinators.Host', type: 'string', versions: '4+' },
    { name: 'Coordinators.Port', type: 'int32', versions: '4+' },
    { name: 'Coordinators.ErrorCode', type: 'int16', versions: '4+' },
    { name: 'Coordinators.ErrorMessage', type: 'string', versions: '4+', nullable: true },
  ],
}
