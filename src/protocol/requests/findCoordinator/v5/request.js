const requestV4 = require('../v4/request')

/**
 * FindCoordinatorRequest (Version: 5) — flexible (KIP-482), request header v2
 *
 * Adds no fields over v4: v5 only admits the `TRANSACTION_ABORTABLE` (120) error code on the
 * coordinator lookup, which is a response-side change. The request wire shape is byte-identical
 * to v4.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890
 * @see clients/src/main/resources/common/message/FindCoordinatorRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.coordinatorKey
 * @param {number} options.coordinatorType
 */
module.exports = ({ coordinatorKey, coordinatorType }) =>
  Object.assign(requestV4({ coordinatorKey, coordinatorType }), { apiVersion: 5 })

module.exports.schema = {
  api: 'FindCoordinator',
  version: 5,
  direction: 'request',
  fields: [
    { name: 'KeyType', type: 'int8', versions: '1+' },
    { name: 'CoordinatorKeys', type: '[]string', versions: '4+' },
  ],
}
