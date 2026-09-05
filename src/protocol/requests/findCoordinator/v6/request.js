const requestV4 = require('../v4/request')

/**
 * FindCoordinatorRequest (Version: 6) — flexible (KIP-482), request header v2
 *
 * Adds no fields over v4/v5: v6 only defines the `"groupId:topicId:partition"` key format for
 * share-group lookups (key type SHARE, 2), which `src/protocol/coordinatorTypes.js` never emits
 * (it exports GROUP and TRANSACTION only). The request wire shape is byte-identical to v4.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-932
 * @see clients/src/main/resources/common/message/FindCoordinatorRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.coordinatorKey
 * @param {number} options.coordinatorType
 */
module.exports = ({ coordinatorKey, coordinatorType }) =>
  Object.assign(requestV4({ coordinatorKey, coordinatorType }), { apiVersion: 6 })

module.exports.schema = {
  api: 'FindCoordinator',
  version: 6,
  direction: 'request',
  fields: [
    { name: 'KeyType', type: 'int8', versions: '1+' },
    { name: 'CoordinatorKeys', type: '[]string', versions: '4+' },
  ],
}
