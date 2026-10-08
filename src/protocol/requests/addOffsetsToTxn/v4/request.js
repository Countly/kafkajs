const requestV3 = require('../v3/request')

/**
 * AddOffsetsToTxn Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * The v3 body. v4 only lets the broker answer TRANSACTION_ABORTABLE (KIP-890), which surfaces as
 * the abortable protocol error it already is.
 * @see clients/src/main/resources/common/message/AddOffsetsToTxnRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.transactionalId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {string} options.groupId
 * @returns {object} the protocol request
 */
module.exports = ({ transactionalId, producerId, producerEpoch, groupId }) =>
  Object.assign(requestV3({ transactionalId, producerId, producerEpoch, groupId }), {
    apiVersion: 4,
  })

module.exports.schema = {
  api: 'AddOffsetsToTxn',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'GroupId', type: 'string', versions: '0+' },
  ],
}
