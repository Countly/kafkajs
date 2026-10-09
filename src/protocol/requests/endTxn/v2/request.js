const requestV0 = require('../v0/request')

/**
 * EndTxn Request (Version: 2) => transactional_id producer_id producer_epoch committed
 *   transactional_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   committed => BOOLEAN
 *
 * The v0/v1 body. v2 only lets the broker answer PRODUCER_FENCED where it answered
 * INVALID_PRODUCER_EPOCH to v0 and v1.
 * @see clients/src/main/resources/common/message/EndTxnRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.transactionalId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {boolean} options.transactionResult true commits, false aborts
 * @returns {object} the protocol request
 */
module.exports = ({ transactionalId, producerId, producerEpoch, transactionResult }) =>
  Object.assign(requestV0({ transactionalId, producerId, producerEpoch, transactionResult }), {
    apiVersion: 2,
  })

module.exports.schema = {
  api: 'EndTxn',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'Committed', type: 'bool', versions: '0+' },
  ],
}
