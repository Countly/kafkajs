const requestV3 = require('../v3/request')

/**
 * EndTxn Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * The v3 body. v4 only lets the broker answer TRANSACTION_ABORTABLE (KIP-890). It is the highest
 * version a transaction may use while its coordinator runs transaction version 1.
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
  Object.assign(requestV3({ transactionalId, producerId, producerEpoch, transactionResult }), {
    apiVersion: 4,
  })

module.exports.schema = {
  api: 'EndTxn',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'Committed', type: 'bool', versions: '0+' },
  ],
}
