const requestV3 = require('../v3/request')

/**
 * EndTxn Request (Version: 5) — flexible (KIP-482), request header v2
 *
 * The v3 body. Under transaction version 2 (KIP-890 part 2) the coordinator bumps the producer
 * epoch at the end of every transaction and answers with the new producer id and epoch.
 * @see clients/src/main/resources/common/message/EndTxnRequest.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
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
    apiVersion: 5,
  })

module.exports.schema = {
  api: 'EndTxn',
  version: 5,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'Committed', type: 'bool', versions: '0+' },
  ],
}
