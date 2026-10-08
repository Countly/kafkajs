const requestV0 = require('../v0/request')

/**
 * AddOffsetsToTxn Request (Version: 2) => transactional_id producer_id producer_epoch group_id
 *   transactional_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   group_id => STRING
 *
 * The v0/v1 body. v2 only lets the broker answer PRODUCER_FENCED where it answered
 * INVALID_PRODUCER_EPOCH to v0 and v1.
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
  Object.assign(requestV0({ transactionalId, producerId, producerEpoch, groupId }), {
    apiVersion: 2,
  })

module.exports.schema = {
  api: 'AddOffsetsToTxn',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'GroupId', type: 'string', versions: '0+' },
  ],
}
