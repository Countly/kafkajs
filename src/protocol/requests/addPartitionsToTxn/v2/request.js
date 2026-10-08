const requestV0 = require('../v0/request')

/**
 * AddPartitionsToTxn Request (Version: 2) => transactional_id producer_id producer_epoch [topics]
 *   transactional_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 *
 * The v0/v1 body. v2 only lets the broker answer PRODUCER_FENCED where it answered
 * INVALID_PRODUCER_EPOCH to v0 and v1.
 * @see clients/src/main/resources/common/message/AddPartitionsToTxnRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.transactionalId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {{ topic: string, partitions: number[] }[]} options.topics
 * @returns {object} the protocol request
 */
module.exports = ({ transactionalId, producerId, producerEpoch, topics }) =>
  Object.assign(requestV0({ transactionalId, producerId, producerEpoch, topics }), {
    apiVersion: 2,
  })

module.exports.schema = {
  api: 'AddPartitionsToTxn',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'V3AndBelowTransactionalId', type: 'string', versions: '0-3' },
    { name: 'V3AndBelowProducerId', type: 'int64', versions: '0-3' },
    { name: 'V3AndBelowProducerEpoch', type: 'int16', versions: '0-3' },
    { name: 'V3AndBelowTopics', type: '[]AddPartitionsToTxnTopic', versions: '0-3' },
    { name: 'V3AndBelowTopics.Name', type: 'string', versions: '0+' },
    { name: 'V3AndBelowTopics.Partitions', type: '[]int32', versions: '0+' },
  ],
}
