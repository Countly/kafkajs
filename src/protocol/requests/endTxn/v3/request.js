const Encoder = require('../../../encoder')
const { EndTxn: apiKey } = require('../../apiKeys')

/**
 * EndTxn Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * TransactionalId => COMPACT_STRING
 * ProducerId => INT64
 * ProducerEpoch => INT16
 * Committed => BOOLEAN
 * TAG_BUFFER
 * @see clients/src/main/resources/common/message/EndTxnRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options
 * @param {string} options.transactionalId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {boolean} options.transactionResult true commits, false aborts
 * @returns {object} the protocol request
 */
module.exports = ({ transactionalId, producerId, producerEpoch, transactionResult }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'EndTxn',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(transactionalId)
      .writeInt64(producerId)
      .writeInt16(producerEpoch)
      .writeBoolean(transactionResult)
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'EndTxn',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'Committed', type: 'bool', versions: '0+' },
  ],
}
