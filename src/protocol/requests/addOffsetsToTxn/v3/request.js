const Encoder = require('../../../encoder')
const { AddOffsetsToTxn: apiKey } = require('../../apiKeys')

/**
 * AddOffsetsToTxn Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * TransactionalId => COMPACT_STRING
 * ProducerId => INT64
 * ProducerEpoch => INT16
 * GroupId => COMPACT_STRING
 * TAG_BUFFER
 *
 * Under transaction version 2 (KIP-890) a client does not send it: TxnOffsetCommit adds the
 * offsets to the transaction itself.
 * @see clients/src/main/resources/common/message/AddOffsetsToTxnRequest.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 *
 * @param {object} options
 * @param {string} options.transactionalId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {string} options.groupId
 * @returns {object} the protocol request
 */
module.exports = ({ transactionalId, producerId, producerEpoch, groupId }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'AddOffsetsToTxn',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(transactionalId)
      .writeInt64(producerId)
      .writeInt16(producerEpoch)
      .writeUVarIntString(groupId)
      .writeTaggedFields(),
})

module.exports.schema = {
  api: 'AddOffsetsToTxn',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '0+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '0+' },
    { name: 'GroupId', type: 'string', versions: '0+' },
  ],
}
