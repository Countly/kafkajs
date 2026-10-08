const Encoder = require('../../../encoder')
const { InitProducerId: apiKey } = require('../../apiKeys')

/**
 * InitProducerId Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * TransactionalId => COMPACT_NULLABLE_STRING
 * TransactionTimeoutMs => INT32
 * ProducerId => INT64 (default -1)
 * ProducerEpoch => INT16 (default -1)
 * TAG_BUFFER
 *
 * v3 adds `ProducerId` and `ProducerEpoch` (KIP-360), so a producer can resume after an
 * INVALID_PRODUCER_EPOCH error: a re-initialization sends the pair it holds, which the coordinator
 * checks against its own before it bumps the epoch. A first initialization sends the -1 defaults.
 * @see https://kafka.apache.org/43/design/protocol
 * @see https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89068820
 * @see clients/src/main/resources/common/message/InitProducerIdRequest.json
 *
 * @param {object} options
 * @param {string|null} [options.transactionalId] null for a non-transactional (idempotent-only) producer
 * @param {number} options.transactionTimeout
 * @param {string|number} [options.producerId=-1] the producer id held: the decimal string a
 *   response decodes to, or a number; -1 on a first initialization
 * @param {number} [options.producerEpoch=-1] the epoch held; -1 on a first initialization
 * @returns {object} the protocol request
 */
module.exports = ({
  transactionalId,
  transactionTimeout,
  producerId = -1,
  producerEpoch = -1,
}) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'InitProducerId',
  encode: async () => {
    return new Encoder()
      .writeUVarIntString(transactionalId)
      .writeInt32(transactionTimeout)
      .writeInt64(producerId)
      .writeInt16(producerEpoch)
      .writeTaggedFields()
  },
})

module.exports.schema = {
  api: 'InitProducerId',
  version: 3,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+', nullable: true },
    { name: 'TransactionTimeoutMs', type: 'int32', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '3+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '3+' },
  ],
}
