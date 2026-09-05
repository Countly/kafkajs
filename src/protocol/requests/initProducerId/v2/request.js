const Encoder = require('../../../encoder')
const { InitProducerId: apiKey } = require('../../apiKeys')

/**
 * InitProducerId Request (Version: 2) — flexible (KIP-482), request header v2
 *
 * TransactionalId => COMPACT_NULLABLE_STRING
 * TransactionTimeoutMs => INT32
 * TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/InitProducerIdRequest.json
 *
 * @param {object} options
 * @param {string} [options.transactionalId] null for a non-transactional (idempotent-only) producer
 * @param {number} options.transactionTimeout
 */
module.exports = ({ transactionalId, transactionTimeout }) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'InitProducerId',
  encode: async () => {
    return new Encoder()
      .writeUVarIntString(transactionalId)
      .writeInt32(transactionTimeout)
      .writeTaggedFields()
  },
})

module.exports.schema = {
  api: 'InitProducerId',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+', nullable: true },
    { name: 'TransactionTimeoutMs', type: 'int32', versions: '0+' },
  ],
}
