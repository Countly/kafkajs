const Encoder = require('../../../encoder')
const { InitProducerId: apiKey } = require('../../apiKeys')

/**
 * The producer id and epoch a client would send to disambiguate a reused transactional id after
 * a fencing. This client does not yet bump a fenced producer's epoch, so both fields carry their
 * own defaults, which is what a client that has never held a producer id sends.
 * @see clients/src/main/resources/common/message/InitProducerIdRequest.json
 */
const NO_PRODUCER_ID = '-1'
const NO_PRODUCER_EPOCH = -1

/**
 * InitProducerId Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * Same wire shape as v3. This version widens the response's error set with `PRODUCER_FENCED`
 * (90), sent where the broker previously answered `INVALID_PRODUCER_EPOCH`; the client's error
 * table already has `PRODUCER_FENCED` marked non-retriable, so it surfaces unchanged.
 *
 * TransactionalId => COMPACT_NULLABLE_STRING
 * TransactionTimeoutMs => INT32
 * ProducerId => INT64 (default -1)
 * ProducerEpoch => INT16 (default -1)
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
  apiVersion: 4,
  apiName: 'InitProducerId',
  encode: async () => {
    return new Encoder()
      .writeUVarIntString(transactionalId)
      .writeInt32(transactionTimeout)
      .writeInt64(NO_PRODUCER_ID)
      .writeInt16(NO_PRODUCER_EPOCH)
      .writeTaggedFields()
  },
})

module.exports.schema = {
  api: 'InitProducerId',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '0+', nullable: true },
    { name: 'TransactionTimeoutMs', type: 'int32', versions: '0+' },
    { name: 'ProducerId', type: 'int64', versions: '3+' },
    { name: 'ProducerEpoch', type: 'int16', versions: '3+' },
  ],
}
