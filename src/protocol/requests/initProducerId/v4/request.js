const requestV3 = require('../v3/request')

/**
 * InitProducerId Request (Version: 4) — flexible (KIP-482), request header v2
 *
 * v3's body under its own version number. This version widens the response's error set with
 * `PRODUCER_FENCED` (90), sent where the broker previously answered `INVALID_PRODUCER_EPOCH`; the
 * client's error table already has `PRODUCER_FENCED` marked non-retriable, so it surfaces
 * unchanged.
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
 * @param {object} options as for v3
 * @param {string|null} [options.transactionalId] null for a non-transactional (idempotent-only) producer
 * @param {number} options.transactionTimeout
 * @param {string|number} [options.producerId=-1] the producer id held; -1 on a first initialization
 * @param {number} [options.producerEpoch=-1] the epoch held; -1 on a first initialization
 * @returns {object} the protocol request
 */
module.exports = ({ transactionalId, transactionTimeout, producerId, producerEpoch }) =>
  Object.assign(requestV3({ transactionalId, transactionTimeout, producerId, producerEpoch }), {
    apiVersion: 4,
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
