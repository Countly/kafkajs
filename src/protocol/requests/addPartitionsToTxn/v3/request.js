const Encoder = require('../../../encoder')
const { AddPartitionsToTxn: apiKey } = require('../../apiKeys')

/**
 * AddPartitionsToTxn Request (Version: 3) — flexible (KIP-482), request header v2
 *
 * V3AndBelowTransactionalId => COMPACT_STRING
 * V3AndBelowProducerId => INT64
 * V3AndBelowProducerEpoch => INT16
 * V3AndBelowTopics => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY of INT32
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * The last version a client sends: from v4 the request batches several transactions and is
 * reserved for brokers verifying a partition is in a transaction. Under transaction version 2
 * (KIP-890) a client does not send it at all — Produce adds the partition implicitly.
 * @see clients/src/main/resources/common/message/AddPartitionsToTxnRequest.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 *
 * @param {object} options
 * @param {string} options.transactionalId
 * @param {string|number} options.producerId
 * @param {number} options.producerEpoch
 * @param {{ topic: string, partitions: number[] }[]} options.topics
 * @returns {object} the protocol request
 */
module.exports = ({ transactionalId, producerId, producerEpoch, topics }) => ({
  apiKey,
  apiVersion: 3,
  apiName: 'AddPartitionsToTxn',
  encode: async () =>
    new Encoder()
      .writeUVarIntString(transactionalId)
      .writeInt64(producerId)
      .writeInt16(producerEpoch)
      .writeUVarIntArray(topics.map(encodeTopic))
      .writeTaggedFields(),
})

const encodeTopic = ({ topic, partitions }) =>
  new Encoder().writeUVarIntString(topic).writeUVarIntArray(partitions, 'int32').writeTaggedFields()

module.exports.schema = {
  api: 'AddPartitionsToTxn',
  version: 3,
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
