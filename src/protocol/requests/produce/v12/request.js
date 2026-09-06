const requestV11 = require('../v11/request')

/**
 * ProduceRequest (Version: 12) - flexible, request header v2
 *
 * Wire-identical to v11. From v12 a produce inside a transaction implicitly adds the partition to
 * the transaction under transaction version 2 (KIP-890), which is why a transactional send from
 * this client stays below v12 until that path is implemented.
 * @see clients/src/main/resources/common/message/ProduceRequest.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 */
module.exports = (args) => Object.assign(requestV11(args), { apiVersion: 12 })

module.exports.schema = {
  api: 'Produce',
  version: 12,
  direction: 'request',
  fields: [
    { name: 'TransactionalId', type: 'string', versions: '3+', nullable: true },
    { name: 'Acks', type: 'int16', versions: '0+' },
    { name: 'TimeoutMs', type: 'int32', versions: '0+' },
    { name: 'TopicData', type: '[]TopicProduceData', versions: '0+' },
    { name: 'TopicData.Name', type: 'string', versions: '0-12' },
    { name: 'TopicData.PartitionData', type: '[]PartitionProduceData', versions: '0+' },
    { name: 'TopicData.PartitionData.Index', type: 'int32', versions: '0+' },
    { name: 'TopicData.PartitionData.Records', type: 'records', versions: '0+', nullable: true },
  ],
}
