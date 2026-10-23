const requestV3 = require('../v3/request')

/**
 * ProduceRequest (Version: 8)
 *
 * KIP-467 only changes the response (RecordErrors, ErrorMessage); the request wire shape is
 * identical to v3-v7.
 *
 * TransactionalId => NULLABLE_STRING
 * Acks => INT16
 * TimeoutMs => INT32
 * TopicData => [Name [PartitionData]]
 *   Name => STRING
 *   PartitionData => Index Records
 *     Index => INT32
 *     Records => RECORDS
 *
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ProduceRequest.json (Apache Kafka 4.3.1)
 *
 * @param {object} options see `../v3/request` for the full option list
 */
module.exports = ({
  acks,
  timeout,
  transactionalId,
  producerId,
  producerEpoch,
  compression,
  compressionLevel,
  topicData,
}) =>
  Object.assign(
    requestV3({
      acks,
      timeout,
      transactionalId,
      producerId,
      producerEpoch,
      compression,
      compressionLevel,
      topicData,
    }),
    { apiVersion: 8 }
  )

module.exports.schema = {
  api: 'Produce',
  version: 8,
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
