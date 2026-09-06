const requestV17 = require('../v17/request')

/**
 * FetchRequest (Version: 18) — flexible, KIP-1166
 *
 * Wire-identical to v17: KIP-1166 adds `Topics.Partitions.HighWatermark` (partition tag 1), a
 * replica-only field this client never sends.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1166 (Add HighWatermark to the FetchRequest)
 * @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
 * @param {object} args see `fetch/v17/request.js` — same shape
 * @returns {{apiKey: number, apiVersion: number, apiName: string, encode: () => Promise<import('../../../encoder')>}}
 */
module.exports = (args) => Object.assign(requestV17(args), { apiVersion: 18 })

module.exports.schema = {
  api: 'Fetch',
  version: 18,
  direction: 'request',
  fields: [
    { name: 'ClusterId', type: 'string', versions: '12+', nullable: true, tag: 0 },
    { name: 'ReplicaState', type: 'ReplicaState', versions: '15+', tag: 1 },
    { name: 'ReplicaState.ReplicaId', type: 'int32', versions: '15+' },
    { name: 'ReplicaState.ReplicaEpoch', type: 'int64', versions: '15+' },
    { name: 'MaxWaitMs', type: 'int32', versions: '0+' },
    { name: 'MinBytes', type: 'int32', versions: '0+' },
    { name: 'MaxBytes', type: 'int32', versions: '3+' },
    { name: 'IsolationLevel', type: 'int8', versions: '4+' },
    { name: 'SessionId', type: 'int32', versions: '7+' },
    { name: 'SessionEpoch', type: 'int32', versions: '7+' },
    { name: 'Topics', type: '[]FetchTopic', versions: '0+' },
    { name: 'Topics.TopicId', type: 'uuid', versions: '13+' },
    { name: 'Topics.Partitions', type: '[]FetchPartition', versions: '0+' },
    { name: 'Topics.Partitions.Partition', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.CurrentLeaderEpoch', type: 'int32', versions: '9+' },
    { name: 'Topics.Partitions.FetchOffset', type: 'int64', versions: '0+' },
    { name: 'Topics.Partitions.LastFetchedEpoch', type: 'int32', versions: '12+' },
    { name: 'Topics.Partitions.LogStartOffset', type: 'int64', versions: '5+' },
    { name: 'Topics.Partitions.PartitionMaxBytes', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ReplicaDirectoryId', type: 'uuid', versions: '17+', tag: 0 },
    { name: 'Topics.Partitions.HighWatermark', type: 'int64', versions: '18+', tag: 1 },
    { name: 'ForgottenTopicsData', type: '[]ForgottenTopic', versions: '7+' },
    { name: 'ForgottenTopicsData.TopicId', type: 'uuid', versions: '13+' },
    { name: 'ForgottenTopicsData.Partitions', type: '[]int32', versions: '7+' },
    { name: 'RackId', type: 'string', versions: '11+' },
  ],
}
