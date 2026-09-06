const requestV13 = require('../v13/request')

/**
 * FetchRequest (Version: 14) — flexible, request header v2
 *
 * Wire-identical to v13: v14 adds only the `OFFSET_MOVED_TO_TIERED_STORAGE` (109) error code to
 * the response, which is data rather than a field. Its own `apiVersion` is what makes a broker
 * answer at v14.
 * @see clients/src/main/resources/common/message/FetchRequest.json (Apache Kafka 4.3.1)
 * @param {object} args see `fetch/v13/request.js` — same shape, including `topicIds`
 * @returns {{apiKey: number, apiVersion: number, apiName: string, encode: () => Promise<import('../../../encoder')>}}
 */
module.exports = (args) => Object.assign(requestV13(args), { apiVersion: 14 })

module.exports.schema = {
  api: 'Fetch',
  version: 14,
  direction: 'request',
  fields: [
    { name: 'ClusterId', type: 'string', versions: '12+', nullable: true, tag: 0 },
    { name: 'ReplicaId', type: 'int32', versions: '0-14' },
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
    { name: 'ForgottenTopicsData', type: '[]ForgottenTopic', versions: '7+' },
    { name: 'ForgottenTopicsData.TopicId', type: 'uuid', versions: '13+' },
    { name: 'ForgottenTopicsData.Partitions', type: '[]int32', versions: '7+' },
    { name: 'RackId', type: 'string', versions: '11+' },
  ],
}
