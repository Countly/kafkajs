const { decode, parse } = require('../v17/response')

/**
 * FetchResponse (Version: 18) — flexible, KIP-1166
 *
 * Wire-identical to v17 — KIP-1166 only touches the request. `decode`/`parse` are reused
 * unchanged.
 * @see clients/src/main/resources/common/message/FetchResponse.json (Apache Kafka 4.3.1)
 */

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Fetch',
  version: 18,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '7+' },
    { name: 'SessionId', type: 'int32', versions: '7+' },
    { name: 'Responses', type: '[]FetchableTopicResponse', versions: '0+' },
    { name: 'Responses.TopicId', type: 'uuid', versions: '13+' },
    { name: 'Responses.Partitions', type: '[]PartitionData', versions: '0+' },
    { name: 'Responses.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Responses.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.Partitions.HighWatermark', type: 'int64', versions: '0+' },
    { name: 'Responses.Partitions.LastStableOffset', type: 'int64', versions: '4+' },
    { name: 'Responses.Partitions.LogStartOffset', type: 'int64', versions: '5+' },
    {
      name: 'Responses.Partitions.DivergingEpoch',
      type: 'EpochEndOffset',
      versions: '12+',
      tag: 0,
    },
    { name: 'Responses.Partitions.DivergingEpoch.Epoch', type: 'int32', versions: '12+' },
    { name: 'Responses.Partitions.DivergingEpoch.EndOffset', type: 'int64', versions: '12+' },
    {
      name: 'Responses.Partitions.CurrentLeader',
      type: 'LeaderIdAndEpoch',
      versions: '12+',
      tag: 1,
    },
    { name: 'Responses.Partitions.CurrentLeader.LeaderId', type: 'int32', versions: '12+' },
    { name: 'Responses.Partitions.CurrentLeader.LeaderEpoch', type: 'int32', versions: '12+' },
    { name: 'Responses.Partitions.SnapshotId', type: 'SnapshotId', versions: '12+', tag: 2 },
    { name: 'Responses.Partitions.SnapshotId.EndOffset', type: 'int64', versions: '0+' },
    { name: 'Responses.Partitions.SnapshotId.Epoch', type: 'int32', versions: '0+' },
    {
      name: 'Responses.Partitions.AbortedTransactions',
      type: '[]AbortedTransaction',
      versions: '4+',
      nullable: true,
    },
    { name: 'Responses.Partitions.AbortedTransactions.ProducerId', type: 'int64', versions: '4+' },
    { name: 'Responses.Partitions.AbortedTransactions.FirstOffset', type: 'int64', versions: '4+' },
    { name: 'Responses.Partitions.PreferredReadReplica', type: 'int32', versions: '11+' },
    { name: 'Responses.Partitions.Records', type: 'records', versions: '0+', nullable: true },
    { name: 'NodeEndpoints', type: '[]NodeEndpoint', versions: '16+', tag: 0 },
    { name: 'NodeEndpoints.NodeId', type: 'int32', versions: '16+' },
    { name: 'NodeEndpoints.Host', type: 'string', versions: '16+' },
    { name: 'NodeEndpoints.Port', type: 'int32', versions: '16+' },
    { name: 'NodeEndpoints.Rack', type: 'string', versions: '16+', nullable: true },
  ],
}
