const { decode, parse } = require('../v10/response')

/**
 * ProduceResponse (Version: 11) — flexible (KIP-482), response header v1
 *
 * Wire-identical to v10 — v11 adds no new fields to either the request or the response. It exists
 * only so a transactional produce can be answered with `TRANSACTION_ABORTABLE` (120, KIP-890)
 * instead of a version that predates the error code; `decode`/`parse` are reused unchanged.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ProduceResponse.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 */

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Produce',
  version: 11,
  direction: 'response',
  fields: [
    { name: 'Responses', type: '[]TopicProduceResponse', versions: '0+' },
    { name: 'Responses.Name', type: 'string', versions: '0-12' },
    { name: 'Responses.PartitionResponses', type: '[]PartitionProduceResponse', versions: '0+' },
    { name: 'Responses.PartitionResponses.Index', type: 'int32', versions: '0+' },
    { name: 'Responses.PartitionResponses.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.PartitionResponses.BaseOffset', type: 'int64', versions: '0+' },
    { name: 'Responses.PartitionResponses.LogAppendTimeMs', type: 'int64', versions: '2+' },
    { name: 'Responses.PartitionResponses.LogStartOffset', type: 'int64', versions: '5+' },
    {
      name: 'Responses.PartitionResponses.RecordErrors',
      type: '[]BatchIndexAndErrorMessage',
      versions: '8+',
    },
    { name: 'Responses.PartitionResponses.RecordErrors.BatchIndex', type: 'int32', versions: '8+' },
    {
      name: 'Responses.PartitionResponses.RecordErrors.BatchIndexErrorMessage',
      type: 'string',
      versions: '8+',
      nullable: true,
    },
    {
      name: 'Responses.PartitionResponses.ErrorMessage',
      type: 'string',
      versions: '8+',
      nullable: true,
    },
    {
      name: 'Responses.PartitionResponses.CurrentLeader',
      type: 'LeaderIdAndEpoch',
      versions: '10+',
      tag: 0,
    },
    {
      name: 'Responses.PartitionResponses.CurrentLeader.LeaderId',
      type: 'int32',
      versions: '10+',
    },
    {
      name: 'Responses.PartitionResponses.CurrentLeader.LeaderEpoch',
      type: 'int32',
      versions: '10+',
    },
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'NodeEndpoints', type: '[]NodeEndpoint', versions: '10+', tag: 0 },
    { name: 'NodeEndpoints.NodeId', type: 'int32', versions: '10+' },
    { name: 'NodeEndpoints.Host', type: 'string', versions: '10+' },
    { name: 'NodeEndpoints.Port', type: 'int32', versions: '10+' },
    { name: 'NodeEndpoints.Rack', type: 'string', versions: '10+', nullable: true },
  ],
}
