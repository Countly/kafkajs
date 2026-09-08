const { decode, parse } = require('../v8/response')

/**
 * OffsetFetch Response (Version: 9) — flexible (KIP-482), response header v1
 *
 * The body is v8's, field for field, so v8's `decode` and `parse` are re-exported rather than
 * copied — including the flattening of the single `Groups` element into the `responses` shape
 * every earlier version returns. v9 only widens what the group-level `ErrorCode` may hold: a
 * consumer-protocol fetch can come back with STALE_MEMBER_EPOCH (113) or UNKNOWN_MEMBER_ID (25).
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @see clients/src/main/resources/common/message/OffsetFetchResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'OffsetFetch',
  version: 9,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '3+' },
    { name: 'Groups', type: '[]OffsetFetchResponseGroup', versions: '8+' },
    { name: 'Groups.GroupId', type: 'string', versions: '8+' },
    { name: 'Groups.Topics', type: '[]OffsetFetchResponseTopics', versions: '8+' },
    { name: 'Groups.Topics.Name', type: 'string', versions: '8-9' },
    { name: 'Groups.Topics.Partitions', type: '[]OffsetFetchResponsePartitions', versions: '8+' },
    { name: 'Groups.Topics.Partitions.PartitionIndex', type: 'int32', versions: '8+' },
    { name: 'Groups.Topics.Partitions.CommittedOffset', type: 'int64', versions: '8+' },
    { name: 'Groups.Topics.Partitions.CommittedLeaderEpoch', type: 'int32', versions: '8+' },
    { name: 'Groups.Topics.Partitions.Metadata', type: 'string', versions: '8+', nullable: true },
    { name: 'Groups.Topics.Partitions.ErrorCode', type: 'int16', versions: '8+' },
    { name: 'Groups.ErrorCode', type: 'int16', versions: '8+' },
  ],
}
