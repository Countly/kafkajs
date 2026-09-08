const { decode, parse } = require('../v8/response')

/**
 * OffsetCommitResponse (Version: 9) — flexible (KIP-482), response header v1
 *
 * The body is v8's, field for field, so v8's `decode` and `parse` are re-exported rather than
 * copied. v9 only widens what `ErrorCode` may hold: a consumer-protocol commit can come back with
 * STALE_MEMBER_EPOCH (113) or GROUP_ID_NOT_FOUND (69), which are values of a field that already
 * existed, not new fields.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 * @see clients/src/main/resources/common/message/OffsetCommitResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'OffsetCommit',
  version: 9,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '3+' },
    { name: 'Topics', type: '[]OffsetCommitResponseTopic', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0-9' },
    { name: 'Topics.Partitions', type: '[]OffsetCommitResponsePartition', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
  ],
}
