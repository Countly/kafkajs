const { decode, parse } = require('../v6/response')

/**
 * OffsetFetch Response (Version: 7) — flexible (KIP-482), response header v1
 *
 * v7 adds no wire fields (the pending-commit partition error is a broker-side behaviour change,
 * signalled through the existing `errorCode`); the wire shape and decoded result are
 * byte-identical to v6.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetFetchResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'OffsetFetch',
  version: 7,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '3+' },
    { name: 'Topics', type: '[]OffsetFetchResponseTopic', versions: '0-7' },
    { name: 'Topics.Name', type: 'string', versions: '0-7' },
    { name: 'Topics.Partitions', type: '[]OffsetFetchResponsePartition', versions: '0-7' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0-7' },
    { name: 'Topics.Partitions.CommittedOffset', type: 'int64', versions: '0-7' },
    { name: 'Topics.Partitions.CommittedLeaderEpoch', type: 'int32', versions: '5-7' },
    { name: 'Topics.Partitions.Metadata', type: 'string', versions: '0-7', nullable: true },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0-7' },
    { name: 'ErrorCode', type: 'int16', versions: '2-7' },
  ],
}
