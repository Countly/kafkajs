const { decode, parse } = require('../v6/response')

/**
 * ListOffsets Response (Version: 8) — flexible (KIP-482), response header v1
 *
 * v8 enables listing offsets by local log start offset (KIP-405, tiered storage); this client
 * never asks for it, so the wire shape and decoded result are byte-identical to v6.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
 * @see clients/src/main/resources/common/message/ListOffsetsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ListOffsets',
  version: 8,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '2+' },
    { name: 'Topics', type: '[]ListOffsetsTopicResponse', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+' },
    { name: 'Topics.Partitions', type: '[]ListOffsetsPartitionResponse', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Partitions.Timestamp', type: 'int64', versions: '1+' },
    { name: 'Topics.Partitions.Offset', type: 'int64', versions: '1+' },
    { name: 'Topics.Partitions.LeaderEpoch', type: 'int32', versions: '4+' },
  ],
}
