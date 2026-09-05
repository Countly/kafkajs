const { decode, parse } = require('../v6/response')

/**
 * ListOffsets Response (Version: 7) — flexible (KIP-482), response header v1
 *
 * v7 is the same as v6 (KIP-734): the MAX_TIMESTAMP sentinel this version enables is a request
 * concept, and this client never sends it, so the wire shape and decoded result are
 * byte-identical to v6.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-734%3A+Improve+AdminClient.listOffsets+to+return+timestamp+and+offset+for+the+record+with+the+largest+timestamp
 * @see clients/src/main/resources/common/message/ListOffsetsResponse.json (Apache Kafka 4.3.1)
 */
module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'ListOffsets',
  version: 7,
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
