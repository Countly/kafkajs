const Decoder = require('../../../decoder')
const { parse } = require('../v2/response')

/**
 * OffsetFetch Response (Version: 5) => throttle_time_ms [responses] error_code
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition offset committed_leader_epoch metadata error_code
 *       partition => INT32
 *       offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => NULLABLE_STRING
 *       error_code => INT16
 *   error_code => INT16
 *
 * v5 adds `CommittedLeaderEpoch`: the leader epoch of the *consumed record* whose offset was
 * committed. That is a different quantity from the current-metadata epoch this client's
 * `MetadataCache` tracks, so it is surfaced here additively and stored nowhere — writing it into
 * the routing cache would move that cache on a value unrelated to the current leader.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetFetchResponse.json (Apache Kafka 4.3.1)
 */

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const responses = decoder.readArray(decodeResponses)
  const errorCode = decoder.readInt16()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    responses,
    errorCode,
  }
}

const decodeResponses = (decoder) => ({
  topic: decoder.readString(),
  partitions: decoder.readArray(decodePartitions),
})

const decodePartitions = (decoder) => ({
  partition: decoder.readInt32(),
  offset: decoder.readInt64().toString(),
  committedLeaderEpoch: decoder.readInt32(),
  metadata: decoder.readString(),
  errorCode: decoder.readInt16(),
})

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'OffsetFetch',
  version: 5,
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
