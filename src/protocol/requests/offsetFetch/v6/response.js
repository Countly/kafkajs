const Decoder = require('../../../decoder')
const { parse } = require('../v2/response')

/**
 * OffsetFetch Response (Version: 6) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * Topics => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY
 *     PartitionIndex => INT32
 *     CommittedOffset => INT64
 *     CommittedLeaderEpoch => INT32
 *     Metadata => COMPACT_NULLABLE_STRING
 *     ErrorCode => INT16
 *     TAG_BUFFER
 *   TAG_BUFFER
 * ErrorCode => INT16
 * TAG_BUFFER
 *
 * v6 re-encodes v5's fields as compact with trailing tag buffers; the decoded shape stays
 * identical to v5 (`responses[].topic`/`partitions[].partition`/`.committedLeaderEpoch`/etc).
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetFetchResponse.json (Apache Kafka 4.3.1)
 */

const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const responses = decoder.readUVarIntArray(decodeResponses) || []
  const errorCode = decoder.readInt16()

  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    responses,
    errorCode,
  }
}

const decodeResponses = (decoder) => {
  const response = {
    topic: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartitions) || [],
  }

  decoder.readTaggedFields()
  return response
}

const decodePartitions = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
    offset: decoder.readInt64().toString(),
    committedLeaderEpoch: decoder.readInt32(),
    metadata: decoder.readUVarIntString(),
    errorCode: decoder.readInt16(),
  }

  decoder.readTaggedFields()
  return partition
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'OffsetFetch',
  version: 6,
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
