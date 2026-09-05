const Decoder = require('../../../decoder')
const { parse } = require('../v2/response')

/**
 * OffsetCommitResponse (Version: 8) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32
 * Topics => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   Partitions => COMPACT_ARRAY
 *     PartitionIndex => INT32
 *     ErrorCode => INT16
 *     TAG_BUFFER
 *   TAG_BUFFER
 * TAG_BUFFER
 *
 * v8 re-encodes v7's fields as compact with trailing tag buffers; the decoded shape stays
 * identical to v4 (`responses[].topic`/`partitions[].partition`/`partitions[].errorCode`).
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/OffsetCommitResponse.json (Apache Kafka 4.3.1)
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const responses = decoder.readUVarIntArray(decodeResponses)

  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    responses,
  }
}

const decodeResponses = (decoder) => {
  const response = {
    topic: decoder.readUVarIntString(),
    partitions: decoder.readUVarIntArray(decodePartitions),
  }

  decoder.readTaggedFields()
  return response
}

const decodePartitions = (decoder) => {
  const partition = {
    partition: decoder.readInt32(),
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
  api: 'OffsetCommit',
  version: 8,
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
