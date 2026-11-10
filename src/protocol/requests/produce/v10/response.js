const Decoder = require('../../../decoder')
const { parse } = require('../partitionResults')

/**
 * ProduceResponse (Version: 10) — flexible (KIP-482), response header v1
 *
 * Responses => COMPACT_ARRAY
 *   Name => COMPACT_STRING
 *   PartitionResponses => COMPACT_ARRAY
 *     Index => INT32
 *     ErrorCode => INT16
 *     BaseOffset => INT64
 *     LogAppendTimeMs => INT64
 *     LogStartOffset => INT64
 *     RecordErrors => COMPACT_ARRAY
 *       BatchIndex => INT32
 *       BatchIndexErrorMessage => COMPACT_NULLABLE_STRING
 *       TAG_BUFFER
 *     ErrorMessage => COMPACT_NULLABLE_STRING
 *     TAG_BUFFER
 *       tag 0 CurrentLeader => LeaderId(INT32) LeaderEpoch(INT32)
 *   TAG_BUFFER
 * ThrottleTimeMs => INT32
 * TAG_BUFFER
 *   tag 0 NodeEndpoints => COMPACT_ARRAY of NodeId(INT32) Host(COMPACT_STRING) Port(INT32)
 *                          Rack(COMPACT_NULLABLE_STRING) TAG_BUFFER
 *
 * v10 adds two KIP-951 tagged fields, both tag 0 but in different structs: `CurrentLeader` on
 * each partition response and `NodeEndpoints` at the top level. The broker only populates them on
 * `NOT_LEADER_OR_FOLLOWER` (6) or `FENCED_LEADER_EPOCH` (74), so a decode without them costs
 * nothing beyond the empty tagged-field read every flexible struct already pays for.
 * @see https://kafka.apache.org/43/design/protocol
 * @see clients/src/main/resources/common/message/ProduceResponse.json (Apache Kafka 4.3.1)
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
 */

// Both are tag 0, but in different structs — the partition response and the top-level response —
// so they share a number, not a namespace.
const PARTITION_CURRENT_LEADER_TAG = 0
const RESPONSE_NODE_ENDPOINTS_TAG = 0

const decodeRecordError = (decoder) => {
  const recordError = {
    batchIndex: decoder.readInt32(),
    batchIndexErrorMessage: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return recordError
}

/**
 * @param {Map<number, Buffer>} tags the partition struct's tagged-field section
 * @returns {{ leaderId: number, leaderEpoch: number }|null} `null` when the broker didn't send
 *   the hint, which is the common case — every retriable-but-not-leadership error, and every
 *   success
 */
const decodeCurrentLeader = (tags) => {
  const raw = tags.get(PARTITION_CURRENT_LEADER_TAG)

  if (!raw) {
    return null
  }

  const decoder = new Decoder(raw)
  return { leaderId: decoder.readInt32(), leaderEpoch: decoder.readInt32() }
}

/**
 * @param {Map<number, Buffer>} tags the response body's tagged-field section
 * @returns {{ nodeId: number, host: string, port: number, rack: string|null }[]} empty when the
 *   broker sent no hint
 */
const decodeNodeEndpoints = (tags) => {
  const raw = tags.get(RESPONSE_NODE_ENDPOINTS_TAG)

  if (!raw) {
    return []
  }

  return (
    new Decoder(raw).readUVarIntArray((decoder) => {
      const endpoint = {
        nodeId: decoder.readInt32(),
        host: decoder.readUVarIntString(),
        port: decoder.readInt32(),
        rack: decoder.readUVarIntString(),
      }
      decoder.readTaggedFields()
      return endpoint
    }) || []
  )
}

const decodePartition = (decoder) => {
  const partition = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const baseOffset = decoder.readInt64().toString()
  const logAppendTime = decoder.readInt64().toString()
  const logStartOffset = decoder.readInt64().toString()
  const recordErrors = Decoder.requireArray(
    decoder.readUVarIntArray(decodeRecordError),
    'ProduceResponse.Responses.PartitionResponses.RecordErrors'
  )
  const errorMessage = decoder.readUVarIntString()
  const currentLeader = decodeCurrentLeader(decoder.readTaggedFields())

  return {
    partition,
    errorCode,
    baseOffset,
    logAppendTime,
    logStartOffset,
    recordErrors,
    errorMessage,
    currentLeader,
  }
}

const decodeTopic = (decoder) => {
  const topic = {
    topicName: decoder.readUVarIntString(),
    partitions: Decoder.requireArray(
      decoder.readUVarIntArray(decodePartition),
      'ProduceResponse.Responses.PartitionResponses'
    ),
  }

  decoder.readTaggedFields()
  return topic
}

/**
 * @param {Buffer} rawData
 * @returns {Promise<object>} decoded response, keyed the same as v9 plus additive
 *   `partition.currentLeader` (`null` when absent) and top-level `nodeEndpoints` (`[]` when
 *   absent)
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const topics = Decoder.requireArray(
    decoder.readUVarIntArray(decodeTopic),
    'ProduceResponse.Responses'
  )
  const throttleTime = decoder.readInt32()
  const tags = decoder.readTaggedFields()

  return {
    topics,
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    nodeEndpoints: decodeNodeEndpoints(tags),
  }
}

module.exports = {
  decode,
  parse,
  // v13 keys each topic by id instead of by name, so it decodes `TopicId` itself and reuses these
  // three helpers unchanged rather than duplicating them.
  decodePartition,
  decodeNodeEndpoints,
  RESPONSE_NODE_ENDPOINTS_TAG,
}

module.exports.schema = {
  api: 'Produce',
  version: 10,
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
