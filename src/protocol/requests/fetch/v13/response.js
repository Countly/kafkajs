const Decoder = require('../../../decoder')
const { buildDecodePartition } = require('../v12/response')
const { parse } = require('../v12/parse')
const decoderCache = require('../decoderCache')

/**
 * FetchResponse (Version: 13) — flexible, response header v1
 *
 * Identical to v12 except that each topic is keyed by `TopicId` (uuid) instead of `Topic`. The
 * name is not restored here: the codec has no logger and no per-request state, and the mapping
 * belongs to whoever built the request.
 * @see clients/src/main/resources/common/message/FetchResponse.json (Apache Kafka 4.3.1)
 */
const buildDecode = (options) => {
  const decodePartition = buildDecodePartition(options)

  const decodeResponse = async (decoder) => {
    const topicId = decoder.readUuid()
    const partitions = Decoder.requireArray(
      await decoder.readUVarIntArrayAsync(decodePartition),
      'FetchResponse.Responses.Partitions'
    )
    decoder.readTaggedFields()
    return { topicId, partitions }
  }

  return async (rawData) => {
    const decoder = new Decoder(rawData)
    const clientSideThrottleTime = decoder.readInt32()
    const errorCode = decoder.readInt16()
    const sessionId = decoder.readInt32()
    const responses = Decoder.requireArray(
      await decoder.readUVarIntArrayAsync(decodeResponse),
      'FetchResponse.Responses'
    )
    decoder.readTaggedFields()

    return { throttleTime: 0, clientSideThrottleTime, errorCode, sessionId, responses }
  }
}

// Module-local, as in v12: nothing outside this file reads them.
const decoderFor = decoderCache(buildDecode)

/**
 * @param {Buffer} rawData
 * @param {{ checkCrcs?: boolean, maxDecompressedBatchBytes?: number }} [options]
 * @returns {Promise<object>}
 */
const decode = (rawData, options = {}) => decoderFor(options)(rawData)

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Fetch',
  version: 13,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '7+' },
    { name: 'SessionId', type: 'int32', versions: '7+' },
    { name: 'Responses', type: '[]FetchableTopicResponse', versions: '0+' },
    { name: 'Responses.TopicId', type: 'uuid', versions: '13+' },
    { name: 'Responses.Partitions', type: '[]PartitionData', versions: '0+' },
    { name: 'Responses.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Responses.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Responses.Partitions.HighWatermark', type: 'int64', versions: '0+' },
    { name: 'Responses.Partitions.LastStableOffset', type: 'int64', versions: '4+' },
    { name: 'Responses.Partitions.LogStartOffset', type: 'int64', versions: '5+' },
    {
      name: 'Responses.Partitions.DivergingEpoch',
      type: 'EpochEndOffset',
      versions: '12+',
      tag: 0,
    },
    { name: 'Responses.Partitions.DivergingEpoch.Epoch', type: 'int32', versions: '12+' },
    { name: 'Responses.Partitions.DivergingEpoch.EndOffset', type: 'int64', versions: '12+' },
    {
      name: 'Responses.Partitions.CurrentLeader',
      type: 'LeaderIdAndEpoch',
      versions: '12+',
      tag: 1,
    },
    { name: 'Responses.Partitions.CurrentLeader.LeaderId', type: 'int32', versions: '12+' },
    { name: 'Responses.Partitions.CurrentLeader.LeaderEpoch', type: 'int32', versions: '12+' },
    { name: 'Responses.Partitions.SnapshotId', type: 'SnapshotId', versions: '12+', tag: 2 },
    { name: 'Responses.Partitions.SnapshotId.EndOffset', type: 'int64', versions: '0+' },
    { name: 'Responses.Partitions.SnapshotId.Epoch', type: 'int32', versions: '0+' },
    {
      name: 'Responses.Partitions.AbortedTransactions',
      type: '[]AbortedTransaction',
      versions: '4+',
      nullable: true,
    },
    { name: 'Responses.Partitions.AbortedTransactions.ProducerId', type: 'int64', versions: '4+' },
    { name: 'Responses.Partitions.AbortedTransactions.FirstOffset', type: 'int64', versions: '4+' },
    { name: 'Responses.Partitions.PreferredReadReplica', type: 'int32', versions: '11+' },
    { name: 'Responses.Partitions.Records', type: 'records', versions: '0+', nullable: true },
  ],
}
