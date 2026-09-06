const Decoder = require('../../../decoder')
const { buildDecodePartition } = require('../v12/response')
const { parse } = require('../v12/parse')

// NodeEndpoints is tag 0 on the response body; Produce uses tag 0 for its own body field too —
// the two APIs share a number, not a namespace.
const RESPONSE_NODE_ENDPOINTS_TAG = 0

/**
 * Decodes the KIP-951 leader hint carried in the response body's tagged-field section.
 * @param {Map<number, Buffer>} tags the response body's tagged-field section
 * @returns {{ nodeId: number, host: string, port: number, rack: string|null }[]} empty when the
 *   broker sent no hint, which is every response except a leadership error
 */
const decodeNodeEndpoints = (tags) => {
  const raw = tags.get(RESPONSE_NODE_ENDPOINTS_TAG)
  if (!raw) return []

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

/**
 * FetchResponse (Version: 16) — flexible, KIP-951
 *
 * Identical to v15 apart from one addition: `NodeEndpoints` (tag 0) on the response body, hinting
 * at the host/port of a leader named in a partition's `CurrentLeader` when this client's metadata
 * is stale. Decoding the hint is the wire half only; applying it is `Cluster#applyLeaderHint`'s
 * job. Builds its own decoder pair — rather than re-exporting v15's — because v15's decode already
 * discards the response's tag map before returning, so there is nowhere to read tag 0 from.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client
 * @see clients/src/main/resources/common/message/FetchResponse.json (Apache Kafka 4.3.1)
 * @param {{ checkCrcs: boolean }} options
 * @returns {(rawData: Buffer) => Promise<object>}
 */
const buildDecode = (options) => {
  const decodePartition = buildDecodePartition(options)

  const decodeResponse = async (decoder) => {
    const topicId = decoder.readUuid()
    const partitions = await decoder.readUVarIntArrayAsync(decodePartition)
    decoder.readTaggedFields()
    return { topicId, partitions }
  }

  return async (rawData) => {
    const decoder = new Decoder(rawData)
    const clientSideThrottleTime = decoder.readInt32()
    const errorCode = decoder.readInt16()
    const sessionId = decoder.readInt32()
    const responses = await decoder.readUVarIntArrayAsync(decodeResponse)
    const tags = decoder.readTaggedFields()

    return {
      throttleTime: 0,
      clientSideThrottleTime,
      errorCode,
      sessionId,
      responses,
      nodeEndpoints: decodeNodeEndpoints(tags),
    }
  }
}

// Built twice, at import, as v12/v13's decoder pairs are: nothing outside this file reads them.
const decoders = {
  true: buildDecode({ checkCrcs: true }),
  false: buildDecode({ checkCrcs: false }),
}

/**
 * @param {Buffer} rawData
 * @param {{ checkCrcs?: boolean }} [options]
 * @returns {Promise<object>}
 */
const decode = (rawData, options = {}) => decoders[options.checkCrcs !== false](rawData)

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Fetch',
  version: 16,
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
    { name: 'NodeEndpoints', type: '[]NodeEndpoint', versions: '16+', tag: 0 },
    { name: 'NodeEndpoints.NodeId', type: 'int32', versions: '16+' },
    { name: 'NodeEndpoints.Host', type: 'string', versions: '16+' },
    { name: 'NodeEndpoints.Port', type: 'int32', versions: '16+' },
    { name: 'NodeEndpoints.Rack', type: 'string', versions: '16+', nullable: true },
  ],
}
