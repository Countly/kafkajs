const Decoder = require('../../../decoder')
const { parse: parseV12 } = require('./parse')
const decodeMessages = require('../v4/decodeMessages')

// The three KIP-320/KIP-951/Raft tagged fields on a partition response. They share a struct, so
// these are three distinct tags, not three namespaces.
const DIVERGING_EPOCH_TAG = 0
const CURRENT_LEADER_TAG = 1
const SNAPSHOT_ID_TAG = 2

/** @param {Map<number, Buffer>} tags @returns {{ epoch: number, endOffset: string }|null} */
const decodeDivergingEpoch = (tags) => {
  const raw = tags.get(DIVERGING_EPOCH_TAG)
  if (!raw) return null
  const decoder = new Decoder(raw)
  return { epoch: decoder.readInt32(), endOffset: decoder.readInt64().toString() }
}

/** @param {Map<number, Buffer>} tags @returns {{ leaderId: number, leaderEpoch: number }|null} */
const decodeCurrentLeader = (tags) => {
  const raw = tags.get(CURRENT_LEADER_TAG)
  if (!raw) return null
  const decoder = new Decoder(raw)
  return { leaderId: decoder.readInt32(), leaderEpoch: decoder.readInt32() }
}

/** @param {Map<number, Buffer>} tags @returns {{ endOffset: string, epoch: number }|null} */
const decodeSnapshotId = (tags) => {
  const raw = tags.get(SNAPSHOT_ID_TAG)
  if (!raw) return null
  const decoder = new Decoder(raw)
  return { endOffset: decoder.readInt64().toString(), epoch: decoder.readInt32() }
}

const decodeAbortedTransactions = (decoder) => {
  const abortedTransaction = {
    producerId: decoder.readInt64().toString(),
    firstOffset: decoder.readInt64().toString(),
  }
  decoder.readTaggedFields()
  return abortedTransaction
}

/**
 * The per-partition reader, lifted out of `buildDecode` so v13 can build its own decoder pair
 * from it: v13 differs from v12 only in the topic key, and copying this reader into it would be a
 * second body for one function.
 * @param {{ checkCrcs: boolean }} options
 * @returns {(decoder: Decoder) => Promise<object>}
 */
const buildDecodePartition = (options) => {
  const messageOptions = { ...options, compact: true }

  return async (decoder) => {
    const partition = decoder.readInt32()
    const errorCode = decoder.readInt16()
    const highWatermark = decoder.readInt64().toString()
    const lastStableOffset = decoder.readInt64().toString()
    const lastStartOffset = decoder.readInt64().toString()
    const abortedTransactions = decoder.readUVarIntArray(decodeAbortedTransactions) || []
    const preferredReadReplica = decoder.readInt32()
    const records = await decodeMessages(decoder, messageOptions)
    const tags = decoder.readTaggedFields()

    return {
      partition,
      errorCode,
      highWatermark,
      lastStableOffset,
      lastStartOffset,
      abortedTransactions,
      preferredReadReplica,
      divergingEpoch: decodeDivergingEpoch(tags),
      currentLeader: decodeCurrentLeader(tags),
      snapshotId: decodeSnapshotId(tags),
      ...records,
    }
  }
}

/**
 * Builds the reader chain for one value of `checkCrcs`. Called twice at module load so a fetch
 * response allocates no closures.
 * @param {{ checkCrcs: boolean }} options
 * @returns {(rawData: Buffer) => Promise<object>}
 */
const buildDecode = (options) => {
  const decodePartition = buildDecodePartition(options)

  const decodeResponse = async (decoder) => {
    const topicName = decoder.readUVarIntString()
    const partitions = Decoder.requireArray(
      await decoder.readUVarIntArrayAsync(decodePartition),
      'FetchResponse.Responses.Partitions'
    )
    decoder.readTaggedFields()
    return { topicName, partitions }
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

// Built twice, at import, exactly as `v11/response.js` does, so a fetch response allocates no
// closure. They are module-local: nothing outside this file reads them, and exporting them so a
// test could would be a test-only export.
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
  buildDecodePartition,
  parse: parseV12,
}

module.exports.schema = {
  api: 'Fetch',
  version: 12,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '1+' },
    { name: 'ErrorCode', type: 'int16', versions: '7+' },
    { name: 'SessionId', type: 'int32', versions: '7+' },
    { name: 'Responses', type: '[]FetchableTopicResponse', versions: '0+' },
    { name: 'Responses.Topic', type: 'string', versions: '0-12' },
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
