const { ShareDecoder: Decoder } = require('../../shareFetch/codec')
const decodeMessages = require('../../fetch/v4/decodeMessages')
const { KafkaJSPartialMessageError } = require('../../../../errors')

/** @param {Decoder} decoder @param {object} options @returns {Promise<object>} Validated compact Records envelope with inherited messages/corruption result. */
const readRecords = async (decoder, options) => {
  const start = decoder.offset
  const length = decoder.readUVarInt()
  if (length === 0) throw new KafkaJSPartialMessageError('Null required Records')
  decoder.ensureReadable(length - 1, 'compact Records')
  decoder.offset = start
  return decodeMessages(decoder, { ...options, compact: true })
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeCurrentLeader = (decoder, options) => {
  const value = {
    leaderId: decoder.readInt32(),
    leaderEpoch: decoder.readInt32(),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeAcquiredRecords = (decoder, options) => {
  const value = {
    firstOffset: decoder.readOffset(),
    lastOffset: decoder.readOffset(),
    deliveryCount: decoder.readInt16(),
  }
  decoder.readTaggedFields()
  if (BigInt(value.lastOffset) < BigInt(value.firstOffset))
    throw new KafkaJSPartialMessageError('Reversed acquired range')
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {Promise<object>} Decoded structure. */
const decodePartitions = async (decoder, options) => {
  const value = {
    partitionIndex: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readCompactString(true),
    acknowledgeErrorCode: decoder.readInt16(),
    acknowledgeErrorMessage: decoder.readCompactString(true),
    currentLeader: decodeCurrentLeader(decoder, options),
    ...(await readRecords(decoder, options)),
    acquiredRecords: decoder.readCompactArray((decoder) => decodeAcquiredRecords(decoder, options)),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {Promise<object>} Decoded structure. */
const decodeResponses = async (decoder, options) => {
  const value = {
    topicId: decoder.readUuid(),
    partitions: await decoder.readCompactArrayAsync(
      async (decoder) => await decodePartitions(decoder, options)
    ),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeNodeEndpoints = (decoder, options) => {
  const value = {
    nodeId: decoder.readInt32(),
    host: decoder.readCompactString(),
    port: decoder.readInt32(),
    rack: decoder.readCompactString(true),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {Promise<object>} Decoded structure. */
const decodeBody = async (decoder, options) => {
  const value = {
    throttleTimeMs: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readCompactString(true),
    acquisitionLockTimeoutMs: decoder.readInt32(),
    responses: await decoder.readCompactArrayAsync(
      async (decoder) => await decodeResponses(decoder, options)
    ),
    nodeEndpoints: decoder.readCompactArray((decoder) => decodeNodeEndpoints(decoder, options)),
  }
  decoder.readTaggedFields()
  return value
}

/**
 * Decode a released flexible response without classifying broker errors.
 * @param {Buffer} rawData Message body after the response header.
 * @param {{checkCrcs?: boolean}} [options] Inherited record decoder options.
 * @returns {Promise<object>} Exact field domains; int64 offsets/lag remain decimal strings.
 * @throws {KafkaJSPartialMessageError} on malformed or truncated fields.
 */
const decode = async (rawData, options = {}) => {
  const decoder = new Decoder(rawData)
  const { throttleTimeMs, ...data } = await decodeBody(decoder, options)
  decoder.finish()
  return { throttleTime: 0, clientSideThrottleTime: throttleTimeMs, ...data }
}
/** @param {object} data @returns {Promise<object>} Broker error codes remain data for their owning classifier. */
const parse = async (data) => data
module.exports = { decode, parse }

module.exports.schema = {
  api: 'ShareFetch',
  version: 2,
  direction: 'response',
  fields: [
    {
      name: 'ThrottleTimeMs',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'ErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'AcquisitionLockTimeoutMs',
      type: 'int32',
      versions: '1+',
    },
    {
      name: 'Responses',
      type: '[]ShareFetchableTopicResponse',
      versions: '0+',
    },
    {
      name: 'Responses.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions',
      type: '[]PartitionData',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.PartitionIndex',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.ErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'Responses.Partitions.AcknowledgeErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.AcknowledgeErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'Responses.Partitions.CurrentLeader',
      type: 'LeaderIdAndEpoch',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.CurrentLeader.LeaderId',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.CurrentLeader.LeaderEpoch',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.Records',
      type: 'records',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.AcquiredRecords',
      type: '[]AcquiredRecords',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.AcquiredRecords.FirstOffset',
      type: 'int64',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.AcquiredRecords.LastOffset',
      type: 'int64',
      versions: '0+',
    },
    {
      name: 'Responses.Partitions.AcquiredRecords.DeliveryCount',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'NodeEndpoints',
      type: '[]NodeEndpoint',
      versions: '0+',
    },
    {
      name: 'NodeEndpoints.NodeId',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'NodeEndpoints.Host',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'NodeEndpoints.Port',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'NodeEndpoints.Rack',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
  ],
}
