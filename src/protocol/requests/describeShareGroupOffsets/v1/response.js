const { ShareDecoder: Decoder } = require('../../shareFetch/codec')

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodePartitions = (decoder, options) => {
  const value = {
    partitionIndex: decoder.readInt32(),
    startOffset: decoder.readOffset(true),
    leaderEpoch: decoder.readInt32(),
    lag: decoder.readOffset(true),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readCompactString(true),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeTopics = (decoder, options) => {
  const value = {
    topicName: decoder.readCompactString(),
    topicId: decoder.readUuid(),
    partitions: decoder.readCompactArray((decoder) => decodePartitions(decoder, options)),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeGroups = (decoder, options) => {
  const value = {
    groupId: decoder.readCompactString(),
    topics: decoder.readCompactArray((decoder) => decodeTopics(decoder, options)),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readCompactString(true),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeBody = (decoder, options) => {
  const value = {
    throttleTimeMs: decoder.readInt32(),
    groups: decoder.readCompactArray((decoder) => decodeGroups(decoder, options)),
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
  api: 'DescribeShareGroupOffsets',
  version: 1,
  direction: 'response',
  fields: [
    {
      name: 'ThrottleTimeMs',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Groups',
      type: '[]DescribeShareGroupOffsetsResponseGroup',
      versions: '0+',
    },
    {
      name: 'Groups.GroupId',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Topics',
      type: '[]DescribeShareGroupOffsetsResponseTopic',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.TopicName',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.Partitions',
      type: '[]DescribeShareGroupOffsetsResponsePartition',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.Partitions.PartitionIndex',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.Partitions.StartOffset',
      type: 'int64',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.Partitions.LeaderEpoch',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.Partitions.Lag',
      type: 'int64',
      versions: '1+',
    },
    {
      name: 'Groups.Topics.Partitions.ErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'Groups.Topics.Partitions.ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'Groups.ErrorCode',
      type: 'int16',
      versions: '0+',
    },
    {
      name: 'Groups.ErrorMessage',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
  ],
}
