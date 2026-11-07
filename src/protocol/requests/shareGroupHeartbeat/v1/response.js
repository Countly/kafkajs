const { ShareDecoder: Decoder } = require('../../shareFetch/codec')

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeTopicPartitions = (decoder, options) => {
  const value = {
    topicId: decoder.readUuid(),
    partitions: decoder.readCompactArray((decoder) => decoder.readInt32()),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeAssignment = (decoder, options) => {
  const value = {
    topicPartitions: decoder.readCompactArray((decoder) => decodeTopicPartitions(decoder, options)),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeBody = (decoder, options) => {
  const value = {
    throttleTimeMs: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readCompactString(true),
    memberId: decoder.readCompactString(true),
    memberEpoch: decoder.readInt32(),
    heartbeatIntervalMs: decoder.readInt32(),
    assignment: decoder.readNullableStruct(decodeAssignment),
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
  api: 'ShareGroupHeartbeat',
  version: 1,
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
      name: 'MemberId',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'MemberEpoch',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'HeartbeatIntervalMs',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Assignment',
      type: 'Assignment',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'Assignment.TopicPartitions',
      type: '[]TopicPartitions',
      versions: '0+',
    },
    {
      name: 'Assignment.TopicPartitions.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'Assignment.TopicPartitions.Partitions',
      type: '[]int32',
      versions: '0+',
    },
  ],
}
