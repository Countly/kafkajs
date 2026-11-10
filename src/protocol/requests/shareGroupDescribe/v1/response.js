const { ShareDecoder: Decoder } = require('../../shareFetch/codec')

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeTopicPartitions = (decoder, options) => {
  const value = {
    topicId: decoder.readUuid(),
    topicName: decoder.readCompactString(),
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
const decodeMembers = (decoder, options) => {
  const value = {
    memberId: decoder.readCompactString(),
    rackId: decoder.readCompactString(true),
    memberEpoch: decoder.readInt32(),
    clientId: decoder.readCompactString(),
    clientHost: decoder.readCompactString(),
    subscribedTopicNames: decoder.readCompactArray((decoder) => decoder.readCompactString()),
    assignment: decodeAssignment(decoder, options),
  }
  decoder.readTaggedFields()
  return value
}

/** @param {Decoder} decoder @param {object} [options] @returns {object} Decoded structure. */
const decodeGroups = (decoder, options) => {
  const value = {
    errorCode: decoder.readInt16(),
    errorMessage: decoder.readCompactString(true),
    groupId: decoder.readCompactString(),
    groupState: decoder.readCompactString(),
    groupEpoch: decoder.readInt32(),
    assignmentEpoch: decoder.readInt32(),
    assignorName: decoder.readCompactString(),
    members: decoder.readCompactArray((decoder) => decodeMembers(decoder, options)),
    authorizedOperations: decoder.readInt32(),
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
  api: 'ShareGroupDescribe',
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
      type: '[]DescribedGroup',
      versions: '0+',
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
    {
      name: 'Groups.GroupId',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.GroupState',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.GroupEpoch',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Groups.AssignmentEpoch',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Groups.AssignorName',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Members',
      type: '[]Member',
      versions: '0+',
    },
    {
      name: 'Groups.Members.MemberId',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Members.RackId',
      type: 'string',
      versions: '0+',
      nullable: true,
    },
    {
      name: 'Groups.Members.MemberEpoch',
      type: 'int32',
      versions: '0+',
    },
    {
      name: 'Groups.Members.ClientId',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Members.ClientHost',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Members.SubscribedTopicNames',
      type: '[]string',
      versions: '0+',
    },
    {
      name: 'Groups.Members.Assignment',
      type: 'Assignment',
      versions: '0+',
    },
    {
      name: 'Groups.Members.Assignment.TopicPartitions',
      type: '[]TopicPartitions',
      versions: '0+',
    },
    {
      name: 'Groups.Members.Assignment.TopicPartitions.TopicId',
      type: 'uuid',
      versions: '0+',
    },
    {
      name: 'Groups.Members.Assignment.TopicPartitions.TopicName',
      type: 'string',
      versions: '0+',
    },
    {
      name: 'Groups.Members.Assignment.TopicPartitions.Partitions',
      type: '[]int32',
      versions: '0+',
    },
    {
      name: 'Groups.AuthorizedOperations',
      type: 'int32',
      versions: '0+',
    },
  ],
}
