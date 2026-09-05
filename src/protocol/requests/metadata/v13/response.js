const Decoder = require('../../../decoder')
const { parse: parseV0 } = require('../v0/response')
const { failure, createErrorFromCode } = require('../../../error')

/**
 * MetadataResponse (Version: 13) — flexible (KIP-482), response header v1
 *
 * ThrottleTimeMs => INT32 (3+)
 * Brokers => COMPACT_ARRAY (0+)
 *   NodeId => INT32, Host => COMPACT_STRING, Port => INT32, Rack => COMPACT_NULLABLE_STRING (1+)
 *   TAG_BUFFER
 * ClusterId => COMPACT_NULLABLE_STRING (2+)
 * ControllerId => INT32 (1+)
 * Topics => COMPACT_ARRAY (0+)
 *   ErrorCode => INT16, Name => COMPACT_STRING (nullable from 12), TopicId => UUID (10+),
 *   IsInternal => BOOLEAN (1+)
 *   Partitions => COMPACT_ARRAY (0+)
 *     ErrorCode, PartitionIndex, LeaderId, LeaderEpoch (7+), ReplicaNodes, IsrNodes,
 *     OfflineReplicas (5+)
 *     TAG_BUFFER
 *   TopicAuthorizedOperations => INT32 (8+)
 *   TAG_BUFFER
 * ErrorCode => INT16 (13+)
 * TAG_BUFFER
 *
 * ErrorCode is the top-level error code added in v13 — the transport for REBOOTSTRAP_REQUIRED
 * (129), which tells the client its metadata is stale and it should rebootstrap. It is
 * `ignorable` in the message spec only in the sender-side sense (droppable when writing an
 * older version); this decoder reads it unconditionally on every v13 response.
 * @see clients/src/main/resources/common/message/MetadataResponse.json
 */

const decodeInt32 = (decoder) => decoder.readInt32()

const decodeBroker = (decoder) => {
  const broker = {
    nodeId: decoder.readInt32(),
    host: decoder.readUVarIntString(),
    port: decoder.readInt32(),
    rack: decoder.readUVarIntString(),
  }

  decoder.readTaggedFields()
  return broker
}

const decodePartition = (decoder) => {
  const partition = {
    partitionErrorCode: decoder.readInt16(),
    partitionId: decoder.readInt32(),
    leader: decoder.readInt32(),
    leaderEpoch: decoder.readInt32(),
    replicas: decoder.readUVarIntArray(decodeInt32) || [],
    isr: decoder.readUVarIntArray(decodeInt32) || [],
    offlineReplicas: decoder.readUVarIntArray(decodeInt32) || [],
  }

  decoder.readTaggedFields()
  return partition
}

const decodeTopic = (decoder) => {
  const topic = {
    topicErrorCode: decoder.readInt16(),
    topic: decoder.readUVarIntString(),
    topicId: decoder.readUuid(),
    isInternal: decoder.readBoolean(),
    partitionMetadata: decoder.readUVarIntArray(decodePartition) || [],
    topicAuthorizedOperations: decoder.readInt32(),
  }

  decoder.readTaggedFields()
  return topic
}

/**
 * Decodes a MetadataResponse v13 payload.
 * @param {Buffer} rawData
 * @returns {Promise<object>} decoded response, keyed the same as v12 plus `errorCode`
 */
const decode = async (rawData) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const brokers = decoder.readUVarIntArray(decodeBroker) || []
  const clusterId = decoder.readUVarIntString()
  const controllerId = decoder.readInt32()
  const topicMetadata = decoder.readUVarIntArray(decodeTopic) || []
  const errorCode = decoder.readInt16()

  decoder.readTaggedFields()

  return {
    throttleTime: 0,
    clientSideThrottleTime: throttleTime,
    errorCode,
    brokers,
    clusterId,
    controllerId,
    topicMetadata,
  }
}

/**
 * The top-level error code is checked first, so a broker telling the client to rebootstrap
 * (REBOOTSTRAP_REQUIRED) surfaces as that error rather than as whatever topic-level error
 * happens to accompany it.
 * @param {object} data
 * @returns {Promise<object>} data
 * @throws {import('../../../../errors').KafkaJSProtocolError} on any error code
 */
const parse = async (data) => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return parseV0(data)
}

module.exports = {
  decode,
  parse,
}

module.exports.schema = {
  api: 'Metadata',
  version: 13,
  direction: 'response',
  fields: [
    { name: 'ThrottleTimeMs', type: 'int32', versions: '3+' },
    { name: 'Brokers', type: '[]MetadataResponseBroker', versions: '0+' },
    { name: 'Brokers.NodeId', type: 'int32', versions: '0+' },
    { name: 'Brokers.Host', type: 'string', versions: '0+' },
    { name: 'Brokers.Port', type: 'int32', versions: '0+' },
    { name: 'Brokers.Rack', type: 'string', versions: '1+', nullable: true },
    { name: 'ClusterId', type: 'string', versions: '2+', nullable: true },
    { name: 'ControllerId', type: 'int32', versions: '1+' },
    { name: 'Topics', type: '[]MetadataResponseTopic', versions: '0+' },
    { name: 'Topics.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Name', type: 'string', versions: '0+', nullable: true },
    { name: 'Topics.TopicId', type: 'uuid', versions: '10+' },
    { name: 'Topics.IsInternal', type: 'bool', versions: '1+' },
    { name: 'Topics.Partitions', type: '[]MetadataResponsePartition', versions: '0+' },
    { name: 'Topics.Partitions.ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Topics.Partitions.PartitionIndex', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.LeaderId', type: 'int32', versions: '0+' },
    { name: 'Topics.Partitions.LeaderEpoch', type: 'int32', versions: '7+' },
    { name: 'Topics.Partitions.ReplicaNodes', type: '[]int32', versions: '0+' },
    { name: 'Topics.Partitions.IsrNodes', type: '[]int32', versions: '0+' },
    { name: 'Topics.Partitions.OfflineReplicas', type: '[]int32', versions: '5+' },
    { name: 'Topics.TopicAuthorizedOperations', type: 'int32', versions: '8+' },
    { name: 'ErrorCode', type: 'int16', versions: '13+' },
  ],
}
